package fqlite.timemap;

import fqlite.base.Global;
import fqlite.base.Limits;
import fqlite.base.ThemeManager;
import fqlite.rag.RAGPipeline;
import fqlite.sql.DBManager;
import fqlite.sql.QueryableDatabase;
import fqlite.sql.ReadOnlySqliteDatabase;
import fqlite.timemap.DataAnalyzer.DataPoint;
import fqlite.timemap.DataAnalyzer.ResponseRecordDataPoint;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.*;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.stage.FileChooser;
import javafx.stage.FileChooser.ExtensionFilter;

import java.io.File;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Root panel for the geo/time analysis feature.
 *
 * <h3>Layout</h3>
 * <pre>
 * ┌─────────────────────────────────────────────────────────────┐
 * │  Header bar                          [MBTiles status/btn]  │
 * ├──────────────────┬───────────────────────┬──────────────────┤
 * │  TimelineView    │  MapView              │  DetailPane      │
 * │  vertical,       │  fills remaining      │  scrollable      │
 * │  ~260 px wide    │  space                │  metadata        │
 * ├──────────────────┴───────────────────────┴──────────────────┤
 * │  LLM bar:  [prompt field]  [Ask ▶]  [status]               │
 * └─────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h3>LLM interaction</h3>
 * <p>The embedded prompt bar at the bottom sends the user's natural-language
 * question to the {@link RAGPipeline}.  The pipeline returns a SELECT statement
 * which is then executed directly against the attached {@link QueryableDatabase}
 * (the in-memory copy of recovered records, or the database file itself when
 * it was opened directly via {@link #setBaseDatabase}).
 * The resulting rows are converted to a {@link ConcurrentHashMap} and fed back
 * into {@link #setData} so the timeline, map and detail panel update
 * automatically — without opening a separate SQL Analyser window.</p>
 *
 * <p>Call {@link #attachLLM(RAGPipeline, QueryableDatabase)} once after the
 * pipeline has been initialised to enable the bar; until then it shows a
 * "not configured" hint and the button is disabled.</p>
 */
public class MapViewPane extends BorderPane {

    private static final Logger LOG = Logger.getLogger(MapViewPane.class.getName());

    // Sub-views
    private final TimelineView timelineView = new TimelineView();
    private final MapView      mapView      = new MapView();
    private final DetailPane   detailPane   = new DetailPane();
    private final PoliceAnalysisPane policeAnalysisPane = new PoliceAnalysisPane();

    // ── MOC/MTC tab ──────────────────────────────────────────────────────────
    // A second, independent MapView showing only call records (call_indicator
    // MOC = outgoing / MTC = incoming), colour-coded. Unlike the main map tab
    // it always reflects the *full* analyzed dataset (allPoints), not the
    // timeline's current brush filter, because the point of this tab is to
    // answer "where were all the calls", not "where were the currently
    // filtered rows that happen to also be calls".
    private final MapView mocMtcMapView = new MapView();
    private final Label   mocMtcCountLabel = new Label();
    /** time range of the MOC/MTC tab */
    private final CheckBox      mocTimeBox = new CheckBox();
    private final DateTimeField mocFrom    = new DateTimeField();
    private final DateTimeField mocTo      = new DateTimeField();
    private final Button        mocApply   = new Button();
    private final Label         mocArrow   = new Label("→");
    /** all MOC/MTC calls of the shown records (single records only) */
    private List<DataPoint> mocMtcBase = List.of();
    /** overview of time-filtered calls of a large database, or null */
    private Overview mocOverview;
    private int mocGeneration;
    /** Details list below {@link #mocMtcMapView}: one row per MOC/MTC call, with duration + direction. */
    private final TableView<DataPoint> mocMtcTable;
    private static final Color MOC_COLOR = Color.web("#22c55e"); // green-500 — outgoing
    private static final Color MTC_COLOR = Color.web("#3b82f6"); // blue-500  — incoming

    // State
    private final DataAnalyzer    analyzer         = new DataAnalyzer();
    private       Theme           theme            = Theme.DARK;
    private       List<DataPoint> allPoints        = List.of();
    private       List<DataPoint> currentMapPoints = List.of();

    // Shown while setData() analyzes a (potentially very large) dataset on a
    // background thread — see setData() for why this is no longer done
    // synchronously on the FX thread.
    private final ProgressIndicator loadingIndicator = new ProgressIndicator();
    private final Label             loadingLabel     = new Label("Lade Geodaten…");
    private final VBox              loadingOverlay   = new VBox(10, loadingIndicator, loadingLabel);

    // The database name is needed for SQLParser (key into DBManager)
    private String dbName = null;

    // ── Header widgets ────────────────────────────────────────────────────────
    private final Label  appTitle;
    private final Label  subtitle;
    private final HBox   titleBox;
    private final Button mbTilesButton;
    private final Label  mbTilesStatus;

    // ── LLM bar widgets ───────────────────────────────────────────────────────
    private final TextField llmPromptField;
    private final Button    llmRunButton;
    /** stops the running query (see {@link #cancelOperations}) */
    private final Button    llmCancelButton = new Button();
    private final Label     llmStatusLabel;
    private final HBox      llmBar;
    /** Shown instead of map redraw when an LLM prompt is classified as a co-location query. */
    private final TableView<CoLocationGroup> coLocationTable;
    /** Header text of these two reflects whichever identifier (IMSI/MSISDN/IMEI) the last query used. */
    private TableColumn<CoLocationGroup, Integer> colCoLocationCount;
    private TableColumn<CoLocationGroup, String>  colCoLocationIds;

    /** Shown alongside the map redraw when an LLM prompt is classified as a "roamers" (Wanderer) query. */
    private final TableView<RoamerRow> roamersTable;

    /** Shown alongside the map redraw when an LLM prompt is classified as a "wechsler" (Geräte-/SIM-Wechsel) query. */
    private final TableView<WechslerRow> wechslerTable;
    /** duration restriction of the current request of the assistant ("länger als 60 Minuten"), or null */
    private volatile RAGPipeline.DurationFilter llmDuration;

    /** {@code sql} with the duration restriction of the current request */
    private String withDuration(String sql) {
        RAGPipeline.DurationFilter d = llmDuration;
        return d == null ? sql : RAGPipeline.addCondition(sql, d.sql());
    }

    /** appends the duration restriction to the status line (FX thread) */
    private void appendDurationStatus() {
        RAGPipeline.DurationFilter d = llmDuration;
        if (d != null) llmStatusLabel.setText(llmStatusLabel.getText() + "  |  " + Lang.t("Dauer ", "duration ") + d.label());
    }

    /** participants (one row per IMSI) of a filter query of the assistant */
    private final TableView<ParticipantRow> participantsTable;
    /** query of the assistant the participants list belongs to (see {@link #showParticipants}) */
    private volatile int participantsGen;

    /** Set by {@link #attachLLM}; {@code null} means LLM not yet ready. */
    private RAGPipeline    ragPipeline  = null;
    /**
     * Database the generated SQL, the presets and the reset run against. Set
     * by {@link #attachLLM} or {@link #setBaseDatabase}.
     */
    private QueryableDatabase database = null;

    // =========================================================================
    // Construction
    // =========================================================================

    public MapViewPane() {

        // ── App title ─────────────────────────────────────────────────────────
        appTitle = new Label("\u00B7Lighthouse");
        appTitle.setFont(Font.font("Monospace", FontWeight.BOLD, 19));

        subtitle = new Label(Lang.t(
                "Forensische Anrufdatensatz-Analyse (CDR)",
                "Forensic & Call Data Record (CDR) Analysis"));
        subtitle.setFont(Font.font("Monospace", 14));
        Lang.addListener(() -> subtitle.setText(Lang.t(
                "Forensische Anrufdatensatz-Analyse (CDR)",
                "Forensic & Call Data Record (CDR) Analysis")));

        // Explicit German/English switch, independent of the system locale
        // Lang otherwise defaults to. Placed in the title bar so it's
        // reachable from anywhere in the window, not tucked into a submenu.
        Button langToggle = new Button(langToggleLabel());
        langToggle.setFont(Font.font("Monospace", 13));
        Tooltip langToggleTooltip = new Tooltip(Lang.t(
                "Zwischen deutscher und englischer Beschriftung umschalten.",
                "Switch between German and English labeling."));
        langToggle.setTooltip(langToggleTooltip);
        Lang.addListener(() -> langToggleTooltip.setText(Lang.t(
                "Zwischen deutscher und englischer Beschriftung umschalten.",
                "Switch between German and English labeling.")));
        langToggle.setOnAction(e -> {
            Lang.toggleGerman();
            langToggle.setText(langToggleLabel());
        });

        // ── MBTiles: Pfad aus fqlite.conf laden falls noch nicht gesetzt ──────
        autoLoadMbTilesFromConfig();

        // ── MBTiles offline control ───────────────────────────────────────────
        mbTilesButton = new Button(Lang.t("\uD83D\uDDFA\uFE0F Offline-Karte laden\u2026", "\uD83D\uDDFA\uFE0F Load offline map\u2026"));
        mbTilesButton.setFont(Font.font("Monospace", 14));
        mbTilesButton.setOnAction(e -> onMbTilesButtonClicked());
        Tooltip mbTilesTooltip = new Tooltip(Lang.t(
                "MBTiles-Datei als Offline-Kartenquelle laden.\n" +
                "Tiles werden zuerst aus dieser Datei bezogen;\n" +
                "fehlende Tiles werden weiterhin online geladen.",
                "Load an MBTiles file as an offline map source.\n" +
                "Tiles are fetched from this file first;\n" +
                "missing tiles are still loaded online."));
        Tooltip.install(mbTilesButton, mbTilesTooltip);
        Lang.addListener(() -> mbTilesTooltip.setText(Lang.t(
                "MBTiles-Datei als Offline-Kartenquelle laden.\n" +
                "Tiles werden zuerst aus dieser Datei bezogen;\n" +
                "fehlende Tiles werden weiterhin online geladen.",
                "Load an MBTiles file as an offline map source.\n" +
                "Tiles are fetched from this file first;\n" +
                "missing tiles are still loaded online.")));

        mbTilesStatus = new Label();
        mbTilesStatus.setFont(Font.font("Monospace", 13));
        updateMbTilesStatus();
        Lang.addListener(this::updateMbTilesStatus);

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        titleBox = new HBox(16, appTitle, subtitle, spacer, langToggle, mbTilesStatus, mbTilesButton);
        titleBox.setAlignment(Pos.CENTER_LEFT);
        titleBox.setPadding(new Insets(10, 20, 10, 20));
        setTop(titleBox);

        // ── Three-column layout via SplitPane ─────────────────────────────────
        timelineView.setMinWidth(210);
        timelineView.setPrefWidth(270);
        detailPane.setMinWidth(180);
        detailPane.setPrefWidth(220);

        SplitPane splitPane = new SplitPane(timelineView, mapView, detailPane);
        splitPane.setOrientation(javafx.geometry.Orientation.HORIZONTAL);
        splitPane.setDividerPositions(0.21, 0.83);

        // ── MOC/MTC tab: dedicated map showing only call records, coloured by
        // direction (outgoing/incoming) ────────────────────────────────────
        mocMtcMapView.setMarkerColorOverride(dp ->
                dp instanceof ResponseRecordDataPoint rr && "MTC".equals(rr.getCallIndicator())
                        ? MTC_COLOR : MOC_COLOR);
        mocMtcMapView.setSelectionListener(mocMtcMapView::focusPoint);

        Label mocLegendDot   = legendDot(MOC_COLOR);
        Label mtcLegendDot   = legendDot(MTC_COLOR);
        Region legendGap     = new Region();
        legendGap.setMinWidth(20);
        Region legendSpacer  = new Region();
        HBox.setHgrow(legendSpacer, Priority.ALWAYS);
        Label mocLegendLabel = new Label(Lang.t("MOC (ausgehend)", "MOC (outgoing)"));
        Label mtcLegendLabel = new Label(Lang.t("MTC (eingehend)", "MTC (incoming)"));
        Lang.addListener(() -> {
            mocLegendLabel.setText(Lang.t("MOC (ausgehend)", "MOC (outgoing)"));
            mtcLegendLabel.setText(Lang.t("MTC (eingehend)", "MTC (incoming)"));
        });
        HBox mocMtcLegend = new HBox(6,
                mocLegendDot, mocLegendLabel,
                legendGap, mtcLegendDot, mtcLegendLabel,
                legendSpacer, mocMtcCountLabel);
        mocMtcLegend.setAlignment(Pos.CENTER_LEFT);
        mocMtcLegend.setPadding(new Insets(6, 14, 6, 14));
        mocMtcLegend.setStyle("-fx-font-family: monospace; -fx-font-size: 14px;");

        mocMtcTable = buildMocMtcTable();

        SplitPane mocMtcSplit = new SplitPane(mocMtcMapView, mocMtcTable);
        mocMtcSplit.setOrientation(javafx.geometry.Orientation.VERTICAL);
        mocMtcSplit.setDividerPositions(0.62);

        BorderPane mocMtcTabContent = new BorderPane();
        mocTimeBox.setText(Lang.t("Zeitraum einschränken", "Restrict time range"));
        mocApply.setText(Lang.t("Anwenden", "Apply"));
        mocFrom.setPromptText(Lang.t("JJJJ-MM-TT (von)", "YYYY-MM-DD (from)"));
        mocTo.setPromptText(Lang.t("JJJJ-MM-TT (bis)", "YYYY-MM-DD (to)"));
        Lang.addListener(() -> {
            mocTimeBox.setText(Lang.t("Zeitraum einschränken", "Restrict time range"));
            mocApply.setText(Lang.t("Anwenden", "Apply"));
            mocFrom.setPromptText(Lang.t("JJJJ-MM-TT (von)", "YYYY-MM-DD (from)"));
            mocTo.setPromptText(Lang.t("JJJJ-MM-TT (bis)", "YYYY-MM-DD (to)"));
        });
        mocTimeBox.setFont(Font.font("Monospace", 14));
        mocFrom.setFieldFont(Font.font("Monospace", 14));
        mocTo.setFieldFont(Font.font("Monospace", 14));
        mocFrom.setDisable(true);
        mocTo.setDisable(true);
        mocApply.setDisable(true);
        mocTimeBox.setOnAction(e -> {
            boolean on = mocTimeBox.isSelected();
            mocFrom.setDisable(!on);
            mocTo.setDisable(!on);
            mocApply.setDisable(!on);
            refreshMocMtc();
        });
        mocApply.setOnAction(e -> refreshMocMtc());
        HBox mocTimeRow = new HBox(8, mocTimeBox, mocFrom, mocArrow, mocTo, mocApply);
        mocTimeRow.setAlignment(Pos.CENTER_LEFT);
        mocTimeRow.setPadding(new Insets(0, 14, 6, 14));
        mocMtcTabContent.setTop(new VBox(mocMtcLegend, mocTimeRow));
        mocMtcTabContent.setCenter(mocMtcSplit);

        // ── Tabs: map/timeline view vs. dedicated police-analysis panel ───────
        Tab mapTab = new Tab(Lang.t("🗺️ Karte & Zeitleiste", "🗺️ Map & Timeline"), splitPane);
        mapTab.setClosable(false);
        Tab mocMtcTab = new Tab("📞 MOC/MTC", mocMtcTabContent);
        mocMtcTab.setClosable(false);
        Tab policeTab = new Tab(Lang.t("🔎 Statistiken", "🔎 Statistics"), policeAnalysisPane);
        policeTab.setClosable(false);
        Lang.addListener(() -> {
            mapTab.setText(Lang.t("🗺️ Karte & Zeitleiste", "🗺️ Map & Timeline"));
            policeTab.setText(Lang.t("🔎 Statistiken", "🔎 Statistics"));
        });

        TabPane centerTabs = new TabPane(mapTab, mocMtcTab, policeTab);
        centerTabs.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);

        // Loading overlay: visible while setData() is analyzing a dataset on
        // a background thread, so opening the map with a large recovered
        // database shows immediate feedback instead of an apparently frozen
        // window.
        loadingIndicator.setMaxSize(64, 64);
        loadingLabel.setFont(Font.font("Monospace", 15));
        loadingOverlay.setAlignment(Pos.CENTER);
        loadingOverlay.setMouseTransparent(false);
        loadingOverlay.setStyle("-fx-background-color: rgba(0,0,0,0.55);");
        loadingOverlay.setVisible(false);
        loadingOverlay.setManaged(false);

        StackPane centerStack = new StackPane(centerTabs, loadingOverlay);
        setCenter(centerStack);

        // "Auf Karte anzeigen" aus dem Kontextmenü der Polizeiliche-Auswertung-
        // Ergebnistabelle: zurück zum Karten-Tab wechseln und den Punkt zentrieren.
        policeAnalysisPane.setMapCenterListener((lat, lon) -> {
            centerTabs.getSelectionModel().select(mapTab);
            mapView.centerOnCoordinate(lat, lon);
        });
        // Selects the row's IMSI in the path overlay too (when present —
        // see PoliceAnalysisPane#setImsiSelectListener), so a "Wanderer"
        // row shows every visited cell, not just the first one centered
        // on above.
        policeAnalysisPane.setImsiSelectListener(mapView::selectImsiForPath);
        // the identifiers of a statistics result: their records in map and timeline
        policeAnalysisPane.setResultListener(this::showStatisticsResult);

        // ── Wire selection events ─────────────────────────────────────────────
        timelineView.setSelectionListener(this::onTimelinePointSelected);
        mapView.setSelectionListener(this::onMapPointSelected);
        mapView.setLocationLoadListener(this::onOverviewLocationLoad);
        timelineView.setFilterListener(filtered -> {
            currentMapPoints = filtered;
            mapView.setData(filtered);
        });

        // ── LLM bar (bottom) ──────────────────────────────────────────────────
        llmPromptField = new TextField();
        llmPromptField.setPromptText(Lang.t(
                "Natürlichsprachige Anfrage an LLM (z. B. \"Alle Einträge mit GPS-Koordinaten\")…",
                "Natural-language query to the LLM (e.g. \"All entries with GPS coordinates\")…"));
        Lang.addListener(() -> llmPromptField.setPromptText(Lang.t(
                "Natürlichsprachige Anfrage an LLM (z. B. \"Alle Einträge mit GPS-Koordinaten\")…",
                "Natural-language query to the LLM (e.g. \"All entries with GPS coordinates\")…")));
        llmPromptField.setFont(Font.font("Monospace", 14));
        HBox.setHgrow(llmPromptField, Priority.ALWAYS);
        llmPromptField.setOnAction(e -> onLlmRun());   // Enter triggers run

        llmRunButton = new Button(Lang.t("Anfrage \u25B6", "Ask \u25B6"));
        Lang.addListener(() -> llmRunButton.setText(Lang.t("Anfrage \u25B6", "Ask \u25B6")));
        llmRunButton.setFont(Font.font("Monospace", FontWeight.BOLD, 14));
        Tooltip llmRunTooltip = new Tooltip(Lang.t(
                "Prompt an LLM senden → SELECT generieren → Karte aktualisieren",
                "Send prompt to LLM → generate SELECT → update map"));
        llmRunButton.setTooltip(llmRunTooltip);
        Lang.addListener(() -> llmRunTooltip.setText(Lang.t(
                "Prompt an LLM senden → SELECT generieren → Karte aktualisieren",
                "Send prompt to LLM → generate SELECT → update map")));
        llmRunButton.setDisable(true);   // enabled by attachLLM()
        llmRunButton.setOnAction(e -> onLlmRun());

        // Small "reset to full dataset" button
        Button llmResetButton = new Button("\u21BA Reset");
        llmResetButton.setFont(Font.font("Monospace", 14));
        Tooltip llmResetTooltip = new Tooltip(Lang.t(
                "Vollständige Datenmenge wiederherstellen",
                "Restore the full dataset"));
        llmResetButton.setTooltip(llmResetTooltip);
        Lang.addListener(() -> llmResetTooltip.setText(Lang.t(
                "Vollständige Datenmenge wiederherstellen",
                "Restore the full dataset")));
        llmResetButton.setOnAction(e -> onLlmReset());

        llmStatusLabel = new Label(Lang.t("\u23F3 LLM wird initialisiert\u2026", "\u23F3 Initializing LLM\u2026"));
        llmStatusLabel.setFont(Font.font("Monospace", 13));


        VBox bottom = new VBox();
        llmCancelButton.setText(Lang.t("■ Abbrechen", "■ Cancel"));
        Lang.addListener(() -> llmCancelButton.setText(Lang.t("■ Abbrechen", "■ Cancel")));
        llmCancelButton.setFont(Font.font("Monospace", FontWeight.BOLD, 14));
        llmCancelButton.setTooltip(new Tooltip(Lang.t("Laufende Abfrage abbrechen", "Cancel the running query")));
        llmCancelButton.setOnAction(e -> cancelOperations());
        llmCancelButton.setManaged(false);
        llmCancelButton.setVisible(false);
        llmBar = new HBox(8,
                new Label("\uD83E\uDD16"),
                llmPromptField,
                llmRunButton,
                llmCancelButton,
                llmResetButton
        );
        llmBar.setAlignment(Pos.CENTER_LEFT);
        llmBar.setPadding(new Insets(8, 14, 8, 14));

        coLocationTable = buildCoLocationTable();
        coLocationTable.setManaged(false);
        coLocationTable.setVisible(false);

        roamersTable = buildRoamersTable();
        roamersTable.setManaged(false);
        roamersTable.setVisible(false);

        wechslerTable = buildWechslerTable();
        wechslerTable.setManaged(false);
        wechslerTable.setVisible(false);

        participantsTable = buildParticipantsTable();
        participantsTable.setManaged(false);
        participantsTable.setVisible(false);

        bottom.setPadding(new Insets(8, 14, 8, 14));
        bottom.getChildren().add(llmBar);
        bottom.getChildren().add(llmStatusLabel);
        bottom.getChildren().add(coLocationTable);
        bottom.getChildren().add(roamersTable);
        bottom.getChildren().add(wechslerTable);
        bottom.getChildren().add(participantsTable);
        setBottom(bottom);



        // ── Theme ─────────────────────────────────────────────────────────────
        if (ThemeManager.isDark()) setTheme(Theme.DARK);
        else                       setTheme(Theme.LIGHT);

        applyThemeToSelf();
    }

    // =========================================================================
    // Public API
    // =========================================================================

    /**
     * Connects the LLM pipeline and the database to this pane so the prompt
     * bar becomes functional.
     *
     * <p>This method may be called at any time (even after the pane is already
     * visible).  It is idempotent: calling it again replaces the previous
     * pipeline.</p>
     *
     * @param pipeline an initialised {@link RAGPipeline} (schema already loaded)
     * @param mdb      the database that backs the current analysis
     */
    /** Button caption for the DE/EN toggle, showing the language it would switch to. */
    private static String langToggleLabel() {
        return Lang.isGerman() ? "DE | en" : "de | EN";
    }

    public void attachLLM(RAGPipeline pipeline, QueryableDatabase mdb) {
        this.ragPipeline = pipeline;
        this.database  = mdb;
        this.dbName      = mdb != null ? mdb.getDbName() : null;
        policeAnalysisPane.setDatabase(mdb);
        Platform.runLater(() -> {
            if (pipeline != null) {
                llmRunButton.setDisable(false);
                // Show which compute mode the local LLM actually ended up
                // running with (GPU offload vs. CPU-only) right behind the
                // ready-state text, so an examiner can tell at a glance why
                // a query might be slow without digging into logs.
                llmStatusLabel.setText(Lang.t("\u2705 LLM bereit (", "\u2705 LLM ready (") + pipeline.getComputeModeLabel() + ")");
                llmStatusLabel.setTextFill(theme.accent2);
                llmPromptField.setPromptText(Lang.t(
                        "Natürlichsprachige Anfrage an LLM (z. B. \"Alle Einträge mit GPS-Koordinaten\")…",
                        "Natural-language query to the LLM (e.g. \"All entries with GPS coordinates\")…"));
            } else {
                llmRunButton.setDisable(true);
                llmStatusLabel.setText(Lang.t("\u26AA LLM nicht verfügbar", "\u26AA LLM not available"));
                llmStatusLabel.setTextFill(theme.detailMuted);
            }
            applyLlmBarTheme();
        });
    }

    public void setData(
            ConcurrentHashMap<String, ObservableList<ObservableList<String>>> resultlist,
            Map<String, List<String>> headers) {
        setData(resultlist, headers, false);
    }

    /**
     * @param clearSequenceSelection forwarded to {@link MapView#setData(List, boolean)}
     *        — {@code true} forces the IMSI/IMEI sequence-analysis selectors
     *        back to "no selection" instead of restoring whatever was
     *        selected before. Used by {@link #onLlmReset()} so "Reset"
     *        actually returns the window to its initial state, instead of
     *        immediately re-running the same sequence analysis because the
     *        previously selected IMSI/IMEI is still present in the restored
     *        base dataset.
     */
    public void setData(
            ConcurrentHashMap<String, ObservableList<ObservableList<String>>> resultlist,
            Map<String, List<String>> headers,
            boolean clearSequenceSelection) {

        Map<String, List<String>> safeHeaders = new HashMap<>(headers);
        for (Map.Entry<String, ObservableList<ObservableList<String>>> e : resultlist.entrySet()) {
            String key = e.getKey();
            if (!safeHeaders.containsKey(key)) {
                int colCount = e.getValue().isEmpty() ? 0 : e.getValue().get(0).size();
                List<String> auto = new ArrayList<>();
                for (int i = 0; i < colCount; i++) auto.add("Col_" + i);
                safeHeaders.put(key, auto);
            }
        }

        // analyzer.analyze() walks every row of every supplied table looking
        // for timestamp/geo columns, and mapView.setData() below performs a
        // first synchronous render(). For a large forensic recovery (tens or
        // hundreds of thousands of rows, e.g. a full ETSI response_records
        // import) this used to run directly on the FX thread, which froze
        // the whole application — the map window didn't even become visible
        // until it finished, since LocationWindow.start() calls
        // setBaseData()/setData() before showing the stage. Do the analysis
        // off the FX thread and apply the result back via Platform.runLater,
        // mirroring the background-worker pattern already used by the LLM
        // query methods elsewhere in this class (see e.g. onLlmRun()).
        Platform.runLater(() -> {
            loadingLabel.setText("Lade Geodaten…");
            loadingOverlay.setManaged(true);
            loadingOverlay.setVisible(true);
        });
        Thread worker = new Thread(() -> {
            List<DataPoint> points = analyzer.analyze(resultlist, safeHeaders, (doneRows, totalRows) ->
                    Platform.runLater(() -> loadingLabel.setText(String.format(
                            "Lade Geodaten… %,d / %,d Zeilen", doneRows, totalRows))));
            Platform.runLater(() -> applyPoints(points, clearSequenceSelection));
        }, "mapview-analyze-worker");
        worker.setDaemon(true);
        worker.start();
    }

    /**
     * Shows already analysed points in timeline, maps and call table and hides
     * the loading overlay. Must run on the FX thread.
     */
    private void applyPoints(List<DataPoint> points, boolean clearSequenceSelection) {
        allPoints        = points;
        currentMapPoints = points;
        timelineView.setData(points);
        mapView.setData(points, clearSequenceSelection);
        mocMtcBase = filterMocMtc(points);
        presetMocRange();
        refreshMocMtc();
        detailPane.clear(theme);
        loadingOverlay.setVisible(false);
        loadingOverlay.setManaged(false);
    }

    public void setData(
            ConcurrentHashMap<String, ObservableList<ObservableList<String>>> resultlist) {
        setData(resultlist, Map.of());
    }

    /**
     * Executes a SELECT statement against this pane's database and redraws
     * the map/timeline with the result — the same display path used by
     * {@link #onLlmRun()}'s fixed-template branches.
     *
     * <p>Used by {@code fqlite.rag.LLMWindow}'s chat assistant (via
     * {@link LocationWindow#getActivePane()}) so a query run from the chat
     * window also updates an already-open map/timeline view, instead of
     * only showing the result in the chat's own separate SQL-Analyzer
     * results list. A no-op if no database is attached
     * ({@link #attachLLM} was never called) or {@code sql} isn't a SELECT.</p>
     *
     * @param sql a SELECT statement, typically one of the
     *            {@code RAGPipeline#build...Sql} fixed templates
     */
    public void runQueryAndDisplay(String sql) throws Exception {
        if (database == null || sql == null) return;
        String trimmed = sql.trim();
        if (!trimmed.regionMatches(true, 0, "SELECT", 0, 6)) return;

        if (costGate(null, List.of(trimmed), false, null, null) instanceof QueryCost.Cancel) {
            notRun();
            return;
        }
        QueryRows qr;
        beginOperation();
        try {
            qr = queryForDisplay(trimmed);
        } finally {
            endOperation();
        }
        if (qr == null) return;

        Platform.runLater(() -> {
            setCoLocationTableVisible(false);
            setParticipantsTableVisible(false);
            setRoamersTableVisible(false);
            setWechslerTableVisible(false);
            showQueryRows(qr);
            setLlmStatus("✅ " + String.format("%,d", qr.rowCount) + " " + Lang.t("Datensatz/Datensätze", "record(s)"), theme.accent2);
        });
    }

    /** Stores the base dataset that {@link #onLlmReset()} restores. */
    private ConcurrentHashMap<String, ObservableList<ObservableList<String>>> baseResultList;
    private Map<String, List<String>> baseHeaders;

    /**
     * Like {@link #setData(ConcurrentHashMap, Map)} but also remembers the
     * data as the "base" dataset for the reset button.
     */
    public void setBaseData(
            ConcurrentHashMap<String, ObservableList<ObservableList<String>>> resultlist,
            Map<String, List<String>> headers) {
        this.baseResultList = new ConcurrentHashMap<>(resultlist);
        this.baseHeaders    = new HashMap<>(headers);
        this.basePoints     = null;
        setData(resultlist, headers);
    }

    /**
     * Points of the base dataset when it was loaded directly from the database
     * file (see {@link #setBaseDatabase}); restored as-is by {@link #onLlmReset()}
     * instead of re-reading and re-analysing the whole table.
     */
    private volatile List<DataPoint> basePoints;

    /**
     * Uses a Lighthouse database file, opened directly and read-only, as the
     * base dataset — the alternative to {@link #setBaseData} for databases
     * that are too large to be recovered into memory by fqlite.
     *
     * <p>The table {@code response_records} is streamed row by row on a
     * separate connection (see {@link ReadOnlySqliteDatabase#openConnection()})
     * on a background thread straight into {@link DataPoint}s (see
     * {@link DataAnalyzer#analyzeResponseRecordsTable}); no copy of the table
     * is kept in memory, and complete rows are read on demand by rowid. Analysis presets and the query assistant run
     * their SQL against the file itself and use the importer's indexes. The
     * presets are available right away, the query assistant once the LLM is
     * attached via {@link #attachLLM}.</p>
     *
     * @param db the opened database; it stays owned by the caller
     */
    public void setBaseDatabase(ReadOnlySqliteDatabase db) {
        this.baseResultList = null;
        this.baseHeaders    = null;
        this.basePoints     = null;
        closeOverviews();
        this.baseOverview   = null;
        this.database       = db;
        this.dbName         = db.getDbName();
        policeAnalysisPane.setDatabase(db);

        Platform.runLater(() -> {
            loadingLabel.setText("Lade Geodaten…");
            loadingOverlay.setManaged(true);
            loadingOverlay.setVisible(true);
        });
        this.directDb = db;
        if (rowLoader != null) rowLoader.close();
        this.rowLoader = new DbRowLoader(db);
        Thread worker = new Thread(() -> {
            List<DataPoint> points;
            try {
                // large tables: overview (aggregates, records read page by page)
                long rows = Overview.count(db::openConnection, null);
                if (Limits.LIGHTHOUSE_OVERVIEW_ROWS == 0 || rows >= Limits.LIGHTHOUSE_OVERVIEW_ROWS) {
                    // an overview built before for this unchanged file is read from the cache
                    String key = cacheKey(db.getFile());
                    java.io.File cache = key == null ? null : cacheFile(key);
                    Overview cached = cache == null ? null : Overview.load(cache, key, analyzer, db::openConnection, rowLoader);
                    if (cached != null) cache.setLastModified(System.currentTimeMillis());   // recently used
                    Overview ov = cached != null ? cached
                            : Overview.build(analyzer, db::openConnection, rowLoader, null, null, rows,
                            (doneRows, all) -> Platform.runLater(() -> loadingLabel.setText(String.format(
                                    Lang.t("Erstelle Übersicht… %,d / %,d Zeilen", "Building overview… %,d / %,d rows"),
                                    doneRows, all))),
                            threads());
                    if (ov != null && cached == null && cache != null) {
                        try {
                            ov.save(cache, key);
                            pruneCache(cache.getParentFile());
                        } catch (Exception e) {
                            LOG.log(Level.INFO, "Overview cache not written: " + cache, e);
                        }
                    }
                    if (ov != null) {
                        Platform.runLater(() -> {
                            baseOverview = ov;
                            setupOverviewMode();
                            applyOverview(ov, true);
                            setLlmStatus(String.format(Lang.t(
                                    "Übersicht: %,d Datensätze – Ergebnisse bis %,d Datensätze werden vollständig geladen",
                                    "Overview: %,d records – results up to %,d records are loaded completely"),
                                    ov.getRecords(), Limits.LIGHTHOUSE_DETAIL_ROWS)
                                    + (cached != null ? Lang.t(" (Übersicht aus dem Cache)", " (overview from cache)") : ""),
                                    theme.label);
                        });
                        return;
                    }
                }
                // large tables are read by several connections in parallel
                points = analyzer.analyzeResponseRecordsTable(db::openConnection, (doneRows, all) ->
                        Platform.runLater(() -> loadingLabel.setText(String.format(
                                "Lade Geodaten… %,d / %,d Zeilen", doneRows, all))),
                        rowLoader, threads());
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "Loading response_records from " + db.getFile() + " failed", e);
                points = null;
            }
            if (points == null) {
                Platform.runLater(() -> {
                    loadingOverlay.setVisible(false);
                    loadingOverlay.setManaged(false);
                    setLlmStatus(Lang.t(
                            "⚠️ Tabelle response_records konnte nicht gelesen werden",
                            "⚠️ Table response_records could not be read"), theme.hover);
                });
                return;
            }
            final List<DataPoint> loaded = points;
            basePoints = loaded;
            Platform.runLater(() -> applyPoints(loaded, false));
        }, "mapview-db-loader");
        worker.setDaemon(true);
        worker.start();
    }

    // =========================================================================
    // Overview of large databases (see Overview)
    // =========================================================================

    /** database opened directly (setBaseDatabase), else null */
    private ReadOnlySqliteDatabase directDb;
    private DbRowLoader rowLoader;
    /** overview of the whole table when it is too large to load every record */
    private volatile Overview baseOverview;
    /** overview currently shown (base or filtered), or null when single records are shown */
    private Overview currentOverview;
    /** condition of the current query / location result (null: whole table) */
    private String resultCond;
    /** condition of the timeline filter bar (null: none) */
    private String timelineCond;
    /** condition currently displayed (resultCond AND timelineCond) */
    private String shownCond;
    /** guards against results of an older filter arriving after a newer one */
    private int filterGeneration;

    private static int threads() {
        return Math.max(1, Math.min(8, Runtime.getRuntime().availableProcessors()));
    }

    private DataAnalyzer.ConnectionSource source() {
        ReadOnlySqliteDatabase db = directDb;
        return () -> {
            checkCancelled();
            java.sql.Connection c = db.openConnection();
            try {
                for (Selection sel : selections) sel.attach(c);
            } catch (SQLException e) {
                c.close();
                throw e;
            }
            if (runningOps.get() > 0) {
                opConnections.removeIf(x -> { try { return x.isClosed(); } catch (SQLException e) { return true; } });
                opConnections.add(c);
            }
            return c;
        };
    }

    // ── running operations: cancel and duration estimate ─────────────────

    /** connections opened by running operations (interrupted by "Cancel") */
    private final Set<java.sql.Connection> opConnections = ConcurrentHashMap.newKeySet();
    private final java.util.concurrent.atomic.AtomicInteger runningOps = new java.util.concurrent.atomic.AtomicInteger();
    private volatile boolean cancelRequested;
    private final Map<String, Long> costRanges = QueryCost.newRangeCache();

    /** An operation (query, filter) starts: shows the cancel button. */
    private void beginOperation() {
        if (runningOps.getAndIncrement() == 0) cancelRequested = false;
        Platform.runLater(() -> {
            llmCancelButton.setManaged(true);
            llmCancelButton.setVisible(true);
            llmCancelButton.setDisable(false);
        });
    }

    /** An operation ended: hides the cancel button when none is left. */
    private void endOperation() {
        if (runningOps.decrementAndGet() <= 0) {
            runningOps.set(0);
            opConnections.clear();
            Platform.runLater(() -> {
                if (runningOps.get() > 0) return;
                llmCancelButton.setManaged(false);
                llmCancelButton.setVisible(false);
            });
        }
    }

    /**
     * Stops the running queries: every statement on the connections of the
     * running operations and on the shared connection is interrupted
     * (sqlite3_interrupt); the operations end with an error that is shown as
     * "cancelled", the data shown before stays.
     */
    private void cancelOperations() {
        if (runningOps.get() == 0) return;
        cancelRequested = true;
        llmCancelButton.setDisable(true);
        setLlmStatus(Lang.t("⏹ Wird abgebrochen…", "⏹ Cancelling…"), theme.label);
        for (java.sql.Connection c : opConnections) QueryCost.interrupt(c);
        if (database != null) QueryCost.interrupt(database.getConnectionObject());
    }

    /** true if the running operation was cancelled or {@code e} reports an interrupt */
    private boolean wasCancelled(Throwable e) {
        return cancelRequested || (e != null && QueryCost.isInterrupt(e));
    }

    /**
     * Estimates the duration of the queries {@code sqls} on a database opened
     * directly and asks whether to run them when it exceeds the setting
     * (Settings > Display &amp; limits). Returns {@link QueryCost#RUN} for small
     * databases and quick queries.
     *
     * @param what        description for the question (the request)
     * @param restrictable true if the query can be run with another time range
     * @param from        current start of the time range, or null
     * @param to          current end, or null
     */
    private QueryCost.Decision costGate(String what, List<String> sqls, boolean restrictable, String from, String to) {
        if (directDb == null || Limits.LIGHTHOUSE_CONFIRM_SECONDS == 0) return QueryCost.RUN;
        QueryCost.Estimate est;
        try (java.sql.Connection c = directDb.openConnection()) {
            if (tableRows < 0) tableRows = QueryCost.tableRows(c);
            est = QueryCost.estimate(c, sqls, tableRows, costRanges);
        } catch (SQLException e) {
            return QueryCost.RUN;
        }
        if (!QueryCost.needsConfirmation(est)) return QueryCost.RUN;
        String[] preset = null;
        if (restrictable) {
            if (from != null && to != null) {
                preset = new String[] { from, to };
            } else {
                String range = queryDataDateRange();
                java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                        "(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}).*?(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})")
                        .matcher(range == null ? "" : range);
                if (m.find()) preset = new String[] { m.group(1), m.group(2) };
            }
        }
        return QueryCost.ask(getScene() != null ? getScene().getWindow() : null, what, est, preset);
    }

    /**
     * Ends a cancelled operation: once "Cancel" was pressed, no further step
     * of the running operation starts (a statement interrupted earlier would
     * otherwise just be followed by the next one).
     */
    private void checkCancelled() throws SQLException {
        if (cancelRequested && runningOps.get() > 0) throw new SQLException("interrupted (cancelled)");
    }

    /** {@code database.execute} that stops a cancelled operation */
    private ResultSet exec(String sql) throws SQLException {
        checkCancelled();
        ResultSet rs = database.execute(sql);
        if (rs == null) checkCancelled();
        return rs;
    }

    /** status after "Cancel" in the question */
    private void notRun() {
        Platform.runLater(() -> {
            setLlmStatus(Lang.t("⏹ Nicht ausgeführt", "⏹ Not run"), theme.label);
            llmRunButton.setDisable(false);
        });
    }

    /** results of conditions with subqueries, evaluated once (see Selection) */
    private final List<Selection> selections = new java.util.concurrent.CopyOnWriteArrayList<>();

    /** Closes the selections no longer used by the shown data or the filters. */
    private void releaseSelections() {
        for (Selection sel : selections) {
            boolean used = sel.isUsedBy(resultCond) || sel.isUsedBy(shownCond)
                    || currentOverview != null && sel.isUsedBy(currentOverview.getCondition());
            if (!used) {
                selections.remove(sel);
                sel.close();
            }
        }
    }

    /** row counts of start_time ranges (see tuneTimeRanges) */
    private final Map<String, Long> rangeCounts = new ConcurrentHashMap<>();
    private long tableRows = -1;

    /**
     * Without statistics SQLite reads a {@code start_time BETWEEN} range
     * through the index even when it covers most of the table; on a large
     * file every row is then read in random order, which takes many minutes.
     * Ranges with more than 5 % of the records get a unary plus so they are
     * checked during a sequential scan instead (same result).
     */
    private String tuneTimeRanges(String sql) {
        if (directDb == null || sql == null) return sql;
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("(?i)(?<![+\\w.\"])start_time(\\s+BETWEEN\\s+'([^']*)'\\s+AND\\s+'([^']*)')").matcher(sql);
        StringBuilder sb = new StringBuilder();
        try (java.sql.Connection c = directDb.openConnection()) {
            if (tableRows < 0) {
                try (java.sql.Statement st = c.createStatement();
                     ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM response_records")) {
                    tableRows = rs.next() ? rs.getLong(1) : 0;
                }
            }
            while (m.find()) {
                String key = m.group(2) + "\u0000" + m.group(3);
                Long n = rangeCounts.get(key);
                if (n == null) {
                    try (java.sql.PreparedStatement ps = c.prepareStatement(
                            "SELECT COUNT(*) FROM response_records WHERE start_time BETWEEN ? AND ?")) {
                        ps.setString(1, m.group(2));
                        ps.setString(2, m.group(3));
                        try (ResultSet rs = ps.executeQuery()) { n = rs.next() ? rs.getLong(1) : 0L; }
                    }
                    rangeCounts.put(key, n);
                }
                boolean broad = tableRows > 0 && n * 20 > tableRows;
                m.appendReplacement(sb, java.util.regex.Matcher.quoteReplacement((broad ? "+start_time" : "start_time") + m.group(1)));
            }
            m.appendTail(sb);
            return sb.toString();
        } catch (SQLException e) {
            return sql;
        }
    }

    /** cached overviews kept at most (oldest are deleted) */
    private static final int CACHE_FILES = 20;

    /**
     * Identifies the state of a database file for the overview cache: path,
     * size and modification time of the file and of a -wal file. Any change
     * gives a new key, so a changed database is analysed again.
     */
    static String cacheKey(File db) {
        try {
            File f = db.getCanonicalFile();
            File wal = new File(f.getPath() + "-wal");
            return f.getPath() + "|" + f.length() + "|" + f.lastModified()
                    + "|" + (wal.isFile() ? wal.length() + "|" + wal.lastModified() : "-");
        } catch (java.io.IOException e) {
            return null;
        }
    }

    /**
     * Cache file of an overview: {@code ~/.fqlite/lighthouse-cache/<hash>.ovc}.
     * Kept outside the folder of the database, so evidence folders are not
     * written to.
     */
    static File cacheFile(String key) {
        try {
            byte[] h = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            File dir = new File(new File(System.getProperty("user.home"), ".fqlite"), "lighthouse-cache");
            if (!dir.isDirectory() && !dir.mkdirs()) return null;
            return new File(dir, java.util.HexFormat.of().formatHex(h) + ".ovc");
        } catch (java.security.NoSuchAlgorithmException e) {
            return null;
        }
    }

    /** keeps the most recently written cache files */
    private static void pruneCache(File dir) {
        File[] files = dir == null ? null : dir.listFiles((d, n) -> n.endsWith(".ovc"));
        if (files == null || files.length <= CACHE_FILES) return;
        Arrays.sort(files, Comparator.comparingLong(File::lastModified).reversed());
        for (int i = CACHE_FILES; i < files.length; i++) files[i].delete();
    }

    /** Releases the connections and selections of a database opened directly (window closed). */
    public void dispose() {
        closeOverviews();
        if (mocOverview != null) { mocOverview.close(); mocOverview = null; }
        for (Selection sel : selections) sel.close();
        selections.clear();
        if (rowLoader != null) rowLoader.close();
    }

    /** closes the page connections of all overviews */
    private void closeOverviews() {
        if (currentOverview != null && currentOverview != baseOverview) currentOverview.close();
        if (baseOverview != null) baseOverview.close();
        currentOverview = null;
    }

    /** Wires the timeline for a large database: filters run as SQL, pick lists come from the index. */
    private void setupOverviewMode() {
        timelineView.setExternalFilter(this::onTimelineFilter);
        timelineView.setPickSource(mode -> {
            Overview ov = baseOverview;
            return ov == null ? List.of() : ov.distinct(idColumn(mode));
        });
        timelineView.setWindowListener(this::onTimelineWindow);
        resultCond = null;
        timelineCond = null;
        shownCond = null;
    }

    private static String idColumn(TimelineView.IdMode mode) {
        return switch (mode) {
            case IMSI -> "imsi";
            case IMEI -> "na_device_id";
            case MSISDN -> "msisdn";
        };
    }

    /** Shows an overview in timeline, maps and call table. Must run on the FX thread. */
    private void applyOverview(Overview ov, boolean clearSequenceSelection) {
        if (currentOverview != null && currentOverview != ov && currentOverview != baseOverview)
            currentOverview.close();
        currentOverview = ov;
        allPoints = List.of();
        currentMapPoints = List.of();
        mapView.setOverviewData(ov.getRepresentatives(), ov.getTotals(), ov.getRecords(), ov, clearSequenceSelection);
        // after the map: the timeline may set the counts of its visible time span
        timelineView.setPagedData(new TimelineView.PagedSource() {
            @Override public int size() { return ov.timelineSize(); }
            @Override public DataPoint get(int idx) { return ov.timelineGet(idx); }
            @Override public java.time.Instant timeAt(int idx) { return ov.timelineTimeAt(idx); }
            @Override public int indexOf(DataPoint dp) { return ov.timelineIndexOf(dp); }
        });
        mocMtcBase = List.of();
        presetMocRange();
        refreshMocMtc();
        detailPane.clear(theme);
        loadingOverlay.setVisible(false);
        loadingOverlay.setManaged(false);
    }

    // ── MOC/MTC tab: time range ─────────────────────────────────────────────

    private static final java.util.regex.Pattern ISO_SECONDS =
            java.util.regex.Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");

    /**
     * Shows the calls of the current data in the MOC/MTC tab, restricted to the
     * time range when it is switched on: single records are filtered in memory
     * by their timestamp; on an overview of a large database the calls of the
     * range are read from the file (completely up to the detail limit, else as
     * an overview).
     */
    private void refreshMocMtc() {
        int gen = ++mocGeneration;
        if (mocOverview != null) { mocOverview.close(); mocOverview = null; }
        String from = null, to = null;
        if (mocTimeBox.isSelected()) {
            from = mocFrom.getText();
            to = mocTo.getText();
            if (!ISO_SECONDS.matcher(from).matches() || !ISO_SECONDS.matcher(to).matches()) {
                mocMtcCountLabel.setText(Lang.t("⚠️ Zeitraum unvollständig", "⚠️ Time range incomplete"));
                return;
            }
        }
        Overview ov = currentOverview;
        if (ov == null) {
            List<DataPoint> list = mocMtcBase;
            if (from != null) {
                java.time.Instant a = DataAnalyzer.parseIsoSeconds(from), z = DataAnalyzer.parseIsoSeconds(to);
                List<DataPoint> f = new ArrayList<>();
                for (DataPoint dp : list) {
                    java.time.Instant t = dp.getTimestamp();
                    if (t != null && a != null && z != null && !t.isBefore(a) && !t.isAfter(z)) f.add(dp);
                }
                list = f;
            }
            showMocPoints(list);
            return;
        }
        String ci = ov.column("call_indicator") != null ? ov.column("call_indicator") : "call_indicator";
        String st = ov.column("start_time") != null ? ov.column("start_time") : "start_time";
        if (from == null) {
            mocMtcMapView.setOverviewData(ov.getCallRepresentatives(), ov.getCallTotals(), ov.getCallRecords(), ov, false);
            loadMocTable(ov, gen, ci + " IN ('MOC','MTC')" + (ov.getCondition() != null ? " AND (" + ov.getCondition() + ")" : ""),
                    ov.getCallRecords());
            return;
        }
        String cond = tuneTimeRanges(ci + " IN ('MOC','MTC') AND " + st + " BETWEEN '" + from + "' AND '" + to + "'")
                + (ov.getCondition() != null ? " AND (" + ov.getCondition() + ")" : "");
        mocMtcCountLabel.setText(Lang.t("⏳ Anrufe werden gelesen…", "⏳ Reading calls…"));
        Thread t = new Thread(() -> {
            beginOperation();
            try {
                long n = Overview.count(source(), cond);
                if (n <= Limits.LIGHTHOUSE_DETAIL_ROWS) {
                    List<DataPoint> pts = n == 0 ? List.of()
                            : analyzer.analyzeResponseRecordsTable(source(), cond, n, null, rowLoader, threads());
                    List<DataPoint> calls = filterMocMtc(pts == null ? List.of() : pts);
                    Platform.runLater(() -> { if (gen == mocGeneration) showMocPoints(calls); });
                } else {
                    Overview co = Overview.build(analyzer, source(), rowLoader, cond, null, n, null, threads());
                    Platform.runLater(() -> {
                        if (gen != mocGeneration || co == null) { if (co != null) co.close(); return; }
                        mocOverview = co;
                        mocMtcMapView.setOverviewData(co.getCallRepresentatives(), co.getCallTotals(), co.getCallRecords(), co, false);
                        mocMtcTable.getItems().clear();
                        mocMtcCountLabel.setText(String.format(Lang.t(
                                "%,d Anrufe (Liste erst ab höchstens %,d – bitte filtern)", "%,d calls (list only up to %,d – please filter)"),
                                co.getCallRecords(), Limits.LIGHTHOUSE_DETAIL_ROWS));
                    });
                }
            } catch (Exception e) {
                boolean stop = wasCancelled(e);
                if (!stop) LOG.log(Level.WARNING, "Reading calls failed", e);
                Platform.runLater(() -> {
                    if (gen == mocGeneration)
                        mocMtcCountLabel.setText(stop ? Lang.t("⏹ Abgebrochen", "⏹ Cancelled") : "❌ " + e.getMessage());
                });
            } finally {
                endOperation();
            }
        }, "mapview-calls");
        t.setDaemon(true);
        t.start();
    }

    /** MOC/MTC tab with single calls (map and list) */
    private void showMocPoints(List<DataPoint> calls) {
        mocMtcMapView.setData(calls);
        mocMtcCountLabel.setText(String.format(Lang.t("%,d Anrufe", "%,d calls"), calls.size()));
        List<DataPoint> sorted = new ArrayList<>(calls);
        sorted.sort(Comparator.comparing(DataPoint::getTimestamp, Comparator.nullsLast(Comparator.naturalOrder())));
        mocMtcTable.getItems().setAll(sorted);
    }

    /** call list of an overview: loaded in the background up to the detail limit */
    private void loadMocTable(Overview ov, int gen, String callCond, long calls) {
        mocMtcTable.getItems().clear();
        if (calls > Limits.LIGHTHOUSE_DETAIL_ROWS) {
            mocMtcCountLabel.setText(String.format(Lang.t(
                    "%,d Anrufe (Liste erst ab höchstens %,d – bitte filtern)", "%,d calls (list only up to %,d – please filter)"),
                    calls, Limits.LIGHTHOUSE_DETAIL_ROWS));
            return;
        }
        mocMtcCountLabel.setText(String.format(Lang.t("%,d Anrufe", "%,d calls"), calls));
        Thread t = new Thread(() -> {
            try {
                List<DataPoint> pts = calls == 0 ? List.of()
                        : analyzer.analyzeResponseRecordsTable(source(), callCond, calls, null, rowLoader, threads());
                List<DataPoint> mocMtc = new ArrayList<>(filterMocMtc(pts == null ? List.of() : pts));
                mocMtc.sort(Comparator.comparing(DataPoint::getTimestamp, Comparator.nullsLast(Comparator.naturalOrder())));
                Platform.runLater(() -> { if (gen == mocGeneration && currentOverview == ov) mocMtcTable.getItems().setAll(mocMtc); });
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Reading calls failed", e);
            }
        }, "mapview-calls");
        t.setDaemon(true);
        t.start();
    }

    /**
     * Marks the time span of the data in the calendars of the MOC/MTC tab and
     * presets empty fields with it: single records from their timestamps, an
     * overview from the first and last start_time (index).
     */
    private void presetMocRange() {
        if (currentOverview == null) {
            java.time.Instant a = null, z = null;
            for (DataPoint dp : mocMtcBase) {
                java.time.Instant t = dp.getTimestamp();
                if (t == null) continue;
                if (a == null || t.isBefore(a)) a = t;
                if (z == null || t.isAfter(z)) z = t;
            }
            setMocRange(a, z);
            return;
        }
        QueryableDatabase db = database;
        if (db == null) return;
        Thread t = new Thread(() -> {
            String min = null, max = null;
            try {
                ResultSet rs = db.execute("SELECT start_time FROM response_records WHERE start_time >= '' "
                        + "AND typeof(start_time) = 'text' ORDER BY start_time LIMIT 1");
                if (rs != null) { if (rs.next()) min = rs.getString(1); rs.close(); }
                rs = db.execute("SELECT start_time FROM response_records WHERE start_time >= '' "
                        + "AND typeof(start_time) = 'text' ORDER BY start_time DESC LIMIT 1");
                if (rs != null) { if (rs.next()) max = rs.getString(1); rs.close(); }
            } catch (Exception e) {
                return;
            }
            java.time.Instant a = min == null ? null : DataAnalyzer.parseIsoSeconds(min.trim());
            java.time.Instant z = max == null ? null : DataAnalyzer.parseIsoSeconds(max.trim());
            Platform.runLater(() -> setMocRange(a, z));
        }, "mapview-call-range");
        t.setDaemon(true);
        t.start();
    }

    private void setMocRange(java.time.Instant a, java.time.Instant z) {
        if (a == null || z == null) return;
        java.time.LocalDateTime from = java.time.LocalDateTime.ofInstant(a, java.time.ZoneOffset.UTC);
        java.time.LocalDateTime to = java.time.LocalDateTime.ofInstant(z, java.time.ZoneOffset.UTC);
        mocFrom.setRange(from, to);
        mocTo.setRange(from, to);
        if (mocFrom.isEmpty()) mocFrom.setValue(from);
        if (mocTo.isEmpty()) mocTo.setValue(to);
    }

    /** Timeline "Map: visible period": counts of the visible time span on the map. */
    private void onTimelineWindow(java.time.Instant from, java.time.Instant to) {
        Overview ov = currentOverview;
        if (ov == null || !mapView.isOverview()) return;
        mapView.updateWeights(from == null ? ov.getTotals() : ov.weightsFor(from, to));
    }

    /** SQL string literal for a LIKE '%term%' (case-insensitive like the filter on loaded points) */
    private static String likeContains(String term) {
        String t = term.toLowerCase(Locale.ROOT).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                .replace("'", "''");
        return "'%" + t + "%' ESCAPE '\\'";
    }

    /** Filter bar of the timeline on a large database: the filter runs as SQL. */
    private void onTimelineFilter(TimelineView.FilterKind kind, String value, TimelineView.IdMode mode) {
        Overview base = baseOverview;
        if (base == null) return;
        String col = base.column(idColumn(mode));
        String term = value == null ? "" : value.trim();
        String st = base.column("start_time") != null ? base.column("start_time") : "start_time";
        boolean tableMatches = "response_records".contains(term.toLowerCase(Locale.ROOT));
        String cond;
        if (kind == TimelineView.FilterKind.PICK) {
            cond = col == null ? "0" : col + " = '" + term.replace("'", "''") + "'";
        } else if (term.isEmpty()) {
            cond = null;
        } else {
            String like = likeContains(term);
            cond = switch (kind) {
                case ID -> col == null ? "0" : col + " LIKE " + like;
                case TIMESTAMP -> st + " LIKE " + like;
                case TABLE -> tableMatches ? null : "0";
                default -> tableMatches ? null
                        : "(" + st + " LIKE " + like + (col != null ? " OR " + col + " LIKE " + like : "") + ")";
            };
        }
        timelineCond = cond;
        showCondition(and(resultCond, timelineCond), null);
    }

    private static String and(String a, String b) {
        if (a == null) return b;
        if (b == null) return a;
        return "(" + a + ") AND (" + b + ")";
    }

    /** Overview: double-click on a location loads its records. */
    private void onOverviewLocationLoad(DataPoint rep) {
        Overview ov = currentOverview;
        if (ov == null) return;
        String cond = ov.locationCondition(rep);
        if (cond == null) return;
        timelineView.clearFilterSilently();
        timelineCond = null;
        resultCond = cond;
        String where = rep.getCoordinate() != null ? rep.getCoordinate().toString() : "";
        showCondition(cond, Lang.t("Standort ", "Location ") + where);
    }

    /**
     * Shows the records matching {@code cond} (null: the base overview):
     * completely if they are at most {@link Limits#LIGHTHOUSE_DETAIL_ROWS},
     * else as an overview. Runs in the background.
     */
    private void showCondition(String cond, String label) {
        if (baseOverview == null) return;
        if (Objects.equals(cond, shownCond)) return;
        shownCond = cond;
        int gen = ++filterGeneration;
        if (cond == null) {
            applyOverview(baseOverview, false);
            setLlmStatus(String.format(Lang.t("Übersicht: %,d Datensätze", "Overview: %,d records"),
                    baseOverview.getRecords()), theme.label);
            return;
        }
        setLlmStatus(Lang.t("⏳ Filter wird ausgeführt…", "⏳ Running filter…"), theme.label);
        Thread worker = new Thread(() -> {
            beginOperation();
            try {
                DisplayResult r = loadCondition(cond, label, (doneRows, all) -> Platform.runLater(() -> {
                    if (gen == filterGeneration)
                        setLlmStatus(String.format(Lang.t("⏳ Lade %,d / %,d", "⏳ Loading %,d / %,d"), doneRows, all), theme.label);
                }));
                Platform.runLater(() -> {
                    if (gen != filterGeneration) { if (r.overview != null) r.overview.close(); return; }
                    showResult(r);
                    setLlmStatus((r.overview != null
                            ? String.format(Lang.t("Übersicht: %,d Datensätze", "Overview: %,d records"), r.count)
                            : String.format(Lang.t("✅ %,d Datensätze", "✅ %,d records"), r.count))
                            + (label != null ? " | " + label : ""), r.count == 0 ? theme.hover : theme.accent2);
                });
            } catch (Exception e) {
                boolean stop = wasCancelled(e);
                if (!stop) LOG.log(Level.WARNING, "Filter failed: " + cond, e);
                Platform.runLater(() -> {
                    if (gen != filterGeneration) return;
                    // the filter was not applied: the next change runs it again
                    shownCond = "\u0000";
                    setLlmStatus(stop ? Lang.t("⏹ Filter abgebrochen", "⏹ Filter cancelled") : "❌ " + e.getMessage(),
                            stop ? theme.label : theme.hover);
                });
            } finally {
                endOperation();
            }
        }, "mapview-filter");
        worker.setDaemon(true);
        worker.start();
    }

    /** records of a condition: loaded completely or as an overview */
    private static final class DisplayResult {
        final List<DataPoint> points;
        final Overview overview;
        final long count;
        /** the condition actually used (a selection replaces a condition with subqueries) */
        String cond;
        DisplayResult(List<DataPoint> points, Overview overview, long count) {
            this.points = points; this.overview = overview; this.count = count;
        }
    }

    /** Reads the records matching {@code cond} (on the calling, non-FX thread). */
    private DisplayResult loadCondition(String cond, String label, DataAnalyzer.ProgressListener progress) throws SQLException {
        return loadCondition(cond, label, progress, null);
    }

    /** @param pre the records of {@code cond} if already selected (see directRoamers), else null */
    private DisplayResult loadCondition(String cond, String label, DataAnalyzer.ProgressListener progress,
                                        Selection pre) throws SQLException {
        String loadCond = cond;
        long count;
        java.util.BitSet rows = null;
        if (pre != null || DataAnalyzer.hasSubquery(cond)) {
            // evaluate the subqueries once; counting, loading and the pages
            // then use the stored rowids
            Selection sel = pre;
            if (sel == null) {
                try (java.sql.Connection c = source().open()) {
                    sel = Selection.create(c, cond);
                }
            }
            selections.add(sel);
            cond = sel.condition();
            loadCond = sel.lookupCondition();
            count = sel.count();
            rows = sel.rows();
        } else {
            count = Overview.count(source(), cond);
        }
        DisplayResult r;
        if (baseOverview == null || count <= Limits.LIGHTHOUSE_DETAIL_ROWS) {
            List<DataPoint> pts = count == 0 ? List.of()
                    : analyzer.analyzeResponseRecordsTable(source(), loadCond, count, progress, rowLoader, threads());
            r = new DisplayResult(pts == null ? List.of() : pts, null, count);
        } else {
            Overview ov = Overview.build(analyzer, source(), rowLoader, cond, label, count, progress, threads(), rows);
            r = new DisplayResult(null, ov, count);
        }
        r.cond = cond;
        return r;
    }

    private void showResult(DisplayResult r) {
        if (r.overview != null) {
            applyOverview(r.overview, false);
        } else {
            if (currentOverview != null && currentOverview != baseOverview) currentOverview.close();
            currentOverview = null;
            applyPoints(r.points, false);
        }
        releaseSelections();
    }

    /**
     * Condition of a query of the form {@code SELECT * FROM response_records
     * [WHERE ...] [ORDER BY ...]} (the templates of the query assistant), so
     * that its result can be read directly from the file; {@code ""} for all
     * records, {@code null} for any other query.
     */
    static String simpleCondition(String sql) {
        if (sql == null) return null;
        String t = sql.trim();
        while (t.endsWith(";")) t = t.substring(0, t.length() - 1).trim();
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("(?is)^SELECT\\s+\\*\\s+FROM\\s+response_records\\b(.*)$").matcher(t);
        if (!m.matches()) return null;
        String rest = m.group(1).trim();
        if (rest.isEmpty()) return "";
        // keywords at top level (outside parentheses and quotes)
        int depth = 0;
        boolean inStr = false, inId = false;
        int orderAt = -1;
        String upper = rest.toUpperCase(Locale.ROOT);
        for (int i = 0; i < rest.length(); i++) {
            char ch = rest.charAt(i);
            if (inStr) { if (ch == '\'') inStr = false; continue; }
            if (inId) { if (ch == '"') inId = false; continue; }
            if (ch == '\'') { inStr = true; continue; }
            if (ch == '"') { inId = true; continue; }
            if (ch == '(') { depth++; continue; }
            if (ch == ')') { depth--; continue; }
            if (depth != 0) continue;
            boolean wordStart = i == 0 || !(Character.isLetterOrDigit(rest.charAt(i - 1)) || rest.charAt(i - 1) == '_');
            if (!wordStart) continue;
            for (String kw : new String[] { "LIMIT", "GROUP", "HAVING", "UNION", "INTERSECT", "EXCEPT", "JOIN", "WINDOW", "OFFSET" }) {
                int e = i + kw.length();
                if (upper.startsWith(kw, i) && (e >= rest.length() || !(Character.isLetterOrDigit(rest.charAt(e)) || rest.charAt(e) == '_')))
                    return null;
            }
            if (orderAt < 0 && upper.startsWith("ORDER", i)) orderAt = i;
        }
        if (depth != 0 || inStr || inId) return null;
        String head = (orderAt >= 0 ? rest.substring(0, orderAt) : rest).trim();
        if (head.isEmpty()) return "";
        if (head.length() < 6 || !head.regionMatches(true, 0, "WHERE", 0, 5)
                || !Character.isWhitespace(head.charAt(5))) return null;
        String cond = head.substring(5).trim();
        return cond.isEmpty() ? null : cond;
    }

    /**
     * Runs a query for display: on a database opened directly, queries of the
     * simple form ({@link #simpleCondition}) are read from the file (compact
     * points, or an overview when the base is an overview and the result is
     * large); any other query is read into memory as before.
     *
     * @return the rows, or {@code null} if the query failed
     */
    private QueryRows queryForDisplay(String sql) throws Exception {
        return queryForDisplay(sql, null);
    }

    /** @param pre the records of {@code sql} if already selected, else null */
    private QueryRows queryForDisplay(String sql, Selection pre) throws Exception {
        checkCancelled();
        sql = tuneTimeRanges(sql);
        String cond = directDb != null ? simpleCondition(sql) : null;
        if (cond != null) {
            String c = cond.isEmpty() ? null : cond;
            DisplayResult r;
            try {
                r = loadCondition(c, null, (doneRows, all) -> Platform.runLater(() ->
                        setLlmStatus(String.format(Lang.t("⏳ Lade %,d / %,d", "⏳ Loading %,d / %,d"), doneRows, all), theme.label)),
                        pre);
            } catch (SQLException e) {
                LOG.log(Level.INFO, "Query failed: " + sql, e);
                return null;
            }
            List<String> cols = new ArrayList<>();
            try (java.sql.Connection conn = source().open();
                 java.sql.Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT * FROM response_records LIMIT 0")) {
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) cols.add(rs.getMetaData().getColumnName(i));
            }
            ConcurrentHashMap<String, ObservableList<ObservableList<String>>> data = new ConcurrentHashMap<>();
            data.put("response_records", FXCollections.observableArrayList());
            Map<String, List<String>> headers = new HashMap<>();
            headers.put("response_records", cols);
            QueryRows qr = new QueryRows(data, headers, (int) Math.min(Integer.MAX_VALUE, r.count));
            qr.direct = r;
            qr.cond = r.cond;
            return qr;
        }
        ResultSet rs = exec(sql);
        if (rs == null) return null;
        try {
            return toResponseRecordsTable(rs);
        } finally {
            rs.close();
        }
    }

    /** Shows rows returned by {@link #queryForDisplay}. Must run on the FX thread. */
    private void showQueryRows(QueryRows qr) {
        if (baseOverview != null) {
            timelineView.clearFilterSilently();
            timelineCond = null;
            ++filterGeneration;
        }
        if (qr.direct != null) {
            if (baseOverview != null) {
                timelineView.setExternalFilter(this::onTimelineFilter);
                resultCond = qr.cond;
                shownCond = qr.cond;
            }
            showResult(qr.direct);
        } else {
            if (baseOverview != null) {
                // a query that is no simple condition: the timeline filter then
                // filters the loaded records only
                timelineView.setExternalFilter(null);
                resultCond = null;
                shownCond = "\u0000";
                if (currentOverview != null && currentOverview != baseOverview) currentOverview.close();
                currentOverview = null;
            }
            setData(qr.data, qr.headers, false);
        }
    }

    /**
     * Reads single {@code response_records} rows by rowid for points loaded via
     * {@link #setBaseDatabase} (tooltip, detail panel, copy). The last row is
     * cached, since a tooltip or the detail panel asks for several fields of
     * the same point in a row.
     */
    private static final class DbRowLoader implements DataAnalyzer.RawRowLoader {
        private final ReadOnlySqliteDatabase db;
        private long lastRowId = Long.MIN_VALUE;
        private List<String> lastRow;
        /**
         * Own connection: the shared one of the database is used by the
         * analyses and the query assistant, and a long query there (a preset,
         * a Wanderer grouping) would block this lookup - it runs on the FX
         * thread for tooltips and the detail panel, so the whole window hung
         * until the query had finished.
         */
        private java.sql.Connection conn;
        private java.sql.PreparedStatement byRowid;

        DbRowLoader(ReadOnlySqliteDatabase db) { this.db = db; }

        @Override
        public synchronized List<String> load(long rowId) {
            if (rowId == lastRowId && lastRow != null) return lastRow;
            List<String> row = new ArrayList<>();
            try {
                if (conn == null) {
                    conn = db.openConnection();
                    byRowid = conn.prepareStatement("SELECT * FROM response_records WHERE rowid = ?");
                }
                java.sql.PreparedStatement ps = byRowid;
                ps.setLong(1, rowId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        int n = rs.getMetaData().getColumnCount();
                        for (int i = 1; i <= n; i++) row.add(rs.getString(i));
                    }
                }
            } catch (SQLException e) {
                LOG.log(Level.WARNING, "Reading response_records row " + rowId + " failed", e);
                close();
                return List.of();
            }
            lastRowId = rowId;
            lastRow = row;
            return row;
        }

        synchronized void close() {
            try { if (conn != null) conn.close(); } catch (SQLException ignored) { }
            conn = null;
            byRowid = null;
        }
    }

    public void setTheme(Theme t) {
        this.theme = t;
        applyThemeToSelf();
        timelineView.applyTheme(t);
        mapView.applyTheme(t);
        mocMtcMapView.applyTheme(t);
        mocMtcTable.setStyle(theme.bgStyle() + theme.borderTopStyle());
        String fieldCss = "-fx-background-color: " + Theme.hex(t.bg) + "; -fx-text-fill: " + Theme.hex(t.labelStrong) + ";"
                + "-fx-border-color: " + Theme.hex(t.border) + "; -fx-border-width: 1px; -fx-background-radius: 3; -fx-border-radius: 3;";
        mocFrom.setFieldStyle(fieldCss);
        mocTo.setFieldStyle(fieldCss);
        mocFrom.setLabelColor(t.label);
        mocTo.setLabelColor(t.label);
        mocTimeBox.setTextFill(t.label);
        mocArrow.setTextFill(t.label);
        detailPane.applyTheme(t);
        policeAnalysisPane.applyTheme(t);
    }

    /**
     * Builds a small filled-circle {@link Label} used as a colour swatch in
     * the MOC/MTC tab's legend bar.
     */
    private static Label legendDot(Color c) {
        Label dot = new Label("●"); // ●
        dot.setTextFill(c);
        dot.setStyle("-fx-font-size: 17px;");
        return dot;
    }

    /**
     * Builds the details list shown below {@link #mocMtcMapView}: one row per
     * MOC/MTC call with its time, direction, duration, MSISDN, IMSI and
     * coordinate. Clicking a row centers {@link #mocMtcMapView} on that
     * record, mirroring the row-click convention used by
     * {@link #buildRoamersTable()} etc.
     */
    private TableView<DataPoint> buildMocMtcTable() {
        TableView<DataPoint> table = new TableView<>();
        Label mocMtcPlaceholder = new Label(Lang.t("Keine MOC/MTC-Einträge gefunden.", "No MOC/MTC entries found."));
        table.setPlaceholder(mocMtcPlaceholder);
        Lang.addListener(() -> mocMtcPlaceholder.setText(Lang.t("Keine MOC/MTC-Einträge gefunden.", "No MOC/MTC entries found.")));

        TableColumn<DataPoint, String> colTime = new TableColumn<>(Lang.t("Zeit", "Time"));
        colTime.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().getFormattedTimestamp()));
        colTime.setPrefWidth(150);

        TableColumn<DataPoint, String> colDirection = new TableColumn<>(Lang.t("Richtung", "Direction"));
        colDirection.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(callDirectionLabel(cd.getValue())));
        colDirection.setPrefWidth(150);

        TableColumn<DataPoint, String> colDuration = new TableColumn<>(Lang.t("Dauer", "Duration"));
        colDuration.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(
                formatCallDuration(cd.getValue() instanceof ResponseRecordDataPoint rr ? rr.getDurationSeconds() : null)));
        colDuration.setPrefWidth(90);

        TableColumn<DataPoint, String> colMsisdn = new TableColumn<>(Lang.t("Rufnummer", "Phone_Number"));
        colMsisdn.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().getMsisdn()));
        colMsisdn.setPrefWidth(130);

        TableColumn<DataPoint, String> colImsi = new TableColumn<>("IMSI");
        colImsi.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().getImsi()));
        colImsi.setPrefWidth(160);

        TableColumn<DataPoint, String> colCoord = new TableColumn<>(Lang.t("Koordinate", "Coordinate"));
        colCoord.setCellValueFactory(cd -> {
            DataAnalyzer.GeoCoordinate c = cd.getValue().getCoordinate();
            return new ReadOnlyObjectWrapper<>(c != null ? c.toString() : "–");
        });
        colCoord.setPrefWidth(150);

        table.getColumns().addAll(List.of(colTime, colDirection, colDuration, colMsisdn, colImsi, colCoord));
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
        Lang.addListener(() -> {
            colTime.setText(Lang.t("Zeit", "Time"));
            colDirection.setText(Lang.t("Richtung", "Direction"));
            colDuration.setText(Lang.t("Dauer", "Duration"));
            colMsisdn.setText(Lang.t("Rufnummer", "Phone_Number"));
            colCoord.setText(Lang.t("Koordinate", "Coordinate"));
        });

        table.setRowFactory(tv -> {
            TableRow<DataPoint> row = new TableRow<>();
            row.setOnMouseClicked(e -> {
                DataPoint dp = row.getItem();
                if (dp == null) return;
                mocMtcMapView.focusPoint(dp);
            });
            return row;
        });

        // Copy selected cells to clipboard via context menu or Ctrl/Cmd+C.
        TableClipboardSupport.install(table, "lighthouse_moc_mtc.csv");

        return table;
    }

    /**
     * "Ausgehend (MOC)" / "Eingehend (MTC)" / "–" for anything else — mirrors
     * the colour mapping installed via {@link MapView#setMarkerColorOverride}
     * on {@link #mocMtcMapView} so the list and the map markers agree.
     */
    private static String callDirectionLabel(DataPoint dp) {
        if (!(dp instanceof ResponseRecordDataPoint rr)) return "–";
        String ci = rr.getCallIndicator();
        if ("MOC".equals(ci)) return Lang.t("Ausgehend (MOC)", "Outgoing (MOC)");
        if ("MTC".equals(ci)) return Lang.t("Eingehend (MTC)", "Incoming (MTC)");
        return "–";
    }

    /**
     * Formats a raw {@code duration_seconds} string as {@code HH:MM:SS},
     * mirroring {@link SequenceAnalyzer.Stop#formattedDwell()}'s convention.
     * Returns {@code "–"} if {@code raw} is null/blank, not parseable, or zero.
     */
    private static String formatCallDuration(String raw) {
        if (raw == null || raw.isBlank()) return "–";
        long seconds;
        try {
            seconds = (long) Double.parseDouble(raw.trim());
        } catch (NumberFormatException e) {
            return "–";
        }
        if (seconds <= 0) return "–";
        long h = seconds / 3600;
        long m = (seconds % 3600) / 60;
        long s = seconds % 60;
        return String.format("%02d:%02d:%02d", h, m, s);
    }

    /**
     * Filters {@code points} down to {@link ResponseRecordDataPoint}s whose
     * {@code call_indicator} marks them as an actual call (MOC = outgoing,
     * MTC = incoming) — i.e. excludes SMS (MOM/MTM), GPRS (SGP) and any
     * non-call-record table. Points without a resolvable coordinate are
     * dropped too, though {@link MapView#setData} would filter those out on
     * its own; doing it here as well keeps {@link #mocMtcCountLabel}'s count
     * accurate.
     */
    private static List<DataPoint> filterMocMtc(List<DataPoint> points) {
        List<DataPoint> out = new ArrayList<>();
        for (DataPoint p : points) {
            if (p.getCoordinate() == null) continue;
            if (p instanceof ResponseRecordDataPoint rr) {
                String ci = rr.getCallIndicator();
                if ("MOC".equals(ci) || "MTC".equals(ci)) out.add(rr);
            }
        }
        return out;
    }

    public Theme getTheme() { return theme; }

    // =========================================================================
    // LLM bar handlers
    // =========================================================================

    /**
     * Sends the prompt to the LLM. The prompt is first classified as either
     * a "co-location" question ("welche IMSIs waren gemeinsam in einer
     * Funkzelle?") or a plain filter query; co-location queries are answered
     * via a fixed SQL self-join + results table (see {@link #runColocationQuery}),
     * everything else keeps the original generateSQL → map-redraw flow
     * (see {@link #runFilterQuery}).
     *
     * <p>The whole process runs on a daemon thread to keep the UI responsive.
     * Progress feedback is given through {@link #llmStatusLabel}.</p>
     */
    /**
     * Queries the in-memory database for the actual date range of
     * {@code response_records.start_time} and returns a human-readable
     * "MIN bis MAX" string (e.g. {@code "2024-09-01 00:00:00 bis 2024-09-30 23:59:59"})
     * that is injected into the LLM intent-classification prompt so the model
     * derives missing years from the dataset rather than from the current
     * calendar year. Returns {@code null} if the DB is unavailable or empty.
     */
    private String queryDataDateRange() {
        if (database == null) return null;
        try {
            java.sql.ResultSet rs = exec(
                    "SELECT MIN(start_time), MAX(start_time) FROM response_records WHERE start_time IS NOT NULL");
            if (rs == null) return null;
            try {
                if (rs.next()) {
                    String min = rs.getString(1);
                    String max = rs.getString(2);
                    if (min != null && !min.isBlank() && max != null && !max.isBlank()) {
                        return min + Lang.t(" bis ", " to ") + max;
                    }
                }
            } finally {
                rs.close();
            }
        } catch (Exception ignored) { }
        return null;
    }

    private void onLlmRun() {
        String prompt = llmPromptField.getText().trim();
        if (prompt.isEmpty() || ragPipeline == null) return;
        lastLlmQuestion = prompt;
        participantsGen++;

        llmRunButton.setDisable(true);
        setLlmStatus(Lang.t("⏳ Analysiere Anfrage…", "⏳ Analyzing query…"), theme.label);
        setCoLocationTableVisible(false);
        setParticipantsTableVisible(false);
        setRoamersTableVisible(false);
        setWechslerTableVisible(false);

        Thread worker = new Thread(() -> {
            beginOperation();
            try {
                llmDuration = RAGPipeline.findDurationMentionedInText(prompt);
                RAGPipeline.QueryIntent intent = ragPipeline.classifyIntent(prompt, queryDataDateRange());

                if (intent.isAmbiguousTimeOfDayWithoutDate()) {
                    // The request named a time-of-day (e.g. "zwischen 10:00
                    // und 10:30 Uhr") but no date, and the loaded dataset
                    // spans more than one day, so classifyIntent couldn't
                    // safely guess which day is meant (see
                    // RAGPipeline.resolveDateForTimeOnlyWindow). Checked
                    // before every other branch below — cell/country
                    // overrides and every QueryIntent.is*() gate would
                    // otherwise run with an unrestricted or guessed time
                    // window instead of surfacing the ambiguity.
                    Platform.runLater(() -> {
                        setLlmStatus(Lang.t("⚠️ Uhrzeit ohne Datum — die Datenbank umfasst mehrere Tage, bitte ein Datum ergänzen", "⚠️ Time without a date — the database spans multiple days, please add a date"), theme.hover);
                        llmRunButton.setDisable(false);
                    });
                    return;
                }

                // Deterministic safety net, checked FIRST: the small local
                // model unreliably classifies country-name requests — it has
                // been observed to mistake them for an (ambiguous) co-location
                // request instead of country_filter (e.g. a request without a
                // time range comes back as intent="colocation" with no time
                // window, triggering a bogus clarification prompt; with a time
                // range it comes back as intent="colocation" WITH a window,
                // which would silently run a co-location query instead of an
                // actual country filter). Country names are a small, fixed,
                // enumerable set, so check the raw prompt directly and let an
                // unambiguous country mention always win, regardless of what
                // the model classified the rest of the request as.
                String mentionedPrefix = CountryCallingCodeLookup.findPrefixMentionedInText(prompt);
                String[] cellIdentifier = RAGPipeline.findCellIdentifierMentionedInText(prompt);
                if (mentionedPrefix != null) {
                    String country = CountryCallingCodeLookup.getCountryNameByPrefix(mentionedPrefix);
                    runCountryFilterQuery(country, intent.startTime, intent.endTime);
                } else if (cellIdentifier != null) {
                    // Same reasoning as the country-mention check above: a
                    // named LAC/TAC/CI is a small, unambiguous numeric token
                    // the model has no reliable way to place into structured
                    // JSON, and — critically — letting this fall through to
                    // intent.isPlainTimeFilter() below would silently drop
                    // the cell condition and return every record in the time
                    // window instead of just the ones at that site.
                    runCellFilterQuery(cellIdentifier[0], cellIdentifier[1], intent.callSmsType, intent.startTime, intent.endTime);
                } else if (intent.isColocation()) {
                    runColocationQuery(intent);
                } else if (intent.isAmbiguousColocation()) {
                    // Recognised as a co-location request, but no clean time
                    // window could be extracted. Falling back to the
                    // free-form SQL generator here would silently run a
                    // query it isn't equipped to express (it can't reliably
                    // build the self-join a co-location query needs) — ask
                    // for clarification instead.
                    Platform.runLater(() -> {
                        setLlmStatus(Lang.t("⚠️ Zeitraum konnte nicht erkannt werden — bitte Zeitraum präzisieren", "⚠️ Could not recognize the time range — please specify a time range"), theme.hover);
                        llmRunButton.setDisable(false);
                    });
                } else if (intent.isCallsOrSms()) {
                    runCallsSmsQuery(intent);
                } else if (intent.isCountryFilter()) {
                    runCountryFilterQuery(intent);
                } else if (intent.isRoamers()) {
                    runRoamersQuery(intent);
                } else if (intent.isWechsler()) {
                    runWechslerQuery(intent);
                } else if (intent.isPlainTimeFilter()) {
                    // classifyIntent already extracted a clean time window
                    // for this plain filter request — run the fixed SQL
                    // template directly instead of paying for a second,
                    // much slower free-form generateSQL() call (see
                    // RAGPipeline.buildPlainFilterSql).
                    runPlainTimeFilterQuery(intent);
                } else {
                    // For intent="filter" pass the date-normalized text (ISO dates,
                    // years injected) to the SQL generator — the raw prompt may
                    // contain partial dates like "02. Juli" that the small model
                    // resolves to the wrong year when given the original text.
                    String filterPrompt = (intent.normalizedRequest != null)
                            ? intent.normalizedRequest : prompt;
                    runFilterQuery(filterPrompt);
                }
            } catch (Exception ex) {
                boolean stop = wasCancelled(ex);
                Platform.runLater(() -> {
                    setLlmStatus(stop ? Lang.t("⏹ Abgebrochen", "⏹ Cancelled") : "❌ " + ex.getMessage(),
                            stop ? theme.label : theme.hover);
                    llmRunButton.setDisable(false);
                });
            } finally {
                boolean stop = cancelRequested;
                endOperation();
                if (stop) Platform.runLater(() -> {
                    setLlmStatus(Lang.t("⏹ Abgebrochen – die bisherige Anzeige bleibt", "⏹ Cancelled – the previous view stays"), theme.label);
                    llmRunButton.setDisable(false);
                });
            }
        }, "llm-map-worker");
        worker.setDaemon(true);
        worker.start();
    }

    /**
     * Original LLM flow: the model generates a SELECT, which is executed and
     * fed into {@link #setData} so the timeline/map/detail panel redraw.
     * Runs on the calling (worker) thread; only UI updates are marshalled
     * via {@link Platform#runLater}.
     */
    private void runFilterQuery(String prompt) throws Exception {
        // 1. LLM generates SELECT
        String sql = ragPipeline.generateSQL(prompt);
        if (!ragPipeline.isValidSQL(sql)) {
            Platform.runLater(() -> {
                setLlmStatus(Lang.t("⚠️ LLM konnte keine gültige SQL erzeugen — bitte Anfrage umformulieren", "⚠️ LLM could not generate valid SQL — please rephrase the query"), theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }
        if (costGate(prompt, List.of(sql), false, null, null) instanceof QueryCost.Cancel) {
            notRun();
            return;
        }
        Platform.runLater(() -> setLlmStatus(Lang.t("⚙️ Führe aus: ", "⚙️ Running: ") + abbreviate(sql, 60), theme.label));

        // 2. Execute SQL (database file or in-memory DB)
        QueryRows qr = queryForDisplay(sql);
        if (qr == null) {
            // Statt nur "Kein Ergebnis": die SQLite-Fehlermeldung anzeigen (z.B. unbekannte Spalte)
            String err = fqlite.rag.SqlRepair.prepareError(database.getConnectionObject(), sql);
            final String msg = err != null
                    ? Lang.t("⚠️ SQL-Fehler: ", "⚠️ SQL error: ") + abbreviate(err, 90)
                    : Lang.t("⚠️ Kein Ergebnis", "⚠️ No result");
            Platform.runLater(() -> {
                setLlmStatus(msg, theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        final String finalSql = sql;

        // 4. Ergebnisse ohne Zeitstempel + Koordinaten (Anzahlen, Summen, Kontaktlisten, …)
        //    lassen sich nicht auf Karte/Zeitleiste darstellen → Ergebnisfenster statt setData.
        //    Karte und Zeitleiste bleiben dabei unveraendert.
        List<String> resultCols = qr.headers.getOrDefault("response_records", List.of());
        if (!isMapDisplayable(resultCols) || qr.rowCount == 0) {
            List<List<String>> resultRows = new ArrayList<>(qr.data.getOrDefault(
                    "response_records", FXCollections.observableArrayList()));
            Platform.runLater(() -> {
                ResultPopup.show(getScene() != null ? getScene().getWindow() : null, theme,
                        prompt, finalSql, resultCols, resultRows);
                setLlmStatus(qr.rowCount == 0
                        ? Lang.t("⚠️ Keine Treffer", "⚠️ No matches") + " | " + abbreviate(finalSql, 50)
                        : "✅ " + Lang.t("Ergebnis im Fenster angezeigt", "Result shown in window")
                          + " | " + abbreviate(finalSql, 50),
                        qr.rowCount == 0 ? theme.hover : theme.accent2);
                llmRunButton.setDisable(false);
            });
            return;
        }

        Platform.runLater(() -> {
            setCoLocationTableVisible(false);
            setParticipantsTableVisible(false);
            setRoamersTableVisible(false);
            setWechslerTableVisible(false);
            showQueryRows(qr);
            setLlmStatus(
                    "✅ " + qr.rowCount + " " + Lang.t("Zeile(n)", "row(s)") + " | " + abbreviate(finalSql, 50),
                    theme.accent2);
            llmRunButton.setDisable(false);
        });
        showParticipants(sql, qr);
    }

    /**
     * Calls/SMS flow: executes the fixed SQL template (built by
     * {@link RAGPipeline#buildCallsSmsSql}) for the direction (incoming
     * call, outgoing call, sent SMS, received SMS) and optional time window
     * the LLM extracted. Like {@link #runFilterQuery}, the result is plain
     * {@code response_records} rows, so it's fed straight into
     * {@link #setData} to redraw the map/timeline — there's no pairing step
     * like in {@link #runColocationQuery}.
     */
    private void runCallsSmsQuery(RAGPipeline.QueryIntent intent) throws Exception {
        Platform.runLater(() -> setLlmStatus(Lang.t("⚙️ Suche Anrufe/SMS…", "⚙️ Searching calls/SMS…"), theme.label));

        String sql0 = RAGPipeline.buildCallsSmsSql(intent.callSmsType, intent.startTime, intent.endTime);
        String sql = withDuration(sql0);
        if (sql == null) {
            Platform.runLater(() -> {
                setLlmStatus(Lang.t("⚠️ Anfrage konnte nicht zugeordnet werden", "⚠️ Could not classify the query"), theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        QueryCost.Decision decision = costGate(lastLlmQuestion, List.of(sql), true, intent.startTime, intent.endTime);
        if (decision instanceof QueryCost.Cancel) {
            notRun();
            return;
        }
        if (decision instanceof QueryCost.Restrict r) {
            runCallsSmsQuery(intent.withTimeRange(r.from(), r.to()));
            return;
        }

        QueryRows qr = queryForDisplay(sql);
        if (qr == null) {
            Platform.runLater(() -> {
                setLlmStatus(Lang.t("⚠️ Kein Ergebnis", "⚠️ No result"), theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        if (showNoHitsIfEmpty(qr, sql)) return;   // Karte unveraendert lassen, "Keine Treffer" im Fenster

        Platform.runLater(() -> {
            setCoLocationTableVisible(false);
            setParticipantsTableVisible(false);
            setRoamersTableVisible(false);
            setWechslerTableVisible(false);
            showQueryRows(qr);
            setLlmStatus(
                    qr.rowCount == 0
                            ? Lang.t("⚠️ Keine passenden Anrufe/SMS im Zeitfenster gefunden", "⚠️ No matching calls/SMS found in the time window")
                            : "✅ " + qr.rowCount + " " + Lang.t("Datensatz/Datensätze gefunden", "record(s) found"),
                    qr.rowCount == 0 ? theme.hover : theme.accent2);
            appendDurationStatus();
            llmRunButton.setDisable(false);
        });
        showParticipants(sql, qr);
    }

    /**
     * Plain time-window filter flow: executes the fixed SQL template (built
     * by {@link RAGPipeline#buildPlainFilterSql}) for the time window the
     * LLM extracted, skipping the free-form {@link RAGPipeline#generateSQL}
     * call entirely. Like {@link #runCallsSmsQuery}, the result is plain
     * {@code response_records} rows fed straight into {@link #setData} to
     * redraw the map/timeline.
     */
    private void runPlainTimeFilterQuery(RAGPipeline.QueryIntent intent) throws Exception {
        Platform.runLater(() -> setLlmStatus(Lang.t("⚙️ Suche Teilnehmer im Zeitfenster…", "⚙️ Searching subscribers in the time window…"), theme.label));

        String sql0 = RAGPipeline.buildPlainFilterSql(intent.startTime, intent.endTime);
        String sql = withDuration(sql0);
        if (sql == null) {
            Platform.runLater(() -> {
                setLlmStatus(Lang.t("⚠️ Zeitraum konnte nicht erkannt werden — bitte Zeitraum präzisieren", "⚠️ Could not recognize the time range — please specify a time range"), theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        QueryCost.Decision decision = costGate(lastLlmQuestion, List.of(sql), true, intent.startTime, intent.endTime);
        if (decision instanceof QueryCost.Cancel) {
            notRun();
            return;
        }
        if (decision instanceof QueryCost.Restrict r) {
            runPlainTimeFilterQuery(intent.withTimeRange(r.from(), r.to()));
            return;
        }

        QueryRows qr = queryForDisplay(sql);
        if (qr == null) {
            Platform.runLater(() -> {
                setLlmStatus(Lang.t("⚠️ Kein Ergebnis", "⚠️ No result"), theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        if (showNoHitsIfEmpty(qr, sql)) return;   // Karte unveraendert lassen, "Keine Treffer" im Fenster

        Platform.runLater(() -> {
            setCoLocationTableVisible(false);
            setParticipantsTableVisible(false);
            setRoamersTableVisible(false);
            setWechslerTableVisible(false);
            showQueryRows(qr);
            setLlmStatus(
                    qr.rowCount == 0
                            ? Lang.t("⚠️ Keine Teilnehmer im Zeitfenster gefunden", "⚠️ No subscribers found in the time window")
                            : "✅ " + qr.rowCount + " " + Lang.t("Datensatz/Datensätze gefunden", "record(s) found"),
                    qr.rowCount == 0 ? theme.hover : theme.accent2);
            appendDurationStatus();
            llmRunButton.setDisable(false);
        });
        showParticipants(sql, qr);
    }

    /**
     * Cell/LAC/CI filter flow: executes the fixed SQL template (built by
     * {@link RAGPipeline#buildCellFilterSql}) for the LAC/TAC and/or CI
     * value found in the raw prompt by
     * {@link RAGPipeline#findCellIdentifierMentionedInText}, plus the
     * (optional) call/SMS direction and time window {@code classifyIntent}
     * still extracted despite the cell mention taking over dispatch (e.g.
     * "alle ausgehenden Anrufe an der Funkzelle mit CellID 19" — without
     * passing {@code callSmsType} through here, the direction would be
     * silently dropped and every record at that cell returned instead of
     * just the outgoing calls). Like
     * {@link #runCountryFilterQuery(String, String, String)}, this is
     * reached directly from the deterministic text check in
     * {@link #onLlmRun()}, not via one of the {@code QueryIntent.is*()}
     * gates.
     */
    private void runCellFilterQuery(String lac, String ci, String callSmsType, String startTime, String endTime) throws Exception {
        String label = (lac != null ? "LAC/TAC " + lac : "") + (lac != null && ci != null ? ", " : "") + (ci != null ? "CI " + ci : "");
        String directionLabel = switch (callSmsType == null ? "" : callSmsType) {
            case "ausgehender_anruf" -> Lang.t("ausgehende Anrufe ", "outgoing calls ");
            case "eingehender_anruf" -> Lang.t("eingehende Anrufe ", "incoming calls ");
            case "gesendete_sms" -> Lang.t("gesendete SMS ", "sent SMS ");
            case "empfangene_sms" -> Lang.t("empfangene SMS ", "received SMS ");
            case "alle_anrufe" -> Lang.t("Anrufe ", "calls ");
            case "alle_sms" -> Lang.t("SMS ", "SMS ");
            default -> "";
        };
        Platform.runLater(() -> setLlmStatus(Lang.t("⚙️ Suche ", "⚙️ Searching ") + directionLabel + Lang.t("an Funkzelle ", "at cell site ") + label + "…", theme.label));

        String sql0 = RAGPipeline.buildCellFilterSql(lac, ci, callSmsType, startTime, endTime);
        String sql = withDuration(sql0);
        if (sql == null) {
            Platform.runLater(() -> {
                setLlmStatus(Lang.t("⚠️ Funkzelle konnte nicht erkannt werden", "⚠️ Could not recognize the cell site"), theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        QueryCost.Decision decision = costGate(lastLlmQuestion, List.of(sql), true, startTime, endTime);
        if (decision instanceof QueryCost.Cancel) {
            notRun();
            return;
        }
        if (decision instanceof QueryCost.Restrict r) {
            runCellFilterQuery(lac, ci, callSmsType, r.from(), r.to());
            return;
        }

        QueryRows qr = queryForDisplay(sql);
        if (qr == null) {
            Platform.runLater(() -> {
                setLlmStatus(Lang.t("⚠️ Kein Ergebnis", "⚠️ No result"), theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        if (showNoHitsIfEmpty(qr, sql)) return;   // Karte unveraendert lassen, "Keine Treffer" im Fenster

        Platform.runLater(() -> {
            setCoLocationTableVisible(false);
            setParticipantsTableVisible(false);
            setRoamersTableVisible(false);
            setWechslerTableVisible(false);
            showQueryRows(qr);
            setLlmStatus(
                    qr.rowCount == 0
                            ? Lang.t("⚠️ Keine ", "⚠️ No ") + directionLabel + Lang.t("an Funkzelle ", "at cell site ") + label + Lang.t(" im Zeitfenster gefunden", " found in the time window")
                            : "✅ " + qr.rowCount + " " + directionLabel + Lang.t("an Funkzelle ", "at cell site ") + label,
                    qr.rowCount == 0 ? theme.hover : theme.accent2);
            appendDurationStatus();
            llmRunButton.setDisable(false);
        });
        showParticipants(sql, qr);
    }

    /**
     * Country-filter flow: executes the fixed SQL template (built by
     * {@link RAGPipeline#buildCountryFilterSql}) for the country name and
     * optional time window the LLM extracted. Like
     * {@link #runCallsSmsQuery}, the result is plain {@code response_records}
     * rows fed straight into {@link #setData} to redraw the map/timeline.
     */
    private void runCountryFilterQuery(RAGPipeline.QueryIntent intent) throws Exception {
        runCountryFilterQuery(intent.country, intent.startTime, intent.endTime);
    }

    /**
     * Same flow as {@link #runCountryFilterQuery(RAGPipeline.QueryIntent)},
     * but takes the country name directly instead of via a
     * {@link RAGPipeline.QueryIntent} — used by the deterministic country-
     * mention fallback in {@link #onLlmRun()}, which resolves the country
     * itself ({@link CountryCallingCodeLookup#findPrefixMentionedInText})
     * rather than relying on the LLM's intent classification.
     */
    private void runCountryFilterQuery(String country, String startTime, String endTime) throws Exception {
        Platform.runLater(() -> setLlmStatus(Lang.t("⚙️ Suche Rufnummern aus ", "⚙️ Searching phone numbers from ") + country + "…", theme.label));

        String sql0 = RAGPipeline.buildCountryFilterSql(country, startTime, endTime);
        String sql = withDuration(sql0);
        if (sql == null) {
            Platform.runLater(() -> {
                setLlmStatus(Lang.t("⚠️ Land \"" + country + "\" konnte keiner Vorwahl zugeordnet werden", "⚠️ Country \"" + country + "\" could not be matched to a calling code"), theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        QueryCost.Decision decision = costGate(lastLlmQuestion, List.of(sql), true, startTime, endTime);
        if (decision instanceof QueryCost.Cancel) {
            notRun();
            return;
        }
        if (decision instanceof QueryCost.Restrict r) {
            runCountryFilterQuery(country, r.from(), r.to());
            return;
        }

        QueryRows qr = queryForDisplay(sql);
        if (qr == null) {
            Platform.runLater(() -> {
                setLlmStatus(Lang.t("⚠️ Kein Ergebnis", "⚠️ No result"), theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        if (showNoHitsIfEmpty(qr, sql)) return;   // Karte unveraendert lassen, "Keine Treffer" im Fenster

        Platform.runLater(() -> {
            setCoLocationTableVisible(false);
            setParticipantsTableVisible(false);
            setRoamersTableVisible(false);
            setWechslerTableVisible(false);
            showQueryRows(qr);
            setLlmStatus(
                    qr.rowCount == 0
                            ? Lang.t("⚠️ Keine Rufnummern aus ", "⚠️ No phone numbers from ") + country + Lang.t(" im Zeitfenster gefunden", " found in the time window")
                            : "✅ " + qr.rowCount + " " + Lang.t("Datensatz/Datensätze aus ", "record(s) from ") + country,
                    qr.rowCount == 0 ? theme.hover : theme.accent2);
            appendDurationStatus();
            llmRunButton.setDisable(false);
        });
        showParticipants(sql, qr);
    }

    /**
     * Roamers ("Wanderer") flow: executes the fixed SQL template (built by
     * {@link RAGPipeline#buildRoamersSql}) for the optional identifier type
     * and time window the LLM extracted. Like {@link #runCallsSmsQuery}, the
     * detail rows are fed straight into {@link #setData} to redraw the
     * map/timeline — every returned row belongs to a subscriber seen at more
     * than one distinct cell site.
     *
     * <p>Additionally runs the existing, already-reviewed
     * {@link PoliceAnalysisQueries.AnalysisType#ROAMERS} aggregation (one row
     * per IMSI, with cell count and the list of visited sites) and shows it
     * in {@link #roamersTable} beneath the map, the same way
     * {@link #runColocationQuery} shows {@link #coLocationTable} — clicking a
     * row jumps the map to that subscriber's first visited site and overlays
     * their full path.</p>
     */
    private void runRoamersQuery(RAGPipeline.QueryIntent intent) throws Exception {
        Platform.runLater(() -> setLlmStatus(Lang.t("⚙️ Suche Wanderer…", "⚙️ Searching roamers…"), theme.label));

        QueryCost.Decision decision = costGate(lastLlmQuestion, List.of(RAGPipeline.buildRoamersSql(intent.startTime, intent.endTime, intent.identifierColumn), PoliceAnalysisQueries.buildSql(PoliceAnalysisQueries.AnalysisType.ROAMERS, intent.startTime, intent.endTime)), true, intent.startTime, intent.endTime);
        if (decision instanceof QueryCost.Cancel) {
            notRun();
            return;
        }
        if (decision instanceof QueryCost.Restrict r) {
            runRoamersQuery(intent.withTimeRange(r.from(), r.to()));
            return;
        }
        if (intent.allCells) {
            runAllCellsRoamers(intent);
            return;
        }

        String sql = RAGPipeline.buildRoamersSql(intent.startTime, intent.endTime, intent.identifierColumn);
        String summarySql = PoliceAnalysisQueries.buildSql(
                PoliceAnalysisQueries.AnalysisType.ROAMERS, intent.startTime, intent.endTime);

        // Database opened directly: the table below the map (Wanderer per
        // IMSI) is computed first. Its IMSIs are the result of the template's
        // subquery, so the records are then selected by membership in that
        // list instead of grouping the whole table a second time.
        List<RoamerRow> summaryRows = new ArrayList<>();
        Selection pre = directDb != null ? roamerSelection(sql, summarySql, summaryRows) : null;

        QueryRows qr = queryForDisplay(sql, pre);
        if (qr == null) {
            Platform.runLater(() -> {
                setLlmStatus(Lang.t("⚠️ Kein Ergebnis", "⚠️ No result"), theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        if (showNoHitsIfEmpty(qr, sql)) return;   // Karte unveraendert lassen, "Keine Treffer" im Fenster

        if (pre == null) {
            ResultSet summaryRs = exec(tuneTimeRanges(summarySql));
            if (summaryRs != null) {
                while (summaryRs.next()) summaryRows.add(roamerRow(summaryRs));
                summaryRs.close();
            }
        }

        Platform.runLater(() -> {
            setCoLocationTableVisible(false);
            setParticipantsTableVisible(false);
            setWechslerTableVisible(false);
            roamersTable.getItems().setAll(summaryRows);
            setRoamersTableVisible(!summaryRows.isEmpty());
            showQueryRows(qr);
            setLlmStatus(
                    qr.rowCount == 0
                            ? Lang.t("⚠️ Keine Wanderer im Zeitfenster gefunden", "⚠️ No roamers found in the time window")
                            : "✅ " + summaryRows.size() + " " + Lang.t("Wanderer", "roamers") + " (" + qr.rowCount + " " + Lang.t("Datensatz/Datensätze", "record(s)") + ")",
                    qr.rowCount == 0 ? theme.hover : theme.accent2);
            llmRunButton.setDisable(false);
        });
    }

    /**
     * Wechsler (Geräte-/SIM-Wechsel) flow: executes the fixed SQL template
     * (built by {@link RAGPipeline#buildWechslerSql}) for the optional time
     * window the LLM extracted. Like {@link #runRoamersQuery}, the detail
     * rows are fed straight into {@link #setData} to redraw the
     * map/timeline — every returned row belongs to either a subscriber
     * linked to more than one device (Geräte-Wechsel) or a device linked to
     * more than one subscriber (SIM-Wechsel).
     *
     * <p>Additionally runs the existing, already-reviewed
     * {@link PoliceAnalysisQueries.AnalysisType#DEVICE_SIM_SWAP} aggregation
     * (one row per affected IMSI/Geräte-ID, with the switch count and the
     * list of the other linked identifiers) and shows it in
     * {@link #wechslerTable} beneath the map, the same way
     * {@link #runRoamersQuery} shows {@link #roamersTable}.</p>
     */
    private void runWechslerQuery(RAGPipeline.QueryIntent intent) throws Exception {
        Platform.runLater(() -> setLlmStatus(Lang.t("⚙️ Suche Wechsler…", "⚙️ Searching swaps…"), theme.label));

        String sql = RAGPipeline.buildWechslerSql(intent.startTime, intent.endTime);

        QueryCost.Decision decision = costGate(lastLlmQuestion, List.of(sql, PoliceAnalysisQueries.buildSql(PoliceAnalysisQueries.AnalysisType.DEVICE_SIM_SWAP, intent.startTime, intent.endTime)), true, intent.startTime, intent.endTime);
        if (decision instanceof QueryCost.Cancel) {
            notRun();
            return;
        }
        if (decision instanceof QueryCost.Restrict r) {
            runWechslerQuery(intent.withTimeRange(r.from(), r.to()));
            return;
        }

        QueryRows qr = queryForDisplay(sql);
        if (qr == null) {
            Platform.runLater(() -> {
                setLlmStatus(Lang.t("⚠️ Kein Ergebnis", "⚠️ No result"), theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        if (showNoHitsIfEmpty(qr, sql)) return;   // Karte unveraendert lassen, "Keine Treffer" im Fenster

        String summarySql = PoliceAnalysisQueries.buildSql(
                PoliceAnalysisQueries.AnalysisType.DEVICE_SIM_SWAP, intent.startTime, intent.endTime);
        List<WechslerRow> summaryRows = new ArrayList<>();
        ResultSet summaryRs = exec(tuneTimeRanges(summarySql));
        if (summaryRs != null) {
            while (summaryRs.next()) {
                summaryRows.add(new WechslerRow(
                        summaryRs.getString(Lang.t("Wechseltyp", "Swap_Type")),
                        summaryRs.getString(Lang.t("Kennung", "Identifier")),
                        summaryRs.getInt(Lang.t("Anzahl_Wechsel", "Swap_Count")),
                        summaryRs.getString("Details")));
            }
            summaryRs.close();
        }

        Platform.runLater(() -> {
            setCoLocationTableVisible(false);
            setParticipantsTableVisible(false);
            setRoamersTableVisible(false);
            wechslerTable.getItems().setAll(summaryRows);
            setWechslerTableVisible(!summaryRows.isEmpty());
            showQueryRows(qr);
            setLlmStatus(
                    qr.rowCount == 0
                            ? Lang.t("⚠️ Keine Wechsler im Zeitfenster gefunden", "⚠️ No swaps found in the time window")
                            : "✅ " + summaryRows.size() + " " + Lang.t("Wechsler", "swaps") + " (" + qr.rowCount + " " + Lang.t("Datensatz/Datensätze", "record(s)") + ")",
                    qr.rowCount == 0 ? theme.hover : theme.accent2);
            llmRunButton.setDisable(false);
        });
    }

    /**
     * Converts a {@code response_records}-shaped {@link ResultSet} into the
     * {@code ConcurrentHashMap}/header format {@link #setData} expects.
     * "response_records" is used as the virtual table name so
     * {@link DataAnalyzer} recognises the schema and produces
     * {@code ResponseRecordDataPoint}s (required for cell-tower resolution
     * and for drawing markers/timeline dots).
     */
    private QueryRows toResponseRecordsTable(ResultSet rs) throws Exception {
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();

        List<String> colNames = new ArrayList<>();
        for (int i = 1; i <= cols; i++) colNames.add(meta.getColumnName(i));

        ObservableList<ObservableList<String>> rows = FXCollections.observableArrayList();
        int rowCount = 0;
        int limit = baseOverview != null ? Limits.LIGHTHOUSE_DETAIL_ROWS : Integer.MAX_VALUE;
        while (rs.next()) {
            if (rowCount >= limit)
                throw new SQLException(String.format(Lang.t(
                        "Ergebnis hat mehr als %,d Datensätze – bitte die Anfrage einschränken (z. B. Zeitraum)",
                        "Result has more than %,d records – please narrow the query (e.g. time range)"), limit));
            ObservableList<String> row = FXCollections.observableArrayList();
            for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
            rows.add(row);
            rowCount++;
        }

        final String virtualTable = "response_records";
        ConcurrentHashMap<String, ObservableList<ObservableList<String>>> result =
                new ConcurrentHashMap<>();
        result.put(virtualTable, rows);

        Map<String, List<String>> headers = new HashMap<>();
        headers.put(virtualTable, colNames);

        return new QueryRows(result, headers, rowCount);
    }

    /**
     * True, wenn eine Ergebnisspalte {@code start_time} sowie Breiten- und
     * Laengengrad enthaelt — dieselbe Bedingung, unter der
     * {@link DataAnalyzer} die Zeilen als response_records-Punkte auf Karte und
     * Zeitleiste abbildet. Sonst (z. B. {@code COUNT(*)}, Kontaktlisten,
     * Summen) wird das Ergebnis in einem {@link ResultPopup} gezeigt.
     */
    private static boolean isMapDisplayable(List<String> cols) {
        if (cols == null || cols.isEmpty()) return false;
        Set<String> lower = new HashSet<>();
        for (String c : cols) if (c != null) lower.add(c.toLowerCase(Locale.ROOT));
        boolean ts  = lower.contains("start_time");
        boolean lat = lower.contains("latitude_dec") || lower.contains("latitude");
        boolean lon = lower.contains("longitude_dec") || lower.contains("longitude");
        return ts && lat && lon;
    }

    /** Zuletzt im LLM-Feld gestellte Frage (fuer das Ergebnisfenster). */
    private volatile String lastLlmQuestion = "";

    /**
     * Zeigt bei 0 Treffern ein "Keine Treffer"-Fenster und laesst Karte und
     * Zeitleiste unveraendert (statt sie zu leeren).
     *
     * @return {@code true}, wenn es keine Treffer gab und der Aufrufer abbrechen soll
     */
    private boolean showNoHitsIfEmpty(QueryRows qr, String sql) {
        if (qr.rowCount > 0) return false;
        List<String> cols = qr.headers.getOrDefault("response_records", List.of());
        Platform.runLater(() -> {
            ResultPopup.show(getScene() != null ? getScene().getWindow() : null, theme,
                    lastLlmQuestion, sql, cols, List.of());
            setLlmStatus(Lang.t("⚠️ Keine Treffer", "⚠️ No matches"), theme.hover);
            llmRunButton.setDisable(false);
        });
        return true;
    }

    /** Small holder returned by {@link #toResponseRecordsTable}. */
    private static final class QueryRows {
        final ConcurrentHashMap<String, ObservableList<ObservableList<String>>> data;
        final Map<String, List<String>> headers;
        final int rowCount;
        /** read directly from the file (see queryForDisplay), else null */
        DisplayResult direct;
        /** condition of a direct result */
        String cond;

        QueryRows(ConcurrentHashMap<String, ObservableList<ObservableList<String>>> data,
                  Map<String, List<String>> headers, int rowCount) {
            this.data = data;
            this.headers = headers;
            this.rowCount = rowCount;
        }
    }

    /**
     * Co-location flow: executes the fixed self-join SQL template (built by
     * {@link RAGPipeline#buildColocationSql}) for the time window the LLM
     * extracted, groups the resulting IMSI pairs in Java by shared site
     * coordinate, and shows the groups in {@link #coLocationTable} instead
     * of redrawing the map via {@link #setData}.
     */
    private void runColocationQuery(RAGPipeline.QueryIntent intent) throws Exception {
        Platform.runLater(() -> setLlmStatus(Lang.t("⚙️ Suche gemeinsame Funkzellen…", "⚙️ Searching shared cell sites…"), theme.label));

        String sql = RAGPipeline.buildColocationSql(intent.startTime, intent.endTime, intent.identifierColumn);
        if (sql == null) {
            Platform.runLater(() -> {
                setLlmStatus(Lang.t("⚠️ Zeitraum konnte nicht erkannt werden", "⚠️ Could not recognize the time range"), theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        QueryCost.Decision decision = costGate(lastLlmQuestion, List.of(sql), true, intent.startTime, intent.endTime);
        if (decision instanceof QueryCost.Cancel) {
            notRun();
            return;
        }
        if (decision instanceof QueryCost.Restrict r) {
            runColocationQuery(intent.withTimeRange(r.from(), r.to()));
            return;
        }

        ResultSet rs = exec(sql);
        if (rs == null) {
            Platform.runLater(() -> {
                setLlmStatus(Lang.t("⚠️ Kein Ergebnis", "⚠️ No result"), theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        Map<String, CoLocationGroup> groups = new LinkedHashMap<>();
        while (rs.next()) {
            String idA    = rs.getString("id_a");
            String idB    = rs.getString("id_b");
            double lat    = rs.getDouble("lat");
            double lon    = rs.getDouble("lon");
            Integer mcc    = getNullableInt(rs, "mcc");
            Integer mnc    = getNullableInt(rs, "mnc");
            Integer lacTac = getNullableInt(rs, "lac_tac");
            Integer ci     = getNullableInt(rs, "ci");
            String startA = rs.getString("start_a");
            String startB = rs.getString("start_b");

            // Group by the decoded cell identity (MCC/MNC/LAC-TAC/CI) when
            // available — this is what RAGPipeline#sameCellExpr actually
            // matched on, and is more precise than the raw coordinate (e.g.
            // multi-sector sites share one rounded lat/lon across several
            // distinct cells). Falls back to the coordinate for rows where
            // cell identity couldn't be decoded (older DBs, undecodable ULI).
            boolean hasCellId = mcc != null && mnc != null && lacTac != null && ci != null;
            String key = hasCellId
                    ? "cell:" + mcc + "-" + mnc + "-" + lacTac + "-" + ci
                    : "geo:" + lat + "," + lon;
            CoLocationGroup g = groups.computeIfAbsent(key, k ->
                    new CoLocationGroup(lat, lon, hasCellId ? mcc : null, hasCellId ? mnc : null,
                            hasCellId ? lacTac : null, hasCellId ? ci : null));
            g.ids.add(idA);
            g.ids.add(idB);
            g.expand(startA);
            g.expand(startB);
        }
        rs.close();

        List<CoLocationGroup> result = new ArrayList<>(groups.values());
        result.removeIf(g -> g.ids.size() < 2);

        // Also fetch the individual matching response_records rows so the
        // matched identifiers/sites show up as markers on the map and as
        // dots on the timeline — the table alone doesn't trigger a map redraw.
        QueryRows detailRows = null;
        if (!result.isEmpty()) {
            String detailSql = RAGPipeline.buildColocationDetailSql(intent.startTime, intent.endTime, intent.identifierColumn);
            if (detailSql != null) {
                ResultSet detailRs = exec(detailSql);
                if (detailRs != null) {
                    detailRows = toResponseRecordsTable(detailRs);
                    detailRs.close();
                }
            }
        }
        final QueryRows finalDetailRows = detailRows;

        Platform.runLater(() -> {
            colCoLocationCount.setText(Lang.t("Anzahl " + intent.identifierLabel + "s", intent.identifierLabel + " Count"));
            colCoLocationIds.setText(intent.identifierLabel + "s");
            coLocationTable.getItems().setAll(result);
            setCoLocationTableVisible(!result.isEmpty());
            setParticipantsTableVisible(false);
            setRoamersTableVisible(false);
            setWechslerTableVisible(false);
            if (finalDetailRows != null) {
                setData(finalDetailRows.data, finalDetailRows.headers);
            }
            if (result.isEmpty()) {
                ResultPopup.show(getScene() != null ? getScene().getWindow() : null, theme,
                        lastLlmQuestion, sql, List.of(), List.of());
            }
            setLlmStatus(
                    result.isEmpty()
                            ? Lang.t("⚠️ Keine gemeinsamen Funkzellen im Zeitfenster gefunden (", "⚠️ No shared cell sites found in the time window (") + intent.identifierLabel + ")"
                            : "✅ " + result.size() + " " + Lang.t("Standort(e) mit gemeinsamer Anmeldung (", "location(s) with shared registration (") + intent.identifierLabel + ")",
                    result.isEmpty() ? theme.hover : theme.accent2);
            llmRunButton.setDisable(false);
        });
    }

    /**
     * Restores the full base dataset (before any LLM filter was applied).
     */
    private void onLlmReset() {
        participantsGen++;
        setCoLocationTableVisible(false);
        setParticipantsTableVisible(false);
        setRoamersTableVisible(false);
        setWechslerTableVisible(false);
        if (baseOverview != null) {
            timelineView.clearFilterSilently();
            timelineCond = null;
            resultCond = null;
            shownCond = null;
            ++filterGeneration;
            timelineView.setExternalFilter(this::onTimelineFilter);
            applyOverview(baseOverview, true);
            releaseSelections();
            setLlmStatus(Lang.t("↺ Alle Daten wiederhergestellt", "↺ All data restored"), theme.label);
        } else if (basePoints != null) {
            applyPoints(basePoints, true);
            setLlmStatus(Lang.t("↺ Alle Daten wiederhergestellt", "↺ All data restored"), theme.label);
        } else if (baseResultList != null) {
            setData(baseResultList, baseHeaders != null ? baseHeaders : Map.of(), true);
            setLlmStatus(Lang.t("↺ Alle Daten wiederhergestellt", "↺ All data restored"), theme.label);
        }
    }

    private void setLlmStatus(String text, Color color) {
        llmStatusLabel.setText(text);
        llmStatusLabel.setTextFill(color);
    }

    private static String abbreviate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max - 1) + "…";
    }

    // =========================================================================
    // Co-location results table
    // =========================================================================

    /** Shows/hides the co-location results table beneath the LLM bar. */
    private void setCoLocationTableVisible(boolean visible) {
        coLocationTable.setManaged(visible);
        coLocationTable.setVisible(visible);
        if (visible) coLocationTable.setPrefHeight(180);
    }

    /** Builds the (initially empty/hidden) co-location results table. */
    private TableView<CoLocationGroup> buildCoLocationTable() {
        TableView<CoLocationGroup> table = new TableView<>();
        Label coLocationPlaceholder = new Label(Lang.t("Keine gemeinsamen Funkzellen gefunden.", "No shared cell sites found."));
        table.setPlaceholder(coLocationPlaceholder);
        Lang.addListener(() -> coLocationPlaceholder.setText(Lang.t("Keine gemeinsamen Funkzellen gefunden.", "No shared cell sites found.")));

        TableColumn<CoLocationGroup, String> colPos = new TableColumn<>(Lang.t("Standort", "Location"));
        colPos.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().position()));
        colPos.setPrefWidth(150);

        TableColumn<CoLocationGroup, String> colCell = new TableColumn<>(Lang.t("Funkzelle (MCC-MNC-LAC/TAC-CI)", "Cell Site (MCC-MNC-LAC/TAC-CI)"));
        colCell.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().cellLabel()));
        colCell.setPrefWidth(190);

        TableColumn<CoLocationGroup, String> colFrom = new TableColumn<>(Lang.t("Von", "From"));
        colFrom.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().earliest));
        colFrom.setPrefWidth(150);

        TableColumn<CoLocationGroup, String> colTo = new TableColumn<>(Lang.t("Bis", "To"));
        colTo.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().latest));
        colTo.setPrefWidth(150);

        colCoLocationCount = new TableColumn<>(Lang.t("Anzahl IMSIs", "IMSI Count"));
        colCoLocationCount.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().ids.size()));
        colCoLocationCount.setPrefWidth(100);

        colCoLocationIds = new TableColumn<>("IMSIs");
        colCoLocationIds.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().idList()));

        table.getColumns().addAll(List.of(colPos, colCell, colFrom, colTo, colCoLocationCount, colCoLocationIds));
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
        Lang.addListener(() -> {
            colPos.setText(Lang.t("Standort", "Location"));
            colCell.setText(Lang.t("Funkzelle (MCC-MNC-LAC/TAC-CI)", "Cell Site (MCC-MNC-LAC/TAC-CI)"));
            colFrom.setText(Lang.t("Von", "From"));
            colTo.setText(Lang.t("Bis", "To"));
        });

        table.setRowFactory(tv -> {
            TableRow<CoLocationGroup> row = new TableRow<>();
            row.setOnMouseClicked(e -> {
                if (row.getItem() != null) mapView.centerOnCoordinate(row.getItem().lat, row.getItem().lon);
            });
            return row;
        });

        // Copy selected cells to clipboard via context menu or Ctrl/Cmd+C.
        TableClipboardSupport.install(table, "lighthouse_gleiche_funkzelle.csv");

        return table;
    }

    /** One physical site where ≥2 distinct identifiers (IMSI/MSISDN/IMEI) were registered within the queried time window. */
    private static final class CoLocationGroup {
        final double lat, lon;
        /** Decoded cell identity (see {@link UliDecoder}); {@code null} if unavailable (older DB, undecodable ULI). */
        final Integer mcc, mnc, lacTac, ci;
        final Set<String> ids = new TreeSet<>();
        String earliest;
        String latest;

        CoLocationGroup(double lat, double lon, Integer mcc, Integer mnc, Integer lacTac, Integer ci) {
            this.lat = lat;
            this.lon = lon;
            this.mcc = mcc;
            this.mnc = mnc;
            this.lacTac = lacTac;
            this.ci = ci;
        }

        void expand(String timestamp) {
            if (timestamp == null) return;
            if (earliest == null || timestamp.compareTo(earliest) < 0) earliest = timestamp;
            if (latest == null || timestamp.compareTo(latest) > 0) latest = timestamp;
        }

        String idList() { return String.join(", ", ids); }

        String position() { return String.format("%.5f, %.5f", lat, lon); }

        /** "262-2-LAC 1234-CI 56", or "–" when cell identity couldn't be decoded for this site. */
        String cellLabel() {
            if (mcc == null || mnc == null || lacTac == null || ci == null) return "–";
            return mcc + "-" + mnc + "-LAC/TAC " + lacTac + "-CI " + ci;
        }
    }

    /** Reads an {@code INTEGER} column as {@code null} (not {@code 0}) when the value is SQL NULL. */
    private static Integer getNullableInt(ResultSet rs, String columnLabel) throws SQLException {
        int v = rs.getInt(columnLabel);
        return rs.wasNull() ? null : v;
    }

    // =========================================================================
    // Statistics result in map and timeline
    // =========================================================================

    /**
     * Shows the records of the identifiers named by a result of the
     * Statistics tab (e.g. every record of the Wanderer found) in map and
     * timeline: records in the analysis' time range whose phone number, IMSI
     * or IMEI is one of them. On a database opened directly they are selected
     * in one pass over the file (membership checked in memory, see
     * {@link Selection#create(java.sql.Connection, String, String, Selection.RowFilter)}).
     * Runs in the background; the cancel button stops it.
     */
    private void showStatisticsResult(PoliceAnalysisPane.ResultIds ids) {
        participantsGen++;
        llmRunButton.setDisable(true);
        setLlmStatus(String.format(Lang.t("⏳ Statistik \"%s\": Datensätze von %,d Kennungen werden geladen …",
                "⏳ Statistics \"%s\": loading the records of %,d identifiers …"), ids.label, ids.size()), theme.label);
        Thread worker = new Thread(() -> {
            beginOperation();
            try {
                loadStatisticsResult(ids);
            } catch (Exception ex) {
                boolean stop = wasCancelled(ex);
                if (!stop) LOG.log(Level.WARNING, "Statistics result could not be shown on the map", ex);
                Platform.runLater(() -> {
                    setLlmStatus(stop ? Lang.t("⏹ Abgebrochen – die bisherige Anzeige bleibt", "⏹ Cancelled – the previous view stays")
                            : "❌ " + ex.getMessage(), stop ? theme.label : theme.hover);
                    llmRunButton.setDisable(false);
                });
            } finally {
                boolean stop = cancelRequested;
                endOperation();
                if (stop) Platform.runLater(() -> {
                    setLlmStatus(Lang.t("⏹ Abgebrochen – die bisherige Anzeige bleibt", "⏹ Cancelled – the previous view stays"), theme.label);
                    llmRunButton.setDisable(false);
                });
            }
        }, "statistics-map-worker");
        worker.setDaemon(true);
        worker.start();
    }

    private void loadStatisticsResult(PoliceAnalysisPane.ResultIds ids) throws Exception {
        String time = ids.fromIso != null && !ids.fromIso.isBlank() && ids.toIso != null && !ids.toIso.isBlank()
                ? " AND start_time BETWEEN '" + ids.fromIso.replace("'", "''") + "' AND '" + ids.toIso.replace("'", "''") + "'"
                : "";
        String base = "latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL" + time;
        QueryRows qr;
        if (directDb != null) {
            String sql = "SELECT * FROM response_records WHERE " + base;
            String outer = simpleCondition(tuneTimeRanges(sql));
            Selection sel;
            try (java.sql.Connection c = source().open()) {
                sel = Selection.create(c, outer, "msisdn, imsi, na_device_id", r -> {
                    String m = r.getString(2), i = r.getString(3), e = r.getString(4);
                    return m != null && ids.msisdn.contains(m) || i != null && ids.imsi.contains(i) || e != null && ids.imei.contains(e);
                });
            }
            checkCancelled();
            qr = queryForDisplay(sql, sel);
        } else {
            // database analysed in memory: small enough for literal lists
            String sql = "SELECT * FROM response_records WHERE " + base + " AND ("
                    + inList("msisdn", ids.msisdn) + " OR " + inList("imsi", ids.imsi) + " OR " + inList("na_device_id", ids.imei)
                    + ") ORDER BY start_time";
            qr = queryForDisplay(sql);
        }
        if (qr == null) {
            Platform.runLater(() -> {
                setLlmStatus(Lang.t("⚠️ Kein Ergebnis", "⚠️ No result"), theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }
        Platform.runLater(() -> {
            setCoLocationTableVisible(false);
            setParticipantsTableVisible(false);
            setRoamersTableVisible(false);
            setWechslerTableVisible(false);
            if (qr.rowCount > 0) showQueryRows(qr);
            else releaseSelections();
            setLlmStatus(qr.rowCount == 0
                            ? String.format(Lang.t("⚠️ Statistik \"%s\": keine Datensätze mit Koordinaten – die Karte bleibt unverändert",
                                    "⚠️ Statistics \"%s\": no records with coordinates – the map stays unchanged"), ids.label)
                            : String.format(Lang.t("📊 Statistik \"%s\": %,d Datensätze von %,d Kennungen",
                                    "📊 Statistics \"%s\": %,d records of %,d identifiers"), ids.label, (long) qr.rowCount, ids.size()),
                    qr.rowCount == 0 ? theme.hover : theme.accent2);
            llmRunButton.setDisable(false);
        });
    }

    /** {@code column IN ('a', 'b', ...)}, or {@code 0} for no values */
    private static String inList(String column, java.util.Set<String> values) {
        if (values.isEmpty()) return "0";
        StringBuilder sb = new StringBuilder(column).append(" IN (");
        boolean first = true;
        for (String v : values) {
            if (!first) sb.append(',');
            first = false;
            sb.append('\'').append(v.replace("'", "''")).append('\'');
        }
        return sb.append(')').toString();
    }

    // =========================================================================
    // Participants list (filter queries of the assistant)
    // =========================================================================

    /**
     * SQL of the participants list for the records of {@code sql}: one row per
     * IMSI with phone numbers, devices, number of records and first/last
     * record; {@code null} if {@code sql} is not a plain
     * {@code SELECT * FROM response_records WHERE ...} query.
     */
    static String participantsSql(String sql) {
        String cond = simpleCondition(sql);
        if (cond == null) return null;
        String where = cond.isEmpty() ? "" : "(" + cond + ") AND ";
        return "SELECT imsi, GROUP_CONCAT(DISTINCT msisdn), GROUP_CONCAT(DISTINCT na_device_id),"
                + " COUNT(*), MIN(start_time), MAX(start_time)"
                + " FROM response_records WHERE " + where + "imsi IS NOT NULL AND imsi <> ''"
                + " GROUP BY +imsi ORDER BY COUNT(*) DESC, imsi";
    }

    /** the participants list from records already in memory, sorted as {@link #participantsSql} */
    private static List<ParticipantRow> participantsOf(List<DataPoint> points) {
        final class Acc {
            final java.util.LinkedHashSet<String> msisdns = new java.util.LinkedHashSet<>(), imeis = new java.util.LinkedHashSet<>();
            long n;
            java.time.Instant first, last;
        }
        Map<String, Acc> by = new HashMap<>();
        for (DataPoint dp : points) {
            String imsi = dp.getImsi();
            if (imsi == null || imsi.isEmpty()) continue;
            Acc a = by.computeIfAbsent(imsi, k -> new Acc());
            a.n++;
            if (dp.getMsisdn() != null) a.msisdns.add(dp.getMsisdn());
            if (dp.getImei() != null) a.imeis.add(dp.getImei());
            java.time.Instant t = dp.getTimestamp();
            if (t != null) {
                if (a.first == null || t.isBefore(a.first)) a.first = t;
                if (a.last == null || t.isAfter(a.last)) a.last = t;
            }
        }
        java.time.format.DateTimeFormatter f = java.time.format.DateTimeFormatter
                .ofPattern("yyyy-MM-dd HH:mm:ss").withZone(java.time.ZoneOffset.UTC);
        List<ParticipantRow> rows = new ArrayList<>(by.size());
        for (Map.Entry<String, Acc> e : by.entrySet()) {
            Acc a = e.getValue();
            rows.add(new ParticipantRow(e.getKey(), String.join(",", a.msisdns), String.join(",", a.imeis), a.n,
                    a.first == null ? null : f.format(a.first), a.last == null ? null : f.format(a.last)));
        }
        rows.sort((x, y) -> x.records != y.records ? Long.compare(y.records, x.records) : x.imsi.compareTo(y.imsi));
        return rows;
    }

    /**
     * Computes the participants list of a filter query after its records are
     * shown and puts it below the map: from the loaded records if the result
     * is in memory, else by a query (overview of a large result, or a
     * database analysed in memory). Runs on the worker of the query (the
     * cancel button stays active meanwhile); a list that arrives after a new
     * query was started is dropped.
     */
    private void showParticipants(String sql, QueryRows qr) throws Exception {
        int gen = participantsGen;
        String psql = participantsSql(tuneTimeRanges(sql));
        if (psql == null) return;
        List<ParticipantRow> rows;
        if (qr != null && qr.direct != null && qr.direct.points != null) {
            rows = participantsOf(qr.direct.points);
        } else {
            Platform.runLater(() -> {
                if (gen == participantsGen)
                    setLlmStatus(llmStatusLabel.getText() + Lang.t("  |  ⏳ Teilnehmerliste …", "  |  ⏳ Participants list …"), theme.accent2);
            });
            rows = new ArrayList<>();
            ResultSet rs = exec(psql);
            if (rs == null) {
                Platform.runLater(() -> {
                    if (gen != participantsGen) return;
                    String t = llmStatusLabel.getText();
                    int at = t.lastIndexOf("  |  ⏳");
                    setLlmStatus((at >= 0 ? t.substring(0, at) : t)
                            + Lang.t("  |  Teilnehmerliste abgebrochen", "  |  participants list cancelled"), theme.accent2);
                });
                return;
            }
            try {
                while (rs.next()) {
                    rows.add(new ParticipantRow(rs.getString(1), rs.getString(2), rs.getString(3),
                            rs.getLong(4), rs.getString(5), rs.getString(6)));
                }
            } finally {
                rs.close();
            }
        }
        final List<ParticipantRow> result = rows;
        Platform.runLater(() -> {
            if (gen != participantsGen) return;
            participantsTable.getItems().setAll(result);
            setParticipantsTableVisible(!result.isEmpty());
            String t = llmStatusLabel.getText();
            int at = t.lastIndexOf("  |  ⏳");
            setLlmStatus((at >= 0 ? t.substring(0, at) : t) + "  |  " + String.format("%,d", result.size())
                    + Lang.t(" Teilnehmer (IMSI) – Liste unten", " participants (IMSI) – list below"), theme.accent2);
        });
    }

    /** Shows/hides the participants table beneath the LLM bar. */
    private void setParticipantsTableVisible(boolean visible) {
        participantsTable.setManaged(visible);
        participantsTable.setVisible(visible);
        if (visible) participantsTable.setPrefHeight(220);
        else participantsTable.getItems().clear();
    }

    /** Builds the (initially empty/hidden) participants table. */
    private TableView<ParticipantRow> buildParticipantsTable() {
        TableView<ParticipantRow> table = new TableView<>();
        Label placeholder = new Label(Lang.t("Keine Teilnehmer.", "No participants."));
        table.setPlaceholder(placeholder);

        TableColumn<ParticipantRow, String> colImsi = new TableColumn<>("IMSI");
        colImsi.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().imsi));
        colImsi.setPrefWidth(160);

        TableColumn<ParticipantRow, String> colMsisdn = new TableColumn<>(Lang.t("Rufnummer(n)", "Phone number(s)"));
        colMsisdn.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().msisdns));
        colMsisdn.setPrefWidth(160);

        TableColumn<ParticipantRow, String> colImei = new TableColumn<>(Lang.t("Gerät(e) (IMEI)", "Device(s) (IMEI)"));
        colImei.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().imeis));
        colImei.setPrefWidth(180);

        TableColumn<ParticipantRow, Number> colCount = new TableColumn<>(Lang.t("Datensätze", "Records"));
        colCount.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().records));
        colCount.setPrefWidth(90);

        TableColumn<ParticipantRow, String> colFrom = new TableColumn<>(Lang.t("Erster Datensatz", "First record"));
        colFrom.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().first));
        colFrom.setPrefWidth(150);

        TableColumn<ParticipantRow, String> colTo = new TableColumn<>(Lang.t("Letzter Datensatz", "Last record"));
        colTo.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().last));
        colTo.setPrefWidth(150);

        table.getColumns().addAll(List.of(colImsi, colMsisdn, colImei, colCount, colFrom, colTo));
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
        Lang.addListener(() -> {
            placeholder.setText(Lang.t("Keine Teilnehmer.", "No participants."));
            colMsisdn.setText(Lang.t("Rufnummer(n)", "Phone number(s)"));
            colImei.setText(Lang.t("Gerät(e) (IMEI)", "Device(s) (IMEI)"));
            colCount.setText(Lang.t("Datensätze", "Records"));
            colFrom.setText(Lang.t("Erster Datensatz", "First record"));
            colTo.setText(Lang.t("Letzter Datensatz", "Last record"));
        });

        // click: path of the IMSI on the map (as in the Wanderer table)
        table.setRowFactory(tv -> {
            TableRow<ParticipantRow> row = new TableRow<>();
            row.setOnMouseClicked(e -> {
                ParticipantRow r = row.getItem();
                if (r != null && r.imsi != null && !r.imsi.isBlank()) mapView.selectImsiForPath(r.imsi);
            });
            return row;
        });

        // Copy selected cells to clipboard via context menu or Ctrl/Cmd+C.
        TableClipboardSupport.install(table, "lighthouse_teilnehmer.csv");
        return table;
    }

    /** one row of the participants list */
    private static final class ParticipantRow {
        final String imsi, msisdns, imeis, first, last;
        final long records;

        ParticipantRow(String imsi, String msisdns, String imeis, long records, String first, String last) {
            this.imsi = imsi;
            this.msisdns = msisdns == null ? "" : msisdns.replace(",", ", ");
            this.imeis = imeis == null ? "" : imeis.replace(",", ", ");
            this.records = records;
            this.first = first;
            this.last = last;
        }
    }

    // =========================================================================
    // Roamers ("Wanderer") results table
    // =========================================================================

    /** Shows/hides the roamers results table beneath the LLM bar. */
    private void setRoamersTableVisible(boolean visible) {
        roamersTable.setManaged(visible);
        roamersTable.setVisible(visible);
        if (visible) roamersTable.setPrefHeight(180);
    }

    /** Builds the (initially empty/hidden) roamers results table. */
    private TableView<RoamerRow> buildRoamersTable() {
        TableView<RoamerRow> table = new TableView<>();
        Label roamersPlaceholder = new Label(Lang.t("Keine Wanderer gefunden.", "No roamers found."));
        table.setPlaceholder(roamersPlaceholder);
        Lang.addListener(() -> roamersPlaceholder.setText(Lang.t("Keine Wanderer gefunden.", "No roamers found.")));

        TableColumn<RoamerRow, String> colImsi = new TableColumn<>("IMSI");
        colImsi.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().imsi));
        colImsi.setPrefWidth(160);

        TableColumn<RoamerRow, String> colMsisdn = new TableColumn<>(Lang.t("Rufnummer", "Phone_Number"));
        colMsisdn.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().msisdn));
        colMsisdn.setPrefWidth(130);

        TableColumn<RoamerRow, Number> colCount = new TableColumn<>(Lang.t("Anzahl Funkzellen", "Cell Site Count"));
        colCount.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().cellCount));
        colCount.setPrefWidth(120);

        TableColumn<RoamerRow, String> colFrom = new TableColumn<>(Lang.t("Von", "From"));
        colFrom.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().earliest));
        colFrom.setPrefWidth(150);

        TableColumn<RoamerRow, String> colTo = new TableColumn<>(Lang.t("Bis", "To"));
        colTo.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().latest));
        colTo.setPrefWidth(150);

        TableColumn<RoamerRow, String> colSites = new TableColumn<>(Lang.t("Funkzellen", "Cell Sites"));
        colSites.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().sitesRaw));

        table.getColumns().addAll(List.of(colImsi, colMsisdn, colCount, colFrom, colTo, colSites));
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
        Lang.addListener(() -> {
            colMsisdn.setText(Lang.t("Rufnummer", "Phone_Number"));
            colCount.setText(Lang.t("Anzahl Funkzellen", "Cell Site Count"));
            colFrom.setText(Lang.t("Von", "From"));
            colTo.setText(Lang.t("Bis", "To"));
            colSites.setText(Lang.t("Funkzellen", "Cell Sites"));
        });

        table.setRowFactory(tv -> {
            TableRow<RoamerRow> row = new TableRow<>();
            row.setOnMouseClicked(e -> {
                RoamerRow r = row.getItem();
                if (r == null) return;
                if (r.firstLat != null && r.firstLon != null) {
                    mapView.centerOnCoordinate(r.firstLat, r.firstLon);
                }
                if (r.imsi != null && !r.imsi.isBlank()) {
                    mapView.selectImsiForPath(r.imsi);
                }
            });
            return row;
        });

        // Copy selected cells to clipboard via context menu or Ctrl/Cmd+C.
        TableClipboardSupport.install(table, "lighthouse_wanderer.csv");

        return table;
    }

    /**
     * One row of the {@link PoliceAnalysisQueries.AnalysisType#ROAMERS}
     * aggregation: a subscriber (IMSI) seen at more than one distinct cell
     * site within the queried time window. {@code sitesRaw} is the
     * {@code "<lat>|<lon>,<lat>|<lon>,..."} list produced by that query's
     * {@code GROUP_CONCAT} — here we only need the first site to
     * center the map on a row click.
     */
    /**
     * "Wanderer in allen Funkzellen": the IMSIs seen at every cell site of the
     * time range. The Wanderer table is computed first (sorted by the number
     * of cell sites); the IMSIs with the highest number are shown with their
     * records. If none reached every cell site, those with the most cell sites
     * are shown and the status line says so.
     */
    private void runAllCellsRoamers(RAGPipeline.QueryIntent intent) throws Exception {
        String time = PoliceAnalysisQueries.timeFilterClause(intent.startTime, intent.endTime, "");
        String summarySql = PoliceAnalysisQueries.buildSql(
                PoliceAnalysisQueries.AnalysisType.ROAMERS, intent.startTime, intent.endTime);
        List<RoamerRow> rows = new ArrayList<>();
        ResultSet rs = exec(tuneTimeRanges(summarySql));
        if (rs != null) {
            try {
                while (rs.next()) rows.add(roamerRow(rs));
            } finally {
                rs.close();
            }
        }
        long sites = 0;
        ResultSet cs = exec(tuneTimeRanges("SELECT COUNT(DISTINCT latitude_dec || ',' || longitude_dec) "
                + "FROM response_records WHERE latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL" + time));
        if (cs != null) {
            try {
                if (cs.next()) sites = cs.getLong(1);
            } finally {
                cs.close();
            }
        }
        if (rows.isEmpty()) {
            Platform.runLater(() -> {
                setLlmStatus(Lang.t("⚠️ Keine Wanderer im Zeitfenster gefunden", "⚠️ No roamers found in the time window"), theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }
        int max = 0;
        for (RoamerRow r : rows) max = Math.max(max, r.cellCount);
        List<RoamerRow> top = new ArrayList<>();
        StringBuilder in = new StringBuilder();
        for (RoamerRow r : rows) {
            if (r.cellCount != max) continue;
            top.add(r);
            if (in.length() > 0) in.append(',');
            in.append('\'').append(r.imsi.replace("'", "''")).append('\'');
        }
        String sql = "SELECT * FROM response_records WHERE imsi IN (" + in + ") "
                + "AND latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL" + time + " ORDER BY imsi, start_time";
        QueryRows qr = queryForDisplay(sql);
        if (qr == null) {
            Platform.runLater(() -> {
                setLlmStatus(Lang.t("⚠️ Kein Ergebnis", "⚠️ No result"), theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }
        final int best = max;
        final long allSites = sites;
        Platform.runLater(() -> {
            setCoLocationTableVisible(false);
            setParticipantsTableVisible(false);
            setWechslerTableVisible(false);
            roamersTable.getItems().setAll(top);
            setRoamersTableVisible(true);
            showQueryRows(qr);
            setLlmStatus(best >= allSites
                    ? String.format(Lang.t("✅ %,d Wanderer in allen %,d Funkzellen (%,d Datensätze)",
                            "✅ %,d roamer(s) in all %,d cell sites (%,d records)"), top.size(), allSites, qr.rowCount)
                    : String.format(Lang.t("⚠️ Kein Wanderer war in allen %,d Funkzellen – angezeigt: %,d Wanderer mit den meisten (%,d) Funkzellen",
                            "⚠️ No roamer was in all %,d cell sites – shown: %,d roamer(s) with the most (%,d) cell sites"),
                            allSites, top.size(), best),
                    best >= allSites ? theme.accent2 : theme.hover);
            llmRunButton.setDisable(false);
        });
    }

    private static RoamerRow roamerRow(ResultSet rs) throws SQLException {
        return new RoamerRow(
                rs.getString("IMSI"),
                rs.getString(Lang.t("Rufnummer", "Phone_Number")),
                rs.getInt(Lang.t("Anzahl_Funkzellen", "Cell_Site_Count")),
                rs.getString(Lang.t("Funkzellen", "Cell_Sites")),
                rs.getString(Lang.t("Von", "From_Time")),
                rs.getString(Lang.t("Bis", "To_Time")));
    }

    /**
     * Runs the Wanderer summary into {@code rows} and selects the records of
     * the Wanderer query by membership in its IMSIs (one pass, no second
     * grouping). Returns {@code null} - nothing done - if the query is not the
     * IMSI template or both do not cover the same time range.
     */
    private Selection roamerSelection(String sql, String summarySql, List<RoamerRow> rows) throws SQLException {
        String marker = "AND +imsi IN (";
        int at = sql.indexOf(marker);
        if (at < 0 || sql.contains("start_time BETWEEN") != summarySql.contains("start_time BETWEEN")) return null;
        int depth = 0, close = -1;
        for (int i = at + marker.length() - 1; i < sql.length(); i++) {
            char ch = sql.charAt(i);
            if (ch == '(') depth++;
            else if (ch == ')' && --depth == 0) { close = i; break; }
        }
        if (close < 0) return null;
        String outer = simpleCondition(tuneTimeRanges(sql.substring(0, at) + sql.substring(close + 1)));
        if (outer == null || outer.isEmpty()) return null;
        java.util.regex.Matcher tm = java.util.regex.Pattern.compile("start_time BETWEEN '[^']*' AND '[^']*'").matcher(sql);
        String time = tm.find() ? " AND " + tm.group() : "";

        List<RoamerRow> found = new ArrayList<>();
        Set<String> imsis = new HashSet<>();
        ResultSet rs = exec(tuneTimeRanges(summarySql));
        if (rs == null) return null;
        try {
            while (rs.next()) {
                RoamerRow r = roamerRow(rs);
                found.add(r);
                imsis.add(r.imsi);
            }
        } finally {
            rs.close();
        }
        // The template only requires imsi <> '', the summary TRIM(imsi) <> '':
        // add Wanderer whose IMSI consists of spaces only (small index range).
        ResultSet sp = exec("SELECT imsi FROM response_records WHERE imsi >= ' ' AND imsi < '!' "
                + "AND TRIM(imsi) = '' AND latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL" + time
                + " GROUP BY imsi HAVING COUNT(DISTINCT latitude_dec || ',' || longitude_dec) > 1");
        if (sp == null) return null;
        try {
            while (sp.next()) imsis.add(sp.getString(1));
        } finally {
            sp.close();
        }
        Selection sel;
        try (java.sql.Connection c = source().open()) {
            sel = Selection.create(c, outer, "imsi", r -> imsis.contains(r.getString(2)));
        }
        rows.addAll(found);
        return sel;
    }

    private static final class RoamerRow {
        final String imsi;
        final String msisdn;
        final int cellCount;
        final String sitesRaw;
        final String earliest;
        final String latest;
        final Double firstLat;
        final Double firstLon;

        RoamerRow(String imsi, String msisdn, int cellCount, String sitesRaw, String earliest, String latest) {
            this.imsi = imsi;
            this.msisdn = msisdn;
            this.cellCount = cellCount;
            this.sitesRaw = sitesRaw;
            this.earliest = earliest;
            this.latest = latest;

            Double lat = null, lon = null;
            if (sitesRaw != null && !sitesRaw.isBlank()) {
                String firstSite = sitesRaw.split(",")[0];
                String[] parts = firstSite.split("\\|");
                if (parts.length == 2) {
                    try {
                        lat = Double.parseDouble(parts[0].trim());
                        lon = Double.parseDouble(parts[1].trim());
                    } catch (NumberFormatException ignored) {
                        // leave lat/lon null — row click just won't re-center the map
                    }
                }
            }
            this.firstLat = lat;
            this.firstLon = lon;
        }
    }

    // =========================================================================
    // Wechsler (Geräte-/SIM-Wechsel) results table
    // =========================================================================

    /** Shows/hides the Wechsler results table beneath the LLM bar. */
    private void setWechslerTableVisible(boolean visible) {
        wechslerTable.setManaged(visible);
        wechslerTable.setVisible(visible);
        if (visible) wechslerTable.setPrefHeight(180);
    }

    /** Builds the (initially empty/hidden) Wechsler results table. */
    private TableView<WechslerRow> buildWechslerTable() {
        TableView<WechslerRow> table = new TableView<>();
        Label wechslerPlaceholder = new Label(Lang.t("Keine Wechsler gefunden.", "No swaps found."));
        table.setPlaceholder(wechslerPlaceholder);
        Lang.addListener(() -> wechslerPlaceholder.setText(Lang.t("Keine Wechsler gefunden.", "No swaps found.")));

        TableColumn<WechslerRow, String> colType = new TableColumn<>(Lang.t("Wechseltyp", "Swap_Type"));
        colType.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().wechseltyp));
        colType.setPrefWidth(260);

        TableColumn<WechslerRow, String> colKennung = new TableColumn<>(Lang.t("Kennung", "Identifier"));
        colKennung.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().kennung));
        colKennung.setPrefWidth(160);

        TableColumn<WechslerRow, Number> colCount = new TableColumn<>(Lang.t("Anzahl Wechsel", "Swap_Count"));
        colCount.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().anzahlWechsel));
        colCount.setPrefWidth(110);

        TableColumn<WechslerRow, String> colDetails = new TableColumn<>("Details");
        colDetails.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().details));

        table.getColumns().addAll(List.of(colType, colKennung, colCount, colDetails));
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
        Lang.addListener(() -> {
            colType.setText(Lang.t("Wechseltyp", "Swap_Type"));
            colKennung.setText(Lang.t("Kennung", "Identifier"));
            colCount.setText(Lang.t("Anzahl Wechsel", "Swap_Count"));
        });

        // The summary query (PoliceAnalysisQueries.AnalysisType.DEVICE_SIM_SWAP)
        // has no lat/lon columns to centre the map on directly, so a row click
        // instead resolves an IMSI to overlay its path:
        //  - "Geräte-Wechsel" rows: Kennung IS the IMSI.
        //  - "SIM-Wechsel" rows: Kennung is the device ID, Details lists every
        //    IMSI that shared it — the first one is used.
        table.setRowFactory(tv -> {
            TableRow<WechslerRow> row = new TableRow<>();
            row.setOnMouseClicked(e -> {
                WechslerRow r = row.getItem();
                if (r == null) return;
                String imsi = r.isGeraeteWechsel() ? r.kennung : r.firstDetail();
                if (imsi != null && !imsi.isBlank()) {
                    mapView.selectImsiForPath(imsi);
                }
            });
            return row;
        });

        // Copy selected cells to clipboard via context menu or Ctrl/Cmd+C.
        TableClipboardSupport.install(table, "lighthouse_wechsler.csv");

        return table;
    }

    /**
     * One row of the {@link PoliceAnalysisQueries.AnalysisType#DEVICE_SIM_SWAP}
     * aggregation: either a subscriber (IMSI) linked to more than one device
     * ("Geräte-Wechsel", {@code kennung} = IMSI, {@code details} = the
     * different device IDs) or a device linked to more than one subscriber
     * ("SIM-Wechsel", {@code kennung} = device ID, {@code details} = the
     * different IMSIs) within the queried time window.
     */
    private static final class WechslerRow {
        final String wechseltyp;
        final String kennung;
        final int anzahlWechsel;
        final String details;

        WechslerRow(String wechseltyp, String kennung, int anzahlWechsel, String details) {
            this.wechseltyp = wechseltyp;
            this.kennung = kennung;
            this.anzahlWechsel = anzahlWechsel;
            this.details = details;
        }

        boolean isGeraeteWechsel() {
            return wechseltyp != null
                    && (wechseltyp.startsWith("Geräte-Wechsel") || wechseltyp.startsWith("Device swap"));
        }

        /** First comma-separated entry of {@link #details}, or {@code null} if empty. */
        String firstDetail() {
            if (details == null || details.isBlank()) return null;
            return details.split(",")[0].trim();
        }
    }

    // =========================================================================
    // Selection handlers
    // =========================================================================

    private void onTimelinePointSelected(DataPoint dp) {
        detailPane.show(dp, theme);
        if (dp.getCoordinate() != null && mapView.isOverview()) {
            // keep the overview, only centre on the record's location
            mapView.focusPoint(dp);
        } else if (dp.getCoordinate() != null) {
            mapView.setData(List.of(dp));
            mapView.focusPoint(dp);
        }
    }

    private void onMapPointSelected(DataPoint dp) {
        // NOTE: this used to call mapView.setData(currentMapPoints) here,
        // which unconditionally re-fits the view to the whole dataset's
        // bounding box (see MapView.setData -> fitView()) — since the
        // dataset itself never actually changes on a marker click, that
        // silently reset the user's current zoom/pan on every single click
        // (reported as clicking a cell tower "jumping back out" of a zoomed-in
        // view). highlightPoint() only marks dp as focused and repaints,
        // leaving centerLat/centerLon/zoom untouched.
        mapView.highlightPoint(dp);
        detailPane.show(dp, theme);
        timelineView.selectPoint(dp);
    }

    // =========================================================================
    // MBTiles
    // =========================================================================

    /**
     * Lädt den MBTiles-Pfad aus {@code ~/.fqlite/fqlite.conf} falls
     * {@link TileCache} noch keine MBTiles-Datei konfiguriert hat.
     *
     * <p>Wird im Konstruktor aufgerufen, damit die Offline-Karte sofort
     * beim ersten Öffnen des MapView aktiv ist — auch ohne manuellen
     * Klick auf „Offline-Karte laden…".</p>
     */
    private static void autoLoadMbTilesFromConfig() {
        if (TileCache.getInstance().isMbTilesLoaded()) return;  // bereits gesetzt

        try {
            java.io.File baseDir = new java.io.File(
                    System.getProperty("user.home"), ".fqlite");
            java.io.File conf = new java.io.File(baseDir, "fqlite.conf");
            if (!conf.exists()) return;

            java.util.Properties props = new java.util.Properties();
            try (java.io.FileInputStream fis = new java.io.FileInputStream(conf)) {
                props.load(fis);
            }

            String raw = props.getProperty("MBTILES_PATH");
            if (raw == null || raw.isBlank()) return;

            java.nio.file.Path p = java.nio.file.Path.of(raw.trim());
            if (java.nio.file.Files.exists(p)) {
                TileCache.getInstance().setMbTilesPath(p);
                LOG.info("MapViewPane: MBTiles auto-loaded from config: " + p);
            } else {
                LOG.warning("MapViewPane: configured MBTiles not found: " + p);
            }
        } catch (Exception ex) {
            LOG.warning("MapViewPane: could not auto-load MBTiles – " + ex.getMessage());
        }
    }

    private void onMbTilesButtonClicked() {
        FileChooser fc = new FileChooser();
        fc.setTitle(Lang.t("MBTiles-Offline-Karte öffnen", "Open MBTiles offline map"));
        fc.getExtensionFilters().addAll(
                new ExtensionFilter(Lang.t("MBTiles-Dateien", "MBTiles files"), "*.mbtiles"),
                new ExtensionFilter(Lang.t("Alle Dateien", "All files"),    "*.*"));
        File initial = lastMbTilesDir();
        if (initial != null) fc.setInitialDirectory(initial);
        File chosen = fc.showOpenDialog(getScene() != null ? getScene().getWindow() : null);
        if (chosen == null) return;
        TileCache.getInstance().setMbTilesPath(chosen.toPath());
        updateMbTilesStatus();
        mapView.applyTheme(theme);
    }

    private void updateMbTilesStatus() {
        TileCache tc = TileCache.getInstance();
        if (tc.isMbTilesLoaded()) {
            mbTilesStatus.setText("\u2705 " + tc.getMbTilesName());
            mbTilesStatus.setTextFill(theme.accent2);
            mbTilesButton.setText(Lang.t("\uD83D\uDDFA\uFE0F Andere Karte\u2026", "\uD83D\uDDFA\uFE0F Other map\u2026"));
        } else {
            mbTilesStatus.setText("\u26AA offline");
            mbTilesStatus.setTextFill(theme.detailMuted);
            mbTilesButton.setText(Lang.t("\uD83D\uDDFA\uFE0F Offline-Karte laden\u2026", "\uD83D\uDDFA\uFE0F Load offline map\u2026"));
        }
    }

    private File lastMbTilesDir() {
        Path p = TileCache.getInstance().getMbTilesPath();
        if (p != null && p.getParent() != null) return p.getParent().toFile();
        return new File(System.getProperty("user.home"));
    }

    // =========================================================================
    // Theme
    // =========================================================================

    private void applyThemeToSelf() {
        setStyle(theme.bgStyle());
        titleBox.setStyle(theme.bgStyle() + theme.borderBottomStyle());
        appTitle.setTextFill(theme.labelStrong);
        subtitle.setTextFill(theme.label);

        // MBTiles button
        mbTilesButton.setStyle(
                "-fx-background-color: " + Theme.hex(theme.bgAlt) + ";" +
                "-fx-text-fill: "        + Theme.hex(theme.labelStrong) + ";" +
                "-fx-border-color: "     + Theme.hex(theme.border) + ";" +
                "-fx-border-width: 1px;" +
                "-fx-background-radius: 3; -fx-border-radius: 3;" +
                "-fx-cursor: hand; -fx-padding: 4 10 4 10;");

        applyLlmBarTheme();
        updateMbTilesStatus();
    }

    private void applyLlmBarTheme() {
        llmBar.setStyle(theme.bgAltStyle() + theme.borderTopStyle());
        llmPromptField.setStyle(
                "-fx-background-color: " + Theme.hex(theme.bg) + ";" +
                "-fx-text-fill: "        + Theme.hex(theme.labelStrong) + ";" +
                "-fx-border-color: "     + Theme.hex(theme.border) + ";" +
                "-fx-border-width: 1px;" +
                "-fx-background-radius: 3; -fx-border-radius: 3;");
        llmRunButton.setStyle(
                "-fx-background-color: " + Theme.hex(theme.accent1) + ";" +
                "-fx-text-fill: "        + Theme.hex(theme.bg) + ";" +
                "-fx-font-weight: bold;" +
                "-fx-background-radius: 3; -fx-border-radius: 3;" +
                "-fx-cursor: hand; -fx-padding: 4 14 4 14;");

        // Preserve current status colour; just re-apply label on LLM node
        llmBar.getChildren().stream()
                .filter(n -> n instanceof Label && n != llmStatusLabel)
                .map(n -> (Label) n)
                .forEach(l -> l.setTextFill(theme.label));

        coLocationTable.setStyle(theme.bgStyle() + theme.borderTopStyle());
        roamersTable.setStyle(theme.bgStyle() + theme.borderTopStyle());
        wechslerTable.setStyle(theme.bgStyle() + theme.borderTopStyle());
        participantsTable.setStyle(theme.bgStyle() + theme.borderTopStyle());
    }

    // =========================================================================
    // Detail pane (inner class – unchanged)
    // =========================================================================

    static class DetailPane extends ScrollPane {

        private final VBox content = new VBox(8);

        // The DataPoint currently shown (null if nothing is selected yet) -
        // kept around so the "Copy as CSV" context-menu action can export
        // exactly the record that's on screen.
        private DataPoint currentDp;

        DetailPane() {
            setFitToWidth(true);
            content.setPadding(new Insets(14));
            setContent(content);
            setMinSize(0, 0);

            MenuItem copyCsv = new MenuItem(Lang.t("Als CSV kopieren", "Copy as CSV"));
            Lang.addListener(() -> copyCsv.setText(Lang.t("Als CSV kopieren", "Copy as CSV")));
            copyCsv.setOnAction(e -> copyAsCsv());
            ContextMenu menu = new ContextMenu(copyCsv);
            // Grey the item out instead of showing it when there's nothing
            // selected yet (currentDp == null right after clear()/startup).
            menu.setOnShowing(e -> copyCsv.setDisable(currentDp == null));
            setContextMenu(menu);

            clear(Theme.DARK);
        }

        /**
         * Copies the currently displayed record to the system clipboard as a
         * two-line CSV (header row + value row), using the column names and
         * raw column values exactly as shown under "RAW COLUMNS". Respects
         * the user's configured CSV separator (Settings -> CSV_SEPARATOR),
         * the same one used by the table view's "copy line" action.
         */
        private void copyAsCsv() {
            if (currentDp == null)
                return;

            List<String> cols = currentDp.getColumnNames();
            List<String> row  = currentDp.getRawRow();
            if (cols == null || row == null)
                return;

            String sep = Global.CSV_SEPARATOR;
            if ("[TAB]".equals(sep))
                sep = "\t";

            int n = Math.min(cols.size(), row.size());

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < n; i++) {
                if (i > 0) sb.append(sep);
                sb.append(csvEscape(cols.get(i), sep));
            }
            sb.append(System.lineSeparator());
            for (int i = 0; i < n; i++) {
                if (i > 0) sb.append(sep);
                sb.append(csvEscape(row.get(i), sep));
            }

            ClipboardContent clipboardContent = new ClipboardContent();
            clipboardContent.putString(sb.toString());
            Clipboard.getSystemClipboard().setContent(clipboardContent);
        }

        /**
         * Quotes a CSV field if it contains the separator, a quote, or a
         * line break, doubling any embedded quotes - standard CSV escaping.
         */
        private static String csvEscape(String value, String sep) {
            if (value == null)
                return "";
            String escaped = value.replace("\"", "\"\"");
            boolean mustQuote = value.contains(sep) || value.contains("\"")
                    || value.contains("\n") || value.contains("\r");
            return mustQuote ? "\"" + escaped + "\"" : escaped;
        }

        void applyTheme(Theme t) {
            String bg = Theme.hex(t.bgAlt);
            setStyle("-fx-background: " + bg + "; -fx-background-color: " + bg + ";");
            content.setStyle("-fx-background-color: " + bg + ";");
        }

        void clear(Theme t) {
            currentDp = null;
            content.getChildren().clear();
            applyTheme(t);
            Label placeholder = new Label("\u2190 Select a dot\non the timeline\nor a map marker");
            placeholder.setFont(Font.font("Monospace", 14));
            placeholder.setTextFill(t.axis);
            placeholder.setAlignment(Pos.CENTER);
            content.getChildren().add(placeholder);
        }

        void show(DataPoint dp, Theme t) {
            currentDp = dp;
            content.getChildren().clear();
            applyTheme(t);

            addRow("TABLE", dp.getTableName(),                Theme.hex(t.accent1), t);
            addRow("ROW",   String.valueOf(dp.getRowIndex()), Theme.hex(t.label),   t);
            if (dp.getTimestamp() != null)
                addRow("TIME", dp.getFormattedTimestamp() + " UTC", Theme.hex(t.accent2), t);
            if (dp.getCoordinate() != null)
                addRow("GEO",  dp.getCoordinate().toString(), Theme.hex(t.hover), t);

            addSeparator(t);

            if (dp instanceof ResponseRecordDataPoint rr) {
                addSectionHeader("RESPONSE RECORD", t);
                addOptional("END TIME",    rr.getEndTime() != null ? rr.getEndTime() + " UTC" : null, Theme.hex(t.accent2), t);
                addOptional("PARTY TYPE",  rr.getPartyType(),        Theme.hex(t.labelStrong), t);
                addOptional("NW ACCESS",   rr.getNwAccessType(),     Theme.hex(t.label), t);
                addOptional("DEVICE ID",   rr.getNaDeviceId(),       Theme.hex(t.labelStrong), t);
                addOptional("MSISDN",      rr.getMsisdn(),           Theme.hex(t.accent1), t);
                addOptional("IMSI",        rr.getImsi(),             Theme.hex(t.accent1), t);
                addOptional("DURATION",    formatDuration(rr.getDurationSeconds()), Theme.hex(t.label), t);
                addOptional("APN",         rr.getApn(),              Theme.hex(t.label), t);
                addOptional("AZIMUTH",     rr.getAzimuth() != null ? rr.getAzimuth() + "\u00b0" : null, Theme.hex(t.label), t);
                addOptional("MAP DATUM",   rr.getMapDatum(),         Theme.hex(t.label), t);
                addOptional("LT RAW",      rr.getLtRaw(),            Theme.hex(t.tick), t);
                addOptional("LO RAW",      rr.getLoRaw(),            Theme.hex(t.tick), t);
                addOptional("USER LOC",    rr.getUserLocationInfo(), Theme.hex(t.tick), t);
                addOptional("CALL IND.",   rr.getCallIndicator(),    Theme.hex(t.label), t);
                addOptional("CALL ACTION", rr.getCallActionCode(),   Theme.hex(t.label), t);
                addOptional("CALL SUBTYPE",rr.getCallSubtype(),      Theme.hex(t.label), t);
                addOptional("SESSION ID",  rr.getSessionId(),        Theme.hex(t.label), t);
                addOptional("TYPE EXTRA",  rr.getTypeOfDataExtra(),  Theme.hex(t.label), t);
                if (rr.getOtherInformation() != null && !rr.getOtherInformation().isBlank()) {
                    addSectionHeader("OTHER INFO", t);
                    for (String kv : rr.getOtherInformation().split(";\\s*"))
                        if (!kv.isBlank()) addRow("", kv.trim(), Theme.hex(t.detailValue), t);
                }
                addSeparator(t);
                addSectionHeader("RAW COLUMNS", t);
            }

            List<String> cols = dp.getColumnNames();
            List<String> row  = dp.getRawRow();
            for (int i = 0; i < Math.min(cols.size(), row.size()); i++) {
                String v = row.get(i);
                if (v != null && !v.isBlank())
                    addRow(cols.get(i), v, Theme.hex(t.detailValue), t);
            }
        }

        private void addSectionHeader(String title, Theme t) {
            Label h = new Label(title);
            h.setFont(Font.font("Monospace", FontWeight.BOLD, 13));
            h.setTextFill(t.accent1);
            h.setPadding(new Insets(4, 0, 2, 0));
            content.getChildren().add(h);
        }

        private void addSeparator(Theme t) {
            Separator sep = new Separator();
            sep.setStyle("-fx-background-color: " + Theme.hex(t.border) + ";");
            content.getChildren().add(sep);
        }

        private void addOptional(String key, String value, String hex, Theme t) {
            if (value != null && !value.isBlank()) addRow(key, value, hex, t);
        }

        private static String formatDuration(String raw) {
            if (raw == null || raw.isBlank()) return null;
            try {
                long s = Long.parseLong(raw.trim());
                if (s < 60)   return s + " s";
                if (s < 3600) return String.format("%d min %02d s", s / 60, s % 60);
                return String.format("%d h %02d min", s / 3600, (s % 3600) / 60);
            } catch (NumberFormatException e) { return raw; }
        }

        private void addRow(String key, String value, String valueHex, Theme t) {
            Label k = new Label(key);
            k.setFont(Font.font("Monospace", FontWeight.BOLD, 13));
            k.setTextFill(t.detailMuted);
            k.setPrefWidth(82);

            Label v = new Label(value);
            v.setFont(Font.font("Monospace", 13));
            v.setTextFill(Color.web(valueHex));
            v.setWrapText(true);
            v.setMaxWidth(Double.MAX_VALUE);

            HBox rowBox = new HBox(6, k, v);
            rowBox.setAlignment(Pos.TOP_LEFT);
            HBox.setHgrow(v, Priority.ALWAYS);
            content.getChildren().add(rowBox);
        }
    }
}
