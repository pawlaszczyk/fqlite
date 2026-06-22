package fqlite.timemap;

import fqlite.base.ThemeManager;
import fqlite.rag.RAGPipeline;
import fqlite.sql.DBManager;
import fqlite.sql.InMemoryDatabase;
import fqlite.timemap.DataAnalyzer.DataPoint;
import fqlite.timemap.DataAnalyzer.ResponseRecordDataPoint;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.*;
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
 * which is then executed directly against the {@link InMemoryDatabase}.
 * The resulting rows are converted to a {@link ConcurrentHashMap} and fed back
 * into {@link #setData} so the timeline, map and detail panel update
 * automatically — without opening a separate SQL Analyser window.</p>
 *
 * <p>Call {@link #attachLLM(RAGPipeline, InMemoryDatabase)} once after the
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

    // State
    private final DataAnalyzer    analyzer         = new DataAnalyzer();
    private       Theme           theme            = Theme.DARK;
    private       List<DataPoint> allPoints        = List.of();
    private       List<DataPoint> currentMapPoints = List.of();

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

    /** Set by {@link #attachLLM}; {@code null} means LLM not yet ready. */
    private RAGPipeline    ragPipeline  = null;
    /** Set by {@link #attachLLM}; needed to execute the generated SQL. */
    private InMemoryDatabase inMemoryDB = null;

    // =========================================================================
    // Construction
    // =========================================================================

    public MapViewPane() {

        // ── App title ─────────────────────────────────────────────────────────
        appTitle = new Label("\u2B21 GEO\u00B7TIME ANALYZER");
        appTitle.setFont(Font.font("Monospace", FontWeight.BOLD, 15));

        subtitle = new Label("Timestamps & geo-coordinates from SQLite tables");
        subtitle.setFont(Font.font("Monospace", 11));

        // ── MBTiles: Pfad aus fqlite.conf laden falls noch nicht gesetzt ──────
        autoLoadMbTilesFromConfig();

        // ── MBTiles offline control ───────────────────────────────────────────
        mbTilesButton = new Button("\uD83D\uDDFA\uFE0F Offline-Karte laden\u2026");
        mbTilesButton.setFont(Font.font("Monospace", 11));
        mbTilesButton.setOnAction(e -> onMbTilesButtonClicked());
        Tooltip.install(mbTilesButton, new Tooltip(
                "MBTiles-Datei als Offline-Kartenquelle laden.\n" +
                "Tiles werden zuerst aus dieser Datei bezogen;\n" +
                "fehlende Tiles werden weiterhin online geladen."));

        mbTilesStatus = new Label();
        mbTilesStatus.setFont(Font.font("Monospace", 10));
        updateMbTilesStatus();

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        titleBox = new HBox(16, appTitle, subtitle, spacer, mbTilesStatus, mbTilesButton);
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

        // ── Tabs: map/timeline view vs. dedicated police-analysis panel ───────
        Tab mapTab = new Tab("🗺️ Karte & Zeitleiste", splitPane);
        mapTab.setClosable(false);
        Tab policeTab = new Tab("🔎 Polizeiliche Auswertung", policeAnalysisPane);
        policeTab.setClosable(false);

        TabPane centerTabs = new TabPane(mapTab, policeTab);
        centerTabs.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
        setCenter(centerTabs);

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

        // ── Wire selection events ─────────────────────────────────────────────
        timelineView.setSelectionListener(this::onTimelinePointSelected);
        mapView.setSelectionListener(this::onMapPointSelected);
        timelineView.setFilterListener(filtered -> {
            currentMapPoints = filtered;
            mapView.setData(filtered);
        });

        // ── LLM bar (bottom) ──────────────────────────────────────────────────
        llmPromptField = new TextField();
        llmPromptField.setPromptText(
                "Natürlichsprachige Anfrage an LLM (z. B. \"Alle Einträge mit GPS-Koordinaten\")…");
        llmPromptField.setFont(Font.font("Monospace", 11));
        HBox.setHgrow(llmPromptField, Priority.ALWAYS);
        llmPromptField.setOnAction(e -> onLlmRun());   // Enter triggers run

        llmRunButton = new Button("Ask \u25B6");
        llmRunButton.setFont(Font.font("Monospace", FontWeight.BOLD, 11));
        llmRunButton.setTooltip(new Tooltip(
                "Prompt an LLM senden → SELECT generieren → Karte aktualisieren"));
        llmRunButton.setDisable(true);   // enabled by attachLLM()
        llmRunButton.setOnAction(e -> onLlmRun());

        // Small "reset to full dataset" button
        Button llmResetButton = new Button("\u21BA Reset");
        llmResetButton.setFont(Font.font("Monospace", 11));
        llmResetButton.setTooltip(new Tooltip("Vollständige Datenmenge wiederherstellen"));
        llmResetButton.setOnAction(e -> onLlmReset());

        llmStatusLabel = new Label("\u23F3 LLM wird initialisiert\u2026");
        llmStatusLabel.setFont(Font.font("Monospace", 10));


        VBox bottom = new VBox();
        llmBar = new HBox(8,
                new Label("\uD83E\uDD16"),
                llmPromptField,
                llmRunButton,
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

        bottom.setPadding(new Insets(8, 14, 8, 14));
        bottom.getChildren().add(llmBar);
        bottom.getChildren().add(llmStatusLabel);
        bottom.getChildren().add(coLocationTable);
        bottom.getChildren().add(roamersTable);
        bottom.getChildren().add(wechslerTable);
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
     * @param mdb      the {@link InMemoryDatabase} that backs the current
     *                 analysis; its name must match the key in {@link DBManager}
     */
    public void attachLLM(RAGPipeline pipeline, InMemoryDatabase mdb) {
        this.ragPipeline = pipeline;
        this.inMemoryDB  = mdb;
        this.dbName      = mdb != null ? mdb.getDbName() : null;
        policeAnalysisPane.setDatabase(mdb);
        Platform.runLater(() -> {
            if (pipeline != null) {
                llmRunButton.setDisable(false);
                llmStatusLabel.setText("\u2705 LLM bereit");
                llmStatusLabel.setTextFill(theme.accent2);
                llmPromptField.setPromptText(
                        "Natürlichsprachige Anfrage an LLM (z. B. \"Alle Einträge mit GPS-Koordinaten\")…");
            } else {
                llmRunButton.setDisable(true);
                llmStatusLabel.setText("\u26AA LLM nicht verfügbar");
                llmStatusLabel.setTextFill(theme.detailMuted);
            }
            applyLlmBarTheme();
        });
    }

    public void setData(
            ConcurrentHashMap<String, ObservableList<ObservableList<String>>> resultlist,
            Map<String, List<String>> headers) {

        if (!Platform.isFxApplicationThread()) {
            Platform.runLater(() -> setData(resultlist, headers));
            return;
        }

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

        allPoints        = analyzer.analyze(resultlist, safeHeaders);
        currentMapPoints = allPoints;
        timelineView.setData(allPoints);
        mapView.setData(allPoints);
        detailPane.clear(theme);
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
        if (inMemoryDB == null || sql == null) return;
        String trimmed = sql.trim();
        if (!trimmed.regionMatches(true, 0, "SELECT", 0, 6)) return;

        ResultSet rs = inMemoryDB.execute(trimmed);
        if (rs == null) return;

        QueryRows qr = toResponseRecordsTable(rs);
        rs.close();

        Platform.runLater(() -> {
            setCoLocationTableVisible(false);
            setRoamersTableVisible(false);
            setWechslerTableVisible(false);
            setData(qr.data, qr.headers);
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
        setData(resultlist, headers);
    }

    public void setTheme(Theme t) {
        this.theme = t;
        applyThemeToSelf();
        timelineView.applyTheme(t);
        mapView.applyTheme(t);
        detailPane.applyTheme(t);
        policeAnalysisPane.applyTheme(t);
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
    private void onLlmRun() {
        String prompt = llmPromptField.getText().trim();
        if (prompt.isEmpty() || ragPipeline == null) return;

        llmRunButton.setDisable(true);
        setLlmStatus("⏳ Analysiere Anfrage…", theme.label);
        setCoLocationTableVisible(false);
        setRoamersTableVisible(false);
        setWechslerTableVisible(false);

        Thread worker = new Thread(() -> {
            try {
                RAGPipeline.QueryIntent intent = ragPipeline.classifyIntent(prompt);

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
                if (mentionedPrefix != null) {
                    String country = CountryCallingCodeLookup.getCountryNameByPrefix(mentionedPrefix);
                    runCountryFilterQuery(country, intent.startTime, intent.endTime);
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
                        setLlmStatus("⚠️ Zeitraum konnte nicht erkannt werden — bitte Zeitraum präzisieren", theme.hover);
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
                } else {
                    runFilterQuery(prompt);
                }
            } catch (Exception ex) {
                Platform.runLater(() -> {
                    setLlmStatus("❌ " + ex.getMessage(), theme.hover);
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
                setLlmStatus("⚠️ LLM konnte keine gültige SQL erzeugen — bitte Anfrage umformulieren", theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }
        Platform.runLater(() -> setLlmStatus("⚙️ Führe aus: " + abbreviate(sql, 60), theme.label));

        // 2. Execute SQL against in-memory DB
        ResultSet rs = inMemoryDB.execute(sql);
        if (rs == null) {
            Platform.runLater(() -> {
                setLlmStatus("⚠️ Kein Ergebnis", theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        // 3. Convert ResultSet → ConcurrentHashMap (same format as setData)
        QueryRows qr = toResponseRecordsTable(rs);
        rs.close();

        final String finalSql = sql;
        Platform.runLater(() -> {
            setCoLocationTableVisible(false);
            setRoamersTableVisible(false);
            setWechslerTableVisible(false);
            setData(qr.data, qr.headers);
            setLlmStatus(
                    "✅ " + qr.rowCount + " Zeile(n) | " + abbreviate(finalSql, 50),
                    theme.accent2);
            llmRunButton.setDisable(false);
        });
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
        Platform.runLater(() -> setLlmStatus("⚙️ Suche Anrufe/SMS…", theme.label));

        String sql = RAGPipeline.buildCallsSmsSql(intent.callSmsType, intent.startTime, intent.endTime);
        if (sql == null) {
            Platform.runLater(() -> {
                setLlmStatus("⚠️ Anfrage konnte nicht zugeordnet werden", theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        ResultSet rs = inMemoryDB.execute(sql);
        if (rs == null) {
            Platform.runLater(() -> {
                setLlmStatus("⚠️ Kein Ergebnis", theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        QueryRows qr = toResponseRecordsTable(rs);
        rs.close();

        Platform.runLater(() -> {
            setCoLocationTableVisible(false);
            setRoamersTableVisible(false);
            setWechslerTableVisible(false);
            setData(qr.data, qr.headers);
            setLlmStatus(
                    qr.rowCount == 0
                            ? "⚠️ Keine passenden Anrufe/SMS im Zeitfenster gefunden"
                            : "✅ " + qr.rowCount + " Datensatz/Datensätze gefunden",
                    qr.rowCount == 0 ? theme.hover : theme.accent2);
            llmRunButton.setDisable(false);
        });
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
        Platform.runLater(() -> setLlmStatus("⚙️ Suche Rufnummern aus " + country + "…", theme.label));

        String sql = RAGPipeline.buildCountryFilterSql(country, startTime, endTime);
        if (sql == null) {
            Platform.runLater(() -> {
                setLlmStatus("⚠️ Land \"" + country + "\" konnte keiner Vorwahl zugeordnet werden", theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        ResultSet rs = inMemoryDB.execute(sql);
        if (rs == null) {
            Platform.runLater(() -> {
                setLlmStatus("⚠️ Kein Ergebnis", theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        QueryRows qr = toResponseRecordsTable(rs);
        rs.close();

        Platform.runLater(() -> {
            setCoLocationTableVisible(false);
            setRoamersTableVisible(false);
            setWechslerTableVisible(false);
            setData(qr.data, qr.headers);
            setLlmStatus(
                    qr.rowCount == 0
                            ? "⚠️ Keine Rufnummern aus " + country + " im Zeitfenster gefunden"
                            : "✅ " + qr.rowCount + " Datensatz/Datensätze aus " + country,
                    qr.rowCount == 0 ? theme.hover : theme.accent2);
            llmRunButton.setDisable(false);
        });
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
        Platform.runLater(() -> setLlmStatus("⚙️ Suche Wanderer…", theme.label));

        String sql = RAGPipeline.buildRoamersSql(intent.startTime, intent.endTime, intent.identifierColumn);

        ResultSet rs = inMemoryDB.execute(sql);
        if (rs == null) {
            Platform.runLater(() -> {
                setLlmStatus("⚠️ Kein Ergebnis", theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        QueryRows qr = toResponseRecordsTable(rs);
        rs.close();

        String summarySql = PoliceAnalysisQueries.buildSql(
                PoliceAnalysisQueries.AnalysisType.ROAMERS, intent.startTime, intent.endTime);
        List<RoamerRow> summaryRows = new ArrayList<>();
        ResultSet summaryRs = inMemoryDB.execute(summarySql);
        if (summaryRs != null) {
            while (summaryRs.next()) {
                summaryRows.add(new RoamerRow(
                        summaryRs.getString("IMSI"),
                        summaryRs.getString("Rufnummer"),
                        summaryRs.getInt("Anzahl_Funkzellen"),
                        summaryRs.getString("Funkzellen"),
                        summaryRs.getString("Von"),
                        summaryRs.getString("Bis")));
            }
            summaryRs.close();
        }

        Platform.runLater(() -> {
            setCoLocationTableVisible(false);
            setWechslerTableVisible(false);
            roamersTable.getItems().setAll(summaryRows);
            setRoamersTableVisible(!summaryRows.isEmpty());
            setData(qr.data, qr.headers);
            setLlmStatus(
                    qr.rowCount == 0
                            ? "⚠️ Keine Wanderer im Zeitfenster gefunden"
                            : "✅ " + summaryRows.size() + " Wanderer (" + qr.rowCount + " Datensatz/Datensätze)",
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
        Platform.runLater(() -> setLlmStatus("⚙️ Suche Wechsler…", theme.label));

        String sql = RAGPipeline.buildWechslerSql(intent.startTime, intent.endTime);

        ResultSet rs = inMemoryDB.execute(sql);
        if (rs == null) {
            Platform.runLater(() -> {
                setLlmStatus("⚠️ Kein Ergebnis", theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        QueryRows qr = toResponseRecordsTable(rs);
        rs.close();

        String summarySql = PoliceAnalysisQueries.buildSql(
                PoliceAnalysisQueries.AnalysisType.DEVICE_SIM_SWAP, intent.startTime, intent.endTime);
        List<WechslerRow> summaryRows = new ArrayList<>();
        ResultSet summaryRs = inMemoryDB.execute(summarySql);
        if (summaryRs != null) {
            while (summaryRs.next()) {
                summaryRows.add(new WechslerRow(
                        summaryRs.getString("Wechseltyp"),
                        summaryRs.getString("Kennung"),
                        summaryRs.getInt("Anzahl_Wechsel"),
                        summaryRs.getString("Details")));
            }
            summaryRs.close();
        }

        Platform.runLater(() -> {
            setCoLocationTableVisible(false);
            setRoamersTableVisible(false);
            wechslerTable.getItems().setAll(summaryRows);
            setWechslerTableVisible(!summaryRows.isEmpty());
            setData(qr.data, qr.headers);
            setLlmStatus(
                    qr.rowCount == 0
                            ? "⚠️ Keine Wechsler im Zeitfenster gefunden"
                            : "✅ " + summaryRows.size() + " Wechsler (" + qr.rowCount + " Datensatz/Datensätze)",
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
        while (rs.next()) {
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

    /** Small holder returned by {@link #toResponseRecordsTable}. */
    private static final class QueryRows {
        final ConcurrentHashMap<String, ObservableList<ObservableList<String>>> data;
        final Map<String, List<String>> headers;
        final int rowCount;

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
        Platform.runLater(() -> setLlmStatus("⚙️ Suche gemeinsame Funkzellen…", theme.label));

        String sql = RAGPipeline.buildColocationSql(intent.startTime, intent.endTime, intent.identifierColumn);
        if (sql == null) {
            Platform.runLater(() -> {
                setLlmStatus("⚠️ Zeitraum konnte nicht erkannt werden", theme.hover);
                llmRunButton.setDisable(false);
            });
            return;
        }

        ResultSet rs = inMemoryDB.execute(sql);
        if (rs == null) {
            Platform.runLater(() -> {
                setLlmStatus("⚠️ Kein Ergebnis", theme.hover);
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
                ResultSet detailRs = inMemoryDB.execute(detailSql);
                if (detailRs != null) {
                    detailRows = toResponseRecordsTable(detailRs);
                    detailRs.close();
                }
            }
        }
        final QueryRows finalDetailRows = detailRows;

        Platform.runLater(() -> {
            colCoLocationCount.setText("Anzahl " + intent.identifierLabel + "s");
            colCoLocationIds.setText(intent.identifierLabel + "s");
            coLocationTable.getItems().setAll(result);
            setCoLocationTableVisible(!result.isEmpty());
            setRoamersTableVisible(false);
            setWechslerTableVisible(false);
            if (finalDetailRows != null) {
                setData(finalDetailRows.data, finalDetailRows.headers);
            }
            setLlmStatus(
                    result.isEmpty()
                            ? "⚠️ Keine gemeinsamen Funkzellen im Zeitfenster gefunden (" + intent.identifierLabel + ")"
                            : "✅ " + result.size() + " Standort(e) mit gemeinsamer Anmeldung (" + intent.identifierLabel + ")",
                    result.isEmpty() ? theme.hover : theme.accent2);
            llmRunButton.setDisable(false);
        });
    }

    /**
     * Restores the full base dataset (before any LLM filter was applied).
     */
    private void onLlmReset() {
        setCoLocationTableVisible(false);
        setRoamersTableVisible(false);
        setWechslerTableVisible(false);
        if (baseResultList != null) {
            setData(baseResultList, baseHeaders != null ? baseHeaders : Map.of());
            setLlmStatus("↺ Alle Daten wiederhergestellt", theme.label);
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
        table.setPlaceholder(new Label("Keine gemeinsamen Funkzellen gefunden."));

        TableColumn<CoLocationGroup, String> colPos = new TableColumn<>("Standort");
        colPos.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().position()));
        colPos.setPrefWidth(150);

        TableColumn<CoLocationGroup, String> colCell = new TableColumn<>("Funkzelle (MCC-MNC-LAC/TAC-CI)");
        colCell.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().cellLabel()));
        colCell.setPrefWidth(190);

        TableColumn<CoLocationGroup, String> colFrom = new TableColumn<>("Von");
        colFrom.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().earliest));
        colFrom.setPrefWidth(150);

        TableColumn<CoLocationGroup, String> colTo = new TableColumn<>("Bis");
        colTo.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().latest));
        colTo.setPrefWidth(150);

        colCoLocationCount = new TableColumn<>("Anzahl IMSIs");
        colCoLocationCount.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().ids.size()));
        colCoLocationCount.setPrefWidth(100);

        colCoLocationIds = new TableColumn<>("IMSIs");
        colCoLocationIds.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().idList()));

        table.getColumns().addAll(List.of(colPos, colCell, colFrom, colTo, colCoLocationCount, colCoLocationIds));
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);

        table.setRowFactory(tv -> {
            TableRow<CoLocationGroup> row = new TableRow<>();
            row.setOnMouseClicked(e -> {
                if (row.getItem() != null) mapView.centerOnCoordinate(row.getItem().lat, row.getItem().lon);
            });
            return row;
        });

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
        table.setPlaceholder(new Label("Keine Wanderer gefunden."));

        TableColumn<RoamerRow, String> colImsi = new TableColumn<>("IMSI");
        colImsi.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().imsi));
        colImsi.setPrefWidth(160);

        TableColumn<RoamerRow, String> colMsisdn = new TableColumn<>("Rufnummer");
        colMsisdn.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().msisdn));
        colMsisdn.setPrefWidth(130);

        TableColumn<RoamerRow, Number> colCount = new TableColumn<>("Anzahl Funkzellen");
        colCount.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().cellCount));
        colCount.setPrefWidth(120);

        TableColumn<RoamerRow, String> colFrom = new TableColumn<>("Von");
        colFrom.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().earliest));
        colFrom.setPrefWidth(150);

        TableColumn<RoamerRow, String> colTo = new TableColumn<>("Bis");
        colTo.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().latest));
        colTo.setPrefWidth(150);

        TableColumn<RoamerRow, String> colSites = new TableColumn<>("Funkzellen");
        colSites.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().sitesRaw));

        table.getColumns().addAll(List.of(colImsi, colMsisdn, colCount, colFrom, colTo, colSites));
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);

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
        table.setPlaceholder(new Label("Keine Wechsler gefunden."));

        TableColumn<WechslerRow, String> colType = new TableColumn<>("Wechseltyp");
        colType.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().wechseltyp));
        colType.setPrefWidth(260);

        TableColumn<WechslerRow, String> colKennung = new TableColumn<>("Kennung");
        colKennung.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().kennung));
        colKennung.setPrefWidth(160);

        TableColumn<WechslerRow, Number> colCount = new TableColumn<>("Anzahl Wechsel");
        colCount.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().anzahlWechsel));
        colCount.setPrefWidth(110);

        TableColumn<WechslerRow, String> colDetails = new TableColumn<>("Details");
        colDetails.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().details));

        table.getColumns().addAll(List.of(colType, colKennung, colCount, colDetails));
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);

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
            return wechseltyp != null && wechseltyp.startsWith("Geräte-Wechsel");
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
        if (dp.getCoordinate() != null) {
            mapView.setData(List.of(dp));
            mapView.focusPoint(dp);
        }
    }

    private void onMapPointSelected(DataPoint dp) {
        mapView.setData(currentMapPoints);
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
        fc.setTitle("MBTiles-Offline-Karte öffnen");
        fc.getExtensionFilters().addAll(
                new ExtensionFilter("MBTiles-Dateien", "*.mbtiles"),
                new ExtensionFilter("Alle Dateien",    "*.*"));
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
            mbTilesButton.setText("\uD83D\uDDFA\uFE0F Andere Karte\u2026");
        } else {
            mbTilesStatus.setText("\u26AA offline");
            mbTilesStatus.setTextFill(theme.detailMuted);
            mbTilesButton.setText("\uD83D\uDDFA\uFE0F Offline-Karte laden\u2026");
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
    }

    // =========================================================================
    // Detail pane (inner class – unchanged)
    // =========================================================================

    static class DetailPane extends ScrollPane {

        private final VBox content = new VBox(8);

        DetailPane() {
            setFitToWidth(true);
            content.setPadding(new Insets(14));
            setContent(content);
            setMinSize(0, 0);
            clear(Theme.DARK);
        }

        void applyTheme(Theme t) {
            String bg = Theme.hex(t.bgAlt);
            setStyle("-fx-background: " + bg + "; -fx-background-color: " + bg + ";");
            content.setStyle("-fx-background-color: " + bg + ";");
        }

        void clear(Theme t) {
            content.getChildren().clear();
            applyTheme(t);
            Label placeholder = new Label("\u2190 Select a dot\non the timeline\nor a map marker");
            placeholder.setFont(Font.font("Monospace", 11));
            placeholder.setTextFill(t.axis);
            placeholder.setAlignment(Pos.CENTER);
            content.getChildren().add(placeholder);
        }

        void show(DataPoint dp, Theme t) {
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
            h.setFont(Font.font("Monospace", FontWeight.BOLD, 10));
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
            k.setFont(Font.font("Monospace", FontWeight.BOLD, 10));
            k.setTextFill(t.detailMuted);
            k.setPrefWidth(82);

            Label v = new Label(value);
            v.setFont(Font.font("Monospace", 10));
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
