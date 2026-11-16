package fqlite.timemap;

import fqlite.sql.QueryableDatabase;
import fqlite.timemap.PoliceAnalysisQueries.AnalysisType;
import fqlite.timemap.PoliceAnalysisQueries.Params;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.logging.Logger;

/**
 * Dedicated panel for the police investigative focus areas
 * ("Untersuchungsschwerpunkte der Polizei zu Funkmastdaten"):
 *
 * <ol>
 *   <li>Alle Rufnummern</li>
 *   <li>In-/ausländische Rufnummern</li>
 *   <li>A-/B-/C-Teilnehmer</li>
 *   <li>Teilnehmer in der Funkzelle</li>
 *   <li>Verbindungstyp (Telefon/SMS/GPRS)</li>
 *   <li>Wechsler von Endgeräten/SIM-Karten</li>
 *   <li>Kreuztreffer über mehrere Datenquellen</li>
 *   <li>Wanderer (mehrere Funkzellen)</li>
 *   <li>Ähnliche Rufnummern (Rufnummernblöcke)</li>
 *   <li>Wiederkehrende Rufnummernpaare</li>
 *   <li>Twins</li>
 * </ol>
 *
 * <p>Item 12 ("Filterung vom Datum bis einschließlich Stunde/Minute/Sekunde")
 * is implemented as the optional time-range filter at the top of this panel,
 * which applies to whichever of the 11 analyses above is currently selected,
 * rather than as a 12th standalone analysis.</p>
 *
 * <p>Every analysis is a fixed SQL template from {@link PoliceAnalysisQueries}
 * — never LLM-generated — executed against {@code response_records} via
 * {@link QueryableDatabase} (in-memory copy or the database file itself). Results are shown in a generic table whose
 * columns are built dynamically from the query's {@link ResultSetMetaData},
 * the same approach used by {@code fqlite.sql.SQLParser#fillTable}.</p>
 *
 * <p>Every user-facing label/button/status text in this panel is bilingual
 * (German/English) via {@link Lang#t}, following the same runtime
 * "DE | EN" toggle used elsewhere in the Lighthouse window; a single
 * {@link Lang#addListener} registered in the constructor re-applies all of
 * them (plus the analysis combo box's display and the parameter-field
 * labels) whenever the language is switched.</p>
 */
public class PoliceAnalysisPane extends BorderPane {

    private static final Logger LOG = Logger.getLogger(PoliceAnalysisPane.class.getName());

    private static final Pattern TIMESTAMP_PATTERN =
            Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");

    private final Label title = new Label();

    private final ComboBox<AnalysisType> typeBox = new ComboBox<>();
    private final Label descriptionLabel = new Label();

    private final CheckBox timeFilterEnabled = new CheckBox();
    /** take the identifiers of a result over to "Map & Timeline" */
    private final CheckBox mapTransfer = new CheckBox();
    private final DateTimeField fromField = new DateTimeField();
    private final DateTimeField toField   = new DateTimeField();

    // Identifier/Gebiet-Eingabe — nur sichtbar/benötigt für Auswertungen mit
    // AnalysisType#requiresIdentifier bzw. #requiresArea, siehe
    // updateParamFieldsVisibility().
    private final Label identifierLabel = new Label();
    private final TextField identifierField = new TextField();
    private final Label areaLabel = new Label();
    private final TextField areaField = new TextField();
    private final Label radiusLabel = new Label();
    private final TextField radiusField = new TextField();

    /** Shows the (optional, online) HiCellTek device-info result for DEVICE_INFO_LOOKUP — hidden otherwise. */
    private final Label deviceInfoLabel = new Label();

    private final Button runButton = new Button();
    private final TableView<ObservableList<String>> resultTable = new TableView<>();
    private final Label resultTablePlaceholder = new Label();
    private final Label statusLabel = new Label();

    private QueryableDatabase database = null;
    private Theme theme = Theme.DARK;

    /** Column names of the current {@link #resultTable} contents, set by {@link #fillResultTable}. */
    private List<String> currentColumnNames = List.of();

    /**
     * Invoked with (lat, lon) when the analyst picks "Auf Karte anzeigen" from
     * a result row's context menu — wired by {@link MapViewPane} to switch to
     * the map tab and centre on that coordinate.
     */
    private BiConsumer<Double, Double> mapCenterListener;

    /** Registers the callback used to jump to/centre the map for a result row. */
    public void setMapCenterListener(BiConsumer<Double, Double> listener) {
        this.mapCenterListener = listener;
    }

    /**
     * Invoked (in addition to {@link #mapCenterListener}) with the row's
     * "IMSI" column value, if the current analysis has one — wired by
     * {@link MapViewPane} to also select that IMSI in the map's path
     * overlay. A single coordinate (from {@code mapCenterListener}) is only
     * ever the first of possibly several cells a "Wanderer"-type result
     * visited; selecting the IMSI draws all of them, which is what answers
     * what such a row is actually about.
     */
    private java.util.function.Consumer<String> imsiSelectListener;

    /** Registers the callback used to additionally select a row's IMSI in the map's path overlay. */
    public void setImsiSelectListener(java.util.function.Consumer<String> listener) {
        this.imsiSelectListener = listener;
    }

    public PoliceAnalysisPane() {

        title.setText(Lang.t("🔎 Statistiken", "🔎 Statistics"));
        title.setFont(Font.font("Monospace", FontWeight.BOLD, 18));

        typeBox.getItems().addAll(AnalysisType.values());
        typeBox.getSelectionModel().selectFirst();
        typeBox.setMaxWidth(Double.MAX_VALUE);
        typeBox.setOnAction(e -> {
            updateDescription();
            updateParamFieldsVisibility();
        });

        descriptionLabel.setWrapText(true);
        descriptionLabel.setFont(Font.font("Monospace", 13));
        updateDescription();

        timeFilterEnabled.setText(Lang.t("Zeitraum einschränken", "Restrict time range"));
        mapTransfer.setText(Lang.t("Ergebnis in Karte & Zeitleiste übernehmen", "Show result in Map & Timeline"));
        mapTransfer.setSelected(true);
        mapTransfer.setTooltip(new Tooltip(Lang.t(
                "Zeigt im Tab \"Karte & Zeitleiste\" die Datensätze der Kennungen des Ergebnisses\n"
                        + "(im eingestellten Zeitraum), z. B. alle Datensätze der gefundenen Wanderer.\n"
                        + "Übersichtsauswertungen (alle Rufnummern, Funkzellen, Länder, …) ändern die Karte nicht.",
                "Shows the records of the identifiers in the result (within the time range)\n"
                        + "in the \"Map & Timeline\" tab, e.g. all records of the roamers found.\n"
                        + "Overview analyses (all numbers, cell sites, countries, …) leave the map unchanged.")));
        fromField.setPromptText(Lang.t("JJJJ-MM-TT (von)", "YYYY-MM-DD (from)"));
        toField.setPromptText(Lang.t("JJJJ-MM-TT (bis)", "YYYY-MM-DD (to)"));
        fromField.setFieldFont(Font.font("Monospace", 14));
        toField.setFieldFont(Font.font("Monospace", 14));
        fromField.setDisable(true);
        toField.setDisable(true);

        timeFilterEnabled.setOnAction(e -> {
            boolean on = timeFilterEnabled.isSelected();
            fromField.setDisable(!on);
            toField.setDisable(!on);
        });

        identifierField.setFont(Font.font("Monospace", 14));
        areaField.setFont(Font.font("Monospace", 14));
        areaField.setPromptText("52.5200,13.4050");
        radiusField.setFont(Font.font("Monospace", 14));
        radiusField.setPromptText("500");
        radiusField.setPrefWidth(70);
        identifierLabel.setFont(Font.font("Monospace", 13));
        areaLabel.setFont(Font.font("Monospace", 13));
        radiusLabel.setFont(Font.font("Monospace", 13));
        areaLabel.setText(Lang.t("Gebiet (Breitengrad,Längengrad):", "Area (latitude,longitude):"));
        radiusLabel.setText(Lang.t("Radius (m):", "Radius (m):"));

        deviceInfoLabel.setWrapText(true);
        deviceInfoLabel.setFont(Font.font("Monospace", 13));
        deviceInfoLabel.setManaged(false);
        deviceInfoLabel.setVisible(false);

        runButton.setText(Lang.t("▶ Ausführen", "▶ Run"));
        runButton.setFont(Font.font("Monospace", FontWeight.BOLD, 14));
        runButton.setDisable(true);   // enabled by setDatabase()
        runButton.setOnAction(e -> {
            if (running) cancelAnalysis();
            else runAnalysis();
        });

        HBox timeBox = new HBox(8, timeFilterEnabled, fromField, new Label("→"), toField);
        timeBox.setAlignment(Pos.CENTER_LEFT);

        HBox paramBox = new HBox(8, identifierLabel, identifierField, areaLabel, areaField, radiusLabel, radiusField);
        paramBox.setAlignment(Pos.CENTER_LEFT);
        HBox.setHgrow(identifierField, Priority.ALWAYS);

        HBox controls = new HBox(10, typeBox, runButton, mapTransfer);
        controls.setAlignment(Pos.CENTER_LEFT);
        HBox.setHgrow(typeBox, Priority.ALWAYS);

        VBox top = new VBox(8, title, controls, descriptionLabel, timeBox, paramBox, deviceInfoLabel);
        top.setPadding(new Insets(12, 16, 12, 16));
        setTop(top);

        updateParamFieldsVisibility();

        resultTable.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);
        resultTablePlaceholder.setText(Lang.t(
                "Auswertungstyp wählen und auf \"Ausführen\" klicken.",
                "Select an analysis type and click \"Run\"."));
        resultTable.setPlaceholder(resultTablePlaceholder);
        // Copy selected cells to clipboard via context menu or Ctrl/Cmd+C —
        // merged into the existing per-row menu below rather than a second,
        // competing table-level one.
        TableClipboardSupport.enableCellSelectionAndCopyShortcut(resultTable);

        resultTable.setRowFactory(tv -> {
            TableRow<ObservableList<String>> row = new TableRow<>();
            MenuItem showOnMapItem = new MenuItem(Lang.t("📍 Auf Karte anzeigen", "📍 Show on map"));
            Lang.addListener(() -> showOnMapItem.setText(Lang.t("📍 Auf Karte anzeigen", "📍 Show on map")));
            showOnMapItem.setOnAction(e -> showSelectedRowOnMap(row.getItem()));
            ContextMenu rowMenu = new ContextMenu(showOnMapItem, TableClipboardSupport.copyMenuItem(resultTable),
                    TableClipboardSupport.exportMenuItem(resultTable, this::csvFileName, () -> resultCsv));
            row.contextMenuProperty().bind(
                    Bindings.when(row.emptyProperty()).then((ContextMenu) null).otherwise(rowMenu));
            return row;
        });
        // also on the empty area below the rows
        resultTable.setContextMenu(new ContextMenu(TableClipboardSupport.copyMenuItem(resultTable),
                TableClipboardSupport.exportMenuItem(resultTable, this::csvFileName, () -> resultCsv)));
        setCenter(resultTable);

        statusLabel.setText(Lang.t("⚪ Keine Datenbank verbunden", "⚪ No database connected"));
        statusLabel.setFont(Font.font("Monospace", 13));
        statusLabel.setPadding(new Insets(6, 16, 8, 16));
        setBottom(statusLabel);

        applyTheme(theme);

        // Re-applies every static label/button/placeholder above, plus the
        // combo box's own display (AnalysisType#toString() reflects
        // Lang.isGerman() dynamically, but the ComboBox skin only re-reads
        // it when the item list itself changes) and the parameter-field
        // labels/prompts (via updateParamFieldsVisibility(), which already
        // branches on Lang.isGerman()) — so a language switch mid-session
        // updates everything already on screen, not just newly created widgets.
        Lang.addListener(() -> {
            title.setText(Lang.t("🔎 Statistiken", "🔎 Statistics"));
            timeFilterEnabled.setText(Lang.t("Zeitraum einschränken", "Restrict time range"));
            mapTransfer.setText(Lang.t("Ergebnis in Karte & Zeitleiste übernehmen", "Show result in Map & Timeline"));
            fromField.setPromptText(Lang.t("JJJJ-MM-TT (von)", "YYYY-MM-DD (from)"));
            toField.setPromptText(Lang.t("JJJJ-MM-TT (bis)", "YYYY-MM-DD (to)"));
            areaLabel.setText(Lang.t("Gebiet (Breitengrad,Längengrad):", "Area (latitude,longitude):"));
            radiusLabel.setText(Lang.t("Radius (m):", "Radius (m):"));
            runButton.setText(running ? Lang.t("■ Abbrechen", "■ Cancel") : Lang.t("▶ Ausführen", "▶ Run"));
            resultTablePlaceholder.setText(Lang.t(
                    "Auswertungstyp wählen und auf \"Ausführen\" klicken.",
                    "Select an analysis type and click \"Run\"."));

            AnalysisType selected = typeBox.getSelectionModel().getSelectedItem();
            typeBox.setItems(FXCollections.observableArrayList(AnalysisType.values()));
            typeBox.getSelectionModel().select(selected);

            updateDescription();
            updateParamFieldsVisibility();
        });
    }

    /** Connects the database that backs the current case. Call this whenever the underlying data changes. */
    public void setDatabase(QueryableDatabase mdb) {
        this.database = mdb;
        this.tableRows = -1;
        this.rangeCounts.clear();
        if (mdb != null) presetTimeRange(mdb);
        Platform.runLater(() -> {
            runButton.setDisable(mdb == null);
            setStatus(mdb == null
                    ? Lang.t("⚪ Keine Datenbank verbunden", "⚪ No database connected")
                    : Lang.t("✅ Bereit", "✅ Ready"),
                    mdb == null ? theme.detailMuted : theme.accent2);
        });
    }

    /**
     * Presets the time range with the first and last start_time of the
     * records (read through the index in the background) and marks the days
     * of the data in the calendars. Values the analyst already chose stay.
     */
    private void presetTimeRange(QueryableDatabase mdb) {
        Thread t = new Thread(() -> {
            String min = null, max = null;
            try {
                ResultSet rs = mdb.execute("SELECT start_time FROM response_records WHERE start_time >= '' "
                        + "AND typeof(start_time) = 'text' ORDER BY start_time LIMIT 1");
                if (rs != null) { if (rs.next()) min = rs.getString(1); rs.close(); }
                rs = mdb.execute("SELECT start_time FROM response_records WHERE start_time >= '' "
                        + "AND typeof(start_time) = 'text' ORDER BY start_time DESC LIMIT 1");
                if (rs != null) { if (rs.next()) max = rs.getString(1); rs.close(); }
            } catch (Exception e) {
                return;
            }
            java.time.Instant a = min == null ? null : DataAnalyzer.parseIsoSeconds(min.trim());
            java.time.Instant z = max == null ? null : DataAnalyzer.parseIsoSeconds(max.trim());
            if (a == null || z == null) return;
            java.time.LocalDateTime from = java.time.LocalDateTime.ofInstant(a, java.time.ZoneOffset.UTC);
            java.time.LocalDateTime to = java.time.LocalDateTime.ofInstant(z, java.time.ZoneOffset.UTC);
            Platform.runLater(() -> {
                if (database != mdb) return;
                fromField.setRange(from, to);
                toField.setRange(from, to);
                if (fromField.isEmpty()) fromField.setValue(from);
                if (toField.isEmpty()) toField.setValue(to);
            });
        }, "statistics-time-range");
        t.setDaemon(true);
        t.start();
    }

    public void applyTheme(Theme t) {
        this.theme = t;
        setStyle(t.bgStyle());
        descriptionLabel.setTextFill(t.label);
        fromField.setFieldStyle(fieldStyle(t));
        toField.setFieldStyle(fieldStyle(t));
        fromField.setLabelColor(t.label);
        toField.setLabelColor(t.label);
        identifierField.setStyle(fieldStyle(t));
        areaField.setStyle(fieldStyle(t));
        radiusField.setStyle(fieldStyle(t));
        identifierLabel.setTextFill(t.label);
        areaLabel.setTextFill(t.label);
        radiusLabel.setTextFill(t.label);
        deviceInfoLabel.setTextFill(t.label);
        mapTransfer.setTextFill(t.label);
        runButton.setStyle(
                "-fx-background-color: " + Theme.hex(t.accent1) + ";" +
                "-fx-text-fill: "        + Theme.hex(t.bg) + ";" +
                "-fx-font-weight: bold;" +
                "-fx-background-radius: 3; -fx-border-radius: 3;" +
                "-fx-cursor: hand; -fx-padding: 4 14 4 14;");
        statusLabel.setTextFill(t.label);
    }

    private String fieldStyle(Theme t) {
        return "-fx-background-color: " + Theme.hex(t.bg) + ";" +
                "-fx-text-fill: "        + Theme.hex(t.labelStrong) + ";" +
                "-fx-border-color: "     + Theme.hex(t.border) + ";" +
                "-fx-border-width: 1px;" +
                "-fx-background-radius: 3; -fx-border-radius: 3;";
    }

    private void updateDescription() {
        AnalysisType type = typeBox.getSelectionModel().getSelectedItem();
        descriptionLabel.setText(type != null ? type.description() : "");
    }

    /**
     * Shows/hides and (re-)labels the identifier/Gebiet/Radius input fields
     * based on the currently selected {@link AnalysisType}'s
     * {@code requiresIdentifier}/{@code requiresArea} flags, and clears the
     * device-info banner from a previous run.
     */
    private void updateParamFieldsVisibility() {
        AnalysisType type = typeBox.getSelectionModel().getSelectedItem();
        boolean needsId = type != null && type.requiresIdentifier;
        boolean needsArea = type != null && type.requiresArea;

        identifierLabel.setManaged(needsId);
        identifierLabel.setVisible(needsId);
        identifierField.setManaged(needsId);
        identifierField.setVisible(needsId);
        if (!needsId) identifierField.clear();

        areaLabel.setManaged(needsArea);
        areaLabel.setVisible(needsArea);
        areaField.setManaged(needsArea);
        areaField.setVisible(needsArea);
        radiusLabel.setManaged(needsArea);
        radiusLabel.setVisible(needsArea);
        radiusField.setManaged(needsArea);
        radiusField.setVisible(needsArea);
        if (!needsArea) {
            areaField.clear();
            radiusField.clear();
        }

        deviceInfoLabel.setManaged(false);
        deviceInfoLabel.setVisible(false);
        deviceInfoLabel.setText("");

        if (type == AnalysisType.FILTER_BY_COUNTRY) {
            identifierLabel.setText(Lang.t("Landesvorwahl (z. B. 49):", "Country calling code (e.g. 49):"));
            identifierField.setPromptText("49");
        } else {
            identifierLabel.setText(Lang.t("Kennung (Rufnummer/IMSI/IMEI):", "Identifier (phone number/IMSI/IMEI):"));
            identifierField.setPromptText("491701234567");
        }
    }

    /** Renders a {@link TacLookupService.Result} (device-info hit or explanatory error) below the filter controls. */
    private void showDeviceInfo(TacLookupService.Result r) {
        deviceInfoLabel.setManaged(true);
        deviceInfoLabel.setVisible(true);
        if (r.hasDeviceInfo()) {
            String sourceLabel = "lokal".equals(r.source)
                    ? Lang.t("lokale TAC-Datenbank", "local TAC database")
                    : Lang.t(
                        "HiCellTek-Onlineabfrage, ungeprüfte Schnittstelle — siehe Javadoc von TacLookupService",
                        "HiCellTek online lookup, unverified interface — see TacLookupService's Javadoc");
            deviceInfoLabel.setText("📱 TAC " + r.tac + " (" + sourceLabel + "): "
                    + Lang.t("Hersteller=", "Manufacturer=") + (r.brand != null ? r.brand : "?")
                    + Lang.t(", Modell=", ", Model=") + (r.model != null ? r.model : "?")
                    + (r.chipsetFamily != null ? Lang.t(", Chipsatz=", ", Chipset=") + r.chipsetFamily : ""));
            deviceInfoLabel.setTextFill(theme.accent2);
        } else {
            deviceInfoLabel.setText(Lang.t("📱 Gerätelookup: ", "📱 Device lookup: ") + r.error);
            deviceInfoLabel.setTextFill(theme.detailMuted);
        }
    }

    private void setStatus(String text, Color color) {
        statusLabel.setText(text);
        statusLabel.setTextFill(color);
    }

    /**
     * Validates the optional time filter and, where required, the
     * identifier/Gebiet inputs, builds the SQL for the selected
     * {@link AnalysisType} via {@link PoliceAnalysisQueries}, executes it on
     * a background thread and renders the result. For
     * {@link AnalysisType#DEVICE_INFO_LOOKUP} it additionally fires the
     * (optional, online) {@link TacLookupService} lookup on the same
     * background thread.
     */
    private void runAnalysis() {
        AnalysisType type = typeBox.getSelectionModel().getSelectedItem();
        if (type == null || database == null) return;

        Params params = new Params();

        if (timeFilterEnabled.isSelected()) {
            params.fromIso = fromField.getText().trim();
            params.toIso = toField.getText().trim();
            if (!TIMESTAMP_PATTERN.matcher(params.fromIso).matches() || !TIMESTAMP_PATTERN.matcher(params.toIso).matches()) {
                setStatus(Lang.t(
                        "⚠️ Zeitraum bitte im Format JJJJ-MM-TT HH:MM:SS angeben",
                        "⚠️ Please enter the time range in the format YYYY-MM-DD HH:MM:SS"), theme.hover);
                return;
            }
        }

        if (type.requiresIdentifier) {
            params.identifier = identifierField.getText().trim();
            if (params.identifier.isEmpty()) {
                setStatus(Lang.t("⚠️ Bitte eine Kennung eingeben", "⚠️ Please enter an identifier"), theme.hover);
                return;
            }
        }

        if (type.requiresArea) {
            params.areaCenter = areaField.getText().trim();
            if (params.areaCenter.isEmpty()) {
                setStatus(Lang.t(
                        "⚠️ Bitte ein Gebiet (Breitengrad,Längengrad) eingeben",
                        "⚠️ Please enter an area (latitude,longitude)"), theme.hover);
                return;
            }
            String radiusText = radiusField.getText().trim();
            if (!radiusText.isEmpty()) {
                try {
                    params.radiusMeters = Double.parseDouble(radiusText);
                } catch (NumberFormatException ex) {
                    setStatus(Lang.t(
                            "⚠️ Radius bitte als Zahl in Metern angeben",
                            "⚠️ Please enter the radius as a number in meters"), theme.hover);
                    return;
                }
            }
        }

        final String sql;
        try {
            sql = PoliceAnalysisQueries.buildSql(type, params);
        } catch (IllegalArgumentException ex) {
            setStatus("⚠️ " + ex.getMessage(), theme.hover);
            return;
        }

        final String lookupIdentifier = (type == AnalysisType.DEVICE_INFO_LOOKUP) ? params.identifier : null;

        setRunning(true);
        setStatus(Lang.t("⏳ Führe Auswertung aus…", "⏳ Running analysis…"), theme.label);
        deviceInfoLabel.setManaged(false);
        deviceInfoLabel.setVisible(false);
        final QueryableDatabase db = database;
        final String preset0 = fromField.getText(), preset1 = toField.getText();
        final boolean canRestrict = true;
        final boolean transfer = mapTransfer.isSelected() && resultListener != null && ResultIds.isTransferable(type);

        Thread worker = new Thread(() -> {
            try {
                // large database opened directly: estimate and ask before a long run
                if (db instanceof fqlite.sql.ReadOnlySqliteDatabase) {
                    java.sql.Connection c = db.getConnectionObject();
                    if (tableRows < 0) tableRows = QueryCost.tableRows(c);
                    QueryCost.Estimate est = QueryCost.estimate(c, List.of(sql), tableRows, rangeCounts);
                    if (QueryCost.needsConfirmation(est)) {
                        QueryCost.Decision d = QueryCost.ask(getScene() != null ? getScene().getWindow() : null,
                                type.label(), est, canRestrict && !preset0.isEmpty() && !preset1.isEmpty()
                                        ? new String[] { preset0, preset1 } : null);
                        if (d instanceof QueryCost.Cancel) {
                            Platform.runLater(() -> {
                                setStatus(Lang.t("⏹ Nicht ausgeführt", "⏹ Not run"), theme.label);
                                setRunning(false);
                            });
                            return;
                        }
                        if (d instanceof QueryCost.Restrict r) {
                            Platform.runLater(() -> {
                                setRunning(false);
                                timeFilterEnabled.setSelected(true);
                                fromField.setDisable(false);
                                toField.setDisable(false);
                                fromField.setValue(java.time.LocalDateTime.parse(r.from().replace(' ', 'T')));
                                toField.setValue(java.time.LocalDateTime.parse(r.to().replace(' ', 'T')));
                                runAnalysis();
                            });
                            return;
                        }
                    }
                }
                ResultSet rs = db.execute(sql);
                if (rs == null) {
                    Platform.runLater(() -> {
                        setStatus(cancelled ? Lang.t("⏹ Abgebrochen", "⏹ Cancelled")
                                : Lang.t("⚠️ Kein Ergebnis", "⚠️ No result"), cancelled ? theme.label : theme.hover);
                        setRunning(false);
                    });
                    return;
                }

                ResultSetMetaData meta = rs.getMetaData();
                int cols = meta.getColumnCount();
                List<String> colNames = new java.util.ArrayList<>();
                for (int i = 1; i <= cols; i++) colNames.add(meta.getColumnName(i));

                // At most MAX_RESULT_ROWS rows are kept: pair analyses of dense
                // cell dumps return tens of millions of rows, which as table
                // rows took more than 30 GB. The remaining rows are only
                // counted (the results are sorted, the first rows are the
                // relevant ones).
                // every row also goes into a CSV file: "Export to CSV…" then
                // writes the complete result, also beyond MAX_RESULT_ROWS
                java.nio.file.Path csv = java.nio.file.Files.createTempFile("fqlite-statistics-", ".csv");
                csv.toFile().deleteOnExit();
                java.io.Writer csvOut = TableClipboardSupport.csvWriter(csv);
                csvOut.write(TableClipboardSupport.csvLine(colNames));
                List<String> csvCells = new java.util.ArrayList<>(cols);
                ObservableList<ObservableList<String>> rows = FXCollections.observableArrayList();
                ResultIds ids = transfer ? ResultIds.forResult(type, colNames, params) : null;
                long total = 0;
                while (rs.next()) {
                    if (ids != null && !ids.add(rs)) ids = null;   // too many identifiers
                    csvCells.clear();
                    for (int i = 1; i <= cols; i++) csvCells.add(rs.getString(i));
                    csvOut.write(TableClipboardSupport.csvLine(csvCells));
                    if (++total > MAX_RESULT_ROWS) continue;
                    ObservableList<String> row = FXCollections.observableArrayList();
                    for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
                    rows.add(row);
                }
                rs.close();
                csvOut.close();
                final long totalRows = total;
                final ResultIds mapIds = ids;
                final boolean tooMany = transfer && ids == null && ResultIds.isTransferable(type);

                // Online device-info lookup, if applicable, also happens on
                // this background thread (it's a blocking HTTP call).
                final TacLookupService.Result deviceInfo =
                        (lookupIdentifier != null) ? TacLookupService.lookup(lookupIdentifier) : null;

                Platform.runLater(() -> {
                    setResultCsv(csv);
                    fillResultTable(colNames, rows);
                    if (deviceInfo != null) showDeviceInfo(deviceInfo);
                    if (mapIds != null && resultListener != null && !mapIds.isEmpty()) resultListener.accept(mapIds);
                    if (totalRows > rows.size()) {
                        setStatus(String.format(Lang.t("⚠️ %,d Zeilen – angezeigt werden die ersten %,d (nach Sortierung) | %s",
                                "⚠️ %,d rows – the first %,d (in sort order) are shown | %s"), totalRows, rows.size(), type.label()), theme.hover);
                    } else {
                        setStatus("✅ " + rows.size() + " " + Lang.t("Zeile(n)", "row(s)") + " | " + type.label(), theme.accent2);
                    }
                    if (tooMany) statusLabel.setText(statusLabel.getText() + Lang.t(
                            "  |  zu viele Kennungen für die Karte", "  |  too many identifiers for the map"));
                    setRunning(false);
                });
            } catch (Exception ex) {
                boolean stop = cancelled || QueryCost.isInterrupt(ex);
                if (!stop) LOG.warning("PoliceAnalysisPane: query failed for " + type + ": " + ex.getMessage());
                Platform.runLater(() -> {
                    setStatus(stop ? Lang.t("⏹ Abgebrochen", "⏹ Cancelled") : "❌ " + ex.getMessage(),
                            stop ? theme.label : theme.hover);
                    setRunning(false);
                });
            }
        }, "police-analysis-worker");
        worker.setDaemon(true);
        worker.start();
    }

    private java.util.function.Consumer<ResultIds> resultListener;

    /** complete result of the last analysis as CSV (temporary file), or null */
    private volatile java.nio.file.Path resultCsv;
    private volatile AnalysisType resultType;

    private void setResultCsv(java.nio.file.Path csv) {
        java.nio.file.Path old = resultCsv;
        resultCsv = csv;
        resultType = typeBox.getSelectionModel().getSelectedItem();
        if (old != null && !old.equals(csv)) {
            try { java.nio.file.Files.deleteIfExists(old); } catch (java.io.IOException ignored) { }
        }
    }

    /** proposed file name for "Export to CSV…" */
    private String csvFileName() {
        AnalysisType t = resultType != null ? resultType : typeBox.getSelectionModel().getSelectedItem();
        String name = t == null ? "statistik" : t.label().replaceAll("[^\\p{L}\\p{N}]+", "_").replaceAll("^_|_$", "");
        return "lighthouse_" + name + ".csv";
    }

    /**
     * Called (on the FX thread) after an analysis whose result names
     * identifiers, if "Show result in Map & Timeline" is selected.
     */
    public void setResultListener(java.util.function.Consumer<ResultIds> listener) {
        this.resultListener = listener;
    }

    /**
     * The identifiers named by the result of an analysis, by column of
     * {@code response_records}, and its time range: the records to show in
     * "Map & Timeline" are those in the time range whose phone number, IMSI
     * or IMEI is one of them.
     */
    public static final class ResultIds {
        /** identifiers taken over at most (memory, and a map of them would be unreadable) */
        static final int MAX_IDS = 2_000_000;

        public final String label, fromIso, toIso;
        public final java.util.Set<String> msisdn = new java.util.HashSet<>(), imsi = new java.util.HashSet<>(), imei = new java.util.HashSet<>();
        /** per result column: target set, or null */
        private java.util.Set<String>[] target;
        /** per result column: values are comma-separated lists */
        private boolean[] list;
        /** column with the identifier type of a "Kennung" column (CROSS_HITS, DEVICE_SIM_SWAP), or -1 */
        private int typeCol = -1, idCol = -1;
        private final AnalysisType type;

        private ResultIds(AnalysisType type, String label, String fromIso, String toIso) {
            this.type = type;
            this.label = label;
            this.fromIso = fromIso;
            this.toIso = toIso;
        }

        /** false for overview analyses whose result does not single out identifiers */
        static boolean isTransferable(AnalysisType type) {
            return switch (type) {
                case ALL_NUMBERS, DOMESTIC_FOREIGN, PARTY_ROLE, PARTICIPANTS_PER_CELL, CONNECTION_TYPE,
                     CELL_SITES, IDENTIFIER_INVENTORY, COUNTRY_HISTOGRAM -> false;
                default -> true;
            };
        }

        @SuppressWarnings("unchecked")
        static ResultIds forResult(AnalysisType type, List<String> cols, Params params) {
            ResultIds r = new ResultIds(type, type.label(), params.fromIso, params.toIso);
            r.target = new java.util.Set[cols.size()];
            r.list = new boolean[cols.size()];
            for (int i = 0; i < cols.size(); i++) {
                String c = cols.get(i);
                switch (c) {
                    case "IMSI", "Begleiter_IMSI", "Companion_IMSI", "Gegenpart_IMSI", "Counterpart_IMSI" -> r.target[i] = r.imsi;
                    case "IMEI", "Begleiter_IMEI", "Companion_IMEI", "Gegenpart_IMEI", "Counterpart_IMEI" -> r.target[i] = r.imei;
                    case "Rufnummer", "Phone_Number", "Rufnummer_A", "Rufnummer_B", "Phone_Number_A", "Phone_Number_B",
                         "Begleiter_Rufnummer", "Companion_Phone_Number", "Gegenpart_Rufnummer", "Counterpart_Phone_Number" -> r.target[i] = r.msisdn;
                    case "Rufnummern", "Phone_Numbers", "Anrufer", "Callers" -> { r.target[i] = r.msisdn; r.list[i] = true; }
                    case "Angerufener", "Called_Party" -> r.target[i] = r.msisdn;
                    case "Kennung", "Identifier" -> r.idCol = i;
                    case "Kennungstyp", "Identifier_Type", "Wechseltyp", "Swap_Type" -> r.typeCol = i;
                    default -> { }
                }
            }
            // Wanderer: the IMSI is the subscriber found; its phone number would add other SIMs
            if (type == AnalysisType.ROAMERS) {
                for (int i = 0; i < r.target.length; i++) if (r.target[i] != r.imsi) r.target[i] = null;
            }
            // the entered identifier belongs to the result (its own records)
            if (params.identifier != null && !params.identifier.isBlank() && type != AnalysisType.FILTER_BY_COUNTRY) {
                String id = params.identifier.trim();
                r.msisdn.add(id);
                r.imsi.add(id);
                r.imei.add(id);
            }
            return r;
        }

        /** adds the identifiers of the current row; false if there are too many */
        boolean add(ResultSet rs) throws java.sql.SQLException {
            for (int i = 0; i < target.length; i++) {
                if (target[i] == null) continue;
                String v = rs.getString(i + 1);
                if (v == null || v.isEmpty()) continue;
                if (list[i]) {
                    for (String p : v.split(",")) if (!p.isEmpty()) target[i].add(p);
                } else {
                    target[i].add(v);
                }
            }
            if (idCol >= 0) {
                String v = rs.getString(idCol + 1);
                String t = typeCol >= 0 ? rs.getString(typeCol + 1) : null;
                if (v != null && !v.isEmpty() && t != null) {
                    if (t.startsWith("IMSI") || t.startsWith("Geräte-Wechsel") || t.startsWith("Device swap")) imsi.add(v);
                    else if (t.startsWith("IMEI") || t.startsWith("SIM")) imei.add(v);
                    else if (t.startsWith("Rufnummer") || t.startsWith("Phone")) msisdn.add(v);
                }
            }
            return msisdn.size() + imsi.size() + imei.size() <= MAX_IDS;
        }

        public boolean isEmpty() { return msisdn.isEmpty() && imsi.isEmpty() && imei.isEmpty(); }

        /** number of identifiers */
        public int size() { return msisdn.size() + imsi.size() + imei.size(); }
    }

    /** result rows kept for the table at most (the rest is only counted) */
    static final int MAX_RESULT_ROWS = 100_000;

    /** an analysis is running (the run button then cancels it) */
    private volatile boolean running;
    private volatile boolean cancelled;
    private volatile long tableRows = -1;
    private final java.util.Map<String, Long> rangeCounts = QueryCost.newRangeCache();

    private void setRunning(boolean on) {
        running = on;
        if (on) cancelled = false;
        runButton.setDisable(database == null);
        runButton.setText(on ? Lang.t("■ Abbrechen", "■ Cancel") : Lang.t("▶ Ausführen", "▶ Run"));
    }

    /** Stops the running analysis (interrupts its statement). */
    private void cancelAnalysis() {
        cancelled = true;
        setStatus(Lang.t("⏹ Wird abgebrochen…", "⏹ Cancelling…"), theme.label);
        if (database != null) QueryCost.interrupt(database.getConnectionObject());
    }

    /** Rebuilds {@link #resultTable}'s columns from scratch — query shapes differ per analysis type. */
    private void fillResultTable(List<String> columns, ObservableList<ObservableList<String>> rows) {
        currentColumnNames = columns;
        resultTable.getColumns().clear();
        resultTable.getItems().clear();

        for (int i = 0; i < columns.size(); i++) {
            final int idx = i;
            TableColumn<ObservableList<String>, String> col = new TableColumn<>(columns.get(i));
            col.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(
                    idx < cd.getValue().size() ? cd.getValue().get(idx) : ""));
            col.setPrefWidth(140);
            resultTable.getColumns().add(col);
        }
        resultTable.setItems(rows);
    }

    /**
     * Handles "Auf Karte anzeigen" for the right-clicked result row: looks up
     * the shared cell-site column (see {@link PoliceAnalysisQueries#isCellSiteColumn})
     * that every coordinate-bearing {@link PoliceAnalysisQueries} template
     * uses, and forwards a coordinate to {@link #mapCenterListener}. Two
     * cell formats are supported:
     * <ul>
     *   <li>a single site, {@code "<lat>, <lon>"} (most analyses); or</li>
     *   <li>several sites, {@code "<lat>|<lon>,<lat>|<lon>,..."} (e.g.
     *       {@link PoliceAnalysisQueries.AnalysisType#ROAMERS}'s
     *       "Funkzellen"/"Cell_Sites" column) — the first listed site is used.</li>
     * </ul>
     * Shows a status hint instead of failing silently if the current
     * analysis has no such column or the row has no coordinate.
     */
    private void showSelectedRowOnMap(ObservableList<String> rowData) {
        if (rowData == null || mapCenterListener == null) return;

        int idx = -1;
        for (int i = 0; i < currentColumnNames.size(); i++) {
            if (PoliceAnalysisQueries.isCellSiteColumn(currentColumnNames.get(i))) { idx = i; break; }
        }
        if (idx < 0 || idx >= rowData.size()) {
            setStatus(Lang.t(
                    "⚠️ Diese Auswertung enthält keine Funkzellen-Koordinate",
                    "⚠️ This analysis has no cell-site coordinate"), theme.hover);
            return;
        }

        String cell = rowData.get(idx);
        if (cell == null || cell.isBlank()) {
            setStatus(Lang.t("⚠️ Keine Koordinate in dieser Zeile", "⚠️ No coordinate in this row"), theme.hover);
            return;
        }

        // Multi-site cell (e.g. ROAMERS' "Funkzellen"/"Cell_Sites" list):
        // take the first "<lat>|<lon>" entry before the first ',' that
        // separates sites.
        boolean multiSite = cell.indexOf('|') >= 0;
        String first = multiSite ? cell.split(",")[0] : cell;
        String[] parts = first.split(multiSite ? "\\|" : ",");
        if (parts.length != 2) {
            setStatus(Lang.t(
                    "⚠️ Koordinate konnte nicht gelesen werden: ",
                    "⚠️ Could not read coordinate: ") + cell, theme.hover);
            return;
        }
        try {
            double lat = Double.parseDouble(parts[0].trim());
            double lon = Double.parseDouble(parts[1].trim());
            mapCenterListener.accept(lat, lon);
            if (multiSite) {
                setStatus(Lang.t(
                        "📍 Erste von mehreren Funkzellen angezeigt: ",
                        "📍 Showing first of several cell sites: ") + first.replace("|", ", "), theme.accent2);
            }

            // If this analysis has an "IMSI" column, also select it in the
            // map's path overlay — relevant for ROAMERS and similar
            // multi-cell results, where the single coordinate above is only
            // ever the first stop.
            int imsiIdx = currentColumnNames.indexOf("IMSI");
            if (imsiIdx >= 0 && imsiIdx < rowData.size() && imsiSelectListener != null) {
                String imsi = rowData.get(imsiIdx);
                if (imsi != null && !imsi.isBlank()) imsiSelectListener.accept(imsi);
            }
        } catch (NumberFormatException ex) {
            setStatus(Lang.t(
                    "⚠️ Koordinate konnte nicht gelesen werden: ",
                    "⚠️ Could not read coordinate: ") + cell, theme.hover);
        }
    }
}
