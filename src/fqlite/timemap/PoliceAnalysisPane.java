package fqlite.timemap;

import fqlite.sql.InMemoryDatabase;
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
 * {@link InMemoryDatabase}. Results are shown in a generic table whose
 * columns are built dynamically from the query's {@link ResultSetMetaData},
 * the same approach used by {@code fqlite.sql.SQLParser#fillTable}.</p>
 */
public class PoliceAnalysisPane extends BorderPane {

    private static final Logger LOG = Logger.getLogger(PoliceAnalysisPane.class.getName());

    private static final Pattern TIMESTAMP_PATTERN =
            Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");

    private final ComboBox<AnalysisType> typeBox = new ComboBox<>();
    private final Label descriptionLabel = new Label();

    private final CheckBox timeFilterEnabled = new CheckBox("Zeitraum einschränken");
    private final TextField fromField = new TextField();
    private final TextField toField   = new TextField();

    // Identifier/Gebiet-Eingabe — nur sichtbar/benötigt für Auswertungen mit
    // AnalysisType#requiresIdentifier bzw. #requiresArea, siehe
    // updateParamFieldsVisibility().
    private final Label identifierLabel = new Label("Kennung (Rufnummer/IMSI/IMEI):");
    private final TextField identifierField = new TextField();
    private final Label areaLabel = new Label("Gebiet (Breitengrad,Längengrad):");
    private final TextField areaField = new TextField();
    private final Label radiusLabel = new Label("Radius (m):");
    private final TextField radiusField = new TextField();

    /** Shows the (optional, online) HiCellTek device-info result for DEVICE_INFO_LOOKUP — hidden otherwise. */
    private final Label deviceInfoLabel = new Label();

    private final Button runButton = new Button("▶ Ausführen");
    private final TableView<ObservableList<String>> resultTable = new TableView<>();
    private final Label statusLabel = new Label("⚪ Keine Datenbank verbunden");

    private InMemoryDatabase inMemoryDB = null;
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

        Label title = new Label("🔎 Polizeiliche Auswertung");
        title.setFont(Font.font("Monospace", FontWeight.BOLD, 14));

        typeBox.getItems().addAll(AnalysisType.values());
        typeBox.getSelectionModel().selectFirst();
        typeBox.setMaxWidth(Double.MAX_VALUE);
        typeBox.setOnAction(e -> {
            updateDescription();
            updateParamFieldsVisibility();
        });

        descriptionLabel.setWrapText(true);
        descriptionLabel.setFont(Font.font("Monospace", 10));
        updateDescription();

        fromField.setPromptText("JJJJ-MM-TT HH:MM:SS (von)");
        toField.setPromptText("JJJJ-MM-TT HH:MM:SS (bis)");
        fromField.setFont(Font.font("Monospace", 11));
        toField.setFont(Font.font("Monospace", 11));
        fromField.setDisable(true);
        toField.setDisable(true);

        timeFilterEnabled.setOnAction(e -> {
            boolean on = timeFilterEnabled.isSelected();
            fromField.setDisable(!on);
            toField.setDisable(!on);
        });

        identifierField.setFont(Font.font("Monospace", 11));
        areaField.setFont(Font.font("Monospace", 11));
        areaField.setPromptText("52.5200,13.4050");
        radiusField.setFont(Font.font("Monospace", 11));
        radiusField.setPromptText("500");
        radiusField.setPrefWidth(70);
        identifierLabel.setFont(Font.font("Monospace", 10));
        areaLabel.setFont(Font.font("Monospace", 10));
        radiusLabel.setFont(Font.font("Monospace", 10));

        deviceInfoLabel.setWrapText(true);
        deviceInfoLabel.setFont(Font.font("Monospace", 10));
        deviceInfoLabel.setManaged(false);
        deviceInfoLabel.setVisible(false);

        runButton.setFont(Font.font("Monospace", FontWeight.BOLD, 11));
        runButton.setDisable(true);   // enabled by setDatabase()
        runButton.setOnAction(e -> runAnalysis());

        HBox timeBox = new HBox(8, timeFilterEnabled, fromField, new Label("→"), toField);
        timeBox.setAlignment(Pos.CENTER_LEFT);

        HBox paramBox = new HBox(8, identifierLabel, identifierField, areaLabel, areaField, radiusLabel, radiusField);
        paramBox.setAlignment(Pos.CENTER_LEFT);
        HBox.setHgrow(identifierField, Priority.ALWAYS);

        HBox controls = new HBox(10, typeBox, runButton);
        controls.setAlignment(Pos.CENTER_LEFT);
        HBox.setHgrow(typeBox, Priority.ALWAYS);

        VBox top = new VBox(8, title, controls, descriptionLabel, timeBox, paramBox, deviceInfoLabel);
        top.setPadding(new Insets(12, 16, 12, 16));
        setTop(top);

        updateParamFieldsVisibility();

        resultTable.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);
        resultTable.setPlaceholder(new Label("Auswertungstyp wählen und auf \"Ausführen\" klicken."));
        resultTable.setRowFactory(tv -> {
            TableRow<ObservableList<String>> row = new TableRow<>();
            MenuItem showOnMapItem = new MenuItem("📍 Auf Karte anzeigen");
            showOnMapItem.setOnAction(e -> showSelectedRowOnMap(row.getItem()));
            ContextMenu rowMenu = new ContextMenu(showOnMapItem);
            row.contextMenuProperty().bind(
                    Bindings.when(row.emptyProperty()).then((ContextMenu) null).otherwise(rowMenu));
            return row;
        });
        setCenter(resultTable);

        statusLabel.setFont(Font.font("Monospace", 10));
        statusLabel.setPadding(new Insets(6, 16, 8, 16));
        setBottom(statusLabel);

        applyTheme(theme);
    }

    /** Connects the in-memory database that backs the current case. Call this whenever the underlying data changes. */
    public void setDatabase(InMemoryDatabase mdb) {
        this.inMemoryDB = mdb;
        Platform.runLater(() -> {
            runButton.setDisable(mdb == null);
            setStatus(mdb == null ? "⚪ Keine Datenbank verbunden" : "✅ Bereit", mdb == null ? theme.detailMuted : theme.accent2);
        });
    }

    public void applyTheme(Theme t) {
        this.theme = t;
        setStyle(t.bgStyle());
        descriptionLabel.setTextFill(t.label);
        fromField.setStyle(fieldStyle(t));
        toField.setStyle(fieldStyle(t));
        identifierField.setStyle(fieldStyle(t));
        areaField.setStyle(fieldStyle(t));
        radiusField.setStyle(fieldStyle(t));
        identifierLabel.setTextFill(t.label);
        areaLabel.setTextFill(t.label);
        radiusLabel.setTextFill(t.label);
        deviceInfoLabel.setTextFill(t.label);
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
        descriptionLabel.setText(type != null ? type.description : "");
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
            identifierLabel.setText("Landesvorwahl (z. B. 49):");
            identifierField.setPromptText("49");
        } else {
            identifierLabel.setText("Kennung (Rufnummer/IMSI/IMEI):");
            identifierField.setPromptText("491701234567");
        }
    }

    /** Renders a {@link TacLookupService.Result} (device-info hit or explanatory error) below the filter controls. */
    private void showDeviceInfo(TacLookupService.Result r) {
        deviceInfoLabel.setManaged(true);
        deviceInfoLabel.setVisible(true);
        if (r.hasDeviceInfo()) {
            String sourceLabel = "lokal".equals(r.source)
                    ? "lokale TAC-Datenbank"
                    : "HiCellTek-Onlineabfrage, ungeprüfte Schnittstelle — siehe Javadoc von TacLookupService";
            deviceInfoLabel.setText("📱 TAC " + r.tac + " (" + sourceLabel + "): "
                    + "Hersteller=" + (r.brand != null ? r.brand : "?")
                    + ", Modell=" + (r.model != null ? r.model : "?")
                    + (r.chipsetFamily != null ? ", Chipsatz=" + r.chipsetFamily : ""));
            deviceInfoLabel.setTextFill(theme.accent2);
        } else {
            deviceInfoLabel.setText("📱 Gerätelookup: " + r.error);
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
        if (type == null || inMemoryDB == null) return;

        Params params = new Params();

        if (timeFilterEnabled.isSelected()) {
            params.fromIso = fromField.getText().trim();
            params.toIso = toField.getText().trim();
            if (!TIMESTAMP_PATTERN.matcher(params.fromIso).matches() || !TIMESTAMP_PATTERN.matcher(params.toIso).matches()) {
                setStatus("⚠️ Zeitraum bitte im Format JJJJ-MM-TT HH:MM:SS angeben", theme.hover);
                return;
            }
        }

        if (type.requiresIdentifier) {
            params.identifier = identifierField.getText().trim();
            if (params.identifier.isEmpty()) {
                setStatus("⚠️ Bitte eine Kennung eingeben", theme.hover);
                return;
            }
        }

        if (type.requiresArea) {
            params.areaCenter = areaField.getText().trim();
            if (params.areaCenter.isEmpty()) {
                setStatus("⚠️ Bitte ein Gebiet (Breitengrad,Längengrad) eingeben", theme.hover);
                return;
            }
            String radiusText = radiusField.getText().trim();
            if (!radiusText.isEmpty()) {
                try {
                    params.radiusMeters = Double.parseDouble(radiusText);
                } catch (NumberFormatException ex) {
                    setStatus("⚠️ Radius bitte als Zahl in Metern angeben", theme.hover);
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

        runButton.setDisable(true);
        setStatus("⏳ Führe Auswertung aus…", theme.label);
        deviceInfoLabel.setManaged(false);
        deviceInfoLabel.setVisible(false);

        Thread worker = new Thread(() -> {
            try {
                ResultSet rs = inMemoryDB.execute(sql);
                if (rs == null) {
                    Platform.runLater(() -> {
                        setStatus("⚠️ Kein Ergebnis", theme.hover);
                        runButton.setDisable(false);
                    });
                    return;
                }

                ResultSetMetaData meta = rs.getMetaData();
                int cols = meta.getColumnCount();
                List<String> colNames = new java.util.ArrayList<>();
                for (int i = 1; i <= cols; i++) colNames.add(meta.getColumnName(i));

                ObservableList<ObservableList<String>> rows = FXCollections.observableArrayList();
                while (rs.next()) {
                    ObservableList<String> row = FXCollections.observableArrayList();
                    for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
                    rows.add(row);
                }
                rs.close();

                // Online device-info lookup, if applicable, also happens on
                // this background thread (it's a blocking HTTP call).
                final TacLookupService.Result deviceInfo =
                        (lookupIdentifier != null) ? TacLookupService.lookup(lookupIdentifier) : null;

                Platform.runLater(() -> {
                    fillResultTable(colNames, rows);
                    if (deviceInfo != null) showDeviceInfo(deviceInfo);
                    setStatus("✅ " + rows.size() + " Zeile(n) | " + type.label, theme.accent2);
                    runButton.setDisable(false);
                });
            } catch (Exception ex) {
                LOG.warning("PoliceAnalysisPane: query failed for " + type + ": " + ex.getMessage());
                Platform.runLater(() -> {
                    setStatus("❌ " + ex.getMessage(), theme.hover);
                    runButton.setDisable(false);
                });
            }
        }, "police-analysis-worker");
        worker.setDaemon(true);
        worker.start();
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
     * a "Funkzelle"-named column (the convention every coordinate-bearing
     * {@link PoliceAnalysisQueries} template uses) and forwards a coordinate
     * to {@link #mapCenterListener}. Two cell formats are supported:
     * <ul>
     *   <li>a single site, {@code "<lat>, <lon>"} (most analyses); or</li>
     *   <li>several sites, {@code "<lat>|<lon>,<lat>|<lon>,..."} (e.g.
     *       {@link PoliceAnalysisQueries.AnalysisType#ROAMERS}'s "Funkzellen"
     *       column) — the first listed site is used.</li>
     * </ul>
     * Shows a status hint instead of failing silently if the current
     * analysis has no such column or the row has no coordinate.
     */
    private void showSelectedRowOnMap(ObservableList<String> rowData) {
        if (rowData == null || mapCenterListener == null) return;

        int idx = -1;
        for (int i = 0; i < currentColumnNames.size(); i++) {
            if (currentColumnNames.get(i).startsWith("Funkzelle")) { idx = i; break; }
        }
        if (idx < 0 || idx >= rowData.size()) {
            setStatus("⚠️ Diese Auswertung enthält keine Funkzellen-Koordinate", theme.hover);
            return;
        }

        String cell = rowData.get(idx);
        if (cell == null || cell.isBlank()) {
            setStatus("⚠️ Keine Koordinate in dieser Zeile", theme.hover);
            return;
        }

        // Multi-site cell (e.g. ROAMERS' "Funkzellen" list): take the first
        // "<lat>|<lon>" entry before the first ',' that separates sites.
        boolean multiSite = cell.indexOf('|') >= 0;
        String first = multiSite ? cell.split(",")[0] : cell;
        String[] parts = first.split(multiSite ? "\\|" : ",");
        if (parts.length != 2) {
            setStatus("⚠️ Koordinate konnte nicht gelesen werden: " + cell, theme.hover);
            return;
        }
        try {
            double lat = Double.parseDouble(parts[0].trim());
            double lon = Double.parseDouble(parts[1].trim());
            mapCenterListener.accept(lat, lon);
            if (multiSite) {
                setStatus("📍 Erste von mehreren Funkzellen angezeigt: " + first.replace("|", ", "), theme.accent2);
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
            setStatus("⚠️ Koordinate konnte nicht gelesen werden: " + cell, theme.hover);
        }
    }
}
