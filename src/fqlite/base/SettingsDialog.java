package fqlite.base;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.logging.Level;
import fqlite.log.AppLog;
import fqlite.timemap.BeaconDbClient;
import fqlite.timemap.CellTowerResolver;
import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.RadioButton;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TextField;
import javafx.scene.control.ToggleGroup;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.scene.text.TextAlignment;
import javafx.stage.Stage;


public class SettingsDialog extends Application {

    final RadioButton r1 = new RadioButton("don't export any BLOBs");
    final RadioButton r3 = new RadioButton("export BLOB values as separate files");
    final ChoiceBox<String> loglevel = new ChoiceBox<>();

    private static final long PREVIEW_EPOCH_SECONDS = 1_682_694_647L;

    @Override
    public void start(final Stage stage) {
        stage.setTitle("FQLite Settings");

        final String cssLayout =
                "-fx-border-color: black;\n" +
                "-fx-border-insets: 5;\n" +
                "-fx-padding: 10;\n" +
                "-fx-spacing: 10;\n" +
                "-fx-border-width: 1;\n";

        // =====================================================================
        // TAB 1 – Common
        // =====================================================================

        VBox commonContent = new VBox(10);
        commonContent.setPadding(new Insets(10, 8, 10, 8));

        // ── CSV & HTML Export ────────────────────────────────────────────────
        Label heading = new Label("CSV & HTML Export");
        heading.setFont(Font.font("Verdana", FontWeight.BOLD, 12));
        heading.setTextAlignment(TextAlignment.CENTER);

        ToggleGroup toggleGroup = new ToggleGroup();
        r1.setToggleGroup(toggleGroup);
        r3.setToggleGroup(toggleGroup);

        VBox fileexportproperties = new VBox();
        fileexportproperties.setStyle(cssLayout);
        fileexportproperties.getChildren().addAll(r1, r3);
        if (Global.EXPORT_MODE == Global.EXPORT_MODES.DONTEXPORT) r1.setSelected(true);
        else if (Global.EXPORT_MODE == Global.EXPORT_MODES.TOSEPARATEFILES) r3.setSelected(true);

        // ── CSV Settings ─────────────────────────────────────────────────────
        Label heading2 = new Label("CSV Settings");
        heading2.setFont(Font.font("Verdana", FontWeight.BOLD, 12));

        CheckBox exportTblHeader = new CheckBox("always write table header to .csv");
        exportTblHeader.setSelected(Global.EXPORTTABLEHEADER);

        ChoiceBox<String> choiceBox = new ChoiceBox<>();
        choiceBox.getItems().addAll(",", ";", "[TAB]");
        choiceBox.setTooltip(new Tooltip("Select a separator for .csv export"));
        choiceBox.getSelectionModel().select(Global.CSV_SEPARATOR);

        VBox exportBox = new VBox();
        exportBox.setPadding(new Insets(5));
        exportBox.setSpacing(8);
        exportBox.setStyle(cssLayout);
        HBox sepRow = new HBox(new Label("Separator: "), choiceBox);
        sepRow.setSpacing(10);
        sepRow.setPadding(new Insets(4, 0, 0, 0));
        exportBox.getChildren().addAll(exportTblHeader, sepRow);

        // ── Log Settings ─────────────────────────────────────────────────────
        Label heading3 = new Label("Log Settings");
        heading3.setFont(Font.font("Verdana", FontWeight.BOLD, 12));

        loglevel.getItems().addAll(
                Level.SEVERE.toString(), Level.WARNING.toString(),
                Level.INFO.toString(), Level.FINEST.toString());
        loglevel.setTooltip(new Tooltip("Select a LOG Level"));
        loglevel.getSelectionModel().select(Global.LOGLEVEL.toString());

        HBox loggrp = new HBox(new Label("Level: "), loglevel);
        loggrp.setPadding(new Insets(5, 5, 5, 0));
        loggrp.setSpacing(10);
        loggrp.setStyle(cssLayout);

        // ── Timestamp Format ──────────────────────────────────────────────────
        Label heading4 = new Label("Timestamp Format");
        heading4.setFont(Font.font("Verdana", FontWeight.BOLD, 12));

        VBox tsBox = new VBox();
        tsBox.setPadding(new Insets(5));
        tsBox.setSpacing(6);
        tsBox.setStyle(cssLayout);

        TextField tsPatternField = new TextField(Global.TIMESTAMP_FORMAT);
        tsPatternField.setPrefWidth(210);
        tsPatternField.setTooltip(new Tooltip(
                "Unicode CLDR date/time pattern\n" +
                "e.g.  yyyy-MM-dd HH:mm:ss Z\n" +
                "      MM/dd/yyyy - HH:mm:ss zzzz"));
        HBox tsPatternRow = new HBox(new Label("Pattern:"), tsPatternField);
        tsPatternRow.setSpacing(8);
        tsPatternRow.setAlignment(Pos.CENTER_LEFT);

        Label tsPreviewValue = new Label();
        tsPreviewValue.setStyle("-fx-font-family: monospace;");
        HBox tsPreviewRow = new HBox(new Label("Preview:"), tsPreviewValue);
        tsPreviewRow.setSpacing(8);
        tsPreviewRow.setAlignment(Pos.CENTER_LEFT);

        ToggleGroup tzGroup = new ToggleGroup();
        RadioButton tsUTC   = new RadioButton("UTC");
        RadioButton tsLocal = new RadioButton("Local  (" + ZoneId.systemDefault().getId() + ")");
        tsUTC.setToggleGroup(tzGroup);
        tsLocal.setToggleGroup(tzGroup);
        if (Global.TIMESTAMP_USE_UTC) tsUTC.setSelected(true); else tsLocal.setSelected(true);
        HBox tzRow = new HBox(tsUTC, tsLocal);
        tzRow.setSpacing(14);
        tzRow.setAlignment(Pos.CENTER_LEFT);

        Runnable updatePreview = () -> {
            String pat = tsPatternField.getText().trim();
            ZoneId zone = tsUTC.isSelected() ? ZoneOffset.UTC : ZoneId.systemDefault();
            try {
                tsPreviewValue.setText(ZonedDateTime
                        .ofInstant(Instant.ofEpochSecond(PREVIEW_EPOCH_SECONDS), zone)
                        .format(DateTimeFormatter.ofPattern(pat)));
                tsPreviewValue.setTextFill(Color.BLACK);
                tsPatternField.setStyle("");
            } catch (IllegalArgumentException ex) {
                tsPreviewValue.setText("Invalid pattern");
                tsPreviewValue.setTextFill(Color.RED);
                tsPatternField.setStyle("-fx-border-color: red;");
            }
        };
        tsPatternField.textProperty().addListener((o, a, b) -> updatePreview.run());
        tzGroup.selectedToggleProperty().addListener((o, a, b) -> updatePreview.run());
        updatePreview.run();

        Button btnISO = new Button("ISO  (yyyy-MM-dd HH:mm:ss Z)");
        Button btnUS  = new Button("US   (MM/dd/yyyy - HH:mm:ss Z)");
        btnISO.setStyle("-fx-font-size: 10;");
        btnUS .setStyle("-fx-font-size: 10;");
        btnISO.setOnAction(e -> tsPatternField.setText("yyyy-MM-dd HH:mm:ss Z"));
        btnUS .setOnAction(e -> tsPatternField.setText("MM/dd/yyyy - HH:mm:ss Z"));
        HBox tsQuickRow = new HBox(6, btnISO, btnUS);

        tsBox.getChildren().addAll(tsPatternRow, tsPreviewRow, tzRow, tsQuickRow);

        commonContent.getChildren().addAll(
                heading,  fileexportproperties,
                heading2, exportBox,
                heading3, loggrp,
                heading4, tsBox);

        ScrollPane commonScroll = new ScrollPane(commonContent);
        commonScroll.setFitToWidth(true);

        Tab commonTab = new Tab("Common", commonScroll);
        commonTab.setClosable(false);

        // =====================================================================
        // TAB 2 – Location
        // =====================================================================

        VBox locationContent = new VBox(10);
        locationContent.setPadding(new Insets(10, 8, 10, 8));

        // ── Offline-Karten (MBTiles) ──────────────────────────────────────────
        Label headingMBT = new Label("Offline-Karten (MBTiles)");
        headingMBT.setFont(Font.font("Verdana", FontWeight.BOLD, 12));

        VBox mbtBox = new VBox();
        mbtBox.setPadding(new Insets(5));
        mbtBox.setSpacing(6);
        mbtBox.setStyle(cssLayout);

        // Current path from TileCache (may be null)
        String currentMbPath = "";
        try {
            java.nio.file.Path mp = fqlite.timemap.TileCache.getInstance().getMbTilesPath();
            if (mp != null) currentMbPath = mp.toString();
        } catch (Exception ignored) {}

        TextField mbtField = new TextField(currentMbPath);
        mbtField.setPrefWidth(210);
        mbtField.setPromptText("Pfad zur .mbtiles-Datei");
        mbtField.setTooltip(new Tooltip(
                "Offline-Kartendaten im MBTiles-Format.\n" +
                "Download z.B. von maptiler.com oder openfreemap.org.\n" +
                "Wenn gesetzt, werden Kartenkacheln zuerst aus dieser\n" +
                "Datei geladen (kein Internetzugriff nötig)."));
        Button mbtBrowseBtn = new Button("Durchsuchen…");
        mbtBrowseBtn.setOnAction(e -> {
            javafx.stage.FileChooser fc = new javafx.stage.FileChooser();
            fc.setTitle("MBTiles-Datei öffnen");
            fc.getExtensionFilters().addAll(
                    new javafx.stage.FileChooser.ExtensionFilter("MBTiles", "*.mbtiles"),
                    new javafx.stage.FileChooser.ExtensionFilter("Alle Dateien", "*.*"));
            if (!mbtField.getText().isBlank()) {
                java.io.File cur = new java.io.File(mbtField.getText()).getParentFile();
                if (cur != null && cur.exists()) fc.setInitialDirectory(cur);
            }
            java.io.File chosen = fc.showOpenDialog(stage);
            if (chosen != null) mbtField.setText(chosen.getAbsolutePath());
        });
        Button mbtClearBtn = new Button("Entfernen");
        mbtClearBtn.setStyle("-fx-font-size: 10;");
        mbtClearBtn.setOnAction(e -> mbtField.setText(""));

        HBox mbtPathRow   = new HBox(8, new Label("Datei:"), mbtField, mbtBrowseBtn);
        mbtPathRow.setAlignment(Pos.CENTER_LEFT);
        HBox mbtButtonRow = new HBox(8, mbtClearBtn);
        mbtButtonRow.setAlignment(Pos.CENTER_LEFT);

        Label mbtInfoLabel = new Label(
                "Download: maptiler.com  ·  openfreemap.org  ·  openmaptiles.org");
        mbtInfoLabel.setStyle("-fx-font-size: 10; -fx-text-fill: #0078d4;");

        mbtBox.getChildren().addAll(mbtPathRow, mbtButtonRow, mbtInfoLabel);

        // ── OpenCelliD ───────────────────────────────────────────────────────
        Label headingOCI = new Label("OpenCelliD");
        headingOCI.setFont(Font.font("Verdana", FontWeight.BOLD, 12));

        VBox cellBox = new VBox();
        cellBox.setPadding(new Insets(5));
        cellBox.setSpacing(6);
        cellBox.setStyle(cssLayout);

        TextField cellCsvField = new TextField(
                CellTowerResolver.getInstance().getCsvPath() != null
                        ? CellTowerResolver.getInstance().getCsvPath().toString() : "");
        cellCsvField.setPrefWidth(210);
        cellCsvField.setPromptText("Pfad zur OpenCelliD-CSV (z.B. 262.csv)");
        cellCsvField.setTooltip(new Tooltip(
                "Länderspezifische OpenCelliD-CSV-Datei.\n" +
                "Download: opencellid.org/downloads.php\n" +
                "Für Deutschland: 262.csv  (~18 MB, 244 000 Masten)\n" +
                "Unterstützt auch .csv.gz (komprimiert)."));
        Button cellBrowseBtn = new Button("Durchsuchen…");
        cellBrowseBtn.setOnAction(e -> {
            javafx.stage.FileChooser fc = new javafx.stage.FileChooser();
            fc.setTitle("OpenCelliD CSV-Datei öffnen");
            fc.getExtensionFilters().addAll(
                    new javafx.stage.FileChooser.ExtensionFilter("CSV-Dateien", "*.csv", "*.csv.gz"),
                    new javafx.stage.FileChooser.ExtensionFilter("Alle Dateien", "*.*"));
            if (!cellCsvField.getText().isBlank()) {
                java.io.File cur = new java.io.File(cellCsvField.getText()).getParentFile();
                if (cur != null && cur.exists()) fc.setInitialDirectory(cur);
            }
            java.io.File chosen = fc.showOpenDialog(stage);
            if (chosen != null) cellCsvField.setText(chosen.getAbsolutePath());
        });
        HBox cellCsvRow = new HBox(8, new Label("CSV-Datei:"), cellCsvField, cellBrowseBtn);
        cellCsvRow.setAlignment(Pos.CENTER_LEFT);

        int     cachedCount = CellTowerResolver.getInstance().getCachedTowerCount();
        boolean csvLoaded   = CellTowerResolver.getInstance().isCsvLoaded();
        Label cellStatusLabel = new Label(
                csvLoaded
                        ? "\u2705 " + String.format("%,d", cachedCount) + " Masten geladen"
                        : CellTowerResolver.getInstance().isCsvConfigured()
                        ? "\u23F3 CSV konfiguriert, noch nicht geladen"
                        : "\u26AA Keine CSV konfiguriert");
        cellStatusLabel.setStyle("-fx-font-size: 10; -fx-font-family: monospace;");

        Button btnClearCache = new Button("Index leeren");
        btnClearCache.setStyle("-fx-font-size: 10;");
        btnClearCache.setTooltip(new Tooltip(
                "Entfernt den geladenen Index aus dem RAM.\n" +
                "Die CSV-Datei bleibt erhalten."));
        btnClearCache.setOnAction(e -> {
            CellTowerResolver.getInstance().clearCache();
            cellStatusLabel.setText("\u26AA Index geleert");
        });

        Button btnPreload = new Button("Jetzt laden");
        btnPreload.setStyle("-fx-font-size: 10;");
        btnPreload.setTooltip(new Tooltip("CSV sofort in den Speicher laden."));
        btnPreload.setOnAction(e -> {
            String p = cellCsvField.getText().trim();
            if (!p.isBlank()) {
                CellTowerResolver.getInstance().setCsvPath(java.nio.file.Path.of(p));
                CellTowerResolver.getInstance().loadAsync();
                cellStatusLabel.setText("\u23F3 Wird geladen\u2026");
            }
        });

        Label cellInfoLabel = new Label(
                "Download: opencellid.org/downloads.php  (kostenlos nach Registrierung)");
        cellInfoLabel.setStyle("-fx-font-size: 10; -fx-text-fill: #0078d4;");

        HBox cellButtonRow = new HBox(8, btnPreload, btnClearCache, cellStatusLabel);
        cellButtonRow.setAlignment(Pos.CENTER_LEFT);
        cellBox.getChildren().addAll(cellCsvRow, cellButtonRow, cellInfoLabel);

        // ── beaconDB ──────────────────────────────────────────────────────────
        Label headingBDB = new Label("beaconDB (Online-Fallback)");
        headingBDB.setFont(Font.font("Verdana", FontWeight.BOLD, 12));

        VBox beaconBox = new VBox();
        beaconBox.setPadding(new Insets(5));
        beaconBox.setSpacing(6);
        beaconBox.setStyle(cssLayout);

        CheckBox beaconEnabledBox = new CheckBox("Online-Abfrage via beaconDB aktivieren");
        beaconEnabledBox.setSelected(BeaconDbClient.getInstance().isEnabled());
        beaconEnabledBox.setTooltip(new Tooltip(
                "Wenn aktiviert, wird beaconDB online abgefragt wenn\n" +
                "weder die lokale OpenCelliD-CSV noch der GPS-Fallback\n" +
                "einen Funkmasten finden konnten.\n\n" +
                "Erfordert Internetverbindung. Kein API-Key nötig.\n" +
                "Es wird ein identifizierender User-Agent gesendet."));

        int   cachedBeacon = BeaconDbClient.getInstance().getCacheSize();
        Label beaconCacheLabel = new Label(cachedBeacon > 0
                ? "\uD83D\uDCBE " + cachedBeacon + " Einträge im Cache"
                : "\uD83D\uDCBE Kein Cache vorhanden");
        beaconCacheLabel.setStyle("-fx-font-size: 10; -fx-font-family: monospace;");

        Button btnClearBeaconCache = new Button("Cache leeren");
        btnClearBeaconCache.setStyle("-fx-font-size: 10;");
        btnClearBeaconCache.setTooltip(new Tooltip(
                "Löscht RAM- und Disk-Cache (~/.fqlite/beacondb/).\n" +
                "Beim nächsten Einblenden werden alle Masten neu abgefragt."));
        btnClearBeaconCache.setOnAction(e -> {
            BeaconDbClient.getInstance().clearCache();
            beaconCacheLabel.setText("\uD83D\uDCBE Cache geleert");
        });

        HBox beaconCacheRow = new HBox(8, btnClearBeaconCache, beaconCacheLabel);
        beaconCacheRow.setAlignment(Pos.CENTER_LEFT);

        Label beaconInfoLabel = new Label(
                "Endpunkt: api.beacondb.net/v1/geolocate  (kein API-Key)");
        beaconInfoLabel.setStyle("-fx-font-size: 10; -fx-font-family: monospace;");

        Label beaconPrivacyLabel = new Label(
                "Datenschutz: MCC/MNC/LAC/ECI werden an den Server übermittelt.");
        beaconPrivacyLabel.setStyle("-fx-font-size: 10; -fx-text-fill: #888;");

        Label beaconStatusLabel = new Label(
                BeaconDbClient.getInstance().isEnabled() ? "\u2705 Aktiviert" : "\u26AA Deaktiviert");
        beaconStatusLabel.setStyle("-fx-font-size: 10; -fx-font-family: monospace;");
        beaconEnabledBox.selectedProperty().addListener((o, a, on) ->
                beaconStatusLabel.setText(on ? "\u2705 Aktiviert" : "\u26AA Deaktiviert"));

        beaconBox.getChildren().addAll(
                beaconEnabledBox, beaconCacheRow,
                beaconInfoLabel, beaconPrivacyLabel, beaconStatusLabel);

        // ── Lokale TAC-Datenbank ─────────────────────────────────────────────
        Label headingTacDb = new Label("Lokale TAC-Datenbank");
        headingTacDb.setFont(Font.font("Verdana", FontWeight.BOLD, 12));

        VBox tacDbBox = new VBox();
        tacDbBox.setPadding(new Insets(5));
        tacDbBox.setSpacing(6);
        tacDbBox.setStyle(cssLayout);

        TextField tacDbField = new TextField(
                fqlite.timemap.TacLocalDatabase.getInstance().getFilePath() != null
                        ? fqlite.timemap.TacLocalDatabase.getInstance().getFilePath().toString() : "");
        tacDbField.setPrefWidth(210);
        tacDbField.setPromptText("Pfad zur TAC-Datenbank (Brand/TAC/SPECS, TSV)");
        tacDbField.setTooltip(new Tooltip(
                "Tab-getrennte Datei mit Spalten Brand, TAC, SPECS\n" +
                "(z.B. data/tac_db.txt im Projektordner).\n" +
                "Wird bei der Auswertung \"IMEI → Gerätetyp\" der\n" +
                "Online-API immer vorgezogen, sofern ein Treffer\n" +
                "vorliegt — es ist dann kein API-Key/Internet nötig.\n" +
                "Unterstützt auch .gz (komprimiert)."));
        Button tacDbBrowseBtn = new Button("Durchsuchen…");
        tacDbBrowseBtn.setOnAction(e -> {
            javafx.stage.FileChooser fc = new javafx.stage.FileChooser();
            fc.setTitle("TAC-Datenbank-Datei öffnen");
            fc.getExtensionFilters().addAll(
                    new javafx.stage.FileChooser.ExtensionFilter("TAC-Datenbank", "*.txt", "*.tsv", "*.csv", "*.gz"),
                    new javafx.stage.FileChooser.ExtensionFilter("Alle Dateien", "*.*"));
            if (!tacDbField.getText().isBlank()) {
                java.io.File cur = new java.io.File(tacDbField.getText()).getParentFile();
                if (cur != null && cur.exists()) fc.setInitialDirectory(cur);
            }
            java.io.File chosen = fc.showOpenDialog(stage);
            if (chosen != null) tacDbField.setText(chosen.getAbsolutePath());
        });
        HBox tacDbPathRow = new HBox(8, new Label("Datei:"), tacDbField, tacDbBrowseBtn);
        tacDbPathRow.setAlignment(Pos.CENTER_LEFT);

        int     cachedTac  = fqlite.timemap.TacLocalDatabase.getInstance().getEntryCount();
        boolean tacDbLoaded = fqlite.timemap.TacLocalDatabase.getInstance().isLoaded();
        Label tacDbStatusLabel = new Label(
                tacDbLoaded
                        ? "✅ " + String.format("%,d", cachedTac) + " TACs geladen"
                        : fqlite.timemap.TacLocalDatabase.getInstance().isConfigured()
                        ? "⏳ Datei konfiguriert, noch nicht geladen"
                        : "⚪ Keine Datei konfiguriert");
        tacDbStatusLabel.setStyle("-fx-font-size: 10; -fx-font-family: monospace;");

        Button btnClearTacDbCache = new Button("Index leeren");
        btnClearTacDbCache.setStyle("-fx-font-size: 10;");
        btnClearTacDbCache.setTooltip(new Tooltip(
                "Entfernt den geladenen Index aus dem RAM.\n" +
                "Die Datei bleibt erhalten."));
        btnClearTacDbCache.setOnAction(e -> {
            fqlite.timemap.TacLocalDatabase.getInstance().clearCache();
            tacDbStatusLabel.setText("⚪ Index geleert");
        });

        Button btnPreloadTacDb = new Button("Jetzt laden");
        btnPreloadTacDb.setStyle("-fx-font-size: 10;");
        btnPreloadTacDb.setTooltip(new Tooltip("Datei sofort in den Speicher laden."));
        btnPreloadTacDb.setOnAction(e -> {
            String p = tacDbField.getText().trim();
            if (!p.isBlank()) {
                fqlite.timemap.TacLocalDatabase.getInstance().setFilePath(java.nio.file.Path.of(p));
                fqlite.timemap.TacLocalDatabase.getInstance().loadAsync();
                tacDbStatusLabel.setText("⏳ Wird geladen…");
            }
        });

        HBox tacDbButtonRow = new HBox(8, btnPreloadTacDb, btnClearTacDbCache, tacDbStatusLabel);
        tacDbButtonRow.setAlignment(Pos.CENTER_LEFT);
        tacDbBox.getChildren().addAll(tacDbPathRow, tacDbButtonRow);

        // ── IMEI/TAC-Gerätelookup (HiCellTek) ───────────────────────────────────
        Label headingTac = new Label("IMEI/TAC-Gerätelookup (HiCellTek)");
        headingTac.setFont(Font.font("Verdana", FontWeight.BOLD, 12));

        VBox tacBox = new VBox();
        tacBox.setPadding(new Insets(5));
        tacBox.setSpacing(6);
        tacBox.setStyle(cssLayout);

        CheckBox tacEnabledBox = new CheckBox("Online-Abfrage für Gerätetyp (IMEI/TAC) aktivieren");
        tacEnabledBox.setSelected(fqlite.timemap.TacLookupService.isEnabled());
        tacEnabledBox.setTooltip(new Tooltip(
                "Wenn aktiviert, wird bei der Auswertung \"IMEI → Gerätetyp\"\n" +
                "zusätzlich zum lokalen Datensatzbefund die HiCellTek-API\n" +
                "online abgefragt (Hersteller/Modell/Chipsatz anhand der TAC).\n\n" +
                "Erfordert Internetverbindung und einen API-Key (siehe unten).\n" +
                "Status: Code-Stub, Schnittstelle nicht verifiziert — siehe\n" +
                "Javadoc von TacLookupService."));

        PasswordField tacApiKeyField = new PasswordField();
        String currentTacKey = fqlite.timemap.TacLookupService.getApiKey();
        if (currentTacKey != null) tacApiKeyField.setText(currentTacKey);
        tacApiKeyField.setPrefWidth(210);
        tacApiKeyField.setPromptText("HiCellTek API-Key");
        tacApiKeyField.setTooltip(new Tooltip(
                "API-Key von hicelltek.com. Alternativ kann er auch über\n" +
                "die System-Property 'fqlite.hicelltek.apikey' gesetzt werden\n" +
                "(diese hat Vorrang, falls hier nichts eingetragen ist)."));
        HBox tacKeyRow = new HBox(8, new Label("API-Key:"), tacApiKeyField);
        tacKeyRow.setAlignment(Pos.CENTER_LEFT);

        Label tacInfoLabel = new Label(
                "Endpunkt: imei.hicelltek.com/api/v1/tac/lookup  (API-Key nötig)");
        tacInfoLabel.setStyle("-fx-font-size: 10; -fx-font-family: monospace;");

        Label tacPrivacyLabel = new Label(
                "Datenschutz: die 8-stellige TAC (kein vollständiges IMEI) wird übermittelt.");
        tacPrivacyLabel.setStyle("-fx-font-size: 10; -fx-text-fill: #888;");

        Label tacStatusLabel = new Label(
                fqlite.timemap.TacLookupService.isEnabled() ? "✅ Aktiviert" : "⚪ Deaktiviert");
        tacStatusLabel.setStyle("-fx-font-size: 10; -fx-font-family: monospace;");
        tacEnabledBox.selectedProperty().addListener((o, a, on) ->
                tacStatusLabel.setText(on ? "✅ Aktiviert" : "⚪ Deaktiviert"));

        tacBox.getChildren().addAll(
                tacEnabledBox, tacKeyRow,
                tacInfoLabel, tacPrivacyLabel, tacStatusLabel);

        locationContent.getChildren().addAll(
                headingMBT, mbtBox,
                headingOCI, cellBox,
                headingBDB, beaconBox,
                headingTacDb, tacDbBox,
                headingTac, tacBox);

        ScrollPane locationScroll = new ScrollPane(locationContent);
        locationScroll.setFitToWidth(true);

        Tab locationTab = new Tab("Location", locationScroll);
        locationTab.setClosable(false);

        // =====================================================================
        // TabPane + ButtonBar
        // =====================================================================
        TabPane tabPane = new TabPane(commonTab, locationTab);
        tabPane.setTabMinWidth(80);

        ButtonBar buttonBar = new ButtonBar();
        buttonBar.setPadding(new Insets(8, 8, 8, 8));

        Button applyButton  = new Button("Apply");
        Button cancelButton = new Button("Cancel");
        ButtonBar.setButtonData(applyButton,  ButtonBar.ButtonData.APPLY);
        ButtonBar.setButtonData(cancelButton, ButtonBar.ButtonData.CANCEL_CLOSE);
        buttonBar.getButtons().addAll(applyButton, cancelButton);

        applyButton.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(final ActionEvent e) {

                // Log level
                switch (loglevel.getValue()) {
                    case "SEVERE":  Global.LOGLEVEL = Level.SEVERE;  break;
                    case "WARNING": Global.LOGLEVEL = Level.WARNING; break;
                    case "INFO":    Global.LOGLEVEL = Level.INFO;    break;
                    case "FINE":    Global.LOGLEVEL = Level.FINE;    break;
                }

                // Export mode
                if (r1.isSelected())      Global.EXPORT_MODE = Global.EXPORT_MODES.DONTEXPORT;
                else if (r3.isSelected()) Global.EXPORT_MODE = Global.EXPORT_MODES.TOSEPARATEFILES;

                File baseDir = new File(System.getProperty("user.home"), ".fqlite");
                String path  = baseDir.getAbsolutePath() + File.separator + "fqlite.conf";

                Properties appProps = new Properties();
                try {
                    appProps.load(new FileInputStream(path));

                    appProps.setProperty("EXPORTMODE", Global.EXPORT_MODE.name());
                    Global.EXPORTTABLEHEADER = exportTblHeader.isSelected();
                    appProps.setProperty("EXPORT_THEADER", Global.EXPORTTABLEHEADER ? "true" : "false");
                    Global.CSV_SEPARATOR = choiceBox.getSelectionModel().getSelectedItem();
                    appProps.setProperty("CSV_SEPARATOR", Global.CSV_SEPARATOR);
                    appProps.setProperty("LOG-LEVEL", Global.LOGLEVEL.toString());

                    // Timestamp format
                    String newPattern = tsPatternField.getText().trim();
                    try {
                        DateTimeFormatter.ofPattern(newPattern);
                        Global.TIMESTAMP_FORMAT = newPattern;
                    } catch (IllegalArgumentException patEx) {
                        AppLog.error("Rejected invalid timestamp pattern: " + newPattern);
                    }
                    appProps.setProperty("TIMESTAMP_FORMAT", Global.TIMESTAMP_FORMAT);
                    Global.TIMESTAMP_USE_UTC = tsUTC.isSelected();
                    appProps.setProperty("TIMESTAMP_USE_UTC", Global.TIMESTAMP_USE_UTC ? "true" : "false");

                    // MBTiles offline map
                    String mbtPath = mbtField.getText().trim();
                    if (!mbtPath.isBlank()) {
                        java.nio.file.Path mbt = java.nio.file.Path.of(mbtPath);
                        fqlite.timemap.TileCache.getInstance().setMbTilesPath(mbt);
                        appProps.setProperty("MBTILES_PATH", mbtPath);
                    } else {
                        fqlite.timemap.TileCache.getInstance().setMbTilesPath(null);
                        appProps.remove("MBTILES_PATH");
                    }

                    // OpenCelliD CSV
                    String csvPathStr = cellCsvField.getText().trim();
                    if (!csvPathStr.isBlank()) {
                        CellTowerResolver.getInstance().setCsvPath(java.nio.file.Path.of(csvPathStr));
                        appProps.setProperty("OPENCELLID_CSV", csvPathStr);
                    } else {
                        CellTowerResolver.getInstance().setCsvPath(null);
                        appProps.remove("OPENCELLID_CSV");
                    }

                    // beaconDB
                    boolean beaconEnabled = beaconEnabledBox.isSelected();
                    BeaconDbClient.getInstance().setEnabled(beaconEnabled);
                    appProps.setProperty("BEACONDB_ENABLED", beaconEnabled ? "true" : "false");

                    // Lokale TAC-Datenbank
                    String tacDbPathStr = tacDbField.getText().trim();
                    if (!tacDbPathStr.isBlank()) {
                        fqlite.timemap.TacLocalDatabase.getInstance().setFilePath(java.nio.file.Path.of(tacDbPathStr));
                        appProps.setProperty("TACDB_PATH", tacDbPathStr);
                    } else {
                        fqlite.timemap.TacLocalDatabase.getInstance().setFilePath(null);
                        appProps.remove("TACDB_PATH");
                    }

                    // IMEI/TAC-Gerätelookup (HiCellTek)
                    boolean tacEnabled = tacEnabledBox.isSelected();
                    fqlite.timemap.TacLookupService.setEnabled(tacEnabled);
                    appProps.setProperty("TACLOOKUP_ENABLED", tacEnabled ? "true" : "false");
                    String tacKey = tacApiKeyField.getText().trim();
                    if (!tacKey.isBlank()) {
                        fqlite.timemap.TacLookupService.setApiKey(tacKey);
                        appProps.setProperty("TACLOOKUP_APIKEY", tacKey);
                    } else {
                        fqlite.timemap.TacLookupService.setApiKey(null);
                        appProps.remove("TACLOOKUP_APIKEY");
                    }

                    appProps.store(new FileOutputStream(path), null);

                } catch (Exception err) {
                    AppLog.error(err.getMessage());
                }

                Stage s = (Stage) applyButton.getScene().getWindow();
                s.close();
            }
        });

        cancelButton.setOnAction(e -> {
            Stage s = (Stage) applyButton.getScene().getWindow();
            s.close();
        });

        VBox root = new VBox(tabPane, buttonBar);
        root.setSpacing(0);

        Scene settingsScene = new Scene(root, 520, 600);
        ThemeManager.register(settingsScene);
        stage.setScene(settingsScene);
        stage.setAlwaysOnTop(true);
        stage.show();
    }
}
