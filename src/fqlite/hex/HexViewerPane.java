package fqlite.hex;

import javafx.concurrent.Task;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.scene.control.*;
import javafx.scene.input.*;
import javafx.scene.layout.*;
import javafx.stage.FileChooser;

import java.io.File;
import java.nio.file.Path;

public class HexViewerPane extends BorderPane {

    private HexCanvas            hexCanvas;
    private ScrollBar            vScroll;
    private Label                statusLabel;
    private Label                posLabel;
    private TextField            searchField;
    private TextField            gotoField;
    private ProgressBar          progressBar;
    private Button               searchBtn, gotoBtn;
    private ToggleButton         searchModeBtn; // HEX | ASCII
    private ByteInterpreterPanel interpPanel;

    private VirtualFileReader currentReader;
    private long totalRows = 0;
    private boolean dragging = false;

    public HexViewerPane() {
        buildUI();
        bindEvents();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // UI
    // ─────────────────────────────────────────────────────────────────────────

    private void buildUI() {

        // ── Toolbar ───────────────────────────────────────────────────────────
        // Zwei Zeilen: [Datei | Suche | Goto]
        // Um in 800px zu passen: Felder kompakt, kein Spacer-Trick nötig.

        //openBtn   = styledBtn("📂 Öffnen", "btn-primary");
        searchBtn     = new Button("🔍 Search");
        gotoBtn       = new Button("↵ Goto");
        searchModeBtn = new ToggleButton("HEX");
        searchModeBtn.setSelected(false);          // false = HEX, true = ASCII
        searchModeBtn.setPrefWidth(52);
        searchModeBtn.getStyleClass().add("search-mode-btn");
        // Label + Prompt je nach Modus aktualisieren
        searchModeBtn.selectedProperty().addListener((obs, wasAscii, isAscii) -> {
            searchModeBtn.setText(isAscii ? "ASCII" : "HEX");
            searchField.setPromptText(isAscii ? "Search text …" : "FF D8 … oder FFD8");
            searchField.clear();
        });

        searchField = new TextField();
        searchField.setPromptText("FF D8 … oder FFD8");
        searchField.setPrefWidth(130);

        gotoField = new TextField();
        gotoField.setPromptText("Offset hex");
        gotoField.setPrefWidth(100);

        progressBar = new ProgressBar(0);
        progressBar.setPrefWidth(70);
        progressBar.setVisible(false);
        progressBar.setManaged(false);

        // Zeile 1: Datei / Suche / Goto
        HBox toolRow1 = new HBox(4,
                //openBtn,
                new Separator(Orientation.VERTICAL),
                searchField, searchModeBtn, searchBtn,
                new Separator(Orientation.VERTICAL),
                gotoField, gotoBtn,
                new Separator(Orientation.VERTICAL),
                progressBar
        );
        toolRow1.setAlignment(Pos.CENTER_LEFT);

        VBox toolbar = new VBox(2, toolRow1);
        toolbar.setPadding(new Insets(5, 8, 5, 8));
        toolbar.setMaxWidth(Double.MAX_VALUE);

        // ── Canvas + Scrollbar ────────────────────────────────────────────────
        hexCanvas = new HexCanvas(650, 440);

        vScroll = new ScrollBar();
        vScroll.setOrientation(Orientation.VERTICAL);
        vScroll.setMin(0);
        vScroll.setValue(0);
        vScroll.setPrefWidth(14);

        // StackPane-Wrapper damit HBox die volle Breite einnimmt
        HBox canvasRow = new HBox(hexCanvas, vScroll);
        //canvasRow.getStyleClass().add("canvas-area");

        // ── Interpreter-Panel ─────────────────────────────────────────────────
        interpPanel = new ByteInterpreterPanel();
        //interpPanel.getStyleClass().add("interp-panel");
        // Panel ist erst sichtbar wenn Byte selektiert – managed bleibt true
        // damit die VBox die Höhe korrekt reserviert.
        interpPanel.setPrefHeight(200);
        interpPanel.setVisible(true);
        //interpPanel.setManaged(true);   // kein Platz wenn leer

        // BorderPane als Center-Layout:
        //   Top    = nichts
        //   Center = Canvas-Zeile (bekommt allen freien Platz)
        //   Bottom = Interpreter-Panel (feste Höhe, nur wenn sichtbar)
        BorderPane centerPane = new BorderPane();
        centerPane.setCenter(canvasRow);
        //centerPane.setBottom(interpPanel);

        // Canvas passt sich der canvasRow-Größe an
        canvasRow.layoutBoundsProperty().addListener((obs, old, b) -> {
            double cw = Math.max(100, Math.min(b.getWidth() - vScroll.getPrefWidth() - 1, 8000));
            double ch = Math.max(80,  Math.min(b.getHeight(), 8000));
            if (Math.abs(hexCanvas.getWidth() - cw) > 0.5 ||
                Math.abs(hexCanvas.getHeight() - ch) > 0.5) {
                hexCanvas.setWidth(cw);
                hexCanvas.setHeight(ch);
                if (currentReader != null) {
                    int vis = hexCanvas.getVisibleRowCount();
                    vScroll.setMax(Math.max(0, totalRows - vis));
                    vScroll.setVisibleAmount(vis);
                }
                hexCanvas.redraw();
            }
        });

        // ── Statuszeile ───────────────────────────────────────────────────────
        statusLabel = new Label("Started.");
        statusLabel.getStyleClass().add("status-label");
        posLabel = new Label();
        posLabel.getStyleClass().add("pos-label");

        Region statusSpacer = new Region();
        HBox.setHgrow(statusSpacer, Priority.ALWAYS);

        HBox statusBar = new HBox(8, statusLabel, statusSpacer, posLabel);
        statusBar.setPadding(new Insets(3, 8, 3, 8));
        statusBar.setAlignment(Pos.CENTER_LEFT);
        statusBar.getStyleClass().add("status-bar");

        setTop(toolbar);
        setCenter(centerPane);
        setBottom(statusBar);
    }

    /** Interpreter-Panel ein-/ausblenden und managed-Flag synchron halten. */
    private void showInterpreter(boolean show) {
        interpPanel.setVisible(show);
        interpPanel.setManaged(show);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Events
    // ─────────────────────────────────────────────────────────────────────────

    private void bindEvents() {

        hexCanvas.setOnScroll((ScrollEvent e) -> {
            if (currentReader == null) return;
            scrollToRow(hexCanvas.getFirstVisibleRow() + (e.getDeltaY() < 0 ? 3 : -3));
        });

        vScroll.valueProperty().addListener((o, old, nw) -> {
            if (currentReader == null) return;
            long row = Math.round(nw.doubleValue());
            if (row != hexCanvas.getFirstVisibleRow())
                hexCanvas.setFirstVisibleRow(row);
        });

        hexCanvas.setFocusTraversable(true);
        hexCanvas.setOnKeyPressed(e -> {
            if (currentReader == null) return;
            long cur = hexCanvas.getFirstVisibleRow();
            int  vis = hexCanvas.getVisibleRowCount();
            switch (e.getCode()) {
                case DOWN      -> scrollToRow(cur + 1);
                case UP        -> scrollToRow(cur - 1);
                case PAGE_DOWN -> scrollToRow(cur + vis);
                case PAGE_UP   -> scrollToRow(cur - vis);
                case HOME      -> scrollToRow(0);
                case END       -> scrollToRow(totalRows - vis);
                case C         -> { if (e.isShortcutDown()) copyToClipboard(CopyFormat.HEX_DUMP); }
                case ESCAPE    -> { hexCanvas.clearSelection(); hexCanvas.redraw(); showInterpreter(false); updateSelectionStatus(); }
                default -> {}
            }
        });

        hexCanvas.setOnMousePressed(e -> {
            hexCanvas.requestFocus();
            if (e.getButton() != MouseButton.PRIMARY) return;
            long b = hexCanvas.byteAtCanvasPos(e.getX(), e.getY());
            if (b < 0) return;
            if (e.isShiftDown() && hexCanvas.getSelectedByte() >= 0) hexCanvas.extendSelection(b);
            else hexCanvas.startSelection(b);
            dragging = true;
            updateSelectionStatus();
            refreshInterpreter();
        });

        hexCanvas.setOnMouseDragged(e -> {
            if (!dragging || currentReader == null) return;
            long b = hexCanvas.byteAtCanvasPos(e.getX(), e.getY());
            if (b >= 0) {
                hexCanvas.extendSelection(b);
                updateSelectionStatus();
                refreshInterpreter();
                if (e.getY() < 30) scrollToRow(hexCanvas.getFirstVisibleRow() - 1);
                else if (e.getY() > hexCanvas.getHeight() - 10) scrollToRow(hexCanvas.getFirstVisibleRow() + 1);
            }
        });

        hexCanvas.setOnMouseReleased(e -> dragging = false);

        hexCanvas.setOnContextMenuRequested(e -> {
            dragging = false; // MouseReleased kommt nach Rechtsklick nicht – manuell zurücksetzen
            if (currentReader == null) return;
            if (!hexCanvas.hasRangeSelection() && hexCanvas.getSelectedByte() < 0) return;
            ContextMenu menu = new ContextMenu();
            MenuItem mDump  = new MenuItem("📋  copy Hex-Dump ");  mDump.setOnAction(ev  -> copyToClipboard(CopyFormat.HEX_DUMP));
            MenuItem mHex   = new MenuItem("📋  copy Hex-Bytes"); mHex.setOnAction(ev   -> copyToClipboard(CopyFormat.HEX_ONLY));
            MenuItem mAscii = new MenuItem("📋  copy ASCII");     mAscii.setOnAction(ev -> copyToClipboard(CopyFormat.ASCII));
            MenuItem mC     = new MenuItem("📋  copy C-Array");   mC.setOnAction(ev     -> copyToClipboard(CopyFormat.C_ARRAY));
            menu.getItems().addAll(mDump, mHex, mAscii, mC);
            menu.setAutoHide(true); // schliesst sich automatisch bei Klick ausserhalb
            menu.show(hexCanvas.getScene().getWindow(), e.getScreenX(), e.getScreenY());
        });

        searchBtn.setOnAction(e -> performSearch());
        searchField.setOnAction(e -> performSearch());
        gotoBtn.setOnAction(e -> gotoOffset());
        gotoField.setOnAction(e -> gotoOffset());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Byte-Interpretation
    // ─────────────────────────────────────────────────────────────────────────

    private void refreshInterpreter() {
        if (currentReader == null) { showInterpreter(true); return; }

        long from;
        int  wantBytes;
        if (hexCanvas.hasRangeSelection()) {
            from      = hexCanvas.getSelectionStart();
            wantBytes = (int) Math.min(hexCanvas.getSelectionEnd() - from + 1, 8);
        } else if (hexCanvas.getSelectedByte() >= 0) {
            from      = hexCanvas.getSelectedByte();
            wantBytes = 8;
        } else {
            showInterpreter(true);
            return;
        }

        long available = currentReader.getFileSize() - from;
        int  toRead    = (int) Math.min(wantBytes, available);
        if (toRead <= 0) { showInterpreter(true); return; }

        try {
            byte[] rowData = currentReader.readRows(
                    from / VirtualFileReader.BYTES_PER_ROW,
                    (int)((from + toRead - 1) / VirtualFileReader.BYTES_PER_ROW
                          - from / VirtualFileReader.BYTES_PER_ROW + 1));
            int off = (int)(from % VirtualFileReader.BYTES_PER_ROW);
            byte[] bytes = new byte[toRead];
            System.arraycopy(rowData, off, bytes, 0, toRead);
            interpPanel.update(bytes, from);
            showInterpreter(true);
        } catch (Exception ex) {
            showInterpreter(false);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Clipboard
    // ─────────────────────────────────────────────────────────────────────────

    private enum CopyFormat { HEX_DUMP, HEX_ONLY, ASCII, C_ARRAY }

    private void copyToClipboard(CopyFormat format) {
        if (currentReader == null) return;
        long from, to;
        if (hexCanvas.hasRangeSelection()) {
            from = hexCanvas.getSelectionStart();
            to   = hexCanvas.getSelectionEnd();
        } else if (hexCanvas.getSelectedByte() >= 0) {
            long sel = hexCanvas.getSelectedByte();
            from = (sel / 16) * 16;
            to   = Math.min(from + 15, currentReader.getFileSize() - 1);
        } else return;

        long count = to - from + 1;
        if (count > 4 * 1024 * 1024) { showError("Max. 4 MB could be copied."); return; }

        byte[] raw;
        try {
            int rowFrom  = (int)(from / 16);
            int rowCount = (int)(to / 16 - rowFrom + 1);
            byte[] rowData = currentReader.readRows(rowFrom, rowCount);
            raw = new byte[(int) count];
            System.arraycopy(rowData, (int)(from % 16), raw, 0, (int) count);
        } catch (Exception ex) { showError("IO-Error: " + ex.getMessage()); return; }

        String text = switch (format) {
            case HEX_DUMP -> buildHexDump(from, raw);
            case HEX_ONLY -> buildHexOnly(raw);
            case ASCII    -> buildAscii(raw);
            case C_ARRAY  -> buildCArray(from, raw);
        };

        ClipboardContent cc = new ClipboardContent();
        cc.putString(text);
        Clipboard.getSystemClipboard().setContent(cc);
        statusLabel.setText(String.format("✅ %,d Bytes copied (0x%X–0x%X)", count, from, to));
    }

    private String buildHexDump(long base, byte[] raw) {
        StringBuilder sb = new StringBuilder();
        int rows = (raw.length + 15) / 16;
        for (int r = 0; r < rows; r++) {
            sb.append(String.format("%016X  ", base + r * 16L));
            for (int c = 0; c < 16; c++) {
                int i = r*16+c;
                sb.append(i < raw.length ? String.format("%02X ", raw[i]) : "   ");
                if (c == 7) sb.append(' ');
            }
            sb.append(" |");
            for (int c = 0; c < 16; c++) {
                int i = r*16+c; if (i >= raw.length) break;
                int v = raw[i]&0xFF; sb.append((v>=32&&v<127)?(char)v:'.');
            }
            sb.append("|\n");
        }
        return sb.toString();
    }

    private String buildHexOnly(byte[] raw) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < raw.length; i++) {
            if (i > 0) sb.append(i%16==0 ? '\n' : ' ');
            sb.append(String.format("%02X", raw[i]&0xFF));
        }
        return sb.toString();
    }

    private String buildAscii(byte[] raw) {
        StringBuilder sb = new StringBuilder(raw.length);
        for (byte b : raw) { int v=b&0xFF; sb.append((v>=32&&v<127)?(char)v:'.'); }
        return sb.toString();
    }

    private String buildCArray(long base, byte[] raw) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("/* offset 0x%X, %d bytes */\nuint8_t data[] = {\n    ", base, raw.length));
        for (int i = 0; i < raw.length; i++) {
            sb.append(String.format("0x%02X", raw[i]&0xFF));
            if (i < raw.length-1) { sb.append(','); sb.append((i+1)%16==0 ? "\n    " : " "); }
        }
        sb.append("\n};\n");
        return sb.toString();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Status line
    // ─────────────────────────────────────────────────────────────────────────

    private void updateSelectionStatus() {
        if (hexCanvas.hasRangeSelection()) {
            long from = hexCanvas.getSelectionStart(), to = hexCanvas.getSelectionEnd();
            posLabel.setText(String.format("Selection 0x%X–0x%X  |  %,d Bytes", from, to, to-from+1));
        } else if (hexCanvas.getSelectedByte() >= 0) {
            long b = hexCanvas.getSelectedByte();
            posLabel.setText(String.format("Offset 0x%X  |  0x%02X (%d)", b, readByteAt(b), readByteAt(b)));
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // file
    // ─────────────────────────────────────────────────────────────────────────

    //private void openFile() {
    //    FileChooser fc = new FileChooser();
    //    fc.setTitle("Datei öffnen");
    //    fc.getExtensionFilters().add(new FileChooser.ExtensionFilter("Alle Dateien", "*.*"));
    //    File file = fc.showOpenDialog(getScene().getWindow());
    //    if (file == null) return;
    //    loadFile(file.toPath());
    //}

    public void loadFile(Path path) {
        try {
            if (currentReader != null) currentReader.close();
            currentReader = new VirtualFileReader(path);
        } catch (Exception ex) { showError("Could not open file : " + ex.getMessage()); return; }
        totalRows = currentReader.getTotalRows();
        hexCanvas.setReader(currentReader);
        showInterpreter(false);
        int vis = hexCanvas.getVisibleRowCount();
        vScroll.setMax(Math.max(0, totalRows - vis));
        vScroll.setVisibleAmount(vis);
        vScroll.setValue(0);
        long size = currentReader.getFileSize();
        statusLabel.setText(String.format("📄 %s  |  %,d Bytes (%.2f MB)  |  %,d lines",
                path.getFileName(), size, size/1_048_576.0, totalRows));
        posLabel.setText("");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Scrolling
    // ─────────────────────────────────────────────────────────────────────────

    private void scrollToRow(long row) {
        if (currentReader == null) return;
        int  vis     = hexCanvas.getVisibleRowCount();
        long clamped = Math.max(0, Math.min(row, totalRows - vis));
        // Nur den ScrollBar setzen – der Listener aktualisiert den Canvas.
        // Kein doppeltes Setzen, kein Reentry-Problem.
        vScroll.setValue(clamped);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Suche
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Parst den Suchbegriff flexibel:
     *  - Hex-Modus: "DDFF", "DD FF", "DD,FF", "0xDD 0xFF" werden alle akzeptiert.
     *  - ASCII-Modus: der Text wird direkt als UTF-8-Bytes interpretiert.
     * @return byte[] oder null bei Fehler
     */
    private byte[] parseSearchPattern(String text) {
        if (searchModeBtn.isSelected()) {
            // ASCII-Modus: Text direkt als UTF-8-Bytes
            return text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        // Hex-Modus: Leerzeichen, Kommas, 0x-Präfixe tolerieren, Rest paarweise
        String hex = text
                .replaceAll("0[xX]", "")            // 0x-Präfixe entfernen
                .replaceAll("[^0-9A-Fa-f]", "");    // alles außer Hex-Ziffern entfernen
        if (hex.isEmpty() || hex.length() % 2 != 0) return null;
        byte[] pat = new byte[hex.length() / 2];
        for (int i = 0; i < pat.length; i++)
            pat[i] = (byte) Integer.parseInt(hex.substring(i*2, i*2+2), 16);
        return pat;
    }

    private void performSearch() {
        if (currentReader == null) return;
        String text = searchField.getText().trim();
        if (text.isEmpty()) return;
        byte[] pattern = parseSearchPattern(text);
        if (pattern == null || pattern.length == 0) {
            showError("Ungültiges Suchmuster.\n\n" +
                      "Hex-Bytes: FF D8  oder  FFD8  oder  FF,D8\n" +
                      "ASCII-Text: \"hello\"  oder  \'hello\'");
            return;
        }

        long startByte = hexCanvas.getSelectedByte() >= 0 ? hexCanvas.getSelectedByte() + 1 : 0;
        progressBar.setVisible(true); progressBar.setManaged(true);
        progressBar.setProgress(ProgressIndicator.INDETERMINATE_PROGRESS);
        statusLabel.setText("🔍 Searching ... …");

        byte[] pat = pattern;
        Task<Long> task = new Task<>() {
            @Override protected Long call() throws Exception {
                long fileSize = currentReader.getFileSize();
                int bufSize = Math.max(65536, pat.length * 2);
                byte[] buf = new byte[bufSize];
                long pos = startByte;
                while (pos < fileSize) {
                    // readRows liest auf volle Zeilengrenzen – raw kann groesser
                    // sein als toRead. startOff = Byte-Offset innerhalb Zeile 0.
                    int startOff = (int)(pos % VirtualFileReader.BYTES_PER_ROW);
                    int toRead   = (int) Math.min(bufSize, fileSize - pos);
                    long firstRow = pos / VirtualFileReader.BYTES_PER_ROW;
                    int rowCount  = (int)(((pos + toRead - 1) / VirtualFileReader.BYTES_PER_ROW) - firstRow + 1);
                    byte[] raw    = currentReader.readRows(firstRow, rowCount);
                    int available = Math.min(toRead, raw.length - startOff);
                    System.arraycopy(raw, startOff, buf, 0, available);
                    for (int i = 0; i <= available - pat.length; i++) {
                        boolean m = true;
                        for (int j = 0; j < pat.length; j++) if (buf[i+j]!=pat[j]) { m=false; break; }
                        if (m) return pos + i;
                    }
                    pos += Math.max(1, available - pat.length + 1);
                }
                return -1L;
            }
        };
        task.setOnSucceeded(ev -> {
            progressBar.setVisible(false); progressBar.setManaged(false);
            long found = task.getValue();
            if (found >= 0) {
                hexCanvas.startSelection(found);
                if (pat.length > 1) hexCanvas.extendSelection(found + pat.length - 1);
                scrollToRow(found/16 - hexCanvas.getVisibleRowCount()/2);
                updateSelectionStatus();
                refreshInterpreter();
                statusLabel.setText(String.format("✅ Found at offset 0x%X", found));
            } else {
                statusLabel.setText("❌ No match.");
            }
        });
        task.setOnFailed(ev -> {
            progressBar.setVisible(false); progressBar.setManaged(false);
            showError("Search Error: " + task.getException().getMessage());
        });
        new Thread(task, "hex-search").start();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Goto
    // ─────────────────────────────────────────────────────────────────────────

    public void gotoOffset() {
        if (currentReader == null) return;
        String text = gotoField.getText().trim().replaceFirst("^0[xX]", "");
        try {
            long offset = Long.parseUnsignedLong(text, 16);
            hexCanvas.setSelectedByte(offset);
            scrollToRow(offset/16 - hexCanvas.getVisibleRowCount()/2);
            updateSelectionStatus();
            refreshInterpreter();
        } catch (NumberFormatException ex) { showError("Invalid Offset, z.B.  1A2B3C"); }
    }

    public void gotoOffset(long offset) {
        if (currentReader == null) return;
        try {
            hexCanvas.setSelectedByte(offset);
            scrollToRow(offset/16 - hexCanvas.getVisibleRowCount()/2);
            updateSelectionStatus();
            refreshInterpreter();
        } catch (NumberFormatException ex) { showError("Invalid Offset, z.B.  1A2B3C"); }
    }


    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private int readByteAt(long offset) {
        try {
            byte[] d = currentReader.readRows(offset/VirtualFileReader.BYTES_PER_ROW, 1);
            return d[(int)(offset%VirtualFileReader.BYTES_PER_ROW)] & 0xFF;
        } catch (Exception e) { return 0; }
    }

    private void showError(String msg) {
        Alert a = new Alert(Alert.AlertType.ERROR, msg, ButtonType.OK);
        a.setHeaderText(null); a.setTitle("Error"); a.showAndWait();
    }
}
