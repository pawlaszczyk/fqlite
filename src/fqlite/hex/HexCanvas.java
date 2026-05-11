package fqlite.hex;

import fqlite.base.ThemeManager;
import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.FontPosture;
import javafx.scene.text.FontWeight;

/**
 * Canvas-basierter Hex-Viewer – kompaktes Layout für 800×600.
 * Unterstützt Einzelbyte- und Bereichsauswahl.
 */
public class HexCanvas extends Canvas {

    // ── Layout (kompakt für 800×600) ──────────────────────────────────────────
    static final double PAD_LEFT   = 8;
    static final double ROW_HEIGHT = 17;
    static final double ADDR_W     = 130;
    static final double HEX_COL_W  = 22;
    static final double SEP1       = 8;
    static final double SEP2       = 10;
    static final double CHAR_W     = 8;
    static final double HEADER_H   = ROW_HEIGHT + 4;

    // ── Dark palette ──────────────────────────────────────────────────────────
    private static final Color D_BG           = Color.web("#0d1117");
    private static final Color D_BG_ALT       = Color.web("#111820");
    private static final Color D_FG_ADDR      = Color.web("#58a6ff");
    private static final Color D_FG_HEX       = Color.web("#e6edf3");
    private static final Color D_FG_HEX_ZERO  = Color.web("#3d4450");
    private static final Color D_FG_ASCII     = Color.web("#8b949e");
    private static final Color D_FG_ASCII_PRI = Color.web("#56d364");
    private static final Color D_FG_HEADER    = Color.web("#f0883e");
    private static final Color D_FG_SEL       = Color.web("#1f6feb");
    private static final Color D_FG_SEL_TXT   = Color.WHITE;
    private static final Color D_FG_RANGE     = Color.web("#2d4a7a");
    private static final Color D_FG_RANGE_TXT = Color.web("#c8d8ff");
    private static final Color D_GRID_LINE    = Color.web("#21262d");
    private static final Color D_HEADER_BG    = Color.web("#161b22");

    // ── Light palette ─────────────────────────────────────────────────────────
    private static final Color L_BG           = Color.web("#f5f6fa");
    private static final Color L_BG_ALT       = Color.web("#eef0f7");
    private static final Color L_FG_ADDR      = Color.web("#0550ae");
    private static final Color L_FG_HEX       = Color.web("#18203a");
    private static final Color L_FG_HEX_ZERO  = Color.web("#b0b8cc");
    private static final Color L_FG_ASCII     = Color.web("#4a5580");
    private static final Color L_FG_ASCII_PRI = Color.web("#1a7f37");
    private static final Color L_FG_HEADER    = Color.web("#b34800");
    private static final Color L_FG_SEL       = Color.web("#388bfd");
    private static final Color L_FG_SEL_TXT   = Color.WHITE;
    private static final Color L_FG_RANGE     = Color.web("#c8d8f8");
    private static final Color L_FG_RANGE_TXT = Color.web("#0a1228");
    private static final Color L_GRID_LINE    = Color.web("#c0c8dc");
    private static final Color L_HEADER_BG    = Color.web("#eef0f7");

    // ── State ─────────────────────────────────────────────────────────────────
    private VirtualFileReader reader;
    private long firstVisibleRow  = 0;
    private long selectedByte     = -1;
    private long selectionAnchor  = -1;
    private long selectionEnd     = -1;
    private Font monoFont;
    private Font headerFont;

    public HexCanvas(double width, double height) {
        super(width, height);
        monoFont   = Font.font("Courier New", FontWeight.NORMAL,  FontPosture.REGULAR, 13);
        headerFont = Font.font("Courier New", FontWeight.BOLD,    FontPosture.REGULAR, 13);
    }

    private Color c(Color dark, Color light) {
        return ThemeManager.isDark() ? dark : light;
    }

    // ── Public API ────────────────────────────────────────────────────────────

    public void setReader(VirtualFileReader r) {
        this.reader = r;
        this.firstVisibleRow = 0;
        clearSelection();
        redraw();
    }

    public void setFirstVisibleRow(long row) { this.firstVisibleRow = row; redraw(); }
    public long getFirstVisibleRow()         { return firstVisibleRow; }
    public int  getVisibleRowCount()         { return (int) ((getHeight() - HEADER_H) / ROW_HEIGHT); }

    public long getSelectedByte()            { return selectedByte; }
    public void setSelectedByte(long b)      { selectedByte = b; selectionAnchor = b; selectionEnd = -1; redraw(); }

    public void startSelection(long anchor)  { selectedByte = anchor; selectionAnchor = anchor; selectionEnd = -1; redraw(); }
    public void extendSelection(long end)    { if (selectionAnchor < 0) selectionAnchor = end; selectionEnd = end; redraw(); }

    public void clearSelection()             { selectedByte = -1; selectionAnchor = -1; selectionEnd = -1; }

    public long getSelectionStart()  { return (selectionAnchor < 0 || selectionEnd < 0) ? -1 : Math.min(selectionAnchor, selectionEnd); }
    public long getSelectionEnd()    { return (selectionAnchor < 0 || selectionEnd < 0) ? -1 : Math.max(selectionAnchor, selectionEnd); }
    public boolean hasRangeSelection() { return selectionAnchor >= 0 && selectionEnd >= 0 && selectionAnchor != selectionEnd; }

    // ── Hit-Test ──────────────────────────────────────────────────────────────

    public long byteAtCanvasPos(double x, double y) {
        if (reader == null || y < HEADER_H) return -1;
        int rowInView = (int) ((y - HEADER_H) / ROW_HEIGHT);
        long row = firstVisibleRow + rowInView;
        if (row >= reader.getTotalRows()) return -1;

        double hexStart = PAD_LEFT + ADDR_W + SEP1;
        for (int col = 0; col < 16; col++) {
            double extra = (col >= 8) ? SEP2 : 0;
            double cx = hexStart + col * HEX_COL_W + extra;
            if (x >= cx && x < cx + HEX_COL_W) {
                long b = row * 16 + col;
                return (b < reader.getFileSize()) ? b : -1;
            }
        }
        double asciiStart = PAD_LEFT + ADDR_W + SEP1 + 16 * HEX_COL_W + SEP2 + SEP1;
        for (int col = 0; col < 16; col++) {
            double cx = asciiStart + col * CHAR_W;
            if (x >= cx && x < cx + CHAR_W) {
                long b = row * 16 + col;
                return (b < reader.getFileSize()) ? b : -1;
            }
        }
        return -1;
    }

    // ── Rendering ─────────────────────────────────────────────────────────────

    public void redraw() {
        GraphicsContext gc = getGraphicsContext2D();
        double w = getWidth(), h = getHeight();

        Color bg         = c(D_BG,           L_BG);
        Color bgAlt      = c(D_BG_ALT,       L_BG_ALT);
        Color headerBg   = c(D_HEADER_BG,    L_HEADER_BG);
        Color gridLine   = c(D_GRID_LINE,    L_GRID_LINE);
        Color fgHeader   = c(D_FG_HEADER,    L_FG_HEADER);
        Color fgAddr     = c(D_FG_ADDR,      L_FG_ADDR);
        Color fgHex      = c(D_FG_HEX,       L_FG_HEX);
        Color fgHexZero  = c(D_FG_HEX_ZERO,  L_FG_HEX_ZERO);
        Color fgAscii    = c(D_FG_ASCII,     L_FG_ASCII);
        Color fgAsciiP   = c(D_FG_ASCII_PRI, L_FG_ASCII_PRI);
        Color fgSel      = c(D_FG_SEL,       L_FG_SEL);
        Color fgSelTxt   = c(D_FG_SEL_TXT,   L_FG_SEL_TXT);
        Color fgRange    = c(D_FG_RANGE,     L_FG_RANGE);
        Color fgRangeTxt = c(D_FG_RANGE_TXT, L_FG_RANGE_TXT);

        long rangeFrom = getSelectionStart();
        long rangeTo   = getSelectionEnd();
        boolean hasRange = hasRangeSelection();

        gc.setFill(bg);
        gc.fillRect(0, 0, w, h);

        // Header
        gc.setFill(headerBg);
        gc.fillRect(0, 0, w, HEADER_H);
        gc.setStroke(gridLine);
        gc.setLineWidth(1);
        gc.strokeLine(0, HEADER_H, w, HEADER_H);

        double hexStart   = PAD_LEFT + ADDR_W + SEP1;
        double asciiStart = hexStart + 16 * HEX_COL_W + SEP2 + SEP1;

        gc.setFont(headerFont);
        gc.setFill(fgHeader);
        gc.fillText("Offset", PAD_LEFT, HEADER_H - 4);
        for (int i = 0; i < 16; i++) {
            double extra = (i >= 8) ? SEP2 : 0;
            gc.fillText(String.format("%02X", i), hexStart + i * HEX_COL_W + extra, HEADER_H - 4);
        }
        gc.fillText("ASCII", asciiStart, HEADER_H - 4);

        //if (reader == null) {
        //    gc.setFill(fgHexZero);
        //    gc.setFont(Font.font("Segoe UI", FontWeight.NORMAL, 13));
        //    gc.fillText("Keine Datei geladen – 📂 Öffnen klicken.", PAD_LEFT + 20, h / 2);
        //   return;
        //}
        if (reader == null){ return;}

        int visRows = getVisibleRowCount();
        byte[] data;
        try {
            data = reader.readRows(firstVisibleRow, visRows + 1);
        } catch (Exception e) {
            gc.setFill(Color.RED);
            gc.fillText("Reading ERROR: " + e.getMessage(), 10, 60);
            return;
        }

        gc.setFont(monoFont);

        for (int r = 0; r <= visRows; r++) {
            long absRow = firstVisibleRow + r;
            if (absRow >= reader.getTotalRows()) break;

            double rowY = HEADER_H + r * ROW_HEIGHT;
            long rowByteStart = absRow * 16;

            if (r % 2 == 1) { gc.setFill(bgAlt); gc.fillRect(0, rowY, w, ROW_HEIGHT); }

            gc.setFill(fgAddr);
            gc.fillText(String.format("%016X", rowByteStart), PAD_LEFT, rowY + ROW_HEIGHT - 4);

            for (int col = 0; col < 16; col++) {
                int  dataIdx    = r * 16 + col;
                long byteOffset = rowByteStart + col;
                if (byteOffset >= reader.getFileSize()) break;

                double extra = (col >= 8) ? SEP2 : 0;
                double cx    = hexStart + col * HEX_COL_W + extra;
                double ax    = asciiStart + col * CHAR_W;

                byte braw = data[dataIdx];
                int  bval = braw & 0xFF;

                boolean isSingle = !hasRange && byteOffset == selectedByte;
                boolean inRange  =  hasRange && byteOffset >= rangeFrom && byteOffset <= rangeTo;

                // Hex
                if      (isSingle) { gc.setFill(fgSel);   gc.fillRect(cx-2, rowY+1, HEX_COL_W, ROW_HEIGHT-2); gc.setFill(fgSelTxt); }
                else if (inRange)  { gc.setFill(fgRange);  gc.fillRect(cx-2, rowY+1, HEX_COL_W, ROW_HEIGHT-2); gc.setFill(fgRangeTxt); }
                else               { gc.setFill(bval == 0 ? fgHexZero : fgHex); }
                gc.fillText(String.format("%02X", bval), cx, rowY + ROW_HEIGHT - 4);

                // ASCII
                char ch = (bval >= 32 && bval < 127) ? (char) bval : '.';
                if      (isSingle) { gc.setFill(fgSel);   gc.fillRect(ax-1, rowY+1, CHAR_W, ROW_HEIGHT-2); gc.setFill(fgSelTxt); }
                else if (inRange)  { gc.setFill(fgRange);  gc.fillRect(ax-1, rowY+1, CHAR_W, ROW_HEIGHT-2); gc.setFill(fgRangeTxt); }
                else               { gc.setFill(ch != '.' ? fgAsciiP : fgAscii); }
                gc.fillText(String.valueOf(ch), ax, rowY + ROW_HEIGHT - 4);
            }

            gc.setStroke(gridLine);
            gc.setLineWidth(0.5);
            gc.strokeLine(0, rowY + ROW_HEIGHT, w, rowY + ROW_HEIGHT);
        }
    }
}
