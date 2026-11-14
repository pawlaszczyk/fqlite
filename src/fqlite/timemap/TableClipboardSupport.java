package fqlite.timemap;

import javafx.collections.ObservableList;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.MenuItem;
import javafx.scene.control.SelectionMode;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TablePosition;
import javafx.scene.control.TableView;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.scene.input.KeyEvent;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Adds Excel-style "copy the selected cells to the clipboard" support to a
 * {@link TableView} of forensic records: enables multi-cell range
 * selection, wires Ctrl/Cmd+C, and hands back a ready-made context-menu
 * item for callers that want to merge it into their own menu (one of these
 * tables already installs a per-row context menu of its own).
 * <p>
 * Copied text is tab-separated between columns and newline-separated
 * between rows, matching what spreadsheet apps paste back in — including a
 * blank field for any unselected cell inside the selection's bounding
 * rectangle, so a ragged multi-row/multi-column selection still lines up
 * column-wise instead of shifting values left.
 */
final class TableClipboardSupport {

    private TableClipboardSupport() {}

    private static final KeyCodeCombination COPY_COMBO =
            new KeyCodeCombination(KeyCode.C, KeyCombination.SHORTCUT_DOWN);

    /**
     * Enables multi-cell selection on {@code table} and installs the
     * Ctrl/Cmd+C key handler. Does not touch the table's context menu —
     * call {@link #copyMenuItem(TableView)} separately and add it wherever
     * fits (a fresh {@link ContextMenu}, or merged into an existing one).
     */
    static void enableCellSelectionAndCopyShortcut(TableView<?> table) {
        table.getSelectionModel().setCellSelectionEnabled(true);
        table.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);

        table.addEventHandler(KeyEvent.KEY_PRESSED, e -> {
            if (COPY_COMBO.match(e)) {
                copySelectionToClipboard(table);
                e.consume();
            }
        });
    }

    /**
     * A ready-made "Kopieren"/"Copy" menu item wired to copy the table's
     * current cell selection. The accelerator is shown as a hint on the
     * item; the actual Ctrl/Cmd+C shortcut is handled independently by
     * {@link #enableCellSelectionAndCopyShortcut} so it works whether or
     * not the context menu has ever been opened.
     */
    static MenuItem copyMenuItem(TableView<?> table) {
        MenuItem item = new MenuItem(Lang.t("Kopieren", "Copy"));
        item.setAccelerator(COPY_COMBO);
        item.setOnAction(e -> copySelectionToClipboard(table));
        return item;
    }

    /**
     * Convenience for the common case: installs the key shortcut and gives
     * the table its own fresh context menu containing only the copy item.
     * Skip this and compose {@link #enableCellSelectionAndCopyShortcut} +
     * {@link #copyMenuItem} manually when the table already has its own
     * context menu (e.g. a per-row one) to merge into instead.
     */
    static void install(TableView<?> table) {
        install(table, "lighthouse_tabelle.csv");
    }

    /** As {@link #install(TableView)}, with "Export to CSV…" saving under {@code fileName} by default. */
    static void install(TableView<?> table, String fileName) {
        enableCellSelectionAndCopyShortcut(table);
        table.setContextMenu(new ContextMenu(copyMenuItem(table), exportMenuItem(table, fileName)));
    }

    // ── CSV export ───────────────────────────────────────────────────────────

    /** source of a complete export, if the table holds only part of the result */
    interface CsvSource {
        /** the complete CSV (header and every row), or null to export the table */
        java.nio.file.Path file();
    }

    /**
     * "Als CSV exportieren…" / "Export to CSV…": writes every row of the table
     * (not only the selected ones) with its column headers.
     */
    static MenuItem exportMenuItem(TableView<?> table, String fileName) {
        return exportMenuItem(table, fileName, null);
    }

    /** @param full complete result (written instead of the table rows when it returns a file) */
    static MenuItem exportMenuItem(TableView<?> table, String fileName, CsvSource full) {
        return exportMenuItem(table, () -> fileName, full);
    }

    /** @param fileName proposed file name, asked when the item is chosen */
    static MenuItem exportMenuItem(TableView<?> table, java.util.function.Supplier<String> fileName, CsvSource full) {
        MenuItem item = new MenuItem(Lang.t("Als CSV exportieren…", "Export to CSV…"));
        Lang.addListener(() -> item.setText(Lang.t("Als CSV exportieren…", "Export to CSV…")));
        item.setOnAction(e -> exportCsv(table, fileName.get(), full));
        return item;
    }

    private static java.io.File lastDir;

    static void exportCsv(TableView<?> table, String fileName, CsvSource full) {
        javafx.stage.FileChooser fc = new javafx.stage.FileChooser();
        fc.setTitle(Lang.t("Als CSV exportieren", "Export to CSV"));
        fc.setInitialFileName(fileName);
        fc.getExtensionFilters().add(new javafx.stage.FileChooser.ExtensionFilter("CSV", "*.csv"));
        if (lastDir != null && lastDir.isDirectory()) fc.setInitialDirectory(lastDir);
        java.io.File f = fc.showSaveDialog(table.getScene() != null ? table.getScene().getWindow() : null);
        if (f == null) return;
        lastDir = f.getParentFile();
        java.nio.file.Path src = full != null ? full.file() : null;
        // the cell texts are read on the FX thread, the file is written in the background
        List<String> lines = src != null ? null : csvLines(table);
        Thread t = new Thread(() -> {
            try {
                if (src != null) {
                    java.nio.file.Files.copy(src, f.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                } else {
                    try (java.io.Writer w = csvWriter(f.toPath())) {
                        for (String l : lines) w.write(l);
                    }
                }
            } catch (java.io.IOException ex) {
                javafx.application.Platform.runLater(() -> new javafx.scene.control.Alert(
                        javafx.scene.control.Alert.AlertType.ERROR,
                        Lang.t("CSV-Export fehlgeschlagen: ", "CSV export failed: ") + ex.getMessage()).showAndWait());
            }
        }, "csv-export");
        t.setDaemon(true);
        t.start();
    }

    /** header and every row of the table, each a CSV line with line end */
    private static List<String> csvLines(TableView<?> table) {
        List<? extends TableColumn<?, ?>> cols = table.getVisibleLeafColumns();
        List<String> lines = new ArrayList<>(table.getItems().size() + 1);
        List<String> head = new ArrayList<>(cols.size());
        for (TableColumn<?, ?> c : cols) head.add(c.getText());
        lines.add(csvLine(head));
        for (int r = 0; r < table.getItems().size(); r++) {
            List<String> cells = new ArrayList<>(cols.size());
            for (TableColumn<?, ?> c : cols) {
                Object v = c.getCellData(r);
                cells.add(v == null ? "" : v.toString());
            }
            lines.add(csvLine(cells));
        }
        return lines;
    }

    /**
     * Writer for a CSV file: UTF-8 with byte order mark, so that spreadsheet
     * programs read umlauts correctly.
     */
    static java.io.Writer csvWriter(java.nio.file.Path p) throws java.io.IOException {
        java.io.Writer w = java.nio.file.Files.newBufferedWriter(p, java.nio.charset.StandardCharsets.UTF_8);
        w.write('\uFEFF');
        return w;
    }

    /** one CSV line (with line end) in the separator of the settings */
    static String csvLine(List<String> cells) {
        String sep = fqlite.base.Global.CSV_SEPARATOR;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cells.size(); i++) {
            if (i > 0) sb.append(sep);
            String v = cells.get(i) == null ? "" : cells.get(i);
            if (v.contains(sep) || v.contains("\"") || v.contains("\n") || v.contains("\r")) {
                sb.append('"').append(v.replace("\"", "\"\"")).append('"');
            } else {
                sb.append(v);
            }
        }
        return sb.append("\r\n").toString();
    }

    /**
     * Copies the current selection as tab-separated cell text, one line
     * per row, preserving the selection's row/column layout.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void copySelectionToClipboard(TableView<?> table) {
        ObservableList<TablePosition> cells = table.getSelectionModel().getSelectedCells();
        if (cells.isEmpty()) return;

        int minRow = Integer.MAX_VALUE, maxRow = Integer.MIN_VALUE;
        int minCol = Integer.MAX_VALUE, maxCol = Integer.MIN_VALUE;
        Set<Long> selected = new HashSet<>();
        for (Object o : cells) {
            TablePosition p = (TablePosition) o;
            int r = p.getRow();
            int c = p.getColumn();
            minRow = Math.min(minRow, r);
            maxRow = Math.max(maxRow, r);
            minCol = Math.min(minCol, c);
            maxCol = Math.max(maxCol, c);
            selected.add(cellKey(r, c));
        }

        int colCount = table.getColumns().size();
        StringBuilder sb = new StringBuilder();
        for (int r = minRow; r <= maxRow; r++) {
            for (int c = minCol; c <= maxCol; c++) {
                if (c > minCol) sb.append('\t');
                if (selected.contains(cellKey(r, c)) && c >= 0 && c < colCount) {
                    TableColumn<?, ?> col = table.getColumns().get(c);
                    Object value = col.getCellData(r);
                    if (value != null) sb.append(value);
                }
            }
            sb.append('\n');
        }
        if (sb.length() > 0) sb.setLength(sb.length() - 1); // drop trailing newline

        ClipboardContent content = new ClipboardContent();
        content.putString(sb.toString());
        Clipboard.getSystemClipboard().setContent(content);
    }

    private static long cellKey(int row, int col) {
        return ((long) row << 32) | (col & 0xFFFFFFFFL);
    }
}
