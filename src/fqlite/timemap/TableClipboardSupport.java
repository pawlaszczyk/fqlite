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

import java.util.HashSet;
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
        enableCellSelectionAndCopyShortcut(table);
        table.setContextMenu(new ContextMenu(copyMenuItem(table)));
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
