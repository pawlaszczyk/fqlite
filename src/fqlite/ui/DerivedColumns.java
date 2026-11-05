package fqlite.ui;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import fqlite.base.Job;
import fqlite.blob.BlobDecoder;
import fqlite.forensics.RowLayout;
import fqlite.types.BLOBElement;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.ObservableList;
import javafx.scene.control.MenuItem;
import javafx.scene.control.SeparatorMenuItem;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;

/**
 * Columns computed from the decoded content of another column ("Decode column into fields"):
 * one column per selected field path of a plist / NSKeyedArchiver / typedstream / protobuf /
 * JSON value, after removing any compression. The source data is not changed; the columns
 * exist only in the view and are marked with a "▸" in the header.
 */
public final class DerivedColumns {

	private DerivedColumns() {
	}

	public static final String DERIVED_KEY = "fqlite.derived";
	/** pseudo path: the decoded text / one-line summary */
	public static final String SUMMARY = "(decoded text)";
	/** pseudo path: the layers found, e.g. "zstd → bplist" */
	public static final String FORMAT = "(format)";

	/** the raw bytes of a cell: the cached BLOB or the UTF-8 bytes of the text; null if empty */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static byte[] cellBytes(Job job, TableColumn col, ObservableList<String> row) {
		if (col == null || row == null)
			return null;
		Object o = col.getCellObservableValue(row) == null ? null : col.getCellObservableValue(row).getValue();
		if (o == null)
			return null;
		String s = o.toString();
		if (s.isEmpty() || s.equals("null"))
			return null;
		if (s.startsWith("[BLOB-")) {
			int to = s.indexOf(']');
			if (to < 0 || job == null || job.bincache == null)
				return null;
			String idx = s.substring(6, to);
			String off = RowLayout.offset(row);
			String key = (off != null && off.trim().length() > 2 ? off.trim() : idx) + "-" + idx;
			BLOBElement e = job.bincache.get(key);
			return e == null ? null : e.binary;
		}
		return s.getBytes(StandardCharsets.UTF_8);
	}

	/** the decoded fields of one cell, including the pseudo paths */
	public static Map<String, String> decodeCell(byte[] b) {
		Map<String, String> m = new LinkedHashMap<>();
		if (b == null)
			return m;
		BlobDecoder.Result r = BlobDecoder.decode(b);
		m.put(FORMAT, r.chain().isEmpty() ? (r.value() instanceof String ? "text" : "binary") : r.chain()
				+ (r.error() != null ? " (" + r.error() + ")" : ""));
		String sum = BlobDecoder.summary(r);
		if (sum != null)
			m.put(SUMMARY, sum);
		m.putAll(BlobDecoder.fields(r));
		return m;
	}

	/** state shared by the derived columns of one source column */
	private static final class Source {
		final Job job;
		final TableColumn<?, ?> col;
		final Map<ObservableList<String>, Map<String, String>> cache = new WeakHashMap<>();

		Source(Job job, TableColumn<?, ?> col) {
			this.job = job;
			this.col = col;
		}

		@SuppressWarnings("rawtypes")
		Map<String, String> fields(ObservableList<String> row) {
			Map<String, String> f = cache.get(row);
			if (f == null) {
				try {
					f = decodeCell(cellBytes(job, (TableColumn) col, row));
				} catch (RuntimeException | StackOverflowError e) {
					f = Map.of(FORMAT, "error: " + e.getMessage());
				}
				cache.put(row, f);
			}
			return f;
		}
	}

	@SuppressWarnings("unchecked")
	private static List<TableColumn<?, ?>> derived(TableView<?> t) {
		return (List<TableColumn<?, ?>>) t.getProperties().computeIfAbsent(DERIVED_KEY, k -> new ArrayList<TableColumn<?, ?>>());
	}

	public static boolean hasDerived(TableView<?> t) {
		Object o = t.getProperties().get(DERIVED_KEY);
		return o instanceof List<?> l && !l.isEmpty();
	}

	/** adds one column per path behind the source column */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void add(TableView table, Job job, TableColumn src, List<String> paths) {
		Source source = new Source(job, src);
		int at = table.getColumns().indexOf(src) + 1;
		for (TableColumn<?, ?> c : derived(table))
			if (c.getProperties().get("fqlite.source") == src)
				at = Math.max(at, table.getColumns().indexOf(c) + 1);
		String base = src.getText();
		for (String path : paths) {
			TableColumn<ObservableList<String>, String> c = new TableColumn<>(base + " \u25B8 " + path);
			c.getProperties().put("fqlite.source", src);
			c.getProperties().put("fqlite.path", path);
			c.getProperties().put("fqlite.title", base + " ▸ " + path);
			c.setCellValueFactory(p -> {
				if (p.getValue() == null)
					return new SimpleStringProperty("");
				String v = source.fields(p.getValue()).get(path);
				return new SimpleStringProperty(v == null ? "" : v);
			});
			c.setComparator(NATURAL);
			c.setPrefWidth(Math.min(320, Math.max(110, 8 * Math.min(40, path.length() + base.length()))));
			c.setStyle("-fx-background-color: rgba(59,130,246,0.07);");
			table.getColumns().add(Math.min(at, table.getColumns().size()), c);
			at++;
			derived(table).add(c);
		}
	}

	/** removes all derived columns (or those of one source column) */
	@SuppressWarnings("rawtypes")
	public static void remove(TableView table, TableColumn src) {
		List<TableColumn<?, ?>> l = derived(table);
		List<TableColumn<?, ?>> gone = new ArrayList<>();
		for (TableColumn<?, ?> c : l)
			if (src == null || c.getProperties().get("fqlite.source") == src)
				gone.add(c);
		table.getColumns().removeAll(gone);
		l.removeAll(gone);
	}

	/** header text of a column */
	public static String title(TableColumn<?, ?> c) {
		Object t = c.getProperties().get("fqlite.title");
		return t != null ? t.toString() : c.getText();
	}

	public static boolean isDerived(TableColumn<?, ?> c) {
		return c != null && c.getProperties().containsKey("fqlite.source");
	}

	/**
	 * Index of a view column among the original (not derived) columns - the index the hex
	 * dump list and the SQL types refer to; -1 for a derived column.
	 */
	public static int baseIndex(TableView<?> t, int viewIndex) {
		if (viewIndex < 0 || viewIndex >= t.getColumns().size() || isDerived(t.getColumns().get(viewIndex)))
			return -1;
		int n = 0;
		for (int i = 0; i < viewIndex; i++)
			if (!isDerived(t.getColumns().get(i)))
				n++;
		return n;
	}

	/** number of original (not derived) columns */
	public static int baseCount(TableView<?> t) {
		int n = 0;
		for (TableColumn<?, ?> c : t.getColumns())
			if (!isDerived(c))
				n++;
		return n;
	}

	/** context-menu entries for the column of the selected cell */
	@SuppressWarnings("rawtypes")
	public static List<MenuItem> menuItems(javafx.stage.Window owner, TableView table, Job job, TableColumn col) {
		List<MenuItem> items = new ArrayList<>();
		if (col == null)
			return items;
		TableColumn src = col.getProperties().get("fqlite.source") instanceof TableColumn s ? s : col;
		MenuItem decode = new MenuItem("Decode column \"" + title(src) + "\" into fields...");
		decode.setOnAction(e -> new DecodeColumnDialog(owner, table, job, src).show());
		items.add(new SeparatorMenuItem());
		items.add(decode);
		if (hasDerived(table)) {
			MenuItem rm = new MenuItem("Remove decoded columns");
			rm.setOnAction(e -> remove(table, null));
			items.add(rm);
		}
		return items;
	}

	/** numbers numerically, everything else as text */
	public static final Comparator<String> NATURAL = (a, b) -> {
		if (a == null || a.isEmpty())
			return b == null || b.isEmpty() ? 0 : -1;
		if (b == null || b.isEmpty())
			return 1;
		try {
			return Double.compare(Double.parseDouble(a), Double.parseDouble(b));
		} catch (NumberFormatException e) {
			return a.compareToIgnoreCase(b);
		}
	};
}
