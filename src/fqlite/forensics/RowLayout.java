package fqlite.forensics;

import java.util.List;

/**
 * Access to the fields of a result row independent of whether the GUI has already
 * prepended its line number.
 * <pre>
 * raw (Job/WALReader/RollbackJournalReader): [table, pll, rowid, status, offset, values...]
 *                     WAL rows additionally: [table, pll, rowid, status, offset, commit, dbpage, frame, salt1, salt2, values...]
 * after GUI.update_table():                   [lineno, table, pll, ...]
 * </pre>
 * GUI.update_table() inserts the line number into the very same list objects, on the JavaFX
 * thread, possibly while a WAL or journal analysis still reads them - so every reader has to
 * determine the layout per row.
 */
public final class RowLayout {

	private RowLayout() {
	}

	/** 1 if a line number was prepended, else 0 */
	public static int base(List<String> row) {
		if (row == null || row.size() < 2)
			return 0;
		String s = row.get(0);
		if (s == null || s.isEmpty())
			return 0;
		for (int i = 0; i < s.length(); i++)
			if (!Character.isDigit(s.charAt(i)))
				return 0;
		return 1;
	}

	public static String get(List<String> row, int field) {
		int i = base(row) + field;
		return row != null && i < row.size() ? row.get(i) : null;
	}

	public static void set(List<String> row, int field, String value) {
		row.set(base(row) + field, value);
	}

	public static final int TABLE = 0, PLL = 1, ROWID = 2, STATUS = 3, OFFSET = 4, VALUES = 5;
	public static final int WAL_COMMIT = 5, WAL_DBPAGE = 6, WAL_FRAME = 7, WAL_SALT1 = 8, WAL_SALT2 = 9, WAL_VALUES = 10;

	public static String pll(List<String> r) { return get(r, PLL); }
	public static String rowid(List<String> r) { return get(r, ROWID); }
	public static String status(List<String> r) { return get(r, STATUS); }
	public static String offset(List<String> r) { return get(r, OFFSET); }

	/** column values (DB / journal rows) */
	public static List<String> values(List<String> r) {
		int b = base(r) + VALUES;
		return b <= r.size() ? r.subList(b, r.size()) : List.of();
	}

	/** column values of a WAL row */
	public static List<String> walValues(List<String> r) {
		int b = base(r) + WAL_VALUES;
		return b <= r.size() ? r.subList(b, r.size()) : List.of();
	}

	public static long number(String s) {
		try {
			return Long.parseLong(s.trim());
		} catch (Exception e) {
			return -1;
		}
	}

	/** number of fields (without the line number) */
	public static int size(List<String> r) {
		return r == null ? 0 : r.size() - base(r);
	}
}
