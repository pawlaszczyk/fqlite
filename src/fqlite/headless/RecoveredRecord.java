package fqlite.headless;

import java.util.Collections;
import java.util.List;

/**
 * One record recovered by FQLite.
 *
 * <p>FQLite's internal row layout is
 * {@code [table, info, rowid, status, offset, (WAL: commit, dbpage, walframe, salt1, salt2,) values...]};
 * this class exposes the parts by name. Values are FQLite's display strings; a BLOB value
 * appears as a reference like {@code [BLOB-3]} whose bytes are available through
 * {@link RecoveryResult#getBlob(RecoveredRecord, int)}.</p>
 *
 * @author pawlaszc
 */
public final class RecoveredRecord {

	private static final String[] PARTIAL_FLAGS = { "⚠ truncated", "⚠ overflow overwritten" };

	private final Source source;
	private final String table;
	private final String info;
	private final String rowId;
	private final String rawStatus;
	private final String offset;
	private final String walCommit, walDbPage, walFrame, walSalt1, walSalt2;
	private final List<String> values;

	RecoveredRecord(Source source, String bucket, List<String> row) {
		this.source = source;
		this.table = bucket;
		this.info = get(row, 1);
		this.rowId = get(row, 2);
		this.rawStatus = get(row, 3);
		this.offset = get(row, 4);
		int first = 5;
		if (source == Source.WAL) {
			walCommit = get(row, 5);
			walDbPage = get(row, 6);
			walFrame = get(row, 7);
			walSalt1 = get(row, 8);
			walSalt2 = get(row, 9);
			first = 10;
		} else {
			walCommit = walDbPage = walFrame = walSalt1 = walSalt2 = null;
		}
		this.values = row.size() > first
				? Collections.unmodifiableList(new java.util.ArrayList<>(row.subList(first, row.size())))
				: List.of();
	}

	private static String get(List<String> row, int i) {
		return i < row.size() ? row.get(i) : null;
	}

	public Source getSource() { return source; }

	/** table or index the record belongs to (FQLite's result bucket) */
	public String getTable() { return table; }

	/** status derived from {@link #getRawStatus()} */
	public Status getStatus() { return Status.of(rawStatus); }

	/** FQLite's status column as is ({@code " "}, {@code "D"}, {@code "F"}, ...) */
	public String getRawStatus() { return rawStatus; }

	/** row id, as text ({@code -1} or empty if unknown, e.g. for a carved record) */
	public String getRowId() { return rowId; }

	/** byte offset of the record in its source file, as text */
	public String getOffset() { return offset; }

	/**
	 * FQLite's info column: payload/header length {@code [pll|hl]} plus flags such as
	 * {@code "old version (UPDATE)"}, {@code "updated in WAL frame n"} or {@code "⚠ truncated"}.
	 */
	public String getInfo() { return info; }

	/** true if FQLite marks the record as incomplete (truncated or overflow overwritten) */
	public boolean isPartial() {
		if (info == null)
			return false;
		for (String f : PARTIAL_FLAGS)
			if (info.contains(f))
				return true;
		return false;
	}

	/** true if FQLite marks the record as an older version (superseded by an UPDATE/DELETE) */
	public boolean isSuperseded() {
		return info != null && (info.contains("old version (UPDATE)") || info.contains("updated in WAL frame")
				|| info.contains("deleted in WAL frame") || info.contains("deleted after journal snapshot"));
	}

	/** column values (without FQLite's meta columns) */
	public List<String> getValues() { return values; }

	/** WAL only: commit flag of the frame, otherwise null */
	public String getWalCommit() { return walCommit; }

	/** WAL only: database page number of the frame, otherwise null */
	public String getWalDbPage() { return walDbPage; }

	/** WAL only: frame number, otherwise null */
	public String getWalFrame() { return walFrame; }

	/** WAL only: salt-1 of the frame, otherwise null */
	public String getWalSalt1() { return walSalt1; }

	/** WAL only: salt-2 of the frame, otherwise null */
	public String getWalSalt2() { return walSalt2; }

	@Override
	public String toString() {
		return source + ":" + table + " " + getStatus() + " rowid=" + rowId + " @" + offset + " " + values;
	}
}
