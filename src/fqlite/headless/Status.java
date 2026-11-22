package fqlite.headless;

/**
 * Status of a recovered record within its source (database file, WAL archive or
 * rollback journal), taken from FQLite's status column.
 *
 * @author pawlaszc
 */
public enum Status {
	/** regular record of a b-tree page (in the database file: a live row) */
	LIVE,
	/** deleted record, carved from the free area of a page ("D") */
	DELETED,
	/** record recovered from a page of the freelist ("F") */
	FREELIST,
	/** any other status value - see {@link RecoveredRecord#getRawStatus()} */
	UNKNOWN;

	static Status of(String raw) {
		if (raw == null)
			return UNKNOWN;
		String s = raw.trim();
		if (s.isEmpty())
			return LIVE;
		if (s.startsWith("D"))
			return DELETED;
		if (s.startsWith("F"))
			return FREELIST;
		return UNKNOWN;
	}
}
