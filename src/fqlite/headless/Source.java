package fqlite.headless;

/** File a record was recovered from. */
public enum Source {
	/** the SQLite database file itself */
	DATABASE,
	/** the write-ahead log ({@code <db>-wal}) */
	WAL,
	/** the rollback journal ({@code <db>-journal}) */
	ROLLBACK_JOURNAL
}
