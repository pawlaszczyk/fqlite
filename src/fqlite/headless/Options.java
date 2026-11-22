package fqlite.headless;

/**
 * Settings of an analysis (fluent setters). Defaults: WAL archive and rollback journal next
 * to the database are analysed, all index entries are loaded, one worker thread.
 *
 * @author pawlaszc
 */
public final class Options {

	boolean wal = true;
	boolean journal = true;
	boolean loadAllIndexEntries = true;
	int threads = 1;

	public static Options defaults() {
		return new Options();
	}

	/** analyse {@code <db>-wal} if present (default true) */
	public Options wal(boolean on) { wal = on; return this; }

	/** analyse {@code <db>-journal} if present (default true) */
	public Options journal(boolean on) { journal = on; return this; }

	/**
	 * load the live entries of large indexes, which FQLite otherwise loads on demand
	 * (default true - otherwise these entries are missing from the result)
	 */
	public Options loadAllIndexEntries(boolean on) { loadAllIndexEntries = on; return this; }

	/**
	 * number of worker threads of the recovery engine (default 1). Note: FQLite keeps this
	 * value globally ({@code Global.numberofThreads}), it applies to all analyses of the JVM.
	 */
	public Options threads(int n) {
		if (n < 1)
			throw new IllegalArgumentException("threads must be >= 1");
		threads = n;
		return this;
	}
}
