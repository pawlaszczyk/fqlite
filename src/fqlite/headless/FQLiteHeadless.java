package fqlite.headless;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import fqlite.base.Global;
import fqlite.base.Job;
import fqlite.base.RollbackJournalReader;
import fqlite.base.WALReader;
import fqlite.descriptor.IndexDescriptor;
import fqlite.descriptor.TableDescriptor;
import javafx.collections.ObservableList;

/**
 * Headless entry point of FQLite: runs the forensic recovery engine on a SQLite database
 * without any user interface and returns the recovered records.
 *
 * <pre>{@code
 * RecoveryResult r = FQLiteHeadless.analyze(Path.of("evidence.db"));
 * for (RecoveredRecord rec : r.getRecords("messages"))
 *     if (rec.getStatus() == Status.DELETED)
 *         System.out.println(rec.getValues());
 * r.writeJson(Path.of("evidence.json"));
 * }</pre>
 *
 * <p>Runtime requirements: Java 21, {@code org.openjfx:javafx-base} (only the collection
 * classes - no JavaFX toolkit, no display) and {@code org.antlr:antlr4-runtime:4.8}.
 * The database file is only read, never written. A companion {@code <db>-wal} or
 * {@code <db>-journal} is analysed as well (see {@link Options}).</p>
 *
 * <p>Thread safety: every call works on its own engine instance; the number of worker
 * threads is a JVM-wide setting of FQLite ({@link Options#threads(int)}).</p>
 */
public final class FQLiteHeadless {

	static {
		// must be set before the first FQLite class is initialised: the logger then
		// creates no JavaFX log window (see fqlite.log.AppLog)
		System.setProperty("fqlite.headless", "true");
	}

	private FQLiteHeadless() {
	}

	/** FQLite version of the engine */
	public static String version() {
		return Global.FQLITE_VERSION;
	}

	/** analyses {@code db} with the default {@link Options} */
	public static RecoveryResult analyze(Path db) throws IOException {
		return analyze(db, Options.defaults());
	}

	/** analyses {@code db} with the default {@link Options} */
	public static RecoveryResult analyze(String db) throws IOException {
		return analyze(Path.of(db), Options.defaults());
	}

	/**
	 * Runs the recovery on {@code db}.
	 *
	 * @throws FileNotFoundException if {@code db} is not a readable file
	 * @throws IOException           if the engine fails
	 */
	public static RecoveryResult analyze(Path db, Options options) throws IOException {
		if (options == null)
			options = Options.defaults();
		File file = db.toAbsolutePath().toFile();
		if (!file.isFile() || !Files.isReadable(file.toPath()))
			throw new FileNotFoundException("not a readable file: " + file);

		Global.numberofThreads = options.threads;
		String path = file.getAbsolutePath();
		File walFile = new File(path + "-wal");
		File rjFile = new File(path + "-journal");
		boolean useWal = options.wal && walFile.isFile() && walFile.length() > 0;
		boolean useJournal = options.journal && rjFile.isFile() && rjFile.length() > 0;

		long start = System.currentTimeMillis();
		Job job = new Job();
		job.setPath(path);
		if (useWal)
			job.readWAL = true;        // as the GUI: the schema may only be in the WAL archive

		try {
			job.run(path);
		} catch (RuntimeException e) {
			throw new IOException("FQLite recovery failed for " + path + ": " + e.getMessage(), e);
		}
		if (job.headers.isEmpty() && job.resultlist.isEmpty())
			throw new IOException("no SQLite schema or records found in " + path
					+ " (not a SQLite database, or the file is encrypted)");

		if (options.loadAllIndexEntries)
			job.loadAllDeferredIndexes();

		List<RecoveredRecord> records = new ArrayList<>();
		collect(records, Source.DATABASE, job.resultlist);

		if (useJournal) {
			try {
				RollbackJournalReader rol = new RollbackJournalReader(rjFile.getAbsolutePath(), job);
				rol.ps = job.ps;
				rol.parse();
				job.rol = rol;
				collect(records, Source.ROLLBACK_JOURNAL, rol.resultlist);
			} catch (IOException | RuntimeException e) {
				throw new IOException("rollback journal could not be analysed: " + rjFile + ": " + e.getMessage(), e);
			}
		}
		if (useWal) {
			try {
				WALReader wal = new WALReader(walFile.getAbsolutePath(), job);
				job.wal = wal;
				wal.parse();
				collect(records, Source.WAL, wal.resultlist);
			} catch (RuntimeException e) {
				throw new IOException("WAL archive could not be analysed: " + walFile + ": " + e.getMessage(), e);
			}
		}

		Map<String, TableInfo> tables = new LinkedHashMap<>();
		for (TableDescriptor td : job.headers)
			if (td.tblname != null && !tables.containsKey(td.tblname))
				tables.put(td.tblname, new TableInfo(td.tblname,
						td.tblname.startsWith("__") ? TableInfo.Kind.INTERNAL : TableInfo.Kind.TABLE,
						withoutWalColumns(td.columnnames), td.sqltypes, td.sql));
		for (IndexDescriptor id : job.indices)
			if (id.idxname != null && !id.idxname.isEmpty() && !tables.containsKey(id.idxname))
				tables.put(id.idxname, new TableInfo(id.idxname, TableInfo.Kind.INDEX,
						id.columnnames, id.sqltypes, safeSql(id)));
		for (RecoveredRecord r : records)          // buckets without a descriptor (e.g. unassigned records)
			tables.computeIfAbsent(r.getTable(), n -> new TableInfo(n,
					n.startsWith("__") ? TableInfo.Kind.INTERNAL : TableInfo.Kind.TABLE, null, null, null));

		long duration = System.currentTimeMillis() - start;
		return new RecoveryResult(file.toPath(), new ArrayList<>(tables.values()), records, job.bincache,
				useWal, useJournal, duration);
	}

	private static final List<String> WAL_COLUMNS = List.of("commit", "dbpage", "walframe", "salt1", "salt2");

	/** Job adds the WAL meta columns to the sqlite_master descriptor if a WAL is read - drop them */
	private static List<String> withoutWalColumns(List<String> cols) {
		if (cols != null && cols.size() >= WAL_COLUMNS.size() && cols.subList(0, WAL_COLUMNS.size()).equals(WAL_COLUMNS))
			return cols.subList(WAL_COLUMNS.size(), cols.size());
		return cols;
	}

	private static String safeSql(IndexDescriptor id) {
		try {
			return id.getSql();
		} catch (RuntimeException e) {
			return null;
		}
	}

	private static void collect(List<RecoveredRecord> out, Source src,
			Map<String, ObservableList<ObservableList<String>>> buckets) {
		if (buckets == null)
			return;
		for (Map.Entry<String, ObservableList<ObservableList<String>>> e : buckets.entrySet()) {
			String table = e.getKey();
			ObservableList<ObservableList<String>> rows = e.getValue();
			if (rows == null)
				continue;
			synchronized (rows) {
				for (ObservableList<String> row : rows)
					if (row != null)
						out.add(new RecoveredRecord(src, table, row));
			}
		}
	}
}
