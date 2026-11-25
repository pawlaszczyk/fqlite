package fqlite.headless;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import fqlite.analyzer.BLOBCache;
import fqlite.base.Global;
import fqlite.types.BLOBElement;

/**
 * Result of {@link FQLiteHeadless#analyze}: schema objects and all recovered records of the
 * database file and, if present, its WAL archive and rollback journal.
 *
 * Note: This class is only be used in the headless variant of fqlite.
 *
 * @author pawlaszc
 */
public final class RecoveryResult {

	private final Path database;
	private final List<TableInfo> tables;
	private final List<RecoveredRecord> records;
	private final BLOBCache blobs;
	private final boolean walAnalysed, journalAnalysed;
	private final long durationMillis;

	RecoveryResult(Path database, List<TableInfo> tables, List<RecoveredRecord> records, BLOBCache blobs,
			boolean walAnalysed, boolean journalAnalysed, long durationMillis) {
		this.database = database;
		this.tables = Collections.unmodifiableList(tables);
		this.records = Collections.unmodifiableList(records);
		this.blobs = blobs;
		this.walAnalysed = walAnalysed;
		this.journalAnalysed = journalAnalysed;
		this.durationMillis = durationMillis;
	}

	public Path getDatabase() { return database; }

	/** tables, indexes and internal buckets, in the order FQLite found them */
	public List<TableInfo> getTables() { return tables; }

	/** schema object by name, or null */
	public TableInfo getTable(String name) {
		for (TableInfo t : tables)
			if (t.getName().equals(name))
				return t;
		return null;
	}

	/** all records of all sources */
	public List<RecoveredRecord> getRecords() { return records; }

	/** all records (of all sources) of one table or index */
	public List<RecoveredRecord> getRecords(String table) {
		return filter(r -> r.getTable().equals(table));
	}

	/** all records of one source */
	public List<RecoveredRecord> getRecords(Source source) {
		return filter(r -> r.getSource() == source);
	}

	/** all records with the given status (e.g. {@link Status#DELETED}) */
	public List<RecoveredRecord> getRecords(Status status) {
		return filter(r -> r.getStatus() == status);
	}

	/** all records that match {@code p} */
	public List<RecoveredRecord> filter(Predicate<RecoveredRecord> p) {
		List<RecoveredRecord> out = new ArrayList<>();
		for (RecoveredRecord r : records)
			if (p.test(r))
				out.add(r);
		return out;
	}

	public boolean isWalAnalysed() { return walAnalysed; }

	public boolean isJournalAnalysed() { return journalAnalysed; }

	public long getDurationMillis() { return durationMillis; }

	/**
	 * Bytes of a BLOB value: {@code column} is the index into {@link RecoveredRecord#getValues()}, whose
	 * value is a reference like {@code [BLOB-3]}. Returns null if the value is not a BLOB or
	 * its bytes are unknown.
	 */
	public byte[] getBlob(RecoveredRecord record, int column) {
		if (blobs == null || column < 0 || column >= record.getValues().size())
			return null;
		String v = record.getValues().get(column);
		if (v == null || !v.startsWith("[BLOB-"))
			return null;
		int to = v.indexOf(']');
		if (to < 0)
			return null;
		int n;
		try {
			n = Integer.parseInt(v.substring(6, to));
		} catch (NumberFormatException e) {
			return null;
		}
		// key as in Auxiliary.storeBLOB(): "<offset>-<n>" (offset with fewer than 3 digits: "<n>-<n>")
		String off = record.getOffset();
		String key = (off != null && off.length() > 2 ? off : String.valueOf(n)) + "-" + n;
		BLOBElement e = blobs.get(key);
		return e == null ? null : e.binary;
	}

	/**
	 * JSON text of a BLOB value stored as SQLite binary JSON (JSONB, written by {@code jsonb()},
	 * {@code jsonb_set()} etc. since SQLite 3.45). Returns null if the value is not a BLOB or not
	 * a valid JSONB array/object. The bytes themselves are available unchanged via {@link #getBlob}.
	 */
	public String getJson(RecoveredRecord record, int column) {
		return fqlite.blob.Jsonb.tryJson(getBlob(record, column));
	}

	/** number of records per table and status - a quick overview */
	public Map<String, Map<Status, Integer>> summary() {
		Map<String, Map<Status, Integer>> m = new LinkedHashMap<>();
		for (RecoveredRecord r : records)
			m.computeIfAbsent(r.getTable(), k -> new LinkedHashMap<>()).merge(r.getStatus(), 1, Integer::sum);
		return m;
	}

	// ------------------------------------------------------------------ JSON

	/** the result as a JSON document (see {@link #writeJson(Writer)}) */
	public String toJson() {
		java.io.StringWriter w = new java.io.StringWriter();
		try {
			writeJson(w);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		return w.toString();
	}

	/** writes the result as JSON (UTF-8) to {@code file} */
	public void writeJson(Path file) throws IOException {
		try (Writer w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
			writeJson(w);
		}
	}

	/**
	 * Writes the result as JSON:
	 * <pre>{
	 *   "tool": "FQLite", "version": "...", "database": "...", "durationMs": n,
	 *   "walAnalysed": b, "journalAnalysed": b,
	 *   "tables": [ {"name", "kind", "columns": [...], "types": [...], "sql"} ],
	 *   "records": [ {"source", "table", "status", "rawStatus", "rowid", "offset", "info",
	 *                 "partial", "superseded", "values": [...], "wal": {...}} ]
	 * }</pre>
	 */
	public void writeJson(Writer out) throws IOException {
		Writer w = out instanceof BufferedWriter || out instanceof java.io.StringWriter ? out : new BufferedWriter(out);
		w.write("{\n  \"tool\": \"FQLite\",\n  \"version\": ");
		str(w, Global.FQLITE_VERSION);
		w.write(",\n  \"database\": ");
		str(w, database.toString());
		w.write(",\n  \"durationMs\": " + durationMillis);
		w.write(",\n  \"walAnalysed\": " + walAnalysed);
		w.write(",\n  \"journalAnalysed\": " + journalAnalysed);
		w.write(",\n  \"tables\": [");
		boolean first = true;
		for (TableInfo t : tables) {
			w.write(first ? "\n    {\"name\": " : ",\n    {\"name\": ");
			first = false;
			str(w, t.getName());
			w.write(", \"kind\": ");
			str(w, t.getKind().name());
			w.write(", \"columns\": ");
			arr(w, t.getColumnNames());
			w.write(", \"types\": ");
			arr(w, t.getColumnTypes());
			w.write(", \"sql\": ");
			str(w, t.getSql());
			w.write('}');
		}
		w.write("\n  ],\n  \"records\": [");
		first = true;
		for (RecoveredRecord r : records) {
			w.write(first ? "\n    {\"source\": " : ",\n    {\"source\": ");
			first = false;
			str(w, r.getSource().name());
			w.write(", \"table\": ");
			str(w, r.getTable());
			w.write(", \"status\": ");
			str(w, r.getStatus().name());
			w.write(", \"rawStatus\": ");
			str(w, r.getRawStatus());
			w.write(", \"rowid\": ");
			str(w, r.getRowId());
			w.write(", \"offset\": ");
			str(w, r.getOffset());
			w.write(", \"info\": ");
			str(w, r.getInfo());
			w.write(", \"partial\": " + r.isPartial());
			w.write(", \"superseded\": " + r.isSuperseded());
			if (r.getSource() == Source.WAL) {
				w.write(", \"wal\": {\"commit\": ");
				str(w, r.getWalCommit());
				w.write(", \"dbpage\": ");
				str(w, r.getWalDbPage());
				w.write(", \"frame\": ");
				str(w, r.getWalFrame());
				w.write(", \"salt1\": ");
				str(w, r.getWalSalt1());
				w.write(", \"salt2\": ");
				str(w, r.getWalSalt2());
				w.write('}');
			}
			w.write(", \"values\": ");
			arr(w, r.getValues());
			w.write('}');
		}
		w.write("\n  ]\n}\n");
		w.flush();
	}

	private static void arr(Writer w, List<String> l) throws IOException {
		w.write('[');
		for (int i = 0; i < l.size(); i++) {
			if (i > 0)
				w.write(", ");
			str(w, l.get(i));
		}
		w.write(']');
	}

	static void str(Writer w, String s) throws IOException {
		if (s == null) {
			w.write("null");
			return;
		}
		w.write('"');
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			switch (c) {
				case '"' -> w.write("\\\"");
				case '\\' -> w.write("\\\\");
				case '\n' -> w.write("\\n");
				case '\r' -> w.write("\\r");
				case '\t' -> w.write("\\t");
				case '\b' -> w.write("\\b");
				case '\f' -> w.write("\\f");
				default -> {
					if (c < 0x20 || (Character.isSurrogate(c) && !validSurrogate(s, i)))
						w.write(String.format("\\u%04x", (int) c));
					else
						w.write(c);
				}
			}
		}
		w.write('"');
	}

	/** a lone surrogate (possible in carved data) would make the JSON invalid UTF-8 */
	private static boolean validSurrogate(String s, int i) {
		char c = s.charAt(i);
		if (Character.isHighSurrogate(c))
			return i + 1 < s.length() && Character.isLowSurrogate(s.charAt(i + 1));
		return i > 0 && Character.isHighSurrogate(s.charAt(i - 1));
	}

	// ------------------------------------------------------------------ CSV

	/**
	 * Writes one CSV file per table/index into {@code dir} ({@code <table>.csv}, WAL and
	 * journal records in {@code <table>.wal.csv} / {@code <table>.journal.csv}), separator
	 * {@code ;}, UTF-8, RFC-4180 quoting. Columns: _STATUS, _ROWID, _OFFSET, _INFO,
	 * (WAL: _COMMIT, _DBPAGE, _WALFRAME, _SALT1, _SALT2), then the table columns.
	 *
	 * @return the files written
	 */
	public List<Path> writeCsv(Path dir) throws IOException {
		Files.createDirectories(dir);
		Map<String, List<RecoveredRecord>> groups = new LinkedHashMap<>();
		for (RecoveredRecord r : records) {
			String suffix = switch (r.getSource()) {
				case DATABASE -> "";
				case WAL -> ".wal";
				case ROLLBACK_JOURNAL -> ".journal";
			};
			groups.computeIfAbsent(r.getTable() + suffix, k -> new ArrayList<>()).add(r);
		}
		List<Path> written = new ArrayList<>();
		for (Map.Entry<String, List<RecoveredRecord>> g : groups.entrySet()) {
			List<RecoveredRecord> rows = g.getValue();
			RecoveredRecord any = rows.get(0);
			Path f = dir.resolve(safeName(g.getKey()) + ".csv");
			int width = 0;
			for (RecoveredRecord r : rows)
				width = Math.max(width, r.getValues().size());
			List<String> header = new ArrayList<>(List.of("_STATUS", "_ROWID", "_OFFSET", "_INFO"));
			if (any.getSource() == Source.WAL)
				header.addAll(List.of("_COMMIT", "_DBPAGE", "_WALFRAME", "_SALT1", "_SALT2"));
			TableInfo ti = getTable(any.getTable());
			List<String> cols = ti == null ? List.of() : ti.getColumnNames();
			for (int i = 0; i < width; i++)
				header.add(i < cols.size() ? cols.get(i) : "col" + (i + 1));
			try (Writer w = Files.newBufferedWriter(f, StandardCharsets.UTF_8)) {
				csvLine(w, header);
				for (RecoveredRecord r : rows) {
					List<String> line = new ArrayList<>(List.of(r.getStatus().name(), nz(r.getRowId()),
							nz(r.getOffset()), nz(r.getInfo())));
					if (r.getSource() == Source.WAL)
						line.addAll(List.of(nz(r.getWalCommit()), nz(r.getWalDbPage()), nz(r.getWalFrame()),
								nz(r.getWalSalt1()), nz(r.getWalSalt2())));
					line.addAll(r.getValues());
					csvLine(w, line);
				}
			}
			written.add(f);
		}
		return written;
	}

	private static String nz(String s) {
		return s == null ? "" : s;
	}

	private static String safeName(String s) {
		return s.replaceAll("[^A-Za-z0-9._-]", "_");
	}

	private static void csvLine(Writer w, List<String> cells) throws IOException {
		for (int i = 0; i < cells.size(); i++) {
			if (i > 0)
				w.write(';');
			String c = cells.get(i);
			if (c == null)
				continue;
			if (c.indexOf(';') >= 0 || c.indexOf('"') >= 0 || c.indexOf('\n') >= 0 || c.indexOf('\r') >= 0)
				w.write('"' + c.replace("\"", "\"\"") + '"');
			else
				w.write(c);
		}
		w.write("\r\n");
	}
}
