package fqlite.forensics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import fqlite.base.Global;
import fqlite.base.Job;
import fqlite.descriptor.TableDescriptor;
import fqlite.util.Auxiliary;
import javafx.collections.ObservableList;

/**
 * Classification and confidence of every recovered record of the database file:
 * <ul>
 * <li>class: <i>older version</i> (an earlier state of a row that still exists),
 * <i>re-inserted</i> (deleted, the same content exists again under another rowid),
 * <i>deleted</i>, <i>unreferenced page</i> (a page no b-tree or freelist refers to) and
 * <i>index entry</i> (carved from an index);</li>
 * <li>where it was found (freeblock, unallocated area, freelist page, ...);</li>
 * <li>confidence high / medium / low with the reasons (completeness, reconstructed header, lost
 * rowid, values that do not fit the column types).</li>
 * </ul>
 */
public final class RecordClassifier {

	private RecordClassifier() {
	}

	public enum Kind {
		OLDER_VERSION("older version"), REINSERTED("re-inserted"), DELETED("deleted"), UNREFERENCED("unreferenced page"),
		INDEX_ENTRY("index entry");

		public final String label;

		Kind(String l) {
			label = l;
		}
	}

	public enum Confidence {
		HIGH("high"), MEDIUM("medium"), LOW("low");

		public final String label;

		Confidence(String l) {
			label = l;
		}
	}

	public record Result(String table, long rowid, Kind kind, Confidence confidence, int score, String foundIn, String reasons,
			String location, String values, List<String> row) {
	}

	/** classifies all recovered records of the database (and the carved index entries, if given) */
	public static List<Result> classify(Job job, List<IndexCarver.Entry> indexEntries) {
		List<Result> out = new ArrayList<>();
		if (job == null || job.resultlist == null)
			return out;
		PageMap pm = null;
		try {
			pm = PageMap.analyse(job.path);
		} catch (IOException | RuntimeException e) {
			// classification without page map
		}
		for (String table : new java.util.TreeSet<>(job.resultlist.keySet())) {
			if (table.startsWith("__"))
				continue;
			ObservableList<ObservableList<String>> rows = job.resultlist.get(table);
			if (rows == null)
				continue;
			TableDescriptor td = descriptor(job, table);
			classifyTable(job, table, td, List.copyOf(rows), pm, out);
		}
		Set<Long> seenOffsets = new HashSet<>();
		for (Result r : out)
			if (r.kind() == Kind.INDEX_ENTRY && r.row() != null)
				seenOffsets.add(RowLayout.number(RowLayout.offset(r.row())));
		if (indexEntries != null)
			for (IndexCarver.Entry e : indexEntries) {
				if (seenOffsets.contains(e.offset()))
					continue;                       // the same entry is already in the index view
				int score = switch (e.confidence().toLowerCase().startsWith("high") ? 2 : e.confidence().toLowerCase().startsWith("medium") ? 1 : 0) {
				case 2 -> 85;
				case 1 -> 60;
				default -> 35;
				};
				StringBuilder v = new StringBuilder();
				for (int i = 0; i < e.values().size() - 1; i++)
					v.append(i > 0 ? " | " : "").append(i < e.columns().size() ? e.columns().get(i) + "=" : "").append(e.values().get(i));
				out.add(new Result(e.table().isEmpty() ? e.index() : e.table(), e.rowid(), Kind.INDEX_ENTRY, level(score), score,
						e.where(), "index " + e.index() + "; " + e.confidence() + "; row: " + e.rowState(),
						"offset " + e.offset(), v.toString(), null));
			}
		return out;
	}

	private static final Map<String, Object[]> CACHE = new java.util.concurrent.ConcurrentHashMap<>();

	/** classification of a single result row of the database view (null if it is a regular live record) */
	public static Result classifyRow(Job job, String table, List<String> row) {
		if (job == null || job.resultlist == null || job.resultlist.get(table) == null)
			return null;
		List<ObservableList<String>> rows = List.copyOf(job.resultlist.get(table));
		java.io.File f = new java.io.File(job.path);
		String key = job.path + "|" + table;
		Object[] c = CACHE.get(key);
		List<Result> res;
		if (c != null && (int) c[0] == rows.size() && (long) c[1] == f.lastModified()) {
			@SuppressWarnings("unchecked")
			List<Result> cached = (List<Result>) c[2];
			res = cached;
		} else {
			PageMap pm = null;
			try {
				pm = PageMap.analyse(job.path);
			} catch (IOException | RuntimeException e) {
				// without page map
			}
			res = new ArrayList<>();
			classifyTable(job, table, descriptor(job, table), rows, pm, res);
			CACHE.put(key, new Object[] { rows.size(), f.lastModified(), res });
		}
		for (Result r : res)
			if (r.row() == row)
				return r;
		return null;
	}

	static boolean isIndex(Job job, String table) {
		if (job.indices != null)
			for (fqlite.descriptor.IndexDescriptor id : job.indices)
				if (table.equals(id.idxname))
					return true;
		return false;
	}

	static TableDescriptor descriptor(Job job, String table) {
		if (job.headers != null)
			for (TableDescriptor td : job.headers)
				if (table.equals(td.tblname))
					return td;
		return null;
	}

	static Confidence level(int score) {
		return score >= 75 ? Confidence.HIGH : score >= 45 ? Confidence.MEDIUM : Confidence.LOW;
	}

	private static void classifyTable(Job job, String table, TableDescriptor td, List<ObservableList<String>> rows, PageMap pm,
			List<Result> out) {
		// live rows: rowid -> values, content -> rowids
		Map<Long, List<String>> live = new HashMap<>();
		Map<String, Set<Long>> liveByContent = new HashMap<>();
		for (ObservableList<String> r : rows) {
			String st = RowLayout.status(r);
			if (st == null || !st.equals(Global.REGULAR_RECORD))
				continue;
			long id = RowLayout.number(RowLayout.rowid(r));
			List<String> v = RowLayout.values(r);
			if (id > 0) {
				live.put(id, v);
				String k = contentKey(v, id);
				if (k != null)
					liveByContent.computeIfAbsent(k, x -> new HashSet<>()).add(id);
			}
		}
		// versions (also assigns carved records without rowid by content)
		Map<List<String>, Long> rowidByContent = new IdentityHashMap<>();
		try {
			for (Map.Entry<Long, List<RowHistory.Version>> e : RowHistory.collect(job, table).entrySet())
				for (RowHistory.Version v : e.getValue())
					if (v.row != null && v.source == RowHistory.Source.DATABASE)
						rowidByContent.put(v.row, e.getKey());
		} catch (RuntimeException e) {
			// without history
		}
		List<String> types = td != null && td.columntypes != null ? td.columntypes : List.of();
		List<String> sql = td != null && td.sqltypes != null ? td.sqltypes : List.of();
		List<String> names = td != null && td.columnnames != null ? td.columnnames : List.of();
		boolean hasRowid = td == null || td.ROWID;
		for (ObservableList<String> r : rows) {
			String st = RowLayout.status(r);
			if (st == null)
				continue;
			long off = RowLayout.number(RowLayout.offset(r));
			int page = pm != null && off >= 0 ? pm.pageOf(off) : -1;
			PageMap.Kind pk = pm != null && page > 0 ? pm.kindOf(page) : null;
			boolean recovered = !st.equals(Global.REGULAR_RECORD);
			boolean unrefPage = pk == PageMap.Kind.UNREFERENCED;
			if (!recovered && !unrefPage)
				continue;
			List<String> vals = RowLayout.values(r);
			String flags = RowHistory.flags(r);
			long id = RowLayout.number(RowLayout.rowid(r));
			long assigned = rowidByContent.getOrDefault(r, -1L);
			int score = 100;
			List<String> why = new ArrayList<>();
			// ---- where
			String area = pm == null ? (st.equals(Global.FREELIST_ENTRY) ? "freelist page" : "page") : pm.areaOf(off);
			if (unrefPage)
				area = "unreferenced page";
			switch (area) {
			case "freeblock" -> why.add("found in a freeblock of page " + page + " (deleted cell)");
			case "unallocated area" -> {
				score -= 10;
				why.add("carved from the unallocated area of page " + page + " (no cell structure left)");
			}
			case "cell area" -> {
				score -= 5;
				why.add("cell of page " + page + " that no cell pointer refers to");
			}
			case "freelist leaf" -> why.add("on freed page " + page + " (freelist)");
			case "freelist trunk" -> {
				score -= 15;
				why.add("on freelist trunk page " + page + " (partly overwritten by page pointers)");
			}
			case "overflow" -> {
				score -= 15;
				why.add("inside an overflow page (" + page + ")");
			}
			case "unreferenced page" -> {
				score -= 5;
				why.add("page " + page + " is referenced by no b-tree and not by the freelist");
			}
			default -> why.add("page " + page + " (" + area + ")");
			}
			// ---- completeness
			if (flags.contains(Auxiliary.FLAG_TRUNCATED)) {
				score -= 40;
				why.add("record incomplete (truncated)");
			}
			if (flags.contains("header\u2260payload")) {
				score -= 35;
				why.add("header does not match the payload");
			}
			if (flags.contains(Auxiliary.FLAG_OVERFLOW_OVERWRITTEN)) {
				score -= 30;
				why.add("overflow pages overwritten (only the local part)");
			}
			if (hasRowid && id <= 0) {
				score -= 10;
				String lost = area.equals("freeblock") ? "rowid overwritten by the freeblock header (first bytes reconstructed)"
						: "rowid not recoverable (start of the cell overwritten)";
				why.add(assigned > 0 ? lost + ", assigned to rowid " + assigned + " by content" : lost);
			}
			// ---- plausibility of the values
			int nonEmpty = 0, mismatch = 0, garbled = 0;
			List<String> bad = new ArrayList<>(), unreadable = new ArrayList<>();
			for (int k = 0; k < vals.size(); k++) {
				String v = RowHistory.norm(vals.get(k));
				if (v.isEmpty())
					continue;
				nonEmpty++;
				String t = k < types.size() ? types.get(k) : "";
				String s = k < sql.size() ? sql.get(k) : "";
				String cn = k < names.size() ? names.get(k) : "column " + (k + 1);
				if (garbled(v)) {
					garbled++;
					if (unreadable.size() < 3)
						unreadable.add(cn);
				} else if (!fits(v, t, s)) {
					mismatch++;
					if (bad.size() < 3)
						bad.add(cn);
				}
			}
			if (garbled > 0) {
				score -= Math.min(40, 20 * garbled);
				why.add(garbled + " value(s) contain unreadable characters (" + String.join(", ", unreadable) + (garbled > 3 ? ", \u2026" : "")
						+ ") - partly overwritten");
			}
			if (mismatch > 0) {
				score -= Math.min(36, 12 * mismatch);
				why.add(mismatch + " value(s) do not fit the column type (" + String.join(", ", bad) + (mismatch > 3 ? ", \u2026" : "") + ")");
			}
			if (nonEmpty <= 1 && vals.size() >= 3) {
				score -= 20;
				why.add("only " + nonEmpty + " of " + vals.size() + " columns have a value");
			}
			// ---- class
			long key = id > 0 ? id : assigned;
			Kind kind = Kind.DELETED;
			if (isIndex(job, table)) {
				score = Math.max(0, Math.min(100, score));
				why.add(0, "entry of index " + table + " (indexed values and rowid)");
				StringBuilder v = new StringBuilder();
				for (int k = 0; k < vals.size() && v.length() < 200; k++)
					v.append(k > 0 ? " | " : "").append(vals.get(k) == null ? "" : vals.get(k));
				out.add(new Result(table, -1, unrefPage ? Kind.UNREFERENCED : Kind.INDEX_ENTRY, level(score), score, area,
						String.join("; ", why), page > 0 ? "page " + page + ", offset " + off : "offset " + off, v.toString(), r));
				continue;
			}
			List<String> cur = key > 0 ? live.get(key) : null;
			if (unrefPage)
				kind = Kind.UNREFERENCED;
			if (cur != null && recovered) {
				int same = 0, diff = 0;
				List<String> changed = new ArrayList<>();
				for (int k = 0; k < Math.max(vals.size(), cur.size()); k++) {
					String x = k < vals.size() ? RowHistory.norm(vals.get(k)) : "", y = k < cur.size() ? RowHistory.norm(cur.get(k)) : "";
					if (x.isEmpty() || y.isEmpty() || x.equals(String.valueOf(key)))
						continue;
					if (x.equals(y))
						same++;
					else {
						diff++;
						if (changed.size() < 4)
							changed.add(k < names.size() ? names.get(k) : "column " + (k + 1));
					}
				}
				if (diff > 0 && same > 0) {
					kind = Kind.OLDER_VERSION;
					why.add(0, "earlier state of rowid " + key + " (live), differs in " + String.join(", ", changed));
				} else if (diff > 0) {
					why.add(0, "rowid " + key + " was reused by a row with completely different content");
				} else
					why.add(0, "identical to the live row " + key);
			} else if (recovered) {
				String k = contentKey(vals, key);
				Set<Long> again = k == null ? null : liveByContent.get(k);
				if (again != null && !again.contains(key) && (key > 0 || !hasRowid)) {
					kind = Kind.REINSERTED;
					why.add(0, "deleted, the same content exists again as rowid " + again.iterator().next());
				} else if (again != null && key <= 0) {
					kind = Kind.REINSERTED;
					why.add(0, "the same content exists as live rowid " + again.iterator().next() + " (deleted and inserted again, or a copy)");
				} else if (flags.contains("old version (UPDATE)")) {
					kind = Kind.OLDER_VERSION;
					why.add(0, "older version (UPDATE) according to the version analysis");
				} else if (key > 0 && hasRowid)
					why.add(0, "rowid " + key + " no longer exists");
				else
					why.add(0, "no live row with this content");
			}
			score = Math.max(0, Math.min(100, score));
			StringBuilder v = new StringBuilder();
			for (int k = 0; k < vals.size() && v.length() < 200; k++)
				v.append(k > 0 ? " | " : "").append(vals.get(k) == null ? "" : vals.get(k));
			out.add(new Result(table, key > 0 ? key : -1, kind, level(score), score, area, String.join("; ", why),
					page > 0 ? "page " + page + ", offset " + off : "offset " + off, v.toString(), r));
		}
	}

	/** non-empty values except the rowid column; null if fewer than two */
	private static String contentKey(List<String> v, long id) {
		StringBuilder sb = new StringBuilder();
		int n = 0;
		for (int k = 0; k < v.size(); k++) {
			String x = RowHistory.norm(v.get(k));
			if (!x.isEmpty() && !x.equals(String.valueOf(id)) && !x.startsWith("[BLOB-")) {
				n++;
				sb.append(k).append('=').append(x).append('\u0001');
			}
		}
		return n >= 2 ? sb.toString() : null;
	}

	/** text with replacement or control characters (overwritten bytes) */
	static boolean garbled(String v) {
		if (v.startsWith("[BLOB-"))
			return false;
		for (int i = 0; i < v.length(); i++) {
			char c = v.charAt(i);
			if (c == '\uFFFD' || (c < 0x20 && c != '\n' && c != '\r' && c != '\t'))
				return true;
		}
		return false;
	}

	/** does a displayed value fit the column affinity? */
	static boolean fits(String v, String affinity, String sqlType) {
		if (v.startsWith("[BLOB-") || v.equals(".."))
			return true;
		String s = sqlType == null ? "" : sqlType.toUpperCase();
		if (s.contains("DATE") || s.contains("TIME"))
			return true;
		switch (affinity == null ? "" : affinity) {
		case "INT":
			return v.matches("-?\\d+") || v.matches("\\d{4}-\\d{2}-\\d{2}.*");
		case "REAL":
			return v.matches("-?\\d+(\\.\\d+)?([eE][-+]?\\d+)?") || v.equalsIgnoreCase("NaN") || v.contains("Infinity");
		default:
			return true;
		}
	}
}
