package fqlite.forensics;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import fqlite.base.Global;
import fqlite.base.Job;
import fqlite.descriptor.TableDescriptor;
import javafx.collections.ObservableList;

/**
 * Every version of a row (same table, same rowid) across the database file, the WAL and the
 * rollback journal: live record, carved deleted / freelist records, WAL frames, journal page
 * images. Built from the results FQLite already holds (Job, WALReader, RollbackJournalReader)
 * and from the version flags of the NIST analysis ("of rowid N" links carved records whose
 * rowid was overwritten).
 */
public final class RowHistory {

	private RowHistory() {
	}

	public enum Source {
		JOURNAL("journal"), DATABASE("database"), WAL("WAL");

		public final String label;

		Source(String l) {
			label = l;
		}
	}

	/** one version of a row */
	public static final class Version {
		public final Source source;
		public final String state;
		public final long rowid;
		public final String location;
		public final String flags;
		public final List<String> values;
		public final List<String> row;      // the result row itself (for navigation)
		final long order1, order2, order3;
		/** column indices that differ from the previous version (empty for the first) */
		public final Set<Integer> changed = new HashSet<>();
		public boolean current;

		Version(Source source, String state, long rowid, String location, String flags, List<String> values,
				List<String> row, long o1, long o2, long o3) {
			this.source = source;
			this.state = state;
			this.rowid = rowid;
			this.location = location;
			this.flags = flags;
			this.values = values;
			this.row = row;
			this.order1 = o1;
			this.order2 = o2;
			this.order3 = o3;
		}
	}

	/** a row with more than one version, or a deleted row */
	public record Change(String table, long rowid, String kind, int versions, String sources, String summary,
			List<Version> history) {
	}

	private static final Pattern OF_ROWID = Pattern.compile("of rowid (\\d+)");

	public static List<String> columnNames(Job job, String table) {
		if (job != null && job.headers != null)
			for (TableDescriptor td : job.headers)
				if (table.equals(td.tblname) && td.columnnames != null)
					return td.columnnames;
		return List.of();
	}

	/** the rowid a result row belongs to (carved rows: from the "of rowid N" flag) or -1 */
	public static long rowidOf(List<String> r) {
		long id = RowLayout.number(RowLayout.rowid(r));
		if (id > 0)
			return id;
		String pll = RowLayout.pll(r);
		if (pll != null) {
			Matcher m = OF_ROWID.matcher(pll);
			if (m.find())
				return Long.parseLong(m.group(1));
		}
		return -1;
	}

	/** flags of the _PLL column without the [pll|hl] part */
	public static String flags(List<String> r) {
		String p = RowLayout.pll(r);
		return p == null ? "" : p.replaceAll("\\[\\d+\\|\\d+\\]", "").trim();
	}

	/** all versions of all rows of a table, grouped by rowid */
	public static Map<Long, List<Version>> collect(Job job, String table) {
		Map<Long, List<Version>> m = new TreeMap<>();
		if (job == null)
			return m;
		add(m, job.resultlist == null ? null : job.resultlist.get(table), Source.DATABASE, job.ps);
		if (job.wal != null && job.wal.resultlist != null)
			add(m, job.wal.resultlist.get(table), Source.WAL, job.ps);
		if (job.rol != null && job.rol.resultlist != null)
			add(m, job.rol.resultlist.get(table), Source.JOURNAL, job.ps);
		matchByContent(m, job, table);
		for (List<Version> l : m.values())
			finish(l);
		return m;
	}

	/** the versions of one row in chronological order */
	public static List<Version> history(Job job, String table, long rowid) {
		List<Version> l = collect(job, table).get(rowid);
		return l == null ? List.of() : l;
	}

	private static void add(Map<Long, List<Version>> m, ObservableList<ObservableList<String>> rows, Source src, int ps) {
		addWithRowid(m, rows, src, ps, -1);
	}

	/** forcedRowid > 0: the rowid determined by content matching */
	private static void addWithRowid(Map<Long, List<Version>> m, ObservableList<ObservableList<String>> rows, Source src, int ps,
			long forcedRowid) {
		if (rows == null)
			return;
		for (ObservableList<String> r : List.copyOf(rows)) {
			if (RowLayout.size(r) <= RowLayout.VALUES)
				continue;
			long id = forcedRowid > 0 ? forcedRowid : rowidOf(r);
			if (id <= 0)
				continue;
			String st = RowLayout.status(r);
			st = st == null ? "" : st.trim();
			String fl = flags(r);
			long off = RowLayout.number(RowLayout.offset(r));
			String state, loc;
			List<String> vals;
			long o1, o2 = 0, o3 = Math.max(off, 0);
			switch (src) {
			case WAL -> {
				vals = RowLayout.walValues(r);
				long frame = RowLayout.number(RowLayout.get(r, RowLayout.WAL_FRAME));
				long salt1 = RowLayout.number(RowLayout.get(r, RowLayout.WAL_SALT1));
				loc = "frame " + frame + " (page " + RowLayout.get(r, RowLayout.WAL_DBPAGE) + ", salt-1 " + salt1 + ")";
				state = st.equals(Global.DELETED_RECORD_IN_PAGE) ? "deleted cell in WAL page" : "WAL version";
				o1 = 3;
				o2 = salt1 * 1_000_000L + frame;
			}
			case JOURNAL -> {
				vals = RowLayout.values(r);
				loc = "journal offset " + off;
				state = st.equals(Global.DELETED_RECORD_IN_PAGE) ? "deleted cell in journal page" : "journal (before last transaction)";
				o1 = 1;
			}
			default -> {
				vals = RowLayout.values(r);
				loc = off >= 0 && ps > 0 ? "page " + (off / ps + 1) + " @ " + (off % ps) + " (offset " + off + ")" : "offset " + off;
				if (st.equals(Global.DELETED_RECORD_IN_PAGE))
					state = "deleted record";
				else if (st.equals(Global.FREELIST_ENTRY))
					state = "freelist page";
				else
					state = "live";
				o1 = st.isEmpty() ? 2 : 0;          // recovered records are older than the journal snapshot
			}
			}
			if (fl.contains("old version (UPDATE)") && !st.isEmpty())
				state = "older version (UPDATE)";
			if (forcedRowid > 0)
				state += " (rowid by content)";
			m.computeIfAbsent(id, k -> new ArrayList<>()).add(new Version(src, state, id, loc, fl, vals, r, o1, o2, o3));
		}
	}

	/**
	 * Carved records whose rowid was overwritten are attached to the row whose version has the
	 * same values in every non-empty column (unique match, at least two values).
	 */
	private static void matchByContent(Map<Long, List<Version>> m, Job job, String table) {
		Map<String, Set<Long>> index = new java.util.HashMap<>();
		for (Map.Entry<Long, List<Version>> e : m.entrySet())
			for (Version v : e.getValue())
				for (int k = 0; k < v.values.size(); k++) {
					String x = norm(v.values.get(k));
					if (!x.isEmpty())
						index.computeIfAbsent(k + "|" + x, z -> new HashSet<>()).add(e.getKey());
				}
		if (index.isEmpty())
			return;
		List<ObservableList<ObservableList<String>>> lists = new ArrayList<>();
		List<Source> srcs = new ArrayList<>();
		if (job.resultlist != null) { lists.add(job.resultlist.get(table)); srcs.add(Source.DATABASE); }
		if (job.wal != null && job.wal.resultlist != null) { lists.add(job.wal.resultlist.get(table)); srcs.add(Source.WAL); }
		if (job.rol != null && job.rol.resultlist != null) { lists.add(job.rol.resultlist.get(table)); srcs.add(Source.JOURNAL); }
		Map<Long, List<Version>> found = new TreeMap<>();
		for (int s = 0; s < lists.size(); s++) {
			ObservableList<ObservableList<String>> rows = lists.get(s);
			Source src = srcs.get(s);
			if (rows == null)
				continue;
			for (ObservableList<String> r : List.copyOf(rows)) {
				if (RowLayout.size(r) <= RowLayout.VALUES || rowidOf(r) > 0)
					continue;
				String st = RowLayout.status(r);
				if (st == null || st.trim().isEmpty())
					continue;                              // only recovered records
				List<String> vals = src == Source.WAL ? RowLayout.walValues(r) : RowLayout.values(r);
				Set<Long> cand = null;
				int nonEmpty = 0;
				for (int k = 0; k < vals.size(); k++) {
					String x = norm(vals.get(k));
					if (x.isEmpty())
						continue;
					nonEmpty++;
					Set<Long> ids = index.get(k + "|" + x);
					if (ids == null) {
						cand = Set.of();
						break;
					}
					if (cand == null)
						cand = new HashSet<>(ids);
					else
						cand.retainAll(ids);
					if (cand.isEmpty())
						break;
				}
				if (nonEmpty < 2 || cand == null || cand.size() != 1)
					continue;
				ObservableList<ObservableList<String>> single = javafx.collections.FXCollections.observableArrayList();
				single.add(r);
				addWithRowid(found, single, src, job.ps, cand.iterator().next());
			}
		}
		for (Map.Entry<Long, List<Version>> e : found.entrySet())
			m.computeIfAbsent(e.getKey(), z -> new ArrayList<>()).addAll(e.getValue());
	}

	private static void finish(List<Version> l) {
		l.sort(Comparator.comparingLong((Version v) -> v.order1).thenComparingLong(v -> v.order2).thenComparingLong(v -> v.order3));
		// identical consecutive copies (e.g. the same row in several WAL frames) stay, but the
		// changed set only marks real differences; an empty carved value (lost id) is no change
		for (int i = 1; i < l.size(); i++) {
			List<String> a = l.get(i - 1).values, b = l.get(i).values;
			for (int k = 0; k < Math.max(a.size(), b.size()); k++) {
				String x = k < a.size() ? norm(a.get(k)) : "", y = k < b.size() ? norm(b.get(k)) : "";
				if (x.isEmpty() || y.isEmpty())
					continue;
				if (!x.equals(y))
					l.get(i).changed.add(k);
			}
		}
		Version cur = currentVersion(l);
		if (cur != null)
			cur.current = true;
	}

	/** the version that is the current state of the row, or null if the row is deleted */
	public static Version currentVersion(List<Version> l) {
		// the newest regular version (database live record, or a WAL version - the WAL is newer
		// than the database file); it is not current if it was deleted afterwards
		Version last = null;
		for (Version v : l)
			if ((v.source == Source.DATABASE && v.state.equals("live")) || (v.source == Source.WAL && v.state.equals("WAL version")))
				last = v;
		if (last == null || last.flags.contains("\u2717"))
			return null;
		return last;
	}

	static String norm(String s) {
		return s == null || s.equals("NULL") || s.equals("null") ? "" : s;
	}

	/** rows of all tables that were updated, deleted or whose rowid was reused */
	public static List<Change> changedRows(Job job) {
		List<Change> out = new ArrayList<>();
		if (job == null || job.resultlist == null)
			return out;
		Set<String> tables = new java.util.TreeSet<>(job.resultlist.keySet());
		if (job.wal != null && job.wal.resultlist != null)
			tables.addAll(job.wal.resultlist.keySet());
		if (job.rol != null && job.rol.resultlist != null)
			tables.addAll(job.rol.resultlist.keySet());
		for (String t : tables) {
			if (t.startsWith("sqlite_") || t.startsWith("__"))
				continue;
			List<String> cols = columnNames(job, t);
			for (Map.Entry<Long, List<Version>> e : collect(job, t).entrySet()) {
				List<Version> l = e.getValue();
				Version cur = currentVersion(l);
				boolean anyOld = false, anyDeleted = false, reused = false;
				Set<Integer> changedCols = new java.util.TreeSet<>();
				for (Version v : l) {
					if (v == cur)
						continue;
					boolean differs = false, shares = false;
					if (cur != null) {
						for (int k = 0; k < Math.max(v.values.size(), cur.values.size()); k++) {
							String x = k < v.values.size() ? norm(v.values.get(k)) : "", y = k < cur.values.size() ? norm(cur.values.get(k)) : "";
							if (x.isEmpty() || y.isEmpty() || x.equals(String.valueOf(v.rowid)))
								continue;
							if (x.equals(y))
								shares = true;
							else {
								differs = true;
								changedCols.add(k);
							}
						}
						if (differs)
							anyOld = true;
						if (differs && !shares)
							reused = true;
					} else
						anyDeleted = true;
				}
				String kind;
				if (cur == null && (anyDeleted || !l.isEmpty()))
					kind = "deleted";
				else if (reused)
					kind = "rowid reused";
				else if (anyOld)
					kind = "updated";
				else
					continue;                // only identical copies
				Set<String> src = new java.util.LinkedHashSet<>();
				for (Version v : l)
					src.add(v.source.label);
				StringBuilder sum = new StringBuilder();
				if (kind.equals("deleted")) {
					Version last = l.get(l.size() - 1);
					sum.append("last seen: ").append(last.state).append(", ").append(last.location);
				} else {
					sum.append("changed: ");
					int i = 0;
					for (int k : changedCols) {
						if (i++ > 0)
							sum.append(", ");
						sum.append(k < cols.size() ? cols.get(k) : "column " + (k + 1));
					}
				}
				out.add(new Change(t, e.getKey(), kind, l.size(), String.join(" + ", src), sum.toString(), l));
			}
		}
		return out;
	}

	/** number of recovered records per table that carry no rowid (not part of any history) */
	public static Map<String, Integer> recordsWithoutRowid(Job job) {
		Map<String, Integer> m = new LinkedHashMap<>();
		if (job == null || job.resultlist == null)
			return m;
		for (Map.Entry<String, ObservableList<ObservableList<String>>> e : job.resultlist.entrySet()) {
			int n = 0;
			for (ObservableList<String> r : List.copyOf(e.getValue())) {
				String st = RowLayout.status(r);
				if (st != null && !st.trim().isEmpty() && rowidOf(r) <= 0)
					n++;
			}
			if (n > 0)
				m.put(e.getKey(), n);
		}
		return m;
	}
}
