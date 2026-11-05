package fqlite.forensics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import fqlite.base.Global;
import fqlite.base.Job;
import fqlite.descriptor.TableDescriptor;
import javafx.collections.ObservableList;

/**
 * Missing row numbers: rowids below the highest current rowid that no current row has, and -
 * for AUTOINCREMENT tables - the rowids between the highest current one and the value in
 * sqlite_sequence. They are evidence of deletions even when nothing of the deleted rows is
 * left; each gap is cross-checked with the recovered records, WAL and journal versions.
 * <p>
 * Caveat (shown in the UI): rowids need not be consecutive (explicit ids in INSERT, rowids
 * chosen at random after the maximum was reached), so a gap is an indication, not proof.
 */
public final class RowidGaps {

	private RowidGaps() {
	}

	/** a run of consecutive missing rowids */
	public record Gap(String table, long from, long to, String reason, String recovered) {
		public long count() {
			return to - from + 1;
		}
	}

	/** summary per table */
	public record TableSummary(String table, long currentRows, long maxRowid, long sequence, long missing,
			long missingRecovered, String note) {
	}

	public static final long MAX_MISSING_LISTED = 100_000;


	/** analyses every rowid table of the database; summaries are returned in {@code out} */
	public static List<Gap> analyse(Job job, List<TableSummary> out) {
		List<Gap> gaps = new ArrayList<>();
		if (job == null || job.resultlist == null)
			return gaps;
		Map<String, Long> seq = sqliteSequence(job);
		for (TableDescriptor td : job.headers) {
			String t = td.tblname;
			if (t == null || t.startsWith("sqlite_") || !td.ROWID || td.isVirtual())
				continue;
			TreeSet<Long> current = new TreeSet<>();
			Map<Long, List<RowHistory.Version>> hist = RowHistory.collect(job, t);
			Map<Long, String> recoveredFrom = new HashMap<>();
			for (Map.Entry<Long, List<RowHistory.Version>> e : hist.entrySet()) {
				RowHistory.Version cur = RowHistory.currentVersion(e.getValue());
				if (cur != null)
					current.add(e.getKey());
				else {
					java.util.Set<String> where = new java.util.LinkedHashSet<>();
					for (RowHistory.Version v : e.getValue())
						where.add(v.state.startsWith(v.source.label) ? v.state : v.source.label + ": " + v.state);
					recoveredFrom.put(e.getKey(), String.join(", ", where));
				}
			}
			long max = current.isEmpty() ? 0 : current.last();
			long sq = seq.getOrDefault(t, -1L);
			long upper = Math.max(max, sq);
			long expected = upper;
			long missing = expected - current.size();
			if (upper <= 0) {
				if (out != null)
					out.add(new TableSummary(t, current.size(), max, sq, 0, 0, current.isEmpty() ? "no rows" : ""));
				continue;
			}
			String note = "";
			if (missing > fqlite.base.Limits.GAPS_MAX_LISTED || (missing > 1000 && current.size() < missing / 100)) {
				note = "rowids not sequential (max " + upper + " for " + current.size() + " rows) - gaps not listed";
				if (out != null)
					out.add(new TableSummary(t, current.size(), max, sq, missing, 0, note));
				continue;
			}
			long rec = 0;
			long runStart = -1;
			String runReason = null, runRecovered = null;
			for (long id = 1; id <= upper + 1; id++) {
				boolean miss = id <= upper && !current.contains(id);
				String reason = id > max ? "after the last row, below sqlite_sequence (" + sq + ")" : "gap";
				String recovered = miss ? recoveredFrom.getOrDefault(id, "") : null;
				if (miss && !recovered.isEmpty())
					rec++;
				boolean extends_ = miss && runStart > 0 && reason.equals(runReason) && recovered.isEmpty() == runRecovered.isEmpty()
						&& recovered.isEmpty();
				if (runStart > 0 && !extends_) {
					gaps.add(new Gap(t, runStart, id - 1, runReason, runRecovered));
					runStart = -1;
				}
				if (miss && runStart < 0) {
					runStart = id;
					runReason = reason;
					runRecovered = recovered;
				}
			}
			if (out != null)
				out.add(new TableSummary(t, current.size(), max, sq, missing, rec, note));
		}
		return gaps;
	}

	/** name -> seq of sqlite_sequence (live rows) */
	public static Map<String, Long> sqliteSequence(Job job) {
		Map<String, Long> m = new TreeMap<>();
		ObservableList<ObservableList<String>> rows = job.resultlist.get("sqlite_sequence");
		if (rows == null)
			return m;
		for (ObservableList<String> r : List.copyOf(rows)) {
			if (!Global.REGULAR_RECORD.equals(RowLayout.status(r)))
				continue;
			List<String> v = RowLayout.values(r);
			if (v.size() >= 2) {
				long n = RowLayout.number(v.get(1));
				if (n > 0)
					m.merge(v.get(0), n, Math::max);
			}
		}
		return m;
	}
}
