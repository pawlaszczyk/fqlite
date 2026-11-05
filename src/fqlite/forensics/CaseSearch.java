package fqlite.forensics;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import fqlite.base.Global;
import fqlite.base.Job;
import javafx.collections.ObservableList;

/**
 * Search over all open databases of a case: every value of every row FQLite holds - live
 * records, recovered records, WAL and journal versions - each hit says where it is from.
 */
public final class CaseSearch {

	private CaseSearch() {
	}

	public enum Mode {
		CONTAINS("contains (any case)"), CONTAINS_CASE("contains (case-sensitive)"), EXACT("exact value"),
		STARTS("starts with"), REGEX("regular expression");

		public final String label;

		Mode(String l) {
			label = l;
		}

		@Override
		public String toString() {
			return label;
		}
	}

	public record Hit(Job job, String db, int sourceKind, String source, String status, String table, long rowid, String column,
			String value, List<String> row) {
	}

	/** a database of the search with its display name */
	public record Target(Job job, String name) {
	}

	public static int search(List<Target> targets, String term, Mode mode, boolean recovered, int maxHits, Consumer<Hit> onHit,
			BooleanSupplier stop) {
		if (term == null || term.isEmpty())
			return 0;
		Pattern rx = mode == Mode.REGEX ? Pattern.compile(term) : null;
		String low = term.toLowerCase(Locale.ROOT);
		int n = 0;
		for (Target tg : targets) {
			Job job = tg.job();
			List<Object[]> sources = new ArrayList<>();
			if (job.resultlist != null)
				sources.add(new Object[] { job.resultlist, 0 });
			if (recovered && job.wal != null && job.wal.resultlist != null)
				sources.add(new Object[] { job.wal.resultlist, 1 });
			if (recovered && job.rol != null && job.rol.resultlist != null)
				sources.add(new Object[] { job.rol.resultlist, 2 });
			for (Object[] so : sources) {
				@SuppressWarnings("unchecked")
				Map<String, ObservableList<ObservableList<String>>> m = (Map<String, ObservableList<ObservableList<String>>>) so[0];
				int kind = (Integer) so[1];
				for (Map.Entry<String, ObservableList<ObservableList<String>>> e : m.entrySet()) {
					String table = e.getKey();
					List<String> cols = RowHistory.columnNames(job, table);
					for (ObservableList<String> r : List.copyOf(e.getValue())) {
						if (stop != null && stop.getAsBoolean())
							return n;
						String st = RowLayout.status(r);
						st = st == null ? "" : st.trim();
						if (!recovered && !st.isEmpty())
							continue;
						List<String> vals = kind == 1 ? RowLayout.walValues(r) : RowLayout.values(r);
						for (int c = 0; c < vals.size(); c++) {
							String v = vals.get(c);
							if (v == null || !matches(v, term, low, mode, rx))
								continue;
							String status;
							String fl = RowHistory.flags(r);
							if (st.equals(Global.DELETED_RECORD_IN_PAGE))
								status = "deleted";
							else if (st.equals(Global.FREELIST_ENTRY))
								status = "freelist";
							else if (fl.contains("\u21BB"))
								status = "older version";
							else if (fl.contains("\u2717"))
								status = "deleted later";
							else
								status = kind == 0 ? "live" : "version";
							String source = kind == 0 ? "database" : kind == 1 ? "WAL frame " + RowLayout.get(r, RowLayout.WAL_FRAME) : "journal";
							onHit.accept(new Hit(job, tg.name(), kind, source, status, table, RowHistory.rowidOf(r),
									c < cols.size() ? cols.get(c) : "column " + (c + 1), excerpt(v, term, low, mode, rx), r));
							if (++n >= maxHits)
								return n;
							break;                       // one hit per row
						}
					}
				}
			}
		}
		return n;
	}

	static boolean matches(String v, String term, String low, Mode mode, Pattern rx) {
		return switch (mode) {
		case CONTAINS -> v.toLowerCase(Locale.ROOT).contains(low);
		case CONTAINS_CASE -> v.contains(term);
		case EXACT -> v.equals(term);
		case STARTS -> v.toLowerCase(Locale.ROOT).startsWith(low);
		case REGEX -> rx.matcher(v).find();
		};
	}

	static String excerpt(String v, String term, String low, Mode mode, Pattern rx) {
		if (v.length() <= 120)
			return v;
		int at = 0;
		if (mode == Mode.REGEX) {
			Matcher m = rx.matcher(v);
			if (m.find())
				at = m.start();
		} else
			at = Math.max(0, v.toLowerCase(Locale.ROOT).indexOf(low));
		int a = Math.max(0, at - 40), b = Math.min(v.length(), at + 80);
		return (a > 0 ? "\u2026" : "") + v.substring(a, b) + (b < v.length() ? "\u2026" : "");
	}
}
