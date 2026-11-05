package fqlite.forensics;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;

import fqlite.base.Global;
import fqlite.base.Job;
import javafx.collections.ObservableList;

/**
 * Timeline: finds the date columns of the open databases (by name and by sampled values that
 * read as plausible dates 1990-2040 in one format) and lists every dated row in time order -
 * optionally also deleted records, WAL and journal versions, each marked by its source.
 */
public final class Timeline {

	private Timeline() {
	}

	/** supported timestamp formats */
	public enum Format {
		UNIX_S("Unix seconds"), UNIX_MS("Unix milliseconds"), UNIX_US("Unix microseconds"), UNIX_NS("Unix nanoseconds"),
		COCOA_S("Cocoa / Mac absolute seconds"), COCOA_NS("Cocoa nanoseconds"), WEBKIT_US("WebKit / Chrome microseconds"),
		FILETIME("Windows FILETIME (100 ns)"), HFS("HFS+ seconds (1904)"), DOTNET(".NET ticks"), OLE("OLE automation days"),
		ISO_TEXT("ISO 8601 / date text");

		public final String label;

		Format(String l) {
			label = l;
		}

		@Override
		public String toString() {
			return label;
		}
	}

	static final long MIN = Instant.parse("1990-01-01T00:00:00Z").getEpochSecond();
	static final long MAX = Instant.parse("2040-12-31T23:59:59Z").getEpochSecond();
	static final long COCOA = 978_307_200L, WEBKIT = 11_644_473_600L, HFS_OFF = 2_082_844_800L, DOTNET_OFF = 62_135_596_800L;

	/** converts a stored value with the given format; null if it is no plausible date */
	public static Instant convert(String raw, Format f) {
		if (raw == null)
			return null;
		String s = raw.trim();
		if (s.isEmpty() || s.equals("NULL"))
			return null;
		try {
			if (f == Format.ISO_TEXT)
				return plausible(parseText(s));
			double d = Double.parseDouble(s);
			Instant i = switch (f) {
			case UNIX_S -> ofSeconds(d);
			case UNIX_MS -> ofSeconds(d / 1e3);
			case UNIX_US -> ofSeconds(d / 1e6);
			case UNIX_NS -> ofSeconds(d / 1e9);
			case COCOA_S -> ofSeconds(d + COCOA);
			case COCOA_NS -> ofSeconds(d / 1e9 + COCOA);
			case WEBKIT_US -> ofSeconds(d / 1e6 - WEBKIT);
			case FILETIME -> ofSeconds(d / 1e7 - WEBKIT);
			case HFS -> ofSeconds(d - HFS_OFF);
			case DOTNET -> ofSeconds(d / 1e7 - DOTNET_OFF);
			case OLE -> ofSeconds((d - 25569.0) * 86400.0);
			default -> null;
			};
			return plausible(i);
		} catch (NumberFormatException e) {
			return null;
		}
	}

	private static Instant ofSeconds(double s) {
		if (Double.isNaN(s) || Double.isInfinite(s) || s < MIN - 1 || s > MAX + 1)
			return null;
		long sec = (long) Math.floor(s);
		return Instant.ofEpochSecond(sec, (long) ((s - sec) * 1e9));
	}

	private static Instant plausible(Instant i) {
		return i == null || i.getEpochSecond() < MIN || i.getEpochSecond() > MAX ? null : i;
	}

	private static final DateTimeFormatter[] TEXT = {
			DateTimeFormatter.ISO_OFFSET_DATE_TIME, DateTimeFormatter.ISO_LOCAL_DATE_TIME,
			DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSS]"), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm[:ss]"),
			DateTimeFormatter.RFC_1123_DATE_TIME };

	static Instant parseText(String s) {
		if (s.length() < 10 || s.length() > 40 || !Character.isDigit(s.charAt(0)) && !Character.isLetter(s.charAt(0)))
			return null;
		for (DateTimeFormatter f : TEXT) {
			try {
				if (f == DateTimeFormatter.ISO_OFFSET_DATE_TIME || f == DateTimeFormatter.RFC_1123_DATE_TIME)
					return OffsetDateTime.parse(s, f).toInstant();
				return LocalDateTime.parse(s, f).toInstant(ZoneOffset.UTC);
			} catch (DateTimeParseException e) {
				// next
			}
		}
		if (s.matches("\\d{4}-\\d{2}-\\d{2}"))
			try {
				return LocalDateTime.parse(s + "T00:00:00").toInstant(ZoneOffset.UTC);
			} catch (DateTimeParseException e) {
				return null;
			}
		return null;
	}

	// ------------------------------------------------------------------ detection

	public static final class DateColumn {
		public final Job job;
		public final String db, table, column;
		public final int index;
		public Format format;
		public final int samples, plausible;
		public final String example;
		public final String reason;
		public boolean selected = true;

		DateColumn(Job job, String db, String table, String column, int index, Format format, int samples, int plausible,
				String example, String reason) {
			this.job = job;
			this.db = db;
			this.table = table;
			this.column = column;
			this.index = index;
			this.format = format;
			this.samples = samples;
			this.plausible = plausible;
			this.example = example;
			this.reason = reason;
		}

		public String confidence() {
			return samples == 0 ? "-" : (100 * plausible / samples) + " %";
		}
	}

	private static final Pattern DATE_NAME = Pattern.compile(
			"(?i).*(time|date|_ts$|^ts$|timestamp|created|modified|updated|expires|expiry|visit|sent|received|deliver|read_|last_|first_|start|end|birth|when|seen).*");
	private static final Pattern NOT_DATE = Pattern.compile(
			"(?i).*(^id$|_id$|^rowid$|count|size|length|len$|phone|number|^num|zip|plz|port|pid|uid|flag|type|status|version|duration|offset|index|width|height|lat|lon).*");

	/** date columns of one database (sampling up to 200 live values per column) */
	public static List<DateColumn> detect(Job job, String dbName) {
		List<DateColumn> out = new ArrayList<>();
		if (job == null || job.resultlist == null)
			return out;
		for (String t : new TreeSet<>(job.resultlist.keySet())) {
			if (t.startsWith("sqlite_") || t.startsWith("__"))
				continue;
			List<String> cols = RowHistory.columnNames(job, t);
			ObservableList<ObservableList<String>> rows = job.resultlist.get(t);
			if (rows == null || rows.isEmpty())
				continue;
			int ncol = cols.size();
			if (ncol == 0)
				continue;
			List<List<String>> samples = new ArrayList<>();
			for (int c = 0; c < ncol; c++)
				samples.add(new ArrayList<>());
			for (ObservableList<String> r : List.copyOf(rows)) {
				if (!Global.REGULAR_RECORD.equals(RowLayout.status(r)))
					continue;
				List<String> v = RowLayout.values(r);
				for (int c = 0; c < ncol && c < v.size(); c++) {
					String x = v.get(c);
					if (x != null && !x.isEmpty() && !x.equals("NULL") && samples.get(c).size() < 200)
						samples.get(c).add(x);
				}
				boolean full = true;
				for (List<String> l : samples)
					full &= l.size() >= 200;
				if (full)
					break;
			}
			for (int c = 0; c < ncol; c++) {
				String name = cols.get(c);
				DateColumn dc = judge(job, dbName, t, name, c, samples.get(c));
				if (dc != null)
					out.add(dc);
			}
		}
		return out;
	}

	private static DateColumn judge(Job job, String db, String table, String name, int c, List<String> vals) {
		if (vals.size() < 2)
			return null;
		boolean hint = DATE_NAME.matcher(name).matches();
		boolean anti = NOT_DATE.matcher(name).matches() && !hint;
		if (anti)
			return null;
		Set<String> distinct = new HashSet<>(vals);
		if (!hint && distinct.size() < Math.max(3, vals.size() / 4))
			return null;                      // repeating values: a flag or a category
		// the format(s) under which most values are plausible; among equally good ones the one
		// whose median date is closest to (but not after) today - e.g. WebKit microseconds and
		// Cocoa nanoseconds, or Unix and Cocoa seconds overlap
		Format best = null;
		int bestN = 0;
		long bestDist = Long.MAX_VALUE;
		long now = Instant.now().getEpochSecond() + 86400L * 366;
		for (Format f : Format.values()) {
			List<Long> ts = new ArrayList<>();
			for (String v : vals) {
				Instant i = convert(v, f);
				if (i != null)
					ts.add(i.getEpochSecond());
			}
			int n = ts.size();
			if (n == 0 || n < bestN)
				continue;
			java.util.Collections.sort(ts);
			long med = ts.get(ts.size() / 2);
			long dist = med > now ? Long.MAX_VALUE / 2 + (med - now) : now - med;
			if (preferred(f, name))
				dist = -1;
			if (n > bestN || dist < bestDist) {
				best = f;
				bestN = n;
				bestDist = dist;
			}
		}
		if (best == null)
			return null;
		double share = bestN / (double) vals.size();
		if (share < (hint ? 0.6 : 0.95))
			return null;
		if (!hint && best != Format.ISO_TEXT && looksLikeCounter(vals))
			return null;
		String ex = vals.get(0) + " = " + convert(vals.get(0), best);
		String reason = (hint ? "name suggests a date; " : "") + bestN + " of " + vals.size() + " sampled values read as " + best.label;
		return new DateColumn(job, db, table, name, c, best, vals.size(), bestN, ex, reason);
	}

	/** Core Data columns (ZTIMESTAMP, ZDATE ...) hold Cocoa seconds */
	private static boolean preferred(Format f, String name) {
		boolean coreData = name.length() > 1 && name.charAt(0) == 'Z' && Character.isUpperCase(name.charAt(1));
		return coreData && f == Format.COCOA_S;
	}

	private static boolean looksLikeCounter(List<String> vals) {
		TreeSet<Double> s = new TreeSet<>();
		for (String v : vals)
			try {
				s.add(Double.parseDouble(v));
			} catch (NumberFormatException e) {
				return false;
			}
		if (s.size() < 3)
			return false;
		double span = s.last() - s.first();
		return span < 10_000 && span <= s.size() * 3;   // values close together, like an id counter
	}

	// ------------------------------------------------------------------ events

	public record Event(Instant time, String db, String source, String status, String table, String column, Format format,
			long rowid, String raw, String description, Job job, List<String> row, int sourceKind) {
	}

	/**
	 * builds the events of the selected columns.
	 *
	 * @param recovered also deleted records (status D/F) of the database file
	 * @param walJournal also versions in the WAL and the rollback journal
	 */
	public static List<Event> build(List<DateColumn> columns, boolean recovered, boolean walJournal, Instant from, Instant to,
			int maxPerColumn, BooleanSupplier stop) {
		List<Event> out = new ArrayList<>();
		Map<String, Map<Long, List<RowHistory.Version>>> hist = new java.util.HashMap<>();
		for (DateColumn dc : columns) {
			if (!dc.selected)
				continue;
			if (stop != null && stop.getAsBoolean())
				break;
			Job job = dc.job;
			List<String> cols = RowHistory.columnNames(job, dc.table);
			int n = 0;
			Set<String> seen = new HashSet<>();    // the same value of the same row is one event
			Map<Long, List<RowHistory.Version>> h = hist.computeIfAbsent(System.identityHashCode(job) + "|" + dc.table,
					k -> RowHistory.collect(job, dc.table));
			current.set(h);
			Map<Object, Long> ids = new java.util.IdentityHashMap<>();
			for (Map.Entry<Long, List<RowHistory.Version>> e : h.entrySet())
				for (RowHistory.Version v : e.getValue())
					ids.put(v.row, e.getKey());
			rowids.set(ids);
			n += add(out, seen, dc, cols, job.resultlist.get(dc.table), 0, recovered, from, to, maxPerColumn - n);
			if (walJournal && job.wal != null && job.wal.resultlist != null)
				n += add(out, seen, dc, cols, job.wal.resultlist.get(dc.table), 1, true, from, to, maxPerColumn - n);
			if (walJournal && job.rol != null && job.rol.resultlist != null)
				add(out, seen, dc, cols, job.rol.resultlist.get(dc.table), 2, true, from, to, maxPerColumn - n);
		}
		out.sort((a, b) -> a.time().compareTo(b.time()));
		return out;
	}

	private static final ThreadLocal<Map<Long, List<RowHistory.Version>>> current = new ThreadLocal<>();
	private static final ThreadLocal<Map<Object, Long>> rowids = new ThreadLocal<>();

	private static int add(List<Event> out, Set<String> seen, DateColumn dc, List<String> cols, ObservableList<ObservableList<String>> rows, int kind,
			boolean nonLive, Instant from, Instant to, int max) {
		if (rows == null || max <= 0)
			return 0;
		int n = 0;
		List<ObservableList<String>> ordered = new ArrayList<>(rows);
		// live records first: a deleted copy with the same value is then not a separate event
		ordered.sort(java.util.Comparator.comparingInt(r -> Global.REGULAR_RECORD.equals(RowLayout.status(r)) ? 0 : 1));
		for (ObservableList<String> r : ordered) {
			String st = RowLayout.status(r);
			st = st == null ? "" : st.trim();
			if (kind == 0 && !st.isEmpty() && !nonLive)
				continue;
			List<String> v = kind == 1 ? RowLayout.walValues(r) : RowLayout.values(r);
			if (dc.index >= v.size())
				continue;
			Instant t = convert(v.get(dc.index), dc.format);
			if (t == null || (from != null && t.isBefore(from)) || (to != null && t.isAfter(to)))
				continue;
			long rid = RowHistory.rowidOf(r);
			if (rid <= 0 && rowids.get() != null) {
				Long x = rowids.get().get(r);           // rowid found by content matching
				if (x != null)
					rid = x;
			}
			String key = rid > 0 ? rid + "|" + v.get(dc.index) : "?" + String.join("|", v);
			if (!seen.add(key))
				continue;
			String source = kind == 0 ? "database" : kind == 1 ? "WAL frame " + RowLayout.get(r, RowLayout.WAL_FRAME) : "journal";
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
			// versions: is this the current state of the row?
			Map<Long, List<RowHistory.Version>> h = current.get();
			if (h != null && rid > 0 && (status.equals("live") || status.equals("version"))) {
				List<RowHistory.Version> l = h.get(rid);
				RowHistory.Version cur = l == null ? null : RowHistory.currentVersion(l);
				if (l != null && cur == null)
					status = "row deleted later";
				else if (cur != null && cur.row != r && !cur.values.equals(v))
					status = "older version";
			}
			out.add(new Event(t, dc.db, source, status, dc.table, dc.column, dc.format, rid, v.get(dc.index),
					describe(v, cols, dc.index), dc.job, r, kind));
			if (++n >= max)
				break;
		}
		return n;
	}

	/** a few describing values of the row (text first) */
	static String describe(List<String> v, List<String> cols, int skip) {
		StringBuilder sb = new StringBuilder();
		int used = 0;
		for (int pass = 0; pass < 2 && used < 3; pass++)
			for (int i = 0; i < v.size() && used < 3; i++) {
				if (i == skip)
					continue;
				String x = v.get(i);
				if (x == null || x.isEmpty() || x.equals("NULL"))
					continue;
				boolean text = !x.matches("-?\\d+(\\.\\d+)?");
				if ((pass == 0) != text || x.startsWith("[BLOB"))
					continue;
				if (sb.length() > 0)
					sb.append(" | ");
				sb.append(i < cols.size() ? cols.get(i) + "=" : "").append(x.length() > 60 ? x.substring(0, 60) + "\u2026" : x);
				used++;
			}
		return sb.toString();
	}

	/** events per bucket for the density chart (bucket width chosen for ~60 bars) */
	public static Map<Instant, Integer> density(List<Event> events, int bars) {
		Map<Instant, Integer> m = new LinkedHashMap<>();
		if (events.isEmpty())
			return m;
		long a = events.get(0).time().getEpochSecond(), b = events.get(events.size() - 1).time().getEpochSecond();
		long w = Math.max(1, (b - a) / Math.max(1, bars) + 1);
		for (long t = a; t <= b; t += w)
			m.put(Instant.ofEpochSecond(t), 0);
		for (Event e : events) {
			long k = a + ((e.time().getEpochSecond() - a) / w) * w;
			m.merge(Instant.ofEpochSecond(k), 1, Integer::sum);
		}
		return m;
	}

	public static String formatUtc(Instant t) {
		return t == null ? "" : DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT).withZone(ZoneOffset.UTC).format(t);
	}
}
