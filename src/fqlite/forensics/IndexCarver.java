package fqlite.forensics;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import fqlite.base.Job;
import fqlite.descriptor.IndexDescriptor;
import fqlite.descriptor.TableDescriptor;

/**
 * Deleted index entries. An index entry holds the indexed values and the rowid of its row; it
 * often survives in a freeblock, in the unallocated area of an index page or on a freed index
 * page after the row itself was overwritten. This class reads the index b-trees of the
 * database file itself (independent of FQLite's table carving), collects the live entries and
 * carves the free areas:
 * <ul>
 * <li>unallocated area of index leaf pages and freed / orphaned index leaf pages: complete
 * cells (header intact) - confidence high</li>
 * <li>freeblocks: the 4-byte freeblock header overwrote the payload length, the header length
 * and the first one or two serial types; they are reconstructed from the freeblock size and the
 * column types of the index - confidence medium (unique reconstruction) or low</li>
 * </ul>
 * Entries identical to a live entry of the same index are left out.
 */
public final class IndexCarver {

	private IndexCarver() {
	}

	public record Entry(String index, String table, List<String> columns, List<String> values, long rowid,
			long offset, String where, String confidence, String rowState) {
	}

	private static final class Idx {
		String name, table;
		int root;
		List<String> cols = new ArrayList<>();
		List<String> affinity = new ArrayList<>();   // per indexed column: INT, TEXT, REAL, BLOB, ANY
		int k = -1;                                  // number of serial types (indexed columns + rowid)
		Set<Integer> pages = new HashSet<>();
		Set<String> live = new HashSet<>();
		long maxRowid = 0;
		/* value domain of the live entries per column: values, numeric range, text length range */
		List<Set<String>> values = new ArrayList<>();
		long[] min, max;
		int[] minLen, maxLen;
		/* rowid -> indexed values of every known version of the row (table data) */
		Map<Long, List<List<String>>> versions = new HashMap<>();

		void learn(Rec r) {
			int n = r.values.size() - 1;
			if (min == null) {
				min = new long[n];
				max = new long[n];
				minLen = new int[n];
				maxLen = new int[n];
				java.util.Arrays.fill(min, Long.MAX_VALUE);
				java.util.Arrays.fill(max, Long.MIN_VALUE);
				java.util.Arrays.fill(minLen, Integer.MAX_VALUE);
			}
			for (int i = 0; i < n && i < min.length; i++) {
				while (values.size() <= i)
					values.add(new HashSet<>());
				String v = r.values.get(i);
				if (values.get(i).size() < 20000)
					values.get(i).add(v);
				long t = r.types.get(i);
				if (t >= 1 && t <= 9 && t != 7) {
					long x = RowLayout.number(v);
					min[i] = Math.min(min[i], x);
					max[i] = Math.max(max[i], x);
				}
				if (t >= 12) {
					minLen[i] = Math.min(minLen[i], v.length());
					maxLen[i] = Math.max(maxLen[i], v.length());
				}
			}
		}

		/** bonus for values that look like the live data */
		int domainScore(Rec r) {
			int s = 0;
			for (int i = 0; i < r.values.size() - 1; i++) {
				String v = r.values.get(i);
				long t = r.types.get(i);
				if (i < values.size() && values.get(i).contains(v))
					s += 4;
				if (min != null && i < min.length) {
					if (t >= 1 && t <= 9 && t != 7 && min[i] <= max[i]) {
						long x = RowLayout.number(v);
						long span = Math.max(1, max[i] - min[i]);
						s += (x >= min[i] - span && x <= max[i] + span) ? 1 : -3;
					}
					if (t >= 12 && minLen[i] <= maxLen[i])
						s += (v.length() >= minLen[i] / 2 && v.length() <= maxLen[i] * 2 + 2) ? 1 : -3;
				}
			}
			return confirmed(r) ? s + 8 : s;
		}

		/** the indexed values equal those of a known version of the row */
		boolean confirmed(Rec r) {
			List<List<String>> vs = versions.get(r.lastAsLong());
			if (vs != null)
				for (List<String> x : vs)
					if (x.equals(r.values.subList(0, r.values.size() - 1)))
						return true;
			return false;
		}
	}

	public static List<Entry> carve(Job job) throws IOException {
		List<Entry> out = new ArrayList<>();
		if (job == null || job.path == null || job.indices == null || job.indices.isEmpty())
			return out;
		File f = new File(job.path);
		try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
			byte[] head = new byte[100];
			raf.readFully(head);
			int ps = ((head[16] & 0xFF) << 8) | (head[17] & 0xFF);
			if (ps == 1)
				ps = 65536;
			int reserved = head[20] & 0xFF;
			int usable = ps - reserved;
			long npages = raf.length() / ps;
			Charset cs = job.db_encoding != null ? job.db_encoding : java.nio.charset.StandardCharsets.UTF_8;
			PageReader pr = new PageReader(raf, ps);

			// table affinities
			Map<String, TableDescriptor> tables = new HashMap<>();
			for (TableDescriptor td : job.headers)
				tables.put(td.tblname, td);

			List<Idx> idx = new ArrayList<>();
			Set<Integer> indexPages = new HashSet<>();
			for (IndexDescriptor d : job.indices) {
				if (d.root <= 0)
					continue;
				Idx x = new Idx();
				x.name = d.idxname;
				x.table = d.tablename;
				x.root = d.root;
				if (d.columnnames != null)
					x.cols.addAll(d.columnnames);
				TableDescriptor td = tables.get(d.tablename);
				for (String c : x.cols)
					x.affinity.add(affinityOf(td, c));
				walk(pr, x.root, npages, x.pages, 0);
				indexPages.addAll(x.pages);
				// live entries and the number of columns
				for (int p : x.pages) {
					byte[] pg = pr.page(p);
					if (pg == null)
						continue;
					int h = p == 1 ? 100 : 0;
					int type = pg[h] & 0xFF;
					if (type != 0x0A && type != 0x02)
						continue;
					int n = u16(pg, h + 3);
					int hdr = type == 0x0A ? 8 : 12;
					for (int i = 0; i < n; i++) {
						int c = u16(pg, h + hdr + 2 * i);
						if (c <= 0 || c >= usable)
							continue;
						int pos = c + (type == 0x02 ? 4 : 0);
						Rec r = Rec.parse(pg, pos, usable, cs, true);
						if (r == null)
							continue;
						if (x.k < 0)
							x.k = r.types.size();
						x.live.add(String.join("\u0001", r.values));
						x.learn(r);
						if (r.lastAsLong() > x.maxRowid)
							x.maxRowid = r.lastAsLong();
					}
				}
				if (x.k < 0)
					x.k = x.cols.size() + 1;
				// indexed values of every known version of each row (live, carved, WAL, journal)
				if (td != null && td.columnnames != null && x.cols.size() == x.k - 1) {
					int[] pos = new int[x.cols.size()];
					boolean ok = true;
					for (int i = 0; i < pos.length; i++) {
						pos[i] = td.columnnames.indexOf(x.cols.get(i));
						ok &= pos[i] >= 0;
					}
					if (ok)
						for (Map.Entry<Long, List<RowHistory.Version>> e : RowHistory.collect(job, x.table).entrySet())
							for (RowHistory.Version v : e.getValue()) {
								List<String> p = new ArrayList<>();
								for (int c : pos)
									p.add(c < v.values.size() ? RowHistory.norm(v.values.get(c)) : "");
								x.versions.computeIfAbsent(e.getKey(), z -> new ArrayList<>()).add(p);
							}
				}
				while (x.affinity.size() < x.k - 1)
					x.affinity.add("ANY");
				idx.add(x);
			}
			if (idx.isEmpty())
				return out;

			Set<Integer> freelist = new HashSet<>(job.freelistpages == null ? List.of() : job.freelistpages);
			Set<String> seen = new HashSet<>();
			for (int p = 1; p <= npages; p++) {
				byte[] pg = pr.page(p);
				if (pg == null)
					continue;
				int h = p == 1 ? 100 : 0;
				if ((pg[h] & 0xFF) != 0x0A)
					continue;
				Idx owner = null;
				for (Idx x : idx)
					if (x.pages.contains(p))
						owner = x;
				boolean reachable = owner != null;
				int n = u16(pg, h + 3);
				int ptrEnd = h + 8 + 2 * n;
				int contentStart = u16(pg, h + 5);
				if (contentStart == 0)
					contentStart = 65536;
				long base = (long) (p - 1) * ps;
				if (!reachable) {
					// freed or orphaned index page: its cells are complete
					String where = freelist.contains(p) ? "freelist page " + p : "unreferenced index page " + p;
					for (int i = 0; i < n && ptrEnd <= usable; i++) {
						int c = u16(pg, h + 8 + 2 * i);
						if (c <= 0 || c >= usable)
							continue;
						Rec r = Rec.parse(pg, c, usable, cs, true);
						if (r == null)
							continue;
						Idx x = matchIndex(idx, r);
						add(out, seen, x, r, base + c, where, "high", null);
					}
					continue;
				}
				// unallocated area: complete old cells
				BitSet used = new BitSet(usable);
				int from = Math.max(ptrEnd, h + 8);
				for (int pos = from; pos < Math.min(contentStart, usable) - 3; pos++) {
					if (used.get(pos))
						continue;
					Rec r = Rec.parse(pg, pos, Math.min(contentStart, usable), cs, false);
					if (r == null || r.types.size() != owner.k || !plausible(owner, r))
						continue;
					add(out, seen, owner, r, base + pos, "page " + p + " unallocated", "high", null);
					used.set(pos, r.end);
					pos = r.end - 1;
				}
				// freeblocks
				int fb = u16(pg, h + 1), guard = 0;
				while (fb > 0 && fb + 4 <= usable && guard++ < 10000) {
					int next = u16(pg, fb), size = u16(pg, fb + 2);
					if (size < 4 || fb + size > usable)
						break;
					Rec r = reconstruct(owner, pg, fb, size, cs);
					if (r != null)
						add(out, seen, owner, r, base + fb, "page " + p + " freeblock", r.confidence, null);
					// adjacent freed cells merge into one freeblock: the following cells are complete
					for (int pos = r != null ? r.end : fb + 4; pos < fb + size - 3; pos++) {
						Rec c = Rec.parse(pg, pos, fb + size, cs, false);
						if (c == null || c.types.size() != owner.k || !plausible(owner, c))
							continue;
						add(out, seen, owner, c, base + pos, "page " + p + " freeblock (merged cell)", "high", null);
						pos = c.end - 1;
					}
					if (next <= fb)
						break;
					fb = next;
				}
			}
		}
		classify(job, out);
		return out;
	}

	private static void add(List<Entry> out, Set<String> seen, Idx x, Rec r, long off, String where, String conf, String state) {
		String key = String.join("\u0001", r.values);
		if (x != null && x.live.contains(key))
			return;                              // identical to a live index entry
		if (!seen.add((x == null ? "?" : x.name) + "\u0002" + key))
			return;                              // the same entry found twice
		List<String> cols = new ArrayList<>(x == null ? List.of() : x.cols);
		while (cols.size() < r.values.size() - 1)
			cols.add("column " + (cols.size() + 1));
		cols.add("rowid");
		out.add(new Entry(x == null ? "(unknown index)" : x.name, x == null ? "" : x.table, cols, r.values,
				r.lastAsLong(), off, where, conf, state));
	}

	/** row state of every entry: what the table still knows about the row */
	private static void classify(Job job, List<Entry> out) {
		Map<String, Map<Long, List<RowHistory.Version>>> cache = new HashMap<>();
		for (int i = 0; i < out.size(); i++) {
			Entry e = out.get(i);
			String state;
			if (e.table().isEmpty() || e.rowid() <= 0)
				state = "unknown";
			else {
				Map<Long, List<RowHistory.Version>> h = cache.computeIfAbsent(e.table(), t -> RowHistory.collect(job, t));
				List<RowHistory.Version> l = h.get(e.rowid());
				RowHistory.Version cur = l == null ? null : RowHistory.currentVersion(l);
				if (l == null || l.isEmpty())
					state = "row deleted - no other trace";
				else if (cur != null)
					state = "row live with other values (older index value)";
				else
					state = "row deleted - also recovered (" + l.get(l.size() - 1).source.label + ")";
			}
			out.set(i, new Entry(e.index(), e.table(), e.columns(), e.values(), e.rowid(), e.offset(), e.where(), e.confidence(), state));
		}
	}

	private static Idx matchIndex(List<Idx> idx, Rec r) {
		Idx best = null;
		int n = 0;
		for (Idx x : idx)
			if (x.k == r.types.size() && plausible(x, r)) {
				best = x;
				n++;
			}
		return n == 1 ? best : null;
	}

	private static boolean plausible(Idx x, Rec r) {
		for (int i = 0; i < r.types.size() - 1 && i < x.affinity.size(); i++)
			if (!typeFits(x.affinity.get(i), r.types.get(i)))
				return false;
		long id = r.lastAsLong();
		long t = r.types.get(r.types.size() - 1);
		return id > 0 && (t >= 1 && t <= 6 || t == 9) && (x.maxRowid == 0 || id <= x.maxRowid * 10 + 1000);
	}

	static boolean typeFits(String aff, long t) {
		if (t == 0)
			return true;
		return switch (aff) {
		case "INT" -> t >= 1 && t <= 6 || t == 8 || t == 9;
		case "REAL" -> t >= 1 && t <= 9;
		case "TEXT" -> t >= 13 && t % 2 == 1;
		case "BLOB" -> t >= 12 && t % 2 == 0;
		default -> t != 10 && t != 11;
		};
	}

	static String affinityOf(TableDescriptor td, String col) {
		if (td == null || td.columnnames == null)
			return "ANY";
		int i = td.columnnames.indexOf(col);
		if (i < 0 || td.sqltypes == null || i >= td.sqltypes.size())
			return "ANY";
		String t = td.sqltypes.get(i).toUpperCase();
		if (t.contains("INT"))
			return "INT";
		if (t.contains("CHAR") || t.contains("CLOB") || t.contains("TEXT"))
			return "TEXT";
		if (t.contains("BLOB") || t.isEmpty())
			return t.isEmpty() ? "ANY" : "BLOB";
		if (t.contains("REAL") || t.contains("FLOA") || t.contains("DOUB"))
			return "REAL";
		return "ANY";
	}

	/**
	 * Freeblock of an index leaf page: [next(2)][size(2)] overwrote either
	 * [pll(1)][hlen(1)][t1][t2] or [pll(2)][hlen(1)][t1]; the remaining serial types and the
	 * data follow. The missing types are chosen so that the record fits the freeblock and its
	 * values are plausible; a unique best candidate is accepted.
	 */
	private static Rec reconstruct(Idx x, byte[] pg, int fb, int size, Charset cs) {
		Rec best = null;
		int bestScore = Integer.MIN_VALUE, ties = 0;
		for (int lost = 2; lost >= 1; lost--) {
			int knownTypes = x.k - lost;
			if (knownTypes < 0)
				continue;
			int pos = fb + 4;
			List<Long> known = new ArrayList<>();
			for (int i = 0; i < knownTypes; i++) {
				long[] v = varint(pg, pos, fb + size);
				if (v == null)
					break;
				known.add(v[0]);
				pos += (int) v[1];
			}
			if (known.size() != knownTypes)
				continue;
			int avail = fb + size - pos;                       // bytes for the data
			int knownLen = 0;
			for (long t : known)
				knownLen += len(t);
			if (knownLen > avail)
				continue;
			for (List<Long> lostTypes : candidates(x, lost, avail - knownLen)) {
				List<Long> types = new ArrayList<>(lostTypes);
				types.addAll(known);
				int total = 0;
				for (long t : types)
					total += len(t);
				if (total > avail)
					continue;
				Rec r = Rec.decode(pg, pos, types, cs);
				if (r == null || !plausible(x, r))
					continue;
				int score = r.score() + x.domainScore(r) + (total == avail ? 3 : (avail - total >= 4 ? 1 : -3));
				if (score > bestScore) {
					bestScore = score;
					best = r;
					ties = 0;
				} else if (score == bestScore && best != null && !best.values.equals(r.values))
					ties++;
			}
		}
		if (best == null || bestScore < 2 || ties > 0)
			return null;                     // no unique reconstruction: not reported
		best.confidence = x.confirmed(best) ? "high (matches a version of the row)" : "medium";
		return best;
	}

	/** possible serial types of the lost leading columns */
	private static List<List<Long>> candidates(Idx x, int lost, int room) {
		List<List<Long>> l = new ArrayList<>();
		List<List<Long>> per = new ArrayList<>();
		for (int i = 0; i < lost; i++) {
			int col = i;                          // the lost types are the first ones
			boolean isRowid = col == x.k - 1;
			List<Long> c = new ArrayList<>();
			if (isRowid) {
				for (long t : new long[] { 1, 2, 3, 4, 5, 6, 9 })
					c.add(t);
			} else {
				String a = col < x.affinity.size() ? x.affinity.get(col) : "ANY";
				c.add(0L);
				if (!a.equals("TEXT") && !a.equals("BLOB"))
					for (long t : new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 })
						if (typeFits(a, t))
							c.add(t);
				if (a.equals("TEXT") || a.equals("ANY"))
					for (int n = 1; n <= room; n++)
						c.add(13L + 2L * n);
				if (a.equals("BLOB"))
					for (int n = 1; n <= room; n++)
						c.add(12L + 2L * n);
			}
			per.add(c);
		}
		if (lost == 1) {
			for (long a : per.get(0))
				l.add(List.of(a));
		} else {
			for (long a : per.get(0))
				for (long b : per.get(1))
					if (len(a) + len(b) <= room)
						l.add(List.of(a, b));
		}
		return l;
	}

	// ------------------------------------------------------------------ records

	static final class Rec {
		List<Long> types = new ArrayList<>();
		List<String> values = new ArrayList<>();
		List<Boolean> printable = new ArrayList<>();
		int end;
		String confidence = "high";

		long lastAsLong() {
			return values.isEmpty() ? -1 : RowLayout.number(values.get(values.size() - 1));
		}

		int score() {
			int s = 0;
			for (int i = 0; i < types.size(); i++) {
				long t = types.get(i);
				if (t >= 13 && t % 2 == 1)
					s += printable.get(i) ? 2 : -5;
				else if (t == 0)
					s -= 1;
				else
					s += 1;
			}
			return s;
		}

		/** a complete cell: [pll][header length][types][data]; strict = inside a b-tree page */
		static Rec parse(byte[] pg, int pos, int limit, Charset cs, boolean strict) {
			long[] pll = varint(pg, pos, limit);
			if (pll == null || pll[0] < 2 || pll[0] > limit)
				return null;
			int p = pos + (int) pll[1];
			long[] hl = varint(pg, p, limit);
			if (hl == null || hl[0] < 2 || hl[0] > 64 || hl[0] > pll[0])
				return null;
			int hend = p + (int) hl[0];
			int q = p + (int) hl[1];
			List<Long> types = new ArrayList<>();
			while (q < hend) {
				long[] t = varint(pg, q, hend);
				if (t == null || t[0] == 10 || t[0] == 11)
					return null;
				types.add(t[0]);
				q += (int) t[1];
			}
			if (q != hend || types.size() < 2)
				return null;
			int total = (int) hl[0];
			for (long t : types)
				total += len(t);
			if (total != pll[0] || hend + total - hl[0] > limit)
				return null;
			Rec r = decode(pg, hend, types, cs);
			if (r == null)
				return null;
			if (!strict)
				for (boolean b : r.printable)
					if (!b)
						return null;
			return r;
		}

		static Rec decode(byte[] pg, int pos, List<Long> types, Charset cs) {
			Rec r = new Rec();
			r.types = types;
			int p = pos;
			for (long t : types) {
				int n = len(t);
				if (p + n > pg.length)
					return null;
				String v;
				boolean ok = true;
				if (t == 0)
					v = "";
				else if (t == 8)
					v = "0";
				else if (t == 9)
					v = "1";
				else if (t >= 1 && t <= 6) {
					long x = pg[p];                // sign-extended
					for (int i = 1; i < n; i++)
						x = (x << 8) | (pg[p + i] & 0xFF);
					v = String.valueOf(x);
				} else if (t == 7) {
					long x = 0;
					for (int i = 0; i < 8; i++)
						x = (x << 8) | (pg[p + i] & 0xFF);
					double d = Double.longBitsToDouble(x);
					v = String.valueOf(d);
					ok = !Double.isNaN(d) && !Double.isInfinite(d);
				} else if (t >= 13 && t % 2 == 1) {
					CharsetDecoder dec = cs.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
							.onUnmappableCharacter(CodingErrorAction.REPORT);
					try {
						v = dec.decode(ByteBuffer.wrap(pg, p, n)).toString();
						for (char ch : v.toCharArray())
							if (ch < 0x20 && ch != '\n' && ch != '\r' && ch != '\t')
								ok = false;
					} catch (Exception e) {
						v = "x'" + hex(pg, p, n) + "'";
						ok = false;
					}
				} else
					v = "x'" + hex(pg, p, n) + "'";
				r.values.add(v);
				r.printable.add(ok);
				p += n;
			}
			r.end = p;
			return r;
		}
	}

	static int len(long t) {
		if (t >= 12)
			return (int) ((t - (t % 2 == 0 ? 12 : 13)) / 2);
		return switch ((int) t) {
		case 1 -> 1;
		case 2 -> 2;
		case 3 -> 3;
		case 4 -> 4;
		case 5 -> 6;
		case 6, 7 -> 8;
		default -> 0;
		};
	}

	/** {value, length} or null */
	static long[] varint(byte[] b, int pos, int limit) {
		long v = 0;
		for (int i = 0; i < 9; i++) {
			if (pos + i >= limit || pos + i >= b.length)
				return null;
			int c = b[pos + i] & 0xFF;
			if (i == 8)
				return new long[] { (v << 8) | c, 9 };
			v = (v << 7) | (c & 0x7F);
			if ((c & 0x80) == 0)
				return new long[] { v, i + 1 };
		}
		return null;
	}

	static int u16(byte[] b, int p) {
		return p + 1 < b.length ? ((b[p] & 0xFF) << 8) | (b[p + 1] & 0xFF) : 0;
	}

	static String hex(byte[] b, int p, int n) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < n && p + i < b.length; i++)
			sb.append(String.format("%02x", b[p + i]));
		return sb.toString();
	}

	private static void walk(PageReader pr, int p, long npages, Set<Integer> pages, int depth) throws IOException {
		if (p < 1 || p > npages || depth > 40 || !pages.add(p))
			return;
		byte[] pg = pr.page(p);
		if (pg == null)
			return;
		int h = p == 1 ? 100 : 0;
		int type = pg[h] & 0xFF;
		if (type != 0x02)
			return;
		int n = u16(pg, h + 3);
		for (int i = 0; i < n; i++) {
			int c = u16(pg, h + 12 + 2 * i);
			if (c > 0 && c + 4 <= pg.length)
				walk(pr, (int) (((pg[c] & 0xFFL) << 24) | ((pg[c + 1] & 0xFF) << 16) | ((pg[c + 2] & 0xFF) << 8) | (pg[c + 3] & 0xFF)),
						npages, pages, depth + 1);
		}
		walk(pr, (int) (((pg[h + 8] & 0xFFL) << 24) | ((pg[h + 9] & 0xFF) << 16) | ((pg[h + 10] & 0xFF) << 8) | (pg[h + 11] & 0xFF)),
				npages, pages, depth + 1);
	}

	/** page cache (index pages are read several times) */
	private static final class PageReader {
		final RandomAccessFile raf;
		final int ps;
		final Map<Integer, byte[]> cache = new HashMap<>();

		PageReader(RandomAccessFile raf, int ps) {
			this.raf = raf;
			this.ps = ps;
		}

		byte[] page(int p) throws IOException {
			byte[] b = cache.get(p);
			if (b != null)
				return b;
			long off = (long) (p - 1) * ps;
			if (off + ps > raf.length())
				return null;
			b = new byte[ps];
			raf.seek(off);
			raf.readFully(b);
			if (cache.size() < 4096)
				cache.put(p, b);
			return b;
		}
	}
}
