package fqlite.forensics;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Structure of a database file, determined independently of the recovery: the role of every
 * page (b-tree page of which table or index, overflow page, freelist trunk/leaf page, pointer
 * map, lock-byte page or <i>unreferenced</i>), the free space inside the b-tree pages
 * (freeblocks and unallocated area) and whether free space was wiped with zeros. The file is
 * only read. Used by the audit report and by the classification of recovered records.
 */
public final class PageMap {

	public enum Kind {
		TABLE_INTERIOR("table interior"), TABLE_LEAF("table leaf"), INDEX_INTERIOR("index interior"), INDEX_LEAF("index leaf"),
		OVERFLOW("overflow"), FREELIST_TRUNK("freelist trunk"), FREELIST_LEAF("freelist leaf"), PTRMAP("pointer map"),
		LOCK_BYTE("lock-byte page"), UNREFERENCED("unreferenced");

		public final String label;

		Kind(String l) {
			label = l;
		}
	}

	/** a schema object with its root page */
	public record Root(String type, String name, String table, int page) {
	}

	// ---- header
	public int pageSize, reserved, usable, headerPageCount, filePages, changeCounter, versionValidFor, freelistTrunk, freelistCount,
			schemaCookie, schemaFormat, largestRoot, incrementalVacuum, textEncoding, userVersion, applicationId, sqliteVersion,
			writeVersion, readVersion;
	public long fileSize;
	public boolean headerOk;

	// ---- pages (index = page number, 0 unused)
	public Kind[] kind;
	public String[] owner;
	public final List<Root> roots = new ArrayList<>();
	public final List<String> anomalies = new ArrayList<>() {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean add(String a) {
			return !contains(a) && super.add(a);
		}
	};
	public int freelistPagesWalked;
	/** pages consisting only of zero bytes */
	public final BitSet zeroed = new BitSet();
	/** free space inside b-tree pages: per page the freeblocks (start, length) and the unallocated area */
	public final Map<Integer, List<int[]>> freeblocks = new LinkedHashMap<>();
	public final Map<Integer, int[]> unallocated = new LinkedHashMap<>();
	/** statistics of the free space */
	public int freeblocksTotal, freeblocksZeroed, unallocatedPages, unallocatedZeroed;
	public long unallocatedBytes, unallocatedNonZero;

	private RandomAccessFile raf;
	private Charset cs = StandardCharsets.UTF_8;

	private PageMap() {
	}

	public static PageMap analyse(String path) throws IOException {
		PageMap m = new PageMap();
		File f = new File(path);
		m.fileSize = f.length();
		try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
			m.raf = raf;
			m.run();
		} finally {
			m.raf = null;
		}
		return m;
	}

	// ------------------------------------------------------------------ helpers

	private byte[] page(int n) throws IOException {
		byte[] b = new byte[pageSize];
		long off = (long) (n - 1) * pageSize;
		if (off >= fileSize)
			return null;
		raf.seek(off);
		int r = raf.read(b);
		if (r < pageSize)
			java.util.Arrays.fill(b, Math.max(r, 0), pageSize, (byte) 0);
		return b;
	}

	static int u16(byte[] b, int p) {
		return (b[p] & 0xFF) << 8 | (b[p + 1] & 0xFF);
	}

	static int u32(byte[] b, int p) {
		return (b[p] & 0xFF) << 24 | (b[p + 1] & 0xFF) << 16 | (b[p + 2] & 0xFF) << 8 | (b[p + 3] & 0xFF);
	}

	/** SQLite varint: {value, length} */
	static long[] varint(byte[] b, int p) {
		long v = 0;
		for (int i = 0; i < 9; i++) {
			if (p + i >= b.length)
				return null;
			int x = b[p + i] & 0xFF;
			if (i == 8)
				return new long[] { (v << 8) | x, 9 };
			v = (v << 7) | (x & 0x7F);
			if ((x & 0x80) == 0)
				return new long[] { v, i + 1 };
		}
		return null;
	}

	private boolean valid(int pg) {
		return pg >= 1 && pg <= filePages;
	}

	private void mark(int pg, Kind k, String who) {
		if (!valid(pg))
			return;
		if (kind[pg] != null && kind[pg] != k)
			anomalies.add("page " + pg + " is used twice: as " + kind[pg].label + (owner[pg] != null ? " (" + owner[pg] + ")" : "")
					+ " and as " + k.label + (who != null ? " (" + who + ")" : ""));
		if (kind[pg] == null) {
			kind[pg] = k;
			owner[pg] = who;
		}
	}

	// ------------------------------------------------------------------ analysis

	private void run() throws IOException {
		byte[] h = new byte[100];
		raf.seek(0);
		if (raf.read(h) < 100 || !new String(h, 0, 15, StandardCharsets.US_ASCII).equals("SQLite format 3")) {
			anomalies.add("no SQLite header (magic string missing)");
			pageSize = 4096;
		} else
			headerOk = true;
		int ps = u16(h, 16);
		pageSize = ps == 1 ? 65536 : ps;
		if (pageSize < 512 || pageSize > 65536 || Integer.bitCount(pageSize) != 1) {
			anomalies.add("invalid page size " + ps + " in the header - 4096 assumed");
			pageSize = 4096;
		}
		writeVersion = h[18] & 0xFF;
		readVersion = h[19] & 0xFF;
		reserved = h[20] & 0xFF;
		usable = pageSize - reserved;
		changeCounter = u32(h, 24);
		headerPageCount = u32(h, 28);
		freelistTrunk = u32(h, 32);
		freelistCount = u32(h, 36);
		schemaCookie = u32(h, 40);
		schemaFormat = u32(h, 44);
		largestRoot = u32(h, 52);
		textEncoding = u32(h, 56);
		userVersion = u32(h, 60);
		incrementalVacuum = u32(h, 64);
		applicationId = u32(h, 68);
		versionValidFor = u32(h, 92);
		sqliteVersion = u32(h, 96);
		cs = textEncoding == 2 ? StandardCharsets.UTF_16LE : textEncoding == 3 ? StandardCharsets.UTF_16BE : StandardCharsets.UTF_8;
		filePages = (int) Math.min(Integer.MAX_VALUE - 1, (fileSize + pageSize - 1) / pageSize);
		kind = new Kind[filePages + 1];
		owner = new String[filePages + 1];

		// lock-byte page (only in files larger than 1 GiB)
		long lockPage = 1073741824L / pageSize + 1;
		if (lockPage <= filePages)
			mark((int) lockPage, Kind.LOCK_BYTE, null);
		// pointer map pages (auto-vacuum)
		if (largestRoot != 0) {
			int per = usable / 5;
			for (long p = 2; p <= filePages; p += per + 1)
				mark((int) p, Kind.PTRMAP, null);
		}
		// schema
		walk(1, "sqlite_master", true, 0, new BitSet());
		for (Root r : List.copyOf(roots))
			if (r.page() > 1)
				walk(r.page(), r.name(), r.type().equals("table"), 0, new BitSet());
		freelist();
		// unreferenced pages, zeroed pages
		for (int pg = 1; pg <= filePages; pg++) {
			if (kind[pg] == null)
				kind[pg] = Kind.UNREFERENCED;
			if (kind[pg] == Kind.UNREFERENCED || kind[pg] == Kind.FREELIST_LEAF || kind[pg] == Kind.FREELIST_TRUNK) {
				byte[] b = page(pg);
				boolean z = true;
				for (int i = (kind[pg] == Kind.FREELIST_TRUNK ? 8 + 4 * Math.max(0, Math.min(u32(b, 4), (usable - 8) / 4)) : 0); i < usable
						&& z; i++)
					z = b[i] == 0;
				if (z)
					zeroed.set(pg);
			}
		}
	}

	private void freelist() throws IOException {
		int trunk = freelistTrunk;
		BitSet seen = new BitSet();
		int guard = 0;
		while (trunk != 0) {
			if (!valid(trunk)) {
				anomalies.add("freelist trunk pointer " + trunk + " is outside the file");
				break;
			}
			if (seen.get(trunk)) {
				anomalies.add("freelist trunk chain contains a loop (page " + trunk + ")");
				break;
			}
			seen.set(trunk);
			mark(trunk, Kind.FREELIST_TRUNK, null);
			freelistPagesWalked++;
			byte[] b = page(trunk);
			int next = u32(b, 0), n = u32(b, 4);
			if (n < 0 || n > (usable - 8) / 4) {
				anomalies.add("freelist trunk page " + trunk + " claims " + n + " leaf pages (max. " + (usable - 8) / 4 + ")");
				n = Math.max(0, Math.min(n, (usable - 8) / 4));
			}
			for (int i = 0; i < n; i++) {
				int leaf = u32(b, 8 + 4 * i);
				if (!valid(leaf)) {
					anomalies.add("freelist leaf pointer " + leaf + " on trunk page " + trunk + " is outside the file");
					continue;
				}
				mark(leaf, Kind.FREELIST_LEAF, null);
				freelistPagesWalked++;
			}
			trunk = next;
			if (++guard > filePages)
				break;
		}
	}

	/** walks a b-tree; page 1 additionally yields the schema (roots) */
	private void walk(int pg, String name, boolean table, int depth, BitSet seen) throws IOException {
		if (!valid(pg)) {
			anomalies.add("b-tree " + name + ": page number " + pg + " outside the file");
			return;
		}
		if (seen.get(pg)) {
			anomalies.add("b-tree " + name + ": page " + pg + " is referenced twice (loop)");
			return;
		}
		if (depth > 40) {
			anomalies.add("b-tree " + name + ": too deep (manipulated child pointers?)");
			return;
		}
		seen.set(pg);
		byte[] b = page(pg);
		int base = pg == 1 ? 100 : 0;
		int t = b[base] & 0xFF;
		Kind k = switch (t) {
		case 0x05 -> Kind.TABLE_INTERIOR;
		case 0x0D -> Kind.TABLE_LEAF;
		case 0x02 -> Kind.INDEX_INTERIOR;
		case 0x0A -> Kind.INDEX_LEAF;
		default -> null;
		};
		if (k == null) {
			anomalies.add("b-tree " + name + ": page " + pg + " has no valid b-tree page type (0x" + Integer.toHexString(t) + ")");
			return;
		}
		mark(pg, k, name);
		boolean interior = k == Kind.TABLE_INTERIOR || k == Kind.INDEX_INTERIOR;
		int hdr = base + (interior ? 12 : 8);
		int ncell = u16(b, base + 3);
		int content = u16(b, base + 5);
		if (content == 0)
			content = 65536;
		int ptrEnd = hdr + 2 * ncell;
		// free space
		if (ptrEnd <= usable && content <= pageSize) {
			int[] un = { ptrEnd, Math.min(content, usable) };
			unallocated.put(pg, un);
			if (un[1] > un[0]) {
				unallocatedPages++;
				unallocatedBytes += un[1] - un[0];
				int nz = 0;
				for (int i = un[0]; i < un[1]; i++)
					if (b[i] != 0)
						nz++;
				unallocatedNonZero += nz;
				if (nz == 0)
					unallocatedZeroed++;
			}
		} else
			anomalies.add("page " + pg + " (" + name + "): " + ncell + " cells do not fit the page");
		List<int[]> fbs = new ArrayList<>();
		int fb = u16(b, base + 1), guard = 0;
		while (fb != 0 && guard++ < 1000) {
			if (fb < ptrEnd || fb + 4 > usable) {
				anomalies.add("page " + pg + " (" + name + "): freeblock pointer " + fb + " outside the cell content area");
				break;
			}
			int next = u16(b, fb), size = u16(b, fb + 2);
			if (size < 4 || fb + size > usable) {
				anomalies.add("page " + pg + " (" + name + "): freeblock at " + fb + " with invalid size " + size);
				break;
			}
			fbs.add(new int[] { fb, size });
			freeblocksTotal++;
			boolean z = true;
			for (int i = fb + 4; i < fb + size && z; i++)
				z = b[i] == 0;
			if (z)
				freeblocksZeroed++;
			if (next != 0 && next <= fb) {
				anomalies.add("page " + pg + " (" + name + "): freeblock list is not ascending (loop?)");
				break;
			}
			fb = next;
		}
		if (!fbs.isEmpty())
			freeblocks.put(pg, fbs);
		// cells
		for (int i = 0; i < ncell; i++) {
			if (hdr + 2 * i + 2 > pageSize)
				break;
			int c = u16(b, hdr + 2 * i);
			if (c < ptrEnd || c >= usable) {
				anomalies.add("page " + pg + " (" + name + "): invalid cell pointer " + c);
				continue;
			}
			try {
				cell(b, pg, c, k, name, table, depth, seen);
			} catch (RuntimeException e) {
				anomalies.add("page " + pg + " (" + name + "): cell at " + c + " unreadable");
			}
		}
		if (interior) {
			int right = u32(b, base + 8);
			walk(right, name, table, depth + 1, seen);
		}
	}

	private void cell(byte[] b, int pg, int c, Kind k, String name, boolean table, int depth, BitSet seen) throws IOException {
		int p = c;
		if (k == Kind.TABLE_INTERIOR) {
			walk(u32(b, p), name, table, depth + 1, seen);
			return;
		}
		if (k == Kind.INDEX_INTERIOR) {
			walk(u32(b, p), name, table, depth + 1, seen);
			p += 4;
		}
		long[] pl = varint(b, p);
		if (pl == null)
			return;
		p += (int) pl[1];
		if (k == Kind.TABLE_LEAF) {
			long[] rid = varint(b, p);
			if (rid == null)
				return;
			p += (int) rid[1];
		}
		long payload = pl[0];
		int u = usable;
		long x = k == Kind.TABLE_LEAF ? u - 35 : ((long) (u - 12) * 64 / 255) - 23;
		long local = payload;
		if (payload > x) {
			long mm = ((long) (u - 12) * 32 / 255) - 23;
			long kk = mm + ((payload - mm) % (u - 4));
			local = kk <= x ? kk : mm;
		}
		byte[] data = null;
		boolean schema = pg == 1 || "sqlite_master".equals(name);
		if (schema && k == Kind.TABLE_LEAF)
			data = new byte[(int) Math.min(payload, 1 << 20)];
		if (data != null)
			System.arraycopy(b, p, data, 0, (int) Math.min(local, Math.min(data.length, b.length - p)));
		if (payload > local) {
			if (p + local + 4 > b.length)
				return;
			int ov = u32(b, p + (int) local);
			long got = local;
			BitSet ovSeen = new BitSet();
			while (ov != 0 && got < payload) {
				if (!valid(ov)) {
					anomalies.add("overflow chain of a cell on page " + pg + " (" + name + ") points outside the file (" + ov + ")");
					break;
				}
				if (ovSeen.get(ov)) {
					anomalies.add("overflow chain on page " + pg + " (" + name + ") contains a loop");
					break;
				}
				ovSeen.set(ov);
				mark(ov, Kind.OVERFLOW, name);
				byte[] o = page(ov);
				int n = (int) Math.min(u - 4, payload - got);
				if (data != null && got < data.length)
					System.arraycopy(o, 4, data, (int) got, (int) Math.min(n, data.length - got));
				got += n;
				ov = u32(o, 0);
			}
		}
		if (data != null)
			schemaRecord(data);
	}

	/** columns type, name, tbl_name, rootpage of a sqlite_master record */
	private void schemaRecord(byte[] d) {
		long[] hl = varint(d, 0);
		if (hl == null)
			return;
		int p = (int) hl[1], end = (int) hl[0];
		List<Long> types = new ArrayList<>();
		while (p < end && p < d.length) {
			long[] v = varint(d, p);
			if (v == null)
				return;
			types.add(v[0]);
			p += (int) v[1];
		}
		int q = end;
		List<Object> vals = new ArrayList<>();
		for (long t : types) {
			int len = len(t);
			if (q + len > d.length)
				return;
			if (t >= 13 && t % 2 == 1)
				vals.add(new String(d, q, len, cs));
			else if (t >= 1 && t <= 6) {
				long v = 0;
				for (int i = 0; i < len; i++)
					v = (v << 8) | (d[q + i] & 0xFF);
				if (len < 8 && (d[q] & 0x80) != 0)
					v -= 1L << (8 * len);
				vals.add(v);
			} else if (t == 8)
				vals.add(0L);
			else if (t == 9)
				vals.add(1L);
			else
				vals.add(null);
			q += len;
		}
		if (vals.size() >= 4 && vals.get(0) instanceof String ty && vals.get(3) instanceof Long rp && rp > 0)
			roots.add(new Root(ty, String.valueOf(vals.get(1)), String.valueOf(vals.get(2)), (int) (long) rp));
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

	// ------------------------------------------------------------------ queries

	public Kind kindOf(int pg) {
		return pg >= 1 && pg <= filePages ? kind[pg] : null;
	}

	public int pageOf(long fileOffset) {
		return (int) (fileOffset / pageSize) + 1;
	}

	/** where in its page a file offset lies: "freeblock", "unallocated area", "cell area" or the page kind */
	public String areaOf(long fileOffset) {
		int pg = pageOf(fileOffset);
		Kind k = kindOf(pg);
		if (k == null)
			return "beyond the end of the file";
		int o = (int) (fileOffset % pageSize);
		if (k == Kind.TABLE_LEAF || k == Kind.TABLE_INTERIOR || k == Kind.INDEX_LEAF || k == Kind.INDEX_INTERIOR) {
			for (int[] fb : freeblocks.getOrDefault(pg, List.of()))
				if (o >= fb[0] && o < fb[0] + fb[1])
					return "freeblock";
			int[] un = unallocated.get(pg);
			if (un != null && o >= un[0] && o < un[1])
				return "unallocated area";
			return "cell area";
		}
		return k.label;
	}

	public Map<Kind, Integer> counts() {
		Map<Kind, Integer> m = new LinkedHashMap<>();
		for (Kind k : Kind.values())
			m.put(k, 0);
		for (int pg = 1; pg <= filePages; pg++)
			if (kind[pg] != null)
				m.merge(kind[pg], 1, Integer::sum);
		return m;
	}

	/** the in-header database size is only valid if it is non-zero and the version-valid-for number matches */
	public boolean headerCountValid() {
		return headerPageCount != 0 && changeCounter == versionValidFor;
	}
}
