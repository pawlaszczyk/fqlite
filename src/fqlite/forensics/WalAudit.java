package fqlite.forensics;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Checks a write-ahead log the way SQLite does on recovery: the header checksum, the salts and
 * the cumulative checksum of every frame. Each frame gets a state:
 * <ul>
 * <li><b>current</b> &ndash; committed, latest version of its page (what SQLite reads),</li>
 * <li><b>superseded</b> &ndash; committed, but a later committed frame holds the same page,</li>
 * <li><b>uncommitted</b> &ndash; valid, but after the last commit frame (never committed),</li>
 * <li><b>stale</b> &ndash; salts of an earlier WAL generation (left over after a WAL reset),</li>
 * <li><b>invalid</b> &ndash; checksum wrong or after the first invalid frame (SQLite ignores it).</li>
 * </ul>
 * The committed transactions allow a "time travel": the database as it was after each commit.
 */
public final class WalAudit {

	public enum State {
		CURRENT("current"), SUPERSEDED("superseded"), UNCOMMITTED("uncommitted"), STALE("stale (earlier generation)"),
		INVALID("invalid checksum");

		public final String label;

		State(String l) {
			label = l;
		}
	}

	public static final class Frame {
		public int index;            // 1-based
		public long offset;          // file offset of the frame header
		public int page;
		public int commitSize;       // "database size after commit" (0 = no commit frame)
		public long salt1, salt2;
		public long ck1, ck2;        // stored checksum
		public boolean saltsMatch, checksumOk;
		public State state;
		public int tx;               // transaction number (valid frames of the current generation), 0 otherwise
	}

	/** a committed transaction */
	public record Commit(int tx, int firstFrame, int lastFrame, int dbPages, Set<Integer> pages) {
	}

	// ---- header
	public long magic;
	public int version, pageSize;
	public long checkpointSeq, salt1, salt2, hck1, hck2;
	public boolean headerOk, headerChecksumOk, bigEndianChecksum;
	public long fileSize;
	public int trailingBytes;
	public final List<Frame> frames = new ArrayList<>();
	public final List<Commit> commits = new ArrayList<>();
	public int lastValidCommitFrame;
	/** uncommitted frames whose checksums were not finalised (open transaction) */
	public int uncommittedWithoutChecksum;
	public final List<String> anomalies = new ArrayList<>();
	// ---- -shm (optional)
	public boolean shmPresent;
	public long shmMxFrame = -1, shmBackfill = -1, shmSalt1, shmSalt2;
	public String path;

	private WalAudit() {
	}

	public static WalAudit analyse(String walPath) throws IOException {
		WalAudit a = new WalAudit();
		a.path = walPath;
		File f = new File(walPath);
		a.fileSize = f.length();
		try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
			a.run(raf);
		}
		String shm = walPath.endsWith("-wal") ? walPath.substring(0, walPath.length() - 4) + "-shm" : null;
		if (shm != null && new File(shm).isFile() && new File(shm).length() >= 136)
			a.shm(new File(shm));
		return a;
	}

	private static long u32(byte[] b, int p) {
		return (b[p] & 0xFFL) << 24 | (b[p + 1] & 0xFFL) << 16 | (b[p + 2] & 0xFFL) << 8 | (b[p + 3] & 0xFFL);
	}

	/** SQLite's walChecksumBytes */
	private long[] checksum(byte[] b, int off, int len, long s1, long s2) {
		for (int i = off; i < off + len; i += 8) {
			long x0, x1;
			if (bigEndianChecksum) {
				x0 = u32(b, i);
				x1 = u32(b, i + 4);
			} else {
				x0 = (b[i] & 0xFFL) | (b[i + 1] & 0xFFL) << 8 | (b[i + 2] & 0xFFL) << 16 | (b[i + 3] & 0xFFL) << 24;
				x1 = (b[i + 4] & 0xFFL) | (b[i + 5] & 0xFFL) << 8 | (b[i + 6] & 0xFFL) << 16 | (b[i + 7] & 0xFFL) << 24;
			}
			s1 = (s1 + x0 + s2) & 0xFFFFFFFFL;
			s2 = (s2 + x1 + s1) & 0xFFFFFFFFL;
		}
		return new long[] { s1, s2 };
	}

	private void run(RandomAccessFile raf) throws IOException {
		byte[] h = new byte[32];
		if (fileSize < 32 || raf.read(h) < 32) {
			anomalies.add("the WAL file is shorter than its header (32 bytes)");
			return;
		}
		magic = u32(h, 0);
		if (magic != 0x377f0682L && magic != 0x377f0683L) {
			anomalies.add(String.format("invalid magic number 0x%08x", magic));
			return;
		}
		headerOk = true;
		bigEndianChecksum = (magic & 1) == 1;
		version = (int) u32(h, 4);
		pageSize = (int) u32(h, 8);
		checkpointSeq = u32(h, 12);
		salt1 = u32(h, 16);
		salt2 = u32(h, 20);
		hck1 = u32(h, 24);
		hck2 = u32(h, 28);
		if (version != 3007000)
			anomalies.add("unexpected WAL format version " + version);
		if (pageSize < 512 || pageSize > 65536 || Integer.bitCount(pageSize) != 1) {
			anomalies.add("invalid page size " + pageSize + " in the WAL header");
			return;
		}
		long[] ck = checksum(h, 0, 24, 0, 0);
		headerChecksumOk = ck[0] == hck1 && ck[1] == hck2;
		if (!headerChecksumOk)
			anomalies.add("header checksum does not match - SQLite ignores the whole WAL");
		long frameSize = 24L + pageSize;
		long n = (fileSize - 32) / frameSize;
		trailingBytes = (int) ((fileSize - 32) % frameSize);
		boolean chain = headerChecksumOk;
		long s1 = ck[0], s2 = ck[1];
		byte[] fr = new byte[(int) frameSize];
		int tx = 1;
		List<Frame> open = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			long off = 32 + i * frameSize;
			raf.seek(off);
			raf.readFully(fr);
			Frame f = new Frame();
			f.index = i + 1;
			f.offset = off;
			f.page = (int) u32(fr, 0);
			f.commitSize = (int) u32(fr, 4);
			f.salt1 = u32(fr, 8);
			f.salt2 = u32(fr, 12);
			f.ck1 = u32(fr, 16);
			f.ck2 = u32(fr, 20);
			f.saltsMatch = f.salt1 == salt1 && f.salt2 == salt2;
			if (chain && f.saltsMatch && f.page != 0) {
				long[] c = checksum(fr, 0, 8, s1, s2);
				c = checksum(fr, 24, pageSize, c[0], c[1]);
				f.checksumOk = c[0] == f.ck1 && c[1] == f.ck2;
				if (f.checksumOk) {
					s1 = c[0];
					s2 = c[1];
				} else
					chain = false;
			}
			if (!f.saltsMatch)
				f.state = State.STALE;
			else if (!f.checksumOk)
				f.state = State.INVALID;
			else {
				f.state = State.UNCOMMITTED;   // provisional
				f.tx = tx;
				open.add(f);
				if (f.commitSize != 0) {
					Set<Integer> pages = new LinkedHashSet<>();
					for (Frame o : open) {
						o.state = State.SUPERSEDED;  // decided below
						pages.add(o.page);
					}
					commits.add(new Commit(tx, open.get(0).index, f.index, f.commitSize, pages));
					lastValidCommitFrame = f.index;
					open.clear();
					tx++;
				}
			}
			frames.add(f);
		}
		for (Frame o : open)
			o.tx = 0;
		// current = latest committed frame of each page
		Map<Integer, Frame> latest = new HashMap<>();
		for (Frame f : frames)
			if (f.state == State.SUPERSEDED)
				latest.put(f.page, f);
		for (Frame f : latest.values())
			f.state = State.CURRENT;
		// Frames of a transaction that was never committed may fail the checksum: SQLite overwrites
		// frames of the open transaction in place and recomputes the checksums only at commit time.
		// If no frame with a failing checksum is a commit frame, they are uncommitted frames.
		boolean commitAmongInvalid = false;
		int inv = 0;
		for (Frame f : frames)
			if (f.state == State.INVALID) {
				inv++;
				commitAmongInvalid |= f.commitSize != 0;
			}
		if (inv > 0 && !commitAmongInvalid) {
			for (Frame f : frames)
				if (f.state == State.INVALID)
					f.state = State.UNCOMMITTED;
			uncommittedWithoutChecksum = inv;
			anomalies.removeIf(x -> x.contains("checksum chain"));
		}
		if (trailingBytes != 0)
			anomalies.add(trailingBytes + " bytes at the end of the WAL do not form a complete frame");
		int invalidAfter = 0;
		for (Frame f : frames)
			if (f.state == State.INVALID)
				invalidAfter++;
		if (invalidAfter > 0)
			anomalies.add(invalidAfter + " frame(s) with the current salts fail the checksum chain - SQLite ignores them "
					+ "(torn write, manipulation or frames following a damaged one)");
	}

	private void shm(File f) {
		try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
			byte[] b = new byte[136];
			raf.readFully(b);
			shmPresent = true;
			// WalIndexHdr in native byte order (little-endian on x86/ARM): mxFrame at 16
			shmMxFrame = le(b, 16);
			shmSalt1 = u32(b, 32);      // the salts are copied byte by byte from the WAL header
			shmSalt2 = u32(b, 36);
			shmBackfill = le(b, 96);    // WalCkptInfo.nBackfill
		} catch (IOException e) {
			shmPresent = false;
		}
	}

	private static long le(byte[] b, int p) {
		return (b[p] & 0xFFL) | (b[p + 1] & 0xFFL) << 8 | (b[p + 2] & 0xFFL) << 16 | (b[p + 3] & 0xFFL) << 24;
	}

	public Map<State, Integer> counts() {
		Map<State, Integer> m = new java.util.LinkedHashMap<>();
		for (State s : State.values())
			m.put(s, 0);
		for (Frame f : frames)
			m.merge(f.state, 1, Integer::sum);
		return m;
	}

	public Frame frame(int index) {
		return index >= 1 && index <= frames.size() ? frames.get(index - 1) : null;
	}

	// ------------------------------------------------------------------ time travel

	/** result of {@link #snapshot}: the database bytes and pages whose state is uncertain */
	public record Snapshot(byte[] data, int pagesFromWal, List<Integer> uncertain) {
	}

	/**
	 * The database as it was after commit {@code tx} (0 = database file without the WAL): the
	 * database file overlaid with the newest frame of every page up to that commit. Pages that
	 * were changed by a later frame already copied into the database file (checkpoint, see
	 * nBackfill in the -shm file) and that have no frame up to the commit are reported as
	 * uncertain - the database file may hold their newer state.
	 */
	public Snapshot snapshot(String dbPath, int tx) throws IOException {
		int lastFrame = 0, dbPages;
		File db = new File(dbPath);
		if (tx > 0) {
			Commit c = commits.get(tx - 1);
			lastFrame = c.lastFrame();
			dbPages = c.dbPages();
		} else
			dbPages = (int) (db.length() / pageSize);
		long size = (long) dbPages * pageSize;
		if (size > Integer.MAX_VALUE - 64)
			throw new IOException("database too large for a snapshot in memory");
		byte[] out = new byte[(int) size];
		try (RandomAccessFile raf = new RandomAccessFile(db, "r")) {
			int n = (int) Math.min(size, raf.length());
			raf.readFully(out, 0, n);
		}
		Map<Integer, Frame> newest = new HashMap<>();
		for (Frame f : frames)
			if (f.tx > 0 && f.index <= lastFrame && f.page <= dbPages)
				newest.put(f.page, f);
		try (RandomAccessFile raf = new RandomAccessFile(path, "r")) {
			for (Frame f : newest.values()) {
				raf.seek(f.offset + 24);
				raf.readFully(out, (f.page - 1) * pageSize, pageSize);
			}
		}
		List<Integer> uncertain = new ArrayList<>();
		long backfill = shmBackfill > 0 && shmSalt1 == salt1 ? shmBackfill : 0;
		if (backfill > lastFrame) {
			Set<Integer> later = new LinkedHashSet<>();
			for (Frame f : frames)
				if (f.tx > 0 && f.index > lastFrame && f.index <= backfill && !newest.containsKey(f.page) && f.page <= dbPages)
					later.add(f.page);
			uncertain.addAll(later);
		}
		return new Snapshot(out, newest.size(), uncertain);
	}
}
