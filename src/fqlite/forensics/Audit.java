package fqlite.forensics;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Audit report of a database and its -wal / -journal files: header consistency (page count,
 * reserved bytes, change counter), structure (freelist, unreferenced pages, b-tree anomalies),
 * traces of secure_delete (free space wiped with zeros), the state of the WAL frames
 * (checksums, salts, commits, checkpoints) and of the rollback journal. Nothing is written.
 */
public final class Audit {

	private Audit() {
	}

	public enum Level {
		OK("OK"), INFO("info"), NOTE("note"), WARN("warning");

		public final String label;

		Level(String l) {
			label = l;
		}
	}

	public record Check(String area, String item, String value, Level level, String note) {
	}

	public record Report(String path, PageMap pages, WalAudit wal, List<Check> checks) {
	}

	public static Report run(String dbPath) {
		List<Check> c = new ArrayList<>();
		PageMap pm = null;
		try {
			pm = PageMap.analyse(dbPath);
			database(pm, c);
		} catch (IOException e) {
			c.add(new Check("database", "file", dbPath, Level.WARN, "could not be read: " + e.getMessage()));
		}
		WalAudit wal = null;
		File w = new File(dbPath + "-wal");
		if (w.isFile()) {
			try {
				wal = WalAudit.analyse(w.getPath());
				wal(wal, pm, c);
			} catch (IOException e) {
				c.add(new Check("WAL", "file", w.getName(), Level.WARN, "could not be read: " + e.getMessage()));
			}
		} else
			c.add(new Check("WAL", "file", "not present", Level.INFO, ""));
		File j = new File(dbPath + "-journal");
		if (j.isFile())
			journal(j, pm, c);
		else
			c.add(new Check("journal", "file", "not present", Level.INFO, ""));
		return new Report(dbPath, pm, wal, c);
	}

	private static String n(long v) {
		return String.format("%,d", v);
	}

	// ------------------------------------------------------------------ database file

	private static void database(PageMap p, List<Check> c) {
		String A = "database";
		c.add(new Check(A, "header string", p.headerOk ? "SQLite format 3" : "missing", p.headerOk ? Level.OK : Level.WARN,
				p.headerOk ? "" : "not a regular SQLite file (encrypted or damaged?)"));
		c.add(new Check(A, "SQLite version (last write)", version(p.sqliteVersion), Level.INFO, ""));
		c.add(new Check(A, "page size", n(p.pageSize), Level.INFO, ""));
		c.add(new Check(A, "reserved bytes per page", String.valueOf(p.reserved), p.reserved == 0 ? Level.OK : Level.NOTE,
				p.reserved == 0 ? "" : "space at the end of every page used by an extension, e.g. encryption (SEE, SQLCipher HMAC) "
						+ "or the checksum VFS; its content is not part of the records"));
		long rest = p.fileSize % p.pageSize;
		c.add(new Check(A, "file size", n(p.fileSize) + " bytes = " + n(p.fileSize / p.pageSize) + " page(s)"
				+ (rest != 0 ? " + " + rest + " bytes" : ""), rest == 0 ? Level.OK : Level.WARN,
				rest == 0 ? "" : "the file size is not a multiple of the page size (truncated copy or appended data)"));
		boolean valid = p.headerCountValid();
		long filePages = p.fileSize / p.pageSize;
		if (!valid)
			c.add(new Check(A, "page count in header", n(p.headerPageCount), Level.NOTE,
					"not valid (change counter " + p.changeCounter + " \u2260 version-valid-for " + p.versionValidFor
							+ "): the file was last written by a very old SQLite version or the header was changed"));
		else if (p.headerPageCount == filePages)
			c.add(new Check(A, "page count in header", n(p.headerPageCount), Level.OK, "matches the file size"));
		else if (p.headerPageCount < filePages)
			c.add(new Check(A, "page count in header", n(p.headerPageCount) + " (file: " + n(filePages) + ")", Level.NOTE,
					(filePages - p.headerPageCount) + " page(s) behind the database end stated in the header - leftovers of a "
							+ "larger database (not truncated), appended data or a manipulated header; examined as unreferenced pages"));
		else
			c.add(new Check(A, "page count in header", n(p.headerPageCount) + " (file: " + n(filePages) + ")", Level.WARN,
					"the header states more pages than the file has: the file is truncated (incomplete copy?) - unless the "
							+ "missing pages are in the WAL"));
		c.add(new Check(A, "file change counter", n(p.changeCounter), Level.INFO, "number of transactions in rollback-journal mode"));
		String jm = p.writeVersion == 2 && p.readVersion == 2 ? "WAL" : p.writeVersion == 1 && p.readVersion == 1 ? "rollback journal (legacy)"
				: p.writeVersion + "/" + p.readVersion;
		c.add(new Check(A, "journal mode (header)", jm, p.writeVersion >= 1 && p.writeVersion <= 2 ? Level.INFO : Level.WARN, ""));
		c.add(new Check(A, "text encoding", switch (p.textEncoding) {
		case 1 -> "UTF-8";
		case 2 -> "UTF-16le";
		case 3 -> "UTF-16be";
		case 0 -> "not set yet";
		default -> "invalid (" + p.textEncoding + ")";
		}, p.textEncoding >= 0 && p.textEncoding <= 3 ? Level.INFO : Level.WARN,
				p.textEncoding == 0 ? "no schema has been written into the database file yet (e.g. everything is still in the WAL)" : ""));
		c.add(new Check(A, "auto-vacuum", p.largestRoot == 0 ? "off" : p.incrementalVacuum != 0 ? "incremental" : "full",
				p.largestRoot == 0 ? Level.INFO : Level.NOTE, p.largestRoot == 0 ? ""
						: "freed pages are moved to the end and cut off on commit: deleted content survives mainly in freeblocks"));
		if (p.applicationId != 0 || p.userVersion != 0)
			c.add(new Check(A, "application id / user version", String.format("0x%08x / %d", p.applicationId, p.userVersion), Level.INFO,
					"set by the application (may identify it)"));
		// structure
		Map<PageMap.Kind, Integer> cnt = p.counts();
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<PageMap.Kind, Integer> e : cnt.entrySet())
			if (e.getValue() > 0)
				sb.append(sb.length() > 0 ? ", " : "").append(e.getValue()).append(" ").append(e.getKey().label);
		c.add(new Check(A, "pages by use", sb.toString(), Level.INFO, p.roots.size() + " tables and indexes in the schema"));
		int fl = cnt.get(PageMap.Kind.FREELIST_TRUNK) + cnt.get(PageMap.Kind.FREELIST_LEAF);
		c.add(new Check(A, "freelist", n(p.freelistCount) + " page(s) in the header, " + n(p.freelistPagesWalked) + " found",
				p.freelistCount == p.freelistPagesWalked ? Level.OK : Level.WARN,
				p.freelistCount == p.freelistPagesWalked ? "" : "the freelist does not match the header (damaged or manipulated)"));
		int unref = cnt.get(PageMap.Kind.UNREFERENCED);
		if (unref > 0) {
			StringBuilder l = new StringBuilder();
			int k = 0, beyond = 0;
			for (int pg = 1; pg <= p.filePages; pg++)
				if (p.kind[pg] == PageMap.Kind.UNREFERENCED) {
					if (valid && pg > p.headerPageCount)
						beyond++;
					if (k++ < 25)
						l.append(k > 1 ? ", " : "").append(pg);
				}
			if (k > 25)
				l.append(", \u2026");
			c.add(new Check(A, "unreferenced pages", n(unref) + ": " + l, Level.NOTE,
					"pages that belong to no b-tree, no overflow chain and not to the freelist"
							+ (beyond > 0 ? " (" + beyond + " of them behind the database end in the header)" : "")
							+ " - remains of dropped tables, of a damaged structure or of manipulated pointers; their records are "
							+ "classified as \"unreferenced page\""));
		} else
			c.add(new Check(A, "unreferenced pages", "none", Level.OK, ""));
		// secure_delete
		int zf = 0;
		for (int pg = 1; pg <= p.filePages; pg++)
			if ((p.kind[pg] == PageMap.Kind.FREELIST_LEAF) && p.zeroed.get(pg))
				zf++;
		int leaves = cnt.get(PageMap.Kind.FREELIST_LEAF);
		if (leaves > 0)
			c.add(new Check(A, "freelist pages wiped with zeros", n(zf) + " of " + n(leaves), zf == leaves ? Level.NOTE : Level.INFO,
					zf == leaves ? "all freed pages are empty - typical for secure_delete" : (leaves - zf) + " freed page(s) still hold data"));
		if (p.freeblocksTotal > 0)
			c.add(new Check(A, "freeblocks wiped with zeros", n(p.freeblocksZeroed) + " of " + n(p.freeblocksTotal),
					p.freeblocksZeroed == p.freeblocksTotal ? Level.NOTE : Level.INFO,
					p.freeblocksZeroed == p.freeblocksTotal ? "the space of deleted cells was overwritten - typical for secure_delete"
							: (p.freeblocksTotal - p.freeblocksZeroed) + " freeblock(s) still hold the remains of deleted cells"));
		if (p.unallocatedBytes > 0)
			c.add(new Check(A, "unallocated area of b-tree pages", n(p.unallocatedBytes) + " bytes, " + n(p.unallocatedNonZero)
					+ " non-zero", Level.INFO, p.unallocatedNonZero == 0 ? "completely zero" : "may hold remains of earlier cells"));
		String sd;
		Level sl = Level.INFO;
		boolean anyFree = leaves > 0 || p.freeblocksTotal > 0;
		boolean allZero = (leaves == 0 || zf == leaves) && (p.freeblocksTotal == 0 || p.freeblocksZeroed == p.freeblocksTotal);
		if (!anyFree)
			sd = "cannot be determined (no freed space)";
		else if (allZero && p.freeblocksTotal + leaves >= 2) {
			sd = "probably ON";
			sl = Level.NOTE;
		} else if (p.freeblocksZeroed > 0 || zf > 0)
			sd = "mixed (partly wiped - secure_delete=FAST, a changed setting or reused space)";
		else
			sd = "probably OFF";
		c.add(new Check(A, "secure_delete (assessment)", sd, sl, "derived from the freeblocks and freed pages; with secure_delete deleted "
				+ "content is overwritten and can only be found in the WAL, the journal or unaffected copies"));
		// group similar findings (same text apart from numbers)
		Map<String, List<String>> groups = new java.util.LinkedHashMap<>();
		for (String a : p.anomalies)
			groups.computeIfAbsent(a.replaceAll("\\d+", "#"), k -> new ArrayList<>()).add(a);
		for (List<String> g : groups.values()) {
			for (int i = 0; i < Math.min(3, g.size()); i++)
				c.add(new Check(A, "structure", g.get(i), Level.WARN, "possible damage or manipulation"));
			if (g.size() > 3)
				c.add(new Check(A, "structure", "\u2026 and " + (g.size() - 3) + " more of this kind", Level.WARN, ""));
		}
	}

	static String version(int v) {
		if (v <= 0)
			return "unknown";
		return (v / 1000000) + "." + (v / 1000 % 1000) + "." + (v % 1000);
	}

	// ------------------------------------------------------------------ WAL

	private static void wal(WalAudit w, PageMap p, List<Check> c) {
		String A = "WAL";
		c.add(new Check(A, "file", new File(w.path).getName() + ", " + n(w.fileSize) + " bytes", Level.INFO, ""));
		if (!w.headerOk) {
			for (String a : w.anomalies)
				c.add(new Check(A, "header", a, Level.WARN, ""));
			return;
		}
		c.add(new Check(A, "header", "format " + w.version + ", page size " + w.pageSize + ", checksums " + (w.bigEndianChecksum ? "big" : "little")
				+ "-endian", p != null && p.pageSize != w.pageSize ? Level.WARN : Level.OK,
				p != null && p.pageSize != w.pageSize ? "page size differs from the database (" + p.pageSize + ") - foreign WAL?" : ""));
		c.add(new Check(A, "header checksum", w.headerChecksumOk ? "valid" : "invalid", w.headerChecksumOk ? Level.OK : Level.WARN,
				w.headerChecksumOk ? "" : "SQLite ignores the whole WAL; FQLite still shows its frames"));
		c.add(new Check(A, "checkpoint sequence / salts", w.checkpointSeq + " / " + w.salt1 + ", " + w.salt2, Level.INFO,
				"the salts change with every reset of the WAL (WAL generation)"));
		Map<WalAudit.State, Integer> cnt = w.counts();
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<WalAudit.State, Integer> e : cnt.entrySet())
			if (e.getValue() > 0)
				sb.append(sb.length() > 0 ? ", " : "").append(e.getValue()).append(" ").append(e.getKey().label);
		c.add(new Check(A, "frames", n(w.frames.size()) + (sb.length() > 0 ? ": " + sb : ""), Level.INFO, ""));
		c.add(new Check(A, "committed transactions", n(w.commits.size()) + (w.lastValidCommitFrame > 0 ? ", last commit in frame "
				+ w.lastValidCommitFrame : ""), Level.INFO, "each can be opened as the database state after that commit (time travel)"));
		if (cnt.get(WalAudit.State.SUPERSEDED) > 0)
			c.add(new Check(A, "superseded frames", n(cnt.get(WalAudit.State.SUPERSEDED)), Level.NOTE,
					"earlier versions of pages that were changed again later - older row versions"));
		if (cnt.get(WalAudit.State.UNCOMMITTED) > 0)
			c.add(new Check(A, "uncommitted frames", n(cnt.get(WalAudit.State.UNCOMMITTED)), Level.NOTE,
					"changes of a transaction that was never committed (aborted, crashed or still running) - SQLite ignores them"
							+ (w.uncommittedWithoutChecksum > 0 ? "; " + w.uncommittedWithoutChecksum + " of them without final checksum "
									+ "(SQLite computes it only at commit)" : "")));
		if (cnt.get(WalAudit.State.STALE) > 0)
			c.add(new Check(A, "stale frames", n(cnt.get(WalAudit.State.STALE)), Level.NOTE,
					"frames of an earlier WAL generation behind the current ones; they hold page versions that were already "
							+ "checkpointed and may have changed since"));
		if (cnt.get(WalAudit.State.INVALID) > 0)
			c.add(new Check(A, "frames with invalid checksum", n(cnt.get(WalAudit.State.INVALID)), Level.WARN,
					"the checksum chain breaks - torn write, damage or manipulation; SQLite ignores these frames and all following"));
		if (w.trailingBytes > 0)
			c.add(new Check(A, "incomplete frame at the end", w.trailingBytes + " bytes", Level.NOTE, "torn write or truncated copy"));
		if (p != null) {
			int beyond = 0;
			for (WalAudit.Frame f : w.frames)
				if (f.page > p.filePages)
					beyond++;
			if (beyond > 0)
				c.add(new Check(A, "frames for pages beyond the database file", n(beyond), Level.INFO,
						"the database grew in the WAL; these pages exist only there"));
		}
		if (w.shmPresent) {
			boolean same = w.shmSalt1 == w.salt1 && w.shmSalt2 == w.salt2;
			c.add(new Check(A, "-shm: last valid frame (mxFrame)", n(w.shmMxFrame) + " (computed: " + w.lastValidCommitFrame + ")",
					!same ? Level.INFO : w.shmMxFrame == w.lastValidCommitFrame ? Level.OK : Level.NOTE,
					!same ? "the -shm belongs to another WAL generation" : w.shmMxFrame == w.lastValidCommitFrame ? ""
							: "the shared-memory index differs from the WAL (stale or changed -shm)"));
			if (same && w.shmBackfill > 0)
				c.add(new Check(A, "-shm: frames already checkpointed (nBackfill)", n(w.shmBackfill), Level.INFO,
						"the database file already contains the pages of these frames; the state before them is no longer in the "
								+ "database file"));
		} else
			c.add(new Check(A, "-shm", "not present", Level.INFO, "without -shm the checkpoint progress is unknown"));
		for (String a : w.anomalies)
			if (!a.contains("checksum chain") && !a.contains("do not form a complete frame") && !a.contains("header checksum"))
				c.add(new Check(A, "anomaly", a, Level.WARN, ""));
	}

	// ------------------------------------------------------------------ rollback journal

	private static final byte[] JMAGIC = { (byte) 0xd9, (byte) 0xd5, 0x05, (byte) 0xf9, 0x20, (byte) 0xa1, 0x63, (byte) 0xd7 };

	private static void journal(File j, PageMap p, List<Check> c) {
		String A = "journal";
		c.add(new Check(A, "file", j.getName() + ", " + n(j.length()) + " bytes", Level.INFO, ""));
		if (j.length() == 0) {
			c.add(new Check(A, "content", "empty", Level.INFO, "journal_mode=TRUNCATE or no transaction"));
			return;
		}
		try (RandomAccessFile raf = new RandomAccessFile(j, "r")) {
			byte[] h = new byte[28];
			raf.readFully(h, 0, (int) Math.min(28, j.length()));
			boolean magic = true, zero = true;
			for (int i = 0; i < 8; i++) {
				magic &= h[i] == JMAGIC[i];
				zero &= h[i] == 0;
			}
			int count = PageMap.u32(h, 8), nonce = PageMap.u32(h, 12), init = PageMap.u32(h, 16), sector = PageMap.u32(h, 20), ps = PageMap.u32(h, 24);
			if (magic)
				c.add(new Check(A, "header", "valid (hot journal)", Level.NOTE,
						"the last transaction was not completed: SQLite would roll it back when the database is opened next - the "
								+ "journal holds the state before that transaction"));
			else if (zero)
				c.add(new Check(A, "header", "zeroed", Level.INFO,
						"committed transaction in journal_mode=PERSIST: the page images of the last transaction remain behind the header"));
			else
				c.add(new Check(A, "header", "unknown", Level.WARN, "no journal magic - damaged or not a rollback journal"));
			boolean guessed = false;
			if (!magic && zero && p != null && (sector == 0 || ps == 0)) {
				// PERSIST zeroes the whole header: assume the database page size and the usual sector sizes
				ps = p.pageSize;
				sector = 0;
				for (int s : new int[] { 512, 4096, 1024, 2048, 8192, 16384, 32768, 65536 })
					if (s + 4 <= j.length()) {
						byte[] b4 = new byte[4];
						raf.seek(s);
						raf.readFully(b4);
						int pg = PageMap.u32(b4, 0);
						if (pg >= 1 && pg <= Math.max(p.filePages, p.headerPageCount)) {
							sector = s;
							break;
						}
					}
				guessed = sector > 0;
			}
			if (sector < 512 || sector > 65536 || ps < 512 || ps > 65536) {
				c.add(new Check(A, "sector / page size", sector + " / " + ps, Level.INFO, "not readable from the header"));
				return;
			}
			c.add(new Check(A, "sector / page size", sector + " / " + ps, p != null && p.pageSize != ps ? Level.WARN : Level.OK,
					p != null && p.pageSize != ps ? "page size differs from the database" : ""));
			if (magic)
				c.add(new Check(A, "pages in the transaction / database size before", (count == -1 ? "until end of file" : n(count)) + " / "
						+ n(init) + " pages", Level.INFO, ""));
			// page records with valid checksum
			long pos = sector, valid = 0, total = 0;
			byte[] rec = new byte[ps + 8];
			while (pos + rec.length <= j.length() && total < 1_000_000) {
				raf.seek(pos);
				raf.readFully(rec);
				int pg = PageMap.u32(rec, 0);
				if (pg == 0)
					break;
				total++;
				int sum = nonce;
				for (int i = ps - 200; i > 0; i -= 200)
					sum += rec[4 + i] & 0xFF;
				if (!guessed && sum == PageMap.u32(rec, 4 + ps))
					valid++;
				if (guessed && (pg < 1 || pg > Math.max(p.filePages, p.headerPageCount))) {
					total--;
					break;
				}
				pos += rec.length;
			}
			if (guessed)
				c.add(new Check(A, "page images", n(total), Level.INFO,
						"header zeroed: checksums cannot be verified (nonce lost); sector size " + sector + " assumed"));
			else
				c.add(new Check(A, "page images", n(total) + ", " + n(valid) + " with valid checksum",
						total == valid ? Level.INFO : Level.NOTE, total == valid ? ""
								: "page images with a wrong checksum belong to an earlier transaction or are damaged"));
		} catch (IOException e) {
			c.add(new Check(A, "file", j.getName(), Level.WARN, "could not be read: " + e.getMessage()));
		}
	}
}
