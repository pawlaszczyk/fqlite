package fqlite.forensics;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import fqlite.base.Global;
import fqlite.log.AppLog;

/**
 * Evidence integrity: the database file and its companions (-wal, -journal) are hashed
 * (SHA-256) when a database is opened, verified again when it is closed, and every export gets
 * a manifest that names the evidence with its hashes and the exported files with theirs.
 * <p>
 * Hashing runs on a background thread; {@link #get(String)} waits for it.
 */
public final class Evidence {

	private Evidence() {
	}

	/** one evidence file as hashed at open time */
	public record EvidenceFile(String path, long size, long mtime, String sha256, Instant hashedAt) {
	}

	/** result of a verification */
	public record Check(String path, boolean exists, boolean sizeSame, boolean mtimeSame, boolean hashSame,
			String sha256Then, String sha256Now) {
		public boolean unchanged() {
			return exists && sizeSame && hashSame;
		}

		public String describe() {
			if (!exists)
				return path + ": MISSING";
			if (unchanged())
				return path + ": unchanged (size" + (mtimeSame ? ", mtime" : "") + " and SHA-256 " + sha256Now + ")"
						+ (mtimeSame ? "" : " - modification time changed");
			return path + ": CHANGED (SHA-256 at open " + sha256Then + ", now " + sha256Now + ")";
		}
	}

	private static final Map<String, CompletableFuture<List<EvidenceFile>>> REGISTRY = new ConcurrentHashMap<>();

	/** the database and its companion files that exist */
	public static List<File> filesOf(String dbPath) {
		List<File> l = new ArrayList<>();
		for (String suffix : new String[] { "", "-wal", "-journal", "-shm" }) {
			File f = new File(dbPath + suffix);
			if (f.isFile())
				l.add(f);
		}
		return l;
	}

	/** starts hashing the evidence of a database that is being opened */
	public static void register(String dbPath) {
		if (dbPath == null)
			return;
		String key = key(dbPath);
		REGISTRY.computeIfAbsent(key, k -> CompletableFuture.supplyAsync(() -> {
			List<EvidenceFile> l = new ArrayList<>();
			for (File f : filesOf(dbPath)) {
				try {
					l.add(new EvidenceFile(f.getAbsolutePath(), f.length(), f.lastModified(), sha256(f), Instant.now()));
				} catch (IOException e) {
					AppLog.error("Evidence: cannot hash " + f + ": " + e.getMessage());
				}
			}
			for (EvidenceFile e : l)
				AppLog.info("Evidence opened: " + e.path() + " size=" + e.size() + " SHA-256=" + e.sha256());
			return l;
		}));
	}

	/** the evidence files of a database as hashed at open time (waits for the hashing) */
	public static List<EvidenceFile> get(String dbPath) {
		CompletableFuture<List<EvidenceFile>> f = REGISTRY.get(key(dbPath));
		if (f == null) {
			register(dbPath);
			f = REGISTRY.get(key(dbPath));
		}
		try {
			return f.get();
		} catch (Exception e) {
			return List.of();
		}
	}

	public static boolean isRegistered(String dbPath) {
		return dbPath != null && REGISTRY.containsKey(key(dbPath));
	}

	/** verifies size, modification time and SHA-256 against the values at open time */
	public static List<Check> verify(String dbPath) {
		List<Check> l = new ArrayList<>();
		for (EvidenceFile e : get(dbPath)) {
			File f = new File(e.path());
			if (!f.isFile()) {
				l.add(new Check(e.path(), false, false, false, false, e.sha256(), null));
				continue;
			}
			String now;
			try {
				now = sha256(f);
			} catch (IOException ex) {
				now = "unreadable: " + ex.getMessage();
			}
			l.add(new Check(e.path(), true, f.length() == e.size(), f.lastModified() == e.mtime(),
					e.sha256().equals(now), e.sha256(), now));
		}
		for (Check c : l)
			AppLog.info("Evidence verified: " + c.describe());
		return l;
	}

	/** verifies on close and forgets the database */
	public static List<Check> close(String dbPath) {
		if (dbPath == null || !isRegistered(dbPath))
			return List.of();
		List<Check> l = verify(dbPath);
		REGISTRY.remove(key(dbPath));
		return l;
	}

	public static Collection<String> registered() {
		return new ArrayList<>(REGISTRY.keySet());
	}

	public static String sha256(File f) throws IOException {
		try (InputStream in = Files.newInputStream(f.toPath())) {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			byte[] buf = new byte[1 << 16];
			int n;
			while ((n = in.read(buf)) > 0)
				md.update(buf, 0, n);
			StringBuilder sb = new StringBuilder();
			for (byte b : md.digest())
				sb.append(String.format("%02x", b));
			return sb.toString();
		} catch (java.security.NoSuchAlgorithmException e) {
			throw new IOException(e);
		}
	}

	private static String key(String p) {
		try {
			return new File(p).getCanonicalPath();
		} catch (IOException e) {
			return new File(p).getAbsolutePath();
		}
	}

	// ------------------------------------------------------------------ exports

	/** files (and their modification times) in a directory, to find the files an export wrote */
	public static Map<File, Long> snapshot(File dir) {
		Map<File, Long> m = new HashMap<>();
		File[] fs = dir == null ? null : dir.listFiles();
		if (fs != null)
			for (File f : fs)
				m.put(f, f.isDirectory() ? -1L : f.lastModified());
		return m;
	}

	/** files written into dir (also inside new sub folders) since the snapshot */
	public static List<File> writtenSince(File dir, Map<File, Long> before) {
		List<File> l = new ArrayList<>();
		File[] fs = dir == null ? null : dir.listFiles();
		if (fs == null)
			return l;
		for (File f : fs) {
			if (f.getName().endsWith(".manifest.json"))
				continue;
			Long t = before.get(f);
			if (f.isDirectory()) {
				if (t == null)
					collect(f, l);
			} else if (t == null || t != f.lastModified())
				l.add(f);
		}
		l.sort(null);
		return l;
	}

	private static void collect(File d, List<File> l) {
		File[] fs = d.listFiles();
		if (fs == null)
			return;
		for (File f : fs)
			if (f.isDirectory())
				collect(f, l);
			else
				l.add(f);
	}

	/**
	 * Writes {@code <first output>.manifest.json} next to the export: tool and version, export
	 * time (UTC), what was exported, the evidence files with their SHA-256 at open time and a
	 * fresh verification, and every exported file with size and SHA-256.
	 *
	 * @return the manifest file, or null if nothing was exported
	 */
	public static File writeManifest(String kind, String scope, String dbPath, List<File> outputs) {
		return writeManifest(kind, scope, dbPath == null ? List.of() : List.of(dbPath), outputs);
	}

	/** manifest of an export that covers several databases (case search, timeline) */
	public static File writeManifest(String kind, String scope, List<String> dbPaths, List<File> outputs) {
		if (outputs == null || outputs.isEmpty())
			return null;
		File first = outputs.get(0);
		File manifest = new File(first.getParentFile(), first.getName() + ".manifest.json");
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("tool", "FQLite");
		m.put("version", Global.FQLITE_VERSION);
		m.put("java", System.getProperty("java.version"));
		m.put("exported_utc", Instant.now().toString());
		m.put("kind", kind);
		m.put("scope", scope);
		List<Map<String, Object>> ev = new ArrayList<>();
		for (String dbPath : dbPaths) {
			List<EvidenceFile> opened = get(dbPath);
			Map<String, Check> now = new HashMap<>();
			for (Check c : verify(dbPath))
				now.put(c.path(), c);
			for (EvidenceFile e : opened) {
				Map<String, Object> x = new LinkedHashMap<>();
				x.put("path", e.path());
				x.put("size", e.size());
				x.put("modified_utc", Instant.ofEpochMilli(e.mtime()).toString());
				x.put("sha256_at_open", e.sha256());
				Check c = now.get(e.path());
				x.put("unchanged_at_export", c != null && c.unchanged());
				ev.add(x);
			}
		}
		m.put("evidence", ev);
		List<Map<String, Object>> out = new ArrayList<>();
		for (File f : outputs) {
			Map<String, Object> x = new LinkedHashMap<>();
			x.put("path", f.getAbsolutePath());
			x.put("size", f.length());
			try {
				x.put("sha256", sha256(f));
			} catch (IOException e) {
				x.put("sha256", "unreadable: " + e.getMessage());
			}
			out.add(x);
		}
		m.put("files", out);
		try (Writer w = Files.newBufferedWriter(manifest.toPath(), StandardCharsets.UTF_8)) {
			w.write(Json.write(m, 0));
			w.write("\n");
		} catch (IOException e) {
			AppLog.error("Manifest could not be written: " + manifest + ": " + e.getMessage());
			return null;
		}
		AppLog.info("Export manifest written: " + manifest);
		return manifest;
	}

	/** minimal JSON writer for maps, lists, strings, numbers and booleans */
	public static final class Json {
		private Json() {
		}

		public static String write(Object o, int ind) {
			String pad = "  ".repeat(ind + 1), end = "  ".repeat(ind);
			if (o == null)
				return "null";
			if (o instanceof Map<?, ?> m) {
				if (m.isEmpty())
					return "{}";
				StringBuilder sb = new StringBuilder("{\n");
				int i = 0;
				for (Map.Entry<?, ?> e : m.entrySet()) {
					sb.append(pad).append(str(String.valueOf(e.getKey()))).append(": ").append(write(e.getValue(), ind + 1));
					sb.append(++i < m.size() ? ",\n" : "\n");
				}
				return sb.append(end).append('}').toString();
			}
			if (o instanceof Collection<?> c) {
				if (c.isEmpty())
					return "[]";
				StringBuilder sb = new StringBuilder("[\n");
				int i = 0;
				for (Object x : c) {
					sb.append(pad).append(write(x, ind + 1));
					sb.append(++i < c.size() ? ",\n" : "\n");
				}
				return sb.append(end).append(']').toString();
			}
			if (o instanceof Number || o instanceof Boolean)
				return String.valueOf(o);
			return str(String.valueOf(o));
		}

		public static String str(String s) {
			StringBuilder sb = new StringBuilder("\"");
			for (char ch : s.toCharArray()) {
				switch (ch) {
				case '"' -> sb.append("\\\"");
				case '\\' -> sb.append("\\\\");
				case '\n' -> sb.append("\\n");
				case '\r' -> sb.append("\\r");
				case '\t' -> sb.append("\\t");
				default -> {
					if (ch < 0x20)
						sb.append(String.format("\\u%04x", (int) ch));
					else
						sb.append(ch);
				}
				}
			}
			return sb.append('"').toString();
		}
	}
}
