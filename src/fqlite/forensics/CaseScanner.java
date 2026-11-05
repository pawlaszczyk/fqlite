package fqlite.forensics;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Finds the SQLite databases of a folder (an extraction): every file that starts with the
 * SQLite header, optionally in sub folders, filtered by file name patterns and size. Only the
 * first 16 bytes of a candidate are read; companion files (-wal, -journal, -shm) are not listed
 * but noted with their database.
 */
public final class CaseScanner {

	private CaseScanner() {
	}

	public record Found(File file, String relativePath, long size, boolean wal, boolean journal) {
	}

	/** scan conditions */
	public static final class Conditions {
		public boolean recursive = true;
		/** comma separated file name patterns with * and ?; empty = all files */
		public String namePatterns = "";
		/** comma separated patterns that exclude a file */
		public String excludePatterns = "";
		public long minSize = 512;
		public long maxSize = Long.MAX_VALUE;
	}

	public record Progress(long filesSeen, long candidates, String current) {
	}

	private static final byte[] MAGIC = "SQLite format 3\0".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

	public static List<Found> scan(File root, Conditions c, Consumer<Found> onFound, Consumer<Progress> onProgress,
			BooleanSupplier stop) throws IOException {
		List<Found> out = new ArrayList<>();
		List<Pattern> include = patterns(c.namePatterns), exclude = patterns(c.excludePatterns);
		long[] seen = { 0, 0 };
		Path base = root.toPath();
		Files.walkFileTree(base, EnumSet.noneOf(FileVisitOption.class), c.recursive ? Integer.MAX_VALUE : 1, new SimpleFileVisitor<>() {
			@Override
			public FileVisitResult visitFile(Path p, BasicFileAttributes a) {
				if (stop != null && stop.getAsBoolean())
					return FileVisitResult.TERMINATE;
				seen[0]++;
				if (onProgress != null && seen[0] % 200 == 0)
					onProgress.accept(new Progress(seen[0], seen[1], p.toString()));
				if (!a.isRegularFile())
					return FileVisitResult.CONTINUE;
				String name = p.getFileName().toString();
				String low = name.toLowerCase(Locale.ROOT);
				if (low.endsWith("-wal") || low.endsWith("-journal") || low.endsWith("-shm"))
					return FileVisitResult.CONTINUE;
				long size = a.size();
				if (size < c.minSize || size > c.maxSize)
					return FileVisitResult.CONTINUE;
				if (!include.isEmpty() && include.stream().noneMatch(x -> x.matcher(name).matches()))
					return FileVisitResult.CONTINUE;
				if (exclude.stream().anyMatch(x -> x.matcher(name).matches()))
					return FileVisitResult.CONTINUE;
				seen[1]++;
				if (!isSqlite(p.toFile()))
					return FileVisitResult.CONTINUE;
				File f = p.toFile();
				Found found = new Found(f, base.relativize(p).toString(), size, new File(f.getPath() + "-wal").isFile(),
						new File(f.getPath() + "-journal").length() > 0);
				out.add(found);
				if (onFound != null)
					onFound.accept(found);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path p, IOException e) {
				return FileVisitResult.CONTINUE;       // unreadable folders are skipped
			}
		});
		if (onProgress != null)
			onProgress.accept(new Progress(seen[0], seen[1], null));
		return out;
	}

	public static boolean isSqlite(File f) {
		try (InputStream in = Files.newInputStream(f.toPath())) {
			byte[] h = in.readNBytes(16);
			if (h.length < 16)
				return false;
			for (int i = 0; i < 16; i++)
				if (h[i] != MAGIC[i])
					return false;
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	static List<Pattern> patterns(String s) {
		List<Pattern> l = new ArrayList<>();
		if (s == null)
			return l;
		for (String p : s.split(",")) {
			p = p.trim();
			if (p.isEmpty())
				continue;
			StringBuilder rx = new StringBuilder();
			for (char ch : p.toCharArray())
				switch (ch) {
				case '*' -> rx.append(".*");
				case '?' -> rx.append('.');
				default -> rx.append(Pattern.quote(String.valueOf(ch)));
				}
			l.add(Pattern.compile(rx.toString(), Pattern.CASE_INSENSITIVE));
		}
		return l;
	}
}
