package fqlite.headless;

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;

/**
 * Command line of the headless JAR.
 *
 * <pre>
 * java -jar fqlite-headless.jar [options] &lt;database&gt;
 *   --json &lt;file|-&gt;   write the result as JSON (- = stdout; default if nothing else is chosen)
 *   --csv &lt;dir&gt;       one CSV file per table into &lt;dir&gt;
 *   --summary         records per table and status on stderr
 *   --no-wal          ignore &lt;db&gt;-wal
 *   --no-journal      ignore &lt;db&gt;-journal
 *   --threads &lt;n&gt;     worker threads (default 1)
 * </pre>
 * Exit codes: 0 ok, 1 analysis failed, 2 wrong arguments.
 */
public final class Main {

	private Main() {
	}

	public static void main(String[] args) {
		System.exit(run(args));
	}

	static int run(String[] args) {
		String json = null, csv = null, db = null;
		boolean summary = false;
		Options o = Options.defaults();
		try {
			for (int i = 0; i < args.length; i++) {
				switch (args[i]) {
					case "--json" -> json = args[++i];
					case "--csv" -> csv = args[++i];
					case "--summary" -> summary = true;
					case "--no-wal" -> o.wal(false);
					case "--no-journal" -> o.journal(false);
					case "--threads" -> o.threads(Integer.parseInt(args[++i]));
					case "--version" -> {
						System.out.println("FQLite headless " + FQLiteHeadless.version());
						return 0;
					}
					case "-h", "--help" -> {
						usage();
						return 0;
					}
					default -> {
						if (args[i].startsWith("--") || db != null) {
							System.err.println("unknown argument: " + args[i]);
							usage();
							return 2;
						}
						db = args[i];
					}
				}
			}
		} catch (ArrayIndexOutOfBoundsException | IllegalArgumentException e) {
			System.err.println("missing or wrong value: " + e.getMessage());
			usage();
			return 2;
		}
		if (db == null) {
			usage();
			return 2;
		}
		if (json == null && csv == null && !summary)
			json = "-";
		try {
			RecoveryResult r = FQLiteHeadless.analyze(Path.of(db), o);
			if (json != null) {
				if (json.equals("-")) {
					Writer w = new OutputStreamWriter(System.out, StandardCharsets.UTF_8);
					r.writeJson(w);
					w.flush();
				} else
					r.writeJson(Path.of(json));
			}
			if (csv != null)
				r.writeCsv(Path.of(csv));
			if (summary) {
				System.err.println(r.getRecords().size() + " records in " + r.getDurationMillis() + " ms ("
						+ r.getDatabase() + (r.isWalAnalysed() ? ", WAL" : "") + (r.isJournalAnalysed() ? ", journal" : "") + ")");
				for (Map.Entry<String, Map<Status, Integer>> e : r.summary().entrySet())
					System.err.println("  " + e.getKey() + ": " + e.getValue());
			}
			return 0;
		} catch (Exception e) {
			System.err.println("ERROR: " + e.getMessage());
			return 1;
		}
	}

	private static void usage() {
		System.err.println("""
				FQLite headless %s - forensic SQLite recovery without GUI
				usage: java -jar fqlite-headless.jar [options] <database>
				  --json <file|->   result as JSON (- = stdout; default)
				  --csv <dir>       one CSV file per table into <dir>
				  --summary         records per table and status on stderr
				  --no-wal          ignore <db>-wal
				  --no-journal      ignore <db>-journal
				  --threads <n>     worker threads (default 1)
				  --version""".formatted(FQLiteHeadless.version()));
	}
}
