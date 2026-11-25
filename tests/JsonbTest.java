import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;

import fqlite.blob.DecodeException;
import fqlite.blob.Jsonb;
import fqlite.headless.FQLiteHeadless;
import fqlite.headless.RecoveredRecord;
import fqlite.headless.RecoveryResult;

/**
 * Checks the JSONB decoder (SQLite binary JSON, 3.45+) and its use in the recovery engine,
 * and that 64-bit rowids are read completely. No GUI. Run with ./gradlew jsonbTest.
 * <p>
 * The vectors were produced by SQLite 3.53 itself: hex(jsonb(x)) and json(jsonb(x)) for random
 * JSON, JSON5 input (INT5, FLOAT5, TEXT5) and the jsonb_set/insert/patch/remove functions. The
 * decoder must return exactly what SQLite's json() returns.
 */
public class JsonbTest {

	static final String[][] VECTORS = {
		{ "1B0B", "[[]]" },
		{ "C8205C225C7530303031C3A45C74F09F98805C225C6E5C75303030315C22612FC3A4", "\"\\\"\\u0001ä\\t😀\\\"\\n\\u0001\\\"a/ä\"" },
		{ "01", "true" },
		{ "CB17C3132D3433353434323032343631353836373130340100", "[-435442024615867104,true,null]" },
		{ "CC31486B305C22C820615C75303065345C75303065345C745C75303065345C7530306534625C5C5C74886B315C75303066630C", "{\"k0\\\"\":\"a\\u00e4\\u00e4\\t\\u00e4\\u00e4b\\\\\\t\",\"k1\\u00fc\":{}}" },
		{ "0B", "[]" },
		{ "0B", "[]" },
		{ "0B", "[]" },
		{ "C3122D3738393434363332373938343435333835", "-78944632798445385" },
		{ "C312323831303833373338323139323131383833", "281083738219211883" },
		{ "01", "true" },
		{ "0B", "[]" },
		{ "01", "true" },
		{ "C3132D373533363437313332333531393432393635", "-753647132351942965" },
		{ "00", "null" },
		{ "0C", "{}" },
		{ "C515332E343234383432313134343330323437652B3235", "3.424842114430247e+25" },
		{ "01", "true" },
		{ "02", "false" },
		{ "0C", "{}" },
		{ "9C376B65794430783146", "{\"key\":31}" },
		{ "CC0C6773696E676C6549615C2762", "{\"single\":\"a'b\"}" },
		{ "CC191764262E35176526352E1766462E3565331767542D30786666", "{\"d\":0.5,\"e\":5.0,\"f\":0.5e3,\"g\":-255}" },
		{ "4B13311332", "[1,2]" },
		{ "4C17781335", "{\"x\":5}" },
		{ "CC0D1779A96C696E655C0A636F6E74", "{\"y\":\"linecont\"}" },
		{ "BC177A895C7834315C765C30", "{\"z\":\"\\u0041\\u000b\\u0000\"}" },
		{ "CC1437657363C80E615C75303065345C6E5C22715C22", "{\"esc\":\"a\\u00e4\\n\\\"q\\\"\"}" },
		{ "3B285C2F", "[\"\\/\"]" },
		{ "AC176113311A623A6E6575", "{\"a\":1,\"b\":\"neu\"}" },
		{ "7B13311332233939", "[1,2,99]" },
		{ "9B1778133135322E3500", "[\"x\",1,2.5,null]" },
		{ "5C176B2B1331", "{\"k\":[1]}" },
		{ "8C1761133117631333", "{\"a\":1,\"c\":3}" },
		{ "4B13311333", "[1,3]" },
		{ "6C17613C176E0B", "{\"a\":{\"n\":[]}}" },
	};

	static int failures = 0;

	static void check(boolean ok, String what) {
		if (!ok) {
			failures++;
			System.out.println("FAIL: " + what);
		}
	}

	public static void main(String[] args) throws Exception {
		HexFormat hex = HexFormat.of();

		// 1. decoder against SQLite's own json()
		for (String[] v : VECTORS) {
			byte[] b = hex.parseHex(v[0]);
			String got;
			try {
				got = Jsonb.toJson(b);
			} catch (DecodeException e) {
				got = "DecodeException: " + e.getMessage();
			}
			check(got.equals(v[1]), "toJson(" + v[0] + ") = " + got + ", expected " + v[1]);
			int t = Jsonb.rootType(b);
			boolean container = (t == Jsonb.ARRAY || t == Jsonb.OBJECT) && b.length > 1;
			check(Jsonb.isJsonb(b) == container, "isJsonb(" + v[0] + ")");
		}

		// 2. damaged / foreign data is rejected, never throws anything else
		byte[] ok = hex.parseHex("4C17611331"); // {"a":1}
		check(!Jsonb.isJsonb(java.util.Arrays.copyOf(ok, ok.length - 1)), "truncated JSONB accepted");
		check(!Jsonb.isJsonb(java.util.Arrays.copyOf(ok, ok.length + 1)), "JSONB with trailing byte accepted");
		check(!Jsonb.isJsonb(hex.parseHex("4C13311331")), "object with INT key accepted");
		check(!Jsonb.isJsonb(hex.parseHex("2B1D")), "reserved element type accepted");
		check(!Jsonb.isJsonb(hex.parseHex("89504E470D0A1A0A0000000D")), "PNG accepted");
		check(!Jsonb.isJsonb("{\"a\":1}".getBytes()), "JSON text accepted");
		Random rnd = new Random(42);
		int fp = 0;
		for (int i = 0; i < 200_000; i++) {
			byte[] r = new byte[2 + rnd.nextInt(200)];
			rnd.nextBytes(r);
			try {
				if (Jsonb.isJsonb(r))
					fp++;
				Jsonb.toJson(r);
			} catch (DecodeException e) {
				// expected
			} catch (RuntimeException | StackOverflowError e) {
				check(false, "unexpected " + e + " for " + hex.formatHex(r));
				break;
			}
		}
		System.out.println("random BLOBs taken for JSONB: " + fp + " of 200000");
		check(fp < 20, "too many random BLOBs taken for JSONB: " + fp);

		// 3. end to end: JSONB BLOBs and 64-bit rowids in a database recovered by FQLite
		Path dir = Files.createTempDirectory("fqlite-jsonb");
		Path db = dir.resolve("jsonb.db");
		long[] rowids = { 5_000_000_000L, -7L, Long.MAX_VALUE, 1730000000123L };
		try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db); Statement s = c.createStatement()) {
			s.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, doc BLOB, note TEXT)");
			try (PreparedStatement p = c.prepareStatement("INSERT INTO t VALUES(?,?,?)")) {
				for (int i = 0; i < VECTORS.length; i++) {
					p.setLong(1, i < rowids.length ? rowids[i] : i + 1);
					p.setBytes(2, hex.parseHex(VECTORS[i][0]));
					p.setString(3, String.valueOf(i)); // vector index (several vectors share a JSON text)
					p.executeUpdate();
				}
			}
		}
		RecoveryResult r = FQLiteHeadless.analyze(db);
		List<RecoveredRecord> recs = r.getRecords("t");
		check(recs.size() == VECTORS.length, "records: " + recs.size() + " of " + VECTORS.length);
		int seen = 0;
		for (RecoveredRecord rec : recs) {
			int i;
			try {
				i = Integer.parseInt(rec.getValues().get(2));
			} catch (NumberFormatException e) {
				check(false, "unexpected record " + rec);
				continue;
			}
			seen++;
			byte[] b = hex.parseHex(VECTORS[i][0]);
			String cell = rec.getValues().get(1);
			String json = r.getJson(rec, 1);
			if (Jsonb.isJsonb(b)) {
				check(VECTORS[i][1].equals(json), "getJson row " + i + ": " + json);
				String preview = VECTORS[i][1].length() > 200 ? VECTORS[i][1].substring(0, 200) + "\u2026" : VECTORS[i][1];
				check(cell.equals("[BLOB-0] JSONB " + preview), "cell row " + i + ": " + cell);
			} else {
				check(json == null, "getJson of a non-container row " + i);
			}
			check(java.util.Arrays.equals(b, r.getBlob(rec, 1)), "raw BLOB bytes changed, row " + i);
			long expected = i < rowids.length ? rowids[i] : i + 1;
			check(String.valueOf(expected).equals(rec.getRowId()), "rowid " + rec.getRowId() + ", expected " + expected);
		}
		check(seen == VECTORS.length, "matched records: " + seen);
		Files.deleteIfExists(db);
		Files.deleteIfExists(dir);

		System.out.println(failures == 0 ? "JsonbTest: all checks passed (" + VECTORS.length + " vectors)" : "JsonbTest: " + failures + " failure(s)");
		System.exit(failures == 0 ? 0 : 1);
	}
}
