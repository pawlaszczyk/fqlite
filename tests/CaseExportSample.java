// Writes a small CASE/UCO JSON-LD sample with the same writer as the CASE export,
// without GUI or database. Validate the output with: case_validate <file>
// Run with ./gradlew caseExportSample
import fqlite.export.CaseJsonLdWriter;
import fqlite.export.CaseJsonLdWriter.Field;
import fqlite.export.CaseJsonLdWriter.Record;
import fqlite.export.CaseJsonLdWriter.Status;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class CaseExportSample {

    public static void main(String[] args) throws Exception {
        String target = args.length > 0 ? args[0] : "case-sample.jsonld";
        String schema = "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, score REAL, note TEXT)";
        try (CaseJsonLdWriter w = new CaseJsonLdWriter(new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(target), StandardCharsets.UTF_8)))) {
            w.begin();
            String tool = w.tool("FQLite", "5.21", "SQLite forensic recovery", "https://github.com/pawlaszczyk/fqlite");
            String examiner = w.identity("examiner");
            String db = w.file("test.db", "/evidence/test.db", 8192L, "2026-11-19T09:00:00Z",
                    "2026-11-19T09:05:00.123456Z", "2026-11-19T09:06:00Z",
                    "d41d8cd98f00b204e9800998ecf8427e", "da39a3ee5e6b4b0d3255bfef95601890afd80709",
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
            String wal = w.file("test.db-wal", "/evidence/test.db-wal", 4152L, null, null, null,
                    null, null, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
            w.record(new Record("users", schema, "1", Status.LIVE, false, 4021L, 37L, null, db, List.of(
                    new Field("id", "1", "INTEGER"), new Field("name", "Alice \"Al\" Müller", "TEXT"),
                    new Field("score", "3.25", "REAL"), new Field("note", "NULL", "TEXT"))));
            w.record(new Record("users", schema, "2", Status.DELETED, false, 3980L, 30L, null, db, List.of(
                    new Field("id", "2", "INTEGER"), new Field("name", "Bob\u0000Hidden", "TEXT"),
                    new Field("score", "abc", "REAL"), new Field("note", "😀 emoji", "TEXT"))));
            w.record(new Record("users", schema, "", Status.FREELIST, true, 12288L, null,
                    "⚠ truncated", db, List.of(new Field("name", "Car", "TEXT"))));
            w.record(new Record("users", schema, "1", Status.OLD_VERSION, false, 120L, null,
                    "↻ updated in WAL frame 3 (commit=true, dbpage=2, walframe=3)", wal, List.of(
                    new Field("id", "1", "INTEGER"), new Field("name", "Alice", "TEXT"))));
            w.action("SQLite record recovery", "Recovery of live and deleted records with FQLite 5.21",
                    tool, examiner, List.of(db, wal), "2026-11-19T10:00:00+01:00", "2026-11-19T10:00:02+01:00");
            w.end();
        }
        System.out.println("written: " + target);
    }
}
