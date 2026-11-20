package fqlite.export;

import fqlite.base.Global;
import fqlite.base.Job;
import fqlite.descriptor.TableDescriptor;
import fqlite.export.CaseJsonLdWriter.Field;
import fqlite.export.CaseJsonLdWriter.Record;
import fqlite.export.CaseJsonLdWriter.Status;
import fqlite.log.AppLog;
import fqlite.types.ExportType;
import fqlite.util.Auxiliary;
import javafx.collections.ObservableList;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Export of recovered SQLite records as CASE (Cyber-investigation Analysis Standard Expression)
 * JSON-LD, CASE 1.5 / UCO 1.4. The graph itself is written by {@link CaseJsonLdWriter}; this class
 * maps the result lists of a {@link Job} onto it.
 * <p>
 * Every recovered record becomes an observable:ObservableObject with its recovery state
 * (live, deleted, deleted on a freelist page, old version of an UPDATE) and, if it is not live,
 * a RecoveredObjectFacet. Every value becomes an observable:TableField (TableFieldFacet with
 * table name, rowid, column name and value). The position of the record in the source file is
 * kept as DataRangeFacet on the relationship record → file. A case-investigation:InvestigativeAction
 * links tool, examiner, source files and results.
 * <p>
 * The output validates with case_validate (case-utils) against CASE 1.5.0.
 */
public class CASEExport {

    private static final int WAL_COLUMNS = 5; // commit, dbpage, walframe, salt1, salt2

    /**
     * Exports recovered SQLite records to a CASE JSON-LD file.
     *
     * @param job        the analysed database
     * @param dbname     display name of the database
     * @param outputPath full path of the target .jsonld file to create
     * @param xpath      export folder used for optional BLOB side-car files
     * @param exp        export source: SQLITEDB, WALARCHIVE, or ROLLBACKJOURNAL
     * @param isTable    if true, export only the single table given by {@code tname}
     * @param tname      name of the table to export when {@code isTable} is true
     * @throws IOException if a source file cannot be read or the output file cannot be written
     */
    public static void exportToCase(Job job, String dbname, String outputPath, String xpath,
                                    ExportType exp, boolean isTable, String tname) throws IOException {

        // ── 1. choose the result list (mirrors exportToHtml) ─────────────────────
        ConcurrentHashMap<String, ObservableList<ObservableList<String>>> exportlist = switch (exp) {
            case ROLLBACKJOURNAL -> job.rol == null ? null : job.rol.resultlist;
            case SQLITEDB -> job.resultlist;
            case WALARCHIVE -> job.wal == null ? null : job.wal.resultlist;
            default -> null;
        };
        if (exportlist == null) {
            AppLog.debug("exportToCase: no result list available for ExportType " + exp);
            return;
        }

        OffsetDateTime exportStart = OffsetDateTime.now();
        int walExtra = exp == ExportType.WALARCHIVE ? WAL_COLUMNS : 0;
        String sourceLabel = switch (exp) {
            case WALARCHIVE -> "write-ahead log";
            case ROLLBACKJOURNAL -> "rollback journal";
            default -> "database";
        };

        try (CaseJsonLdWriter w = new CaseJsonLdWriter(new BufferedWriter(new OutputStreamWriter(
                Files.newOutputStream(Paths.get(outputPath)), StandardCharsets.UTF_8)))) {

            w.begin();

            // ── 2. provenance: tool, examiner, source files ─────────────────────────
            String toolId = w.tool("FQLite", Global.FQLITE_VERSION, "SQLite forensic recovery",
                    "https://github.com/pawlaszczyk/fqlite");
            String examinerId = w.identity(System.getProperty("user.name"));

            List<String> sources = new ArrayList<>();
            String dbFileId = fileNode(w, job.path);
            sources.add(dbFileId);
            String recordSourceId = dbFileId;
            if (exp == ExportType.WALARCHIVE && job.walpath != null) {
                recordSourceId = fileNode(w, job.walpath);
                sources.add(recordSourceId);
            } else if (exp == ExportType.ROLLBACKJOURNAL && job.rollbackjournalpath != null) {
                recordSourceId = fileNode(w, job.rollbackjournalpath);
                sources.add(recordSourceId);
            }

            // ── 3. one record node per row, one TableField per value ────────────────
            int records = 0;
            Enumeration<String> tbls = exportlist.keys();
            while (tbls.hasMoreElements()) {
                String tblname = tbls.nextElement();
                if (isTable && !tname.equals(tblname))
                    continue;
                if (tblname.startsWith("sqlite_"))
                    continue;

                TableDescriptor td = descriptor(job, tblname);
                // index tables have no TableDescriptor (mirrors exportToHtml)
                if (td == null && !tblname.equals("fqlite_freelist"))
                    continue;

                ObservableList<ObservableList<String>> table = exportlist.get(tblname);
                if (table == null || table.isEmpty())
                    continue;

                for (ObservableList<String> row : table) {
                    Record r = toRecord(job, td, tblname, row, walExtra, dbname, xpath, recordSourceId);
                    if (r != null) {
                        w.record(r);
                        records++;
                    }
                }
            }

            // ── 4. the investigative action ─────────────────────────────────────────
            OffsetDateTime start = job.analysisStart != null ? job.analysisStart : exportStart;
            OffsetDateTime end = job.analysisEnd != null ? job.analysisEnd : OffsetDateTime.now();
            w.action("SQLite record recovery",
                    "Recovery of live and deleted records from the " + sourceLabel + " of " + dbname
                            + " with FQLite " + Global.FQLITE_VERSION + " (" + records + " records exported "
                            + OffsetDateTime.now() + ")",
                    toolId, examinerId, sources, start.toString(), end.toString());
            w.end();
        }
    }

    // ── row → record ───────────────────────────────────────────────────────────────

    /**
     * Row layout: [line number (GUI only)], table, PLL/flags, rowid, status, offset,
     * [commit, dbpage, walframe, salt1, salt2 (WAL only)], values...
     */
    private static Record toRecord(Job job, TableDescriptor td, String tblname, List<String> row, int walExtra,
                                   String dbname, String xpath, String sourceId) {
        int base = rowBase(job, tblname, row);
        int dataStart = base + 5 + walExtra;
        if (row.size() < base + 5)
            return null;

        String pll = cell(row, base + 1);
        String rowid = cell(row, base + 2);
        String status = cell(row, base + 3);
        String offset = cell(row, base + 4);

        Status st;
        if (Global.FREELIST_ENTRY.equals(status))
            st = Status.FREELIST;
        else if (Global.DELETED_RECORD_IN_PAGE.equals(status))
            st = Status.DELETED;
        else
            st = Status.LIVE;
        if (pll != null) {
            if (pll.contains(Auxiliary.FLAG_UPDATED) || pll.contains(Auxiliary.FLAG_RJ_SUPERSEDED)
                    || pll.contains(Auxiliary.FLAG_WAL_SUPERSEDED))
                st = Status.OLD_VERSION;
            else if (pll.contains(Auxiliary.FLAG_WAL_DELETED) || pll.contains(Auxiliary.FLAG_RJ_DELETED))
                st = Status.DELETED;
        }
        boolean partial = pll != null
                && (pll.contains(Auxiliary.FLAG_TRUNCATED) || pll.contains(Auxiliary.FLAG_OVERFLOW_OVERWRITTEN));

        StringBuilder note = new StringBuilder();
        if (pll != null && !pll.isBlank())
            note.append("PLL/flags: ").append(pll.trim());
        if (walExtra > 0 && row.size() >= dataStart) {
            String[] names = {"commit", "dbpage", "walframe", "salt1", "salt2"};
            note.append(note.isEmpty() ? "" : "; ").append("WAL");
            for (int i = 0; i < WAL_COLUMNS; i++)
                note.append(i == 0 ? " " : ", ").append(names[i]).append('=').append(cell(row, base + 5 + i));
        }

        List<String> names = td != null ? td.columnnames : List.of();
        List<String> types = td != null ? td.sqltypes : List.of();
        List<Field> fields = new ArrayList<>();
        for (int c = dataStart; c < row.size(); c++) {
            int col = c - dataStart;
            String name = col < names.size() ? names.get(col) : "column" + (col + 1);
            String type = types != null && col < types.size() ? types.get(col) : null;
            String value = row.get(c);
            if (value != null && value.contains("[BLOB")) {
                value = Global.EXPORT_MODE == Global.EXPORT_MODES.TOSEPARATEFILES
                        ? job.exportBLOB(dbname, offset, value, xpath)
                        : "[BLOB]";
                type = null;
            }
            fields.add(new Field(name, value, type));
        }

        return new Record(tblname, td != null ? td.sql : null, rowid, st, partial, parseLong(offset), null,
                note.toString(), sourceId, fields);
    }

    /** 1 if the row starts with the line number of the GUI table, 0 otherwise */
    private static int rowBase(Job job, String tblname, List<String> row) {
        if (row.size() > 1 && tblname.equals(row.get(1)))
            return 1;
        if (!row.isEmpty() && tblname.equals(row.get(0)))
            return 0;
        return job.gui != null ? 1 : 0;
    }

    private static TableDescriptor descriptor(Job job, String tblname) {
        for (TableDescriptor td : job.headers)
            if (tblname.equals(td.tblname))
                return td;
        return null;
    }

    private static String cell(List<String> row, int i) {
        return i < row.size() ? row.get(i) : null;
    }

    /** leading decimal number of a cell, e.g. "4096" or "4096 (page 2)" */
    private static Long parseLong(String s) {
        if (s == null)
            return null;
        s = s.trim();
        int i = 0;
        while (i < s.length() && Character.isDigit(s.charAt(i)))
            i++;
        if (i == 0 || i > 18)
            return null;
        return Long.parseLong(s.substring(0, i));
    }

    // ── source files ───────────────────────────────────────────────────────────────

    /** file node with metadata and MD5/SHA-1/SHA-256 computed in one pass; name and path only if unreadable */
    private static String fileNode(CaseJsonLdWriter w, String path) throws IOException {
        Path p = Paths.get(path);
        String name = p.getFileName() == null ? path : p.getFileName().toString();
        try {
            BasicFileAttributes a = Files.readAttributes(p, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            String[] h = hashes(p);
            return w.file(name, p.toAbsolutePath().toString(), a.size(),
                    a.creationTime().toString(), a.lastModifiedTime().toString(), a.lastAccessTime().toString(),
                    h[0], h[1], h[2]);
        } catch (IOException e) {
            AppLog.debug("exportToCase: cannot read " + path + ": " + e);
            return w.file(name, p.toAbsolutePath().toString(), null, null, null, null, null, null, null);
        }
    }

    private static String[] hashes(Path p) throws IOException {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            byte[] buf = new byte[1 << 20];
            try (InputStream in = Files.newInputStream(p)) {
                int n;
                while ((n = in.read(buf)) > 0) {
                    md5.update(buf, 0, n);
                    sha1.update(buf, 0, n);
                    sha256.update(buf, 0, n);
                }
            }
            HexFormat hex = HexFormat.of();
            return new String[]{hex.formatHex(md5.digest()), hex.formatHex(sha1.digest()), hex.formatHex(sha256.digest())};
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
    }
}
