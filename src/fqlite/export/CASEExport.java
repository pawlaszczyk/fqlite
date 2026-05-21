package fqlite.export;

import fqlite.base.Global;
import fqlite.base.Job;
import fqlite.log.AppLog;
import fqlite.types.ExportType;
import javafx.collections.ObservableList;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CASEExport {

    /**
     * Exports recovered SQLite records to a CASE (Cyber-investigation Analysis Standard Expression)
     * JSON-LD file. The method is analogous to exportToHtml() and uses the same data sources
     * (SQLITEDB, WALARCHIVE, ROLLBACKJOURNAL) and column-name resolution logic.
     *
     * CASE ontology references used:
     *   uco-core   https://ontology.unifiedcyberontology.org/uco/core/
     *   uco-observable https://ontology.unifiedcyberontology.org/uco/observable/
     *   uco-tool   https://ontology.unifiedcyberontology.org/uco/tool/
     *   case-investigation https://ontology.caseontology.org/case/investigation/
     *
     * @param dbname     display name of the database (used as label in the bundle)
     * @param outputPath full path of the target .jsonld file to create
     * @param xpath      export folder used for optional BLOB side-car files
     * @param exp        export source: SQLITEDB, WALARCHIVE, or ROLLBACKJOURNAL
     * @param isTable    if true, export only the single table given by {@code tname}
     * @param tname      name of the table to export when {@code isTable} is true
     * @throws java.io.IOException if the output file cannot be written
     */
    public static void exportToCase(Job job, String dbname, String outputPath, String xpath,
                             ExportType exp, boolean isTable, String tname) throws IOException {

        // ── 1. choose the correct result list (mirrors exportToHtml) ──────────────
        ConcurrentHashMap<String, ObservableList<ObservableList<String>>> exportlist = switch (exp) {
            case ROLLBACKJOURNAL -> job.rol.resultlist;
            case SQLITEDB        -> job.resultlist;
            case WALARCHIVE      -> job.wal.resultlist;
            default              -> null;
        };

        if (exportlist == null) {
            AppLog.debug("exportToCase: no result list available for ExportType " + exp);
            return;
        }

        // ── 2. collect file-system metadata (mirrors exportToHtml) ───────────────
        Path p = Paths.get(job.path);
        Map<String, Object> attributes = Files.readAttributes(p, "*", LinkOption.NOFOLLOW_LINKS);

        String sourcePath;
        String sourceLabel;
        if (exp == ExportType.SQLITEDB) {
            sourcePath  = job.path;
            sourceLabel = "database file";
        } else if (exp == ExportType.WALARCHIVE) {
            int id = job.walpath.lastIndexOf("/");
            sourcePath  = job.walpath.substring(0, id);
            sourceLabel = "write-ahead log";
        } else {
            int id = job.rollbackjournalpath.lastIndexOf("/");
            sourcePath  = job.rollbackjournalpath.substring(0, id);
            sourceLabel = "journal file";
        }

        String sha1   = new DigestUtils(org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_1  ).digestAsHex(new File(job.path));
        String sha256 = new DigestUtils(org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_256).digestAsHex(new File(job.path));
        String md5    = new DigestUtils(org.apache.commons.codec.digest.MessageDigestAlgorithms.MD5    ).digestAsHex(new File(job.path));

        String now = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

        // ── 3. build the JSON-LD document ─────────────────────────────────────────
        StringBuilder sb = new StringBuilder();

        sb.append("{\n");
        sb.append("  \"@context\": {\n");
        sb.append("    \"@vocab\": \"https://ontology.unifiedcyberontology.org/uco/core/\",\n");
        sb.append("    \"case-investigation\": \"https://ontology.caseontology.org/case/investigation/\",\n");
        sb.append("    \"uco-core\":       \"https://ontology.unifiedcyberontology.org/uco/core/\",\n");
        sb.append("    \"uco-observable\": \"https://ontology.unifiedcyberontology.org/uco/observable/\",\n");
        sb.append("    \"uco-tool\":       \"https://ontology.unifiedcyberontology.org/uco/tool/\",\n");
        sb.append("    \"xsd\":            \"http://www.w3.org/2001/XMLSchema#\"\n");
        sb.append("  },\n");
        sb.append("  \"@type\": \"uco-core:Bundle\",\n");
        sb.append("  \"@id\": \"kb:bundle-").append(java.util.UUID.randomUUID()).append("\",\n");
        sb.append("  \"uco-core:name\": ").append(jsonString("FQLite export – " + dbname)).append(",\n");
        sb.append("  \"uco-core:description\": ").append(jsonString("Recovered SQLite records exported by FQLite from " + sourceLabel)).append(",\n");
        sb.append("  \"uco-core:createdTime\": {\"@type\": \"xsd:dateTime\", \"@value\": ").append(jsonString(now)).append("},\n");

        // ── 3a. tool / investigative action ──────────────────────────────────────
        sb.append("  \"uco-core:object\": [\n");

        // Tool provenance
        sb.append("    {\n");
        sb.append("      \"@type\": \"uco-tool:Tool\",\n");
        sb.append("      \"@id\": \"kb:tool-fqlite\",\n");
        sb.append("      \"uco-core:name\": \"FQLite\",\n");
        sb.append("      \"uco-tool:toolVersion\": ").append(jsonString("v" + Global.FQLITE_VERSION)).append(",\n");
        sb.append("      \"uco-tool:creator\": ").append(jsonString(System.getProperty("user.name"))).append("\n");
        sb.append("    },\n");

        // Source file observable
        sb.append("    {\n");
        sb.append("      \"@type\": \"uco-observable:File\",\n");
        sb.append("      \"@id\": \"kb:file-source\",\n");
        sb.append("      \"uco-observable:fileName\": ").append(jsonString(job.filename)).append(",\n");
        sb.append("      \"uco-observable:filePath\": ").append(jsonString(sourcePath + " (" + sourceLabel + ")")).append(",\n");
        sb.append("      \"uco-observable:sizeInBytes\": ").append(attributes.get("size")).append(",\n");
        sb.append("      \"uco-observable:createdTime\":      {\"@type\": \"xsd:dateTime\", \"@value\": ").append(jsonString(attributes.get("creationTime").toString())).append("},\n");
        sb.append("      \"uco-observable:accessedTime\":     {\"@type\": \"xsd:dateTime\", \"@value\": ").append(jsonString(attributes.get("lastAccessTime").toString())).append("},\n");
        sb.append("      \"uco-observable:modifiedTime\":     {\"@type\": \"xsd:dateTime\", \"@value\": ").append(jsonString(attributes.get("lastModifiedTime").toString())).append("},\n");
        sb.append("      \"uco-observable:hash\": [\n");
        sb.append("        {\"@type\": \"uco-observable:Hash\", \"uco-observable:hashMethod\": \"MD5\",    \"uco-observable:hashValue\": ").append(jsonString(md5)).append("},\n");
        sb.append("        {\"@type\": \"uco-observable:Hash\", \"uco-observable:hashMethod\": \"SHA-1\",  \"uco-observable:hashValue\": ").append(jsonString(sha1)).append("},\n");
        sb.append("        {\"@type\": \"uco-observable:Hash\", \"uco-observable:hashMethod\": \"SHA-256\",\"uco-observable:hashValue\": ").append(jsonString(sha256)).append("}\n");
        sb.append("      ]\n");
        sb.append("    }");

        // ── 3b. one CyberItem per table row ──────────────────────────────────────
        Enumeration<String> tbls = exportlist.keys();
        while (tbls.hasMoreElements()) {
            String tblname = tbls.nextElement();

            // honour the isTable / tname filter (mirrors exportToHtml)
            if (isTable && !tname.equals(tblname)) continue;

            // skip index tables and internal sqlite tables (mirrors exportToHtml)
            String[] headers = job.getExentedColumnNamesForTable(tblname, exp);
            if (!tblname.equals("fqlite_freelist") && (headers == null)) continue;
            if (tblname.startsWith("sqlite_")) continue;

            ObservableList<ObservableList<String>> table = exportlist.get(tblname);
            if (table == null || table.isEmpty()) continue;

            int rowIndex = 0;
            for (ObservableList<String> row : table) {

                String offset = (row.size() > 5) ? row.get(5) : "unknown";

                sb.append(",\n");
                sb.append("    {\n");
                sb.append("      \"@type\": \"uco-observable:CyberItem\",\n");
                sb.append("      \"@id\": \"kb:row-").append(escapeCaseId(tblname)).append("-").append(rowIndex).append("\",\n");
                sb.append("      \"uco-core:tag\": ").append(jsonString(tblname)).append(",\n");
                sb.append("      \"case-investigation:exhibitNumber\": ").append(jsonString(offset)).append(",\n");
                sb.append("      \"uco-core:hasFacet\": [\n");
                sb.append("        {\n");
                sb.append("          \"@type\": \"uco-observable:ApplicationAccountFacet\",\n");
                sb.append("          \"uco-observable:application\": {\"@id\": \"kb:file-source\"},\n");
                sb.append("          \"uco-observable:properties\": {\n");

                // write all column / value pairs
                if (headers != null) {
                    int cl = 0;
                    for (int c = 0; c < headers.length && c < row.size(); c++) {
                        // column index 1 is skipped in exportToHtml (internal flag), keep parity
                        if (c == 1) continue;
                        String colName = headers[cl < headers.length ? cl : headers.length - 1];
                        String cellVal = row.get(c);

                        if (cellVal != null && cellVal.contains("[BLOB")) {
                            if (Global.EXPORT_MODE == Global.EXPORT_MODES.TOSEPARATEFILES) {
                                cellVal = job.exportBLOB(dbname, offset, cellVal, xpath);
                            } else {
                                cellVal = "[BLOB]";
                            }
                        }

                        if (cl > 0) sb.append(",\n");
                        sb.append("            ").append(jsonString(colName)).append(": ").append(jsonString(cellVal != null ? cellVal : ""));
                        cl++;
                    }
                }

                sb.append("\n          }\n");
                sb.append("        }\n");
                sb.append("      ]\n");
                sb.append("    }");

                rowIndex++;
            }
        }

        sb.append("\n  ]\n");
        sb.append("}\n");

        // ── 4. write to disk ──────────────────────────────────────────────────────
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
            writer.write(sb.toString());
        }
    }

// ── private helpers ────────────────────────────────────────────────────────────

    /**
     * Serialises a Java string as a JSON string literal, escaping all characters
     * that are special in JSON (backslash, double-quote, and the C0 control chars).
     *
     * @param value the raw string value; {@code null} is treated as empty string
     * @return a JSON string literal including the surrounding double-quotes
     */
    private static String jsonString(String value) {
        if (value == null) value = "";
        StringBuilder sb = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            switch (c) {
                case '"'  -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default   -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append("\"");
        return sb.toString();
    }

    /**
     * Sanitises a table name so that it can be used as the local part of a
     * CASE/UCO blank-node identifier (only letters, digits and hyphens).
     *
     * @param name the raw table name
     * @return a safe identifier fragment
     */
    private static String escapeCaseId(String name) {
        if (name == null) return "unknown";
        return name.replaceAll("[^A-Za-z0-9\\-]", "-");
    }

}
