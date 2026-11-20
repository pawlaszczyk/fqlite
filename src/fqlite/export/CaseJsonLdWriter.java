package fqlite.export;

import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Writes a CASE 1.5 / UCO 1.4 JSON-LD graph for recovered SQLite records.
 * <p>
 * The writer streams: every node is written as soon as it is complete, only the
 * identifiers of the record nodes are kept for the investigative action at the end.
 * It depends on java.base only, so it can be tested without the GUI.
 * <p>
 * Graph layout:
 * <pre>
 *   InvestigativeAction --instrument--> AnalyticTool
 *                       --performer---> Identity
 *                       --object------> File (database / WAL / journal, with FileFacet + ContentDataFacet)
 *                       --result------> ObservableObject (one per recovered record)
 *
 *   ObservableObject (record)  state = live | deleted | freelist | old version, RecoveredObjectFacet if not live
 *   TableField (one per value) TableFieldFacet: tableName, recordRowID, recordFieldName, recordFieldValue
 *   TableField --Contained_Within--> record --Contained_Within (DataRangeFacet: offset)--> File
 * </pre>
 * Validated with case_validate (case-utils, CASE 1.5.0).
 */
public final class CaseJsonLdWriter implements Closeable {

    public static final String KB = "http://example.org/kb/";

    /** Recovery status of a record, mapped to observable:state and the RecoveredObjectFacet. */
    public enum Status {
        LIVE("live"),
        DELETED("deleted"),
        FREELIST("deleted (freelist page)"),
        OLD_VERSION("old version (UPDATE)");

        final String label;

        Status(String label) {
            this.label = label;
        }
    }

    /** One column value of a record. */
    public record Field(String name, String value, String sqlType) {
    }

    /** One recovered record. */
    public record Record(String table, String tableSchema, String rowid, Status status, boolean partial,
                         Long offset, Long size, String note, String sourceFileId, List<Field> fields) {
    }

    private final Writer out;
    private final List<String> results = new ArrayList<>();
    private boolean first = true;

    public CaseJsonLdWriter(Writer out) {
        this.out = out;
    }

    /** Writes the JSON-LD context and opens the graph. */
    public void begin() throws IOException {
        out.write("{\n");
        out.write("  \"@context\": {\n");
        out.write("    \"kb\": \"" + KB + "\",\n");
        out.write("    \"case-investigation\": \"https://ontology.caseontology.org/case/investigation/\",\n");
        out.write("    \"uco-action\": \"https://ontology.unifiedcyberontology.org/uco/action/\",\n");
        out.write("    \"uco-core\": \"https://ontology.unifiedcyberontology.org/uco/core/\",\n");
        out.write("    \"uco-identity\": \"https://ontology.unifiedcyberontology.org/uco/identity/\",\n");
        out.write("    \"uco-observable\": \"https://ontology.unifiedcyberontology.org/uco/observable/\",\n");
        out.write("    \"uco-tool\": \"https://ontology.unifiedcyberontology.org/uco/tool/\",\n");
        out.write("    \"uco-types\": \"https://ontology.unifiedcyberontology.org/uco/types/\",\n");
        out.write("    \"xsd\": \"http://www.w3.org/2001/XMLSchema#\"\n");
        out.write("  },\n");
        out.write("  \"@graph\": [");
    }

    /** The analysing tool. Returns its id. */
    public String tool(String name, String version, String toolType, String reference) throws IOException {
        String id = id("AnalyticTool");
        Obj o = new Obj(id, "uco-tool:AnalyticTool")
                .str("uco-core:name", name)
                .str("uco-tool:version", version)
                .str("uco-tool:toolType", toolType);
        if (reference != null)
            o.typed("uco-tool:references", reference, "xsd:anyURI");
        node(o);
        return id;
    }

    /** The examiner. Returns the id. */
    public String identity(String name) throws IOException {
        String id = id("Identity");
        node(new Obj(id, "uco-identity:Identity").str("uco-core:name", name));
        return id;
    }

    /**
     * A source file with its file system metadata and hashes. Times are ISO-8601 with offset
     * (e.g. {@code FileTime.toString()}); null values are left out.
     */
    public String file(String fileName, String filePath, Long size, String created, String modified,
                       String accessed, String md5, String sha1, String sha256) throws IOException {
        String id = id("File");
        Obj fileFacet = new Obj(id("FileFacet"), "uco-observable:FileFacet")
                .str("uco-observable:fileName", fileName)
                .str("uco-observable:filePath", filePath)
                .typed("uco-observable:sizeInBytes", size == null ? null : size.toString(), "xsd:integer")
                .typed("uco-observable:observableCreatedTime", created, "xsd:dateTime")
                .typed("uco-observable:modifiedTime", modified, "xsd:dateTime")
                .typed("uco-observable:accessedTime", accessed, "xsd:dateTime")
                .bool("uco-observable:isDirectory", false);
        List<Obj> hashes = new ArrayList<>();
        addHash(hashes, "MD5", md5);
        addHash(hashes, "SHA1", sha1);
        addHash(hashes, "SHA256", sha256);
        Obj content = new Obj(id("ContentDataFacet"), "uco-observable:ContentDataFacet")
                .typed("uco-observable:sizeInBytes", size == null ? null : size.toString(), "xsd:integer")
                .objs("uco-observable:hash", hashes);
        node(new Obj(id, "uco-observable:File").objs("uco-core:hasFacet", List.of(fileFacet, content)));
        return id;
    }

    /** One recovered record with its values. Returns the id of the record node. */
    public String record(Record r) throws IOException {
        String recId = id("ObservableObject");
        StringBuilder name = new StringBuilder(r.table());
        if (r.rowid() != null && !r.rowid().isBlank())
            name.append(", rowid ").append(r.rowid().trim());
        if (r.offset() != null)
            name.append(", offset ").append(r.offset());

        Obj rec = new Obj(recId, "uco-observable:ObservableObject")
                .str("uco-core:name", name.toString())
                .str("uco-core:tag", r.table())
                .str("uco-observable:state", r.status().label)
                .str("uco-core:description", blankToNull(r.note()));
        if (r.status() != Status.LIVE) {
            rec.objs("uco-core:hasFacet", List.of(new Obj(id("RecoveredObjectFacet"), "uco-observable:RecoveredObjectFacet")
                    .str("uco-observable:contentRecoveredStatus", r.partial() ? "partially recovered" : "recovered")));
        }
        node(rec);
        results.add(recId);

        // record -> source file, with the position inside the file
        if (r.sourceFileId() != null) {
            Obj rel = relationship(recId, r.sourceFileId());
            if (r.offset() != null) {
                Obj range = new Obj(id("DataRangeFacet"), "uco-observable:DataRangeFacet")
                        .typed("uco-observable:rangeOffset", r.offset().toString(), "xsd:integer")
                        .typed("uco-observable:rangeSize", r.size() == null ? null : r.size().toString(), "xsd:integer");
                rel.objs("uco-core:hasFacet", List.of(range));
            }
            node(rel);
        }

        // one TableField per value
        for (Field f : r.fields()) {
            String fieldId = id("TableField");
            boolean isNull = f.value() == null || "NULL".equals(f.value());
            Obj facet = new Obj(id("TableFieldFacet"), "uco-observable:TableFieldFacet")
                    .str("uco-observable:tableName", r.table())
                    .str("uco-observable:tableSchema", blankToNull(r.tableSchema()))
                    .str("uco-observable:recordFieldName", f.name())
                    .bool("uco-observable:recordFieldIsNull", isNull);
            String rowid = r.rowid() == null ? null : r.rowid().trim();
            if (rowid != null && !rowid.isEmpty())
                facet.typed("uco-observable:recordRowID", rowid, isInteger(rowid) ? "xsd:integer" : "xsd:string");
            if (!isNull)
                value(facet, f);
            node(new Obj(fieldId, "uco-observable:TableField").objs("uco-core:hasFacet", List.of(facet)));
            node(relationship(fieldId, recId));
        }
        return recId;
    }

    /** The investigative action that connects tool, examiner, sources and all records written so far. */
    public void action(String name, String description, String toolId, String performerId, List<String> objectIds,
                       String startTime, String endTime) throws IOException {
        Obj a = new Obj(id("InvestigativeAction"), "case-investigation:InvestigativeAction")
                .str("uco-core:name", name)
                .str("uco-core:description", description)
                .typed("uco-action:startTime", startTime, "xsd:dateTime")
                .typed("uco-action:endTime", endTime, "xsd:dateTime")
                .ref("uco-action:instrument", toolId)
                .ref("uco-action:performer", performerId)
                .refs("uco-action:object", objectIds)
                .refs("uco-action:result", results);
        node(a);
    }

    /** Closes the graph and the document (does not close the underlying writer). */
    public void end() throws IOException {
        out.write("\n  ]\n}\n");
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    private Obj relationship(String source, String target) {
        return new Obj(id("Relationship"), "uco-observable:ObservableRelationship")
                .ref("uco-core:source", source)
                .ref("uco-core:target", target)
                .str("uco-core:kindOfRelationship", "Contained_Within")
                .bool("uco-core:isDirectional", true);
    }

    private void addHash(List<Obj> hashes, String method, String value) {
        if (value == null || value.isEmpty())
            return;
        hashes.add(new Obj(id("Hash"), "uco-types:Hash")
                .str("uco-types:hashMethod", method)
                .typed("uco-types:hashValue", value.toUpperCase(), "xsd:hexBinary"));
    }

    /** value typed by the declared column affinity (SQLite rules), text otherwise */
    private static void value(Obj facet, Field f) {
        String v = f.value();
        String t = f.sqlType() == null ? "" : f.sqlType().toUpperCase();
        if (t.contains("INT") && isInteger(v.trim())) {
            facet.typed("uco-observable:recordFieldValue", v.trim(), "xsd:integer");
            return;
        }
        if ((t.contains("REAL") || t.contains("FLOA") || t.contains("DOUB") || t.contains("NUMERIC") || t.contains("DECIMAL"))) {
            try {
                facet.typed("uco-observable:recordFieldValue", new BigDecimal(v.trim()).toPlainString(), "xsd:decimal");
                return;
            } catch (NumberFormatException ignored) {
                // stored as text despite the affinity
            }
        }
        facet.typed("uco-observable:recordFieldValue", v, "xsd:string");
    }

    private static boolean isInteger(String s) {
        if (s == null || s.isEmpty())
            return false;
        int i = (s.charAt(0) == '-' || s.charAt(0) == '+') ? 1 : 0;
        if (i == s.length())
            return false;
        for (; i < s.length(); i++)
            if (s.charAt(i) < '0' || s.charAt(i) > '9')
                return false;
        return true;
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s;
    }

    private static String id(String type) {
        return "kb:" + type + "-" + UUID.randomUUID();
    }

    private void node(Obj o) throws IOException {
        out.write(first ? "\n    " : ",\n    ");
        first = false;
        out.write(o.toJson());
    }

    /** minimal JSON object builder; properties with null values are omitted */
    private static final class Obj {
        private final StringBuilder sb = new StringBuilder();

        Obj(String id, String type) {
            sb.append("{\"@id\": ").append(quote(id)).append(", \"@type\": ").append(quote(type));
        }

        Obj str(String key, String value) {
            if (value != null)
                sb.append(", ").append(quote(key)).append(": ").append(quote(value));
            return this;
        }

        Obj typed(String key, String value, String datatype) {
            if (value != null)
                sb.append(", ").append(quote(key)).append(": {\"@type\": ").append(quote(datatype))
                        .append(", \"@value\": ").append(quote(value)).append("}");
            return this;
        }

        Obj bool(String key, boolean value) {
            sb.append(", ").append(quote(key)).append(": ").append(value);
            return this;
        }

        Obj ref(String key, String id) {
            if (id != null)
                sb.append(", ").append(quote(key)).append(": {\"@id\": ").append(quote(id)).append("}");
            return this;
        }

        Obj refs(String key, List<String> ids) {
            if (ids == null || ids.isEmpty())
                return this;
            sb.append(", ").append(quote(key)).append(": [");
            for (int i = 0; i < ids.size(); i++)
                sb.append(i == 0 ? "" : ", ").append("{\"@id\": ").append(quote(ids.get(i))).append("}");
            sb.append("]");
            return this;
        }

        Obj objs(String key, List<Obj> objs) {
            if (objs == null || objs.isEmpty())
                return this;
            sb.append(", ").append(quote(key)).append(": [");
            for (int i = 0; i < objs.size(); i++)
                sb.append(i == 0 ? "" : ", ").append(objs.get(i).toJson());
            sb.append("]");
            return this;
        }

        String toJson() {
            return sb + "}";
        }
    }

    /** JSON string literal; escapes quotes, backslashes, control characters and unpaired surrogates */
    static String quote(String value) {
        StringBuilder sb = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20 || (Character.isSurrogate(c) && !validSurrogate(value, i)))
                        sb.append(String.format("\\u%04x", (int) c));
                    else
                        sb.append(c);
                }
            }
        }
        return sb.append('"').toString();
    }

    private static boolean validSurrogate(String s, int i) {
        char c = s.charAt(i);
        if (Character.isHighSurrogate(c))
            return i + 1 < s.length() && Character.isLowSurrogate(s.charAt(i + 1));
        return i > 0 && Character.isHighSurrogate(s.charAt(i - 1));
    }
}
