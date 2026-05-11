package fqlite.viewer.parser;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Generic tree node used by BSON, MessagePack, Thrift Binary/Compact
 * and FlatBuffers parsers.
 *
 * Every node has:
 *   – a {@link Format} tag (which parser produced it)
 *   – a {@link Kind}   tag (DOCUMENT/MAP, ARRAY, SCALAR, …)
 *   – an optional field name / key
 *   – an optional human-readable type name (e.g. "Int32", "Float64", "ObjectId")
 *   – an optional scalar value string
 *   – ordered children (for containers)
 *
 * This design avoids duplicating tree / display logic for five formats.
 */
public class BinaryNode {

    // ── Owning format ────────────────────────────────────────────────────
    public enum Format { BSON, MSGPACK, THRIFT_BINARY, THRIFT_COMPACT, FLATBUFFERS, JAVA_SERIAL }

    // ── Node kind ────────────────────────────────────────────────────────
    public enum Kind {
        DOCUMENT,   // BSON document / Thrift struct / FlatBuffers table
        MAP,        // MessagePack map
        ARRAY,      // BSON array / MessagePack array / FlatBuffers vector
        SCALAR,     // any leaf value (int, float, string, bytes, bool, null …)
        BINARY,     // raw bytes field
        UNKNOWN     // unrecognised type byte
    }

    // ── Fields ───────────────────────────────────────────────────────────
    private final Format              format;
    private final Kind                kind;
    /** Field/key name – null for anonymous array elements. */
    private       String              fieldName;
    /** Codec-level type label, e.g. "Int32", "UTCDate", "Map/16". */
    private final String              typeName;
    /** Human-readable value for SCALAR/BINARY nodes. */
    private       String              value;
    /** Raw bytes for BINARY nodes (shown in Hex tab). */
    private       byte[]              rawBytes;

    private final List<BinaryNode>            children   = new ArrayList<>();
    private final Map<BinaryNode, BinaryNode> mapEntries = new LinkedHashMap<>();

    // ── Constructor ──────────────────────────────────────────────────────
    public BinaryNode(Format format, Kind kind, String fieldName, String typeName) {
        this.format    = format;
        this.kind      = kind;
        this.fieldName = fieldName;
        this.typeName  = typeName;
    }

    // ── Accessors ────────────────────────────────────────────────────────
    public Format              getFormat()     { return format; }
    public Kind                getKind()       { return kind; }
    public String              getFieldName()  { return fieldName; }
    public String              getTypeName()   { return typeName; }
    public String              getValue()      { return value; }
    public byte[]              getRawBytes()   { return rawBytes; }
    public List<BinaryNode>    getChildren()   { return children; }
    public Map<BinaryNode,BinaryNode> getMapEntries() { return mapEntries; }

    public void setFieldName(String n)   { this.fieldName = n; }
    public void setValue(String v)       { this.value = v; }
    public void setRawBytes(byte[] b)    { this.rawBytes = b; }
    public void addChild(BinaryNode c)   { children.add(c); }
    public void addMapEntry(BinaryNode k, BinaryNode v) { mapEntries.put(k, v); }

    // ── Display helpers ──────────────────────────────────────────────────

    public String typeIcon() {
        return switch (kind) {
            case DOCUMENT -> "⊞";
            case MAP      -> "⊡";
            case ARRAY    -> "▶";
            case BINARY   -> "⬡";
            case UNKNOWN  -> "?";
            default       -> {
                if (value != null) {
                    char fc = value.isEmpty() ? ' ' : value.charAt(0);
                    yield (fc == '"') ? "\"" : "#";
                }
                yield "·";
            }
        };
    }

    public String colorHex() {
        return switch (kind) {
            case DOCUMENT -> "#a0c8ff";   // light blue
            case MAP      -> "#a0c8ff";
            case ARRAY    -> "#a0e8a0";   // green
            case BINARY   -> "#c0b0d0";   // lavender
            case UNKNOWN  -> "#ff6060";   // red
            default -> {
                if (typeName != null) {
                    if (typeName.contains("String") || typeName.contains("Str"))
                        yield "#e0a0d0";  // pink
                    if (typeName.contains("Int") || typeName.contains("Long")
                            || typeName.contains("Float") || typeName.contains("Double")
                            || typeName.contains("Byte") || typeName.contains("Short"))
                        yield "#7ec8e3";  // cyan
                    if (typeName.contains("Bool"))
                        yield "#f0c060";  // gold
                    if (typeName.contains("Null") || typeName.contains("Undef"))
                        yield "#909090";  // grey
                }
                yield "#c8d0e8";
            }
        };
    }

    public String treeLabel() {
        String prefix = (fieldName != null && !fieldName.isEmpty())
                ? fieldName + "  :  " : "";
        String type   = (typeName != null && !typeName.isEmpty())
                ? "[" + typeName + "]  " : "";

        return switch (kind) {
            case DOCUMENT -> prefix + typeIcon() + "  " + type
                    + "{ " + children.size() + " field" + (children.size() == 1 ? "" : "s") + " }";
            case MAP      -> prefix + typeIcon() + "  " + type
                    + "{ " + mapEntries.size() + " entry" + (mapEntries.size() == 1 ? "" : "ies") + " }";
            case ARRAY    -> prefix + typeIcon() + "  " + type
                    + "[ " + children.size() + " element" + (children.size() == 1 ? "" : "s") + " ]";
            case BINARY   -> {
                String v = value != null ? value : "";
                String p = v.length() > 72 ? v.substring(0, 72) + "…" : v;
                yield prefix + typeIcon() + "  " + type + p;
            }
            default -> {
                String v = value != null ? value : "<null>";
                String p = v.length() > 90 ? v.substring(0, 90) + "…" : v;
                yield prefix + typeIcon() + "  " + type + p;
            }
        };
    }

    @Override public String toString() { return treeLabel(); }
}
