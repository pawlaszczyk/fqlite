package fqlite.viewer.parser;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents one node in a parsed JSON document.
 *
 * Maps directly to JSON value types:
 *   OBJECT  – { … }
 *   ARRAY   – [ … ]
 *   STRING  – "…"
 *   NUMBER  – 42 / 3.14 / -1e5
 *   BOOLEAN – true / false
 *   NULL    – null
 */
public class JsonNode {

    public enum Type { OBJECT, ARRAY, STRING, NUMBER, BOOLEAN, NULL }

    // ── Fields ──────────────────────────────────────────────────────────
    private final Type                type;

    /** For STRING / NUMBER / BOOLEAN / NULL – the raw parsed text. */
    private final String              rawValue;

    /** For OBJECT – ordered key → child-node pairs. */
    private final Map<String,JsonNode>objectEntries = new LinkedHashMap<>();

    /** For ARRAY – child elements. */
    private final List<JsonNode>      arrayElements = new ArrayList<>();

    /** Key label (set when this node is an object value, for display). */
    private String                    keyLabel;

    // ── Constructors ────────────────────────────────────────────────────
    /** Scalar constructor (STRING, NUMBER, BOOLEAN, NULL). */
    public JsonNode(Type type, String rawValue) {
        this.type     = type;
        this.rawValue = rawValue;
    }

    /** Container constructor (OBJECT, ARRAY). */
    public JsonNode(Type type) {
        this.type     = type;
        this.rawValue = null;
    }

    // ── Accessors ───────────────────────────────────────────────────────
    public Type              getType()         { return type; }
    public String            getRawValue()     { return rawValue; }
    public Map<String,JsonNode>getObjectEntries(){ return objectEntries; }
    public List<JsonNode>    getArrayElements(){ return arrayElements; }
    public String            getKeyLabel()     { return keyLabel; }

    public void setKeyLabel(String k)               { this.keyLabel = k; }
    public void addObjectEntry(String k, JsonNode v){ objectEntries.put(k, v); }
    public void addArrayElement(JsonNode el)        { arrayElements.add(el); }

    // ── Display helpers ─────────────────────────────────────────────────
    public String typeIcon() {
        return switch (type) {
            case OBJECT  -> "⊞";
            case ARRAY   -> "▶";
            case STRING  -> "\"";
            case NUMBER  -> "#";
            case BOOLEAN -> "☑";
            case NULL    -> "∅";
        };
    }

    public String colorHex() {
        return switch (type) {
            case OBJECT  -> "#a0c8ff";   // light blue
            case ARRAY   -> "#a0e8a0";   // green
            case STRING  -> "#e0a0d0";   // pink
            case NUMBER  -> "#7ec8e3";   // blue
            case BOOLEAN -> "#f0c060";   // gold
            case NULL    -> "#c0c0c0";   // grey
        };
    }

    /** Label shown in the tree view. */
    public String treeLabel() {
        String prefix = keyLabel != null ? "\"" + keyLabel + "\"  :  " : "";
        String icon   = typeIcon() + " ";
        return switch (type) {
            case OBJECT  -> prefix + icon + "{ " + objectEntries.size() + " key"
                    + (objectEntries.size() == 1 ? "" : "s") + " }";
            case ARRAY   -> prefix + icon + "[ " + arrayElements.size() + " element"
                    + (arrayElements.size() == 1 ? "" : "s") + " ]";
            case NULL    -> prefix + icon + "null";
            default -> {
                String v = rawValue != null ? rawValue : "";
                String p = v.length() > 90 ? v.substring(0, 90) + "…" : v;
                yield type == Type.STRING
                        ? prefix + icon + "\"" + p + "\""
                        : prefix + icon + p;
            }
        };
    }

    @Override public String toString() { return treeLabel(); }
}
