package fqlite.viewer.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a single node in a parsed Apple Binary PList (bplist00/bplist15/bplist16).
 *
 * Apple's binary plist format stores typed objects (strings, integers, arrays,
 * dicts, data, dates, UIDs …) in an object table, referenced by index.
 * After parsing the object table we build a recursive tree of BPListNodes.
 */
public class BPListNode {

    // ── Node types ─────────────────────────────────────────────────────────
    public enum Type {
        NULL,
        BOOL,
        INT,
        REAL,
        DATE,
        DATA,
        STRING,
        UID,
        ARRAY,
        DICT,
        SET,
        FILL      // 0x0F fill byte – padding
    }

    // ── Fields ─────────────────────────────────────────────────────────────
    private final Type   type;
    private final long   byteOffset;   // offset in source file
    private final byte[] rawBytes;     // raw encoding bytes

    /** Scalar value (for non-container types) – human-readable string. */
    private String scalarValue;

    /** For DICT: ordered key → value pairs. */
    private final Map<BPListNode, BPListNode> dictEntries = new LinkedHashMap<>();

    /** For ARRAY / SET: child elements. */
    private final List<BPListNode> arrayElements = new ArrayList<>();

    /** Key label (only set when this node is a dict value, for display). */
    private String keyLabel;

    // ── Constructor ────────────────────────────────────────────────────────
    public BPListNode(Type type, long byteOffset, byte[] rawBytes) {
        this.type       = type;
        this.byteOffset = byteOffset;
        this.rawBytes   = rawBytes != null ? rawBytes : new byte[0];
    }

    // ── Accessors ──────────────────────────────────────────────────────────
    public Type   getType()         { return type; }
    public long   getByteOffset()   { return byteOffset; }
    public byte[] getRawBytes()     { return rawBytes; }
    public String getScalarValue()  { return scalarValue; }
    public Map<BPListNode, BPListNode> getDictEntries()    { return dictEntries; }
    public List<BPListNode>            getArrayElements()  { return arrayElements; }
    public String getKeyLabel()     { return keyLabel; }

    public void setScalarValue(String v) { this.scalarValue = v; }
    public void setKeyLabel(String k)    { this.keyLabel = k; }

    public void addDictEntry(BPListNode key, BPListNode value) {
        dictEntries.put(key, value);
    }
    public void addArrayElement(BPListNode el) {
        arrayElements.add(el);
    }

    // ── Display helpers ────────────────────────────────────────────────────

    /** Icon prefix for tree display. */
    public String typeIcon() {
        return switch (type) {
            case NULL   -> "∅";
            case BOOL   -> "☑";
            case INT    -> "#";
            case REAL   -> "~";
            case DATE   -> "📅";
            case DATA   -> "⬡";
            case STRING -> "\"";
            case UID    -> "🔑";
            case ARRAY  -> "▶";
            case DICT   -> "⊞";
            case SET    -> "{}";
            case FILL   -> "·";
        };
    }

    /** Short label for this node, used in tree cells. */
    public String treeLabel() {
        String prefix = keyLabel != null ? keyLabel + "  →  " : "";
        String icon   = typeIcon() + " ";
        return switch (type) {
            case DICT  -> prefix + icon + "[Dictionary – " + dictEntries.size() + " entries]";
            case ARRAY -> prefix + icon + "[Array – " + arrayElements.size() + " elements]";
            case SET   -> prefix + icon + "[Set – " + arrayElements.size() + " elements]";
            case NULL  -> prefix + icon + "null";
            default    -> {
                String val = scalarValue != null ? scalarValue : "";
                String preview = val.length() > 90 ? val.substring(0, 90) + "…" : val;
                yield prefix + icon + preview;
            }
        };
    }

    /** Colour hint for the tree cell – used by the dark-theme fallback in BPListNode. */
    public String colorHex() {
        return switch (type) {
            case STRING -> "#e0a0d0";  // pink
            case INT    -> "#7ec8e3";  // blue
            case REAL   -> "#7ec8e3";  // blue
            case BOOL   -> "#f0c060";  // gold
            case DATE   -> "#f0c060";  // gold
            case DATA   -> "#c0b0d0";  // lavender
            case UID    -> "#f08070";  // salmon
            case ARRAY, SET -> "#a0e8a0";  // green
            case DICT   -> "#a0c8ff";  // light blue
            default     -> "#c0c0c0";
        };
    }

    @Override
    public String toString() { return treeLabel(); }
}
