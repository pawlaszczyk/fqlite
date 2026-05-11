package fqlite.viewer.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a single decoded Protobuf field (or root message).
 * Since we have no .proto schema, we work purely with wire types and
 * heuristic interpretation.
 */
public class ProtoField {

    // ── Wire Types ─────────────────────────────────────────────────────────
    public static final int WIRE_VARINT           = 0;  // int32, int64, uint32, uint64, sint32, sint64, bool, enum
    public static final int WIRE_64BIT            = 1;  // fixed64, sfixed64, double
    public static final int WIRE_LEN              = 2;  // string, bytes, embedded message, packed repeated
    public static final int WIRE_SGROUP           = 3;  // Start group (deprecated)
    public static final int WIRE_EGROUP           = 4;  // End group   (deprecated)
    public static final int WIRE_32BIT            = 5;  // fixed32, sfixed32, float

    // ── Fields ─────────────────────────────────────────────────────────────
    private final int    fieldNumber;
    private final int    wireType;
    private final String wireTypeName;

    /** Raw byte offset within the source file (for hex navigation). */
    private final long   byteOffset;
    /** Raw bytes of the value part (without the tag/key byte(s)). */
    private final byte[] rawBytes;

    /** Heuristic / primary decoded value as a readable string. */
    private String       interpretedValue;
    /** Alternative interpretations offered to the user. */
    private final List<InterpretedAs> alternatives = new ArrayList<>();

    /** Child fields – filled when wireType == WIRE_LEN and content looks like a message. */
    private final List<ProtoField> children = new ArrayList<>();

    /** True if this length-delimited field was successfully parsed as a nested message. */
    private boolean nestedMessage = false;

    // ── Constructor ────────────────────────────────────────────────────────
    public ProtoField(int fieldNumber, int wireType, long byteOffset, byte[] rawBytes) {
        this.fieldNumber = fieldNumber;
        this.wireType    = wireType;
        this.byteOffset  = byteOffset;
        this.rawBytes    = rawBytes;
        this.wireTypeName = wireTypeName(wireType);
    }

    // ── Static helpers ─────────────────────────────────────────────────────
    public static String wireTypeName(int wt) {
        return switch (wt) {
            case WIRE_VARINT -> "Varint";
            case WIRE_64BIT  -> "64-bit";
            case WIRE_LEN    -> "Len-delim";
            case WIRE_SGROUP -> "SGroup";
            case WIRE_EGROUP -> "EGroup";
            case WIRE_32BIT  -> "32-bit";
            default          -> "Unknown(" + wt + ")";
        };
    }

    // ── Accessors ──────────────────────────────────────────────────────────
    public int            getFieldNumber()      { return fieldNumber; }
    public int            getWireType()         { return wireType; }
    public String         getWireTypeName()     { return wireTypeName; }
    public long           getByteOffset()       { return byteOffset; }
    public byte[]         getRawBytes()         { return rawBytes; }
    public String         getInterpretedValue() { return interpretedValue; }
    public List<InterpretedAs> getAlternatives(){ return alternatives; }
    public List<ProtoField>    getChildren()    { return children; }
    public boolean        isNestedMessage()    { return nestedMessage; }

    public void setInterpretedValue(String v)   { this.interpretedValue = v; }
    public void setNestedMessage(boolean b)     { this.nestedMessage = b; }

    public void addAlternative(InterpretedAs alt) { alternatives.add(alt); }
    public void addChild(ProtoField child)        { children.add(child); }

    /**
     * Label shown in the tree view.
     */
    public String treeLabel() {
        String base = "Field " + fieldNumber + "  [" + wireTypeName + "]";
        if (interpretedValue != null && !interpretedValue.isEmpty()) {
            String preview = interpretedValue.length() > 80
                    ? interpretedValue.substring(0, 80) + "…"
                    : interpretedValue;
            return base + " = " + preview;
        }
        return base;
    }

    // ──────────────────────────────────────────────────────────────────────
    /** One alternative interpretation of the raw bytes. */
    public record InterpretedAs(String typeName, String value) {
        @Override public String toString() { return typeName + ": " + value; }
    }
}
