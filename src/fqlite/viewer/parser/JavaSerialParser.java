package fqlite.viewer.parser;

import fqlite.viewer.parser.BinaryNode.Format;
import fqlite.viewer.parser.BinaryNode.Kind;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Schema-less Java Object Serialization Stream parser.
 *
 * Spec: https://docs.oracle.com/en/java/docs/books/platform/serialization/spec/protocol.doc.html
 *       (Java Object Serialization Specification, Chapter 6)
 *
 * Stream structure:
 *   magic(2)   = 0xACED
 *   version(2) = 0x0005
 *   content*   = sequence of TC_* blocks
 *
 * ── TC type codes ──────────────────────────────────────────────────────
 *   0x70 TC_NULL           0x71 TC_REFERENCE      0x72 TC_CLASSDESC
 *   0x73 TC_OBJECT         0x74 TC_STRING         0x75 TC_ARRAY
 *   0x76 TC_CLASS          0x77 TC_BLOCKDATA      0x78 TC_ENDBLOCKDATA
 *   0x79 TC_RESET          0x7A TC_BLOCKDATALONG  0x7B TC_EXCEPTION
 *   0x7C TC_LONGSTRING     0x7D TC_PROXYCLASSDESC 0x7E TC_ENUM
 *
 * ── Class descriptor flags (classDescFlags) ───────────────────────────
 *   0x01 SC_WRITE_METHOD   0x02 SC_BLOCK_DATA
 *   0x04 SC_SERIALIZABLE   0x08 SC_EXTERNALIZABLE
 *   0x10 SC_ENUM
 *
 * ── Field type codes ──────────────────────────────────────────────────
 *   B=byte  C=char  D=double  F=float  I=int  J=long  S=short  Z=boolean
 *   [=array ref  L=object ref
 */
public class JavaSerialParser {

    private static final String NAME = "JavaSerial";

    /** Stream magic. */
    public static final int STREAM_MAGIC   = 0xACED;
    /** Stream version. */
    public static final int STREAM_VERSION = 0x0005;

    // TC type codes
    private static final int TC_NULL           = 0x70;
    private static final int TC_REFERENCE      = 0x71;
    private static final int TC_CLASSDESC      = 0x72;
    private static final int TC_OBJECT         = 0x73;
    private static final int TC_STRING         = 0x74;
    private static final int TC_ARRAY          = 0x75;
    private static final int TC_CLASS          = 0x76;
    private static final int TC_BLOCKDATA      = 0x77;
    private static final int TC_ENDBLOCKDATA   = 0x78;
    private static final int TC_RESET          = 0x79;
    private static final int TC_BLOCKDATALONG  = 0x7A;
    private static final int TC_EXCEPTION      = 0x7B;
    private static final int TC_LONGSTRING     = 0x7C;
    private static final int TC_PROXYCLASSDESC = 0x7D;
    private static final int TC_ENUM           = 0x7E;

    // classDescFlags
    private static final int SC_WRITE_METHOD   = 0x01;
    private static final int SC_BLOCK_DATA     = 0x02;
    private static final int SC_SERIALIZABLE   = 0x04;
    private static final int SC_EXTERNALIZABLE = 0x08;
    private static final int SC_ENUM           = 0x10;

    private static final int MAX_DEPTH = 32;

    // ── Parser state ─────────────────────────────────────────────────────
    private byte[] data;
    private int    pos;

    /**
     * Handle table: every object/class/string gets a handle starting at
     * 0x7E0000. We store their display labels for reference resolution.
     */
    private final List<String> handles = new ArrayList<>();

    // ── Public API ───────────────────────────────────────────────────────

    public BinaryNode parse(byte[] raw) throws BinaryParseException {
        if (raw == null || raw.length < 4)
            throw new BinaryParseException(NAME, "Data too short (min 4 bytes)");

        int magic   = ((raw[0] & 0xFF) << 8) | (raw[1] & 0xFF);
        int version = ((raw[2] & 0xFF) << 8) | (raw[3] & 0xFF);

        if (magic != STREAM_MAGIC)
            throw new BinaryParseException(NAME,
                    String.format("Invalid magic 0x%04X (expected 0xACED)", magic));
        if (version != STREAM_VERSION)
            throw new BinaryParseException(NAME,
                    String.format("Unknown stream version 0x%04X (expected 0x0005)", version));

        this.data    = raw;
        this.pos     = 4;
        this.handles.clear();

        BinaryNode root = new BinaryNode(Format.JAVA_SERIAL, Kind.DOCUMENT,
                "stream", "JavaSerialStream");
        root.setValue("magic=0xACED  version=5  (" + raw.length + " bytes)");

        while (pos < data.length) {
            BinaryNode child = readContent(null, 0);
            if (child != null) root.addChild(child);
        }
        return root;
    }

    // ── Content dispatcher ───────────────────────────────────────────────

    private BinaryNode readContent(String fieldName, int depth) throws BinaryParseException {
        if (pos >= data.length) return null;
        if (depth > MAX_DEPTH)
            throw new BinaryParseException(NAME, "Nesting depth exceeds " + MAX_DEPTH);

        int tc = data[pos++] & 0xFF;

        return switch (tc) {
            case TC_OBJECT         -> readObject(fieldName, depth);
            case TC_CLASS          -> readClass(fieldName);
            case TC_ARRAY          -> readArray(fieldName, depth);
            case TC_STRING         -> readString(fieldName, false);
            case TC_LONGSTRING     -> readString(fieldName, true);
            case TC_ENUM           -> readEnum(fieldName);
            case TC_CLASSDESC      -> readClassDesc(fieldName, depth);
            case TC_PROXYCLASSDESC -> readProxyClassDesc(fieldName);
            case TC_REFERENCE      -> readReference(fieldName);
            case TC_NULL           -> nullNode(fieldName);
            case TC_BLOCKDATA      -> readBlockData(fieldName, false);
            case TC_BLOCKDATALONG  -> readBlockData(fieldName, true);
            case TC_ENDBLOCKDATA   -> endBlockNode(fieldName);
            case TC_RESET          -> {
                handles.clear();
                yield leaf(fieldName, "TC_RESET", "<handle table reset>");
            }
            case TC_EXCEPTION      -> leaf(fieldName, "TC_EXCEPTION", "<exception in stream>");
            default -> leaf(fieldName,
                    "Unknown(0x" + Integer.toHexString(tc) + ")", "<?>");
        };
    }

    // ── TC_OBJECT ────────────────────────────────────────────────────────

    private BinaryNode readObject(String fieldName, int depth) throws BinaryParseException {
        BinaryNode obj = new BinaryNode(Format.JAVA_SERIAL, Kind.DOCUMENT,
                fieldName, "Object");
        int handle = assignHandle();

        // classDesc
        BinaryNode cd = readClassDescOrRef("classDesc", depth);
        if (cd != null) {
            obj.setValue(cd.getValue() != null ? cd.getValue() : cd.getFieldName());
            obj.addChild(cd);
        }

        // classdata for each class in the hierarchy (classDesc + all superclasses)
        // We read field values based on what the classDesc told us.
        // For simplicity we read classdata blocks until TC_ENDBLOCKDATA.
        readClassData(obj, depth);

        handles.set(handle, "Object:" + (obj.getValue() != null ? obj.getValue() : "?"));
        return obj;
    }

    // ── TC_CLASS ─────────────────────────────────────────────────────────

    private BinaryNode readClass(String fieldName) throws BinaryParseException {
        int handle = assignHandle();
        BinaryNode cd = readClassDescOrRef("classDesc", 0);
        String name = cd != null && cd.getValue() != null ? cd.getValue() : "?";
        handles.set(handle, "Class:" + name);
        BinaryNode n = leaf(fieldName, "Class", name);
        if (cd != null) n.addChild(cd);
        return n;
    }

    // ── TC_ARRAY ─────────────────────────────────────────────────────────

    private BinaryNode readArray(String fieldName, int depth) throws BinaryParseException {
        BinaryNode cd = readClassDescOrRef("classDesc", depth);
        int handle = assignHandle();
        int count = readInt32();

        String typeName = "Array[" + count + "]";
        String elemTypeCode = (cd != null && cd.getValue() != null)
                ? cd.getValue() : "?";
        // Array class names start with '[', e.g. "[I", "[Ljava.lang.String;"
        char elemType = elemTypeCode.length() > 1 ? elemTypeCode.charAt(1) : '?';

        BinaryNode arr = new BinaryNode(Format.JAVA_SERIAL, Kind.ARRAY,
                fieldName, typeName);
        arr.setValue(elemTypeCode + "  [" + count + " element(s)]");
        handles.set(handle, "Array:" + elemTypeCode + "[" + count + "]");

        int limit = Math.min(count, 1024); // cap very large arrays
        for (int i = 0; i < limit; i++) {
            BinaryNode elem = readFieldValue("[" + i + "]", elemType, depth + 1);
            arr.addChild(elem);
        }
        if (count > limit) {
            arr.addChild(leaf("[…]", "truncated",
                    "… " + (count - limit) + " more elements"));
        }
        return arr;
    }

    // ── TC_STRING / TC_LONGSTRING ─────────────────────────────────────────

    private BinaryNode readString(String fieldName, boolean isLong) throws BinaryParseException {
        int handle = assignHandle();
        String s = isLong ? readLongUtf() : readUtf();
        handles.set(handle, "String:\"" + truncate(s, 40) + "\"");
        return leaf(fieldName, isLong ? "LongString" : "String", "\"" + s + "\"");
    }

    // ── TC_ENUM ──────────────────────────────────────────────────────────

    private BinaryNode readEnum(String fieldName) throws BinaryParseException {
        BinaryNode cd = readClassDescOrRef("classDesc", 0);
        int handle = assignHandle();
        // constantName is a TC_STRING
        if (pos >= data.length)
            throw new BinaryParseException(NAME, "Truncated enum constant name");
        int tc = data[pos++] & 0xFF;
        String constName = "?";
        if (tc == TC_STRING)     constName = readUtf();
        else if (tc == TC_LONGSTRING) constName = readLongUtf();
        else if (tc == TC_REFERENCE) {
            int ref = readInt32();
            int idx = ref - 0x7E0000;
            constName = (idx >= 0 && idx < handles.size()) ? handles.get(idx) : "ref#" + ref;
        }
        String enumClass = (cd != null && cd.getValue() != null)
                ? cd.getValue() : "Enum";
        handles.set(handle, "Enum:" + enumClass + "." + constName);
        return leaf(fieldName, "Enum(" + enumClass + ")", constName);
    }

    // ── TC_CLASSDESC ─────────────────────────────────────────────────────

    private BinaryNode readClassDesc(String fieldName, int depth) throws BinaryParseException {
        String className  = readUtf();
        long   serialUID  = readInt64();
        int    handle     = assignHandle();
        int    flags      = data[pos++] & 0xFF;
        int    fieldCount = readInt16();

        BinaryNode cd = new BinaryNode(Format.JAVA_SERIAL, Kind.DOCUMENT,
                fieldName, "ClassDesc");
        cd.setValue(className);
        handles.set(handle, "ClassDesc:" + className);

        // Flag summary
        cd.addChild(leaf("className",    "String",  "\"" + className + "\""));
        cd.addChild(leaf("serialVersionUID", "long", "0x" + Long.toHexString(serialUID).toUpperCase()
                + "  (" + serialUID + ")"));
        cd.addChild(leaf("classDescFlags", "flags",  describeFlags(flags)));

        // Fields
        BinaryNode fieldsNode = new BinaryNode(Format.JAVA_SERIAL, Kind.ARRAY,
                "fields", "FieldDesc[" + fieldCount + "]");
        for (int i = 0; i < fieldCount; i++) {
            char typeCode = (char)(data[pos++] & 0xFF);
            String fName  = readUtf();
            BinaryNode fd = leaf(fName, "field(" + typeCode + ")",
                    primitiveTypeName(typeCode));
            // For object/array fields there is a className1 (TC_STRING or ref)
            if (typeCode == 'L' || typeCode == '[') {
                if (pos < data.length) {
                    int tcRef = data[pos++] & 0xFF;
                    String clsName = switch (tcRef) {
                        case TC_STRING     -> readUtf();
                        case TC_LONGSTRING -> readLongUtf();
                        case TC_REFERENCE  -> {
                            int r = readInt32();
                            int idx = r - 0x7E0000;
                            yield (idx >= 0 && idx < handles.size())
                                    ? handles.get(idx) : "ref#" + r;
                        }
                        default -> "?";
                    };
                    fd.setValue(primitiveTypeName(typeCode) + "  <" + clsName + ">");
                }
            }
            fieldsNode.addChild(fd);
        }
        cd.addChild(fieldsNode);

        // classAnnotation – read until TC_ENDBLOCKDATA
        BinaryNode annot = new BinaryNode(Format.JAVA_SERIAL, Kind.ARRAY,
                "classAnnotation", "annotation");
        while (pos < data.length) {
            if ((data[pos] & 0xFF) == TC_ENDBLOCKDATA) { pos++; break; }
            BinaryNode a = readContent("annotation", depth + 1);
            if (a != null) annot.addChild(a);
        }
        if (!annot.getChildren().isEmpty()) cd.addChild(annot);

        // superClassDesc
        BinaryNode superCd = readClassDescOrRef("superClassDesc", depth + 1);
        if (superCd != null) cd.addChild(superCd);

        return cd;
    }

    // ── TC_PROXYCLASSDESC ────────────────────────────────────────────────

    private BinaryNode readProxyClassDesc(String fieldName) throws BinaryParseException {
        int handle     = assignHandle();
        int ifaceCount = readInt32();
        BinaryNode pd  = new BinaryNode(Format.JAVA_SERIAL, Kind.DOCUMENT,
                fieldName, "ProxyClassDesc");
        for (int i = 0; i < ifaceCount; i++) {
            String iface = readUtf();
            pd.addChild(leaf("interface[" + i + "]", "String", "\"" + iface + "\""));
        }
        handles.set(handle, "ProxyClassDesc");
        // classAnnotation until TC_ENDBLOCKDATA
        while (pos < data.length) {
            if ((data[pos] & 0xFF) == TC_ENDBLOCKDATA) { pos++; break; }
            BinaryNode a = readContent("annotation", 1);
            if (a != null) pd.addChild(a);
        }
        // superClassDesc
        BinaryNode superCd = readClassDescOrRef("superClassDesc", 1);
        if (superCd != null) pd.addChild(superCd);
        return pd;
    }

    // ── TC_REFERENCE ─────────────────────────────────────────────────────

    private BinaryNode readReference(String fieldName) throws BinaryParseException {
        int handle = readInt32();
        int idx    = handle - 0x7E0000;
        String target = (idx >= 0 && idx < handles.size())
                ? handles.get(idx) : "?";
        return leaf(fieldName, "Reference",
                "→ handle 0x" + Integer.toHexString(handle) + "  [" + target + "]");
    }

    // ── TC_BLOCKDATA / TC_BLOCKDATALONG ───────────────────────────────────

    private BinaryNode readBlockData(String fieldName, boolean isLong)
            throws BinaryParseException {
        int len = isLong ? readInt32() : (data[pos++] & 0xFF);
        if (len < 0 || pos + len > data.length)
            throw new BinaryParseException(NAME,
                    "BlockData length " + len + " exceeds buffer at offset " + pos);
        byte[] bytes = new byte[len];
        System.arraycopy(data, pos, bytes, 0, len);
        pos += len;

        BinaryNode n = new BinaryNode(Format.JAVA_SERIAL, Kind.BINARY,
                fieldName, isLong ? "BlockDataLong" : "BlockData");
        n.setRawBytes(bytes);
        // Heuristic: try UTF-8, else show hex preview
        String utf = tryUtf8(bytes);
        n.setValue(utf != null
                ? "\"" + truncate(utf, 80) + "\""
                : hexPreview(bytes) + "  (" + len + " bytes)");
        return n;
    }

    // ── classdata reader ─────────────────────────────────────────────────

    /**
     * Reads classdata blocks for an object. Without a full class hierarchy
     * we use a best-effort approach: read TC_BLOCKDATA blocks and any
     * TC_* tokens until TC_ENDBLOCKDATA or end-of-useful-content.
     */
    private void readClassData(BinaryNode parent, int depth) throws BinaryParseException {
        // We stop at EOF or when we see something that looks like the end
        // of the object's data. This is inherently heuristic without a
        // schema – we read blocks and TC tokens greedily.
        int safety = 0;
        while (pos < data.length && safety++ < 512) {
            int peek = data[pos] & 0xFF;
            if (peek == TC_ENDBLOCKDATA) {
                pos++; // consume
                return;
            }
            // Stop if we hit a token that belongs to the outer stream
            if (peek == TC_RESET || peek == TC_EXCEPTION) return;

            BinaryNode child = readContent("data", depth + 1);
            if (child != null) parent.addChild(child);
        }
    }

    // ── Field value reader ────────────────────────────────────────────────

    /**
     * Reads a single field value given its type code character.
     * Primitive types are read directly; object/array types recurse via readContent.
     */
    private BinaryNode readFieldValue(String fieldName, char typeCode, int depth)
            throws BinaryParseException {
        return switch (typeCode) {
            case 'B' -> {
                int v = data[pos++] & 0xFF;
                yield leaf(fieldName, "byte", String.valueOf((byte) v));
            }
            case 'C' -> {
                int v = readInt16() & 0xFFFF;
                yield leaf(fieldName, "char", "'" + (char) v + "'  (U+" + Integer.toHexString(v).toUpperCase() + ")");
            }
            case 'D' -> {
                double d = Double.longBitsToDouble(readInt64());
                yield leaf(fieldName, "double", String.valueOf(d));
            }
            case 'F' -> {
                float f = Float.intBitsToFloat(readInt32());
                yield leaf(fieldName, "float", String.valueOf(f));
            }
            case 'I' -> leaf(fieldName, "int",     String.valueOf(readInt32()));
            case 'J' -> leaf(fieldName, "long",    String.valueOf(readInt64()));
            case 'S' -> leaf(fieldName, "short",   String.valueOf((short) readInt16()));
            case 'Z' -> {
                int v = data[pos++] & 0xFF;
                yield leaf(fieldName, "boolean", v != 0 ? "true" : "false");
            }
            case 'L', '[' -> readContent(fieldName, depth);
            default -> leaf(fieldName, "?(" + typeCode + ")", "<?>");
        };
    }

    // ── classDesc-or-reference dispatcher ───────────────────────────────

    private BinaryNode readClassDescOrRef(String fieldName, int depth)
            throws BinaryParseException {
        if (pos >= data.length) return null;
        int tc = data[pos++] & 0xFF;
        return switch (tc) {
            case TC_CLASSDESC      -> readClassDesc(fieldName, depth);
            case TC_PROXYCLASSDESC -> readProxyClassDesc(fieldName);
            case TC_REFERENCE      -> readReference(fieldName);
            case TC_NULL           -> nullNode(fieldName);
            default -> {
                // Put the byte back and return null
                pos--;
                yield null;
            }
        };
    }

    // ── Handle management ─────────────────────────────────────────────────

    /** Assigns the next handle index and returns it. */
    private int assignHandle() {
        int idx = handles.size();
        handles.add("?");
        return idx;
    }

    // ── Leaf factory helpers ─────────────────────────────────────────────

    private static BinaryNode leaf(String fieldName, String typeName, String value) {
        BinaryNode n = new BinaryNode(Format.JAVA_SERIAL, Kind.SCALAR, fieldName, typeName);
        n.setValue(value);
        return n;
    }

    private static BinaryNode nullNode(String fieldName) {
        return leaf(fieldName, "null", "null");
    }

    private static BinaryNode endBlockNode(String fieldName) {
        return leaf(fieldName, "TC_ENDBLOCKDATA", "<end of block>");
    }

    // ── Low-level readers ────────────────────────────────────────────────

    private int readInt16() throws BinaryParseException {
        if (pos + 2 > data.length) throw new BinaryParseException(NAME, "EOF reading int16 at " + pos);
        int v = ((data[pos] & 0xFF) << 8) | (data[pos + 1] & 0xFF);
        pos += 2;
        return v;
    }

    private int readInt32() throws BinaryParseException {
        if (pos + 4 > data.length) throw new BinaryParseException(NAME, "EOF reading int32 at " + pos);
        int v = ByteBuffer.wrap(data, pos, 4).order(ByteOrder.BIG_ENDIAN).getInt();
        pos += 4;
        return v;
    }

    private long readInt64() throws BinaryParseException {
        if (pos + 8 > data.length) throw new BinaryParseException(NAME, "EOF reading int64 at " + pos);
        long v = ByteBuffer.wrap(data, pos, 8).order(ByteOrder.BIG_ENDIAN).getLong();
        pos += 8;
        return v;
    }

    /** Read a modified UTF-8 string (2-byte length prefix). */
    private String readUtf() throws BinaryParseException {
        int len = readInt16() & 0xFFFF;
        if (pos + len > data.length)
            throw new BinaryParseException(NAME, "UTF string length " + len + " exceeds buffer at " + pos);
        String s = new String(data, pos, len, StandardCharsets.UTF_8);
        pos += len;
        return s;
    }

    /** Read a long UTF-8 string (8-byte length prefix). */
    private String readLongUtf() throws BinaryParseException {
        long len = readInt64();
        if (len < 0 || pos + len > data.length)
            throw new BinaryParseException(NAME, "LongUTF length " + len + " invalid at " + pos);
        String s = new String(data, pos, (int) len, StandardCharsets.UTF_8);
        pos += (int) len;
        return s;
    }

    // ── Display helpers ──────────────────────────────────────────────────

    private static String primitiveTypeName(char c) {
        return switch (c) {
            case 'B' -> "byte";    case 'C' -> "char";
            case 'D' -> "double";  case 'F' -> "float";
            case 'I' -> "int";     case 'J' -> "long";
            case 'S' -> "short";   case 'Z' -> "boolean";
            case '[' -> "array";   case 'L' -> "Object";
            default  -> String.valueOf(c);
        };
    }

    private static String describeFlags(int flags) {
        List<String> parts = new ArrayList<>();
        if ((flags & SC_SERIALIZABLE)   != 0) parts.add("SERIALIZABLE");
        if ((flags & SC_EXTERNALIZABLE) != 0) parts.add("EXTERNALIZABLE");
        if ((flags & SC_WRITE_METHOD)   != 0) parts.add("WRITE_METHOD");
        if ((flags & SC_BLOCK_DATA)     != 0) parts.add("BLOCK_DATA");
        if ((flags & SC_ENUM)           != 0) parts.add("ENUM");
        return "0x" + Integer.toHexString(flags).toUpperCase()
                + "  " + String.join(" | ", parts);
    }

    private static String hexPreview(byte[] b) {
        int shown = Math.min(b.length, 32);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < shown; i++) sb.append(String.format("%02X ", b[i] & 0xFF));
        if (b.length > shown) sb.append("…");
        return sb.toString().trim();
    }

    private static String tryUtf8(byte[] bytes) {
        try {
            java.nio.charset.CharsetDecoder dec = StandardCharsets.UTF_8.newDecoder();
            String s = dec.decode(ByteBuffer.wrap(bytes)).toString();
            long printable = s.chars()
                    .filter(c -> c >= 0x20 || c == '\n' || c == '\r' || c == '\t')
                    .count();
            return (printable >= s.length() * 0.85 && !s.isEmpty()) ? s : null;
        } catch (Exception e) { return null; }
    }

    private static String truncate(String s, int max) {
        return s.length() > max ? s.substring(0, max) + "…" : s;
    }
}
