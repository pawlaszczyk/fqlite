package fqlite.viewer.parser;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;


/**
 * Schema-less MessagePack decoder.
 *
 * Spec: https://github.com/msgpack/msgpack/blob/master/spec.md
 *
 * Covers all format families:
 *   positive fixint  0x00–0x7F      fixmap   0x80–0x8F
 *   fixarray         0x90–0x9F      fixstr   0xA0–0xBF
 *   nil/false/true   0xC0–0xC3      bin8/16/32  0xC4–0xC6
 *   ext8/16/32       0xC7–0xC9      float32/64  0xCA–0xCB
 *   uint8/16/32/64   0xCC–0xCF      int8/16/32/64 0xD0–0xD3
 *   fixext1–16       0xD4–0xD8      str8/16/32  0xD9–0xDB
 *   array16/32       0xDC–0xDD      map16/32    0xDE–0xDF
 *   negative fixint  0xE0–0xFF
 */
public class MsgPackParser {

    private static final String NAME = "MessagePack";
    private static final int MAX_DEPTH = 64;

    // ── Public API ───────────────────────────────────────────────────────

    /**
     * Parse a single MessagePack value from the start of {@code data}.
     * Returns the root BinaryNode; remaining bytes (if any) are ignored.
     */
    public BinaryNode parse(byte[] data) throws BinaryParseException {
        if (data == null || data.length == 0)
            throw new BinaryParseException(NAME, "Empty data");
        int[] pos = { 0 };
        BinaryNode root = parseValue(data, pos, null, 0);
        return root;
    }

    // ── Core recursive decoder ───────────────────────────────────────────

    private BinaryNode parseValue(byte[] data, int[] pos, String fieldName, int depth)
            throws BinaryParseException {

        if (depth > MAX_DEPTH)
            throw new BinaryParseException(NAME, "Nesting depth exceeds " + MAX_DEPTH);
        if (pos[0] >= data.length)
            throw new BinaryParseException(NAME, "Unexpected end of data at offset " + pos[0]);

        int fb = data[pos[0]++] & 0xFF; // first byte

        // ── Positive fixint  0x00–0x7F ────────────────────────────────
        if (fb <= 0x7F) return scalar(fieldName, "uint(fix)", String.valueOf(fb));

        // ── fixmap  0x80–0x8F ─────────────────────────────────────────
        if (fb <= 0x8F) return parseMap(data, pos, fieldName, fb & 0x0F, depth);

        // ── fixarray  0x90–0x9F ───────────────────────────────────────
        if (fb <= 0x9F) return parseArray(data, pos, fieldName, fb & 0x0F, depth);

        // ── fixstr  0xA0–0xBF ─────────────────────────────────────────
        if (fb <= 0xBF) {
            int len = fb & 0x1F;
            return scalar(fieldName, "str(fix/" + len + ")", readUtf8(data, pos, len));
        }

        // ── Negative fixint  0xE0–0xFF ────────────────────────────────
        if (fb >= 0xE0) return scalar(fieldName, "int(fix)", String.valueOf((byte) fb));

        // ── Named types ───────────────────────────────────────────────
        return switch (fb) {
            case 0xC0 -> scalar(fieldName, "nil",   "null");
            case 0xC2 -> scalar(fieldName, "false", "false");
            case 0xC3 -> scalar(fieldName, "true",  "true");

            // bin8/16/32
            case 0xC4 -> parseBin(data, pos, fieldName, readUint8(data, pos));
            case 0xC5 -> parseBin(data, pos, fieldName, readUint16BE(data, pos));
            case 0xC6 -> parseBin(data, pos, fieldName, (int) readUint32BE(data, pos));

            // ext8/16/32
            case 0xC7 -> parseExt(data, pos, fieldName, readUint8(data, pos));
            case 0xC8 -> parseExt(data, pos, fieldName, readUint16BE(data, pos));
            case 0xC9 -> parseExt(data, pos, fieldName, (int) readUint32BE(data, pos));

            // float32/64
            case 0xCA -> {
                float f = ByteBuffer.wrap(readExact(data, pos, 4)).order(ByteOrder.BIG_ENDIAN).getFloat();
                yield scalar(fieldName, "float32", String.valueOf(f));
            }
            case 0xCB -> {
                double d = ByteBuffer.wrap(readExact(data, pos, 8)).order(ByteOrder.BIG_ENDIAN).getDouble();
                yield scalar(fieldName, "float64", String.valueOf(d));
            }

            // uint 8/16/32/64
            case 0xCC -> scalar(fieldName, "uint8",  String.valueOf(readUint8(data, pos)));
            case 0xCD -> scalar(fieldName, "uint16", String.valueOf(readUint16BE(data, pos)));
            case 0xCE -> scalar(fieldName, "uint32", Long.toUnsignedString(readUint32BE(data, pos)));
            case 0xCF -> {
                long ul = ByteBuffer.wrap(readExact(data, pos, 8)).order(ByteOrder.BIG_ENDIAN).getLong();
                yield scalar(fieldName, "uint64", Long.toUnsignedString(ul));
            }

            // int 8/16/32/64
            case 0xD0 -> scalar(fieldName, "int8",  String.valueOf((byte)  readUint8(data, pos)));
            case 0xD1 -> scalar(fieldName, "int16", String.valueOf((short) readUint16BE(data, pos)));
            case 0xD2 -> {
                int i = ByteBuffer.wrap(readExact(data, pos, 4)).order(ByteOrder.BIG_ENDIAN).getInt();
                yield scalar(fieldName, "int32", String.valueOf(i));
            }
            case 0xD3 -> {
                long l = ByteBuffer.wrap(readExact(data, pos, 8)).order(ByteOrder.BIG_ENDIAN).getLong();
                yield scalar(fieldName, "int64", String.valueOf(l));
            }

            // fixext 1/2/4/8/16
            case 0xD4 -> parseExt(data, pos, fieldName, 1);
            case 0xD5 -> parseExt(data, pos, fieldName, 2);
            case 0xD6 -> parseExt(data, pos, fieldName, 4);
            case 0xD7 -> parseExt(data, pos, fieldName, 8);
            case 0xD8 -> parseExt(data, pos, fieldName, 16);

            // str 8/16/32
            case 0xD9 -> scalar(fieldName, "str8",  readUtf8(data, pos, readUint8(data, pos)));
            case 0xDA -> scalar(fieldName, "str16", readUtf8(data, pos, readUint16BE(data, pos)));
            case 0xDB -> scalar(fieldName, "str32", readUtf8(data, pos, (int) readUint32BE(data, pos)));

            // array 16/32
            case 0xDC -> parseArray(data, pos, fieldName, readUint16BE(data, pos), depth);
            case 0xDD -> parseArray(data, pos, fieldName, (int) readUint32BE(data, pos), depth);

            // map 16/32
            case 0xDE -> parseMap(data, pos, fieldName, readUint16BE(data, pos), depth);
            case 0xDF -> parseMap(data, pos, fieldName, (int) readUint32BE(data, pos), depth);

            default -> scalar(fieldName, "unknown(0x" + Integer.toHexString(fb) + ")", "<?>");
        };
    }

    // ── Container parsers ────────────────────────────────────────────────

    private BinaryNode parseArray(byte[] data, int[] pos, String fieldName, int count, int depth)
            throws BinaryParseException {
        BinaryNode node = new BinaryNode(BinaryNode.Format.MSGPACK, BinaryNode.Kind.ARRAY, fieldName, "Array/" + count);
        for (int i = 0; i < count; i++)
            node.addChild(parseValue(data, pos, "[" + i + "]", depth + 1));
        return node;
    }

    private BinaryNode parseMap(byte[] data, int[] pos, String fieldName, int count, int depth)
            throws BinaryParseException {
        BinaryNode node = new BinaryNode(BinaryNode.Format.MSGPACK, BinaryNode.Kind.MAP, fieldName, "Map/" + count);
        for (int i = 0; i < count; i++) {
            BinaryNode key = parseValue(data, pos, null, depth + 1);
            BinaryNode val = parseValue(data, pos, null, depth + 1);
            // Use key's value as fieldName for the value node for nicer display
            if (key.getValue() != null)
                val.setFieldName(key.getValue().replace("\"", ""));
            node.addMapEntry(key, val);
            node.addChild(val); // also add as child for simple tree traversal
        }
        return node;
    }

    private BinaryNode parseBin(byte[] data, int[] pos, String fieldName, int len)
            throws BinaryParseException {
        byte[] bytes = readExact(data, pos, len);
        BinaryNode n = new BinaryNode(BinaryNode.Format.MSGPACK, BinaryNode.Kind.BINARY, fieldName, "bin/" + len);
        n.setValue(Base64.getEncoder().encodeToString(bytes) + "  (" + len + " bytes)");
        n.setRawBytes(bytes);
        return n;
    }

    private BinaryNode parseExt(byte[] data, int[] pos, String fieldName, int len)
            throws BinaryParseException {
        int extType = (byte) readUint8(data, pos); // signed type code
        byte[] bytes = readExact(data, pos, len);
        BinaryNode n = new BinaryNode(BinaryNode.Format.MSGPACK, BinaryNode.Kind.BINARY, fieldName,
                "ext(type=" + extType + ")/" + len);
        n.setValue(BsonParser.toHex(bytes));
        n.setRawBytes(bytes);
        return n;
    }

    // ── Low-level readers ────────────────────────────────────────────────

    private static BinaryNode scalar(String fieldName, String typeName, String value) {
        BinaryNode n = new BinaryNode(BinaryNode.Format.MSGPACK, BinaryNode.Kind.SCALAR, fieldName, typeName);
        n.setValue(value);
        return n;
    }

    private static int readUint8(byte[] data, int[] pos) throws BinaryParseException {
        if (pos[0] >= data.length) throw new BinaryParseException(NAME, "EOF at offset " + pos[0]);
        return data[pos[0]++] & 0xFF;
    }

    private static int readUint16BE(byte[] data, int[] pos) throws BinaryParseException {
        byte[] b = readExact(data, pos, 2);
        return ((b[0] & 0xFF) << 8) | (b[1] & 0xFF);
    }

    private static long readUint32BE(byte[] data, int[] pos) throws BinaryParseException {
        byte[] b = readExact(data, pos, 4);
        return ((long)(b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16)
             | ((b[2] & 0xFF) << 8)  |  (b[3] & 0xFF);
    }

    private static byte[] readExact(byte[] data, int[] pos, int len) throws BinaryParseException {
        if (pos[0] + len > data.length)
            throw new BinaryParseException(NAME,
                    "Need " + len + " bytes at offset " + pos[0] + " but only "
                    + (data.length - pos[0]) + " remain");
        byte[] out = new byte[len];
        System.arraycopy(data, pos[0], out, 0, len);
        pos[0] += len;
        return out;
    }

    private static String readUtf8(byte[] data, int[] pos, int len) throws BinaryParseException {
        byte[] bytes = readExact(data, pos, len);
        return "\"" + new String(bytes, StandardCharsets.UTF_8) + "\"";
    }
}
