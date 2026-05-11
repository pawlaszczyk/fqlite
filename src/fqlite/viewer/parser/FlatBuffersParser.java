package fqlite.viewer.parser;

import fqlite.viewer.parser.BinaryNode;
import fqlite.viewer.parser.BinaryNode.Format;
import fqlite.viewer.parser.BinaryNode.Kind;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Heuristic schema-less FlatBuffers decoder.
 *
 * FlatBuffers spec: https://flatbuffers.dev/flatbuffers_internals.html
 *
 * A FlatBuffers buffer starts at byte 0 with a 4-byte little-endian offset
 * (the "root offset") that points to the root table object within the buffer.
 *
 * A Table object:
 *   offset → vtable_offset(soffset_t, 4 bytes)
 *   vtable: vtable_size(2) | object_size(2) | field_offset[0..N](2 each)
 *
 * Because we have no schema we can only decode structural metadata
 * (field count, offsets) plus a few common scalar interpretations.
 * String objects (4-byte length prefix + UTF-8 bytes) and nested Tables
 * are decoded recursively.
 *
 * Vectors (arrays) are also decoded: 4-byte count followed by inline elements.
 *
 * Depth limit: 20 to guard against malformed/circular structures.
 */
public class FlatBuffersParser {

    private static final String NAME      = "FlatBuffers";
    private static final int    MAX_DEPTH = 20;

    // ── Public API ───────────────────────────────────────────────────────

    public BinaryNode parse(byte[] data) throws BinaryParseException {
        if (data == null || data.length < 8)
            throw new BinaryParseException(NAME, "Data too short (min 8 bytes)");

        ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int rootOffset = bb.getInt(0);

        if (rootOffset < 4 || rootOffset >= data.length)
            throw new BinaryParseException(NAME,
                    "Root offset " + rootOffset + " is out of bounds");

        return decodeTable(bb, rootOffset, "root", 0);
    }

    // ── Table decoder ────────────────────────────────────────────────────

    private BinaryNode decodeTable(ByteBuffer bb, int tablePos, String fieldName, int depth)
            throws BinaryParseException {

        if (depth > MAX_DEPTH)
            throw new BinaryParseException(NAME, "Nesting depth exceeds " + MAX_DEPTH);
        if (tablePos < 0 || tablePos + 4 > bb.limit())
            throw new BinaryParseException(NAME, "Table offset " + tablePos + " out of range");

        // The soffset_t at tablePos points backwards to the vtable
        int soffset = bb.getInt(tablePos);
        int vtablePos = tablePos - soffset; // vtable is at tablePos - soffset

        if (vtablePos < 0 || vtablePos + 4 > bb.limit())
            throw new BinaryParseException(NAME,
                    "VTable offset " + vtablePos + " out of range");

        int vtableSize  = bb.getShort(vtablePos)      & 0xFFFF;
        int objectSize  = bb.getShort(vtablePos + 2)  & 0xFFFF;
        int fieldCount  = (vtableSize - 4) / 2;

        BinaryNode table = new BinaryNode(Format.FLATBUFFERS, Kind.DOCUMENT, fieldName,
                "Table[" + fieldCount + " fields, obj=" + objectSize + "B]");

        for (int i = 0; i < fieldCount; i++) {
            int offsetEntry = vtablePos + 4 + i * 2;
            if (offsetEntry + 2 > bb.limit()) break;
            int fieldOffset = bb.getShort(offsetEntry) & 0xFFFF;

            String fName = "field_" + i;

            if (fieldOffset == 0) {
                // Field not present (default value)
                BinaryNode absent = new BinaryNode(Format.FLATBUFFERS, Kind.SCALAR,
                        fName, "absent");
                absent.setValue("<default>");
                table.addChild(absent);
                continue;
            }

            int absPos = tablePos + fieldOffset;
            if (absPos < 0 || absPos >= bb.limit()) {
                BinaryNode err = new BinaryNode(Format.FLATBUFFERS, Kind.UNKNOWN, fName, "error");
                err.setValue("<offset out of range: " + absPos + ">");
                table.addChild(err);
                continue;
            }

            BinaryNode child = decodeField(bb, absPos, fName, depth);
            table.addChild(child);
        }

        return table;
    }

    // ── Field heuristic decoder ──────────────────────────────────────────

    /**
     * At {@code absPos} we don't know the type without a schema.
     * We try the following heuristics in order:
     *   1. Indirect offset → string
     *   2. Indirect offset → nested table
     *   3. Inline 4-byte signed int
     *   4. Inline 4-byte float
     *   5. Inline 8-byte long
     *   6. Inline 1-byte bool/byte
     *   7. Vector (4-byte count + elements)
     * The result may be wrong, but it gives useful hints.
     */
    private BinaryNode decodeField(ByteBuffer bb, int absPos, String fieldName, int depth) {

        if (absPos + 4 > bb.limit()) {
            return leaf(fieldName, "byte",
                    absPos < bb.limit() ? String.valueOf(bb.get(absPos) & 0xFF) : "<eof>");
        }

        // Read the raw 4-byte value at this position
        int raw32 = bb.getInt(absPos);

        // Check if this is an offset (uoffset_t) pointing to a string
        int targetPos = absPos + raw32;
        if (raw32 > 0 && raw32 < bb.limit() && targetPos >= 0 && targetPos + 4 < bb.limit()) {

            // Try string: 4-byte length prefix + UTF-8 bytes
            int strLen = bb.getInt(targetPos);
            if (strLen >= 0 && strLen < 65536
                    && targetPos + 4 + strLen < bb.limit()
                    && targetPos + 4 + strLen + 1 <= bb.limit()) {
                byte[] strBytes = new byte[strLen];
                bb.position(targetPos + 4);
                bb.get(strBytes);
                String s = tryUtf8(strBytes);
                if (s != null) return leaf(fieldName, "String", "\"" + s + "\"");
            }

            // Try nested table: the soffset at targetPos should point back to a vtable
            try {
                return decodeTable(bb, targetPos, fieldName, depth + 1);
            } catch (BinaryParseException ignored) {}

            // Try vector
            try {
                return decodeVector(bb, targetPos, fieldName, depth);
            } catch (BinaryParseException ignored) {}
        }

        // Inline scalar heuristics
        // 4-byte int
        BinaryNode n = leaf(fieldName, "int32/float32",
                raw32 + "  /  " + Float.intBitsToFloat(raw32));
        return n;
    }

    private BinaryNode decodeVector(ByteBuffer bb, int vecPos, String fieldName, int depth)
            throws BinaryParseException {
        if (vecPos + 4 > bb.limit())
            throw new BinaryParseException(NAME, "Vector position out of range");

        int count = bb.getInt(vecPos);
        if (count < 0 || count > 10_000)
            throw new BinaryParseException(NAME, "Unreasonable vector count: " + count);

        BinaryNode vec = new BinaryNode(Format.FLATBUFFERS, Kind.ARRAY, fieldName,
                "Vector[" + count + "]");

        int elemPos = vecPos + 4;
        for (int i = 0; i < Math.min(count, 256); i++) { // cap at 256 elements in tree
            if (elemPos + 4 > bb.limit()) break;
            BinaryNode elem = decodeField(bb, elemPos, "[" + i + "]", depth + 1);
            vec.addChild(elem);
            elemPos += 4; // assume 4-byte elements (most common)
        }
        if (count > 256) {
            BinaryNode more = leaf("[…]", "truncated", "… " + (count - 256) + " more elements");
            vec.addChild(more);
        }
        return vec;
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private static BinaryNode leaf(String fieldName, String typeName, String value) {
        BinaryNode n = new BinaryNode(Format.FLATBUFFERS, Kind.SCALAR, fieldName, typeName);
        n.setValue(value);
        return n;
    }

    private static String tryUtf8(byte[] bytes) {
        if (bytes.length == 0) return "";
        try {
            java.nio.charset.CharsetDecoder dec = StandardCharsets.UTF_8.newDecoder();
            String s = dec.decode(ByteBuffer.wrap(bytes)).toString();
            long printable = s.chars()
                    .filter(c -> c >= 0x20 || c == '\n' || c == '\r' || c == '\t')
                    .count();
            return (printable >= s.length() * 0.85) ? s : null;
        } catch (Exception e) { return null; }
    }
}
