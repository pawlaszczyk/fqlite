package fqlite.viewer.parser;

import fqlite.viewer.model.ProtoField;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Schema-less Protobuf binary decoder.
 *
 * Works purely on wire types without any .proto file.
 * Heuristically tries to interpret length-delimited fields as:
 *   1. UTF-8 string
 *   2. Nested message (recursive)
 *   3. Raw bytes (hex)
 *
 * All integer fields get multiple interpretations (signed/unsigned/zigzag/bool).
 */
public class ProtobufParser {

    /** Maximum nesting depth to prevent infinite recursion on corrupt data. */
    private static final int MAX_DEPTH = 20;

    // ──────────────────────────────────────────────────────────────────────
    // Public API
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Parse the entire byte array as a Protobuf message body.
     *
     * @param data  raw bytes of the .pb file
     * @return list of top-level fields
     * @throws ProtobufParseException if the data is not valid protobuf
     */
    public List<ProtoField> parse(byte[] data) throws ProtobufParseException {
        return parseMessage(data, 0, data.length, 0);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Core recursive parser
    // ──────────────────────────────────────────────────────────────────────

    private List<ProtoField> parseMessage(byte[] data, int start, int end, int depth)
            throws ProtobufParseException {

        List<ProtoField> fields = new ArrayList<>();
        int pos = start;

        while (pos < end) {
            int tagStart = pos;

            // ── Read the tag (field_number << 3 | wire_type) ──────────────
            VarIntResult tagResult = readVarint(data, pos, end);
            if (tagResult == null) break;
            pos = tagResult.nextPos();

            long tag       = tagResult.value();
            int wireType   = (int)(tag & 0x7);
            int fieldNum   = (int)(tag >>> 3);

            if (fieldNum == 0) {
                throw new ProtobufParseException("Invalid field number 0 at offset " + tagStart);
            }

            // ── Decode value based on wire type ───────────────────────────
            switch (wireType) {

                case ProtoField.WIRE_VARINT -> {
                    VarIntResult valResult = readVarint(data, pos, end);
                    if (valResult == null)
                        throw new ProtobufParseException("Truncated varint at offset " + pos);

                    byte[] raw = extractBytes(data, pos, valResult.nextPos());
                    pos = valResult.nextPos();

                    ProtoField field = new ProtoField(fieldNum, wireType, tagStart, raw);
                    interpretVarint(field, valResult.value());
                    fields.add(field);
                }

                case ProtoField.WIRE_64BIT -> {
                    if (pos + 8 > end)
                        throw new ProtobufParseException("Truncated 64-bit field at offset " + pos);

                    byte[] raw = extractBytes(data, pos, pos + 8);
                    pos += 8;

                    ProtoField field = new ProtoField(fieldNum, wireType, tagStart, raw);
                    interpret64Bit(field, raw);
                    fields.add(field);
                }

                case ProtoField.WIRE_LEN -> {
                    VarIntResult lenResult = readVarint(data, pos, end);
                    if (lenResult == null)
                        throw new ProtobufParseException("Truncated length prefix at offset " + pos);
                    pos = lenResult.nextPos();

                    int len = (int) lenResult.value();
                    if (len < 0 || pos + len > end)
                        throw new ProtobufParseException(
                                "Length-delimited field claims " + len + " bytes at offset " + pos
                                        + " but only " + (end - pos) + " bytes remain");

                    byte[] raw = extractBytes(data, pos, pos + len);
                    pos += len;

                    ProtoField field = new ProtoField(fieldNum, wireType, tagStart, raw);
                    interpretLenDelim(field, raw, depth);
                    fields.add(field);
                }

                case ProtoField.WIRE_32BIT -> {
                    if (pos + 4 > end)
                        throw new ProtobufParseException("Truncated 32-bit field at offset " + pos);

                    byte[] raw = extractBytes(data, pos, pos + 4);
                    pos += 4;

                    ProtoField field = new ProtoField(fieldNum, wireType, tagStart, raw);
                    interpret32Bit(field, raw);
                    fields.add(field);
                }

                // Deprecated group types – skip gracefully
                case ProtoField.WIRE_SGROUP, ProtoField.WIRE_EGROUP -> {
                    ProtoField field = new ProtoField(fieldNum, wireType, tagStart, new byte[0]);
                    field.setInterpretedValue("<group – deprecated wire type>");
                    fields.add(field);
                }

                default -> throw new ProtobufParseException(
                        "Unknown wire type " + wireType + " at offset " + tagStart);
            }
        }

        return fields;
    }

    // ──────────────────────────────────────────────────────────────────────
    // Interpretation helpers
    // ──────────────────────────────────────────────────────────────────────

    private void interpretVarint(ProtoField field, long raw) {
        // Primary: show as unsigned decimal
        field.setInterpretedValue(Long.toUnsignedString(raw));

        // Alternatives
        field.addAlternative(new ProtoField.InterpretedAs("uint64",  Long.toUnsignedString(raw)));
        field.addAlternative(new ProtoField.InterpretedAs("int64",   String.valueOf((long) raw)));
        field.addAlternative(new ProtoField.InterpretedAs("sint64",  String.valueOf(zigzagDecode64(raw))));
        field.addAlternative(new ProtoField.InterpretedAs("uint32",  Long.toUnsignedString(raw & 0xFFFFFFFFL)));
        field.addAlternative(new ProtoField.InterpretedAs("int32",   String.valueOf((int) raw)));
        field.addAlternative(new ProtoField.InterpretedAs("sint32",  String.valueOf(zigzagDecode32((int) raw))));
        field.addAlternative(new ProtoField.InterpretedAs("bool",    raw != 0 ? "true" : "false"));
        // Timestamp heuristic: values between 1_000_000_000 and 9_999_999_999 → Unix seconds
        if (raw >= 1_000_000_000L && raw <= 9_999_999_999L) {
            field.addAlternative(new ProtoField.InterpretedAs("unix-timestamp",
                    new java.util.Date(raw * 1000).toString()));
        }
        // Millis heuristic
        if (raw >= 1_000_000_000_000L && raw <= 9_999_999_999_999L) {
            field.addAlternative(new ProtoField.InterpretedAs("unix-millis",
                    new java.util.Date(raw).toString()));
        }
    }

    private void interpret64Bit(ProtoField field, byte[] raw) {
        ByteBuffer bb = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
        double  d  = bb.getDouble(0);
        long    l  = bb.getLong(0);
        long    ul = l; // reinterpret

        field.setInterpretedValue("0x" + Long.toHexString(l).toUpperCase());

        field.addAlternative(new ProtoField.InterpretedAs("double",   String.valueOf(d)));
        field.addAlternative(new ProtoField.InterpretedAs("fixed64",  Long.toUnsignedString(ul)));
        field.addAlternative(new ProtoField.InterpretedAs("sfixed64", String.valueOf(l)));
        field.addAlternative(new ProtoField.InterpretedAs("hex",      "0x" + Long.toHexString(l).toUpperCase()));
    }

    private void interpret32Bit(ProtoField field, byte[] raw) {
        ByteBuffer bb = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
        float   f  = bb.getFloat(0);
        int     i  = bb.getInt(0);

        field.setInterpretedValue("0x" + Integer.toHexString(i).toUpperCase());

        field.addAlternative(new ProtoField.InterpretedAs("float",    String.valueOf(f)));
        field.addAlternative(new ProtoField.InterpretedAs("fixed32",  Integer.toUnsignedString(i)));
        field.addAlternative(new ProtoField.InterpretedAs("sfixed32", String.valueOf(i)));
        field.addAlternative(new ProtoField.InterpretedAs("hex",      "0x" + Integer.toHexString(i).toUpperCase()));
    }

    private void interpretLenDelim(ProtoField field, byte[] raw, int depth) {
        if (raw.length == 0) {
            field.setInterpretedValue("<empty>");
            return;
        }

        // 1. Try UTF-8 string first
        String str = tryUtf8(raw);
        if (str != null) {
            field.setInterpretedValue(str);
            field.addAlternative(new ProtoField.InterpretedAs("string", str));
        }

        // 2. Try nested message (recursive) – but only if not too deep
        if (depth < MAX_DEPTH && raw.length >= 2) {
            try {
                List<ProtoField> children = parseMessage(raw, 0, raw.length, depth + 1);
                if (!children.isEmpty()) {
                    for (ProtoField c : children) field.addChild(c);
                    field.setNestedMessage(true);
                    if (str == null) {
                        field.setInterpretedValue("<nested message – " + children.size() + " field(s)>");
                    } else {
                        field.addAlternative(new ProtoField.InterpretedAs(
                                "nested-message", "<" + children.size() + " field(s)>"));
                    }
                }
            } catch (ProtobufParseException ignored) {
                // Not a valid nested message
            }
        }

        // 3. Always offer hex
        field.addAlternative(new ProtoField.InterpretedAs("bytes (hex)", toHex(raw)));
        field.addAlternative(new ProtoField.InterpretedAs("bytes (base64)",
                java.util.Base64.getEncoder().encodeToString(raw)));

        if (str == null && !field.isNestedMessage()) {
            field.setInterpretedValue(toHex(raw));
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // Low-level byte helpers
    // ──────────────────────────────────────────────────────────────────────

    /** Read a base-128 varint. Returns null if data is exhausted immediately. */
    private VarIntResult readVarint(byte[] data, int pos, int end) {
        if (pos >= end) return null;
        long value = 0;
        int  shift = 0;
        while (pos < end) {
            int b = data[pos++] & 0xFF;
            value |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return new VarIntResult(value, pos);
            shift += 7;
            if (shift >= 64) throw new IllegalStateException("Varint too long");
        }
        return null; // truncated
    }

    private byte[] extractBytes(byte[] data, int from, int to) {
        byte[] out = new byte[to - from];
        System.arraycopy(data, from, out, 0, out.length);
        return out;
    }

    /** Returns the UTF-8 string if the bytes are valid UTF-8 and printable, else null. */
    private String tryUtf8(byte[] raw) {
        try {
            CharsetDecoder dec = StandardCharsets.UTF_8.newDecoder();
            String s = dec.decode(ByteBuffer.wrap(raw)).toString();
            // Reject strings that are mostly control characters
            long printable = s.chars().filter(c -> c >= 0x20 || c == '\n' || c == '\r' || c == '\t').count();
            if (printable >= s.length() * 0.8 && !s.isEmpty()) {
                return s;
            }
            return null;
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    public static String toHex(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return "";
        StringBuilder sb = new StringBuilder(bytes.length * 3);
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString().trim();
    }

    /** ZigZag decode for 64-bit values. */
    private static long zigzagDecode64(long n) {
        return (n >>> 1) ^ -(n & 1);
    }

    /** ZigZag decode for 32-bit values. */
    private static int zigzagDecode32(int n) {
        return (n >>> 1) ^ -(n & 1);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Inner types
    // ──────────────────────────────────────────────────────────────────────

    private record VarIntResult(long value, int nextPos) {}
}
