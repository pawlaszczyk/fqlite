package fqlite.viewer.parser;

import fqlite.util.BinaryFormatDetector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Detects the format of a raw binary (or text) file by inspecting magic bytes
 * and heuristics.
 *
 * Detection order
 * ───────────────
 * 1.  PNG             - magic: 89 50 4E 47 0D 0A 1A 0A
 * 2.  GIF             - magic: "GIF8" (GIF87a oder GIF89a)
 * 3.  BMP             - magic: BMP: "BM"
 * 4.  JPEG            - magic: FF D8 FF
 * 5.  TIFF            - TIFF Little-Endian (II): 49 49 2A 00
 * 6.  HEIC            - "ftyp" at Offset 4, afterward Sub-Type at Offset 8
 * 7.  PDF             - PDF: "%PDF"
 * 8.  GZIP            - GZIP: 1F 8B
 * 9.  BPList          – magic "bplist"
 * 10. XML             – starts with '<' (after BOM strip)
 * 11. JSON            – first significant char is '{' or '['
 * 12. JavaSerial      - Magic 0xACED + version 0x0005
 * 12. BSON            – 4-byte LE doc-size matches total length, ends with 0x00
 * 13. MessagePack     – first byte in valid fixint / fixmap / fixarray / named-type range
 * 14. Thrift Compact  – first byte 0x82 (compact protocol id)
 * 15. Thrift Binary   – first byte is a valid Thrift Binary field type (0x02–0x0F)
 * 16. FlatBuffers     – root offset plausible and vtable magic checks out
 * 17. Base64          – content looks like Base64; inner bytes checked recursively (1–8)
 * 18. Protobuf        – heuristic: first byte encodes valid tag
 * 19. UNKNOWN
 */
public class FormatDetector {

    private FormatDetector() {}

    public static Format detect(byte[] data) {

        if (data == null || data.length == 0) return Format.UNKNOWN;

        if (isPNG(data)) return Format.PNG;
        if (isGIF(data)) return Format.GIF;
        if (isBMP(data)) return Format.BMP;
        if (isJPEG(data)) return Format.JPEG;
        if (isTIFF(data)) return Format.TIFF;
        if (isHEIC(data)) return Format.HEIC;
        if (isPDF(data))  return Format.PDF;
        if (isGZIP(data)) return Format.GZIP;
        if (isBPList(data))        return Format.BPLIST;
        if (isXml(data))           return Format.XML;
        if (isJson(data))          return Format.JSON;
        if (isJavaSerial(data))    return Format.JAVA_SERIAL;

        if (isPlainText(data,512)) return Format.PLAINTEXT;

        Base64Detector.Result b64 = Base64Detector.detect(data);

        if (b64.isBase64()) {
            byte[] inner = b64.decodedBytes();
            if (isBPList(inner))        return Format.BASE64_BPLIST;
            if (isXml(inner))           return Format.BASE64_XML;
            if (isJson(inner))          return Format.BASE64_JSON;
            if (isJavaSerial(inner))    return Format.BASE64_JAVA_SERIAL;
            if (isProtobuf(inner))      return Format.BASE64_PROTO;
            if (isBson(inner))          return Format.BASE64_BSON;
            if (isThriftCompact(inner)) return Format.BASE64_THRIFT_COMPACT;
            if (isFlatBuffers(inner))   return Format.BASE64_FLATBUFFERS;
            if (isMsgPack(inner))       return Format.BASE64_MSGPACK;
            if (isThriftBinary(inner))  return Format.BASE64_THRIFT_BINARY;
            if (isThriftCompactStruct(inner)) return Format.BASE64_THRIFT_COMPACT;
        }


        if (isProtobuf(data)) return Format.PROTOBUF;
        if (isBson(data))          return Format.BSON;
        // Thrift Compact before MsgPack: 0x82 is also a valid MsgPack fixmap byte
        if (isThriftCompact(data)) return Format.THRIFT_COMPACT;
        // FlatBuffers before MsgPack/Thrift: uses a strong structural check
        if (isFlatBuffers(data))   return Format.FLATBUFFERS;
        // MsgPack before Thrift Binary: uses multi-byte lookahead
        if (isMsgPack(data))       return Format.MSGPACK;
        if (isThriftBinary(data))  return Format.THRIFT_BINARY;
        // Bare Thrift Compact struct (no 0x82 envelope) – checked last because
        // it overlaps with Protobuf; a clean STOP-terminated walk is stronger evidence.
        if (isThriftCompactStruct(data)) return Format.THRIFT_COMPACT;
        return Format.UNKNOWN;
    }

    private static boolean isJavaSerial(byte[] data) {
        // Magic 0xACED + version 0x0005 – unambiguous 4-byte signature
        return data.length >= 4
                && (data[0] & 0xFF) == 0xAC
                && (data[1] & 0xFF) == 0xED
                && (data[2] & 0xFF) == 0x00
                && (data[3] & 0xFF) == 0x05;
    }

    private static boolean isBPList(byte[] d) {
        return d.length >= 6 && d[0]=='b' && d[1]=='p' && d[2]=='l' && d[3]=='i' && d[4]=='s' && d[5]=='t';
    }

    private static boolean isXml(byte[] data) {
        int start = 0;
        if (data.length >= 3 && (data[0]&0xFF)==0xEF && (data[1]&0xFF)==0xBB && (data[2]&0xFF)==0xBF) start=3;
        if (data.length >= 2 && (data[0]&0xFF)==0xFE && (data[1]&0xFF)==0xFF) start=2;
        int i = start;
        while (i < data.length && data[i] <= 0x20) i++;
        if (i >= data.length || data[i] != '<') return false;
        if (i + 1 >= data.length) return false;
        char next = (char)(data[i+1] & 0xFF);
        return next=='?' || next=='!' || Character.isLetter(next);
    }

    private static boolean isJson(byte[] data) {
        int len = Math.min(data.length, 1024);
        String s;
        try { s = new String(data, 0, len, StandardCharsets.UTF_8).strip(); } catch (Exception e) { return false; }
        if (s.isEmpty()) return false;
        char first = s.charAt(0);
        if (first != '{' && first != '[') return false;
        return s.indexOf(first == '{' ? '}' : ']') > 0;
    }

    private static boolean isBson(byte[] data) {
        if (data.length < 5) return false;
        int declared = ByteBuffer.wrap(data, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        return declared == data.length && data[data.length-1] == 0x00 && declared >= 5;
    }

    /**
     * MessagePack detection with strict multi-byte lookahead.
     *
     * Requirements (all must hold):
     *   1. First byte is a valid MsgPack format code (not 0xC1).
     *   2. The first complete value can be skipped without running past EOF.
     *   3. Either:
     *      a. A second value immediately follows (strong signal), OR
     *      b. The first value is a non-trivial container (map/array with ≥1
     *         element) or typed scalar that fills the entire buffer.
     *   4. A bare single-byte positive fixint (0x00–0x7F) is never accepted
     *      alone – it overlaps too heavily with other formats.
     */
    private static boolean isMsgPack(byte[] data) {
        if (data.length < 2) return false;

        int b0 = data[0] & 0xFF;
        if (b0 == 0xC1) return false;

        // Positive fixint alone is too ambiguous – reject
        if (b0 <= 0x7F) return false;

        // Negative fixint alone is too ambiguous – reject
        if (b0 >= 0xE0 && data.length < 3) return false;

        try {
            int[] pos = {0};
            int consumed = skipMsgPackValue(data, pos);
            if (consumed < 0) return false;

            // First value exactly fills the buffer
            if (pos[0] == data.length) {
                // Accept only if the first byte indicates a real typed value,
                // not a plain fixint/fixstr that could be anything
                return b0 >= 0x80 && b0 <= 0xBF   // fixmap / fixarray / fixstr
                    || b0 >= 0xC0 && b0 <= 0xDF    // named types (nil, bool, bin, ext, float, uint, int, fixext, str)
                    || b0 >= 0xDC && b0 <= 0xDF;   // array16/32, map16/32
            }

            // A second value starts right after – verify it too
            if (pos[0] < data.length) {
                int b1 = data[pos[0]] & 0xFF;
                if (b1 == 0xC1) return false;
                int[] pos2 = {pos[0]};
                int consumed2 = skipMsgPackValue(data, pos2);
                return consumed2 >= 0;
            }

        } catch (Exception ignored) {}

        return false;
    }

    /**
     * Attempts to skip one MsgPack value starting at {@code pos[0]}.
     * Updates {@code pos[0]} to point past the value.
     * Returns the number of bytes consumed, or -1 on error.
     *
     * This is intentionally a lightweight skip (not a full parse) –
     * it validates structure without allocating objects.
     */
    private static int skipMsgPackValue(byte[] data, int[] pos) {
        if (pos[0] >= data.length) return -1;
        int start = pos[0];
        int b = data[pos[0]++] & 0xFF;

        if (b == 0xC1) return -1;           // never-used → invalid

        // positive fixint 0x00–0x7F: 1 byte total – already consumed
        if (b <= 0x7F) return 1;

        // negative fixint 0xE0–0xFF: 1 byte total
        if (b >= 0xE0) return 1;

        // fixmap 0x80–0x8F: 1 + 2*N values
        if (b <= 0x8F) {
            int n = b & 0x0F;
            for (int i = 0; i < n * 2; i++)
                if (skipMsgPackValue(data, pos) < 0) return -1;
            return pos[0] - start;
        }

        // fixarray 0x90–0x9F: 1 + N values
        if (b <= 0x9F) {
            int n = b & 0x0F;
            for (int i = 0; i < n; i++)
                if (skipMsgPackValue(data, pos) < 0) return -1;
            return pos[0] - start;
        }

        // fixstr 0xA0–0xBF: 1 + len bytes
        if (b <= 0xBF) {
            int len = b & 0x1F;
            if (pos[0] + len > data.length) return -1;
            pos[0] += len;
            return pos[0] - start;
        }

        return switch (b) {
            case 0xC0 -> 1;  // nil
            case 0xC2, 0xC3 -> 1;  // false, true
            // bin 8/16/32
            case 0xC4 -> { int l = readU8(data, pos);  if (l < 0 || pos[0]+l>data.length) yield -1; pos[0]+=l; yield pos[0]-start; }
            case 0xC5 -> { int l = readU16(data, pos); if (l < 0 || pos[0]+l>data.length) yield -1; pos[0]+=l; yield pos[0]-start; }
            case 0xC6 -> { int l = readU32(data, pos); if (l < 0 || pos[0]+l>data.length) yield -1; pos[0]+=l; yield pos[0]-start; }
            // ext 8/16/32 (+1 for type byte)
            case 0xC7 -> { int l = readU8(data, pos);  if (l < 0 || pos[0]+1+l>data.length) yield -1; pos[0]+=1+l; yield pos[0]-start; }
            case 0xC8 -> { int l = readU16(data, pos); if (l < 0 || pos[0]+1+l>data.length) yield -1; pos[0]+=1+l; yield pos[0]-start; }
            case 0xC9 -> { int l = readU32(data, pos); if (l < 0 || pos[0]+1+l>data.length) yield -1; pos[0]+=1+l; yield pos[0]-start; }
            // float32/64
            case 0xCA -> { if (pos[0]+4>data.length) yield -1; pos[0]+=4; yield pos[0]-start; }
            case 0xCB -> { if (pos[0]+8>data.length) yield -1; pos[0]+=8; yield pos[0]-start; }
            // uint 8/16/32/64
            case 0xCC -> { if (pos[0]+1>data.length) yield -1; pos[0]+=1; yield pos[0]-start; }
            case 0xCD -> { if (pos[0]+2>data.length) yield -1; pos[0]+=2; yield pos[0]-start; }
            case 0xCE -> { if (pos[0]+4>data.length) yield -1; pos[0]+=4; yield pos[0]-start; }
            case 0xCF -> { if (pos[0]+8>data.length) yield -1; pos[0]+=8; yield pos[0]-start; }
            // int 8/16/32/64
            case 0xD0 -> { if (pos[0]+1>data.length) yield -1; pos[0]+=1; yield pos[0]-start; }
            case 0xD1 -> { if (pos[0]+2>data.length) yield -1; pos[0]+=2; yield pos[0]-start; }
            case 0xD2 -> { if (pos[0]+4>data.length) yield -1; pos[0]+=4; yield pos[0]-start; }
            case 0xD3 -> { if (pos[0]+8>data.length) yield -1; pos[0]+=8; yield pos[0]-start; }
            // fixext 1/2/4/8/16 (+1 type byte)
            case 0xD4 -> { if (pos[0]+2>data.length)  yield -1; pos[0]+=2;  yield pos[0]-start; }
            case 0xD5 -> { if (pos[0]+3>data.length)  yield -1; pos[0]+=3;  yield pos[0]-start; }
            case 0xD6 -> { if (pos[0]+5>data.length)  yield -1; pos[0]+=5;  yield pos[0]-start; }
            case 0xD7 -> { if (pos[0]+9>data.length)  yield -1; pos[0]+=9;  yield pos[0]-start; }
            case 0xD8 -> { if (pos[0]+17>data.length) yield -1; pos[0]+=17; yield pos[0]-start; }
            // str 8/16/32
            case 0xD9 -> { int l = readU8(data, pos);  if (l < 0 || pos[0]+l>data.length) yield -1; pos[0]+=l; yield pos[0]-start; }
            case 0xDA -> { int l = readU16(data, pos); if (l < 0 || pos[0]+l>data.length) yield -1; pos[0]+=l; yield pos[0]-start; }
            case 0xDB -> { int l = readU32(data, pos); if (l < 0 || pos[0]+l>data.length) yield -1; pos[0]+=l; yield pos[0]-start; }
            // array 16/32
            case 0xDC -> {
                int n = readU16(data, pos); if (n < 0) yield -1;
                for (int i = 0; i < n; i++) if (skipMsgPackValue(data, pos) < 0) yield -1;
                yield pos[0] - start;
            }
            case 0xDD -> {
                int n = readU32(data, pos); if (n < 0) yield -1;
                for (int i = 0; i < n; i++) if (skipMsgPackValue(data, pos) < 0) yield -1;
                yield pos[0] - start;
            }
            // map 16/32
            case 0xDE -> {
                int n = readU16(data, pos); if (n < 0) yield -1;
                for (int i = 0; i < n * 2; i++) if (skipMsgPackValue(data, pos) < 0) yield -1;
                yield pos[0] - start;
            }
            case 0xDF -> {
                int n = readU32(data, pos); if (n < 0) yield -1;
                for (int i = 0; i < n * 2; i++) if (skipMsgPackValue(data, pos) < 0) yield -1;
                yield pos[0] - start;
            }
            default -> -1;
        };
    }

    // Read helpers for skipMsgPackValue (return -1 on EOF instead of throwing)
    private static int readU8(byte[] d, int[] pos) {
        if (pos[0] >= d.length) return -1;
        return d[pos[0]++] & 0xFF;
    }

    private static int readU16(byte[] d, int[] pos) {
        if (pos[0] + 2 > d.length) return -1;
        int v = ((d[pos[0]] & 0xFF) << 8) | (d[pos[0]+1] & 0xFF);
        pos[0] += 2; return v;
    }

    private static int readU32(byte[] d, int[] pos) {
        if (pos[0] + 4 > d.length) return -1;
        int v = ((d[pos[0]] & 0xFF) << 24) | ((d[pos[0]+1] & 0xFF) << 16)
              | ((d[pos[0]+2] & 0xFF) << 8)  |  (d[pos[0]+3] & 0xFF);
        pos[0] += 4;
        return v < 0 ? -1 : v;   // reject > 2 GB lengths
    }

    /**
     * Thrift Compact – two-tier detection:
     *
     * Tier 1 – Message envelope (most common serialised form):
     *   Byte 0 = 0x82 (protocol id = 2, version nibble)
     *   Byte 1 low 5 bits = 0x01 (compact version)
     *
     * Tier 2 – Bare struct (no message header, e.g. direct struct serialisation):
     *   Delegated to isThriftCompactStruct().
     */
    private static boolean isThriftCompact(byte[] data) {
        if (data.length < 2) return false;
        int b0 = data[0] & 0xFF;
        int b1 = data[1] & 0xFF;
        return b0 == 0x82 && (b1 & 0x1F) == 0x01;
    }

    /**
     * Thrift Compact bare-struct detection (no 0x82 message header).
     *
     * A Compact struct consists of a sequence of field headers followed by 0x00 (STOP).
     * Each field header is one byte: high nibble = field-id delta, low nibble = compact type.
     * Special case: delta == 0 means long-form header (0x00 type byte + 2-byte zigzag field_id).
     *
     * We walk through the buffer consuming field headers and their value payloads.
     * If we reach a 0x00 STOP byte cleanly, and we consumed at least one field, it is
     * very likely Thrift Compact.
     *
     * Compact type codes:
     *   1=BoolTrue  2=BoolFalse  3=Byte  4=i16  5=i32  6=i64
     *   7=Double    8=Binary     9=List  10=Set  11=Map  12=Struct
     */
    private static boolean isThriftCompactStruct(byte[] data) {
        if (data.length < 2) return false;

        // Must not start with 0x82 (that is the message-envelope form, handled above)
        if ((data[0] & 0xFF) == 0x82) return false;

        int pos       = 0;
        int lastField = 0;
        int fields    = 0;

        while (pos < data.length) {
            int header = data[pos++] & 0xFF;

            // STOP field – clean termination
            if (header == 0x00) return fields > 0;

            int delta       = (header >> 4) & 0x0F;
            int compactType = header & 0x0F;

            // Long-form: delta == 0 → next bytes are zigzag-encoded field_id (varint)
            if (delta == 0) {
                // read one varint for the field_id
                if (pos >= data.length) return false;
                while (pos < data.length && (data[pos] & 0x80) != 0) pos++;
                pos++; // consume final varint byte
                if (pos > data.length) return false;
            } else {
                lastField += delta;
            }

            // Compact type 0 is invalid in a field header (only valid as STOP)
            if (compactType == 0) return false;

            // Skip the value payload
            pos = skipCompactValue(data, pos, compactType);
            if (pos < 0) return false;

            fields++;
            // Safety: don't scan more than 64 fields
            if (fields > 64) return false;
        }

        // Ran off the end without STOP – only accept if we parsed at least 2 fields
        // (reduces false positives on data that starts with a valid-looking field header)
        return fields >= 2;
    }

    /**
     * Skip one Compact-encoded value of the given compact type.
     * Returns the new position, or -1 on error.
     */
    private static int skipCompactValue(byte[] data, int pos, int cType) {
        return switch (cType) {
            case 1, 2 -> pos;                                      // bool – no extra bytes
            case 3    -> pos + 1;                                  // byte
            case 4, 5, 6 -> {                                      // i16/i32/i64 – varint
                int p = pos;
                while (p < data.length && (data[p] & 0x80) != 0) p++;
                yield (p < data.length) ? p + 1 : -1;
            }
            case 7    -> pos + 8;                                  // double – 8 bytes LE
            case 8    -> {                                         // binary – varint length + bytes
                int p = pos;
                long len = 0; int shift = 0;
                while (p < data.length) {
                    int b = data[p++] & 0xFF;
                    len |= (long)(b & 0x7F) << shift;
                    if ((b & 0x80) == 0) break;
                    shift += 7;
                    if (shift > 28) yield -1;
                }
                yield (p + len <= data.length) ? (int)(p + len) : -1;
            }
            case 9, 10 -> {                                        // list / set – size+type byte + elements
                if (pos >= data.length) yield -1;
                int sizeType = data[pos] & 0xFF;
                int elemType = sizeType & 0x0F;
                int count;
                int p = pos + 1;
                if ((sizeType >> 4) == 0x0F) {                    // count is separate varint
                    long c = 0; int shift = 0;
                    while (p < data.length) {
                        int b = data[p++] & 0xFF;
                        c |= (long)(b & 0x7F) << shift;
                        if ((b & 0x80) == 0) break;
                        shift += 7;
                    }
                    count = (int) c;
                } else {
                    count = (sizeType >> 4) & 0x0F;
                }
                if (count > 10_000) yield -1;
                for (int i = 0; i < count; i++) {
                    p = skipCompactValue(data, p, elemType);
                    if (p < 0) yield -1;
                }
                yield p;
            }
            case 11 -> {                                           // map – count varint + kv-type byte + pairs
                if (pos >= data.length) yield -1;
                long cnt = 0; int shift = 0; int p = pos;
                while (p < data.length) {
                    int b = data[p++] & 0xFF;
                    cnt |= (long)(b & 0x7F) << shift;
                    if ((b & 0x80) == 0) break;
                    shift += 7;
                }
                if (cnt == 0) yield p;                             // empty map – no kv-type byte
                if (p >= data.length) yield -1;
                int kvType   = data[p++] & 0xFF;
                int keyType  = (kvType >> 4) & 0x0F;
                int valType  = kvType & 0x0F;
                if (cnt > 10_000) yield -1;
                for (long i = 0; i < cnt; i++) {
                    p = skipCompactValue(data, p, keyType);
                    if (p < 0) yield -1;
                    p = skipCompactValue(data, p, valType);
                    if (p < 0) yield -1;
                }
                yield p;
            }
            case 12 -> {                                           // nested struct – recurse by scanning to STOP
                int p = pos;
                int depth = 0;
                while (p < data.length && depth <= 32) {
                    int h = data[p++] & 0xFF;
                    if (h == 0x00) { if (depth == 0) yield p; depth--; continue; }
                    int ct = h & 0x0F;
                    if ((h >> 4) == 0) {                           // long-form field id
                        while (p < data.length && (data[p] & 0x80) != 0) p++;
                        p++;
                    }
                    if (ct == 12) { depth++; continue; }
                    p = skipCompactValue(data, p, ct);
                    if (p < 0) yield -1;
                }
                yield -1;
            }
            default -> -1;
        };
    }

    /**
     * Thrift Binary: first byte is a valid struct field type AND
     * bytes 1–2 form a plausible field_id (we accept any value but verify
     * that a value body of the expected size actually fits in the buffer).
     * Also accept 0x00 (STOP = empty struct) only when the buffer is exactly
     * 1 byte – that is the only unambiguous case.
     */
    private static boolean isThriftBinary(byte[] data) {
        if (data.length < 1) return false;
        int t = data[0] & 0xFF;
        if (t == 0x00) return data.length == 1; // bare STOP – empty struct
        if (t < 0x02 || t > 0x0F) return false;
        // Require at least the field_id (2 bytes) after the type byte
        if (data.length < 3) return false;
        // Minimum value body sizes per type
        int minBody = switch (t) {
            case 0x02 -> 1;  // bool
            case 0x03 -> 1;  // byte
            case 0x04 -> 8;  // double
            case 0x06 -> 2;  // int16
            case 0x08 -> 4;  // int32
            case 0x0A -> 8;  // int64
            case 0x0B -> 4;  // string (4-byte length prefix)
            case 0x0C -> 1;  // struct (at least the STOP byte)
            case 0x0D -> 6;  // map (key-type + val-type + 4-byte count)
            case 0x0E, 0x0F -> 5; // set/list (elem-type + 4-byte count)
            default -> 1;
        };
        return data.length >= 3 + minBody;
    }

    private static boolean isFlatBuffers(byte[] data) {
        if (data.length < 8) return false;
        ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int rootOffset = bb.getInt(0);
        if (rootOffset < 4 || rootOffset >= data.length - 4) return false;
        int soffset   = bb.getInt(rootOffset);
        int vtablePos = rootOffset - soffset;
        if (vtablePos < 0 || vtablePos + 4 >= data.length) return false;
        int vtableSize = bb.getShort(vtablePos) & 0xFFFF;
        return vtableSize >= 4 && vtableSize <= 1024 && (vtableSize % 2 == 0);
    }

    private static boolean isProtobuf(byte[] data) {
        if (data.length < 2) return false;
        int firstByte = data[0] & 0xFF;
        return (firstByte >> 3) > 0 && (firstByte & 0x07) <= 5;
    }

    public static boolean isBase64Wrapped(Format fmt) {
        return switch (fmt) {
            case BASE64_BPLIST, BASE64_XML, BASE64_JSON,
                 BASE64_BSON, BASE64_MSGPACK,
                 BASE64_THRIFT_BINARY, BASE64_THRIFT_COMPACT,
                 BASE64_FLATBUFFERS, BASE64_JAVA_SERIAL, BASE64_PROTO -> true;
            default -> false;
        };
    }

    public static boolean isPNG(byte[] data) {
        // PNG: 89 50 4E 47 0D 0A 1A 0A
        return data.length >= 8 && matchesAt(data, 0,
                (byte) 0x89, (byte) 0x50, (byte) 0x4E, (byte) 0x47,
                (byte) 0x0D, (byte) 0x0A, (byte) 0x1A, (byte) 0x0A);
    }

    public static boolean isGIF(byte[] data) {
        // GIF: "GIF8" (GIF87a oder GIF89a)
        return data.length >= 4 && matchesAt(data, 0,
                (byte) 0x47, (byte) 0x49, (byte) 0x46, (byte) 0x38);
    }

    public static boolean isBMP(byte[] data)
    {
        // BMP: "BM"
        return matchesAt(data, 0, (byte) 0x42, (byte) 0x4D);
    }

    public static boolean isJPEG(byte[] data) {
        // JPEG: FF D8 FF
        return data.length >= 3 && matchesAt(data, 0,
                (byte) 0xFF, (byte) 0xD8, (byte) 0xFF);
    }

    public static boolean isTIFF(byte[] data) {
        // TIFF Little-Endian (II): 49 49 2A 00
        return data.length >= 4 && matchesAt(data, 0,
                (byte) 0x49, (byte) 0x49, (byte) 0x2A, (byte) 0x00);
    }

    public static boolean isHEIC(byte[] data) {

        // HEIC: "ftyp" at Offset 4, afterward Sub-Type at Offset 8
        // ISO Base Media Format (MP4-Family)
        if (data.length >= 12 && matchesAt(data, 4,
                (byte) 0x66, (byte) 0x74, (byte) 0x79, (byte) 0x70)) {
            // Sub-Typen: "heic", "heix", "mif1", "msf1", "hevc", "hevx"
            String brand = new String(Arrays.copyOfRange(data, 8, 12));
            if (brand.equals("heic") || brand.equals("heix") ||
                brand.equals("mif1") || brand.equals("msf1") ||
                brand.equals("hevc") || brand.equals("hevx")) {
                return true;
            }
        }

        return false;
    }

    public static boolean isPDF(byte[] data) {

        // PDF: "%PDF"
        return data.length >= 4 && matchesAt(data, 0,
                (byte) 0x25, (byte) 0x50, (byte) 0x44, (byte) 0x46);

    }

    public static boolean isGZIP(byte[] data) {

        // GZIP: 1F 8B
        return data.length >= 2 && matchesAt(data, 0,
                (byte) 0x1F, (byte) 0x8B);
    }


    /**
     * Prüft, ob das Byte-Array lesbaren Plaintext enthält.
     *
     * Strategie:
     *  1. BOM-Erkennung (UTF-8, UTF-16, UTF-32)
     *  2. Null-Byte-Prüfung (Binärdaten enthalten fast immer 0x00)
     *  3. Anteil druckbarer ASCII/UTF-8-Zeichen muss über Schwellwert liegen
     *
     * @param data      das zu prüfende Byte-Array
     * @param sampleSize Anzahl der Bytes, die geprüft werden (z. B. 512)
     * @return true, wenn der Inhalt mit hoher Wahrscheinlichkeit Plaintext ist
     */
    public static boolean isPlainText(byte[] data, int sampleSize) {
        if (data == null || data.length == 0) {
            return false;
        }

        int limit = Math.min(data.length, sampleSize);

        // 1. BOM-Erkennung → eindeutig Text
        if (hasTextBOM(data)) {
            return true;
        }

        // 2. Null-Byte → fast sicher kein Plaintext
        for (int i = 0; i < limit; i++) {
            if (data[i] == 0x00) {
                return false;
            }
        }

        // 3. Anteil druckbarer Zeichen auswerten
        int printable = 0;
        int i = 0;
        while (i < limit) {
            int b = data[i] & 0xFF;

            if (isPrintableAscii(b)) {
                printable++;
                i++;
            } else if (isUtf8ContinuationSequence(data, i, limit)) {
                // Gültige Multi-Byte UTF-8-Sequenz zählt als ein druckbares Zeichen
                int seqLen = utf8SequenceLength(b);
                printable++;
                i += seqLen;
            } else {
                // Weder ASCII noch gültiges UTF-8 → Binär-Byte
                i++;
            }
        }

        double ratio = (double) printable / limit;
        return ratio >= 0.90; // 90 % Schwellwert
    }

// --- Hilfsmethoden ---

    private static boolean hasTextBOM(byte[] data) {
        // UTF-8 BOM: EF BB BF
        if (data.length >= 3
            && (data[0] & 0xFF) == 0xEF
            && (data[1] & 0xFF) == 0xBB
            && (data[2] & 0xFF) == 0xBF) {
            return true;
        }
        // UTF-16 LE BOM: FF FE
        if (data.length >= 2
            && (data[0] & 0xFF) == 0xFF
            && (data[1] & 0xFF) == 0xFE) {
            return true;
        }
        // UTF-16 BE BOM: FE FF
        if (data.length >= 2
            && (data[0] & 0xFF) == 0xFE
            && (data[1] & 0xFF) == 0xFF) {
            return true;
        }
        // UTF-32 LE BOM: FF FE 00 00
        if (data.length >= 4
            && (data[0] & 0xFF) == 0xFF
            && (data[1] & 0xFF) == 0xFE
            && data[2] == 0x00
            && data[3] == 0x00) {
            return true;
        }
        return false;
    }

    private static boolean isPrintableAscii(int b) {
        // Druckbare Zeichen + typische Steuerzeichen in Texten
        return (b >= 0x20 && b <= 0x7E)  // druckbares ASCII
               || b == 0x09                  // Tab
               || b == 0x0A                  // LF  \n
               || b == 0x0D;                 // CR  \r
    }

    private static int utf8SequenceLength(int firstByte) {
        if ((firstByte & 0xE0) == 0xC0) return 2; // 110xxxxx
        if ((firstByte & 0xF0) == 0xE0) return 3; // 1110xxxx
        if ((firstByte & 0xF8) == 0xF0) return 4; // 11110xxx
        return 1;
    }

    private static boolean isUtf8ContinuationSequence(byte[] data, int offset, int limit) {
        int b0 = data[offset] & 0xFF;
        int seqLen = utf8SequenceLength(b0);

        if (seqLen <= 1 || offset + seqLen > limit) {
            return false;
        }
        // Alle Folgebytes müssen 10xxxxxx sein
        for (int j = 1; j < seqLen; j++) {
            if ((data[offset + j] & 0xC0) != 0x80) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks, if at a position {@code offset} within an array for given magic bytes
     * to exist.
     */
    private static boolean matchesAt(byte[] data, int offset, byte... magic) {
        if (data.length < offset + magic.length) {
            return false;
        }
        for (int i = 0; i < magic.length; i++) {
            if (data[offset + i] != magic[i]) {
                return false;
            }
        }
        return true;
    }

    public static Format innerFormat(Format fmt) {
        return switch (fmt) {
            case BASE64_BPLIST         -> Format.BPLIST;
            case BASE64_XML            -> Format.XML;
            case BASE64_JSON           -> Format.JSON;
            case BASE64_BSON           -> Format.BSON;
            case BASE64_MSGPACK        -> Format.MSGPACK;
            case BASE64_THRIFT_BINARY  -> Format.THRIFT_BINARY;
            case BASE64_THRIFT_COMPACT -> Format.THRIFT_COMPACT;
            case BASE64_FLATBUFFERS    -> Format.FLATBUFFERS;
            case BASE64_JAVA_SERIAL    -> Format.JAVA_SERIAL;
            case BASE64_PROTO          -> Format.PROTOBUF;
            default                    -> fmt;
        };
    }

    public static String formatName(Format fmt) {
        return switch (fmt) {
            case PNG -> "Portable Network Graphic (.png)";
            case GIF -> null;
            case BMP -> "Bitmap Format (.bmp)";
            case JPEG -> "Joint Photographic Expert Group (JPEG)";
            case TIFF -> "TIFF Format (.TIFF)";
            case HEIC -> "HEIC Format (.HEIC)";
            case PDF ->  "PDF Format (.pdf)";
            case GZIP -> "GZIP Format (.gz)";
            case AVRO -> "AVRO Format (.avro)";
            case BPLIST                -> "Apple Binary PList";
            case XML                   -> "XML";
            case JSON                  -> "JSON";
            case BSON                  -> "BSON";
            case MSGPACK               -> "MessagePack";
            case THRIFT_BINARY         -> "Thrift Binary";
            case THRIFT_COMPACT        -> "Thrift Compact";
            case FLATBUFFERS           -> "FlatBuffers";
            case JAVA_SERIAL           -> "Java Serialization";
            case PROTOBUF              -> "Google Protobuf";
            case BASE64_BPLIST         -> "Base64 → Apple BPList";
            case BASE64_XML            -> "Base64 → XML";
            case BASE64_JSON           -> "Base64 → JSON";
            case BASE64_BSON           -> "Base64 → BSON";
            case BASE64_MSGPACK        -> "Base64 → MessagePack";
            case BASE64_THRIFT_BINARY  -> "Base64 → Thrift Binary";
            case BASE64_THRIFT_COMPACT -> "Base64 → Thrift Compact";
            case BASE64_FLATBUFFERS    -> "Base64 → FlatBuffers";
            case BASE64_JAVA_SERIAL    -> "Base64 → Java Serialization";
            case BASE64_PROTO          -> "Base64 → Protobuf";
            case UNKNOWN               -> "Unknown";
            case PLAINTEXT             -> "Plaintext";
        };
    }

    public enum Format {
        PNG, GIF, BMP, JPEG, TIFF, HEIC, PDF, GZIP, AVRO,
        BPLIST, XML, JSON,
        BSON, MSGPACK, THRIFT_BINARY, THRIFT_COMPACT, FLATBUFFERS, JAVA_SERIAL,
        PROTOBUF,
        BASE64_BPLIST, BASE64_XML, BASE64_JSON,
        BASE64_BSON, BASE64_MSGPACK,
        BASE64_THRIFT_BINARY, BASE64_THRIFT_COMPACT,
        BASE64_FLATBUFFERS, BASE64_JAVA_SERIAL, BASE64_PROTO,
        PLAINTEXT,
        UNKNOWN
    }
}
