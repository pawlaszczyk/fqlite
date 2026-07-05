package fqlite.viewer.parser;

import fqlite.viewer.model.BPListNode;
import fqlite.viewer.model.BPListNode.Type;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Parser for Apple Binary Property List format — supports the standard,
 * documented "bplist00" variant only.
 *
 * Format overview (bplist00):
 *   ┌──────────────────────────────────────────────────────────┐
 *   │  Header:   "bplist00"  (8 bytes)                        │
 *   │  Objects:  variable-length encoded object table          │
 *   │  Offsets:  offset table (offsets into the object table)  │
 *   │  Trailer:  26 bytes at EOF                               │
 *   │    [6 unused] [offsetIntSize:1] [objectRefSize:1]        │
 *   │    [numObjects:8] [topObject:8] [offsetTableOffset:8]    │
 *   └──────────────────────────────────────────────────────────┘
 *
 * Object encoding:
 *   First byte encodes type (high nibble) and info/count (low nibble).
 *   Counts > 14 use a following INT object to supply the actual count.
 *
 * <p><b>bplist15 / bplist16:</b> Es gibt mindestens zwei weitere, von Apple
 * nicht veröffentlichte Varianten mit dem Magic "bplist15" bzw. "bplist16".
 * Beide sind intern (CoreFoundation bzw. Foundation/XPC) und strukturell
 * NICHT mit obigem Format kompatibel — "bplist16" hat z. B. überhaupt
 * keinen Trailer (Objekte folgen "packed" direkt nach dem Magic) und
 * zusätzliche, undokumentierte Datentypen (UUID, URL, Sets, NULL). Da es
 * dafür keine öffentliche Spezifikation gibt, wird hier bewusst NICHT
 * versucht, diese nach dem bplist00-Schema zu parsen (das würde stillschweigend
 * falsche Werte liefern) — {@link #parse(byte[])} wirft statt­dessen eine
 * {@link BPListUnsupportedVersionException}.</p>
 */
public class BPListParser {

    // Apple's epoch: 2001-01-01 00:00:00 UTC  (CF absolute time)
    private static final long CF_EPOCH_OFFSET_SECONDS = 978307200L;

    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z")
                    .withZone(ZoneId.of("UTC"));

    // ── State per parse call ────────────────────────────────────────────────
    private byte[] data;
    private int    objectRefSize;
    private int    offsetIntSize;
    private long   numObjects;
    private long   topObject;
    private long   offsetTableOffset;
    private long[] offsets;          // offset for each object index

    // ── Public API ──────────────────────────────────────────────────────────

    /**
     * Parse a binary plist from raw bytes.
     *
     * @param raw  file content (must start with "bplist")
     * @return root BPListNode
     */
    public BPListNode parse(byte[] raw) throws BPListParseException {
        this.data = raw;

        // Verify magic
        String magic = new String(raw, 0, Math.min(8, raw.length), StandardCharsets.US_ASCII);
        if (!magic.startsWith("bplist")) {
            throw new BPListParseException("Invalid BPList header (expected 'bplist…')");
        }
        // Nur bplist00 ist öffentlich spezifiziert (Trailer + Offset-Tabelle
        // nach CFBinaryPList.c). bplist15/16 sind strukturell anders (siehe
        // Klassen-Javadoc) und werden bewusst NICHT geraten-geparst.
        String version = magic.length() >= 8 ? magic.substring(6, 8) : "";
        if (!version.equals("00")) {
            throw new BPListUnsupportedVersionException(version);
        }

        if (raw.length < 32) {
            throw new BPListParseException("File too small for a valid BPList trailer");
        }

        // ── Read trailer (last 32 bytes) ─────────────────────────────────
        // Apple spec: last 32 bytes, but the first 6 are unused padding
        int trailerStart = raw.length - 32;
        // Byte 6: offsetIntSize, Byte 7: objectRefSize
        offsetIntSize  = raw[trailerStart + 6] & 0xFF;
        objectRefSize  = raw[trailerStart + 7] & 0xFF;
        numObjects         = readBigEndianLong(raw, trailerStart + 8);
        topObject          = readBigEndianLong(raw, trailerStart + 16);
        offsetTableOffset  = readBigEndianLong(raw, trailerStart + 24);

        if (offsetIntSize < 1 || offsetIntSize > 8) {
            throw new BPListParseException("Invalid offsetIntSize: " + offsetIntSize);
        }
        if (objectRefSize < 1 || objectRefSize > 8) {
            throw new BPListParseException("Invalid objectRefSize: " + objectRefSize);
        }
        if (numObjects <= 0 || numObjects > 10_000_000) {
            throw new BPListParseException("Invalid object count: " + numObjects);
        }

        // ── Read offset table ─────────────────────────────────────────────
        offsets = new long[(int) numObjects];
        long tablePos = offsetTableOffset;
        for (int i = 0; i < numObjects; i++) {
            offsets[i] = readUnsignedInt(raw, (int) tablePos, offsetIntSize);
            tablePos += offsetIntSize;
        }

        // ── Parse root object ─────────────────────────────────────────────
        return parseObject((int) topObject, new ArrayList<>());
    }

    // ── Object parser ───────────────────────────────────────────────────────

    /**
     * Parse the object at object-table index {@code objIndex}.
     * {@code stack} tracks the current parse path to detect cycles.
     */
    private BPListNode parseObject(int objIndex, List<Integer> stack)
            throws BPListParseException {

        if (objIndex < 0 || objIndex >= numObjects) {
            throw new BPListParseException("Object index out of range: " + objIndex);
        }

        // Cycle detection
        if (stack.contains(objIndex)) {
            BPListNode cycle = new BPListNode(Type.STRING, offsets[objIndex], new byte[0]);
            cycle.setScalarValue("<cyclic reference to object " + objIndex + ">");
            return cycle;
        }

        int pos     = (int) offsets[objIndex];
        int marker  = data[pos] & 0xFF;
        int typeNib = (marker >> 4) & 0x0F;
        int infoNib = marker & 0x0F;

        stack = new ArrayList<>(stack);
        stack.add(objIndex);

        return switch (typeNib) {

            // ── 0x0n – null / bool / fill ─────────────────────────────────
            case 0x0 -> {
                Type t = switch (infoNib) {
                    case 0x0 -> Type.NULL;
                    case 0x8 -> Type.BOOL;   // false
                    case 0x9 -> Type.BOOL;   // true
                    case 0xF -> Type.FILL;
                    default  -> Type.NULL;
                };
                BPListNode n = new BPListNode(t, pos, new byte[]{(byte) marker});
                if (t == Type.BOOL) n.setScalarValue(infoNib == 0x9 ? "true" : "false");
                yield n;
            }

            // ── 0x1n – integer ────────────────────────────────────────────
            case 0x1 -> {
                int byteCount = 1 << infoNib;   // 2^n bytes
                byte[] raw2 = readBytes(pos + 1, byteCount);
                long val    = readBigEndianSigned(raw2);
                BPListNode n = new BPListNode(Type.INT, pos, concat(new byte[]{(byte) marker}, raw2));
                n.setScalarValue(String.valueOf(val));
                yield n;
            }

            // ── 0x2n – real ───────────────────────────────────────────────
            case 0x2 -> {
                int byteCount = 1 << infoNib;
                byte[] raw2 = readBytes(pos + 1, byteCount);
                String realStr;
                if (byteCount == 4) {
                    realStr = String.valueOf(ByteBuffer.wrap(raw2).order(ByteOrder.BIG_ENDIAN).getFloat());
                } else if (byteCount == 8) {
                    realStr = String.valueOf(ByteBuffer.wrap(raw2).order(ByteOrder.BIG_ENDIAN).getDouble());
                } else {
                    realStr = "<real " + byteCount + " bytes>";
                }
                BPListNode n = new BPListNode(Type.REAL, pos, concat(new byte[]{(byte) marker}, raw2));
                n.setScalarValue(realStr);
                yield n;
            }

            // ── 0x3n – date (always 8 bytes, CF absolute time as double) ──
            case 0x3 -> {
                byte[] raw2 = readBytes(pos + 1, 8);
                double cfTime = ByteBuffer.wrap(raw2).order(ByteOrder.BIG_ENDIAN).getDouble();
                long epochMillis = (long)((cfTime + CF_EPOCH_OFFSET_SECONDS) * 1000);
                String dateStr = DATE_FMT.format(Instant.ofEpochMilli(epochMillis))
                                 + "  (CF=" + cfTime + ")";
                BPListNode n = new BPListNode(Type.DATE, pos, concat(new byte[]{(byte) marker}, raw2));
                n.setScalarValue(dateStr);
                yield n;
            }

            // ── 0x4n – data (bytes) ───────────────────────────────────────
            case 0x4 -> {
                CountResult cr = readCount(pos, infoNib, stack);
                byte[] bytes   = readBytes(cr.dataStart(), (int) cr.count());
                BPListNode n = new BPListNode(Type.DATA, pos,
                        concat(new byte[]{(byte) marker}, bytes));
                // Show hex + base64
                n.setScalarValue(toHexPreview(bytes) + "\n  Base64: " + Base64.getEncoder().encodeToString(bytes));
                yield n;
            }

            // ── 0x5n – ASCII string ───────────────────────────────────────
            case 0x5 -> {
                CountResult cr = readCount(pos, infoNib, stack);
                byte[] bytes   = readBytes(cr.dataStart(), (int) cr.count());
                BPListNode n = new BPListNode(Type.STRING, pos,
                        concat(new byte[]{(byte) marker}, bytes));
                n.setScalarValue(new String(bytes, StandardCharsets.US_ASCII));
                yield n;
            }

            // ── 0x6n – Unicode string (UTF-16BE) ──────────────────────────
            case 0x6 -> {
                CountResult cr = readCount(pos, infoNib, stack);
                int byteLen    = (int)(cr.count() * 2);  // count = chars, not bytes
                byte[] bytes   = readBytes(cr.dataStart(), byteLen);
                BPListNode n = new BPListNode(Type.STRING, pos,
                        concat(new byte[]{(byte) marker}, bytes));
                n.setScalarValue(new String(bytes, StandardCharsets.UTF_16BE));
                yield n;
            }

            // ── 0x8n – UID ────────────────────────────────────────────────
            case 0x8 -> {
                int byteCount  = infoNib + 1;
                byte[] raw2    = readBytes(pos + 1, byteCount);
                long uid       = readUnsignedInt(raw2, 0, byteCount);
                BPListNode n   = new BPListNode(Type.UID, pos,
                        concat(new byte[]{(byte) marker}, raw2));
                n.setScalarValue("UID(" + uid + ")");
                yield n;
            }

            // ── 0xAn – Array ──────────────────────────────────────────────
            case 0xA -> {
                CountResult cr = readCount(pos, infoNib, stack);
                BPListNode n   = new BPListNode(Type.ARRAY, pos, new byte[]{(byte) marker});
                for (int i = 0; i < cr.count(); i++) {
                    int refPos  = cr.dataStart() + i * objectRefSize;
                    int ref     = (int) readUnsignedInt(data, refPos, objectRefSize);
                    BPListNode child = parseObject(ref, stack);
                    n.addArrayElement(child);
                }
                yield n;
            }

            // ── 0xBn – Ordered Set (same as Array structurally) ───────────
            case 0xB -> {
                CountResult cr = readCount(pos, infoNib, stack);
                BPListNode n   = new BPListNode(Type.SET, pos, new byte[]{(byte) marker});
                for (int i = 0; i < cr.count(); i++) {
                    int refPos = cr.dataStart() + i * objectRefSize;
                    int ref    = (int) readUnsignedInt(data, refPos, objectRefSize);
                    n.addArrayElement(parseObject(ref, stack));
                }
                yield n;
            }

            // ── 0xDn – Dictionary ─────────────────────────────────────────
            case 0xD -> {
                CountResult cr = readCount(pos, infoNib, stack);
                BPListNode n   = new BPListNode(Type.DICT, pos, new byte[]{(byte) marker});
                int count      = (int) cr.count();
                // Keys come first (count refs), then values (count refs)
                for (int i = 0; i < count; i++) {
                    int keyRefPos = cr.dataStart() + i * objectRefSize;
                    int valRefPos = cr.dataStart() + (count + i) * objectRefSize;
                    int keyRef    = (int) readUnsignedInt(data, keyRefPos, objectRefSize);
                    int valRef    = (int) readUnsignedInt(data, valRefPos, objectRefSize);

                    BPListNode keyNode = parseObject(keyRef, stack);
                    BPListNode valNode = parseObject(valRef, stack);

                    // Attach key label to value for nicer display
                    String label = keyNode.getScalarValue() != null
                            ? keyNode.getScalarValue()
                            : keyNode.treeLabel();
                    valNode.setKeyLabel(label);

                    n.addDictEntry(keyNode, valNode);
                }
                yield n;
            }

            // ── Unknown ───────────────────────────────────────────────────
            default -> {
                BPListNode n = new BPListNode(Type.DATA, pos, new byte[]{(byte) marker});
                n.setScalarValue("<unknown type 0x" + Integer.toHexString(typeNib) + ">");
                yield n;
            }
        };
    }

    // ── Count reading ───────────────────────────────────────────────────────

    /**
     * Reads the element count for collections / variable-length objects.
     * If infoNib == 0xF, the count is encoded as a following INT object.
     * Returns the data start offset and the count.
     */
    private CountResult readCount(int markerPos, int infoNib, List<Integer> stack)
            throws BPListParseException {

        if (infoNib != 0xF) {
            return new CountResult(infoNib, markerPos + 1);
        }
        // Count follows as an INT object (marker byte + value bytes)
        int countPos    = markerPos + 1;
        int countMarker = data[countPos] & 0xFF;
        int byteCount   = 1 << (countMarker & 0x0F);
        byte[] raw      = readBytes(countPos + 1, byteCount);
        long count      = readBigEndianUnsigned(raw);
        return new CountResult(count, countPos + 1 + byteCount);
    }

    private record CountResult(long count, int dataStart) {}

    // ── Low-level byte helpers ──────────────────────────────────────────────

    private byte[] readBytes(int offset, int length) throws BPListParseException {
        if (offset < 0 || offset + length > data.length) {
            throw new BPListParseException(
                    "Read attempt out of file bounds: offset=" + offset + " length=" + length
                    + " fileSize=" + data.length);
        }
        byte[] out = new byte[length];
        System.arraycopy(data, offset, out, 0, length);
        return out;
    }

    /** Read 8-byte big-endian long (unsigned interpretation). */
    private static long readBigEndianLong(byte[] buf, int offset) {
        long v = 0;
        for (int i = 0; i < 8; i++) v = (v << 8) | (buf[offset + i] & 0xFF);
        return v;
    }

    /**
     * Read n-byte big-endian value as an unsigned long.
     * Used for element/string counts and data lengths, which are defined by the
     * bplist format as non-negative. Unlike {@link #readBigEndianSigned}, this
     * does NOT sign-extend the leading byte, so a byte such as 0xC8 (200) is
     * read as 200 instead of being incorrectly sign-extended to -56.
     */
    private static long readBigEndianUnsigned(byte[] buf) {
        long v = 0;
        for (byte b : buf) v = (v << 8) | (b & 0xFF);
        return v;
    }

    /** Read n-byte big-endian value as signed long. */
    private static long readBigEndianSigned(byte[] buf) {
        if (buf.length == 0) return 0;
        // Sign-extend the first byte
        long v = (byte) buf[0];
        for (int i = 1; i < buf.length; i++) v = (v << 8) | (buf[i] & 0xFF);
        return v;
    }

    /** Read n-byte big-endian value as unsigned long from byte array at offset. */
    static long readUnsignedInt(byte[] buf, int offset, int size) {
        long v = 0;
        for (int i = 0; i < size; i++) v = (v << 8) | (buf[offset + i] & 0xFF);
        return v;
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private static String toHexPreview(byte[] bytes) {
        int shown = Math.min(bytes.length, 64);
        StringBuilder sb = new StringBuilder(shown * 3);
        for (int i = 0; i < shown; i++) sb.append(String.format("%02X ", bytes[i] & 0xFF));
        if (bytes.length > 64) sb.append("… (").append(bytes.length).append(" bytes total)");
        return sb.toString().trim();
    }
}
