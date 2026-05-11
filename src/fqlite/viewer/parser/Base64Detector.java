package fqlite.viewer.parser;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Detects whether raw file content is Base64-encoded and, if so, decodes it.
 *
 * Strategy
 * ─────────
 * 1. Every single byte must be a printable ASCII character (0x20–0x7E) or
 *    a whitespace/newline used as line-wrap (0x09, 0x0A, 0x0D).
 *    Any byte outside this range → immediately NOT Base64.
 *    This single rule eliminates virtually all binary formats (BPList, Protobuf,
 *    BSON, …) that were previously mis-detected.
 * 2. ≥ 97 % of non-whitespace characters must belong to the Base64 alphabet.
 * 3. After stripping whitespace the length must be a valid Base64 length
 *    (not remainder 1 mod 4).
 * 4. The decoded payload must be at least 8 bytes.
 * 5. Minimum input length: 16 characters (decodes to ≥ 12 bytes) to avoid
 *    false positives on short alphanumeric identifiers.
 */
public class Base64Detector {

    public record Result(boolean isBase64, byte[] decodedBytes) {}

    private static final double MIN_VALID_FRACTION = 0.97;
    private static final int    MIN_LENGTH         = 16;   // raised from 8
    private static final int    MIN_DECODED_BYTES  = 8;    // raised from 4

    private Base64Detector() {}

    public static Result detect(byte[] raw) {
        if (raw == null || raw.length < MIN_LENGTH) return new Result(false, null);

        // ── Rule 1: reject as soon as any non-text byte is found ──────────
        for (byte b : raw) {
            int u = b & 0xFF;
            // Allow: printable ASCII (0x20–0x7E) + TAB (0x09) + LF (0x0A) + CR (0x0D)
            if (u > 0x7E || (u < 0x20 && u != 0x09 && u != 0x0A && u != 0x0D)) {
                return new Result(false, null);
            }
        }

        String text;
        try {
            text = new String(raw, StandardCharsets.US_ASCII).strip();
        } catch (Exception e) {
            return new Result(false, null);
        }

        if (text.length() < MIN_LENGTH) return new Result(false, null);

        // ── Rule 2: count valid Base64 alphabet characters ────────────────
        long valid = 0, total = 0;
        boolean urlSafe = false;
        for (char c : text.toCharArray()) {
            if (Character.isWhitespace(c)) continue;
            total++;
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
                || (c >= '0' && c <= '9') || c == '+' || c == '/' || c == '=') {
                valid++;
            } else if (c == '-' || c == '_') {
                valid++;
                urlSafe = true;
            }
            // anything else (e.g. punctuation like ':', ',', '{') → counts as invalid
        }

        if (total == 0 || (double) valid / total < MIN_VALID_FRACTION) {
            return new Result(false, null);
        }

        // ── Rule 3: length check ──────────────────────────────────────────
        String stripped = text.replaceAll("\\s+", "");
        int remainder = stripped.length() % 4;
        if (remainder == 1) return new Result(false, null);
        if (remainder == 2) stripped += "==";
        if (remainder == 3) stripped += "=";

        // ── Rule 4 + 5: decode and check minimum payload size ─────────────
        try {
            Base64.Decoder decoder = urlSafe
                    ? Base64.getUrlDecoder()
                    : Base64.getDecoder();
            byte[] decoded = decoder.decode(stripped);
            if (decoded.length < MIN_DECODED_BYTES) return new Result(false, null);
            return new Result(true, decoded);
        } catch (IllegalArgumentException e) {
            return new Result(false, null);
        }
    }
}
