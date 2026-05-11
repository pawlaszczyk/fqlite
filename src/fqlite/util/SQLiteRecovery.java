package fqlite.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * SQLite Database Recovery - Header Validation & Debug Utility
 *
 * Provides methods to:
 *  - Validate and trim overwrite prefixes from deleted SQLite records
 *  - Parse and classify SQLite Serial Types
 *  - Generate detailed debug reports from a raw ByteBuffer stream
 */
public class SQLiteRecovery {

    // ═══════════════════════════════════════════════════════════════════════════
    // PUBLIC API
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Rückwärtsvalidierung eines SQLite-Recovery-Matches.
     * Erkennt ob dem eigentlichen Record-Header Overwrite-Bytes vorangestellt wurden.
     *
     * @param hexMatch   Der erkannte Match als Hex-String (z.B. "0000019d1b170407")
     * @param numColumns Anzahl der erwarteten Spalten
     * @return ValidationResult mit bereinigtem Match und Diagnose
     */
    public static ValidationResult validateAndTrimOverwritePrefix(String hexMatch, int numColumns) {
        byte[] data = hexStringToBytes(hexMatch);

        for (int prefixLen = 1; prefixLen < Math.min(5, data.length); prefixLen++) {
            byte[] candidate = Arrays.copyOfRange(data, prefixLen, data.length);

            if (isPlausibleHeader(candidate, numColumns)) {
                byte[] prefix = Arrays.copyOfRange(data, 0, prefixLen);
                return new ValidationResult(
                        true,
                        bytesToHexString(prefix),
                        bytesToHexString(candidate),
                        "NOLENGTH_WITH_OVERWRITE: " + prefixLen + " Byte(s) Overwrite-Prefix erkannt"
                );
            }
        }

        // Kein Prefix gefunden – prüfe ob der Match selbst schon plausibel ist
        if (isPlausibleHeader(data, numColumns)) {
            return new ValidationResult(true, "", hexMatch, "MATCH_OK: Kein Overwrite-Prefix");
        }

        return new ValidationResult(false, "", hexMatch, "INVALID: Header nicht plausibel");
    }

    /**
     * Analysiert einen SQLite-Record-Header vollständig und gibt einen
     * detaillierten Report aus. Fehlende Header-Bytes werden automatisch
     * aus dem ByteBuffer nachgelesen.
     *
     * @param hexMatch    Der erkannte Match als Hex-String (mit möglichem Overwrite-Prefix)
     * @param matchOffset Position des Matches im ByteBuffer
     * @param numColumns  Erwartete Anzahl Spalten
     * @param buffer      Der originale Datenstrom
     */
    public static void debugHeaderReport(String hexMatch, int matchOffset,
                                         int numColumns, ByteBuffer buffer) {

        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║           SQLite Record Header - Debug Report                ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println("  Input:       " + hexMatch.toUpperCase());
        System.out.println("  Offset:      " + matchOffset + " (0x" + Integer.toHexString(matchOffset).toUpperCase() + ")");
        System.out.println("  NumColumns:  " + numColumns);
        System.out.println("  BufferSize:  " + buffer.capacity() + " Bytes");
        System.out.println();

        // Schritt 1: Rückwärtsvalidierung
        ValidationResult validation = validateAndTrimOverwritePrefix(hexMatch, numColumns);
        System.out.println("┌─ Rückwärtsvalidierung ────────────────────────────────────────");
        System.out.println("│  Status:     " + (validation.valid ? "✓ VALID" : "✗ INVALID"));
        System.out.println("│  Diagnose:   " + validation.diagnosis);
        if (!validation.overwritePrefix.isEmpty()) {
            System.out.println("│  Overwrite:  [" + validation.overwritePrefix.toUpperCase() + "] → wird abgeschnitten");
        }
        System.out.println("│  Bereinigt:  [" + validation.cleanedMatch.toUpperCase() + "]");
        System.out.println("└───────────────────────────────────────────────────────────────");
        System.out.println();

        if (!validation.valid) {
            System.out.println("  ✗ Header-Analyse abgebrochen – Match nicht plausibel.");
            return;
        }

        // Schritt 2: Echten Header-Start im Buffer bestimmen
        int overwriteLen = validation.overwritePrefix.length() / 2;
        int cleanedLen   = validation.cleanedMatch.length() / 2;
        int headerStart  = matchOffset + overwriteLen;
        int headerLen    = readHeaderLength(buffer, headerStart);

        System.out.println("┌─ Buffer-Navigation ───────────────────────────────────────────");
        System.out.printf( "│  Match-Offset:          %d (0x%X)%n", matchOffset, matchOffset);
        System.out.printf( "│  Overwrite-Bytes:        %d%n", overwriteLen);
        System.out.printf( "│  Header-Start im Buffer: %d (0x%X)%n", headerStart, headerStart);
        System.out.printf( "│  Header-Länge (Byte 0):  %d Bytes%n", headerLen);

        int bytesInMatch = cleanedLen;
        int bytesMissing = Math.max(0, headerLen - bytesInMatch);

        System.out.printf( "│  Bytes im Match:         %d%n", bytesInMatch);
        System.out.printf( "│  Bytes benötigt:         %d%n", headerLen);

        if (bytesMissing > 0) {
            System.out.printf("│  Fehlende Bytes:         %d → werden aus Buffer nachgelesen%n", bytesMissing);
        } else {
            System.out.println("│  Fehlende Bytes:         0 → Match ist vollständig");
        }
        System.out.println("└───────────────────────────────────────────────────────────────");
        System.out.println();

        // Schritt 3: Vollständigen Header aus Buffer laden
        byte[] fullHeader = readFullHeader(buffer, headerStart, headerLen, validation.cleanedMatch);

        if (fullHeader == null) {
            System.out.println("  ✗ Header konnte nicht vollständig aus Buffer gelesen werden.");
            System.out.printf( "     Buffer-Kapazität: %d, benötigt bis: %d%n",
                    buffer.capacity(), headerStart + headerLen);
            return;
        }

        // Schritt 4: Hex-Dump
        System.out.println("┌─ Header Hex-Dump ─────────────────────────────────────────────");
        printHexDump(fullHeader, headerStart);
        System.out.println("└───────────────────────────────────────────────────────────────");
        System.out.println();

        // Schritt 5: Serial Types parsen
        System.out.println("┌─ Serial Types ────────────────────────────────────────────────");
        System.out.printf( "│  [0x%02X] Byte 0 → Header-Länge: %d Bytes%n", fullHeader[0] & 0xFF, headerLen);
        System.out.println("│");
        System.out.println("│  ┌──────┬──────────┬────────────┬──────────┬──────────────────┐");
        System.out.println("│  │ Col  │ Offset   │ Byte(s)    │ Typ      │ Beschreibung     │");
        System.out.println("│  ├──────┼──────────┼────────────┼──────────┼──────────────────┤");

        int pos = 1;
        int col = 0;
        int totalDataSize = 0;
        List<SerialTypeResult> serialTypes = new ArrayList<>();

        while (pos < headerLen && col < numColumns) {
            int absOffset = headerStart + pos;
            SerialTypeResult result = parseSerialType(fullHeader, pos);
            serialTypes.add(result);

            StringBuilder rawBytes = new StringBuilder();
            for (int i = pos; i < pos + result.bytesConsumed && i < fullHeader.length; i++) {
                rawBytes.append(String.format("%02X ", fullHeader[i] & 0xFF));
            }

            System.out.printf("│  │ %-4d │ 0x%05X  │ %-10s │ %-8s │ %-16s │%n",
                    col,
                    absOffset,
                    rawBytes.toString().trim(),
                    result.category.name(),
                    result.description
            );

            totalDataSize += result.dataSize;
            pos += result.bytesConsumed;
            col++;
        }

        System.out.println("│  └──────┴──────────┴────────────┴──────────┴──────────────────┘");
        System.out.println("│");

        // Schritt 6: Nutzdaten-Layout
        System.out.println("│  Nutzdaten-Layout (ab Offset " + (headerStart + headerLen) + "):");
        System.out.println("│  ┌──────┬──────────┬──────────┬────────────────────────────┐");
        System.out.println("│  │ Col  │ Offset   │ Länge    │ Typ                        │");
        System.out.println("│  ├──────┼──────────┼──────────┼────────────────────────────┤");

        int dataOffset = headerStart + headerLen;
        for (int c = 0; c < serialTypes.size(); c++) {
            SerialTypeResult st = serialTypes.get(c);
            System.out.printf("│  │ %-4d │ 0x%05X  │ %-8d │ %-26s │%n",
                    c, dataOffset, st.dataSize, st.description);
            dataOffset += st.dataSize;
        }

        System.out.println("│  └──────┴──────────┴──────────┴────────────────────────────┘");
        System.out.println("│");

        // Schritt 7: Zusammenfassung
        long invalidCount = serialTypes.stream().filter(r -> !r.valid).count();
        System.out.println("│  Zusammenfassung:");
        System.out.printf( "│    Erkannte Columns:  %d / %d%n", col, numColumns);
        System.out.printf( "│    Header-Bytes:      %d%n", headerLen);
        System.out.printf( "│    Nutzdaten gesamt:  %d Bytes%n", totalDataSize);
        System.out.printf( "│    Record gesamt:     %d Bytes%n", headerLen + totalDataSize);
        System.out.printf( "│    Record Ende:       Offset 0x%X%n", headerStart + headerLen + totalDataSize);
        System.out.println("└───────────────────────────────────────────────────────────────");
        System.out.println();

        // Schritt 8: Gesamtbewertung
        boolean allValid = invalidCount == 0 && col == numColumns;
        System.out.println("┌─ Gesamtbewertung ─────────────────────────────────────────────");
        if (allValid) {
            System.out.println("│  ✓ Record-Header vollständig und plausibel.");
            System.out.println("│  ✓ Match kann für Recovery verwendet werden.");
        } else {
            if (col < numColumns)
                System.out.printf("│  ✗ Zu wenige Columns erkannt (%d/%d).%n", col, numColumns);
            if (invalidCount > 0)
                System.out.printf("│  ✗ %d ungültige Serial Type(s) gefunden.%n", invalidCount);
            System.out.println("│  ✗ Match sollte verworfen oder manuell geprüft werden.");
        }
        System.out.println("└───────────────────────────────────────────────────────────────");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // HEADER VALIDATION
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Prüft ob ein Byte-Array einen plausiblen SQLite-Record-Header darstellt.
     * Mit granularer Serial-Type-Validierung gemäß SQLite-Spezifikation.
     */
    private static boolean isPlausibleHeader(byte[] data, int numColumns) {
        if (data.length < 2) return false;

        int headerLen = data[0] & 0xFF;
        int minHeader = numColumns + 1;
        int maxHeader = numColumns * 3 + 2;
        if (headerLen < minHeader || headerLen > maxHeader) return false;

        int i = 1;
        int columnCount = 0;
        while (i < Math.min(headerLen, data.length) && columnCount < numColumns) {
            SerialTypeResult result = parseSerialType(data, i);
            if (!result.valid) return false;
            i += result.bytesConsumed;
            columnCount++;
        }

        return columnCount == numColumns;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SERIAL TYPE PARSING
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Parst einen SQLite Serial Type (ggf. mehrbytiger Varint) ab Position pos.
     */
    private static SerialTypeResult parseSerialType(byte[] data, int pos) {
        if (pos >= data.length) return SerialTypeResult.invalid();

        long value = 0;
        int bytesConsumed = 0;
        for (int i = 0; i < 9 && (pos + i) < data.length; i++) {
            int b = data[pos + i] & 0xFF;
            bytesConsumed++;
            if (i < 8) {
                value = (value << 7) | (b & 0x7F);
                if ((b & 0x80) == 0) break;
            } else {
                value = (value << 8) | b;
                break;
            }
        }

        return classifySerialType(value, bytesConsumed);
    }

    /**
     * Klassifiziert einen Serial-Type-Wert gemäß SQLite-Spezifikation.
     *
     * Serial Type | Bedeutung
     * ────────────┼──────────────────────────────────────
     *      0      | NULL
     *      1      | 1-Byte signed Integer
     *      2      | 2-Byte signed Integer
     *      3      | 3-Byte signed Integer
     *      4      | 4-Byte signed Integer
     *      5      | 6-Byte signed Integer
     *      6      | 8-Byte signed Integer
     *      7      | 8-Byte IEEE 754 Float
     *      8      | Integer-Konstante 0
     *      9      | Integer-Konstante 1
     *   10,11     | RESERVIERT (intern, nie in DB)
     *   >= 12     | Gerade  → BLOB,  Länge = (N-12)/2
     *             | Ungerade → TEXT, Länge = (N-13)/2
     */
    private static SerialTypeResult classifySerialType(long value, int bytesConsumed) {

        if (value == 10 || value == 11) {
            return new SerialTypeResult(false, bytesConsumed,
                    SerialTypeCategory.RESERVED, 0,
                    "Reservierter Serial Type: " + value);
        }

        if (value == 0) {
            return new SerialTypeResult(true, bytesConsumed,
                    SerialTypeCategory.NULL, 0, "NULL");
        }

        if (value >= 1 && value <= 6) {
            int[] sizes = {1, 2, 3, 4, 6, 8};
            int dataSize = sizes[(int) value - 1];
            return new SerialTypeResult(true, bytesConsumed,
                    SerialTypeCategory.INTEGER, dataSize,
                    "INT(" + dataSize + " Bytes)");
        }

        if (value == 7) {
            return new SerialTypeResult(true, bytesConsumed,
                    SerialTypeCategory.FLOAT, 8, "REAL (8 Bytes)");
        }

        if (value == 8 || value == 9) {
            return new SerialTypeResult(true, bytesConsumed,
                    SerialTypeCategory.INTEGER, 0,
                    "INT_CONST_" + (value - 8));
        }

        if (value >= 12 && value % 2 == 0) {
            long dataSize = (value - 12) / 2;
            if (dataSize > 1_000_000) {
                return new SerialTypeResult(false, bytesConsumed,
                        SerialTypeCategory.BLOB, (int) dataSize,
                        "BLOB zu groß: " + dataSize + " Bytes");
            }
            return new SerialTypeResult(true, bytesConsumed,
                    SerialTypeCategory.BLOB, (int) dataSize,
                    "BLOB(" + dataSize + " Bytes)");
        }

        if (value >= 13 && value % 2 == 1) {
            long dataSize = (value - 13) / 2;
            if (dataSize > 1_000_000) {
                return new SerialTypeResult(false, bytesConsumed,
                        SerialTypeCategory.TEXT, (int) dataSize,
                        "TEXT zu groß: " + dataSize + " Bytes");
            }
            return new SerialTypeResult(true, bytesConsumed,
                    SerialTypeCategory.TEXT, (int) dataSize,
                    "TEXT(" + dataSize + " Bytes)");
        }

        return SerialTypeResult.invalid();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // BUFFER HELPERS
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Liest die Header-Länge (Byte 0) aus dem Buffer.
     */
    private static int readHeaderLength(ByteBuffer buffer, int headerStart) {
        if (headerStart >= buffer.capacity()) return 0;
        return buffer.get(headerStart) & 0xFF;
    }

    /**
     * Lädt den vollständigen Header – zuerst aus dem Match, fehlende Bytes aus dem Buffer.
     */
    private static byte[] readFullHeader(ByteBuffer buffer, int headerStart,
                                         int headerLen, String cleanedMatchHex) {
        if (headerStart + headerLen > buffer.capacity()) return null;

        byte[] fullHeader = new byte[headerLen];
        byte[] matchBytes = hexStringToBytes(cleanedMatchHex);

        int fromMatch = Math.min(matchBytes.length, headerLen);
        System.arraycopy(matchBytes, 0, fullHeader, 0, fromMatch);

        if (fromMatch < headerLen) {
            int bufferReadPos = headerStart + fromMatch;
            for (int i = fromMatch; i < headerLen; i++) {
                fullHeader[i] = buffer.get(bufferReadPos++);
            }
        }

        return fullHeader;
    }

    /**
     * Gibt einen formatierten Hex-Dump aus (16 Bytes pro Zeile).
     */
    private static void printHexDump(byte[] data, int baseOffset) {
        for (int i = 0; i < data.length; i += 16) {
            System.out.printf("│  0x%05X  ", baseOffset + i);

            for (int j = 0; j < 16; j++) {
                if (i + j < data.length) {
                    System.out.printf("%02X ", data[i + j] & 0xFF);
                } else {
                    System.out.print("   ");
                }
                if (j == 7) System.out.print(" ");
            }

            System.out.print(" │ ");
            for (int j = 0; j < 16 && i + j < data.length; j++) {
                char c = (char) (data[i + j] & 0xFF);
                System.out.print(c >= 32 && c < 127 ? c : '.');
            }
            System.out.println();
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // HEX CONVERSION HELPERS
    // ═══════════════════════════════════════════════════════════════════════════

    private static byte[] hexStringToBytes(String hex) {
        hex = hex.replaceAll("\\s+", "");
        byte[] result = new byte[hex.length() / 2];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return result;
    }

    private static String bytesToHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // DATA CLASSES
    // ═══════════════════════════════════════════════════════════════════════════

    public enum SerialTypeCategory {
        NULL, INTEGER, FLOAT, TEXT, BLOB, RESERVED, UNKNOWN
    }

    public static class SerialTypeResult {
        public final boolean valid;
        public final int bytesConsumed;
        public final SerialTypeCategory category;
        public final int dataSize;
        public final String description;

        public SerialTypeResult(boolean valid, int bytesConsumed,
                                SerialTypeCategory category, int dataSize, String description) {
            this.valid = valid;
            this.bytesConsumed = bytesConsumed;
            this.category = category;
            this.dataSize = dataSize;
            this.description = description;
        }

        public static SerialTypeResult invalid() {
            return new SerialTypeResult(false, 1, SerialTypeCategory.UNKNOWN, 0, "INVALID");
        }
    }

    public static class ValidationResult {
        public final boolean valid;
        public final String overwritePrefix;
        public final String cleanedMatch;
        public final String diagnosis;

        public ValidationResult(boolean valid, String overwritePrefix,
                                String cleanedMatch, String diagnosis) {
            this.valid = valid;
            this.overwritePrefix = overwritePrefix;
            this.cleanedMatch = cleanedMatch;
            this.diagnosis = diagnosis;
        }

        @Override
        public String toString() {
            return String.format(
                    "Valid: %b | Prefix: [%s] | CleanedMatch: [%s] | %s",
                    valid, overwritePrefix, cleanedMatch, diagnosis
            );
        }

    }

    // ═══════════════════════════════════════════════════════════════════════════
    // MAIN - Beispielaufruf
    // ═══════════════════════════════════════════════════════════════════════════

    public static void main(String[] args) {
        // Beispiel 1: validateAndTrimOverwritePrefix
        System.out.println("=== Beispiel 1: validateAndTrimOverwritePrefix ===");
        ValidationResult result = validateAndTrimOverwritePrefix("0000019d1b170407", 5);
        System.out.println(result);
        System.out.println();

        // Beispiel 2: debugHeaderReport mit ByteBuffer
        System.out.println("=== Beispiel 2: debugHeaderReport ===");
        byte[] rawData = new byte[8192];
        // Simulierter Datenstrom: Overwrite-Bytes gefolgt vom echten Header
        byte[] matchBytes = {0x00, 0x00, 0x01, (byte)0x9d, 0x1b, 0x17, 0x04, 0x07};
        System.arraycopy(matchBytes, 0, rawData, 7681, matchBytes.length);
        ByteBuffer buffer = ByteBuffer.wrap(rawData);

        debugHeaderReport("0000019d1b170407", 7681, 5, buffer);
    }
}

