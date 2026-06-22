package fqlite.viewer.util;

/**
 * Produces a classic hex dump with offset, hex columns, and ASCII sidebar.
 */
public class HexDump {

    private static final int COLS = 16;

    public static String dump(byte[] data) {
        return dump(data, 0, data.length, 0);
    }

    public static String dump(byte[] data, int from, int to, long baseOffset) {

        if (data == null || data.length == 0) return "<empty>";

        StringBuilder sb = new StringBuilder();
        int i = from;
        while (i < to) {
            // Offset
            sb.append(String.format("%08X  ", baseOffset + (i - from)));
            int rowStart = i;
            // Hex
            for (int col = 0; col < COLS; col++) {
                if (i + col < to) {
                    sb.append(String.format("%02X ", data[i + col] & 0xFF));
                } else {
                    sb.append("   ");
                }
                if (col == 7) sb.append(" ");
            }
            sb.append(" |");
            // ASCII
            for (int col = 0; col < COLS && i + col < to; col++) {
                char c = (char)(data[i + col] & 0xFF);
                sb.append(c >= 0x20 && c < 0x7F ? c : '.');
            }
            sb.append("|\n");
            i += COLS;
        }

        return sb.toString();
    }

    /**
     * Erstellt einen klassischen Hex-Dump (Offset | Hex-Bytes | ASCII).
     *
     * @param data   Rohdaten
     * @param offset Startoffset für die Adressspalte
     * @return Formatierter String
     */
    public static String dump(byte[] data, long offset) {
        if (data == null || data.length == 0) return "(keine Daten)";
        StringBuilder sb = new StringBuilder();
        int cols = 16;
        for (int i = 0; i < data.length; i += cols) {
            sb.append(String.format("%08X  ", offset + i));
            for (int j = 0; j < cols; j++) {
                if (i + j < data.length) {
                    sb.append(String.format("%02X ", data[i + j] & 0xFF));
                } else {
                    sb.append("   ");
                }
                if (j == 7) sb.append(' ');
            }
            sb.append(" |");
            for (int j = 0; j < cols && i + j < data.length; j++) {
                char c = (char) (data[i + j] & 0xFF);
                sb.append(c >= 32 && c < 127 ? c : '.');
            }
            sb.append("|\n");
        }
        return sb.toString();
    }


    /** Gibt die ersten {@code maxBytes} Bytes als Hex-String zurück (ohne Leerzeichen). */
    public static String toHexString(byte[] data, int maxBytes) {
        if (data == null) return "";
        int len = Math.min(data.length, maxBytes);
        StringBuilder sb = new StringBuilder(len * 2);
        for (int i = 0; i < len; i++) sb.append(String.format("%02X", data[i] & 0xFF));
        if (data.length > maxBytes) sb.append("…");
        return sb.toString();
    }

}
