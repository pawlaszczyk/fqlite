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
}
