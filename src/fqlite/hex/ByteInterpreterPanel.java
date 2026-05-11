package fqlite.hex;

import javafx.geometry.Insets;
import javafx.scene.control.Label;
import javafx.scene.layout.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Zeigt Byte-Interpretationen in einem FlowPane an, das automatisch umbricht
 * und sich der verfügbaren Breite anpasst – kein horizontales Überlaufen.
 */
public class ByteInterpreterPanel extends VBox {

    private final FlowPane flow  = new FlowPane();
    private final FlowPane flow2  = new FlowPane();

    private final Label    title = new Label("Byte-Interpretation");

    public ByteInterpreterPanel() {
        setSpacing(0);

        title.getStyleClass().add("interp-title");
        title.setMaxWidth(Double.MAX_VALUE);
        title.setPadding(new Insets(3, 8, 3, 8));

        flow.setHgap(5);
        flow.setVgap(5);
        flow.setPadding(new Insets(3, 4, 3, 4));

        // FlowPane soll sich der Panel-Breite anpassen
        flow.prefWrapLengthProperty().bind(widthProperty());

        VBox lines = new VBox();
        lines.setSpacing(5);
        lines.setPadding(new Insets(5, 5, 5, 5));
        lines.getChildren().addAll(flow, flow2);
        getChildren().addAll(title, lines);
        setVisible(true);
    }

    // ── Public API ────────────────────────────────────────────────────────────

    public void update(byte[] bytes, long baseOffset) {
        flow.getChildren().clear();
        int len = bytes.length;

        title.setText(String.format(
            "Byte-Interpretation  –  Offset 0x%X  (%d Byte%s)",
            baseOffset, len, len == 1 ? "" : "s"));

        // ── 1 Byte ───────────────────────────────────────────────────────────
        add("Int8",  String.valueOf((byte)(bytes[0] & 0xFF)),1);
        add("UInt8", String.valueOf(bytes[0] & 0xFF),1);
        add("Hex",   String.format("0x%02X", bytes[0] & 0xFF),1);
        add("Bin",   toBin8(bytes[0]),1);
        add("Oct",   String.format("0%o",   bytes[0] & 0xFF),1);
        add("ASCII", toAsciiChar(bytes[0]),1);

        // ── 2 Byte ───────────────────────────────────────────────────────────
        if (len >= 2) {
            ByteBuffer be2 = buf(bytes, 2, ByteOrder.BIG_ENDIAN);
            ByteBuffer le2 = buf(bytes, 2, ByteOrder.LITTLE_ENDIAN);
            add("Int16 BE",  String.valueOf(be2.getShort(0)),1);
            add("Int16 LE",  String.valueOf(le2.getShort(0)),1);
            add("UInt16 BE", String.valueOf(be2.getShort(0) & 0xFFFFL),1);
            add("UInt16 LE", String.valueOf(le2.getShort(0) & 0xFFFFL),1);
        }

        // ── 4 Byte ───────────────────────────────────────────────────────────
        if (len >= 4) {
            ByteBuffer be4 = buf(bytes, 4, ByteOrder.BIG_ENDIAN);
            ByteBuffer le4 = buf(bytes, 4, ByteOrder.LITTLE_ENDIAN);
            add("Int32 BE",   String.valueOf(be4.getInt(0)),2);
            add("Int32 LE",   String.valueOf(le4.getInt(0)),2);
            add("UInt32 BE",  String.valueOf(be4.getInt(0) & 0xFFFFFFFFL),2);
            add("UInt32 LE",  String.valueOf(le4.getInt(0) & 0xFFFFFFFFL),2);
            add("Float32 BE", formatFloat(be4.getFloat(0)),2);
            add("Float32 LE", formatFloat(le4.getFloat(0)),2);
        }

        // ── 8 Byte ───────────────────────────────────────────────────────────
//        if (len >= 8) {
//            ByteBuffer be8 = buf(bytes, 8, ByteOrder.BIG_ENDIAN);
//            ByteBuffer le8 = buf(bytes, 8, ByteOrder.LITTLE_ENDIAN);
//            add("Int64 BE",   String.valueOf(be8.getLong(0)),2);
//            add("Int64 LE",   String.valueOf(le8.getLong(0)),2);
//            add("UInt64 BE",  Long.toUnsignedString(be8.getLong(0)),2);
//            add("UInt64 LE",  Long.toUnsignedString(le8.getLong(0)),2);
//            add("Float64 BE", formatDouble(be8.getDouble(0)),2);
//            add("Float64 LE", formatDouble(le8.getDouble(0)),2);
//        }

        // ── VarInt ───────────────────────────────────────────────────────────
//        add("VarInt u", decodeULEB128(bytes),2);
//        add("VarInt s", decodeSLEB128(bytes),2);

        setVisible(true);
    }

    public void clear() {
        flow.getChildren().clear();
        setVisible(false);
    }

    // ── Cell builder ──────────────────────────────────────────────────────────

    private void add(String type, String value, int line) {
        VBox cell = new VBox(1);
        cell.getStyleClass().add("interp-cell");
        cell.setPadding(new Insets(3, 8, 3, 8));
        cell.setPrefWidth(USE_COMPUTED_SIZE); // lässt FlowPane entscheiden

        Label lType = new Label(type);
        lType.getStyleClass().add("interp-type");

        Label lVal = new Label(value);
        lVal.getStyleClass().add("interp-value");

        cell.getChildren().addAll(lType, lVal);
        if (line == 1)
            flow.getChildren().add(cell);
        else
            flow2.getChildren().add(cell);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static ByteBuffer buf(byte[] src, int len, ByteOrder order) {
        byte[] b = new byte[len];
        System.arraycopy(src, 0, b, 0, Math.min(len, src.length));
        return ByteBuffer.wrap(b).order(order);
    }

    private static String toBin8(byte b) {
        int v = b & 0xFF;
        return String.format("%4s %4s",
            Integer.toBinaryString(v >> 4),
            Integer.toBinaryString(v & 0xF)
        ).replace(' ', '0');
    }

    private static String toAsciiChar(byte b) {
        int v = b & 0xFF;
        if (v < 32)   return "<ctrl:" + v + ">";
        if (v == 127) return "<DEL>";
        if (v < 128)  return "'" + (char) v + "'";
        return "<" + v + ">";
    }

    private static String formatFloat(float f) {
        if (Float.isNaN(f))      return "NaN";
        if (Float.isInfinite(f)) return f > 0 ? "+∞" : "−∞";
        return String.format("%.6g", f);
    }

    private static String formatDouble(double d) {
        if (Double.isNaN(d))      return "NaN";
        if (Double.isInfinite(d)) return d > 0 ? "+∞" : "−∞";
        return String.format("%.10g", d);
    }

    private static String decodeULEB128(byte[] bytes) {
        long result = 0; int shift = 0;
        for (int i = 0; i < Math.min(bytes.length, 10); i++) {
            int b = bytes[i] & 0xFF;
            result |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return Long.toUnsignedString(result) + " (" + (i+1) + "B)";
            shift += 7;
        }
        return "overflow";
    }

    private static String decodeSLEB128(byte[] bytes) {
        long result = 0; int shift = 0;
        for (int i = 0; i < Math.min(bytes.length, 10); i++) {
            int b = bytes[i] & 0xFF;
            result |= (long)(b & 0x7F) << shift;
            shift += 7;
            if ((b & 0x80) == 0) {
                if (shift < 64 && (b & 0x40) != 0) result |= -(1L << shift);
                return String.valueOf(result) + " (" + (i+1) + "B)";
            }
        }
        return "overflow";
    }
}
