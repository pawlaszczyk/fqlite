package fqlite.util;

import java.util.Arrays;

/**
 * This detector checks for common header signatures (magic bytes) within
 * a given byte-array.
 *
 * @author pawel
 */
public class BinaryFormatDetector {

    public enum BinaryFormat {
        PNG, GIF, BMP, JPEG, TIFF, HEIC, PDF, PLIST, GZIP, AVRO, BIN
    }

    /**
     * Detects the binary format using Magic Bytes from start of an array (header matching).
     * @param data the Byte-Array to check
     * @return the detected {@link BinaryFormat}, or BIN as fallback
     */
    public static BinaryFormat detect(byte[] data) {
        if (data == null || data.length < 2) {
            return BinaryFormat.BIN;
        }

        // PNG: 89 50 4E 47 0D 0A 1A 0A
        if (data.length >= 8 && matchesAt(data, 0,
                (byte) 0x89, (byte) 0x50, (byte) 0x4E, (byte) 0x47,
                (byte) 0x0D, (byte) 0x0A, (byte) 0x1A, (byte) 0x0A)) {
            return BinaryFormat.PNG;
        }

        // GIF: "GIF8" (GIF87a oder GIF89a)
        if (data.length >= 4 && matchesAt(data, 0,
                (byte) 0x47, (byte) 0x49, (byte) 0x46, (byte) 0x38)) {
            return BinaryFormat.GIF;
        }

        // BMP: "BM"
        if (matchesAt(data, 0,
                (byte) 0x42, (byte) 0x4D)) {
            return BinaryFormat.BMP;
        }

        // JPEG: FF D8 FF
        if (data.length >= 3 && matchesAt(data, 0,
                (byte) 0xFF, (byte) 0xD8, (byte) 0xFF)) {
            return BinaryFormat.JPEG;
        }

        // TIFF Little-Endian (II): 49 49 2A 00
        if (data.length >= 4 && matchesAt(data, 0,
                (byte) 0x49, (byte) 0x49, (byte) 0x2A, (byte) 0x00)) {
            return BinaryFormat.TIFF;
        }

        // TIFF Big-Endian (MM): 4D 4D 00 2A
        if (data.length >= 4 && matchesAt(data, 0,
                (byte) 0x4D, (byte) 0x4D, (byte) 0x00, (byte) 0x2A)) {
            return BinaryFormat.TIFF;
        }

        // HEIC: "ftyp" bei Offset 4, danach Sub-Typ bei Offset 8
        // ISO Base Media Format (MP4-Familie)
        if (data.length >= 12 && matchesAt(data, 4,
                (byte) 0x66, (byte) 0x74, (byte) 0x79, (byte) 0x70)) {
            // Sub-Typen: "heic", "heix", "mif1", "msf1", "hevc", "hevx"
            String brand = new String(Arrays.copyOfRange(data, 8, 12));
            if (brand.equals("heic") || brand.equals("heix") ||
                brand.equals("mif1") || brand.equals("msf1") ||
                brand.equals("hevc") || brand.equals("hevx")) {
                return BinaryFormat.HEIC;
            }
        }

        // PDF: "%PDF"
        if (data.length >= 4 && matchesAt(data, 0,
                (byte) 0x25, (byte) 0x50, (byte) 0x44, (byte) 0x46)) {
            return BinaryFormat.PDF;
        }

        // Binary plist: "bplist"
        if (data.length >= 6 && matchesAt(data, 0,
                (byte) 0x62, (byte) 0x70, (byte) 0x6C, (byte) 0x69,
                (byte) 0x73, (byte) 0x74)) {
            return BinaryFormat.PLIST;
        }

        // GZIP: 1F 8B
        if (data.length >= 2 && matchesAt(data, 0,
                (byte) 0x1F, (byte) 0x8B)) {
            return BinaryFormat.GZIP;
        }

        // Apache Avro: "Obj\x01"
        if (data.length >= 4 && matchesAt(data, 0,
                (byte) 0x4F, (byte) 0x62, (byte) 0x6A, (byte) 0x01)) {
            return BinaryFormat.AVRO;
        }

        return BinaryFormat.BIN;
    }

    /**
     *  Checks, whether the {@code offset} within an array holds the magic number bytes.
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
}
