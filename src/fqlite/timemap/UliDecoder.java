package fqlite.timemap;

/**
 * Decodes the 3GPP User Location Information (ULI) field from its hexadecimal
 * string representation into structured cell identity.
 *
 * <h3>Wire format confirmed by real fqlite captures</h3>
 * <pre>
 *
 * </pre>
 *
 * <h3>GTPv1 vs GTPv2</h3>
 * <ul>
 *   <li>GTPv1 (TS 29.060 §7.7.51): detected when {@code byte[0] == 0x56} (IE-Type 86).
 *       Header is 3 bytes (IE-type + 2-byte length), followed by GLT and optional
 *       sub-type byte, then 7-byte location data (MCC/MNC + location fields).</li>
 *   <li>GTPv2 (TS 29.274 §8.21): {@code byte[0]} is a flags bitmask where each bit
 *       selects a present location block (CGI, SAI, RAI, TAI, ECGI, …).</li>
 *   <li>Raw fallback: no header, MCC/MNC bytes at offset 0.</li>
 * </ul>
 *
 * <h3>MCC/MNC BCD encoding (TS 24.008 §10.5.1.3)</h3>
 * <pre>
 *   Byte 0: MCC digit 2 (high nibble) | MCC digit 1 (low nibble)
 *   Byte 1: MNC digit 3 (high nibble) | MCC digit 3 (low nibble)
 *   Byte 2: MNC digit 2 (high nibble) | MNC digit 1 (low nibble)
 *   MNC digit 3 == 0xF → 2-digit MNC
 * </pre>
 */
public final class UliDecoder {

    private UliDecoder() {}

    // ── Constants ─────────────────────────────────────────────────────────────
    /** GTPv1 ULI IE-Type byte value (decimal 86). */
    private static final int GTPV1_IE_TYPE  = 0x56;

    // GTPv1 Geographic Location Types (byte[3])
    private static final int GLT_CGI = 0x00;
    private static final int GLT_SAI = 0x01;
    private static final int GLT_RAI = 0x02;

    // GTPv1 Sub-Type byte (byte[4]) — carrier extension for ECGI inside GTPv1
    private static final int SUBTYPE_ECGI = 0x10;

    // GTPv2 flag bits (byte[0] when not GTPv1)
    private static final int F_CGI  = 0x01;
    private static final int F_SAI  = 0x02;
    private static final int F_RAI  = 0x04;
    private static final int F_TAI  = 0x08;
    private static final int F_ECGI = 0x10;
    private static final int F_LAI  = 0x20;

    // =========================================================================
    // Public entry point
    // =========================================================================

    /**
     * Decodes the ULI hex string into a {@link CellInfo}, or {@code null} if
     * the input is blank, too short, or cannot be matched to any known format.
     *
     * @param uliHex hex string (spaces/dashes tolerated); may be {@code null}
     */
    public static CellInfo decode(String uliHex) {
        if (uliHex == null || uliHex.isBlank()) return null;
        byte[] b = hexToBytes(uliHex.replaceAll("[^0-9A-Fa-f]", ""));
        if (b == null || b.length < 4) return null;

        int first = b[0] & 0xFF;

        // ── GTPv1: IE-Type = 0x56 ────────────────────────────────────────────
        if (first == GTPV1_IE_TYPE && b.length >= 9) {
            CellInfo ci = decodeGtpv1(b);
            if (ci != null) return ci;
        }

        // ── GTPv2: flags bitmask ──────────────────────────────────────────────
        if (first != GTPV1_IE_TYPE
            && (first & (F_CGI|F_SAI|F_RAI|F_TAI|F_ECGI|F_LAI)) != 0) {
            CellInfo ci = decodeGtpv2(b, first);
            if (ci != null) return ci;
        }

        // ── Raw fallback: treat bytes 0-6 as bare CGI (no header) ────────────
        if (b.length >= 7) {
            CellInfo ci = parseCgi(b, 0, "CGI/2G");
            if (ci != null && ci.mcc >= 100) return ci;
        }
        return null;
    }

    // =========================================================================
    // GTPv1 decoder
    // =========================================================================

    /**
     * Layout (confirmed against real captures):
     * <pre>
     *   b[0]    = 0x56 = IE-Type
     *   b[1-2]  = Length (big-endian)
     *   b[3]    = GLT (Geographic Location Type)
     *   b[4]    = Sub-type / extended indicator
     *             0x10 → ECGI extension: MCC/MNC start at b[5], ECI at b[8]
     *             0x00 → standard: location data starts at b[4] (GLT drives format)
     * </pre>
     */
    private static CellInfo decodeGtpv1(byte[] b) {
        int glt    = b[3] & 0xFF;
        int subTyp = b[4] & 0xFF;

        if (subTyp == SUBTYPE_ECGI) {
            // Extended ECGI encoding: MCC/MNC at offset 5, ECI (4 bytes) at offset 8
            if (b.length < 12) return null;
            return parseEcgi(b, 5, "ECGI/LTE");
        }

        // Standard GTPv1: location data (MCC/MNC + fields) starts at offset 4
        return switch (glt) {
            case GLT_CGI -> parseCgi(b, 4, "CGI/2G");
            case GLT_SAI -> parseCgi(b, 4, "SAI/2G");   // same layout as CGI
            case GLT_RAI -> parseRai(b, 4);
            default      -> parseCgi(b, 4, "CGI/2G");
        };
    }

    // =========================================================================
    // GTPv2 decoder
    // =========================================================================

    private static CellInfo decodeGtpv2(byte[] b, int flags) {
        int off = 1;  // skip flags byte
        if ((flags & F_CGI)  != 0 && off + 7 <= b.length) {
            CellInfo c = parseCgi(b, off, "CGI/2G"); off += 7;
            if (c != null && c.mcc >= 100) return c;
        }
        if ((flags & F_SAI)  != 0 && off + 7 <= b.length) {
            CellInfo c = parseCgi(b, off, "SAI/2G"); off += 7;
            if (c != null && c.mcc >= 100) return c;
        }
        if ((flags & F_RAI)  != 0 && off + 7 <= b.length) {
            CellInfo c = parseRai(b, off); off += 7;
            if (c != null && c.mcc >= 100) return c;
        }
        if ((flags & F_TAI)  != 0 && off + 5 <= b.length) {
            CellInfo c = parseTai(b, off); off += 5;
            if (c != null && c.mcc >= 100) return c;
        }
        if ((flags & F_ECGI) != 0 && off + 7 <= b.length) {
            CellInfo c = parseEcgi(b, off, "ECGI/LTE");
            if (c != null && c.mcc >= 100) return c;
        }
        return null;
    }

    // =========================================================================
    // Per-format parsers
    // =========================================================================

    /** CGI / SAI: MCC/MNC(3) + LAC(2) + CI/SAC(2) = 7 bytes from {@code off}. */
    private static CellInfo parseCgi(byte[] b, int off, String type) {
        if (off + 7 > b.length) return null;
        int[] mm  = mccMnc(b, off);
        int   lac = u16(b, off + 3);
        int   ci  = u16(b, off + 5);
        if (mm[0] < 100) return null;
        return new CellInfo(type, mm[0], mm[1], lac, ci, -1);
    }

    /** RAI: MCC/MNC(3) + LAC(2) + RAC(1) + Spare(1) = 7 bytes. */
    private static CellInfo parseRai(byte[] b, int off) {
        if (off + 7 > b.length) return null;
        int[] mm  = mccMnc(b, off);
        int   lac = u16(b, off + 3);
        int   rac = b[off + 5] & 0xFF;
        if (mm[0] < 100) return null;
        return new CellInfo("RAI/3G", mm[0], mm[1], lac, rac, -1);
    }

    /** TAI: MCC/MNC(3) + TAC(2) = 5 bytes. */
    private static CellInfo parseTai(byte[] b, int off) {
        if (off + 5 > b.length) return null;
        int[] mm  = mccMnc(b, off);
        int   tac = u16(b, off + 3);
        if (mm[0] < 100) return null;
        return new CellInfo("TAI/LTE", mm[0], mm[1], tac, -1, -1);
    }

    /**
     * ECGI: MCC/MNC(3) + ECI(4) = 7 bytes from {@code off}.
     * ECI is 28-bit: upper nibble of first byte is spare.
     * <pre>
     *   eNB-ID  = ECI >> 8   (20 bits)
     *   Cell-ID = ECI & 0xFF  (8 bits)
     * </pre>
     */
    private static CellInfo parseEcgi(byte[] b, int off, String type) {
        if (off + 7 > b.length) return null;
        int[] mm  = mccMnc(b, off);
        if (mm[0] < 100) return null;
        long eci  = ((long)(b[off+3] & 0x0F) << 24)
                    | ((long)(b[off+4] & 0xFF) << 16)
                    | ((long)(b[off+5] & 0xFF) <<  8)
                    |  (long)(b[off+6] & 0xFF);
        int eNbId  = (int)(eci >> 8);
        int cellId = (int)(eci & 0xFF);
        return new CellInfo(type, mm[0], mm[1], eNbId, cellId, (int) eci);
    }

    // =========================================================================
    // MCC/MNC BCD (TS 24.008 §10.5.1.3)
    // =========================================================================

    private static int[] mccMnc(byte[] b, int off) {
        int d1 =  b[off]     & 0x0F;   // MCC digit 1
        int d2 = (b[off]     >> 4) & 0x0F;   // MCC digit 2
        int d3 =  b[off + 1] & 0x0F;   // MCC digit 3
        int n3 = (b[off + 1] >> 4) & 0x0F;   // MNC digit 3 (0xF = 2-digit MNC)
        int n1 =  b[off + 2] & 0x0F;   // MNC digit 1
        int n2 = (b[off + 2] >> 4) & 0x0F;   // MNC digit 2
        int mcc = d1 * 100 + d2 * 10 + d3;
        int mnc = (n3 == 0xF) ? n1 * 10 + n2 : n1 * 100 + n2 * 10 + n3;
        return new int[]{ mcc, mnc };
    }

    // =========================================================================
    // Byte utilities
    // =========================================================================

    private static int u16(byte[] b, int off) {
        return ((b[off] & 0xFF) << 8) | (b[off + 1] & 0xFF);
    }

    private static byte[] hexToBytes(String hex) {
        if (hex.length() % 2 != 0) hex = "0" + hex;
        byte[] r = new byte[hex.length() / 2];
        for (int i = 0; i < r.length; i++) {
            int hi = Character.digit(hex.charAt(i * 2),     16);
            int lo = Character.digit(hex.charAt(i * 2 + 1), 16);
            if (hi < 0 || lo < 0) return null;
            r[i] = (byte)((hi << 4) | lo);
        }
        return r;
    }

    // =========================================================================
    // Result DTO
    // =========================================================================

    /**
     * Decoded cell identity from a ULI element.
     * Fields not applicable for the detected network type are {@code -1}.
     */
    public static final class CellInfo {
        /** e.g. "CGI/2G", "ECGI/LTE". */
        public final String networkType;
        /** Mobile Country Code (e.g. 262). */
        public final int mcc;
        /** Mobile Network Code (e.g. 2). */
        public final int mnc;
        /**
         * LAC / TAC / eNB-ID depending on network type.
         * {@code -1} if unavailable.
         */
        public final int lac;
        /**
         * Cell-ID (CI) for 2G/3G, lower 8 bits of ECI for LTE.
         * {@code -1} if unavailable.
         */
        public final int cellId;
        /**
         * Full 28-bit E-UTRAN Cell Identifier (ECI) for LTE/ECGI.
         * {@code -1} for non-LTE types.
         */
        public final int eci;

        CellInfo(String networkType, int mcc, int mnc, int lac, int cellId, int eci) {
            this.networkType = networkType;
            this.mcc    = mcc;
            this.mnc    = mnc;
            this.lac    = lac;
            this.cellId = cellId;
            this.eci    = eci;
        }

        /** {@code true} when the fields are sufficient for an OpenCelliD query. */
        public boolean isQueryable() {
            return mcc >= 100 && mnc >= 0 && lac >= 0 && cellId >= 0;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder()
                    .append(networkType)
                    .append(" MCC=").append(mcc)
                    .append(" MNC=").append(mnc);
            if (lac    >= 0) sb.append(" LAC=").append(lac);
            if (cellId >= 0) sb.append(" CI=").append(cellId);
            if (eci    >= 0) sb.append(" ECI=").append(eci);
            return sb.toString();
        }
    }
}
