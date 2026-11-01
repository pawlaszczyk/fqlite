package fqlite.location;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GPSParser {


    // -------------------------------------------------------------------------
    // ETSI compass-prefixed coordinates (DMS and decimal-degree)
    // -------------------------------------------------------------------------

    /**
     * Converts an ETSI DMS coordinate (e.g. {@code N515715} or
     * {@code E0144130}) into decimal degrees, rounded to 6 decimal places.
     */
    public static Double parseDmsCoordinate(String raw) {
        if (raw == null) return null;
        raw = raw.trim();
        if (raw.isEmpty()) return null;

        char hemi = Character.toUpperCase(raw.charAt(0));
        if (hemi != 'N' && hemi != 'S' && hemi != 'E' && hemi != 'W') return null;
        String digits = raw.substring(1);
        int deg, min, sec;

        // Some real-world exports (v1.26.1, hybrid NTSU+partyInformation shape)
        // append decimal fractional seconds to the integer DMS string, e.g.
        // "N513746.02" (= 51°37'46.02") or "E0133743.91" (= 13°37'43.91").
        // Split on the decimal point so the length check below only sees the
        // integer digits, then fold the fraction back in during degree conversion.
        double fracSec = 0.0;
        int dotIdx = digits.indexOf('.');
        if (dotIdx >= 0) {
            String fracStr = digits.substring(dotIdx); // ".02"
            try { fracSec = Double.parseDouble("0" + fracStr); } catch (NumberFormatException ignored) { }
            digits = digits.substring(0, dotIdx);
        }

        if (hemi == 'N' || hemi == 'S') {
            if (digits.length() != 6) return null;
            deg = Integer.parseInt(digits.substring(0, 2));
            min = Integer.parseInt(digits.substring(2, 4));
            sec = Integer.parseInt(digits.substring(4, 6));
        } else {
            // Allow exactly one optional leading-zero omission (6 → 7 digits).
            // Do NOT pad shorter strings: "013" from "E013.7655" (decimal degrees)
            // must fall through to parseDecDegCoordinate, not be mis-parsed as DMS.
            if (digits.length() == 6) digits = "0" + digits;
            if (digits.length() != 7) return null;
            deg = Integer.parseInt(digits.substring(0, 3));
            min = Integer.parseInt(digits.substring(3, 5));
            sec = Integer.parseInt(digits.substring(5, 7));
        }

        double decimal = deg + min / 60.0 + (sec + fracSec) / 3600.0;
        if (hemi == 'S' || hemi == 'W') {
            decimal = -decimal;
        }
        return Math.round(decimal * 1_000_000.0) / 1_000_000.0;
    }

    /**
     * Converts a decimal-degree coordinate string as used by
     * {@code <geoCoordinatesDec>} (e.g. {@code N51.5046}, {@code E013.7854})
     * into a signed decimal degree value, rounded to 6 decimal places.
     * Returns {@code null} if the input cannot be parsed.
     */
    public static Double parseDecDegCoordinate(String raw) {
        if (raw == null) return null;
        raw = raw.trim();
        if (raw.isEmpty()) return null;
        char hemi = Character.toUpperCase(raw.charAt(0));
        if (hemi != 'N' && hemi != 'S' && hemi != 'E' && hemi != 'W') return null;
        try {
            double val = Double.parseDouble(raw.substring(1));
            if (hemi == 'S' || hemi == 'W') val = -val;
            return Math.round(val * 1_000_000.0) / 1_000_000.0;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Last-resort fallback for {@code <geoCoordinatesDec>}-style values that
     * carry no {@code N/S/E/W} hemisphere prefix at all — some real-world
     * exports store the decimal-degree value as a plain signed number (e.g.
     * {@code 51.3818}), relying on the fact that German fixed-network/mobile
     * coordinates are unambiguous without it. Returns {@code null} unless the
     * string is a plausible latitude/longitude value ({@code -180}..{@code 180}),
     * so it can't accidentally swallow unrelated numeric fields.
     */
    public static Double parsePlainDecCoordinate(String raw) {
        if (raw == null) return null;
        raw = raw.trim();
        if (raw.isEmpty()) return null;
        char c0 = raw.charAt(0);
        if (!Character.isDigit(c0) && c0 != '-' && c0 != '+') return null;
        try {
            double val = Double.parseDouble(raw);
            if (val < -180 || val > 180) return null;
            return Math.round(val * 1_000_000.0) / 1_000_000.0;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Tries DMS parsing first ({@link #parseDmsCoordinate}); if that returns
     * {@code null} (e.g. for a decimal-degree value like {@code N51.5046}),
     * falls back to {@link #parseDecDegCoordinate}, and finally to
     * {@link #parsePlainDecCoordinate} for hemisphere-less decimal values.
     * Handles both {@code <geoCoordinates>} and {@code <geoCoordinatesDec>}
     * raw values, with or without a compass prefix.
     */
    public static Double parseAnyCoordinate(String raw) {
        if (raw == null) return null;
        Double dms = parseDmsCoordinate(raw);
        if (dms != null) return dms;
        Double dec = parseDecDegCoordinate(raw);
        if (dec != null) return dec;
        return parsePlainDecCoordinate(raw);
    }


    // -------------------------------------------------------------------------
    // Geo patterns
    // -------------------------------------------------------------------------

    /** Decimal lat/lon pair in a single cell, e.g. {@code "48.1351,11.5820"}. */
    private static final Pattern LATLON_PATTERN =
            Pattern.compile("(-?\\d{1,3}\\.\\d+)[,;\\s]+(-?\\d{1,3}\\.\\d+)");

    /** WKT POINT geometry, e.g. {@code "POINT(11.5820 48.1351)"}. */
    private static final Pattern WKT_POINT =
            Pattern.compile("POINT\\((-?\\d+\\.\\d+)\\s+(-?\\d+\\.\\d+)\\)",
                    Pattern.CASE_INSENSITIVE);

    public static GeoCoordinate parseLatLonPair(String s) {
        if (s == null || s.isBlank()) return null;

        Matcher wkt = WKT_POINT.matcher(s);
        if (wkt.find()) {
            try {
                double lon = Double.parseDouble(wkt.group(1));
                double lat = Double.parseDouble(wkt.group(2));
                if (isValidLat(lat) && isValidLon(lon)) return new GeoCoordinate(lat, lon);
            } catch (NumberFormatException ignored) {}
        }
        Matcher ll = LATLON_PATTERN.matcher(s);
        if (ll.find()) {
            try {
                double a = Double.parseDouble(ll.group(1));
                double b = Double.parseDouble(ll.group(2));
                if (isValidLat(a) && isValidLon(b)) return new GeoCoordinate(a, b);
            } catch (NumberFormatException ignored) {}
        }
        return null;
    }


    // -------------------------------------------------------------------------
    // Cell-level heuristics
    // -------------------------------------------------------------------------

    private boolean looksLikeLatLonPair(String s) { return parseLatLonPair(s) != null; }

    public static boolean looksLikeLatitude(String s) {
        if (s == null) return false;
        try { return isValidLat(Double.parseDouble(s.trim())); }
        catch (NumberFormatException e) {
            Double v = parseAnyCoordinate(s);
            return v != null && isValidLat(v);
        }
    }

    public static boolean looksLikeLongitude(String s) {
        if (s == null) return false;
        try { return isValidLon(Double.parseDouble(s.trim())); }
        catch (NumberFormatException e) {
            Double v = parseAnyCoordinate(s);
            return v != null && isValidLon(v);
        }
    }

    private static boolean isValidLat(double v) { return v >= -90  && v <= 90;  }
    private static boolean isValidLon(double v) { return v >= -180 && v <= 180; }


}
