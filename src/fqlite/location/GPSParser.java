package fqlite.location;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GPSParser {


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
        try { return isValidLat(Double.parseDouble(s.trim())); }
        catch (NumberFormatException e) { return false; }
    }

    public static boolean looksLikeLongitude(String s) {
        try { return isValidLon(Double.parseDouble(s.trim())); }
        catch (NumberFormatException e) { return false; }
    }

    private static boolean isValidLat(double v) { return v >= -90  && v <= 90;  }
    private static boolean isValidLon(double v) { return v >= -180 && v <= 180; }


}
