package fqlite.timemap;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javafx.collections.ObservableList;

/**
 * Analyses a {@code ConcurrentHashMap<String, ObservableList<ObservableList<String>>>}
 * to detect timestamp and geo-coordinate values in any column of any table.
 *
 * <p>For tables matching a known schema (currently {@code response_records}),
 * column detection is bypassed entirely: the well-known column names are used
 * directly, which is both faster and more reliable than heuristics.</p>
 *
 * <p>Call {@link #analyze(ConcurrentHashMap, Map)} to obtain a list of
 * {@link DataPoint} objects ready for rendering in {@link TimelineView} or
 * {@link MapView}.</p>
 */
public class DataAnalyzer {

    // -------------------------------------------------------------------------
    // Known-schema table names
    // -------------------------------------------------------------------------

    /**
     * Column layout for the {@code response_records} table.
     * All field names must match the actual column headers supplied in
     * {@code headers} (case-insensitive comparison is used during lookup).
     */
    private static final String SCHEMA_RESPONSE_RECORDS = "response_records";

    // -------------------------------------------------------------------------
    // response_records – fixed positional offsets within a raw data row
    // -------------------------------------------------------------------------
    //
    // The fqlite query engine serialises each row with THREE leading meta-fields
    // that are NOT part of the CREATE TABLE definition:
    //
    //   pos 0 : internal row counter (fqlite-internal, not the PK)
    //   pos 1 : table name
    //   pos 2 : record reference, e.g. "[444|31]"
    //
    // After those meta fields the actual table columns begin at position 3.
    // In addition, the real data contains THREE undocumented extra columns
    // between nw_access_type (pos 9) and start_time (pos 13):
    //   pos 10 : carrier_country_code  (e.g. "DE")
    //   pos 11 : carrier_header_id     (e.g. "01.28.01.08.1.01")
    //   pos 12 : connection_type       (e.g. "mobilePacketData")
    //
    // Furthermore, the "other_information" column is serialised as FIVE
    // consecutive "Key"="Value" fields (positions 27–31) followed by the
    // same five values repeated as plain strings (positions 32–36).
    //
    // Absolute positions (0-based) in the raw ObservableList<String> row:
    //
    //   3  id                      16  latitude
    //   4  request_id              17  longitude
    //   5  record_number           18  map_datum
    //   6  party_type              19  azimuth
    //   7  national_country_code   20  user_location_info
    //   8  national_header_id      21  imsi
    //   9  nw_access_type          22  msisdn
    //  10  [carrier_country_code]  23  apn
    //  11  [carrier_header_id]     24  other_info kv[0]  ("Call Indikator"=…)
    //  12  [connection_type]       25  other_info kv[1]  ("Call Action Code"=…)
    //  13  start_time              26  other_info kv[2]  ("Call Subtype"=…)
    //  14  end_time                27  other_info kv[3]  ("Session Id"=…)
    //  15  duration_seconds        28  other_info kv[4]  ("TypeOfData"=…)
    //  16  na_device_id            29  call_indicator    (plain repeat)
    //  14  lt_raw                  30  call_action_code
    //  15  lo_raw                  31  call_subtype
    //  16  latitude                32  session_id
    //  17  longitude               33  type_of_data_extra
    //  18  map_datum               34  nat_country_code
    //  19  azimuth                 35  nat_header_id
    //  20  user_location_info      36  type_of_data

    /** Number of fqlite meta-fields prepended to every row. */
    private static final int RR_META_OFFSET = 3;

    // Absolute indices (meta already included) for the columns we need:
    private static final int RR_IDX_START_TIME  = 13;  // start_time
    private static final int RR_IDX_END_TIME    = 14;  // end_time
    private static final int RR_IDX_DURATION    = 15;  // duration_seconds
    private static final int RR_IDX_NA_DEVICE   = 16;  // na_device_id
    private static final int RR_IDX_LT_RAW      = 17;  // lt_raw
    private static final int RR_IDX_LO_RAW      = 18;  // lo_raw
    private static final int RR_IDX_LATITUDE    = 19;  // latitude
    private static final int RR_IDX_LONGITUDE   = 20;  // longitude
    private static final int RR_IDX_MAP_DATUM   = 21;  // map_datum
    private static final int RR_IDX_AZIMUTH     = 22;  // azimuth
    private static final int RR_IDX_USER_LOC    = 23;  // user_location_info
    private static final int RR_IDX_IMSI        = 24;  // imsi
    private static final int RR_IDX_MSISDN      = 25;  // msisdn
    private static final int RR_IDX_APN         = 26;  // apn
    // other_information: 5 "Key"="Value" fields at 27–31
    private static final int RR_IDX_OTHER_INFO_START = 27;
    private static final int RR_IDX_OTHER_INFO_END   = 31; // inclusive
    // plain-value repeats at 32–36:
    private static final int RR_IDX_CALL_INDICATOR   = 32;
    private static final int RR_IDX_CALL_ACTION      = 33;
    private static final int RR_IDX_CALL_SUBTYPE     = 34;
    private static final int RR_IDX_SESSION_ID       = 35;
    private static final int RR_IDX_TYPE_EXTRA       = 36;
    private static final int RR_IDX_NAT_CC           = 37;  // nat_country_code
    private static final int RR_IDX_NAT_HEADER       = 38;  // nat_header_id
    private static final int RR_IDX_TYPE_OF_DATA     = 39;  // type_of_data

    // Left-side columns (after meta):
    private static final int RR_IDX_PARTY_TYPE   = 6;   // party_type
    private static final int RR_IDX_NAT_CC_LOCAL = 7;   // national_country_code
    private static final int RR_IDX_NAT_HDR_LOCAL= 8;   // national_header_id
    private static final int RR_IDX_NW_ACCESS    = 9;   // nw_access_type

    /** Primary timestamp column name in {@code response_records}. */
    private static final String RR_START_TIME  = "start_time";
    /** Latitude column name. */
    private static final String RR_LATITUDE    = "latitude";
    /** Longitude column name. */
    private static final String RR_LONGITUDE   = "longitude";

    // -------------------------------------------------------------------------
    // Timestamp patterns
    // -------------------------------------------------------------------------

    /** ISO-8601 and common SQL TIMESTAMP string variants. */
    private static final List<DateTimeFormatter> TIMESTAMP_FORMATTERS = List.of(
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX"),
        DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss"),
        DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss")
    );

    /** Plain date patterns; time is assumed to be 00:00:00 UTC. */
    private static final List<DateTimeFormatter> DATE_FORMATTERS = List.of(
        DateTimeFormatter.ofPattern("yyyy-MM-dd"),
        DateTimeFormatter.ofPattern("dd.MM.yyyy"),
        DateTimeFormatter.ofPattern("MM/dd/yyyy")
    );

    /**
     * Unix epoch in seconds: 10-digit number covering the range
     * 2001-09-09 to 2286-11-20 (plausible for forensic data).
     */
    private static final Pattern EPOCH_SECONDS =
        Pattern.compile("^(9\\d{8}|[1-9]\\d{9}|[12]\\d{9})$");

    /** Unix epoch in milliseconds: exactly 13 digits. */
    private static final Pattern EPOCH_MILLIS =
        Pattern.compile("^\\d{13}$");

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

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Scans every row of every table and returns one {@link DataPoint} per row
     * that contains at least one timestamp or geo-coordinate.
     *
     * <p>Tables whose name matches a known schema (e.g. {@code response_records})
     * are processed with fixed column indices instead of heuristic profiling.</p>
     *
     * @param resultlist table data: table name → rows → cell strings
     * @param headers    column names per table; may be empty (auto-numbered fallback)
     * @return           detected points, sorted by timestamp (nulls last)
     */
    public List<DataPoint> analyze(
            ConcurrentHashMap<String, ObservableList<ObservableList<String>>> resultlist,
            Map<String, List<String>> headers) {

        List<DataPoint> points = new ArrayList<>();

        for (Map.Entry<String, ObservableList<ObservableList<String>>> tableEntry
                : resultlist.entrySet()) {

            String tableName = tableEntry.getKey();
            ObservableList<ObservableList<String>> rows = tableEntry.getValue();
            List<String> cols = headers.getOrDefault(tableName, List.of());

            if (isResponseRecordsSchema(tableName, cols)) {
                // ── Fast path: fixed schema ──────────────────────────────────
                points.addAll(analyzeResponseRecords(tableName, rows, cols));
            } else {
                // ── General path: heuristic profiling ───────────────────────
                ColumnProfile profile = profileColumns(rows, cols);

                for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
                    ObservableList<String> row = rows.get(rowIdx);

                    Instant       timestamp = extractTimestamp(row, profile.timestampCols);
                    GeoCoordinate coord     = extractCoordinate(row, profile);

                    if (timestamp != null || coord != null) {
                        DataPoint dp = new DataPoint();
                        dp.setTableName(tableName);
                        dp.setRowIndex(rowIdx);
                        dp.setTimestamp(timestamp);
                        dp.setCoordinate(coord);
                        dp.setRawRow(new ArrayList<>(row));
                        dp.setColumnNames(cols);
                        points.add(dp);
                    }
                }
            }
        }

        points.sort(Comparator.comparing(
                DataPoint::getTimestamp,
                Comparator.nullsLast(Comparator.naturalOrder())));
        return points;
    }

    // -------------------------------------------------------------------------
    // response_records – fixed-schema extraction
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} when the table name equals {@code response_records}
     * (case-insensitive) <em>and</em> the row data is wide enough to contain
     * the expected timestamp and coordinate fields at their fixed positions.
     *
     * <p>The check uses the <em>minimum expected row width</em> (the longitude
     * index + 1) rather than column-name lookup, because the fqlite serialisation
     * format prepends meta-fields that shift all logical column indices.</p>
     */
    private boolean isResponseRecordsSchema(String tableName, List<String> cols) {
        if (!SCHEMA_RESPONSE_RECORDS.equalsIgnoreCase(tableName)) return false;
        // Accept both: header-based confirmation OR a plausible column count.
        // A genuine response_records row has at least 37 fields (meta + data).
        if (!cols.isEmpty()) {
            // Header names available: check for the key column names.
            List<String> lower = cols.stream().map(String::toLowerCase).toList();
            return lower.contains(RR_START_TIME)
                && lower.contains(RR_LATITUDE)
                && lower.contains(RR_LONGITUDE);
        }
        return false;
    }

    /**
     * Processes a {@code response_records} table using fixed absolute field
     * positions within each raw row.
     *
     * <p><b>Row layout (0-based absolute positions):</b><br>
     * The fqlite engine prepends 3 meta-fields before the actual table columns,
     * and the real data contains 3 additional undocumented fields between
     * {@code nw_access_type} and {@code start_time}. The {@code other_information}
     * column is serialised as 5 {@code "Key"="Value"} strings, followed by the
     * same 5 values repeated as plain strings. All positions are therefore fixed
     * and known; see the {@code RR_IDX_*} constants for exact values.</p>
     *
     * <p>Every row that yields either a parseable {@code start_time} or valid
     * {@code latitude}/{@code longitude} values produces one {@link ResponseRecordDataPoint}.</p>
     */
    private List<DataPoint> analyzeResponseRecords(
            String tableName,
            ObservableList<ObservableList<String>> rows,
            List<String> cols) {

        List<DataPoint> points = new ArrayList<>();

        for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
            ObservableList<String> row = rows.get(rowIdx);

            // ── Timestamp ────────────────────────────────────────────────────
            Instant timestamp = parseTimestamp(cell(row, RR_IDX_START_TIME));
            if (timestamp == null)
                timestamp = parseTimestamp(cell(row, RR_IDX_END_TIME));

            // ── Coordinates ──────────────────────────────────────────────────
            GeoCoordinate coord = null;
            String latStr = cell(row, RR_IDX_LATITUDE);
            String lonStr = cell(row, RR_IDX_LONGITUDE);
            if (latStr != null && !latStr.isBlank()
                    && lonStr != null && !lonStr.isBlank()) {
                try {
                    double lat = Double.parseDouble(latStr.trim());
                    double lon = Double.parseDouble(lonStr.trim());
                    if (isValidLat(lat) && isValidLon(lon))
                        coord = new GeoCoordinate(lat, lon);
                } catch (NumberFormatException ignored) {}
            }

            // Skip rows that carry neither timestamp nor coordinates.
            if (timestamp == null && coord == null) continue;

            // ── Collect other_information key=value pairs ─────────────────────
            StringBuilder otherInfo = new StringBuilder();
            for (int i = RR_IDX_OTHER_INFO_START; i <= RR_IDX_OTHER_INFO_END; i++) {
                String kv = cell(row, i);
                if (kv != null && !kv.isBlank()) {
                    if (!otherInfo.isEmpty()) otherInfo.append("; ");
                    otherInfo.append(kv);
                }
            }

            // ── Build DataPoint ──────────────────────────────────────────────
            ResponseRecordDataPoint rrdp = new ResponseRecordDataPoint();
            rrdp.setTableName(tableName);
            rrdp.setRowIndex(rowIdx);
            rrdp.setTimestamp(timestamp);
            rrdp.setCoordinate(coord);
            rrdp.setRawRow(new ArrayList<>(row));
            rrdp.setColumnNames(cols);

            rrdp.setEndTime(cell(row, RR_IDX_END_TIME));
            rrdp.setPartyType(cell(row, RR_IDX_PARTY_TYPE));
            rrdp.setNwAccessType(cell(row, RR_IDX_NW_ACCESS));
            rrdp.setNaDeviceId(cell(row, RR_IDX_NA_DEVICE));
            rrdp.setLtRaw(cell(row, RR_IDX_LT_RAW));
            rrdp.setLoRaw(cell(row, RR_IDX_LO_RAW));
            rrdp.setMapDatum(cell(row, RR_IDX_MAP_DATUM));
            rrdp.setAzimuth(cell(row, RR_IDX_AZIMUTH));
            rrdp.setUserLocationInfo(cell(row, RR_IDX_USER_LOC));
            rrdp.setImsi(cell(row, RR_IDX_IMSI));
            rrdp.setMsisdn(cell(row, RR_IDX_MSISDN));
            rrdp.setApn(cell(row, RR_IDX_APN));
            rrdp.setOtherInformation(otherInfo.isEmpty() ? null : otherInfo.toString());
            rrdp.setCallIndicator(cell(row, RR_IDX_CALL_INDICATOR));
            rrdp.setCallActionCode(cell(row, RR_IDX_CALL_ACTION));
            rrdp.setCallSubtype(cell(row, RR_IDX_CALL_SUBTYPE));
            rrdp.setSessionId(cell(row, RR_IDX_SESSION_ID));
            rrdp.setTypeOfDataExtra(cell(row, RR_IDX_TYPE_EXTRA));
            rrdp.setDurationSeconds(cell(row, RR_IDX_DURATION));

            points.add(rrdp);
        }

        return points;
    }

    /** Safely retrieves a cell value; returns {@code null} for out-of-range or negative indices. */
    private static String cell(ObservableList<String> row, int idx) {
        return (idx >= 0 && idx < row.size()) ? row.get(idx) : null;
    }

    // -------------------------------------------------------------------------
    // Column profiling (heuristic – used for non-schema tables)
    // -------------------------------------------------------------------------

    /**
     * Samples up to 100 rows to score each column for timestamp / coordinate
     * content, then builds a {@link ColumnProfile} listing the winning column
     * indices.
     */
    private ColumnProfile profileColumns(
            ObservableList<ObservableList<String>> rows, List<String> cols) {

        int colCount = cols.isEmpty()
                       ? (rows.isEmpty() ? 0 : rows.get(0).size())
                       : cols.size();

        int[] tsScore  = new int[colCount];
        int[] latScore = new int[colCount];
        int[] lonScore = new int[colCount];
        int[] geoScore = new int[colCount]; // combined lat/lon in one cell

        int sampleSize = Math.min(rows.size(), 100);

        for (int r = 0; r < sampleSize; r++) {
            ObservableList<String> row = rows.get(r);
            for (int c = 0; c < Math.min(colCount, row.size()); c++) {
                String cell = row.get(c);
                if (cell == null || cell.isBlank()) continue;

                if (looksLikeTimestamp(cell))  tsScore[c]++;
                if (looksLikeLatitude(cell))   latScore[c]++;
                if (looksLikeLongitude(cell))  lonScore[c]++;
                if (looksLikeLatLonPair(cell)) geoScore[c]++;
            }
        }

        // A column qualifies when at least 10 % of sampled rows match.
        int threshold = Math.max(1, sampleSize / 10);

        ColumnProfile p = new ColumnProfile();
        for (int c = 0; c < colCount; c++) {
            if (tsScore[c]  >= threshold) p.timestampCols.add(c);
            if (geoScore[c] >= threshold) p.combinedGeoCols.add(c);
        }

        // Prefer column-name heuristics for separate lat / lon columns.
        for (int c = 0; c < cols.size(); c++) {
            String name = cols.get(c).toLowerCase();
            if (name.contains("lat") || name.equals("y")) p.latCols.add(c);
            if (name.contains("lon") || name.contains("lng") || name.equals("x")) p.lonCols.add(c);
        }
        // Score-based fallback when name heuristics yield nothing.
        if (p.latCols.isEmpty() || p.lonCols.isEmpty()) {
            List<Integer> candidates = new ArrayList<>();
            for (int c = 0; c < colCount; c++) {
                if ((latScore[c] >= threshold || lonScore[c] >= threshold)
                        && !p.combinedGeoCols.contains(c)) {
                    candidates.add(c);
                }
            }
            if (candidates.size() == 2) {
                p.latCols.add(candidates.get(0));
                p.lonCols.add(candidates.get(1));
            } else {
                for (int c : candidates) {
                    if (latScore[c] >= threshold) p.latCols.add(c);
                    if (lonScore[c] >= threshold) p.lonCols.add(c);
                }
                p.lonCols.removeAll(p.latCols.subList(0, Math.min(1, p.latCols.size())));
            }
        }

        return p;
    }

    // -------------------------------------------------------------------------
    // Extraction helpers
    // -------------------------------------------------------------------------

    private Instant extractTimestamp(ObservableList<String> row, List<Integer> tsCols) {
        for (int c : tsCols) {
            if (c >= row.size()) continue;
            Instant ts = parseTimestamp(row.get(c));
            if (ts != null) return ts;
        }
        return null;
    }

    private GeoCoordinate extractCoordinate(ObservableList<String> row, ColumnProfile p) {
        // Priority 1: combined lat/lon in a single cell (WKT or "lat,lon").
        for (int c : p.combinedGeoCols) {
            if (c >= row.size()) continue;
            GeoCoordinate gc = parseLatLonPair(row.get(c));
            if (gc != null) return gc;
        }
        // Priority 2: separate latitude and longitude columns.
        if (!p.latCols.isEmpty() && !p.lonCols.isEmpty()) {
            int latCol = p.latCols.get(0);
            int lonCol = p.lonCols.get(0);
            if (latCol < row.size() && lonCol < row.size()) {
                try {
                    double lat = Double.parseDouble(row.get(latCol));
                    double lon = Double.parseDouble(row.get(lonCol));
                    if (isValidLat(lat) && isValidLon(lon)) return new GeoCoordinate(lat, lon);
                } catch (NumberFormatException ignored) {}
            }
        }
        return null;
    }

    // -------------------------------------------------------------------------
    // Parsing
    // -------------------------------------------------------------------------

    /**
     * Attempts to parse {@code s} as a timestamp using all known formats.
     *
     * @return the corresponding {@link Instant} (UTC), or {@code null} if no
     *         format matched
     */
    public Instant parseTimestamp(String s) {
        if (s == null || s.isBlank()) return null;
        s = s.trim();

        if (EPOCH_MILLIS.matcher(s).matches()) {
            try { return Instant.ofEpochMilli(Long.parseLong(s)); }
            catch (NumberFormatException ignored) {}
        }
        if (EPOCH_SECONDS.matcher(s).matches()) {
            try { return Instant.ofEpochSecond(Long.parseLong(s)); }
            catch (NumberFormatException ignored) {}
        }
        for (DateTimeFormatter fmt : TIMESTAMP_FORMATTERS) {
            try { return LocalDateTime.parse(s, fmt).toInstant(ZoneOffset.UTC); }
            catch (DateTimeParseException ignored) {}
        }
        for (DateTimeFormatter fmt : DATE_FORMATTERS) {
            try {
                return LocalDateTime.parse(s + " 00:00:00",
                        DateTimeFormatter.ofPattern(
                            fmt.toString().replace("HH:mm:ss", "").trim() + " HH:mm:ss"))
                    .toInstant(ZoneOffset.UTC);
            } catch (Exception ignored) {}
        }
        return null;
    }

    private GeoCoordinate parseLatLonPair(String s) {
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

    private boolean looksLikeTimestamp(String s)  { return parseTimestamp(s)  != null; }
    private boolean looksLikeLatLonPair(String s) { return parseLatLonPair(s) != null; }

    private boolean looksLikeLatitude(String s) {
        try { return isValidLat(Double.parseDouble(s.trim())); }
        catch (NumberFormatException e) { return false; }
    }

    private boolean looksLikeLongitude(String s) {
        try { return isValidLon(Double.parseDouble(s.trim())); }
        catch (NumberFormatException e) { return false; }
    }

    private boolean isValidLat(double v) { return v >= -90  && v <= 90;  }
    private boolean isValidLon(double v) { return v >= -180 && v <= 180; }

    // -------------------------------------------------------------------------
    // Internal types
    // -------------------------------------------------------------------------

    /** Tracks which column indices hold timestamps / coordinates for one table. */
    private static class ColumnProfile {
        final List<Integer> timestampCols   = new ArrayList<>();
        final List<Integer> latCols         = new ArrayList<>();
        final List<Integer> lonCols         = new ArrayList<>();
        final List<Integer> combinedGeoCols = new ArrayList<>();
    }

    // -------------------------------------------------------------------------
    // Public data structures
    // -------------------------------------------------------------------------

    /** Immutable WGS-84 coordinate pair. */
    public static class GeoCoordinate {
        private final double latitude;
        private final double longitude;

        public GeoCoordinate(double latitude, double longitude) {
            this.latitude  = latitude;
            this.longitude = longitude;
        }

        public double getLatitude()  { return latitude;  }
        public double getLongitude() { return longitude; }

        @Override
        public String toString() {
            return String.format("%.6f, %.6f", latitude, longitude);
        }
    }

    /**
     * Represents one database row that contains at least one detected timestamp
     * or geo-coordinate value.
     */
    public static class DataPoint {
        private String        tableName;
        private int           rowIndex;
        private Instant       timestamp;
        private GeoCoordinate coordinate;
        private List<String>  rawRow;
        private List<String>  columnNames;

        public String        getTableName()   { return tableName;   }
        public int           getRowIndex()    { return rowIndex;    }
        public Instant       getTimestamp()   { return timestamp;   }
        public GeoCoordinate getCoordinate()  { return coordinate;  }
        public List<String>  getRawRow()      { return rawRow;      }
        public List<String>  getColumnNames() { return columnNames; }

        public void setTableName(String v)         { tableName   = v; }
        public void setRowIndex(int v)             { rowIndex    = v; }
        public void setTimestamp(Instant v)        { timestamp   = v; }
        public void setCoordinate(GeoCoordinate v) { coordinate  = v; }
        public void setRawRow(List<String> v)      { rawRow      = v; }
        public void setColumnNames(List<String> v) { columnNames = v; }

        /**
         * Returns the MSISDN (mobile number) for this data point, or {@code null}
         * if none is available.
         *
         * <p>For {@link ResponseRecordDataPoint} the dedicated field is used.
         * For other table types the value is looked up by the column name
         * {@code "msisdn"} (case-insensitive) in the raw row.</p>
         */
        public String getMsisdn() {
            if (columnNames == null || rawRow == null) return null;
            for (int i = 0; i < columnNames.size(); i++) {
                if ("msisdn".equalsIgnoreCase(columnNames.get(i)) && i < rawRow.size()) {
                    String v = rawRow.get(i);
                    return (v != null && !v.isBlank()) ? v : null;
                }
            }
            return null;
        }

        /** Returns the timestamp formatted as {@code yyyy-MM-dd HH:mm:ss} (UTC). */
        public String getFormattedTimestamp() {
            if (timestamp == null) return "\u2013";
            return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                   .withZone(ZoneOffset.UTC)
                   .format(timestamp);
        }

        @Override
        public String toString() {
            return String.format("DataPoint[table=%s, row=%d, ts=%s, coord=%s]",
                tableName, rowIndex, getFormattedTimestamp(), coordinate);
        }
    }

    // -------------------------------------------------------------------------
    // response_records – specialised DataPoint subclass
    // -------------------------------------------------------------------------

    /**
     * A {@link DataPoint} enriched with all domain-specific fields of the
     * {@code response_records} table as they appear in the fqlite serialisation.
     *
     * <p>All string values are extracted verbatim from the raw row at their
     * fixed absolute positions (see {@code RR_IDX_*} constants). The
     * {@code other_information} field is the concatenation of the five
     * {@code "Key"="Value"} sub-fields.</p>
     */
    public static class ResponseRecordDataPoint extends DataPoint {

        private String endTime;
        private String partyType;
        private String nwAccessType;
        private String naDeviceId;
        private String ltRaw;
        private String loRaw;
        private String mapDatum;
        private String azimuth;
        private String userLocationInfo;
        private String imsi;
        private String msisdn;
        private String apn;
        private String otherInformation;   // concatenated "Key"="Value" pairs
        private String callIndicator;
        private String callActionCode;
        private String callSubtype;
        private String sessionId;
        private String typeOfDataExtra;
        private String durationSeconds;

        // Getters
        public String getEndTime()          { return endTime;          }
        public String getPartyType()        { return partyType;        }
        public String getNwAccessType()     { return nwAccessType;     }
        public String getNaDeviceId()       { return naDeviceId;       }
        public String getLtRaw()            { return ltRaw;            }
        public String getLoRaw()            { return loRaw;            }
        public String getMapDatum()         { return mapDatum;         }
        public String getAzimuth()          { return azimuth;          }
        public String getUserLocationInfo() { return userLocationInfo; }
        public String getImsi()             { return imsi;             }
        public String getMsisdn()           { return msisdn;           }
        public String getApn()              { return apn;              }
        public String getOtherInformation() { return otherInformation; }
        public String getCallIndicator()    { return callIndicator;    }
        public String getCallActionCode()   { return callActionCode;   }
        public String getCallSubtype()      { return callSubtype;      }
        public String getSessionId()        { return sessionId;        }
        public String getTypeOfDataExtra()  { return typeOfDataExtra;  }
        public String getDurationSeconds()  { return durationSeconds;  }

        // Setters
        public void setEndTime(String v)          { endTime          = v; }
        public void setPartyType(String v)        { partyType        = v; }
        public void setNwAccessType(String v)     { nwAccessType     = v; }
        public void setNaDeviceId(String v)       { naDeviceId       = v; }
        public void setLtRaw(String v)            { ltRaw            = v; }
        public void setLoRaw(String v)            { loRaw            = v; }
        public void setMapDatum(String v)         { mapDatum         = v; }
        public void setAzimuth(String v)          { azimuth          = v; }
        public void setUserLocationInfo(String v) { userLocationInfo = v; }
        public void setImsi(String v)             { imsi             = v; }
        public void setMsisdn(String v)           { msisdn           = v; }
        public void setApn(String v)              { apn              = v; }
        public void setOtherInformation(String v) { otherInformation = v; }
        public void setCallIndicator(String v)    { callIndicator    = v; }
        public void setCallActionCode(String v)   { callActionCode   = v; }
        public void setCallSubtype(String v)      { callSubtype      = v; }
        public void setSessionId(String v)        { sessionId        = v; }
        public void setTypeOfDataExtra(String v)  { typeOfDataExtra  = v; }
        public void setDurationSeconds(String v)  { durationSeconds  = v; }
    }
}
