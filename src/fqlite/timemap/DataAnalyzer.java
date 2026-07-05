package fqlite.timemap;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
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
    // response_records – column resolution within a raw data row
    // -------------------------------------------------------------------------
    //
    // The fqlite query engine serialises each row with THREE leading meta-fields
    // that are NOT part of the CREATE TABLE definition:
    //
    //   pos 0 : internal row counter (fqlite-internal, not the PK)
    //   pos 1 : table name
    //   pos 2 : record reference, e.g. "[444|31]"
    //
    // After those meta fields the actual table columns begin at position 3,
    // in exactly the order reported by the table's real column metadata
    // (the "headers"/"cols" list passed into analyze()). Earlier versions of
    // this class hardcoded the absolute position of every field, which broke
    // silently whenever the importer's column order or naming changed (e.g.
    // "latitude"/"longitude" vs. "latitude_dec"/"longitude_dec"). Column
    // positions are now resolved BY NAME via colIndex(), using RR_META_OFFSET
    // as the only fixed assumption.

    /** Number of fqlite meta-fields prepended to every row. */
    private static final int RR_META_OFFSET = 3;

    /** Primary timestamp column name in {@code response_records}. */
    private static final String RR_START_TIME  = "start_time";

    // Column-name aliases accepted for the geo-coordinate fields. The
    // "_dec"/"_raw" suffixed names are what the current EtsiXmlImporter
    // schema uses; the unsuffixed/"lt_raw"/"lo_raw" names are kept for
    // backward compatibility with older databases and the demo data in
    // LocationWindow. Column positions are resolved BY NAME against the
    // real header list (see colIndex()) rather than hardcoded absolute
    // offsets, because the offsets used to drift out of sync with the
    // importer's actual column order and caused coordinates to silently
    // disappear from the map.
    private static final String[] RR_LATITUDE_NAMES  = {"latitude_dec", "latitude"};
    private static final String[] RR_LONGITUDE_NAMES = {"longitude_dec", "longitude"};
    private static final String[] RR_LAT_RAW_NAMES   = {"latitude_raw", "lt_raw"};
    private static final String[] RR_LON_RAW_NAMES   = {"longitude_raw", "lo_raw"};

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
        return analyze(resultlist, headers, null);
    }

    /**
     * Reports how many rows (out of the known total) {@link #analyze} has
     * processed so far. Used by callers running {@link #analyze} on a
     * background thread (see {@code MapViewPane#setData}) to show real
     * progress instead of an indeterminate spinner while a large recovered
     * database (potentially hundreds of thousands to millions of rows) is
     * being scanned for timestamp/geo columns.
     */
    public interface ProgressListener {
        void onProgress(long processedRows, long totalRows);
    }

    /**
     * Same as {@link #analyze(ConcurrentHashMap, Map)}, but reports progress
     * via {@code listener} (may be {@code null} to skip reporting). Progress
     * is reported in batches (not after every single row) to keep the
     * overhead of the callback negligible even for very large tables.
     */
    public List<DataPoint> analyze(
            ConcurrentHashMap<String, ObservableList<ObservableList<String>>> resultlist,
            Map<String, List<String>> headers,
            ProgressListener listener) {

        long totalRows = 0;
        for (ObservableList<ObservableList<String>> rows : resultlist.values()) {
            totalRows += rows.size();
        }
        AtomicLong processed = new AtomicLong();

        List<DataPoint> points = new ArrayList<>();

        for (Map.Entry<String, ObservableList<ObservableList<String>>> tableEntry
                : resultlist.entrySet()) {

            String tableName = tableEntry.getKey();
            ObservableList<ObservableList<String>> rows = tableEntry.getValue();
            List<String> cols = headers.getOrDefault(tableName, List.of());

            if (isResponseRecordsSchema(tableName, cols)) {
                // ── Fast path: fixed schema ──────────────────────────────────
                List<DataPoint> fastPathPoints =
                        analyzeResponseRecords(tableName, rows, cols, processed, totalRows, listener);
                if (!fastPathPoints.isEmpty() || rows.isEmpty()) {
                    points.addAll(fastPathPoints);
                } else {
                    // The header list names a response_records-like schema, but
                    // none of the name-resolved column positions produced a
                    // single usable timestamp or coordinate across the whole
                    // table. This happens when the database was created by an
                    // older/different importer revision whose row layout no
                    // longer lines up 1:1 with today's column list (e.g. extra
                    // columns inserted between nw_access_type and start_time).
                    // Rather than silently showing nothing, fall back to the
                    // same value-based heuristic used for unrecognised tables.
                    points.addAll(analyzeGeneric(tableName, rows, cols, processed, totalRows, listener));
                }
            } else {
                // ── General path: heuristic profiling ───────────────────────
                points.addAll(analyzeGeneric(tableName, rows, cols, processed, totalRows, listener));
            }
        }

        points.sort(Comparator.comparing(
                DataPoint::getTimestamp,
                Comparator.nullsLast(Comparator.naturalOrder())));
        return points;
    }

    /** How many rows to process between {@link ProgressListener} callbacks. */
    private static final long PROGRESS_BATCH = 5_000;

    /**
     * Value-based heuristic extraction: profiles every column for timestamp /
     * coordinate content and builds one {@link DataPoint} per row that yields
     * either. Used for any table that doesn't match a known fixed schema, and
     * as a safety-net fallback when a fixed schema's name-resolved column
     * positions fail to validate against the actual row data.
     */
    private List<DataPoint> analyzeGeneric(
            String tableName,
            ObservableList<ObservableList<String>> rows,
            List<String> cols,
            AtomicLong processed,
            long totalRows,
            ProgressListener listener) {

        List<DataPoint> points = new ArrayList<>();
        ColumnProfile profile = profileColumns(rows, cols);

        // Iterate via the list's own iterator instead of an indexed
        // `rows.get(rowIdx)` loop: `rows` is an ObservableList that may be
        // backed by a sequential-access list (e.g. LinkedList) further up
        // the import pipeline rather than an ArrayList, in which case
        // `.get(index)` costs O(index) and an indexed loop over every row
        // turns the whole table scan into O(n²). A for-each loop always
        // costs O(1) per step regardless of the backing list type.
        int rowIdx = 0;
        for (ObservableList<String> row : rows) {

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
            reportProgress(processed, totalRows, listener);
            rowIdx++;
        }
        return points;
    }

    /**
     * Increments {@code processed} by one and, every {@link #PROGRESS_BATCH}
     * rows (or on the very last row), notifies {@code listener} — a no-op
     * when {@code listener} is {@code null}. Batching keeps the callback
     * overhead (typically a {@code Platform.runLater} hop, see
     * {@code MapViewPane#setData}) negligible even for tables with millions
     * of rows.
     */
    private static void reportProgress(AtomicLong processed, long totalRows, ProgressListener listener) {
        if (listener == null) return;
        long done = processed.incrementAndGet();
        if (done % PROGRESS_BATCH == 0 || done == totalRows) {
            listener.onProgress(done, totalRows);
        }
    }

    // -------------------------------------------------------------------------
    // response_records – fixed-schema extraction
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} when the table name equals {@code response_records}
     * (case-insensitive) <em>and</em> the supplied header list names a
     * {@code start_time} column plus a recognised latitude/longitude column
     * pair (see {@link #RR_LATITUDE_NAMES}/{@link #RR_LONGITUDE_NAMES}).
     */
    private boolean isResponseRecordsSchema(String tableName, List<String> cols) {
        if (!SCHEMA_RESPONSE_RECORDS.equalsIgnoreCase(tableName)) return false;
        if (cols.isEmpty()) return false;
        List<String> lower = cols.stream().map(String::toLowerCase).toList();
        return lower.contains(RR_START_TIME)
            && containsAny(lower, RR_LATITUDE_NAMES)
            && containsAny(lower, RR_LONGITUDE_NAMES);
    }

    private static boolean containsAny(List<String> lower, String[] candidateNames) {
        for (String name : candidateNames) {
            if (lower.contains(name)) return true;
        }
        return false;
    }

    /**
     * Resolves the absolute row index of a column, trying each candidate
     * name in order (case-insensitive) against the real header list and
     * adding {@link #RR_META_OFFSET}.
     *
     * @return the absolute index into the raw row, or {@code -1} if none of
     *         the candidate names is present in {@code cols}
     */
    private static int colIndex(List<String> cols, String... candidateNames) {
        for (String name : candidateNames) {
            for (int i = 0; i < cols.size(); i++) {
                if (name.equalsIgnoreCase(cols.get(i))) {
                    return RR_META_OFFSET + i;
                }
            }
        }
        return -1;
    }

    /**
     * Searches a small window of candidate offsets around {@code naiveIdx} and
     * returns the delta (added to {@code naiveIdx}) under which the most
     * sampled rows validate against {@code validator}, requiring that the
     * column immediately to the right of the candidate ALSO validates.
     *
     * <p>This corrects for databases where the row layout no longer lines up
     * 1:1 with {@code RR_META_OFFSET + colIndex(...)} — e.g. a table created
     * by an older importer revision that inserted a few extra columns between
     * {@code nw_access_type} and {@code start_time}, or a query result that
     * has none of fqlite's usual 3 leading meta-fields at all. The correction
     * is purely value-based (does it actually look like a timestamp?), so it
     * self-corrects regardless of why the shift exists, instead of trusting
     * column names blindly and silently producing zero data points.</p>
     *
     * <p>Specifically for resolving {@code start_time}: a plain single-column
     * score (does {@code idx} alone look like a timestamp?) ties between the
     * true {@code start_time} position and the {@code end_time} column
     * itself (one position later), since both share the exact same format.
     * Scanning {@code ±d} outward and keeping the first candidate to beat the
     * running best score then picks whichever of the two is reached first —
     * which is wrong whenever the true offset is farther from the naive
     * guess than that neighbour (e.g. when there are no leading meta-fields
     * at all, so the correct delta is {@code -RR_META_OFFSET}, while
     * {@code end_time} sits only one column closer). Requiring the next
     * column to ALSO validate disambiguates correctly in both directions,
     * since {@code end_time} itself is followed by {@code duration_seconds}
     * (an integer), which fails the timestamp check.</p>
     *
     * @return the best delta, or {@code 0} when {@code naiveIdx} already
     *         validates as well as or better than any shifted candidate
     */
    private static int findShiftPaired(
            ObservableList<ObservableList<String>> rows,
            int naiveIdx,
            Predicate<String> validator,
            int maxShift) {

        if (naiveIdx < 0) return 0;
        int sample = Math.min(rows.size(), 30);

        int bestDelta = 0;
        int bestScore = scorePairAt(rows, naiveIdx, validator, sample);

        for (int d = 1; d <= maxShift; d++) {
            int scoreForward  = scorePairAt(rows, naiveIdx + d, validator, sample);
            if (scoreForward > bestScore) { bestScore = scoreForward; bestDelta = d; }

            int scoreBackward = scorePairAt(rows, naiveIdx - d, validator, sample);
            if (scoreBackward > bestScore) { bestScore = scoreBackward; bestDelta = -d; }
        }
        return bestDelta;
    }

    /** Scores {@code idx} only when both {@code idx} and {@code idx + 1} validate. */
    private static int scorePairAt(
            ObservableList<ObservableList<String>> rows,
            int idx,
            Predicate<String> validator,
            int sampleSize) {

        if (idx < 0) return 0;
        int score = 0;
        for (int r = 0; r < sampleSize; r++) {
            ObservableList<String> row = rows.get(r);
            if (idx + 1 >= row.size()) continue;
            String v1 = row.get(idx);
            String v2 = row.get(idx + 1);
            if (v1 != null && !v1.isBlank() && validator.test(v1)
                    && v2 != null && !v2.isBlank() && validator.test(v2)) score++;
        }
        return score;
    }

    /** {@code colIndex(cols, names)}, then shifted by {@code delta} (or {@code -1} unresolved). */
    private static int colIndex(List<String> cols, int delta, String... candidateNames) {
        int naive = colIndex(cols, candidateNames);
        return naive < 0 ? -1 : naive + delta;
    }

    /**
     * Processes a {@code response_records} table using column positions
     * resolved by name from the real header list (see {@link #colIndex}),
     * offset by {@link #RR_META_OFFSET} to account for fqlite's 3 leading
     * meta-fields (row counter, table name, record reference) that precede
     * the actual table columns in every raw row.
     *
     * <p>Every row that yields either a parseable {@code start_time}/{@code end_time}
     * or valid latitude/longitude values produces one {@link ResponseRecordDataPoint}.</p>
     */
    private List<DataPoint> analyzeResponseRecords(
            String tableName,
            ObservableList<ObservableList<String>> rows,
            List<String> cols,
            AtomicLong processed,
            long totalRows,
            ProgressListener listener) {

        List<DataPoint> points = new ArrayList<>();

        // Resolve every column position once (by name) for this table,
        // instead of per row — cheap, and avoids hardcoded absolute offsets
        // that silently go stale when the importer's column order changes.
        //
        // A purely name-based lookup still assumes the row's actual layout
        // matches RR_META_OFFSET + the column's position in `cols` exactly.
        // That assumption breaks for databases written by an older importer
        // revision that inserted extra columns between nw_access_type and
        // start_time — the table still LOOKS like response_records by name,
        // but every field from start_time onward sits a few positions further
        // right than its name-resolved index predicts. findShiftPaired() detects
        // that constant offset by checking where start_time's value actually
        // parses as a timestamp, and the same delta is then applied to every
        // other field from start_time onward (id…nw_access_type are read from
        // their naive, unshifted positions, since they were observed to stay
        // correctly aligned even when the later shift exists).
        int naiveStartTime = colIndex(cols, RR_START_TIME);
        int delta = findShiftPaired(rows, naiveStartTime, this::looksLikeTimestamp, 12);

        int idxStartTime    = naiveStartTime < 0 ? -1 : naiveStartTime + delta;
        int idxEndTime      = colIndex(cols, delta, "end_time");
        int idxDuration     = colIndex(cols, delta, "duration_seconds");
        int idxNaDevice     = colIndex(cols, delta, "na_device_id");
        int idxLtRaw        = colIndex(cols, delta, RR_LAT_RAW_NAMES);
        int idxLoRaw        = colIndex(cols, delta, RR_LON_RAW_NAMES);
        int idxLatitude     = colIndex(cols, delta, RR_LATITUDE_NAMES);
        int idxLongitude    = colIndex(cols, delta, RR_LONGITUDE_NAMES);
        int idxMapDatum     = colIndex(cols, delta, "map_datum");
        int idxAzimuth      = colIndex(cols, delta, "azimuth");
        int idxUserLoc      = colIndex(cols, delta, "user_location_info");
        int idxImsi         = colIndex(cols, delta, "imsi");
        int idxMsisdn       = colIndex(cols, delta, "msisdn");
        int idxApn          = colIndex(cols, delta, "apn");
        int idxOtherInfo    = colIndex(cols, delta, "other_information");
        int idxCallIndicator= colIndex(cols, delta, "call_indicator");
        int idxCallAction   = colIndex(cols, delta, "call_action_code");
        int idxCallSubtype  = colIndex(cols, delta, "call_subtype");
        int idxSessionId    = colIndex(cols, delta, "session_id");
        int idxTypeExtra    = colIndex(cols, delta, "type_of_data_extra");
        // party_type / nw_access_type precede the shift point, so they're
        // read from their naive (unshifted) position.
        int idxPartyType    = colIndex(cols, "party_type");
        int idxNwAccess     = colIndex(cols, "nw_access_type");

        // Iterate via the list's own iterator instead of an indexed
        // `rows.get(rowIdx)` loop — see the identical comment in
        // analyzeGeneric() above: `rows` may be backed by a sequential-access
        // list further up the import pipeline, which would make an indexed
        // scan over every row O(n²) for the whole table.
        int rowIdx = 0;
        for (ObservableList<String> row : rows) {

            // Copy each row into a random-access list ONCE: `row` itself may
            // also be sequential-access (a JVM thread dump taken during a
            // large response_records import showed exactly this — the
            // analyzer thread sampled inside
            // ObservableSequentialListWrapper.get() / LinkedList.node(),
            // called from cell() below). Without this copy, every one of the
            // ~20 named-field cell() lookups per row below would re-walk the
            // row from the start, and the transient ListIterator allocated
            // by each such lookup also adds real, sustained GC pressure
            // across hundreds of thousands of rows — both contributing to
            // the kind of background CPU/GC load that starves the FX
            // Application Thread and makes the OS report "Application Not
            // Responding" even though no thread is actually deadlocked. The
            // copy itself costs O(columns) once (a single iterator walk),
            // not O(columns) per lookup.
            List<String> r = new ArrayList<>(row);

            // ── Timestamp ────────────────────────────────────────────────────
            Instant timestamp = parseTimestamp(cell(r, idxStartTime));
            if (timestamp == null)
                timestamp = parseTimestamp(cell(r, idxEndTime));

            // ── Coordinates ──────────────────────────────────────────────────
            GeoCoordinate coord = null;
            String latStr = cell(r, idxLatitude);
            String lonStr = cell(r, idxLongitude);
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
            if (timestamp == null && coord == null) {
                reportProgress(processed, totalRows, listener);
                rowIdx++;
                continue;
            }

            // ── Build DataPoint ──────────────────────────────────────────────
            ResponseRecordDataPoint rrdp = new ResponseRecordDataPoint();
            rrdp.setTableName(tableName);
            rrdp.setRowIndex(rowIdx);
            rrdp.setTimestamp(timestamp);
            rrdp.setCoordinate(coord);
            rrdp.setRawRow(r);
            rrdp.setColumnNames(cols);

            rrdp.setEndTime(cell(r, idxEndTime));
            rrdp.setPartyType(cell(r, idxPartyType));
            rrdp.setNwAccessType(cell(r, idxNwAccess));
            rrdp.setNaDeviceId(cell(r, idxNaDevice));
            rrdp.setLtRaw(cell(r, idxLtRaw));
            rrdp.setLoRaw(cell(r, idxLoRaw));
            rrdp.setMapDatum(cell(r, idxMapDatum));
            rrdp.setAzimuth(cell(r, idxAzimuth));
            rrdp.setUserLocationInfo(cell(r, idxUserLoc));
            rrdp.setImsi(cell(r, idxImsi));
            rrdp.setMsisdn(cell(r, idxMsisdn));
            rrdp.setApn(cell(r, idxApn));
            rrdp.setOtherInformation(cell(r, idxOtherInfo));
            rrdp.setCallIndicator(cell(r, idxCallIndicator));
            rrdp.setCallActionCode(cell(r, idxCallAction));
            rrdp.setCallSubtype(cell(r, idxCallSubtype));
            rrdp.setSessionId(cell(r, idxSessionId));
            rrdp.setTypeOfDataExtra(cell(r, idxTypeExtra));
            rrdp.setDurationSeconds(cell(r, idxDuration));

            // ── ULI decoding ─────────────────────────────────────────────────
            // Attempt to decode the user_location_info hex field into structured
            // cell identity (MCC, MNC, LAC/TAC, CI).  This is a best-effort
            // operation; rows with missing or malformed ULI simply get null.
            UliDecoder.CellInfo cellInfo = UliDecoder.decode(cell(r, idxUserLoc));
            rrdp.setCellInfo(cellInfo);
            // CellTower resolution (OpenCelliD lookup) is deferred: MapView
            // triggers it asynchronously when the cell-tower layer is enabled.

            points.add(rrdp);
            reportProgress(processed, totalRows, listener);
            rowIdx++;
        }

        return points;
    }

    /** Safely retrieves a cell value; returns {@code null} for out-of-range or negative indices. */
    private static String cell(List<String> row, int idx) {
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
        // Priority 2: separate latitude and longitude columns. A table can
        // have more than one name-matched candidate (e.g. a raw DMS column
        // alongside a decimal-degrees column) — try every pairing in order
        // and use the first that actually parses as a valid coordinate,
        // instead of blindly trusting the first column by index (which used
        // to silently swallow valid coordinates whenever a non-numeric
        // "raw" column happened to come first).
        for (int latCol : p.latCols) {
            if (latCol >= row.size()) continue;
            Double lat = parseValidLat(row.get(latCol));
            if (lat == null) continue;
            for (int lonCol : p.lonCols) {
                if (lonCol >= row.size()) continue;
                Double lon = parseValidLon(row.get(lonCol));
                if (lon != null) return new GeoCoordinate(lat, lon);
            }
        }
        return null;
    }

    private Double parseValidLat(String s) {
        if (s == null) return null;
        try {
            double v = Double.parseDouble(s.trim());
            return isValidLat(v) ? v : null;
        } catch (NumberFormatException e) { return null; }
    }

    private Double parseValidLon(String s) {
        if (s == null) return null;
        try {
            double v = Double.parseDouble(s.trim());
            return isValidLon(v) ? v : null;
        } catch (NumberFormatException e) { return null; }
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

        /**
         * Returns the IMSI (subscriber identity) for this data point, or
         * {@code null} if none is available.
         *
         * <p>For {@link ResponseRecordDataPoint} the dedicated field is used.
         * For other table types the value is looked up by the column name
         * {@code "imsi"} (case-insensitive) in the raw row.</p>
         */
        public String getImsi() {
            if (columnNames == null || rawRow == null) return null;
            for (int i = 0; i < columnNames.size(); i++) {
                if ("imsi".equalsIgnoreCase(columnNames.get(i)) && i < rawRow.size()) {
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
     * <p>All string values are extracted verbatim from the raw row, at
     * positions resolved by column name (see {@link DataAnalyzer#colIndex}).
     * The {@code other_information} field holds the raw {@code "Key"="Value"}
     * string as produced by the importer.</p>
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

        /**
         * Decoded ULI (User Location Information) cell identity, or {@code null}
         * if {@code user_location_info} could not be parsed.
         * Populated by {@link DataAnalyzer#analyzeResponseRecords}.
         */
        private UliDecoder.CellInfo cellInfo;

        /**
         * Cell tower resolved from OpenCelliD for this data point.
         * {@code null} until {@link CellTowerResolver} has completed the lookup.
         * Resolution is triggered asynchronously by {@link MapView} when the
         * feature is enabled.
         */
        private volatile CellTower cellTower;

        // Getters
        public String getEndTime()              { return endTime;          }
        public String getPartyType()            { return partyType;        }
        public String getNwAccessType()         { return nwAccessType;     }
        public String getNaDeviceId()           { return naDeviceId;       }
        public String getLtRaw()                { return ltRaw;            }
        public String getLoRaw()                { return loRaw;            }
        public String getMapDatum()             { return mapDatum;         }
        public String getAzimuth()              { return azimuth;          }
        public String getUserLocationInfo()     { return userLocationInfo; }
        @Override
        public String getImsi()                 { return imsi;             }
        public String getMsisdn()               { return msisdn;           }
        public String getApn()                  { return apn;              }
        public String getOtherInformation()     { return otherInformation; }
        public String getCallIndicator()        { return callIndicator;    }
        public String getCallActionCode()       { return callActionCode;   }
        public String getCallSubtype()          { return callSubtype;      }
        public String getSessionId()            { return sessionId;        }
        public String getTypeOfDataExtra()      { return typeOfDataExtra;  }
        public String getDurationSeconds()      { return durationSeconds;  }
        public UliDecoder.CellInfo getCellInfo(){ return cellInfo;         }
        public CellTower getCellTower()         { return cellTower;        }

        // Setters
        public void setEndTime(String v)              { endTime          = v; }
        public void setPartyType(String v)            { partyType        = v; }
        public void setNwAccessType(String v)         { nwAccessType     = v; }
        public void setNaDeviceId(String v)           { naDeviceId       = v; }
        public void setLtRaw(String v)                { ltRaw            = v; }
        public void setLoRaw(String v)                { loRaw            = v; }
        public void setMapDatum(String v)             { mapDatum         = v; }
        public void setAzimuth(String v)              { azimuth          = v; }
        public void setUserLocationInfo(String v)     { userLocationInfo = v; }
        public void setImsi(String v)                 { imsi             = v; }
        public void setMsisdn(String v)               { msisdn           = v; }
        public void setApn(String v)                  { apn              = v; }
        public void setOtherInformation(String v)     { otherInformation = v; }
        public void setCallIndicator(String v)        { callIndicator    = v; }
        public void setCallActionCode(String v)       { callActionCode   = v; }
        public void setCallSubtype(String v)          { callSubtype      = v; }
        public void setSessionId(String v)            { sessionId        = v; }
        public void setTypeOfDataExtra(String v)      { typeOfDataExtra  = v; }
        public void setDurationSeconds(String v)      { durationSeconds  = v; }
        public void setCellInfo(UliDecoder.CellInfo v){ cellInfo         = v; }
        public void setCellTower(CellTower v)         { cellTower        = v; }
    }
}
