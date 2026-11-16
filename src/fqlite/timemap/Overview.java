package fqlite.timemap;

import fqlite.location.GPSParser;
import fqlite.timemap.DataAnalyzer.DataPoint;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Overview of a large {@code response_records} table (Lighthouse, database
 * opened directly): instead of one object per record only aggregates are kept,
 * and single records are read from the file when they are shown.
 *
 * <ul>
 *   <li>One <em>representative</em> per location (coordinate and azimuth): the
 *       earliest record there, with the number of records at that location -
 *       the map draws these weighted. The same per direction for calls
 *       (MOC/MTC).</li>
 *   <li>Records per location and time bucket (one hour, coarser for long
 *       periods): the map can show the counts of a time window.</li>
 *   <li>Records per minute in the order of {@code start_time}: the timeline
 *       shows all records and reads only the visible page through the index on
 *       {@code start_time}.</li>
 * </ul>
 *
 * <p>The memory needed depends on the number of locations, minutes and time
 * buckets, not on the number of records. Built in one (parallel) pass over the
 * table, optionally restricted by an SQL condition (filter of the overview).
 * The timeline contains the records whose {@code start_time} is text (the
 * ETSI importer writes {@code yyyy-MM-dd HH:mm:ss}), in the order of that text
 * and the rowid.</p>
 */
public final class Overview implements AutoCloseable, MapView.IdentifierSource {

    /** records per page of the timeline */
    private static final int BLOCK = 256;
    /** pages kept in memory */
    private static final int CACHE_BLOCKS = 64;
    /** maximum number of time buckets (per location) */
    private static final int MAX_BUCKETS = 2000;

    private final DataAnalyzer analyzer;
    private final DataAnalyzer.ConnectionSource source;
    private final DataAnalyzer.RawRowLoader loader;
    private final DataAnalyzer.RrQuery rq;
    private final String cond;
    private final String label;

    private final long records;
    private final long geoRecords;

    private final List<DataPoint> reps;
    private final long[] repTotal;
    /** per location: indices of the non-empty time buckets and their counts */
    private final char[][] repBucketIdx;
    private final int[][] repBucketCnt;
    private final List<DataPoint> callReps;
    private final long[] callTotal;
    private final long callRecords;

    private final long bucketBase;     // epoch second of bucket 0
    private final int bucketSec;
    private final int nBuckets;

    private final String[] minuteKeys;
    private final long[] minuteCum;    // minuteCum[i] = records before minute i; length n + 1

    private Connection pageConn;
    private final LinkedHashMap<Integer, List<DataPoint>> pages = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Integer, List<DataPoint>> e) {
            return size() > CACHE_BLOCKS;
        }
    };
    /** distinct identifier lists, computed once even when several views ask at the same time */
    private final Map<String, java.util.concurrent.CompletableFuture<List<String>>> distinctCache = new ConcurrentHashMap<>();

    private Overview(Builder b) {
        this.analyzer = b.analyzer; this.source = b.source; this.loader = b.loader; this.rq = b.rq;
        this.cond = b.cond; this.label = b.label;
        this.records = b.records; this.geoRecords = b.geoRecords;
        this.reps = b.reps; this.repTotal = b.repTotal; this.repBucketIdx = b.repBucketIdx; this.repBucketCnt = b.repBucketCnt;
        this.callReps = b.callReps; this.callTotal = b.callTotal; this.callRecords = b.callRecords;
        this.bucketBase = b.bucketBase; this.bucketSec = b.bucketSec; this.nBuckets = b.nBuckets;
        this.minuteKeys = b.minuteKeys; this.minuteCum = b.minuteCum;
    }

    // ------------------------------------------------------------------ data

    /** SQL condition of this overview, or {@code null} for the whole table */
    public String getCondition() { return cond; }

    /** description of the filter (status line), or {@code null} */
    public String getLabel() { return label; }

    /** matching records */
    public long getRecords() { return records; }

    /** matching records with a coordinate */
    public long getGeoRecords() { return geoRecords; }

    /** one point per location, the earliest record there, sorted by time */
    public List<DataPoint> getRepresentatives() { return reps; }

    /** records per location (parallel to {@link #getRepresentatives()}) */
    public long[] getTotals() { return repTotal.clone(); }

    /** one point per location and call direction (MOC/MTC), the earliest such call */
    public List<DataPoint> getCallRepresentatives() { return callReps; }

    /** calls per location and direction (parallel to {@link #getCallRepresentatives()}) */
    public long[] getCallTotals() { return callTotal.clone(); }

    /** matching MOC/MTC records with a coordinate */
    public long getCallRecords() { return callRecords; }

    /** true if records can be counted per time window */
    public boolean hasTimeBuckets() { return nBuckets > 0; }

    /**
     * Records per location in the time window (bucket granularity: whole
     * buckets touched by the window are counted).
     *
     * @return parallel to {@link #getRepresentatives()}
     */
    public long[] weightsFor(Instant from, Instant to) {
        if (nBuckets == 0 || from == null || to == null) return getTotals();
        long b0 = Math.floorDiv(from.getEpochSecond() - bucketBase, bucketSec);
        long b1 = Math.floorDiv(to.getEpochSecond() - bucketBase, bucketSec);
        long[] w = new long[reps.size()];
        if (b1 < 0 || b0 >= nBuckets) return w;
        int lo = (int) Math.max(0, b0), hi = (int) Math.min(nBuckets - 1, b1);
        for (int r = 0; r < w.length; r++) {
            char[] idx = repBucketIdx[r];
            if (idx == null) continue;
            int[] cnt = repBucketCnt[r];
            long sum = 0;
            for (int i = 0; i < idx.length; i++)
                if (idx[i] >= lo && idx[i] <= hi) sum += cnt[i];
            w[r] = sum;
        }
        return w;
    }

    /** duration of one time bucket in seconds */
    public int getBucketSeconds() { return bucketSec; }

    // -------------------------------------------------------------- timeline

    /** records in the timeline (those with a text {@code start_time}) */
    public int timelineSize() {
        return (int) Math.min(Integer.MAX_VALUE, minuteCum[minuteCum.length - 1]);
    }

    /** approximate time (minute) of timeline position {@code idx}, or {@code null} */
    public Instant timelineTimeAt(int idx) {
        int m = minuteOf(idx);
        if (m < 0) return null;
        String k = minuteKeys[m];
        if (k.length() == 16)
            return DataAnalyzer.parseIsoSeconds(k + ":00");
        return null;
    }

    /**
     * Record at timeline position {@code idx}; the page containing it is read
     * from the file if needed.
     */
    public synchronized DataPoint timelineGet(int idx) {
        if (idx < 0 || idx >= timelineSize()) return null;
        int block = idx / BLOCK;
        List<DataPoint> page = pages.get(block);
        if (page == null) {
            try {
                page = loadBlock(block);
            } catch (SQLException e) {
                page = List.of();
            }
            pages.put(block, page);
        }
        int off = idx - block * BLOCK;
        return off < page.size() ? page.get(off) : null;
    }

    /** true if the page of {@code idx} is already in memory */
    public synchronized boolean isLoaded(int idx) {
        return pages.containsKey(idx / BLOCK);
    }

    private List<DataPoint> loadBlock(int block) throws SQLException {
        int start = block * BLOCK;
        int m = minuteOf(start);
        if (m < 0) return List.of();
        long offset = start - minuteCum[m];
        String tail = "WHERE " + rq.q(rq.c.startTime) + " >= ?" + andCond()
                + " ORDER BY " + rq.q(rq.c.startTime) + ", rowid LIMIT " + BLOCK + " OFFSET " + offset;
        List<DataPoint> pts = analyzer.readResponseRecordRows(conn(), rq, tail, new Object[] { minuteKeys[m] }, loader, true);
        return pts;
    }

    /** minute containing timeline position {@code idx}, or -1 */
    private int minuteOf(long idx) {
        if (idx < 0 || minuteKeys.length == 0 || idx >= minuteCum[minuteCum.length - 1]) return -1;
        int lo = 0, hi = minuteKeys.length - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (minuteCum[mid] <= idx) lo = mid; else hi = mid - 1;
        }
        return lo;
    }

    /**
     * Timeline position of {@code dp}, or -1 if it is not part of this
     * overview's timeline.
     */
    public synchronized int timelineIndexOf(DataPoint dp) {
        if (dp == null || dp.getRowId() < 0 || rq.c.startTime < 0) return -1;
        long rowid = dp.getRowId();
        String col = rq.q(rq.c.startTime);
        try {
            String st = null;
            try (PreparedStatement ps = conn().prepareStatement(
                    "SELECT " + col + " FROM response_records WHERE rowid = ? AND typeof(" + col + ") = 'text'" + andCond())) {
                ps.setLong(1, rowid);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) st = rs.getString(1);
                }
            }
            if (st == null) return -1;
            String key = st.length() > 16 ? st.substring(0, 16) : st;
            int m = Arrays.binarySearch(minuteKeys, key);
            if (m < 0) return -1;
            try (PreparedStatement ps = conn().prepareStatement(
                    "SELECT COUNT(*) FROM response_records WHERE " + col + " >= ? AND (" + col + " < ? OR ("
                    + col + " = ? AND rowid < ?))" + andCond())) {
                ps.setString(1, key);
                ps.setString(2, st);
                ps.setString(3, st);
                ps.setLong(4, rowid);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? (int) (minuteCum[m] + rs.getLong(1)) : -1;
                }
            }
        } catch (SQLException e) {
            return -1;
        }
    }

    // ----------------------------------------------------------- identifiers

    /** quoted column name ("imsi", "msisdn", "na_device_id", "start_time", "call_indicator"), or null if absent */
    public String column(String name) {
        int i = switch (name) {
            case "imsi" -> rq.c.imsi;
            case "msisdn" -> rq.c.msisdn;
            case "na_device_id" -> rq.c.naDevice;
            case "start_time" -> rq.c.startTime;
            case "call_indicator" -> rq.c.callIndicator;
            default -> -1;
        };
        return i < 0 ? null : rq.q(i);
    }

    /**
     * Distinct non-blank values of an identifier column among the matching
     * records, sorted (uses the importer's index on the column).
     *
     * @param name "imsi", "msisdn" or "na_device_id"
     */
    @Override
    public List<String> distinct(String name) {
        java.util.concurrent.CompletableFuture<List<String>> f = new java.util.concurrent.CompletableFuture<>();
        java.util.concurrent.CompletableFuture<List<String>> prev = distinctCache.putIfAbsent(name, f);
        if (prev != null) return prev.join();
        List<String> res;
        try {
            res = readDistinct(name);
        } catch (RuntimeException e) {
            res = List.of();
        }
        f.complete(res);
        return res;
    }

    private List<String> readDistinct(String name) {
        String col = column(name);
        List<String> out = new ArrayList<>();
        if (col != null) {
            try (Connection c = source.open();
                 PreparedStatement ps = c.prepareStatement("SELECT " + col + " FROM response_records WHERE "
                         + col + " IS NOT NULL AND " + col + " <> ''" + andCond() + " GROUP BY " + col);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String v = rs.getString(1);
                    if (v != null && !v.isBlank()) out.add(v);
                }
            } catch (SQLException e) {
                return List.of();
            }
        }
        out.sort(Comparator.naturalOrder());
        return List.copyOf(out);
    }

    /**
     * All matching records of one identifier, sorted by time (stable, in
     * rowid order) - the input of the IMSI path and the IMEI storyboard.
     */
    @Override
    public List<DataPoint> recordsOf(String name, String value) {
        String col = column(name);
        if (col == null || value == null) return List.of();
        try (Connection c = source.open()) {
            List<DataPoint> pts = new ArrayList<>(analyzer.readResponseRecordRows(c, rq,
                    "WHERE " + col + " = ?" + andCond() + " ORDER BY rowid", new Object[] { value }, loader, false));
            pts.sort(Comparator.comparing(DataPoint::getTimestamp, Comparator.nullsLast(Comparator.naturalOrder())));
            return pts;
        } catch (SQLException e) {
            return List.of();
        }
    }

    /**
     * SQL condition for the records at the location of {@code rep} (decimal
     * coordinate equal within 1e-9 degrees, azimuth equal), combined with this
     * overview's condition.
     */
    public String locationCondition(DataPoint rep) {
        if (rep == null || rep.getCoordinate() == null || rq.c.latitude < 0 || rq.c.longitude < 0) return null;
        double lat = rep.getCoordinate().getLatitude(), lon = rep.getCoordinate().getLongitude();
        StringBuilder sb = new StringBuilder();
        sb.append(rq.q(rq.c.latitude)).append(" BETWEEN ").append(lat - 1e-9).append(" AND ").append(lat + 1e-9)
          .append(" AND ").append(rq.q(rq.c.longitude)).append(" BETWEEN ").append(lon - 1e-9).append(" AND ").append(lon + 1e-9);
        if (rq.c.azimuth >= 0 && rep instanceof DataAnalyzer.ResponseRecordDataPoint rr) {
            String az = rr.getAzimuth();
            if (az == null) sb.append(" AND ").append(rq.q(rq.c.azimuth)).append(" IS NULL");
            else sb.append(" AND ").append(rq.q(rq.c.azimuth)).append(" = '").append(az.replace("'", "''")).append("'");
        }
        if (cond != null) sb.append(" AND (").append(cond).append(")");
        return sb.toString();
    }

    /** number of records matching {@code where} (all records for {@code null}) */
    public static long count(DataAnalyzer.ConnectionSource source, String where) throws SQLException {
        try (Connection c = source.open();
             java.sql.Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM response_records" + (where != null ? " WHERE " + where : ""))) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    private String andCond() {
        return cond == null ? "" : " AND (" + cond + ")";
    }

    private Connection conn() throws SQLException {
        if (pageConn == null) pageConn = source.open();
        return pageConn;
    }

    @Override
    public synchronized void close() {
        pages.clear();
        if (pageConn != null) {
            try { pageConn.close(); } catch (SQLException ignored) { }
            pageConn = null;
        }
    }

    // ================================================================ cache

    /** format of the cache file; increase when the stored data changes */
    private static final int CACHE_VERSION = 1;
    private static final int CACHE_MAGIC = 0x4C484F56;   // "LHOV"

    /**
     * Writes this overview (whole table only) to {@code file}: the
     * aggregates and the rowids of the representatives, together with
     * {@code key} (identifies the database file) and the number of records
     * and the largest rowid of the table, which are checked again on loading.
     */
    public void save(java.io.File file, String key) throws java.io.IOException, SQLException {
        if (cond != null) throw new IllegalStateException("only the overview of the whole table is cached");
        long[] check = tableCheck(source);
        java.io.File tmp = new java.io.File(file.getPath() + ".tmp");
        try (java.io.DataOutputStream out = new java.io.DataOutputStream(new java.io.BufferedOutputStream(
                new java.util.zip.GZIPOutputStream(new java.io.FileOutputStream(tmp)), 1 << 16))) {
            out.writeInt(CACHE_MAGIC);
            out.writeInt(CACHE_VERSION);
            out.writeUTF(key);
            out.writeLong(check[0]);
            out.writeLong(check[1]);
            out.writeLong(records);
            out.writeLong(geoRecords);
            out.writeLong(callRecords);
            out.writeLong(bucketBase);
            out.writeInt(bucketSec);
            out.writeInt(nBuckets);
            out.writeInt(reps.size());
            for (int i = 0; i < reps.size(); i++) {
                out.writeLong(reps.get(i).getRowId());
                out.writeLong(repTotal[i]);
                char[] idx = repBucketIdx[i];
                int[] cnt = repBucketCnt[i];
                out.writeInt(idx == null ? -1 : idx.length);
                if (idx != null)
                    for (int k = 0; k < idx.length; k++) { out.writeChar(idx[k]); out.writeInt(cnt[k]); }
            }
            out.writeInt(callReps.size());
            for (int i = 0; i < callReps.size(); i++) {
                out.writeLong(callReps.get(i).getRowId());
                out.writeLong(callTotal[i]);
            }
            out.writeInt(minuteKeys.length);
            for (int i = 0; i < minuteKeys.length; i++) {
                out.writeUTF(minuteKeys[i]);
                out.writeLong(minuteCum[i + 1] - minuteCum[i]);
            }
            out.writeInt(CACHE_MAGIC);
        }
        java.nio.file.Files.move(tmp.toPath(), file.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Reads an overview written by {@link #save}. Returns {@code null} if the
     * file does not exist, belongs to another database ({@code key}), has
     * another format, or the table no longer has the same number of records
     * and largest rowid; the representatives are read again from the database.
     */
    public static Overview load(java.io.File file, String key, DataAnalyzer analyzer,
                                DataAnalyzer.ConnectionSource source, DataAnalyzer.RawRowLoader loader) {
        if (file == null || !file.isFile()) return null;
        try (java.io.DataInputStream in = new java.io.DataInputStream(new java.io.BufferedInputStream(
                new java.util.zip.GZIPInputStream(new java.io.FileInputStream(file)), 1 << 16))) {
            if (in.readInt() != CACHE_MAGIC || in.readInt() != CACHE_VERSION || !key.equals(in.readUTF())) return null;
            long count = in.readLong(), maxRowid = in.readLong();
            long[] check = tableCheck(source);
            if (check[0] != count || check[1] != maxRowid) return null;

            Builder b = new Builder(analyzer, source, loader, null, null);
            b.records = in.readLong();
            b.geoRecords = in.readLong();
            b.callRecords = in.readLong();
            b.bucketBase = in.readLong();
            b.bucketSec = in.readInt();
            b.nBuckets = in.readInt();
            int n = in.readInt();
            long[] repIds = new long[n];
            b.repTotal = new long[n];
            b.repBucketIdx = new char[n][];
            b.repBucketCnt = new int[n][];
            for (int i = 0; i < n; i++) {
                repIds[i] = in.readLong();
                b.repTotal[i] = in.readLong();
                int m = in.readInt();
                if (m >= 0) {
                    char[] idx = new char[m];
                    int[] cnt = new int[m];
                    for (int k = 0; k < m; k++) { idx[k] = in.readChar(); cnt[k] = in.readInt(); }
                    b.repBucketIdx[i] = idx;
                    b.repBucketCnt[i] = cnt;
                }
            }
            int c = in.readInt();
            long[] callIds = new long[c];
            b.callTotal = new long[c];
            for (int i = 0; i < c; i++) {
                callIds[i] = in.readLong();
                b.callTotal[i] = in.readLong();
            }
            int mk = in.readInt();
            b.minuteKeys = new String[mk];
            b.minuteCum = new long[mk + 1];
            for (int i = 0; i < mk; i++) {
                b.minuteKeys[i] = in.readUTF();
                b.minuteCum[i + 1] = b.minuteCum[i] + in.readLong();
            }
            if (in.readInt() != CACHE_MAGIC) return null;

            try (Connection conn = source.open()) {
                b.rq = DataAnalyzer.prepareResponseRecords(conn);
                if (b.rq == null) return null;
                Map<Long, DataPoint> byRowid = new HashMap<>();
                long[] all = new long[n + c];
                System.arraycopy(repIds, 0, all, 0, n);
                System.arraycopy(callIds, 0, all, n, c);
                for (int i = 0; i < all.length; i += 500) {
                    StringBuilder ids = new StringBuilder();
                    for (int k = i; k < Math.min(all.length, i + 500); k++) {
                        if (ids.length() > 0) ids.append(',');
                        ids.append(all[k]);
                    }
                    for (DataPoint dp : analyzer.readResponseRecordRows(conn, b.rq, "WHERE rowid IN (" + ids + ")", null, loader, false))
                        byRowid.put(dp.getRowId(), dp);
                }
                b.reps = new ArrayList<>(n);
                for (long id : repIds) {
                    DataPoint dp = byRowid.get(id);
                    if (dp == null || dp.getCoordinate() == null) return null;
                    b.reps.add(dp);
                }
                b.callReps = new ArrayList<>(c);
                for (long id : callIds) {
                    DataPoint dp = byRowid.get(id);
                    if (dp == null || dp.getCoordinate() == null) return null;
                    b.callReps.add(dp);
                }
                b.reps = List.copyOf(b.reps);
                b.callReps = List.copyOf(b.callReps);
            }
            return new Overview(b);
        } catch (java.io.IOException | SQLException | RuntimeException e) {
            return null;
        }
    }

    /** number of records and largest rowid of the table */
    private static long[] tableCheck(DataAnalyzer.ConnectionSource source) throws SQLException {
        try (Connection c = source.open(); java.sql.Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*), MAX(rowid) FROM response_records")) {
            rs.next();
            return new long[] { rs.getLong(1), rs.getLong(2) };
        }
    }

    // ================================================================ build

    /**
     * Builds the overview of the records matching {@code cond}.
     *
     * @param cond     SQL condition on {@code response_records}, or {@code null} for all records
     * @param label    description of the filter for the status line (may be null)
     * @param total    number of matching records if known (progress), else -1
     * @param threads  reading threads
     * @return the overview, or {@code null} if the table lacks the columns of the schema
     */
    public static Overview build(DataAnalyzer analyzer, DataAnalyzer.ConnectionSource source,
                                 DataAnalyzer.RawRowLoader loader, String cond, String label, long total,
                                 DataAnalyzer.ProgressListener listener, int threads) throws SQLException {
        return build(analyzer, source, loader, cond, label, total, listener, threads, null);
    }

    /**
     * @param rows the rowids matching {@code cond} if already known (see
     *             {@link Selection#rows()}): the pass then checks them in
     *             memory instead of evaluating {@code cond} for every row;
     *             {@code cond} is still used for the pages and lists
     */
    public static Overview build(DataAnalyzer analyzer, DataAnalyzer.ConnectionSource source,
                                 DataAnalyzer.RawRowLoader loader, String cond, String label, long total,
                                 DataAnalyzer.ProgressListener listener, int threads,
                                 java.util.BitSet rows) throws SQLException {
        Builder b = new Builder(analyzer, source, loader, cond, label);
        b.rowFilter = rows;
        long t0 = System.nanoTime();
        boolean ok = b.run(total, listener, Math.max(1, threads));
        // read rate of this computer, for the duration estimates of queries
        if (ok && cond == null) QueryCost.record(b.records, (System.nanoTime() - t0) / 1e9);
        return ok ? new Overview(b) : null;
    }

    private record RepKey(long lat, long lon, String azimuth) { }

    /** earliest record (time, then rowid) of a group */
    private static final class Earliest {
        volatile long ts = Long.MAX_VALUE;
        volatile long rowid = Long.MAX_VALUE;

        void offer(long t, long r) {
            if (t > ts) return;
            synchronized (this) {
                if (t < ts || (t == ts && r < rowid)) { ts = t; rowid = r; }
            }
        }

        boolean set() { return rowid != Long.MAX_VALUE; }
    }

    private static final class RepAcc {
        final AtomicLong all = new AtomicLong(), moc = new AtomicLong(), mtc = new AtomicLong();
        final Earliest eAll = new Earliest(), eMoc = new Earliest(), eMtc = new Earliest();
        volatile AtomicIntegerArray buckets;
    }

    /** per range: records per minute of the timeline */
    private static final class MinuteCounts {
        final HashMap<Long, long[]> iso = new HashMap<>();      // key yyyyMMddHHmm
        final HashMap<String, long[]> other = new HashMap<>();  // first 16 characters
    }

    private static final class Builder {
        final DataAnalyzer analyzer;
        final DataAnalyzer.ConnectionSource source;
        final DataAnalyzer.RawRowLoader loader;
        final String cond, label;
        /** rowids to include (instead of evaluating cond in the pass), or null */
        java.util.BitSet rowFilter;
        DataAnalyzer.RrQuery rq;

        long records, geoRecords, callRecords;
        List<DataPoint> reps, callReps;
        long[] repTotal, callTotal;
        char[][] repBucketIdx;
        int[][] repBucketCnt;
        long bucketBase;
        int bucketSec = 3600, nBuckets;
        String[] minuteKeys;
        long[] minuteCum;

        final ConcurrentHashMap<RepKey, RepAcc> accs = new ConcurrentHashMap<>();
        final AtomicLong geo = new AtomicLong(), calls = new AtomicLong(), rows = new AtomicLong();

        Builder(DataAnalyzer analyzer, DataAnalyzer.ConnectionSource source, DataAnalyzer.RawRowLoader loader,
                String cond, String label) {
            this.analyzer = analyzer; this.source = source; this.loader = loader; this.cond = cond; this.label = label;
        }

        boolean run(long total, DataAnalyzer.ProgressListener listener, int threads) throws SQLException {
            long minRowid, maxRowid;
            String scanSql;
            try (Connection conn = source.open()) {
                rq = DataAnalyzer.prepareResponseRecords(conn);
                if (rq == null) return false;
                DataAnalyzer.RrColumns c = rq.c;
                try (java.sql.Statement st = conn.createStatement()) {
                    try (ResultSet rs = st.executeQuery("SELECT MIN(rowid), MAX(rowid) FROM response_records")) {
                        rs.next();
                        minRowid = rs.getLong(1);
                        maxRowid = rs.getLong(2);
                    }
                    if (total < 0 && cond == null) {
                        try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM response_records")) {
                            rs.next();
                            total = rs.getLong(1);
                        }
                    }
                    if (c.startTime >= 0) initBuckets(st, rq.q(c.startTime));
                }
                String none = "NULL";
                scanSql = "SELECT rowid, "
                        + (c.startTime >= 0 ? rq.q(c.startTime) + ", typeof(" + rq.q(c.startTime) + ")" : "NULL, NULL") + ", "
                        + (c.latitude >= 0 ? rq.q(c.latitude) : none) + ", "
                        + (c.longitude >= 0 ? rq.q(c.longitude) : none) + ", "
                        + (c.ltRaw >= 0 ? (c.latitude >= 0 ? DataAnalyzer.rawCoordinate(rq, c.latitude, c.ltRaw) : rq.q(c.ltRaw)) : none) + ", "
                        + (c.loRaw >= 0 ? (c.longitude >= 0 ? DataAnalyzer.rawCoordinate(rq, c.longitude, c.loRaw) : rq.q(c.loRaw)) : none) + ", "
                        + (c.azimuth >= 0 ? rq.q(c.azimuth) : none) + ", "
                        + (c.callIndicator >= 0 ? rq.q(c.callIndicator) : none)
                        + " FROM response_records WHERE rowid BETWEEN ? AND ?"
                        + (cond != null && rowFilter == null ? " AND (" + cond + ")" : "");
            }

            // ranges of rowids, read in parallel
            List<long[]> ranges = new ArrayList<>();
            if (maxRowid >= minRowid) {
                // a condition with a subquery would be evaluated again for every range
                int n = threads > 1 && (rowFilter != null || !DataAnalyzer.hasSubquery(cond)) ? threads * 4 : 1;
                long width = (maxRowid - minRowid) / n + 1;
                for (int i = 0; i < n; i++) {
                    long from = minRowid + i * width;
                    if (from > maxRowid) break;
                    long to = i == n - 1 ? maxRowid : Math.min(maxRowid, from + width - 1);
                    ranges.add(new long[] { from, to });
                }
            }
            final long totalRows = total;
            List<Callable<MinuteCounts>> tasks = new ArrayList<>();
            for (long[] r : ranges)
                tasks.add(() -> scan(scanSql, r[0], r[1], totalRows, listener));
            List<MinuteCounts> parts = new ArrayList<>();
            ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, Math.min(threads, Math.max(1, tasks.size()))), t -> {
                Thread th = new Thread(t, "overview-reader");
                th.setDaemon(true);
                return th;
            });
            try {
                for (Future<MinuteCounts> f : pool.invokeAll(tasks)) parts.add(f.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("interrupted", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof SQLException se) throw se;
                if (cause instanceof RuntimeException re) throw re;
                throw new SQLException(cause);
            } finally {
                pool.shutdownNow();
            }
            if (listener != null) listener.onProgress(rows.get(), Math.max(rows.get(), totalRows));

            records = rows.get();
            geoRecords = geo.get();
            callRecords = calls.get();
            mergeMinutes(parts);
            buildRepresentatives();
            return true;
        }

        /** time buckets between the first and the last text start_time (uses the index) */
        private void initBuckets(java.sql.Statement st, String col) throws SQLException {
            String min = null, max = null;
            try (ResultSet rs = st.executeQuery("SELECT " + col + " FROM response_records WHERE " + col
                    + " >= '' ORDER BY " + col + " LIMIT 1")) {
                if (rs.next()) min = rs.getString(1);
            }
            try (ResultSet rs = st.executeQuery("SELECT " + col + " FROM response_records WHERE " + col
                    + " >= '' AND typeof(" + col + ") = 'text' ORDER BY " + col + " DESC LIMIT 1")) {
                if (rs.next()) max = rs.getString(1);
            }
            Instant a = min == null ? null : DataAnalyzer.parseIsoSeconds(min);
            Instant z = max == null ? null : DataAnalyzer.parseIsoSeconds(max);
            if (a == null || z == null || z.isBefore(a)) { nBuckets = 0; return; }
            long base = Math.floorDiv(a.getEpochSecond(), 3600) * 3600;
            long hours = (z.getEpochSecond() - base) / 3600 + 1;
            int factor = (int) Math.max(1, (hours + MAX_BUCKETS - 1) / MAX_BUCKETS);
            bucketBase = base;
            bucketSec = 3600 * factor;
            nBuckets = (int) ((z.getEpochSecond() - base) / bucketSec + 1);
        }

        private MinuteCounts scan(String sql, long from, long to, long total,
                                  DataAnalyzer.ProgressListener listener) throws SQLException {
            MinuteCounts mc = new MinuteCounts();
            try (Connection conn = source.open(); PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setLong(1, from);
                ps.setLong(2, to);
                try (ResultSet rs = ps.executeQuery()) {
                    RepKey lastKey = null;
                    RepAcc lastAcc = null;
                    java.util.BitSet filter = rowFilter;
                    while (rs.next()) {
                        long rowid = rs.getLong(1);
                        if (filter != null && (rowid < 0 || rowid > Integer.MAX_VALUE || !filter.get((int) rowid))) continue;
                        String st = rs.getString(2);
                        boolean text = "text".equals(rs.getString(3));
                        Double lat = DataAnalyzer.parseDecimalCell(rs.getString(4));
                        Double lon = DataAnalyzer.parseDecimalCell(rs.getString(5));
                        if (lat == null) lat = GPSParser.parseAnyCoordinate(rs.getString(6));
                        if (lon == null) lon = GPSParser.parseAnyCoordinate(rs.getString(7));
                        String az = rs.getString(8);
                        String call = rs.getString(9);

                        // timeline: records per minute of the start_time text
                        if (st != null && text) {
                            if (DataAnalyzer.isIsoSecondsShape(st)) {
                                long k = isoMinuteKey(st);
                                long[] v = mc.iso.get(k);
                                if (v == null) mc.iso.put(k, v = new long[1]);
                                v[0]++;
                            } else {
                                String k = st.length() > 16 ? st.substring(0, 16) : st;
                                long[] v = mc.other.get(k);
                                if (v == null) mc.other.put(k, v = new long[1]);
                                v[0]++;
                            }
                        }

                        Instant ts = st == null ? null : DataAnalyzer.parseIsoSeconds(st.trim());
                        if (ts == null && st != null) ts = analyzer.parseTimestamp(st);
                        if (lat != null && lon != null && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180) {
                            geo.incrementAndGet();
                            RepKey key = new RepKey(Double.doubleToLongBits(lat), Double.doubleToLongBits(lon), az);
                            RepAcc acc;
                            if (key.equals(lastKey)) acc = lastAcc;
                            else {
                                acc = accs.computeIfAbsent(key, x -> new RepAcc());
                                lastKey = key;
                                lastAcc = acc;
                            }
                            long t = ts == null ? Long.MAX_VALUE : ts.getEpochSecond();
                            acc.all.incrementAndGet();
                            acc.eAll.offer(t, rowid);
                            if ("MOC".equals(call)) {
                                calls.incrementAndGet();
                                acc.moc.incrementAndGet();
                                acc.eMoc.offer(t, rowid);
                            } else if ("MTC".equals(call)) {
                                calls.incrementAndGet();
                                acc.mtc.incrementAndGet();
                                acc.eMtc.offer(t, rowid);
                            }
                            if (ts != null && nBuckets > 0) {
                                long b = Math.floorDiv(t - bucketBase, bucketSec);
                                if (b >= 0 && b < nBuckets) {
                                    AtomicIntegerArray bk = acc.buckets;
                                    if (bk == null) {
                                        synchronized (acc) {
                                            if (acc.buckets == null) acc.buckets = new AtomicIntegerArray(nBuckets);
                                            bk = acc.buckets;
                                        }
                                    }
                                    bk.incrementAndGet((int) b);
                                }
                            }
                        }
                        long done = rows.incrementAndGet();
                        if (listener != null && done % 20_000 == 0) listener.onProgress(done, Math.max(done, total));
                    }
                }
            }
            return mc;
        }

        /** yyyyMMddHHmm of a "yyyy-MM-dd HH:mm:ss" value */
        private static long isoMinuteKey(String s) {
            long k = 0;
            for (int i = 0; i < 16; i++) {
                char ch = s.charAt(i);
                if (ch >= '0' && ch <= '9') k = k * 10 + (ch - '0');
            }
            return k;
        }

        private static String isoMinuteString(long k) {
            long mi = k % 100; k /= 100;
            long h = k % 100; k /= 100;
            long d = k % 100; k /= 100;
            long mo = k % 100; k /= 100;
            return String.format("%04d-%02d-%02d %02d:%02d", k, mo, d, h, mi);
        }

        private void mergeMinutes(List<MinuteCounts> parts) {
            TreeMap<String, long[]> all = new TreeMap<>();
            for (MinuteCounts mc : parts) {
                for (Map.Entry<Long, long[]> e : mc.iso.entrySet())
                    all.computeIfAbsent(isoMinuteString(e.getKey()), x -> new long[1])[0] += e.getValue()[0];
                for (Map.Entry<String, long[]> e : mc.other.entrySet())
                    all.computeIfAbsent(e.getKey(), x -> new long[1])[0] += e.getValue()[0];
            }
            minuteKeys = all.keySet().toArray(new String[0]);
            minuteCum = new long[minuteKeys.length + 1];
            int i = 0;
            for (long[] v : all.values()) {
                minuteCum[i + 1] = minuteCum[i] + v[0];
                i++;
            }
        }

        private void buildRepresentatives() throws SQLException {
            List<RepAcc> list = new ArrayList<>(accs.values());
            List<Long> ids = new ArrayList<>();
            for (RepAcc a : list) {
                if (a.eAll.set()) ids.add(a.eAll.rowid);
                if (a.eMoc.set()) ids.add(a.eMoc.rowid);
                if (a.eMtc.set()) ids.add(a.eMtc.rowid);
            }
            Map<Long, DataPoint> byRowid = new HashMap<>();
            try (Connection conn = source.open()) {
                for (int i = 0; i < ids.size(); i += 500) {
                    StringBuilder in = new StringBuilder();
                    for (long id : ids.subList(i, Math.min(ids.size(), i + 500))) {
                        if (in.length() > 0) in.append(',');
                        in.append(id);
                    }
                    for (DataPoint dp : analyzer.readResponseRecordRows(conn, rq, "WHERE rowid IN (" + in + ")", null, loader, false))
                        byRowid.put(dp.getRowId(), dp);
                }
            }

            record Rep(DataPoint dp, long ts, long rowid, long count, char[] idx, int[] cnt) { }
            List<Rep> all = new ArrayList<>(), callList = new ArrayList<>();
            for (RepAcc a : list) {
                DataPoint dp = byRowid.get(a.eAll.rowid);
                if (dp != null && dp.getCoordinate() != null) {
                    char[] idx = null;
                    int[] cnt = null;
                    AtomicIntegerArray ab = a.buckets;
                    if (ab != null) {
                        int nz = 0;
                        for (int i = 0; i < ab.length(); i++) if (ab.get(i) != 0) nz++;
                        idx = new char[nz];
                        cnt = new int[nz];
                        for (int i = 0, k = 0; i < ab.length(); i++) {
                            int v = ab.get(i);
                            if (v != 0) { idx[k] = (char) i; cnt[k++] = v; }
                        }
                        a.buckets = null;
                    }
                    all.add(new Rep(dp, a.eAll.ts, a.eAll.rowid, a.all.get(), idx, cnt));
                }
                if (a.eMoc.set()) {
                    DataPoint m = byRowid.get(a.eMoc.rowid);
                    if (m != null && m.getCoordinate() != null) callList.add(new Rep(m, a.eMoc.ts, a.eMoc.rowid, a.moc.get(), null, null));
                }
                if (a.eMtc.set()) {
                    DataPoint m = byRowid.get(a.eMtc.rowid);
                    if (m != null && m.getCoordinate() != null) callList.add(new Rep(m, a.eMtc.ts, a.eMtc.rowid, a.mtc.get(), null, null));
                }
            }
            Comparator<Rep> order = Comparator.comparingLong(Rep::ts).thenComparingLong(Rep::rowid);
            all.sort(order);
            callList.sort(order);
            reps = new ArrayList<>(all.size());
            repTotal = new long[all.size()];
            repBucketIdx = new char[all.size()][];
            repBucketCnt = new int[all.size()][];
            for (int i = 0; i < all.size(); i++) {
                reps.add(all.get(i).dp());
                repTotal[i] = all.get(i).count();
                repBucketIdx[i] = all.get(i).idx();
                repBucketCnt[i] = all.get(i).cnt();
            }
            callReps = new ArrayList<>(callList.size());
            callTotal = new long[callList.size()];
            for (int i = 0; i < callList.size(); i++) {
                callReps.add(callList.get(i).dp());
                callTotal[i] = callList.get(i).count();
            }
            reps = List.copyOf(reps);
            callReps = List.copyOf(callReps);
        }
    }
}
