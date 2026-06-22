package fqlite.timemap;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

/**
 * Resolves cell tower positions from a locally stored OpenCelliD CSV export.
 *
 * <h3>Four-level lookup strategy</h3>
 * <ol>
 *   <li><b>Exact</b> — {@code (mnc, lac, cell)} key present in index.</li>
 *   <li><b>eNB centroid</b> — for LTE/NR ({@code ECI > 65535}): derives
 *       {@code eNB-ID = ECI >> 8} and searches across <em>all</em> MNCs.
 *       Returns the sample-weighted centroid of all known sectors.
 *       Handles MNC mismatches from carrier-specific GTPv1 encoding.</li>
 *   <li><b>LAC-as-eNB</b> — if decoded LAC equals an eNB-ID (TAC absent
 *       in ULI), same centroid logic across all MNCs.</li>
 *   <li><b>GPS proximity fallback</b> — if no cell is found via ULI but the
 *       {@link DataAnalyzer.DataPoint} carries GPS coordinates, returns the
 *       nearest tower of the same MNC within {@value #GPS_FALLBACK_RADIUS_KM} km.
 *       Use {@link #resolveWithGps} to trigger this level.</li>
 * </ol>
 *
 * <h3>Why GPS fallback is necessary</h3>
 * OpenCelliD is crowd-sourced. Many cell towers — especially those belonging
 * to Vodafone DE (MNC=2) in eastern Germany — are simply absent from the
 * country CSV. For the test dataset all eNBs 201386 / 205573 / 209952
 * are missing from 262.csv; the nearest Vodafone tower is 3.4 km from Guben.
 * The GPS fallback makes these cases visible on the map.
 */
public class CellTowerResolver {

    private static final Logger LOG = Logger.getLogger(CellTowerResolver.class.getName());

    /** Maximum distance (km) for the GPS-proximity fallback. */
    public static final double GPS_FALLBACK_RADIUS_KM = 10.0;

    // ── Singleton ─────────────────────────────────────────────────────────────
    private static final CellTowerResolver INSTANCE = new CellTowerResolver();
    public  static CellTowerResolver getInstance() { return INSTANCE; }

    // ── CSV column indices ────────────────────────────────────────────────────
    private static final int COL_RADIO   = 0;
    private static final int COL_MCC     = 1;
    private static final int COL_MNC     = 2;
    private static final int COL_LAC     = 3;
    private static final int COL_CELL    = 4;
    private static final int COL_LON     = 6;
    private static final int COL_LAT     = 7;
    private static final int COL_RANGE   = 8;
    private static final int COL_SAMPLES = 9;
    private static final int MIN_COLS    = 10;

    // ── Index structures ──────────────────────────────────────────────────────

    /** Primary:  (mnc, lac, cell) → CsvEntry */
    private final Map<Long, CsvEntry>       exactIndex = new HashMap<>(300_000);

    /** Secondary: (mnc, eNB-ID)  → sector list  (LTE/NR only) */
    private final Map<Long, List<CsvEntry>> eNbIndex   = new HashMap<>(70_000);

    /**
     * Tertiary: per-MNC spatial list for GPS-proximity fallback.
     * Key = MNC, value = all entries with valid coordinates, sorted by lat
     * (enables a fast latitude-band pre-filter).
     */
    private final Map<Integer, List<CsvEntry>> gpsIndex = new HashMap<>();

    private final Object  lock    = new Object();
    private volatile Path csvPath = null;
    private volatile boolean loaded  = false;
    private volatile boolean loading = false;

    private final ExecutorService pool = Executors.newFixedThreadPool(2, r -> {
        Thread t = new Thread(r, "celltower-loader");
        t.setDaemon(true);
        return t;
    });

    private CellTowerResolver() {}

    // =========================================================================
    // Configuration
    // =========================================================================

    public void setCsvPath(Path path) {
        synchronized (lock) {
            csvPath = path;
            exactIndex.clear();
            eNbIndex.clear();
            gpsIndex.clear();
            loaded  = false;
            loading = false;
        }
        LOG.info("CellTowerResolver CSV: " + (path != null ? path : "cleared"));
    }

    public Path    getCsvPath()         { return csvPath; }
    public boolean isCsvLoaded()        { return loaded;  }
    public boolean isCsvConfigured()    { return csvPath != null && Files.exists(csvPath); }
    public int     getCachedTowerCount(){ return exactIndex.size(); }

    /** Starts background pre-load so the first resolve call does not block. */
    public void loadAsync() {
        synchronized (lock) {
            if (loaded || loading || csvPath == null) return;
            loading = true;
        }
        pool.submit(this::loadCsv);
    }

    // =========================================================================
    // Public resolve API
    // =========================================================================

    /** Async resolve from ULI cell info only (no GPS fallback). */
    public CompletableFuture<CellTower> resolveAsync(UliDecoder.CellInfo ci) {
        return CompletableFuture.supplyAsync(() -> resolve(ci), pool);
    }

    /**
     * Async resolve with GPS fallback.
     *
     * <p>If levels 1–3 yield no result and {@code gpsLat}/{@code gpsLon} are
     * valid (non-NaN), the nearest tower of the same MNC within
     * {@value #GPS_FALLBACK_RADIUS_KM} km is returned.</p>
     *
     * @param ci     decoded ULI (may be {@code null})
     * @param gpsLat GPS latitude of the data point (from the {@code latitude} column)
     * @param gpsLon GPS longitude of the data point (from the {@code longitude} column)
     */
    public CompletableFuture<CellTower> resolveAsync(
            UliDecoder.CellInfo ci, double gpsLat, double gpsLon) {
        return CompletableFuture.supplyAsync(
                () -> resolveWithGps(ci, gpsLat, gpsLon), pool);
    }

    /** Synchronous resolve from ULI only. */
    public CellTower resolve(UliDecoder.CellInfo ci) {
        if (ci == null || !ci.isQueryable()) return null;
        ensureLoaded();
        return doLookup(ci.mnc, ci.lac, ci.cellId, ci.mcc);
    }

    /**
     * Synchronous resolve with GPS proximity fallback (level 4).
     *
     * @param ci     decoded ULI; may be {@code null}
     * @param gpsLat GPS latitude from the data point
     * @param gpsLon GPS longitude from the data point
     */
    public CellTower resolveWithGps(
            UliDecoder.CellInfo ci, double gpsLat, double gpsLon) {
        ensureLoaded();

        // Levels 1–3: ULI-based CSV lookup
        if (ci != null && ci.isQueryable()) {
            CellTower t = doLookup(ci.mnc, ci.lac, ci.cellId, ci.mcc);
            if (t != null) return t;
        }

        // Level 4: GPS proximity fallback (nearest tower in local CSV)
        if (!Double.isNaN(gpsLat) && !Double.isNaN(gpsLon)) {
            int preferMnc = (ci != null) ? ci.mnc : -1;
            CellTower t4 = nearestByGps(gpsLat, gpsLon, preferMnc);
            if (t4 != null) return t4;
        }

        // Level 5: beaconDB online lookup
        // Only reached when both local CSV and GPS fallback failed.
        // The BeaconDbClient is a no-op when disabled.
        if (ci != null && ci.isQueryable()) {
            CellTower t5 = BeaconDbClient.getInstance().lookup(ci);
            if (t5 != null) {
                LOG.fine("beaconDB hit for " + ci);
                System.out.println("beaconDB hit for " + ci);
                return t5;
            }
        }




        return null;
    }

    /** Direct numeric lookup (bypasses ULI decoding). */
    public CellTower resolve(int mnc, int lac, int cell) {
        ensureLoaded();
        return doLookup(mnc, lac, cell, 262);
    }

    /** Clears all in-memory indices (does not delete the CSV file). */
    public void clearCache() {
        synchronized (lock) {
            exactIndex.clear();
            eNbIndex.clear();
            gpsIndex.clear();
            loaded  = false;
            loading = false;
        }
    }

    // =========================================================================
    // Lookup logic
    // =========================================================================

    private CellTower doLookup(int mnc, int lac, int cell, int mcc) {

        // Level 1: exact (mnc, lac, cell)
        CsvEntry hit = exactIndex.get(exactKey(mnc, lac, cell));
        if (hit != null) return toTower(hit, "exact");

        // Level 2: ECI → eNB centroid, restricted to the MNC decoded from the ULI
        if (cell > 65535) {
            int       eNbId = cell >> 8;
            CellTower t     = eNbCentroid(eNbId, mnc);
            if (t != null) return t;
        }

        // Level 3: LAC might itself be an eNB-ID (TAC absent in ULI)
        CellTower t3 = eNbCentroid(lac, mnc);
        if (t3 != null) return t3;

        return null;
    }

    /**
     * Weighted centroid of all known sectors of {@code eNbId} for the given
     * MNC only.
     *
     * <p>eNB-IDs are assigned per operator and are <b>not</b> globally unique,
     * so this must not search across other MNCs: doing so previously returned
     * geographically unrelated towers (a different operator's network,
     * potentially far away) whenever the decoded operator's own CSV coverage
     * for that eNB-ID was missing. Genuine coverage gaps are instead handled
     * by the GPS-proximity fallback (level 4, see {@link #resolveWithGps}),
     * which at least returns a tower near the device's real position.</p>
     */
    private CellTower eNbCentroid(int eNbId, int mnc) {
        List<CsvEntry> best = eNbIndex.get(eNbKey(mnc, eNbId));
        if (best == null || best.isEmpty()) return null;

        long   wSum = 0;
        double wLon = 0, wLat = 0;
        int    rangeSum = 0, rangeCount = 0;
        for (CsvEntry c : best) {
            long w  = Math.max(1, c.samples);
            wSum   += w; wLon += c.lon * w; wLat += c.lat * w;
            if (c.range > 0) { rangeSum += c.range; rangeCount++; }
        }
        double lon   = wLon / wSum;
        double lat   = wLat / wSum;
        int    range = rangeCount > 0 ? rangeSum / rangeCount : -1;
        int    mcc   = best.get(0).mcc;
        int    tac   = best.get(0).lac;
        return new CellTower(
                best.get(0).radio + "/eNB-centroid",
                mcc, mnc, tac, eNbId,
                lat, lon, range,
                MncLookup.getOperator(mcc, mnc),
                MncLookup.getCountry(mcc),
                (int) Math.min(wSum, Integer.MAX_VALUE),
                best.size() >= 3);
    }

    /**
     * Level-4 fallback: returns the nearest tower to the given GPS position.
     *
     * <p>Searches the preferred MNC first. If nothing is within
     * {@value #GPS_FALLBACK_RADIUS_KM} km, tries all MNCs.</p>
     *
     * @param gpsLat    GPS latitude of the device
     * @param gpsLon    GPS longitude of the device
     * @param preferMnc MNC from the ULI; {@code -1} = no preference
     */
    private CellTower nearestByGps(double gpsLat, double gpsLon, int preferMnc) {
        CsvEntry best   = null;
        double   bestDist = Double.MAX_VALUE;

        // Search preferred MNC first, then all others
        List<Integer> mncsToSearch = new ArrayList<>();
        if (preferMnc > 0) mncsToSearch.add(preferMnc);
        for (Integer mnc : gpsIndex.keySet())
            if (mnc != preferMnc) mncsToSearch.add(mnc);

        for (int mnc : mncsToSearch) {
            List<CsvEntry> list = gpsIndex.get(mnc);
            if (list == null) continue;

            // Latitude band pre-filter: ±0.5° ≈ ±55 km → cheap linear scan
            double latDelta = GPS_FALLBACK_RADIUS_KM / 111.0;
            double latMin   = gpsLat - latDelta;
            double latMax   = gpsLat + latDelta;

            // Binary search for lower bound
            int lo = 0, hi = list.size();
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (list.get(mid).lat < latMin) lo = mid + 1; else hi = mid;
            }

            for (int i = lo; i < list.size(); i++) {
                CsvEntry e = list.get(i);
                if (e.lat > latMax) break;
                double dist = haversineKm(gpsLat, gpsLon, e.lat, e.lon);
                if (dist < bestDist && dist <= GPS_FALLBACK_RADIUS_KM) {
                    bestDist = dist;
                    best     = e;
                }
            }

            // Stop early if we already found a very close match in the preferred MNC
            if (best != null && bestDist < 1.0 && mnc == preferMnc) break;
        }

        if (best == null) return null;
        LOG.fine(String.format("GPS fallback: nearest tower %.2f km away (MNC=%d)", bestDist, best.mnc));
        return toTower(best, String.format("gps-fallback(%.1fkm)", bestDist));
    }

    // ── Haversine distance ────────────────────────────────────────────────────

    private static double haversineKm(double lat1, double lon1,
                                       double lat2, double lon2) {
        final double R = 6371.0;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a    = Math.sin(dLat/2) * Math.sin(dLat/2)
                    + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                    * Math.sin(dLon/2) * Math.sin(dLon/2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    }

    // ── Tower factory ─────────────────────────────────────────────────────────

    private static CellTower toTower(CsvEntry e, String matchSuffix) {
        // ECI: für LTE ist e.cell der volle 28-bit ECI-Wert aus der CSV
        int eci = ("LTE".equals(e.radio) || "NR".equals(e.radio)) ? e.cell : -1;
        return new CellTower(
                e.radio + "/" + matchSuffix,
                e.mcc, e.mnc, e.lac, e.cell & 0xFF, eci,
                e.lat, e.lon, e.range,
                MncLookup.getOperator(e.mcc, e.mnc),
                MncLookup.getCountry(e.mcc),
                e.samples, e.samples >= 5);
    }

    // =========================================================================
    // CSV loading
    // =========================================================================

    private void ensureLoaded() {
        if (loaded) return;
        synchronized (lock) {
            if (loaded)   return;
            if (!loading) loading = true;
            else { waitForLoad(); return; }
        }
        loadCsv();
    }

    private void waitForLoad() {
        while (!loaded) {
            try { Thread.sleep(30); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
    }

    private void loadCsv() {
        Path p;
        synchronized (lock) { p = csvPath; }
        if (p == null || !Files.exists(p)) {
            LOG.warning("CellTowerResolver: CSV not found: " + p);
            synchronized (lock) { loaded = true; loading = false; }
            return;
        }

        long t0 = System.currentTimeMillis();
        int  rows = 0, skipped = 0;

        // Temporary GPS lists (unsorted during load, sorted afterward)
        Map<Integer, List<CsvEntry>> tmpGps = new HashMap<>();

        try (BufferedReader br = openReader(p)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty() || line.startsWith("radio")) continue;
                String[] f = line.split(",", MIN_COLS + 1);
                if (f.length < MIN_COLS) { skipped++; continue; }
                try {
                    String radio   = f[COL_RADIO].intern();
                    int    mcc     = Integer.parseInt(f[COL_MCC].trim());
                    int    mnc     = Integer.parseInt(f[COL_MNC].trim());
                    int    lac     = Integer.parseInt(f[COL_LAC].trim());
                    int    cell    = Integer.parseInt(f[COL_CELL].trim());
                    double lon     = Double.parseDouble(f[COL_LON].trim());
                    double lat     = Double.parseDouble(f[COL_LAT].trim());
                    int    range   = Integer.parseInt(f[COL_RANGE].trim());
                    int    samples = Integer.parseInt(f[COL_SAMPLES].trim());

                    if (lon == 0.0 && lat == 0.0) { skipped++; continue; }

                    CsvEntry e = new CsvEntry(radio, mcc, mnc, lac, cell,
                                              lon, lat, range, samples);

                    exactIndex.put(exactKey(mnc, lac, cell), e);

                    if ("LTE".equals(radio) || "NR".equals(radio)) {
                        eNbIndex.computeIfAbsent(eNbKey(mnc, cell >> 8),
                                                 k -> new ArrayList<>(8))
                                .add(e);
                    }

                    // GPS index: all entries with valid position
                    tmpGps.computeIfAbsent(mnc, k -> new ArrayList<>()).add(e);
                    rows++;

                } catch (NumberFormatException | ArrayIndexOutOfBoundsException ex) {
                    skipped++;
                }
            }
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "CSV load error: " + p, ex);
        }

        // Sort each per-MNC GPS list by latitude for binary-search band filter
        for (Map.Entry<Integer, List<CsvEntry>> entry : tmpGps.entrySet()) {
            List<CsvEntry> list = entry.getValue();
            list.sort(Comparator.comparingDouble(e -> e.lat));
            gpsIndex.put(entry.getKey(), list);
        }

        synchronized (lock) { loaded = true; loading = false; }
        LOG.info(String.format(
                "CellTowerResolver: %,d towers (%,d eNBs, %,d GPS entries, %,d skipped) in %,d ms",
                rows, eNbIndex.size(), rows, skipped,
                System.currentTimeMillis() - t0));
    }

    private static BufferedReader openReader(Path p) throws IOException {
        InputStream is = new BufferedInputStream(Files.newInputStream(p), 512 * 1024);
        if (p.toString().endsWith(".gz")) is = new GZIPInputStream(is);
        return new BufferedReader(new InputStreamReader(is), 256 * 1024);
    }

    // =========================================================================
    // Key encoding
    // =========================================================================

    private static long exactKey(int mnc, int lac, int cell) {
        return ((long)(mnc & 0x3FF) << 44)
             | ((long)(lac & 0xFFFF) << 28)
             |  (cell & 0xFFFFFFF);
    }

    private static long eNbKey(int mnc, int eNbId) {
        return ((long)(mnc & 0xFFFFF) << 20) | (eNbId & 0xFFFFF);
    }

    // =========================================================================
    // Internal data record
    // =========================================================================

    private static final class CsvEntry {
        final String radio;
        final int    mcc, mnc, lac, cell;
        final double lon, lat;
        final int    range, samples;

        CsvEntry(String radio, int mcc, int mnc, int lac, int cell,
                 double lon, double lat, int range, int samples) {
            this.radio = radio; this.mcc = mcc; this.mnc = mnc;
            this.lac   = lac;   this.cell = cell;
            this.lon   = lon;   this.lat  = lat;
            this.range = range; this.samples = samples;
        }
    }
}
