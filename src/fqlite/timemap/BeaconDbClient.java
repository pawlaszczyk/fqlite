package fqlite.timemap;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Locale;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * HTTP client for the <a href="https://beacondb.net">beaconDB</a> geolocation
 * API with two-level caching and in-flight deduplication.
 *
 * <h3>Caching strategy</h3>
 * <pre>
 * lookup(ci)
 *   │
 *   ├─ 1. RAM cache hit?  → return immediately  (ConcurrentHashMap)
 *   │
 *   ├─ 2. In-flight?      → wait for running future  (deduplication)
 *   │
 *   ├─ 3. Disk cache hit? → parse + populate RAM cache + return
 *   │        (~/.fqlite/beacondb/mcc_mnc_lac_cell.csv)
 *   │
 *   └─ 4. HTTP POST to api.beacondb.net/v1/geolocate
 *            → on success: write to disk cache + RAM cache + return
 *            → on 404:     write "NOT_FOUND" sentinel to disk cache + return null
 * </pre>
 *
 * <p>With 36 unique ULIs out of 2286 rows the disk cache is populated after
 * the first pass; every subsequent run serves all results instantly from disk
 * without any network traffic.</p>
 *
 * <h3>Cache key</h3>
 * {@code radioType_mcc_mnc_lac_cellId} — one line per entry in the CSV.
 *
 * <h3>Thread safety</h3>
 * All public methods are thread-safe.
 */
public final class BeaconDbClient {

    private static final Logger LOG = Logger.getLogger(BeaconDbClient.class.getName());

    private static final String API_URL    = "https://api.beacondb.net/v1/geolocate";
    private static final String USER_AGENT =
            "FQLite GeoTimeAnalyzer/1.0 (https://github.com/pawlaszczyk/fqlite)";

    private static final int CONNECT_TIMEOUT_MS = 6_000;
    private static final int READ_TIMEOUT_MS    = 8_000;

    /** Disk cache directory. */
    private static final Path CACHE_DIR =
            Path.of(System.getProperty("user.home"), ".fqlite", "beacondb");

    /** Sentinel stored on disk when beaconDB returns 404 (avoids re-querying). */
    private static final String NOT_FOUND = "NOT_FOUND";

    // ── Singleton ─────────────────────────────────────────────────────────────
    private static final BeaconDbClient INSTANCE = new BeaconDbClient();
    public  static BeaconDbClient getInstance() { return INSTANCE; }

    // ── State ─────────────────────────────────────────────────────────────────
    private volatile boolean enabled = false;

    /**
     * RAM cache: cacheKey → CellTower (or {@code null} for 404 results).
     * Null values are stored as the {@link #NOT_FOUND} sentinel string
     * in the disk cache but as actual {@code null} here after loading.
     * We use a wrapper to distinguish "not yet looked up" from "looked up, 404".
     */
    private final ConcurrentHashMap<String, CellTower> ramCache  =
            new ConcurrentHashMap<>();

    /**
     * Sentinel set: keys for which beaconDB returned 404 so we never retry.
     */
    private final ConcurrentHashMap.KeySetView<String, Boolean> notFoundSet =
            ConcurrentHashMap.newKeySet();

    /**
     * In-flight deduplication: when two threads look up the same key
     * simultaneously, only one fires the HTTP request; the other waits.
     */
    private final ConcurrentHashMap<String, CompletableFuture<CellTower>> inFlight =
            new ConcurrentHashMap<>();

    private BeaconDbClient() {}

    // =========================================================================
    // Configuration
    // =========================================================================

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        LOG.info("BeaconDB online fallback: " + (enabled ? "ENABLED" : "DISABLED"));
    }

    public boolean isEnabled() { return enabled; }

    /** Returns the number of entries currently in the RAM cache. */
    public int getCacheSize() { return ramCache.size() + notFoundSet.size(); }

    /** Clears both RAM and disk cache. */
    public void clearCache() {
        ramCache.clear();
        notFoundSet.clear();
        inFlight.clear();
        try {
            if (Files.exists(CACHE_DIR)) {
                try (var stream = Files.list(CACHE_DIR)) {
                    stream.filter(p -> p.toString().endsWith(".txt"))
                            .forEach(p -> { try { Files.delete(p); } catch (IOException ignored) {} });
                }
            }
        } catch (IOException ex) {
            LOG.log(Level.WARNING, "BeaconDB: cache clear error", ex);
        }
        LOG.info("BeaconDB cache cleared");
    }

    // =========================================================================
    // Public lookup API
    // =========================================================================

    /**
     * Looks up the position of the cell described by {@code ci}.
     *
     * <p>The call is synchronous from the caller's perspective but uses
     * {@link CompletableFuture} internally to deduplicate concurrent requests
     * for the same cell key.</p>
     *
     * @param ci decoded ULI; must not be {@code null}
     * @return resolved {@link CellTower}, or {@code null} on miss / disabled
     */
    public CellTower lookup(UliDecoder.CellInfo ci) {
        if (!enabled || ci == null || !ci.isQueryable()) return null;

        String radioType = toRadioType(ci.networkType);

        // Für LTE/ECGI liefert der UliDecoder:
        //   ci.lac    = eNB-ID  (obere 20 bit des ECI)
        //   ci.cellId = CI      (untere 8 bit des ECI, Sektornummer)
        // beaconDB erwartet den vollen 28-bit ECI als cellId.
        int lac;
        int cellId;
        if ("lte".equals(radioType) || "nr".equals(radioType)) {
            lac    = ci.lac;                              // eNB-ID als LAC-Ersatz
            cellId = (ci.lac << 8) | (ci.cellId & 0xFF); // vollständiger ECI
        } else {
            lac    = ci.lac;
            cellId = ci.cellId;
        }

        String key = cacheKey(radioType, ci.mcc, ci.mnc, lac, cellId);
        return lookupByKey(key, radioType, ci.mcc, ci.mnc, lac, cellId, ci);
    }

    /** Raw numeric lookup (bypasses ULI decoding). */
    public CellTower lookup(String radioType, int mcc, int mnc, int lac, int cellId) {
        if (!enabled) return null;
        String key = cacheKey(radioType, mcc, mnc, lac, cellId);
        return lookupByKey(key, radioType, mcc, mnc, lac, cellId, null);
    }

    // =========================================================================
    // Core lookup (cache hierarchy + deduplication)
    // =========================================================================

    private CellTower lookupByKey(String key, String radioType,
                                  int mcc, int mnc, int lac, int cellId,
                                  UliDecoder.CellInfo ci) {
        // ── 1. RAM cache ──────────────────────────────────────────────────────
        CellTower cached = ramCache.get(key);
        if (cached != null) return cached;
        if (notFoundSet.contains(key)) return null;   // known 404

        // ── 2. Disk cache ─────────────────────────────────────────────────────
        CellTower fromDisk = loadFromDisk(key, ci);
        if (fromDisk != null) {
            ramCache.put(key, fromDisk);
            return fromDisk;
        }
        // loadFromDisk returns null BOTH for "file not found" and "NOT_FOUND".
        // If the key is now in notFoundSet, a 404 was cached on disk.
        if (notFoundSet.contains(key)) return null;

        // ── 3. In-flight deduplication ────────────────────────────────────────
        // Only one thread actually fires the HTTP request; others join the future.
        CompletableFuture<CellTower> mine = new CompletableFuture<>();
        CompletableFuture<CellTower> existing = inFlight.putIfAbsent(key, mine);

        if (existing != null) {
            // Another thread is already fetching this key — wait for it
            try {
                return existing.get(CONNECT_TIMEOUT_MS + READ_TIMEOUT_MS,
                        TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                LOG.log(Level.FINE, "BeaconDB: in-flight wait failed for " + key, ex);
                return null;
            }
        }

        // ── 4. HTTP fetch ─────────────────────────────────────────────────────
        try {
            String    body   = buildJson(radioType, mcc, mnc, lac, cellId);
            CellTower result = post(body, ci);

            if (result != null) {
                ramCache.put(key, result);
                saveToDisk(key, result);
            } else {
                notFoundSet.add(key);
                saveNotFoundToDisk(key);
            }

            mine.complete(result);
            return result;

        } catch (Exception ex) {
            mine.completeExceptionally(ex);
            return null;
        } finally {
            inFlight.remove(key);
        }
    }

    // =========================================================================
    // Disk cache
    // =========================================================================

    /**
     * Cache file format (one file per key, plain text):
     * <pre>
     * NOT_FOUND
     * </pre>
     * or
     * <pre>
     * lat,lng,range,mcc,mnc,lac,cellId,eci,networkType,operator,country
     * </pre>
     */
    private Path cacheFile(String key) {
        // Replace characters illegal in filenames
        String safe = key.replaceAll("[^a-zA-Z0-9_\\-]", "_");
        return CACHE_DIR.resolve(safe + ".txt");
    }

    private CellTower loadFromDisk(String key, UliDecoder.CellInfo ci) {
        Path f = cacheFile(key);
        if (!Files.exists(f)) return null;
        try {
            String line = Files.readString(f).strip();
            if (NOT_FOUND.equals(line)) {
                notFoundSet.add(key);   // re-populate sentinel set
                return null;
            }
            return parseDiskLine(line, ci);
        } catch (IOException ex) {
            LOG.log(Level.FINE, "BeaconDB: disk read error: " + f, ex);
            return null;
        }
    }

    private void saveToDisk(String key, CellTower t) {
        // Locale.US erzwingt Dezimalpunkt statt Komma (wichtig für parseDiskLine)
        String line = String.format(Locale.US,
                "%.6f,%.6f,%d,%d,%d,%d,%d,%d,%s,%s,%s",
                t.latitude, t.longitude, t.rangeMetres,
                t.mcc, t.mnc, t.lac, t.cellId, t.eci,
                safe(t.networkType), safe(t.operatorName), safe(t.country));
        writeDiskFile(cacheFile(key), line);
    }

    private void saveNotFoundToDisk(String key) {
        writeDiskFile(cacheFile(key), NOT_FOUND);
    }

    private void writeDiskFile(Path f, String content) {
        try {
            Files.createDirectories(CACHE_DIR);
            Files.writeString(f, content, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException ex) {
            LOG.log(Level.WARNING, "BeaconDB: disk write error: " + f, ex);
        }
    }

    private static CellTower parseDiskLine(String line, UliDecoder.CellInfo ci) {
        String[] f = line.split(",", -1);
        if (f.length < 11) return null;
        try {
            // Locale.US: Dezimalpunkt auch auf deutschen Systemen korrekt parsen
            double lat     = Double.parseDouble(f[0].replace(',', '.'));
            double lng     = Double.parseDouble(f[1].replace(',', '.'));
            int    range   = Integer.parseInt(f[2]);
            int    mcc     = Integer.parseInt(f[3]);
            int    mnc     = Integer.parseInt(f[4]);
            int    lac     = Integer.parseInt(f[5]);
            int    cellId  = Integer.parseInt(f[6]);
            int    eci     = Integer.parseInt(f[7]);
            String netType = f[8].isEmpty() ? (ci != null ? ci.networkType : "?") : f[8];
            String op      = f[9].isEmpty()  ? null : f[9];
            String country = f[10].isEmpty() ? null : f[10];
            return new CellTower(netType, mcc, mnc, lac, cellId, eci,
                    lat, lng, range, op, country,
                    -1, range > 0 && range <= 5000);
        } catch (NumberFormatException ex) {
            LOG.warning("BeaconDB: parseDiskLine Fehler: " + line + " → " + ex.getMessage());
            return null;
        }
    }

    private static String safe(String s) { return s == null ? "" : s.replace(",", ";"); }

    // =========================================================================
    // HTTP POST
    // =========================================================================

    private CellTower post(String jsonBody, UliDecoder.CellInfo ci) {
        try {
            HttpURLConnection conn =
                    (HttpURLConnection) new URL(API_URL).openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept",       "application/json");
            conn.setRequestProperty("User-Agent",   USER_AGENT);
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(READ_TIMEOUT_MS);
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(jsonBody.getBytes(StandardCharsets.UTF_8));
            }

            int status = conn.getResponseCode();
            if (status == 404) {
                LOG.fine("BeaconDB 404: " + jsonBody);
                return null;
            }
            if (status != 200) {
                LOG.warning("BeaconDB unexpected HTTP " + status);
                return null;
            }

            StringBuilder sb = new StringBuilder();
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(conn.getInputStream(),
                            StandardCharsets.UTF_8))) {
                String l;
                while ((l = br.readLine()) != null) sb.append(l);
            }
            return parseResponse(sb.toString(), ci);

        } catch (IOException ex) {
            LOG.log(Level.FINE, "BeaconDB request failed", ex);
            return null;
        }
    }

    // =========================================================================
    // JSON helpers
    // =========================================================================

    private static String buildJson(String radioType, int mcc, int mnc,
                                    int lac, int cellId) {
        return "{\"cellTowers\":[{" +
               "\"radioType\":\""       + radioType + "\"," +
               "\"mobileCountryCode\":" + mcc       + "," +
               "\"mobileNetworkCode\":" + mnc       + "," +
               "\"locationAreaCode\":"  + lac       + "," +
               "\"cellId\":"            + cellId    +
               "}]}";
    }

    private static CellTower parseResponse(String json, UliDecoder.CellInfo ci) {
        if (json == null || json.isBlank()) return null;

        double lat      = jsonDouble(json, "lat");
        double lng      = jsonDouble(json, "lng");
        double accuracy = jsonDouble(json, "accuracy");

        if (Double.isNaN(lat) || Double.isNaN(lng)) return null;

        // accuracy > 50 000 m bedeutet: beaconDB kennt die Zelle nicht und
        // gibt einen generischen Länder-Schwerpunkt zurück (z.B. Berlin für DE).
        // Solche Antworten sind für forensische Zwecke wertlos → ablehnen.
        if (!Double.isNaN(accuracy) && accuracy > 50_000) {
            LOG.fine(String.format("BeaconDB: accuracy=%.0fm zu groß (Fallback-Antwort) → verworfen",
                    accuracy));
            return null;
        }

        int    mcc     = ci != null ? ci.mcc     : 0;
        int    mnc     = ci != null ? ci.mnc     : 0;
        int    lac     = ci != null ? ci.lac     : -1;
        int    cellId  = ci != null ? ci.cellId  : -1;
        int    eci     = ci != null ? ci.eci     : -1;
        String netType = ci != null ? ci.networkType : "?";
        int    range   = Double.isNaN(accuracy) ? -1 : (int) Math.round(accuracy);

        LOG.fine(String.format("BeaconDB hit: %.5f°N %.5f°E ±%dm", lat, lng, range));

        return new CellTower(
                netType + "/beacondb",
                mcc, mnc, lac, cellId, eci,
                lat, lng, range,
                MncLookup.getOperator(mcc, mnc),
                MncLookup.getCountry(mcc),
                -1,
                range > 0 && range <= 5000);
    }

    private static double jsonDouble(String json, String key) {
        int idx = json.indexOf("\"" + key + "\"");
        if (idx < 0) return Double.NaN;
        int colon = json.indexOf(':', idx);
        if (colon < 0) return Double.NaN;
        int start = colon + 1;
        while (start < json.length()
               && (json.charAt(start) == ' ' || json.charAt(start) == '"'))
            start++;
        int end = start;
        while (end < json.length()
               && "-0123456789.eE+".indexOf(json.charAt(end)) >= 0) end++;
        try { return Double.parseDouble(json.substring(start, end)); }
        catch (NumberFormatException e) { return Double.NaN; }
    }

    // =========================================================================
    // Key + radio type
    // =========================================================================

    private static String cacheKey(String radio, int mcc, int mnc,
                                   int lac, int cellId) {
        return radio + "_" + mcc + "_" + mnc + "_" + lac + "_" + cellId;
    }

    private static String toRadioType(String networkType) {
        if (networkType == null) return "lte";
        String t = networkType.toLowerCase();
        if (t.contains("nr")   || t.contains("5g"))            return "nr";
        if (t.contains("lte")  || t.contains("ecgi")
            || t.contains("tai"))            return "lte";
        if (t.contains("3g")   || t.contains("umts")
            || t.contains("rai"))            return "wcdma";
        return "gsm";
    }
}
