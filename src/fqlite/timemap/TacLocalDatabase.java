package fqlite.timemap;

import java.io.BufferedReader;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

/**
 * Resolves a phone's TAC (Type Allocation Code — the first 8 digits of an
 * IMEI) to brand/model from a locally stored, tab-separated reference file
 * ({@code Brand\tTAC\tSPECS}), instead of querying the (unverified, see
 * {@link TacLookupService}) HiCellTek online API.
 *
 * <p>Mirrors {@link CellTowerResolver}'s "local file first" design: a large
 * (hundreds of thousands of rows) external data file the analyst supplies
 * themselves and configures via Settings → Location, loaded once into an
 * in-memory index, with no network access required.</p>
 *
 * <h3>File format</h3>
 * <pre>
 * Brand&lt;TAB&gt;TAC&lt;TAB&gt;SPECS
 * XIAOMI	86302407	XIAOMI REDMI NOTE 14S, Xiaomi 2502FRA65G, Global Model, 2025
 * </pre>
 * The header row (starting with {@code "Brand"}) is skipped. Rows whose TAC
 * is fewer than 6 or more than 8 digits are skipped as malformed; 6- or
 * 7-digit TACs are left-padded with zeros to 8 digits (some export tools
 * strip a TAC's leading zero when treating it as a number). {@code .gz}
 * files are supported transparently, same as {@link CellTowerResolver}'s CSV.
 */
public final class TacLocalDatabase {

    private static final Logger LOG = Logger.getLogger(TacLocalDatabase.class.getName());

    // ── Singleton ─────────────────────────────────────────────────────────────
    private static final TacLocalDatabase INSTANCE = new TacLocalDatabase();
    public  static TacLocalDatabase getInstance() { return INSTANCE; }

    private TacLocalDatabase() {}

    /** One row of the reference file: manufacturer + free-text model/specs. */
    public static final class Entry {
        public final String brand;
        public final String specs;
        Entry(String brand, String specs) { this.brand = brand; this.specs = specs; }
    }

    private final Map<String, Entry> index = new ConcurrentHashMap<>(280_000);

    private final Object  lock     = new Object();
    private volatile Path filePath = null;
    private volatile boolean loaded  = false;
    private volatile boolean loading = false;

    private final ExecutorService pool = Executors.newFixedThreadPool(1, r -> {
        Thread t = new Thread(r, "tacdb-loader");
        t.setDaemon(true);
        return t;
    });

    // =========================================================================
    // Configuration
    // =========================================================================

    public void setFilePath(Path path) {
        synchronized (lock) {
            filePath = path;
            index.clear();
            loaded  = false;
            loading = false;
        }
        LOG.info("TacLocalDatabase file: " + (path != null ? path : "cleared"));
    }

    public Path    getFilePath()      { return filePath; }
    public boolean isLoaded()         { return loaded; }
    public boolean isConfigured()     { return filePath != null && Files.exists(filePath); }
    public int     getEntryCount()    { return index.size(); }

    /** Starts background pre-load so the first {@link #lookup} call does not block. */
    public void loadAsync() {
        synchronized (lock) {
            if (loaded || loading || filePath == null) return;
            loading = true;
        }
        pool.submit(this::loadFile);
    }

    /** Clears the in-memory index (does not delete the file). */
    public void clearCache() {
        synchronized (lock) {
            index.clear();
            loaded  = false;
            loading = false;
        }
    }

    // =========================================================================
    // Lookup
    // =========================================================================

    /**
     * Looks up device info for the given IMEI or bare TAC. Only the first 8
     * digits (the TAC) are used — see {@link TacLookupService#lookup} for
     * the same convention on the online path.
     *
     * @param imeiOrTac a full IMEI or just its 8-digit TAC prefix; any
     *                  non-digit characters are stripped first
     * @return the matching {@link Entry}, or {@code null} if not configured,
     *         not yet loaded, or the TAC has no entry in the file
     */
    public Entry lookup(String imeiOrTac) {
        if (filePath == null) return null;
        ensureLoaded();
        String digits = imeiOrTac == null ? "" : imeiOrTac.replaceAll("[^0-9]", "");
        if (digits.length() < 8) return null;
        return index.get(digits.substring(0, 8));
    }

    private void ensureLoaded() {
        if (loaded) return;
        synchronized (lock) {
            if (loaded)   return;
            if (!loading) loading = true;
            else { waitForLoad(); return; }
        }
        loadFile();
    }

    private void waitForLoad() {
        while (!loaded) {
            try { Thread.sleep(30); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
    }

    private void loadFile() {
        Path p;
        synchronized (lock) { p = filePath; }
        if (p == null || !Files.exists(p)) {
            LOG.warning("TacLocalDatabase: file not found: " + p);
            synchronized (lock) { loaded = true; loading = false; }
            return;
        }

        long t0 = System.currentTimeMillis();
        int  rows = 0, skipped = 0;

        try (BufferedReader br = openReader(p)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty() || line.startsWith("Brand")) continue;
                String[] f = line.split("\t", 3);
                if (f.length < 3) { skipped++; continue; }

                String brand = f[0].trim();
                String tacRaw = f[1].trim();
                String specs = f[2].trim();

                if (!tacRaw.matches("\\d{6,8}")) { skipped++; continue; }
                String tac = tacRaw.length() == 8 ? tacRaw
                        : "0".repeat(8 - tacRaw.length()) + tacRaw;

                // First entry for a given TAC wins; the file has a handful of
                // exact duplicate rows but no conflicting brand/specs pairs
                // worth merging.
                index.putIfAbsent(tac, new Entry(brand, specs));
                rows++;
            }
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "TacLocalDatabase load error: " + p, ex);
        }

        synchronized (lock) { loaded = true; loading = false; }
        LOG.info(String.format(
                "TacLocalDatabase: %,d TACs (%,d skipped) in %,d ms",
                rows, skipped, System.currentTimeMillis() - t0));
    }

    private static BufferedReader openReader(Path p) throws IOException {
        InputStream is = new BufferedInputStream(Files.newInputStream(p), 512 * 1024);
        if (p.toString().endsWith(".gz")) is = new GZIPInputStream(is);
        return new BufferedReader(new InputStreamReader(is), 256 * 1024);
    }
}
