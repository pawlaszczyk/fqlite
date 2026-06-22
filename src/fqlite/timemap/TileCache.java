package fqlite.timemap;

import javafx.application.Platform;
import javafx.scene.image.Image;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Asynchronous OSM tile loader with a three-level cache:
 *
 * <ol>
 *   <li><b>RAM cache</b> – bounded LRU map of up to {@value #MAX_RAM_TILES} decoded
 *       {@link Image} objects for instant access without any I/O.</li>
 *   <li><b>MBTiles file</b> – optional offline tile database (SQLite container in
 *       the standard MBTiles 1.3 schema, e.g. produced by {@code download-tiles}).
 *       Set via {@link #setMbTilesPath(Path)}.  When a path is configured every
 *       tile lookup checks this file first; tiles found here are served without
 *       any network access, making fully offline operation possible.</li>
 *   <li><b>Disk cache</b> – raw PNG bytes stored under
 *       {@code ~/.fqlite/tilecache/zoom/x/y.png} with a configurable TTL
 *       (default {@value #CACHE_TTL_DAYS} days).  A disk hit avoids the network
 *       entirely and is typically 10–50 × faster than a fresh download.</li>
 * </ol>
 *
 * <p>Tiles that miss all three caches are fetched from the OSM tile servers on a
 * shared background thread pool and delivered to the caller via a
 * {@link BiConsumer} callback that is always invoked on the JavaFX Application
 * Thread.  If the application is offline and no MBTiles file is set, missing
 * tiles are shown as grey placeholders.</p>
 *
 * <h3>MBTiles tile coordinate convention</h3>
 * <p>MBTiles uses the TMS convention where the Y axis is flipped relative to
 * the OSM/XYZ slippy-map convention: {@code tms_y = (2^zoom − 1) − xyz_y}.
 * This class converts automatically so callers always use XYZ coordinates.</p>
 *
 * <p>A silent {@link #prefetch} method lets {@code MapView} warm the cache for
 * adjacent zoom levels so that the next zoom step is served from disk or RAM
 * instead of the network.</p>
 *
 * <p>Tile key format: {@code "zoom/x/y"} matching the OSM slippy-map scheme.</p>
 */
public class TileCache {

    private static final Logger LOG = Logger.getLogger(TileCache.class.getName());

    // -------------------------------------------------------------------------
    // Tuning constants
    // -------------------------------------------------------------------------

    /** Maximum number of decoded {@link Image} objects kept in RAM. */
    private static final int MAX_RAM_TILES = 512;

    /**
     * Disk-cached tiles older than this many days are re-downloaded.
     * OSM tiles are very stable; 30 days is a safe default.
     */
    private static final int CACHE_TTL_DAYS = 30;

    /** Root directory for the on-disk tile store. */
    private static final Path DISK_DIR =
            Path.of(System.getProperty("user.home"), ".fqlite", "tilecache");

    /**
     * OSM-compatible tile URL template.
     * Subdomains a/b/c are rotated for load balancing.
     */
    private static final String[] TILE_SERVERS = {
        "https://a.tile.openstreetmap.org/%d/%d/%d.png",
        "https://b.tile.openstreetmap.org/%d/%d/%d.png",
        "https://c.tile.openstreetmap.org/%d/%d/%d.png"
    };

    /** User-Agent required by OSM tile usage policy. */
    private static final String USER_AGENT = "FQLite GeoTimeAnalyzer/1.0 (JavaFX)";

    private static final int TILE_PX = 256;

    // -------------------------------------------------------------------------
    // Shared singleton
    // -------------------------------------------------------------------------

    private static final TileCache INSTANCE = new TileCache();
    public  static TileCache getInstance() { return INSTANCE; }

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    /** Background thread pool shared by both getTile() and prefetch(). */
    private final ExecutorService pool = Executors.newFixedThreadPool(4, r -> {
        Thread t = new Thread(r, "tile-loader");
        t.setDaemon(true);
        return t;
    });

    /**
     * RAM cache: access-ordered LRU map so the most recently viewed tiles
     * survive longer than tiles from zoom levels the user has left.
     * A {@code null} value means "download in progress" (deduplication
     * placeholder).
     */
    @SuppressWarnings("serial")
    private final Map<String, Image> ramCache =
            new LinkedHashMap<>(MAX_RAM_TILES, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Image> eldest) {
            return size() > MAX_RAM_TILES;
        }
    };

    /** Round-robin index for subdomain rotation across TILE_SERVERS. */
    private int serverIndex = 0;

    // -------------------------------------------------------------------------
    // MBTiles state
    // -------------------------------------------------------------------------

    /**
     * Path to an optional MBTiles file used as offline tile source.
     * {@code null} means no offline source is configured.
     */
    private volatile Path   mbTilesPath = null;

    /**
     * Cached JDBC connection to the MBTiles SQLite database.
     * Opened lazily on the first lookup; closed and nulled when the path
     * is changed or cleared.  Access is guarded by {@code mbLock}.
     */
    private Connection mbConn = null;

    /** Guards {@link #mbConn} and {@link #mbTilesPath}. */
    private final Object mbLock = new Object();

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Returns the tile image immediately if it resides in the RAM cache;
     * otherwise schedules an async lookup (disk → network) and invokes
     * {@code onLoaded} on the FX thread once the image is ready.
     *
     * @param zoom     OSM zoom level
     * @param tileX    tile column
     * @param tileY    tile row
     * @param onLoaded callback(key, image) – always called on the FX thread
     * @return         cached image or {@code null} if not yet available
     */
    public synchronized Image getTile(int zoom, int tileX, int tileY,
                                      BiConsumer<String, Image> onLoaded) {
        String key = cacheKey(zoom, tileX, tileY);

        // 1. RAM hit – return immediately, no I/O
        Image cached = ramCache.get(key);
        if (cached != null) return cached;

        // Already being fetched (null placeholder) → nothing to do
        if (ramCache.containsKey(key)) return null;

        // 2. Schedule MBTiles / disk / network fetch
        ramCache.put(key, null);
        String url = nextTileUrl(zoom, tileX, tileY);

        pool.submit(() -> {
            // 2a. MBTiles hit (offline source, highest priority after RAM)
            Image img = loadFromMbTiles(zoom, tileX, tileY);

            // 2b. Disk cache hit
            if (img == null) img = loadFromDisk(zoom, tileX, tileY);

            // 2c. Network download (also persists to disk cache)
            if (img == null) img = downloadAndCache(url, zoom, tileX, tileY);

            if (img != null) {
                final Image fi = img;
                synchronized (TileCache.this) { ramCache.put(key, fi); }
                Platform.runLater(() -> onLoaded.accept(key, fi));
            }
        });
        return null;
    }

    /**
     * Silently warms the cache for a tile that the user is likely to need
     * soon (e.g. an adjacent zoom level).  No callback is fired; if the tile
     * is already cached or being fetched the call is a no-op.
     *
     * @param zoom  OSM zoom level
     * @param tileX tile column
     * @param tileY tile row
     */
    public void prefetch(int zoom, int tileX, int tileY) {
        String key = cacheKey(zoom, tileX, tileY);
        synchronized (this) {
            if (ramCache.containsKey(key)) return;
            ramCache.put(key, null);
        }
        String url = nextTileUrl(zoom, tileX, tileY);
        pool.submit(() -> {
            Image img = loadFromMbTiles(zoom, tileX, tileY);
            if (img == null) img = loadFromDisk(zoom, tileX, tileY);
            if (img == null) img = downloadAndCache(url, zoom, tileX, tileY);
            if (img != null) {
                final Image fi = img;
                synchronized (TileCache.this) { ramCache.put(key, fi); }
            }
        });
    }

    /** Returns the pixel size of one tile (always 256). */
    public static int tilePx() { return TILE_PX; }

    // -------------------------------------------------------------------------
    // MBTiles configuration
    // -------------------------------------------------------------------------

    /**
     * Configures an MBTiles file as the offline tile source.
     *
     * <p>After this call every tile lookup checks the SQLite database before
     * falling back to the disk cache and the network.  Pass {@code null} to
     * disable the offline source and revert to online-only operation.</p>
     *
     * <p>The method closes any previously open database connection and clears
     * the RAM cache so stale tiles are not served from the old source.</p>
     *
     * @param path absolute path to a {@code .mbtiles} file, or {@code null}
     */
    public void setMbTilesPath(Path path) {
        synchronized (mbLock) {
            closeMbConnection();
            mbTilesPath = path;
        }
        // Clear RAM cache: tiles already decoded may have come from the
        // previous source and must not outlive the configuration change.
        synchronized (this) { ramCache.clear(); }
        LOG.info("MBTiles source " + (path != null ? "set: " + path : "cleared"));
    }

    /**
     * Returns {@code true} if an MBTiles file is currently configured and
     * can be opened successfully.  Useful for updating a status label in the UI.
     */
    public boolean isMbTilesLoaded() {
        synchronized (mbLock) {
            return mbTilesPath != null && Files.exists(mbTilesPath);
        }
    }

    /** Returns the file name of the active MBTiles file, or {@code null}. */
    public String getMbTilesName() {
        Path p = mbTilesPath;
        return p == null ? null : p.getFileName().toString();
    }

    /** Returns the full path of the active MBTiles file, or {@code null}. */
    public Path getMbTilesPath() { return mbTilesPath; }

    // -------------------------------------------------------------------------
    // Cache key and URL helpers
    // -------------------------------------------------------------------------

    private static String cacheKey(int zoom, int tileX, int tileY) {
        return zoom + "/" + tileX + "/" + tileY;
    }

    private synchronized String nextTileUrl(int zoom, int tileX, int tileY) {
        String url = String.format(
                TILE_SERVERS[serverIndex % TILE_SERVERS.length],
                zoom, tileX, tileY);
        serverIndex++;
        return url;
    }

    // -------------------------------------------------------------------------
    // Disk cache
    // -------------------------------------------------------------------------

    /**
     * Returns the file path for a tile inside {@link #DISK_DIR}.
     * Directory structure: {@code <DISK_DIR>/<zoom>/<tileX>/<tileY>.png}
     */
    private Path tileFile(int zoom, int tileX, int tileY) {
        return DISK_DIR.resolve(zoom + "/" + tileX + "/" + tileY + ".png");
    }

    /**
     * Loads a tile from disk if it exists and has not expired.
     * Expired files are deleted automatically.
     *
     * @return decoded {@link Image} or {@code null} on miss / expiry / error
     */
    private Image loadFromDisk(int zoom, int tileX, int tileY) {
        Path f = tileFile(zoom, tileX, tileY);
        if (!Files.exists(f)) return null;
        try {
            FileTime modified = Files.getLastModifiedTime(f);
            Instant expiry    = modified.toInstant().plus(CACHE_TTL_DAYS, ChronoUnit.DAYS);
            if (Instant.now().isAfter(expiry)) {
                Files.deleteIfExists(f);
                LOG.fine("Tile expired, deleted: " + f);
                return null;
            }
            try (InputStream is = Files.newInputStream(f)) {
                Image img = new Image(is);
                if (!img.isError()) {
                    LOG.fine("Disk hit: " + f);
                    return img;
                }
            }
        } catch (IOException ex) {
            LOG.log(Level.FINE, "Disk read error: " + f, ex);
        }
        return null;
    }

    /**
     * Downloads a tile from {@code urlStr}, writes the raw bytes to disk for
     * future cache hits, and returns the decoded {@link Image}.
     *
     * @return decoded image or {@code null} on network / decode error
     */
    private Image downloadAndCache(String urlStr, int zoom, int tileX, int tileY) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
            conn.setRequestProperty("User-Agent", USER_AGENT);
            conn.setConnectTimeout(8_000);
            conn.setReadTimeout(10_000);
            conn.connect();
            if (conn.getResponseCode() != 200) return null;

            byte[] bytes;
            try (InputStream is = conn.getInputStream()) {
                bytes = is.readAllBytes();
            }

            // Persist to disk
            Path f = tileFile(zoom, tileX, tileY);
            try {
                Files.createDirectories(f.getParent());
                Files.write(f, bytes);
                LOG.fine("Cached to disk: " + f);
            } catch (IOException ioEx) {
                // Non-fatal: disk write failure must not prevent tile display
                LOG.log(Level.WARNING, "Failed to write tile to disk: " + f, ioEx);
            }

            Image img = new Image(new ByteArrayInputStream(bytes));
            return img.isError() ? null : img;

        } catch (Exception ex) {
            LOG.log(Level.FINE, "Tile download failed: " + urlStr, ex);
            return null;
        }
    }

    // -------------------------------------------------------------------------
    // MBTiles
    // -------------------------------------------------------------------------

    /**
     * Looks up a tile in the MBTiles SQLite database.
     *
     * <p>MBTiles stores tiles in a table {@code tiles (zoom_level, tile_column,
     * tile_row, tile_data)} where {@code tile_row} uses the TMS convention
     * (Y-axis flipped).  This method converts automatically.</p>
     *
     * @return decoded {@link Image} or {@code null} on miss / error / no source
     */
    private Image loadFromMbTiles(int zoom, int tileX, int tileY) {
        synchronized (mbLock) {
            if (mbTilesPath == null) return null;
            try {
                Connection conn = getMbConnection();
                if (conn == null) return null;

                // MBTiles TMS Y-flip: tms_y = (2^zoom - 1) - xyz_y
                int tmsY = (1 << zoom) - 1 - tileY;

                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT tile_data FROM tiles " +
                        "WHERE zoom_level=? AND tile_column=? AND tile_row=?")) {
                    ps.setInt(1, zoom);
                    ps.setInt(2, tileX);
                    ps.setInt(3, tmsY);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) return null;
                        byte[] bytes = rs.getBytes(1);
                        if (bytes == null || bytes.length == 0) return null;
                        Image img = new Image(new ByteArrayInputStream(bytes));
                        if (!img.isError()) {
                            LOG.fine("MBTiles hit: " + zoom + "/" + tileX + "/" + tileY);
                            return img;
                        }
                    }
                }
            } catch (SQLException ex) {
                LOG.log(Level.WARNING, "MBTiles query error", ex);
            }
        }
        return null;
    }

    /**
     * Returns the open JDBC connection, opening it lazily if necessary.
     * Must be called with {@link #mbLock} held.
     */
    private Connection getMbConnection() {
        if (mbConn != null) {
            try {
                if (!mbConn.isClosed()) return mbConn;
            } catch (SQLException ignored) {}
            mbConn = null;
        }
        if (mbTilesPath == null || !Files.exists(mbTilesPath)) return null;
        try {
            // SQLite JDBC URL; read-only mode avoids accidental writes
            String jdbcUrl = "jdbc:sqlite:" + mbTilesPath.toAbsolutePath();
            mbConn = DriverManager.getConnection(jdbcUrl);
            // Read-only pragma for safety
            try (Statement st = mbConn.createStatement()) {
                st.execute("PRAGMA query_only = ON");
            }
            LOG.info("MBTiles database opened: " + mbTilesPath);
            return mbConn;
        } catch (SQLException ex) {
            LOG.log(Level.WARNING, "Cannot open MBTiles: " + mbTilesPath, ex);
            return null;
        }
    }

    /** Closes the JDBC connection if open. Must be called with {@link #mbLock} held. */
    private void closeMbConnection() {
        if (mbConn != null) {
            try { mbConn.close(); } catch (SQLException ignored) {}
            mbConn = null;
        }
    }
}
