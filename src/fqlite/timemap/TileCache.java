package fqlite.timemap;

import javafx.application.Platform;
import javafx.scene.image.Image;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Asynchronous OSM tile loader with a bounded in-memory LRU cache.
 *
 * <p>Tiles are fetched on a shared background thread pool and delivered to the
 * caller via a {@link BiConsumer} callback that is always invoked on the
 * JavaFX Application Thread.</p>
 *
 * <p>Tile key format: {@code "zoom/x/y"} matching the OSM slippy-map scheme.</p>
 */
public class TileCache {

    private static final Logger LOG = Logger.getLogger(TileCache.class.getName());

    /** Maximum number of tiles held in RAM simultaneously. */
    private static final int MAX_TILES = 256;

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

    private final ExecutorService pool = Executors.newFixedThreadPool(4, r -> {
        Thread t = new Thread(r, "tile-loader");
        t.setDaemon(true);
        return t;
    });

    @SuppressWarnings("serial")
    private final Map<String, Image> cache = new LinkedHashMap<>(MAX_TILES, 0.75f, true) {
        @Override protected boolean removeEldestEntry(Map.Entry<String, Image> eldest) {
            return size() > MAX_TILES;
        }
    };

    private int serverIndex = 0;

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Returns the tile image immediately if it is cached; otherwise starts an
     * async fetch and invokes {@code onLoaded} on the FX thread when done.
     *
     * @param zoom     OSM zoom level
     * @param tileX    tile column
     * @param tileY    tile row
     * @param onLoaded callback(key, image) – called on FX thread after download
     * @return         cached image or {@code null} if not yet available
     */
    public synchronized Image getTile(int zoom, int tileX, int tileY,
                                      BiConsumer<String, Image> onLoaded) {
        String key = zoom + "/" + tileX + "/" + tileY;
        Image cached = cache.get(key);
        if (cached != null) return cached;

        // Kick off async download (avoid duplicate requests)
        cache.put(key, null); // placeholder to deduplicate
        String url = String.format(TILE_SERVERS[serverIndex % TILE_SERVERS.length],
                                   zoom, tileX, tileY);
        serverIndex++;

        pool.submit(() -> {
            Image img = download(url);
            if (img != null) {
                synchronized (TileCache.this) { cache.put(key, img); }
                Platform.runLater(() -> onLoaded.accept(key, img));
            }
        });
        return null;
    }

    /** Returns the pixel size of one tile (always 256). */
    public static int tilePx() { return TILE_PX; }

    // -------------------------------------------------------------------------
    // Download
    // -------------------------------------------------------------------------

    private Image download(String urlStr) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
            conn.setRequestProperty("User-Agent", USER_AGENT);
            conn.setConnectTimeout(8_000);
            conn.setReadTimeout(10_000);
            conn.connect();
            if (conn.getResponseCode() != 200) return null;
            try (InputStream is = conn.getInputStream()) {
                return new Image(is);
            }
        } catch (Exception ex) {
            LOG.log(Level.FINE, "Tile download failed: " + urlStr, ex);
            return null;
        }
    }
}
