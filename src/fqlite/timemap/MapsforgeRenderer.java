package fqlite.timemap;

import javafx.scene.image.Image;
import javafx.scene.image.PixelFormat;
import javafx.scene.image.WritableImage;
import org.mapsforge.core.graphics.GraphicFactory;
import org.mapsforge.core.graphics.TileBitmap;
import org.mapsforge.core.model.Tile;
import org.mapsforge.map.awt.graphics.AwtGraphicFactory;
import org.mapsforge.map.layer.labels.MapDataStoreLabelStore;
import org.mapsforge.map.layer.renderer.DatabaseRenderer;
import org.mapsforge.map.layer.renderer.RendererJob;
import org.mapsforge.map.model.DisplayModel;
import org.mapsforge.map.model.FixedTileSizeDisplayModel;
import org.mapsforge.map.reader.MapFile;
import org.mapsforge.map.reader.header.MapFileInfo;
import org.mapsforge.map.rendertheme.internal.MapsforgeThemes;
import org.mapsforge.map.rendertheme.rule.RenderThemeFuture;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Renders 256×256 map tiles offline from a Mapsforge {@code .map} file
 * (e.g. {@code germany.map} from download.mapsforge.org).
 *
 * <p>Used by {@link TileCache} as offline tile source.  Rendering is pure
 * Java2D (no OpenGL, no native libraries).  Labels are placed with a
 * {@link MapDataStoreLabelStore}, which looks at the neighbouring tiles as
 * well, so names that cross tile borders are drawn consistently on both
 * tiles without needing a shared bitmap cache.</p>
 *
 * <p>Thread safety: Mapsforge's {@link MapFile} synchronises its file reads,
 * and {@link DatabaseRenderer} is designed for multi-threaded rendering, so
 * {@link #render} may be called from the tile loader pool concurrently.</p>
 */
final class MapsforgeRenderer implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(MapsforgeRenderer.class.getName());

    private static final int TILE_PX = 256;

    /** Magic bytes at the start of every Mapsforge binary map file. */
    private static final String MAGIC = "mapsforge binary OSM";

    private final MapFile           mapFile;
    private final DisplayModel      displayModel;
    private final RenderThemeFuture themeFuture;
    private final DatabaseRenderer  renderer;

    /**
     * Opens the map file and starts loading the render theme in the background.
     *
     * @param path path to a Mapsforge {@code .map} file
     * @throws org.mapsforge.map.reader.header.MapFileException if the file is not a valid map file
     */
    MapsforgeRenderer(Path path) {
        GraphicFactory gf = AwtGraphicFactory.INSTANCE;

        // Prefer place names in the UI language if the map is multilingual
        this.mapFile      = new MapFile(path.toFile(), Locale.getDefault().getLanguage());
        this.displayModel = new FixedTileSizeDisplayModel(TILE_PX);
        this.themeFuture  = new RenderThemeFuture(gf, MapsforgeThemes.DEFAULT, displayModel);

        Thread t = new Thread(themeFuture, "mapsforge-theme");
        t.setDaemon(true);
        t.start();

        MapDataStoreLabelStore labels =
                new MapDataStoreLabelStore(mapFile, themeFuture, 1.0f, displayModel, gf);
        // tileCache = null: labels come from the label store, not from cached neighbours
        this.renderer = new DatabaseRenderer(mapFile, gf, null, labels, true, false, null);
    }

    /** {@code true} if the file starts with the Mapsforge magic bytes. */
    static boolean isMapsforgeFile(Path path) {
        if (path == null || !Files.isRegularFile(path)) return false;
        try (InputStream in = Files.newInputStream(path)) {
            byte[] head = in.readNBytes(MAGIC.length());
            return MAGIC.equals(new String(head, StandardCharsets.US_ASCII));
        } catch (IOException e) {
            return false;
        }
    }

    /** Header information (bounding box, map date, zoom range …). */
    MapFileInfo info() {
        return mapFile.getMapFileInfo();
    }

    /**
     * Renders one tile in XYZ (slippy map) coordinates.
     *
     * @return the rendered tile, or {@code null} if the tile lies completely
     *         outside the map area (so the caller can fall back to online tiles)
     *         or rendering failed
     */
    Image render(int zoom, int tileX, int tileY) {
        Tile tile = new Tile(tileX, tileY, (byte) zoom, TILE_PX);
        if (!mapFile.supportsTile(tile)) return null;

        RendererJob job = new RendererJob(tile, mapFile, themeFuture, displayModel, 1.0f, false, false);
        TileBitmap bmp = renderer.executeJob(job);
        if (bmp == null) return null;
        try {
            return toFxImage(AwtGraphicFactory.getBitmap(bmp));
        } catch (Exception ex) {
            LOG.log(Level.WARNING, "Mapsforge tile conversion failed: " + zoom + "/" + tileX + "/" + tileY, ex);
            return null;
        } finally {
            bmp.decrementRefCount();
        }
    }

    /** Copies a BufferedImage into a JavaFX image (safe off the FX thread). */
    private static Image toFxImage(BufferedImage bi) {
        int w = bi.getWidth(), h = bi.getHeight();
        int[] argb = bi.getRGB(0, 0, w, h, null, 0, w);
        WritableImage img = new WritableImage(w, h);
        img.getPixelWriter().setPixels(0, 0, w, h, PixelFormat.getIntArgbInstance(), argb, 0, w);
        return img;
    }

    @Override
    public void close() {
        try { mapFile.close(); } catch (Exception ignored) { }
        try { themeFuture.decrementRefCount(); } catch (Exception ignored) { }
    }
}
