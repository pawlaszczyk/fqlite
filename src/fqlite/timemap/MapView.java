package fqlite.timemap;

import fqlite.timemap.DataAnalyzer.DataPoint;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Cursor;
import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.Tooltip;
import javafx.scene.image.Image;
import javafx.scene.input.MouseEvent;
import javafx.scene.input.ScrollEvent;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Interactive slippy-map rendered entirely on a JavaFX {@link Canvas}.
 *
 * <p>OSM tiles are fetched asynchronously by {@link TileCache} and cached in
 * memory. No WebView, no browser engine and no third-party library are needed
 * at runtime beyond an active internet connection.</p>
 *
 * <h3>Controls</h3>
 * <ul>
 *   <li><b>Scroll wheel / +− buttons</b> — zoom in/out</li>
 *   <li><b>Drag</b> — pan the map</li>
 *   <li><b>Click on a marker</b> — fires the selection listener</li>
 * </ul>
 *
 * <h3>Tile bug fix</h3>
 * Each zoom operation increments a {@code renderGeneration} counter. Tile
 * callbacks that were launched for an older zoom level carry a stale generation
 * number and are silently dropped, preventing ghost tiles and double-renders
 * after rapid zooming.
 */
public class MapView extends BorderPane {

    // -------------------------------------------------------------------------
    // Web-Mercator projection helpers
    // -------------------------------------------------------------------------

    private static double lonToTileX(double lon, int z) {
        return Math.pow(2, z) * (lon + 180.0) / 360.0;
    }

    private static double latToTileY(double lat, int z) {
        double r = Math.toRadians(lat);
        return Math.pow(2, z) * (1 - Math.log(Math.tan(r) + 1.0 / Math.cos(r)) / Math.PI) / 2;
    }

    private static double tileXToLon(double tx, int z) {
        return tx / Math.pow(2, z) * 360.0 - 180.0;
    }

    private static double tileYToLat(double ty, int z) {
        double n = Math.PI - 2 * Math.PI * ty / Math.pow(2, z);
        return Math.toDegrees(Math.atan(Math.sinh(n)));
    }

    // -------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------

    private static final int    MIN_ZOOM = 1;
    private static final int    MAX_ZOOM = 18;
    private static final double MARKER_R = 8.0;
    private static final double HIT_R    = 13.0;

    // -------------------------------------------------------------------------
    // Viewport state
    // -------------------------------------------------------------------------

    private int    zoom      = 4;
    private double centerLat = 51.0;
    private double centerLon = 10.0;

    /**
     * Incremented on every zoom change. Tile-load callbacks compare against
     * this value and discard results that belong to a superseded zoom level.
     */
    private final AtomicInteger renderGeneration = new AtomicInteger(0);

    // -------------------------------------------------------------------------
    // Drag state
    // -------------------------------------------------------------------------

    private double  dragStartX, dragStartY;
    private double  dragStartLat, dragStartLon;
    private boolean wasDragged;

    // -------------------------------------------------------------------------
    // Data and selection
    // -------------------------------------------------------------------------

    private List<DataPoint>     geoPoints     = new ArrayList<>();
    private double[]            markerScreenX;
    private double[]            markerScreenY;
    private int                 hoveredMarker = -1;
    private Consumer<DataPoint> selectionListener;

    /**
     * The point most recently selected via {@link #focusPoint(DataPoint)}.
     * It is rendered with an extra highlight ring so it stands out from
     * regular markers and clusters.
     */
    private DataPoint focusedPoint = null;

    /**
     * Pixel radius within which two markers are merged into one cluster dot.
     * Increase to merge more aggressively; decrease for finer separation.
     */
    private static final double CLUSTER_RADIUS_PX = 18.0;

    /**
     * One rendered dot on the map: either a single {@link DataPoint} or a
     * cluster of several points that overlap at the current zoom level.
     */
    private static final class MarkerCluster {
        final List<DataPoint> members = new ArrayList<>();
        double screenX, screenY;

        boolean isSingle() { return members.size() == 1; }
        DataPoint first()  { return members.get(0); }
    }

    /** Built fresh on every {@link #render()} call from the current screen positions. */
    private List<MarkerCluster> clusters = List.of();

    // -------------------------------------------------------------------------
    // UI widgets
    // -------------------------------------------------------------------------

    private Theme       theme = Theme.DARK;
    private final Canvas    canvas;
    private final StackPane canvasWrapper;
    private final Label     statusLabel;
    private final Label     titleLabel;
    private final HBox      header;
    private final Tooltip   tooltip    = new Tooltip();

    // Zoom control overlay (placed in the bottom-left corner via StackPane)
    private final Button btnZoomIn;
    private final Button btnZoomOut;
    private final Label  zoomLabel;
    private final VBox   zoomControl;

    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    public MapView() {

        // ── Header ────────────────────────────────────────────────────────────
        titleLabel = new Label("MAP");
        titleLabel.setFont(Font.font("Monospace", FontWeight.BOLD, 13));
        titleLabel.setPadding(new Insets(12, 16, 8, 16));

        Label hint = new Label("Scroll \u2014 zoom  \u00b7  Drag \u2014 pan  \u00b7  Click marker \u2014 details");
        hint.setFont(Font.font("Monospace", 11));

        header = new HBox(16, titleLabel, hint);
        header.setAlignment(Pos.CENTER_LEFT);
        setTop(header);

        // ── Canvas ────────────────────────────────────────────────────────────
        canvas = new Canvas();
        canvas.widthProperty().addListener(e -> render());
        canvas.heightProperty().addListener(e -> render());

        canvas.setCursor(Cursor.OPEN_HAND);
        canvas.setOnScroll(this::onScroll);
        canvas.setOnMousePressed(this::onMousePressed);
        canvas.setOnMouseDragged(this::onMouseDragged);
        canvas.setOnMouseReleased(this::onMouseReleased);
        canvas.setOnMouseMoved(this::onMouseMoved);
        canvas.setOnMouseClicked(this::onMouseClicked);

        // ── Zoom control overlay ──────────────────────────────────────────────
        btnZoomIn  = makeZoomButton("+");
        btnZoomOut = makeZoomButton("\u2212"); // minus sign

        zoomLabel = new Label("Z" + zoom);
        zoomLabel.setFont(Font.font("Monospace", FontWeight.BOLD, 11));
        zoomLabel.setAlignment(Pos.CENTER);
        zoomLabel.setPrefWidth(36);

        zoomControl = new VBox(0, btnZoomIn, zoomLabel, btnZoomOut);
        zoomControl.setAlignment(Pos.CENTER);
        zoomControl.setMaxSize(VBox.USE_PREF_SIZE, VBox.USE_PREF_SIZE);

        btnZoomIn.setOnAction(e  -> changeZoom(+1, canvas.getWidth() / 2, canvas.getHeight() / 2));
        btnZoomOut.setOnAction(e -> changeZoom(-1, canvas.getWidth() / 2, canvas.getHeight() / 2));

        // StackPane: canvas fills all space; zoom control sits in bottom-left
        canvasWrapper = new StackPane(canvas, zoomControl);
        StackPane.setAlignment(zoomControl, Pos.BOTTOM_LEFT);
        StackPane.setMargin(zoomControl, new Insets(0, 0, 24, 10));

        canvas.widthProperty().bind(canvasWrapper.widthProperty());
        canvas.heightProperty().bind(canvasWrapper.heightProperty());

        // Allow the SplitPane to shrink the map below the canvas's implicit
        // preferred size. Without this, the bound Canvas reports its last
        // rendered size as minWidth/minHeight, which blocks divider dragging.
        canvasWrapper.setMinSize(0, 0);
        canvasWrapper.setPrefSize(Region.USE_COMPUTED_SIZE, Region.USE_COMPUTED_SIZE);
        setMinSize(0, 0);

        setCenter(canvasWrapper);

        // ── Status bar ────────────────────────────────────────────────────────
        statusLabel = new Label("No geo data loaded.");
        statusLabel.setFont(Font.font("Monospace", 11));
        statusLabel.setPadding(new Insets(6, 16, 6, 16));
        setBottom(statusLabel);

        applyTheme(Theme.DARK);
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /** Loads data and re-centres the view to fit all geo-tagged points. */
    public void setData(List<DataPoint> allPoints) {
        geoPoints = allPoints.stream()
                .filter(p -> p.getCoordinate() != null)
                .toList();

        long withTs = geoPoints.stream().filter(p -> p.getTimestamp() != null).count();
        statusLabel.setText(String.format(
                "%d geo points  |  %d with timestamp  |  %d total",
                geoPoints.size(), withTs, allPoints.size()));

        focusedPoint = null;
        if (!geoPoints.isEmpty()) fitView();
        renderGeneration.incrementAndGet();
        render();
    }

    /** Registers a callback invoked when the user clicks a marker. */
    public void setSelectionListener(Consumer<DataPoint> listener) {
        this.selectionListener = listener;
    }

    /**
     * Centres the map on {@code dp}'s coordinate, zooms to street level if
     * needed, marks the point as focused (highlighted with an extra ring) and
     * repaints.  Called when the user clicks a timeline dot.
     *
     * @param dp the data point to focus; must have a non-null coordinate
     */
    public void focusPoint(DataPoint dp) {
        if (dp == null || dp.getCoordinate() == null) return;
        focusedPoint = dp;
        centerLat = dp.getCoordinate().getLatitude();
        centerLon = dp.getCoordinate().getLongitude();
        // Zoom to a comfortable street-level view if we are currently too far out.
        if (zoom < 12) {
            zoom = 13;
            renderGeneration.incrementAndGet();
            updateZoomLabel();
        }
        render();
    }

    /** Switches the colour palette and repaints. */
    public void applyTheme(Theme t) {
        this.theme = t;
        setStyle(t.bgStyle());
        header.setStyle(t.bgStyle() + t.borderBottomStyle());
        titleLabel.setTextFill(t.accent2);
        statusLabel.setStyle(t.bgStyle() + t.borderTopStyle());
        statusLabel.setTextFill(t.label);

        styleZoomControl(t);

        tooltip.setStyle(
                "-fx-font-family: monospace; -fx-font-size: 11px;" +
                "-fx-background-color: " + Theme.hex(t.bgAlt) + ";" +
                "-fx-text-fill: "        + Theme.hex(t.labelStrong) + ";" +
                "-fx-border-color: "     + Theme.hex(t.border) + "; -fx-border-width: 1;");
        render();
    }

    // -------------------------------------------------------------------------
    // Fit view to data bounds
    // -------------------------------------------------------------------------

    private void fitView() {
        double minLat = geoPoints.stream().mapToDouble(p -> p.getCoordinate().getLatitude()).min().orElse(51);
        double maxLat = geoPoints.stream().mapToDouble(p -> p.getCoordinate().getLatitude()).max().orElse(51);
        double minLon = geoPoints.stream().mapToDouble(p -> p.getCoordinate().getLongitude()).min().orElse(10);
        double maxLon = geoPoints.stream().mapToDouble(p -> p.getCoordinate().getLongitude()).max().orElse(10);

        centerLat = (minLat + maxLat) / 2.0;
        centerLon = (minLon + maxLon) / 2.0;

        double span = Math.max(maxLat - minLat, maxLon - minLon);
        if      (span < 0.005) zoom = 15;
        else if (span < 0.05)  zoom = 13;
        else if (span < 0.5)   zoom = 10;
        else if (span < 5)     zoom = 8;
        else if (span < 20)    zoom = 6;
        else if (span < 60)    zoom = 5;
        else                   zoom = 3;

        updateZoomLabel();
    }

    // -------------------------------------------------------------------------
    // Zoom helper (shared by scroll wheel and +/− buttons)
    // -------------------------------------------------------------------------

    /**
     * Changes the zoom level by {@code delta} steps while keeping the
     * geographic point at pixel ({@code pivotX}, {@code pivotY}) fixed.
     */
    private void changeZoom(int delta, double pivotX, double pivotY) {
        int newZoom = Math.max(MIN_ZOOM, Math.min(MAX_ZOOM, zoom + delta));
        if (newZoom == zoom) return;

        int    tPx   = TileCache.tilePx();
        double cTX   = lonToTileX(centerLon, zoom);
        double cTY   = latToTileY(centerLat, zoom);
        double W     = canvas.getWidth();
        double H     = canvas.getHeight();
        double tlTX  = cTX - W / 2.0 / tPx;
        double tlTY  = cTY - H / 2.0 / tPx;

        // Geographic coordinates of the pivot pixel at the OLD zoom
        double pivotLon = tileXToLon(tlTX + pivotX / tPx, zoom);
        double pivotLat = tileYToLat(tlTY + pivotY / tPx, zoom);

        zoom = newZoom;

        // Re-project pivot into the NEW zoom and shift centre accordingly
        double newPivotTX = lonToTileX(pivotLon, zoom);
        double newPivotTY = latToTileY(pivotLat, zoom);
        double newCTX     = newPivotTX - (pivotX / tPx - W / 2.0 / tPx);
        double newCTY     = newPivotTY - (pivotY / tPx - H / 2.0 / tPx);

        centerLon = tileXToLon(newCTX, zoom);
        centerLat = Math.max(-85.05, Math.min(85.05, tileYToLat(newCTY, zoom)));

        // Invalidate stale tile callbacks from the previous zoom level.
        renderGeneration.incrementAndGet();
        updateZoomLabel();
        render();
    }

    // -------------------------------------------------------------------------
    // Rendering
    // -------------------------------------------------------------------------

    private void render() {
        GraphicsContext gc = canvas.getGraphicsContext2D();
        double W  = canvas.getWidth();
        double H  = canvas.getHeight();
        if (W <= 0 || H <= 0) return;

        gc.setFill(theme.bg);
        gc.fillRect(0, 0, W, H);

        int    tPx  = TileCache.tilePx();
        double cTX  = lonToTileX(centerLon, zoom);
        double cTY  = latToTileY(centerLat, zoom);
        double tlTX = cTX - W / 2.0 / tPx;
        double tlTY = cTY - H / 2.0 / tPx;

        drawTiles(gc, W, H, tlTX, tlTY, tPx);
        drawAttribution(gc, W, H);
        drawMarkers(gc, W, H, tlTX, tlTY, tPx);
    }

    private void drawTiles(GraphicsContext gc, double W, double H,
                           double tlTX, double tlTY, int tPx) {
        int maxIdx = (1 << zoom) - 1;
        int x0 = (int) Math.floor(tlTX);
        int y0 = (int) Math.floor(tlTY);
        int x1 = (int) Math.ceil(tlTX + W / tPx);
        int y1 = (int) Math.ceil(tlTY + H / tPx);

        // Snapshot the current generation so the lambda captures a fixed value.
        int myGeneration = renderGeneration.get();

        for (int ty = y0; ty <= y1; ty++) {
            for (int tx = x0; tx <= x1; tx++) {
                double px = (tx - tlTX) * tPx;
                double py = (ty - tlTY) * tPx;

                int wrappedTX = Math.floorMod(tx, maxIdx + 1);
                int clampedTY = Math.max(0, Math.min(maxIdx, ty));

                Image tile = TileCache.getInstance().getTile(
                        zoom, wrappedTX, clampedTY,
                        // Only re-render when the tile belongs to the current zoom.
                        (key, img) -> {
                            if (renderGeneration.get() == myGeneration) render();
                        });

                if (tile != null && !tile.isError()) {
                    gc.drawImage(tile, px, py, tPx, tPx);
                } else {
                    // Grey placeholder while the tile is loading
                    gc.setFill(theme.bgAlt);
                    gc.fillRect(px, py, tPx - 1, tPx - 1);
                    gc.setStroke(theme.border);
                    gc.setLineWidth(0.5);
                    gc.strokeRect(px, py, tPx - 1, tPx - 1);
                }
            }
        }
    }

    private void drawAttribution(GraphicsContext gc, double W, double H) {
        String text = "\u00a9 OpenStreetMap contributors";
        gc.setFont(Font.font("Monospace", 9));
        double tw = text.length() * 5.4;
        gc.setFill(Color.rgb(255, 255, 255, 0.72));
        gc.fillRect(W - tw - 8, H - 15, tw + 6, 14);
        gc.setFill(Color.rgb(0, 0, 0, 0.85));
        gc.fillText(text, W - tw - 5, H - 4);
    }

    private void drawMarkers(GraphicsContext gc, double W, double H,
                             double tlTX, double tlTY, int tPx) {
        if (geoPoints.isEmpty()) return;

        // ── 1. Compute raw screen positions ──────────────────────────────────
        markerScreenX = new double[geoPoints.size()];
        markerScreenY = new double[geoPoints.size()];
        for (int i = 0; i < geoPoints.size(); i++) {
            DataPoint dp = geoPoints.get(i);
            markerScreenX[i] = (lonToTileX(dp.getCoordinate().getLongitude(), zoom) - tlTX) * tPx;
            markerScreenY[i] = (latToTileY(dp.getCoordinate().getLatitude(),  zoom) - tlTY) * tPx;
        }

        // ── 2. Greedy clustering: merge points within CLUSTER_RADIUS_PX ──────
        boolean[] assigned = new boolean[geoPoints.size()];
        List<MarkerCluster> built = new ArrayList<>();

        for (int i = 0; i < geoPoints.size(); i++) {
            if (assigned[i]) continue;
            MarkerCluster c = new MarkerCluster();
            c.members.add(geoPoints.get(i));
            c.screenX = markerScreenX[i];
            c.screenY = markerScreenY[i];
            assigned[i] = true;

            for (int j = i + 1; j < geoPoints.size(); j++) {
                if (assigned[j]) continue;
                double dx = markerScreenX[j] - c.screenX;
                double dy = markerScreenY[j] - c.screenY;
                if (Math.sqrt(dx * dx + dy * dy) <= CLUSTER_RADIUS_PX) {
                    c.members.add(geoPoints.get(j));
                    // Update centroid of the cluster
                    c.screenX = (c.screenX * (c.members.size() - 1) + markerScreenX[j]) / c.members.size();
                    c.screenY = (c.screenY * (c.members.size() - 1) + markerScreenY[j]) / c.members.size();
                    assigned[j] = true;
                }
            }
            built.add(c);
        }
        clusters = built;

        // ── 3. Draw clusters ─────────────────────────────────────────────────
        gc.setFont(Font.font("Monospace", FontWeight.BOLD, 9));

        for (int ci = 0; ci < clusters.size(); ci++) {
            MarkerCluster c = clusters.get(ci);
            double sx = c.screenX;
            double sy = c.screenY;
            if (sx < -40 || sx > W + 40 || sy < -40 || sy > H + 40) continue;

            boolean hovered = (ci == hoveredMarker);
            int     count   = c.members.size();

            if (c.isSingle()) {
                // ── Single marker (unchanged appearance) ─────────────────────
                DataPoint dp = c.first();
                Color base   = dp.getTimestamp() != null ? theme.accent2 : theme.accent1;
                Color fill   = hovered ? theme.hover : base;
                double r     = hovered ? MARKER_R * 1.35 : MARKER_R;

                gc.setFill(Color.rgb(0, 0, 0, 0.28));
                gc.fillOval(sx - r + 2, sy - r + 2, r * 2, r * 2);

                if (hovered) {
                    gc.setFill(fill.deriveColor(0, 1, 1, 0.3));
                    gc.fillOval(sx - r * 1.8, sy - r * 1.8, r * 3.6, r * 3.6);
                }
                gc.setFill(fill);
                gc.fillOval(sx - r, sy - r, r * 2, r * 2);
                gc.setStroke(fill.brighter());
                gc.setLineWidth(1.5);
                gc.strokeOval(sx - r, sy - r, r * 2, r * 2);
                gc.setFill(Color.WHITE);
                gc.fillOval(sx - 2.5, sy - 2.5, 5, 5);

                // Extra highlight ring for the focused point (selected via timeline)
                if (c.first() == focusedPoint) {
                    gc.setStroke(theme.selected);
                    gc.setLineWidth(2.5);
                    gc.strokeOval(sx - r * 2.2, sy - r * 2.2, r * 4.4, r * 4.4);
                }

            } else {
                // ── Cluster bubble ────────────────────────────────────────────
                // Radius scales with log(count) so even large clusters stay legible.
                double r = MARKER_R * (1.0 + 0.45 * Math.log(count));
                Color fill = hovered ? theme.hover : theme.accent1;

                // Shadow
                gc.setFill(Color.rgb(0, 0, 0, 0.28));
                gc.fillOval(sx - r + 2, sy - r + 2, r * 2, r * 2);

                // Glow ring
                gc.setFill(fill.deriveColor(0, 1, 1, hovered ? 0.4 : 0.22));
                gc.fillOval(sx - r * 1.5, sy - r * 1.5, r * 3, r * 3);

                // Main circle
                gc.setFill(fill);
                gc.fillOval(sx - r, sy - r, r * 2, r * 2);
                gc.setStroke(fill.brighter());
                gc.setLineWidth(hovered ? 2.0 : 1.5);
                gc.strokeOval(sx - r, sy - r, r * 2, r * 2);

                // Count label
                String label = count > 999 ? "999+" : String.valueOf(count);
                gc.setFill(Color.WHITE);
                // Approximate centering (monospace: ~6px per char at size 9)
                double tw = label.length() * 5.5;
                gc.fillText(label, sx - tw / 2.0, sy + 3.5);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Scroll — zoom
    // -------------------------------------------------------------------------

    private void onScroll(ScrollEvent e) {
        changeZoom(e.getDeltaY() > 0 ? +1 : -1, e.getX(), e.getY());
    }

    // -------------------------------------------------------------------------
    // Drag — pan
    // -------------------------------------------------------------------------

    private void onMousePressed(MouseEvent e) {
        dragStartX   = e.getX();
        dragStartY   = e.getY();
        dragStartLat = centerLat;
        dragStartLon = centerLon;
        wasDragged   = false;
        canvas.setCursor(Cursor.CLOSED_HAND);
    }

    private void onMouseDragged(MouseEvent e) {
        double dx = e.getX() - dragStartX;
        double dy = e.getY() - dragStartY;
        if (!wasDragged && Math.abs(dx) + Math.abs(dy) < 4) return;
        wasDragged = true;

        int    tPx = TileCache.tilePx();
        double cTX = lonToTileX(dragStartLon, zoom) - dx / tPx;
        double cTY = latToTileY(dragStartLat, zoom) - dy / tPx;

        centerLon = tileXToLon(cTX, zoom);
        centerLat = Math.max(-85.05, Math.min(85.05, tileYToLat(cTY, zoom)));
        render();
    }

    private void onMouseReleased(MouseEvent e) {
        canvas.setCursor(Cursor.OPEN_HAND);
    }

    // -------------------------------------------------------------------------
    // Hover / click on markers
    // -------------------------------------------------------------------------

    private void onMouseMoved(MouseEvent e) {
        int hit = hitTest(e.getX(), e.getY());
        if (hit != hoveredMarker) {
            hoveredMarker = hit;
            canvas.setCursor(hit >= 0 ? Cursor.HAND : Cursor.OPEN_HAND);
            render();
        }
        if (hit >= 0) {
            tooltip.setText(buildClusterTooltip(clusters.get(hit)));
            Tooltip.install(canvas, tooltip);
        } else {
            Tooltip.uninstall(canvas, tooltip);
        }
    }

    private void onMouseClicked(MouseEvent e) {
        if (wasDragged) return;
        int hit = hitTest(e.getX(), e.getY());
        if (hit >= 0 && selectionListener != null) {
            MarkerCluster c = clusters.get(hit);
            // For a cluster, fire the event for the first (earliest) member.
            selectionListener.accept(c.first());
        }
    }

    private int hitTest(double mx, double my) {
        if (clusters.isEmpty()) return -1;
        for (int i = 0; i < clusters.size(); i++) {
            MarkerCluster c = clusters.get(i);
            // Hit radius grows with cluster size, matching the drawn bubble.
            double r = c.isSingle()
                    ? HIT_R
                    : HIT_R * (1.0 + 0.45 * Math.log(c.members.size()));
            double dx = mx - c.screenX;
            double dy = my - c.screenY;
            if (Math.sqrt(dx * dx + dy * dy) <= r) return i;
        }
        return -1;
    }

    // -------------------------------------------------------------------------
    // Zoom control helpers
    // -------------------------------------------------------------------------

    private Button makeZoomButton(String label) {
        Button b = new Button(label);
        b.setFont(Font.font("Monospace", FontWeight.BOLD, 14));
        b.setPrefSize(36, 36);
        b.setMinSize(36, 36);
        b.setMaxSize(36, 36);
        b.setCursor(Cursor.HAND);
        return b;
    }

    private void styleZoomControl(Theme t) {
        String bg     = Theme.hex(t.bgAlt);
        String fg     = Theme.hex(t.labelStrong);
        String border = Theme.hex(t.border);
        String btnCss =
                "-fx-background-color: " + bg + ";" +
                "-fx-text-fill: "        + fg + ";" +
                "-fx-border-color: "     + border + ";" +
                "-fx-border-width: 1px;" +
                "-fx-background-radius: 0;" +
                "-fx-border-radius: 0;" +
                "-fx-cursor: hand;";
        btnZoomIn.setStyle(btnCss);
        btnZoomOut.setStyle(btnCss);

        zoomLabel.setStyle(
                "-fx-background-color: " + bg + ";" +
                "-fx-text-fill: "        + fg + ";" +
                "-fx-border-color: "     + border + ";" +
                "-fx-border-width: 0 1 0 1;");
        zoomLabel.setTextFill(javafx.scene.paint.Color.web(fg));

        zoomControl.setStyle(
                "-fx-effect: dropshadow(gaussian, rgba(0,0,0,0.35), 6, 0, 1, 2);" +
                "-fx-background-radius: 4; -fx-border-radius: 4;");
    }

    private void updateZoomLabel() {
        zoomLabel.setText("Z" + zoom);
        btnZoomIn.setDisable(zoom >= MAX_ZOOM);
        btnZoomOut.setDisable(zoom <= MIN_ZOOM);
    }

    // -------------------------------------------------------------------------
    // Tooltip
    // -------------------------------------------------------------------------

    private String buildClusterTooltip(MarkerCluster c) {
        if (c.isSingle()) return buildTooltip(c.first());
        StringBuilder sb = new StringBuilder();
        sb.append(c.members.size()).append(" overlapping points");
        // Show time range if all members have timestamps
        long withTs = c.members.stream().filter(p -> p.getTimestamp() != null).count();
        if (withTs > 0) {
            sb.append("  (").append(withTs).append(" with timestamp)");
        }
        sb.append("\n");
        // List first 5 members
        int show = Math.min(5, c.members.size());
        for (int i = 0; i < show; i++) {
            DataPoint dp = c.members.get(i);
            sb.append("  \u2022 row ").append(dp.getRowIndex());
            if (dp.getTimestamp() != null)
                sb.append("  ").append(dp.getFormattedTimestamp()).append(" UTC");
            sb.append("\n");
        }
        if (c.members.size() > show)
            sb.append("  … and ").append(c.members.size() - show).append(" more\n");
        sb.append("Geo: ").append(c.first().getCoordinate());
        return sb.toString().trim();
    }

    private String buildTooltip(DataPoint dp) {
        StringBuilder sb = new StringBuilder();
        sb.append("Table: ").append(dp.getTableName())
                .append("  (row ").append(dp.getRowIndex()).append(")\n");
        if (dp.getTimestamp() != null)
            sb.append("Time:  ").append(dp.getFormattedTimestamp()).append(" UTC\n");
        sb.append("Geo:   ").append(dp.getCoordinate()).append("\n");
        List<String> cols = dp.getColumnNames();
        List<String> row  = dp.getRawRow();
        for (int i = 0; i < Math.min(cols.size(), row.size()); i++) {
            String v = row.get(i);
            if (v != null && !v.isBlank())
                sb.append(cols.get(i)).append(": ").append(v).append("\n");
        }
        return sb.toString().trim();
    }
}
