package fqlite.timemap;

import fqlite.timemap.DataAnalyzer.DataPoint;
import fqlite.timemap.DataAnalyzer.ResponseRecordDataPoint;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Cursor;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.Tooltip;
import javafx.scene.image.Image;
import javafx.scene.input.MouseEvent;
import javafx.scene.input.ScrollEvent;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.util.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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

    // ── Cell-tower layer ──────────────────────────────────────────────────────

    /**
     * When {@code true} the map resolves {@link CellTower} positions for all
     * {@link ResponseRecordDataPoint}s that have a decoded ULI and renders them
     * as orange tower symbols alongside the regular data markers.
     */
    private boolean cellTowerLayerEnabled = false;

    /**
     * Cell towers currently visible, rebuilt by {@link #resolveCellTowers()}
     * after {@link #setData} or when the layer is toggled on.
     */
    private List<CellTower> cellTowers = new ArrayList<>();

    /** Screen positions of {@link #cellTowers}, parallel arrays. */
    private double[] towerScreenX = new double[0];
    private double[] towerScreenY = new double[0];

    /** Index of the currently hovered tower, or -1. */
    private int hoveredTower = -1;

    /** Colour used for cell-tower symbols. */
    private static final Color TOWER_COLOR        = Color.web("#f97316");  // orange-500
    private static final Color TOWER_RANGE_COLOR  = Color.web("#f9731630"); // semi-transparent
    private static final double TOWER_HIT_R = 12.0;

    // ── Dataset-reported cell-site layer ────────────────────────────────────────

    /**
     * When {@code true}, shows the unique geographic positions taken directly
     * from the dataset's own latitude/longitude columns (e.g. ETSI retained-data
     * {@code latitude_dec}/{@code longitude_dec}) as a distinct marker layer.
     *
     * <p>These coordinates denote the cell tower a subscriber was attached to
     * at the time of the record — <b>not</b> the subscriber's device position.
     * Several {@link DataPoint}s frequently share the same coordinate (the same
     * tower serving multiple connections), so this layer deduplicates by
     * position to show how many distinct cell sites occur in the dataset. It is
     * independent of the ULI/OpenCelliD-resolved {@link #cellTowers} layer,
     * which estimates tower positions from the decoded cell identity rather
     * than reading them straight from the record.</p>
     *
     * <p>A physical mast often carries several sector antennas pointing in
     * different directions, all reported at the same coordinate — the
     * {@code azimuth} column (0–360°, 0 = true north, clockwise) tells these
     * apart. {@link #rebuildDbCellSites()} therefore splits records at the
     * same position into separate {@link DbCellSite} entries whenever they
     * report a (meaningfully) different azimuth, and {@link #drawDbCellSymbol}
     * renders each such sector as a directional wedge instead of the plain
     * diamond used when no azimuth is available.</p>
     */
    private boolean dbCellLayerEnabled = false;

    /** Deduplicated cell-site positions derived from {@link #geoPoints}. */
    private List<DbCellSite> dbCellSites = new ArrayList<>();

    /** Screen positions of {@link #dbCellSites}, parallel arrays. */
    private double[] dbCellScreenX = new double[0];
    private double[] dbCellScreenY = new double[0];

    /** Index of the currently hovered dataset cell site, or -1. */
    private int hoveredDbCell = -1;

    /** Colour used for dataset-reported cell-site symbols (distinct from {@link #TOWER_COLOR}). */
    private static final Color  DB_CELL_COLOR  = Color.web("#22c55e"); // green-500
    private static final double DB_CELL_HIT_R  = 17.0;

    // Sector-wedge geometry shared between drawDbCellSymbol(), wedgeBaseLenPx()
    // and the wedge-aware part of hitTestDbCell() — must match so the
    // clickable area lines up with what's actually drawn. The hover-state
    // length is base length × 1.25 (see drawDbCellSymbol), not a separate
    // constant, so it scales the same way whether the base length is the
    // schematic placeholder or a real range estimate.
    private static final double DB_CELL_WEDGE_HALF_SPREAD_DEG = 22.0; // schematic ~44° sector cone
    private static final double DB_CELL_WEDGE_LEN             = 24.0;

    /**
     * One deduplicated cell-site/sector position, with a count of
     * contributing records.
     */
    private static final class DbCellSite {
        final double lat, lon;
        int count;
        /**
         * Decoded ULI (User Location Information) cell identity of the first
         * {@link ResponseRecordDataPoint} seen at this position, or
         * {@code null} if none of the contributing records carried a
         * decodable {@code user_location_info} value (or the records are of
         * a non-ETSI table type). See {@link UliDecoder}.
         */
        UliDecoder.CellInfo cellInfo;
        /**
         * Antenna azimuth in degrees (0–360°, 0 = true north, clockwise) from
         * the {@code azimuth} column of the first contributing record that
         * reported one, or {@code null} if none did. Sites at the same
         * coordinate but a different azimuth are kept as separate
         * {@code DbCellSite} entries — see {@link #rebuildDbCellSites()}.
         */
        Double azimuthDeg;
        /**
         * Estimated cell range in metres, taken from the first contributing
         * record whose {@link ResponseRecordDataPoint#getCellTower()} had
         * already resolved (via OpenCelliD/BeaconDB — see
         * {@link CellTowerResolver}) by the time this site was built, or
         * {@code null} if none had. Resolution runs asynchronously and the
         * dataset cell-site layer is rebuilt on each reload before it
         * necessarily completes, so this is best-effort: a site may render
         * with the schematic placeholder wedge now and gain a real-range
         * sector the next time the layer is rebuilt. Never present in the
         * ETSI export itself — only an external database's estimate.
         */
        Integer rangeMetres;
        /**
         * Short, stable letter identifier ("A", "B", … "Z", "AA", "AB", …)
         * so an analyst can tell distinct sites/sectors apart at a glance —
         * and refer to "Mast B" verbally — without having to read out
         * coordinates. Assigned once per {@link #rebuildDbCellSites()} call,
         * in the order sites are first encountered while scanning
         * {@link #geoPoints}; see {@link #siteLabel(int)}.
         */
        String label;
        DbCellSite(double lat, double lon) { this.lat = lat; this.lon = lon; this.count = 1; }
    }

    /**
     * Spreadsheet-style column label for a zero-based index: 0→"A", 1→"B",
     * …, 25→"Z", 26→"AA", 27→"AB", … Used to give every {@link DbCellSite}
     * a short, human-friendly identifier instead of just raw coordinates.
     */
    private static String siteLabel(int index) {
        StringBuilder sb = new StringBuilder();
        int n = index;
        do {
            sb.insert(0, (char) ('A' + (n % 26)));
            n = n / 26 - 1;
        } while (n >= 0);
        return sb.toString();
    }

    // ── Sequence-analysis layer (IMSI movement timeline) ────────────────────────

    /**
     * Chronological stops for the IMSI currently selected in {@link #imsiCombo},
     * built by {@link SequenceAnalyzer#buildSequence}. Empty when no IMSI is
     * selected. Rendered as a directional path connecting consecutive sites
     * in time order, distinct in colour and shape from both the ULI-resolved
     * tower layer and the dataset cell-site layer so all three can be shown
     * together without being confused for one another.
     */
    private List<SequenceAnalyzer.Stop> sequenceStops = new ArrayList<>();

    /** Screen positions of {@link #sequenceStops}, parallel arrays. */
    private double[] seqScreenX = new double[0];
    private double[] seqScreenY = new double[0];

    /** Index of the currently hovered sequence stop, or -1. */
    private int hoveredSeqStop = -1;

    private static final Color  SEQ_COLOR = Color.web("#a855f7"); // purple-500
    private static final double SEQ_HIT_R = 14.0;

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
    private final HBox      seqBar;
    private final Tooltip   tooltip    = new Tooltip();

    // Zoom control overlay (placed in the bottom-left corner via StackPane)
    private final Button btnZoomIn;
    private final Button btnZoomOut;
    private final Label  zoomLabel;
    private final VBox   zoomControl;

    // Sequence-analysis controls (IMSI selector + result table)
    private final ComboBox<String> imsiCombo;
    private final Label            seqStatusLabel;
    private final TableView<SequenceAnalyzer.Stop> sequenceTable;

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

        // Cell-tower toggle button (right side of header)
        Button btnTower = new Button("\uD83D\uDCF6 Funkmasten");
        btnTower.setFont(Font.font("Monospace", 10));
        btnTower.setTooltip(new Tooltip(
                "Funkzellmasten aus OpenCelliD einblenden.\n" +
                "Erfordert einen API-Schlüssel (opencellid.org).\n" +
                "Bereits aufgelöste Masten werden aus dem lokalen Cache geladen."));
        btnTower.setOnAction(e -> toggleCellTowerLayer(btnTower));

        // Dataset cell-site toggle button — shows the unique tower positions
        // taken directly from the record's own geo columns (e.g. latitude_dec/
        // longitude_dec), as opposed to the ULI/OpenCelliD-resolved towers above.
        Button btnDbCell = new Button("🗼 Funkzellen (Datensatz)");
        btnDbCell.setFont(Font.font("Monospace", 10));
        btnDbCell.setTooltip(new Tooltip(
                "Zeigt die eindeutigen Funkzellen-Standorte, die direkt aus den\n" +
                "Geo-Spalten des Datensatzes stammen (z. B. latitude_dec/longitude_dec).\n" +
                "Diese Koordinaten bezeichnen die Position des Funkmasts, an dem der\n" +
                "Teilnehmer angemeldet war — nicht die Position des Endgeräts."));
        btnDbCell.setOnAction(e -> toggleDbCellLayer(btnDbCell));

        Region headerSpacer = new Region();
        HBox.setHgrow(headerSpacer, Priority.ALWAYS);

        header = new HBox(16, titleLabel, hint, headerSpacer, btnDbCell, btnTower);
        header.setAlignment(Pos.CENTER_LEFT);

        // ── Sequenzanalyse-Leiste (IMSI-Auswahl) ─────────────────────────────
        Label seqLabel = new Label("🧭 Sequenzanalyse (IMSI):");
        seqLabel.setFont(Font.font("Monospace", 10));

        imsiCombo = new ComboBox<>();
        imsiCombo.setPromptText("IMSI wählen …");
        imsiCombo.setPrefWidth(220);
        imsiCombo.setTooltip(new Tooltip(
                "Zeigt den zeitlichen Bewegungsverlauf der gewählten IMSI:\n" +
                "verbindet die Funkzellen, an denen sie chronologisch angemeldet\n" +
                "war, als nummerierten Pfad auf der Karte und listet sie unten\n" +
                "als Tabelle auf."));
        imsiCombo.setOnAction(e -> onImsiSelected(imsiCombo.getValue()));

        seqStatusLabel = new Label("");
        seqStatusLabel.setFont(Font.font("Monospace", 10));

        seqBar = new HBox(10, seqLabel, imsiCombo, seqStatusLabel);
        seqBar.setAlignment(Pos.CENTER_LEFT);
        seqBar.setPadding(new Insets(0, 16, 8, 16));

        VBox topBox = new VBox(header, seqBar);
        setTop(topBox);

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

        // ── Sequenz-Tabelle (unterhalb der Karte, nur sichtbar wenn eine
        //    IMSI ausgewählt ist) ───────────────────────────────────────────
        sequenceTable = buildSequenceTable();
        sequenceTable.setManaged(false);
        sequenceTable.setVisible(false);

        VBox centerBox = new VBox(canvasWrapper, sequenceTable);
        VBox.setVgrow(canvasWrapper, Priority.ALWAYS);
        centerBox.setMinSize(0, 0);
        setCenter(centerBox);

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

        cellTowers.clear();
        focusedPoint = null;
        if (!geoPoints.isEmpty()) fitView();
        renderGeneration.incrementAndGet();

        // If the cell-tower layer is active, resolve towers for the new data.
        if (cellTowerLayerEnabled) resolveCellTowers();

        // If the dataset cell-site layer is active, rebuild it from the new data.
        if (dbCellLayerEnabled) rebuildDbCellSites();

        // Repopulate the IMSI selector for the sequence-analysis layer and
        // clear any sequence built from the previous dataset.
        String previousSelection = imsiCombo.getValue();
        imsiCombo.getItems().setAll(SequenceAnalyzer.listImsis(allPoints));
        sequenceStops = new ArrayList<>();
        seqScreenX = new double[0];
        seqScreenY = new double[0];
        hoveredSeqStop = -1;
        sequenceTable.getItems().clear();
        setSequenceTableVisible(false);
        if (previousSelection != null && imsiCombo.getItems().contains(previousSelection)) {
            imsiCombo.setValue(previousSelection);
            onImsiSelected(previousSelection);
        } else {
            imsiCombo.setValue(null);
            seqStatusLabel.setText("");
        }

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
        centerOn(dp.getCoordinate().getLatitude(), dp.getCoordinate().getLongitude());
    }

    /**
     * Centres the map on ({@code lat}, {@code lon}), zooming to street level
     * if currently too far out, and repaints. Shared by {@link #focusPoint}
     * and the sequence-analysis table's row-click handler.
     */
    private void centerOn(double lat, double lon) {
        centerLat = lat;
        centerLon = lon;
        if (zoom < 12) {
            zoom = 13;
            renderGeneration.incrementAndGet();
            updateZoomLabel();
        }
        render();
    }

    /**
     * Public entry point for other panes (e.g. the LLM co-location results
     * table in {@link MapViewPane}) to centre the map on a coordinate
     * without needing access to internal state.
     */
    public void centerOnCoordinate(double lat, double lon) {
        centerOn(lat, lon);
    }

    /**
     * Selects the given IMSI in the sequence/path overlay ({@link #imsiCombo}),
     * as if the analyst had picked it manually. Used by {@link MapViewPane}
     * when jumping here from a Police-Analysis result row that names an IMSI
     * (e.g. {@code ROAMERS}) — a single "Auf Karte anzeigen" coordinate is
     * only the first of possibly several cells that IMSI visited, so this
     * additionally draws the full chronological path and fits the view to
     * it, which is what actually answers "which cells did this IMSI visit".
     * No-op if {@code imsi} is blank or isn't present in the current
     * dataset's combo items.
     */
    public void selectImsiForPath(String imsi) {
        if (imsi == null || imsi.isBlank()) return;
        if (!imsiCombo.getItems().contains(imsi)) return;
        imsiCombo.setValue(imsi);
        onImsiSelected(imsi);
    }

    /** Switches the colour palette and repaints. */
    public void applyTheme(Theme t) {
        this.theme = t;
        setStyle(t.bgStyle());
        header.setStyle(t.bgStyle() + t.borderBottomStyle());
        seqBar.setStyle(t.bgStyle() + t.borderBottomStyle());
        titleLabel.setTextFill(t.accent2);
        statusLabel.setStyle(t.bgStyle() + t.borderTopStyle());
        statusLabel.setTextFill(t.label);

        styleZoomControl(t);

        tooltip.setStyle(
                "-fx-font-family: monospace; -fx-font-size: 11px;" +
                "-fx-background-color: " + Theme.hex(t.bgAlt) + ";" +
                "-fx-text-fill: "        + Theme.hex(t.labelStrong) + ";" +
                "-fx-border-color: "     + Theme.hex(t.border) + "; -fx-border-width: 1;");
        // Tooltip sofort anzeigen, lange sichtbar lassen
        tooltip.setShowDelay(Duration.millis(200));
        tooltip.setShowDuration(Duration.seconds(30));
        tooltip.setHideDelay(Duration.millis(100));
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
        if (cellTowerLayerEnabled && !cellTowers.isEmpty())
            drawCellTowers(gc, W, H, tlTX, tlTY, tPx);
        drawMarkers(gc, W, H, tlTX, tlTY, tPx);
        // Drawn last (on top): the dataset cell-site ring sits exactly at the
        // same coordinate as the regular data marker it belongs to, so it must
        // render after drawMarkers and as a hollow outline — otherwise it would
        // either be fully hidden behind the marker or fully hide it.
        if (dbCellLayerEnabled && !dbCellSites.isEmpty())
            drawDbCellSites(gc, W, H, tlTX, tlTY, tPx);
        // Sequence path drawn last of all: it's an explicit, user-requested
        // overlay (one selected IMSI) and should never be obscured by the
        // denser always-on layers underneath it.
        if (!sequenceStops.isEmpty())
            drawSequencePath(gc, W, H, tlTX, tlTY, tPx);
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
    // Cell-tower layer
    // -------------------------------------------------------------------------

    /**
     * Toggles the cell-tower layer on/off and updates the button label.
     * When switched on, {@link #resolveCellTowers()} is called to start
     * async OpenCelliD lookups for all data points with a decoded ULI.
     */
    private void toggleCellTowerLayer(Button btn) {
        cellTowerLayerEnabled = !cellTowerLayerEnabled;
        if (cellTowerLayerEnabled) {
            btn.setText("\uD83D\uDCF6 Funkmasten \u2713");
            btn.setStyle("-fx-text-fill: #f97316;");
            // Alle gecachten CellTower-Referenzen löschen —
            // nötig wenn der BeaconDbClient-Cache zwischenzeitlich geleert wurde
            // oder eine neue CSV geladen wurde.
            geoPoints.stream()
                    .filter(p -> p instanceof ResponseRecordDataPoint)
                    .map(p -> (ResponseRecordDataPoint) p)
                    .forEach(rr -> rr.setCellTower(null));
            synchronized (this) { cellTowers.clear(); }
            towerScreenX = new double[0];
            towerScreenY = new double[0];
            hoveredTower = -1;
            resolveCellTowers();
        } else {
            btn.setText("\uD83D\uDCF6 Funkmasten");
            btn.setStyle("");
            synchronized (this) { cellTowers.clear(); }
            towerScreenX = new double[0];
            towerScreenY = new double[0];
            hoveredTower = -1;
            render();
        }
    }

    /**
     * Toggles the dataset cell-site layer on/off. Unlike the ULI-resolved
     * {@link #cellTowers} layer, this needs no async lookup — the positions
     * already exist in {@link #geoPoints} — so it can be rebuilt synchronously.
     */
    private void toggleDbCellLayer(Button btn) {
        dbCellLayerEnabled = !dbCellLayerEnabled;
        if (dbCellLayerEnabled) {
            btn.setText("🗼 Funkzellen (Datensatz) ✓");
            btn.setStyle("-fx-text-fill: #22c55e;");
            rebuildDbCellSites();
        } else {
            btn.setText("🗼 Funkzellen (Datensatz)");
            btn.setStyle("");
            dbCellSites.clear();
            dbCellScreenX = new double[0];
            dbCellScreenY = new double[0];
            hoveredDbCell = -1;
        }
        render();
    }

    /**
     * Rebuilds {@link #dbCellSites} from the current {@link #geoPoints} by
     * deduplicating coordinates within ~50 m — the same threshold used for
     * the ULI-resolved {@link #cellTowers} layer (see {@link #addTowerIfNew}).
     * Each resulting site records how many records reference that position.
     *
     * <p>Records at the same coordinate but a (meaningfully) different
     * {@code azimuth} are kept as separate {@link DbCellSite} entries instead
     * of being merged — a single physical mast often has several sector
     * antennas pointing in different directions, and collapsing them into one
     * marker would hide that. Records without a parsable azimuth still
     * dedupe purely by position, as before.</p>
     */
    private void rebuildDbCellSites() {
        final double DEDUP_DIST_DEG = 0.0005; // ~50 m
        List<DbCellSite> sites = new ArrayList<>();
        for (DataPoint dp : geoPoints) {
            if (dp.getCoordinate() == null) continue;
            double lat = dp.getCoordinate().getLatitude();
            double lon = dp.getCoordinate().getLongitude();
            UliDecoder.CellInfo ci = (dp instanceof ResponseRecordDataPoint rrdp) ? rrdp.getCellInfo() : null;
            Double az = (dp instanceof ResponseRecordDataPoint rrdp) ? parseAzimuth(rrdp.getAzimuth()) : null;
            Integer range = null;
            if (dp instanceof ResponseRecordDataPoint rrdp && rrdp.getCellTower() != null
                    && rrdp.getCellTower().rangeMetres > 0) {
                range = rrdp.getCellTower().rangeMetres;
            }
            DbCellSite existing = null;
            for (DbCellSite s : sites) {
                if (Math.abs(s.lat - lat) < DEDUP_DIST_DEG
                        && Math.abs(s.lon - lon) < DEDUP_DIST_DEG
                        && azimuthMatches(s.azimuthDeg, az)) {
                    existing = s;
                    break;
                }
            }
            if (existing != null) {
                existing.count++;
                // Keep the first decodable cellInfo/range found at this site;
                // later records at the same position are expected to report
                // the same physical cell, so there's nothing to gain by
                // overwriting it.
                if (existing.cellInfo == null && ci != null) existing.cellInfo = ci;
                if (existing.rangeMetres == null && range != null) existing.rangeMetres = range;
            } else {
                DbCellSite site = new DbCellSite(lat, lon);
                site.cellInfo = ci;
                site.azimuthDeg = az;
                site.rangeMetres = range;
                site.label = siteLabel(sites.size());
                sites.add(site);
            }
        }
        dbCellSites = sites;

        // Keep the hit-test screen-position arrays in sync with dbCellSites
        // immediately, not just when the next render() happens to call
        // drawDbCellSites(). render() only repopulates them when dbCellSites
        // is non-empty (see the dbCellLayerEnabled check around the
        // drawDbCellSites call), so a reload that shrinks/empties the list
        // could otherwise leave these arrays stale (longer than the new
        // list), letting hitTestDbCell() report a hit index that's out of
        // bounds for the rebuilt dbCellSites — crashing onMouseMoved.
        dbCellScreenX = new double[sites.size()];
        dbCellScreenY = new double[sites.size()];
        hoveredDbCell = -1;
    }

    /**
     * Parses the raw {@code azimuth} column value into degrees (0–360°,
     * normalised), or {@code null} if blank/unparseable. Tolerates a
     * trailing {@code "°"} sign, since some exports include it.
     */
    private static Double parseAzimuth(String raw) {
        if (raw == null || raw.isBlank()) return null;
        try {
            double v = Double.parseDouble(raw.replace("°", "").trim());
            v %= 360.0;
            if (v < 0) v += 360.0;
            return v;
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    /**
     * {@code true} if two (possibly absent) azimuth readings represent the
     * same sector — both {@code null}, or both present and within a small
     * tolerance (rounding/formatting noise, not a different sector).
     */
    private static boolean azimuthMatches(Double a, Double b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        return Math.abs(a - b) < 0.5;
    }

    private static final String[] COMPASS_ABBREV = {
            "N", "NO", "O", "SO", "S", "SW", "W", "NW"
    };

    /** Maps an azimuth in degrees (0–360°, 0 = N, clockwise) to an 8-point German compass abbreviation. */
    private static String compassAbbrev(double azimuthDeg) {
        int idx = (int) Math.round(azimuthDeg / 45.0) % 8;
        if (idx < 0) idx += 8;
        return COMPASS_ABBREV[idx];
    }

    /**
     * Renders the dataset-reported cell-site layer: one marker per unique
     * (coordinate, azimuth) combination found in the source data's own geo
     * columns, with a count badge when several records share the same
     * site/sector. Sites with a known azimuth are drawn as a directional
     * wedge (see {@link #drawDbCellSymbol}); sites without one keep the
     * plain diamond ring. Wedge length reflects the externally-resolved
     * {@link DbCellSite#rangeMetres} when available, else a fixed schematic
     * placeholder — see {@link #wedgeBaseLenPx}.
     */
    private void drawDbCellSites(GraphicsContext gc, double W, double H,
                                  double tlTX, double tlTY, int tPx) {
        dbCellScreenX = new double[dbCellSites.size()];
        dbCellScreenY = new double[dbCellSites.size()];

        for (int i = 0; i < dbCellSites.size(); i++) {
            DbCellSite s = dbCellSites.get(i);
            double sx = (lonToTileX(s.lon, zoom) - tlTX) * tPx;
            double sy = (latToTileY(s.lat,  zoom) - tlTY) * tPx;
            dbCellScreenX[i] = sx;
            dbCellScreenY[i] = sy;
            if (sx < -80 || sx > W + 80 || sy < -80 || sy > H + 80) continue;

            drawDbCellSymbol(gc, sx, sy, i == hoveredDbCell, s.count, s.azimuthDeg,
                    wedgeBaseLenPx(s), s.rangeMetres != null, s.label);
        }
    }

    /**
     * Base (non-hovered) on-screen wedge length for a dataset cell-site
     * sector. Derived from the externally-resolved {@link DbCellSite#rangeMetres}
     * (OpenCelliD/BeaconDB estimate — see {@link CellTowerResolver}) when
     * available, converted to pixels at the site's latitude and the current
     * zoom and clamped to a sane on-screen size; falls back to the fixed
     * {@link #DB_CELL_WEDGE_LEN} schematic placeholder otherwise. Shared by
     * {@link #drawDbCellSites} (rendering) and {@link #hitTestDbCell}
     * (hover/click), so the drawn and clickable sector sizes always match.
     */
    private double wedgeBaseLenPx(DbCellSite s) {
        if (s.rangeMetres == null) return DB_CELL_WEDGE_LEN;
        double metresToPixels = metresToPixels(s.lat, zoom, TileCache.tilePx());
        double lenPx = s.rangeMetres * metresToPixels;
        // Never collapse below the schematic minimum (illegible) and never
        // balloon past a size that would make one sector dominate the view.
        return Math.max(DB_CELL_WEDGE_LEN, Math.min(lenPx, 220.0));
    }

    /**
     * Draws the dataset cell-site/sector symbol at {@code (cx, cy)}.
     *
     * <p>When {@code azimuthDeg} is known, draws a directional wedge — apex
     * at the reported coordinate, opening toward the antenna's azimuth
     * (0° = true north, clockwise) — so sectors of a multi-antenna mast that
     * share the same coordinate but point in different directions are
     * visually distinguishable instead of collapsing into one marker. The
     * wedge's opening angle is always a schematic placeholder (not a
     * calibrated antenna beamwidth, which isn't part of any available
     * source). Its <em>length</em> is real when {@code rangeIsEstimate} is
     * {@code true} — {@code baseLenPx} then reflects an externally-resolved
     * range estimate (OpenCelliD/BeaconDB), drawn with a dashed outline to
     * mark it as a database estimate rather than ETSI source data; otherwise
     * {@code baseLenPx} is the fixed schematic placeholder, drawn solid.</p>
     *
     * <p>Without an azimuth, falls back to the original hollow diamond
     * "cell site" ring: it sits at the exact same coordinate as the regular
     * data marker drawn underneath it, so the ring must be larger than the
     * marker and unfilled (stroke only) — otherwise either the marker would
     * hide the ring (drawn first) or the ring would hide the marker (drawn
     * filled). The shape itself is deliberately different from
     * {@link #drawTowerSymbol}'s antenna glyph so the dataset-reported layer
     * and the ULI/OpenCelliD-resolved layer stay visually distinguishable
     * when both are shown at once.</p>
     *
     * <p>{@code label} (see {@link DbCellSite#label}) is always drawn next
     * to the marker so distinct sites/sectors can be told apart at a
     * glance; the record count is appended in parentheses only when it's
     * actually informative (more than one record, or while hovering).</p>
     */
    private void drawDbCellSymbol(GraphicsContext gc, double cx, double cy,
                                   boolean hovered, int count, Double azimuthDeg,
                                   double baseLenPx, boolean rangeIsEstimate, String label) {
        Color  c = hovered ? DB_CELL_COLOR.brighter() : DB_CELL_COLOR;
        // Comfortably larger than the regular marker (MARKER_R = 8, up to ~11
        // when hovered) so the diamond/wedge forms a visible ring around it.
        double r = hovered ? 17.0 : 14.0;

        if (hovered) {
            gc.setFill(c.deriveColor(0, 1, 1, 0.18));
            gc.fillOval(cx - 22, cy - 22, 44, 44);
        }

        if (azimuthDeg != null) {
            double az          = Math.toRadians(azimuthDeg);
            double halfSpread  = Math.toRadians(DB_CELL_WEDGE_HALF_SPREAD_DEG);
            double len         = hovered ? baseLenPx * 1.25 : baseLenPx;
            double aLeft = az - halfSpread, aRight = az + halfSpread;

            double[] xs = { cx, cx + Math.sin(aLeft) * len, cx + Math.sin(az) * (len * 1.05), cx + Math.sin(aRight) * len };
            double[] ys = { cy, cy - Math.cos(aLeft) * len, cy - Math.cos(az) * (len * 1.05), cy - Math.cos(aRight) * len };

            gc.setFill(c.deriveColor(0, 1, 1, hovered ? 0.55 : 0.38));
            gc.fillPolygon(xs, ys, 4);
            gc.setStroke(c);
            gc.setLineWidth(hovered ? 2.4 : 1.8);
            // Dashed outline marks a real (externally-resolved) range estimate
            // as such, vs. the solid outline used for the schematic placeholder
            // — so it's visually clear which is ETSI source data and which is
            // a database guess.
            gc.setLineDashes(rangeIsEstimate ? new double[] { 5, 4 } : null);
            gc.strokePolygon(xs, ys, 4);
            gc.setLineDashes((double[]) null);

            // Small core dot marking the exact reported coordinate.
            gc.setFill(c);
            gc.fillOval(cx - 3, cy - 3, 6, 6);

            // The site letter is always shown so distinct sectors/masts stay
            // distinguishable at a glance; the record count is appended only
            // when it's actually informative (several records, or hovering).
            {
                String text = (hovered || count > 1) ? label + " (" + count + ")" : label;
                gc.setFont(Font.font("Monospace", FontWeight.BOLD, 9));
                gc.setFill(c);
                double tw = text.length() * 5.5;
                double lx = cx + Math.sin(az) * (len + 10);
                double ly = cy - Math.cos(az) * (len + 10);
                gc.fillText(text, lx - tw / 2.0, ly);
            }
            return;
        }

        double[] xs = { cx, cx + r, cx, cx - r };
        double[] ys = { cy - r, cy, cy + r, cy };

        gc.setStroke(c);
        gc.setLineWidth(hovered ? 3.0 : 2.2);
        gc.strokePolygon(xs, ys, 4);

        {
            String text = (hovered || count > 1) ? label + " (" + count + ")" : label;
            gc.setFont(Font.font("Monospace", FontWeight.BOLD, 9));
            gc.setFill(c);
            double tw = text.length() * 5.5;
            gc.fillText(text, cx - tw / 2.0, cy - r - 4);
        }
    }

    // -------------------------------------------------------------------------
    // Sequence-analysis layer (IMSI movement timeline)
    // -------------------------------------------------------------------------

    /**
     * Called when the user picks an IMSI from {@link #imsiCombo} (or clears
     * the selection). Rebuilds {@link #sequenceStops} via
     * {@link SequenceAnalyzer#buildSequence}, updates the result table and
     * repaints the path overlay.
     */
    private void onImsiSelected(String imsi) {
        if (imsi == null || imsi.isBlank()) {
            sequenceStops = new ArrayList<>();
            seqScreenX = new double[0];
            seqScreenY = new double[0];
            hoveredSeqStop = -1;
            sequenceTable.getItems().clear();
            setSequenceTableVisible(false);
            seqStatusLabel.setText("");
            render();
            return;
        }

        sequenceStops = SequenceAnalyzer.buildSequence(geoPoints, imsi);
        sequenceTable.getItems().setAll(sequenceStops);
        setSequenceTableVisible(!sequenceStops.isEmpty());

        if (sequenceStops.isEmpty()) {
            seqStatusLabel.setText("⚠ keine Geo-Datensätze für diese IMSI");
        } else if (sequenceStops.size() == 1) {
            seqStatusLabel.setText("nur 1 Standort — keine Bewegung erkennbar");
        } else {
            seqStatusLabel.setText(sequenceStops.size() + " Standorte chronologisch");
        }

        // Fit the view to the sequence so the whole path is visible.
        if (!sequenceStops.isEmpty()) {
            double minLat = sequenceStops.stream().mapToDouble(s -> s.lat).min().orElse(centerLat);
            double maxLat = sequenceStops.stream().mapToDouble(s -> s.lat).max().orElse(centerLat);
            double minLon = sequenceStops.stream().mapToDouble(s -> s.lon).min().orElse(centerLon);
            double maxLon = sequenceStops.stream().mapToDouble(s -> s.lon).max().orElse(centerLon);
            centerLat = (minLat + maxLat) / 2.0;
            centerLon = (minLon + maxLon) / 2.0;
            double span = Math.max(maxLat - minLat, maxLon - minLon);
            if      (span < 0.005) zoom = 15;
            else if (span < 0.05)  zoom = 13;
            else if (span < 0.5)   zoom = 10;
            else if (span < 5)     zoom = 8;
            else if (span < 20)    zoom = 6;
            else                   zoom = 4;
            renderGeneration.incrementAndGet();
            updateZoomLabel();
        }
        render();
    }

    /** Shows/hides the sequence-result table beneath the map. */
    private void setSequenceTableVisible(boolean visible) {
        sequenceTable.setManaged(visible);
        sequenceTable.setVisible(visible);
        if (visible) sequenceTable.setPrefHeight(180);
    }

    /** Builds the (initially empty/hidden) sequence-result table. */
    private TableView<SequenceAnalyzer.Stop> buildSequenceTable() {
        TableView<SequenceAnalyzer.Stop> table = new TableView<>();
        table.setPlaceholder(new Label("Keine Sequenz ausgewählt."));

        TableColumn<SequenceAnalyzer.Stop, Integer> colIdx = new TableColumn<>("#");
        colIdx.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(
                table.getItems().indexOf(cd.getValue()) + 1));
        colIdx.setPrefWidth(36);

        TableColumn<SequenceAnalyzer.Stop, String> colFrom = new TableColumn<>("Von");
        colFrom.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().formattedFirstSeen()));
        colFrom.setPrefWidth(150);

        TableColumn<SequenceAnalyzer.Stop, String> colTo = new TableColumn<>("Bis");
        colTo.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().formattedLastSeen()));
        colTo.setPrefWidth(150);

        TableColumn<SequenceAnalyzer.Stop, String> colDwell = new TableColumn<>("Verweildauer");
        colDwell.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().formattedDwell()));
        colDwell.setPrefWidth(100);

        TableColumn<SequenceAnalyzer.Stop, String> colCell = new TableColumn<>("Zelle");
        colCell.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().cellLabel()));
        colCell.setPrefWidth(180);

        TableColumn<SequenceAnalyzer.Stop, String> colPos = new TableColumn<>("Position");
        colPos.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(
                String.format("%.5f, %.5f", cd.getValue().lat, cd.getValue().lon)));
        colPos.setPrefWidth(150);

        TableColumn<SequenceAnalyzer.Stop, String> colDist = new TableColumn<>("Distanz z. vorh.");
        colDist.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(
                cd.getValue().distanceFromPreviousKm > 0
                        ? String.format("%.2f km", cd.getValue().distanceFromPreviousKm)
                        : "–"));
        colDist.setPrefWidth(110);

        TableColumn<SequenceAnalyzer.Stop, Integer> colCount = new TableColumn<>("Datensätze");
        colCount.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(cd.getValue().recordCount));
        colCount.setPrefWidth(90);

        table.getColumns().addAll(List.of(colIdx, colFrom, colTo, colDwell, colCell, colPos, colDist, colCount));
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);

        table.setRowFactory(tv -> {
            javafx.scene.control.TableRow<SequenceAnalyzer.Stop> row = new javafx.scene.control.TableRow<>();
            row.setOnMouseClicked(e -> {
                if (row.getItem() != null) centerOn(row.getItem().lat, row.getItem().lon);
            });
            return row;
        });

        return table;
    }

    /**
     * Renders the IMSI movement path: a directional polyline connecting
     * consecutive stops in chronological order, plus a numbered marker at
     * each stop. Drawn last (on top of every other layer) since it is a
     * thin overlay and the user explicitly selected it.
     */
    private void drawSequencePath(GraphicsContext gc, double W, double H,
                                   double tlTX, double tlTY, int tPx) {
        seqScreenX = new double[sequenceStops.size()];
        seqScreenY = new double[sequenceStops.size()];

        for (int i = 0; i < sequenceStops.size(); i++) {
            SequenceAnalyzer.Stop s = sequenceStops.get(i);
            seqScreenX[i] = (lonToTileX(s.lon, zoom) - tlTX) * tPx;
            seqScreenY[i] = (latToTileY(s.lat,  zoom) - tlTY) * tPx;
        }

        // ── Connecting line with arrowheads showing direction of travel ──────
        gc.setStroke(SEQ_COLOR);
        gc.setLineWidth(2.2);
        gc.setLineDashes(8, 6);
        for (int i = 1; i < seqScreenX.length; i++) {
            double x0 = seqScreenX[i - 1], y0 = seqScreenY[i - 1];
            double x1 = seqScreenX[i],     y1 = seqScreenY[i];
            gc.strokeLine(x0, y0, x1, y1);
            drawArrowhead(gc, x0, y0, x1, y1);
        }
        gc.setLineDashes(null);

        // ── Numbered stop markers ─────────────────────────────────────────────
        gc.setFont(Font.font("Monospace", FontWeight.BOLD, 9));
        for (int i = 0; i < seqScreenX.length; i++) {
            double sx = seqScreenX[i], sy = seqScreenY[i];
            if (sx < -40 || sx > W + 40 || sy < -40 || sy > H + 40) continue;
            boolean hovered = (i == hoveredSeqStop);
            double r = hovered ? 13.0 : 10.0;
            Color c = hovered ? SEQ_COLOR.brighter() : SEQ_COLOR;

            gc.setFill(Color.rgb(0, 0, 0, 0.3));
            gc.fillOval(sx - r + 2, sy - r + 2, r * 2, r * 2);
            gc.setFill(c);
            gc.fillOval(sx - r, sy - r, r * 2, r * 2);
            gc.setStroke(Color.WHITE);
            gc.setLineWidth(1.2);
            gc.strokeOval(sx - r, sy - r, r * 2, r * 2);

            String label = String.valueOf(i + 1);
            gc.setFill(Color.WHITE);
            double tw = label.length() * 5.5;
            gc.fillText(label, sx - tw / 2.0, sy + 3.2);
        }
    }

    /** Draws a small arrowhead at the midpoint of the segment, pointing from (x0,y0) to (x1,y1). */
    private void drawArrowhead(GraphicsContext gc, double x0, double y0, double x1, double y1) {
        double mx = (x0 + x1) / 2.0, my = (y0 + y1) / 2.0;
        double angle = Math.atan2(y1 - y0, x1 - x0);
        double size = 7.0;
        double a1 = angle + Math.PI - 0.4;
        double a2 = angle + Math.PI + 0.4;
        gc.setLineDashes(null);
        gc.setStroke(SEQ_COLOR);
        gc.setLineWidth(2.2);
        gc.strokeLine(mx, my, mx + size * Math.cos(a1), my + size * Math.sin(a1));
        gc.strokeLine(mx, my, mx + size * Math.cos(a2), my + size * Math.sin(a2));
        gc.setLineDashes(8, 6);
    }

    /** Returns the index of the sequence stop under the mouse, or -1. */
    private int hitTestSeqStop(double mx, double my) {
        if (seqScreenX.length == 0) return -1;
        for (int i = 0; i < seqScreenX.length; i++) {
            double dx = mx - seqScreenX[i];
            double dy = my - seqScreenY[i];
            if (Math.sqrt(dx * dx + dy * dy) <= SEQ_HIT_R) return i;
        }
        return -1;
    }

    /** Builds a tooltip for a sequence stop. */
    private String buildSeqStopTooltip(int index) {
        SequenceAnalyzer.Stop s = sequenceStops.get(index);
        StringBuilder sb = new StringBuilder();
        sb.append("🧭  STANDORT #").append(index + 1).append(" / ").append(sequenceStops.size()).append("\n");
        sb.append("─".repeat(36)).append("\n");
        row(sb, "Von",         s.formattedFirstSeen() + " UTC");
        row(sb, "Bis",         s.formattedLastSeen()  + " UTC");
        row(sb, "Verweildauer",s.formattedDwell());
        row(sb, "Zelle",       s.cellLabel());
        row(sb, "Lat",         String.format("%.6f°", s.lat));
        row(sb, "Lon",         String.format("%.6f°", s.lon));
        row(sb, "Datensätze",  String.valueOf(s.recordCount));
        if (s.distanceFromPreviousKm > 0)
            row(sb, "Distanz", String.format("%.2f km zum vorherigen Standort", s.distanceFromPreviousKm));
        return sb.toString().stripTrailing();
    }

    /**
     * Iterates all {@link ResponseRecordDataPoint}s in {@link #geoPoints},
     * extracts those with a decoded {@link UliDecoder.CellInfo}, and schedules
     * async {@link CellTowerResolver} lookups — with GPS-coordinate fallback.
     *
     * <p>The GPS coordinates from the data point's {@code coordinate} field are
     * passed to {@link CellTowerResolver#resolveAsync(UliDecoder.CellInfo,double,double)}
     * so that the resolver can fall back to nearest-tower proximity search when
     * the ULI-decoded cell identity is absent from the CSV (common for crowd-sourced
     * databases with incomplete coverage).</p>
     */
    private void resolveCellTowers() {
        CellTowerResolver resolver = CellTowerResolver.getInstance();
        BeaconDbClient    beaconDb = BeaconDbClient.getInstance();

        // Mindestens eine Quelle muss konfiguriert sein
        boolean hasCsv      = resolver.isCsvConfigured();
        boolean hasBeaconDb = beaconDb.isEnabled();

        if (!hasCsv && !hasBeaconDb) {
            Platform.runLater(() -> statusLabel.setText(
                    statusLabel.getText()
                    + "  |  ⚠ Keine Quelle: CSV oder beaconDB konfigurieren"));
            return;
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        int rrCount = 0, withGps = 0;

        for (DataPoint dp : geoPoints) {
            if (!(dp instanceof ResponseRecordDataPoint rr)) continue;
            rrCount++;

            double gpsLat = Double.NaN, gpsLon = Double.NaN;
            if (dp.getCoordinate() != null) {
                gpsLat = dp.getCoordinate().getLatitude();
                gpsLon = dp.getCoordinate().getLongitude();
                withGps++;
            }

            // Bereits aufgelöst
            if (rr.getCellTower() != null) {
                addTowerIfNew(rr.getCellTower());
                continue;
            }

            UliDecoder.CellInfo ci = rr.getCellInfo();
            final double fLat = gpsLat, fLon = gpsLon;

            CompletableFuture<Void> f = resolver
                    .resolveAsync(ci, fLat, fLon)
                    .thenAccept(tower -> {
                        if (tower == null) {
                            // kein Treffer in keiner Quelle
                            return;
                        }
                        System.out.printf("[TOWER] %s lat=%.5f lon=%.5f hasPos=%b%n",
                                tower.networkType, tower.latitude, tower.longitude,
                                tower.hasPosition());
                        if (!tower.hasPosition()) return;
                        rr.setCellTower(tower);
                        Platform.runLater(() -> {
                            int before = cellTowers.size();
                            addTowerIfNew(tower);
                            System.out.printf("[TOWER] added=%b total=%d layerOn=%b%n",
                                    cellTowers.size() > before,
                                    cellTowers.size(),
                                    cellTowerLayerEnabled);
                            render();
                        });
                    });
            futures.add(f);
        }

        final int finalRr = rrCount, finalGps = withGps;
        render();

        if (futures.isEmpty()) {
            Platform.runLater(() -> statusLabel.setText(
                    statusLabel.getText()
                    + "  |  ⚠ Keine ResponseRecord-Punkte ("
                    + finalRr + " RR, " + finalGps + " mit GPS)"));
            return;
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenRun(() -> Platform.runLater(() -> {
                    long resolved = geoPoints.stream()
                            .filter(p -> p instanceof ResponseRecordDataPoint rr
                                         && rr.getCellTower() != null)
                            .count();
                    statusLabel.setText(statusLabel.getText()
                                        + "  |  \uD83D\uDCF6 " + cellTowers.size() + " Masten"
                                        + " (" + resolved + "/" + finalRr + " aufgelöst)");
                    // The dataset cell-site sectors' range estimate comes from
                    // these same resolved CellTowers (see DbCellSite.rangeMetres
                    // / wedgeBaseLenPx), but rebuildDbCellSites() only reads
                    // rr.getCellTower() at the moment it runs — typically
                    // before this async resolution finished. Without this,
                    // the dataset layer keeps showing the schematic placeholder
                    // wedges until the user manually toggles it off and on
                    // again. Rebuild it now so newly resolved ranges appear
                    // automatically once tower resolution completes.
                    if (dbCellLayerEnabled) rebuildDbCellSites();
                    render();
                }));
    }

    /**
     * Adds a {@link CellTower} to the layer list, deduplicating by geographic
     * position (within 50 m) rather than by identity fields.
     *
     * <p>Identity-based deduplication (MCC+MNC+LAC+CI) is correct for ULI
     * exact-match results but breaks the GPS-proximity fallback: all 2000+
     * data points share the same GPS location and therefore map to the same
     * nearest tower — whose ECI differs each time because it carries the
     * match-type suffix. Position-based dedup keeps exactly one symbol per
     * physical site regardless of how many data points reference it.</p>
     */
    private synchronized void addTowerIfNew(CellTower tower) {
        final double DEDUP_DIST_DEG = 0.0005;  // ~50 m
        boolean exists = cellTowers.stream().anyMatch(t ->
                Math.abs(t.latitude  - tower.latitude)  < DEDUP_DIST_DEG &&
                Math.abs(t.longitude - tower.longitude) < DEDUP_DIST_DEG);
        if (!exists) {
            cellTowers.add(tower);
            // Karte anpassen falls Tower außerhalb des aktuellen Sichtbereichs
            ensureTowerVisible(tower);
        }
    }

    /**
     * Prüft ob der Tower im aktuellen Sichtbereich liegt.
     * Falls nicht wird die Karte NICHT automatisch verschoben (das wäre störend),
     * aber der Zoom wird ggf. angepasst damit der Tower erreichbar ist.
     * Ausgabe einer Debug-Meldung wenn Tower weit außerhalb liegt.
     */
    private void ensureTowerVisible(CellTower tower) {
        // Sichtbereich-Berechnung
        double W   = canvas.getWidth();
        double H   = canvas.getHeight();
        int    tPx = TileCache.tilePx();
        double cTX = lonToTileX(centerLon, zoom);
        double cTY = latToTileY(centerLat, zoom);
        double tlTX = cTX - W / 2.0 / tPx;
        double tlTY = cTY - H / 2.0 / tPx;

        double sx = (lonToTileX(tower.longitude, zoom) - tlTX) * tPx;
        double sy = (latToTileY(tower.latitude,  zoom) - tlTY) * tPx;

        if (sx < -80 || sx > W + 80 || sy < -80 || sy > H + 80) {
            // Tower liegt außerhalb — Abstand berechnen
            double dLat = Math.abs(tower.latitude  - centerLat);
            double dLon = Math.abs(tower.longitude - centerLon);
            double distKm = Math.sqrt(dLat * dLat + dLon * dLon) * 111.0;
            System.out.printf("[TOWER] ⚠ Tower außerhalb Sichtbereich! " +
                              "lat=%.4f lon=%.4f  sx=%.0f sy=%.0f  dist~%.1fkm%n",
                    tower.latitude, tower.longitude, sx, sy, distKm);
        }
    }

    /**
     * Renders cell-tower symbols (orange antenna icon + optional coverage ring).
     * Coverage radius is shown only at zoom ≥ 12 and only when the tower
     * reports a range ≥ 100 m.
     */
    private void drawCellTowers(GraphicsContext gc, double W, double H,
                                double tlTX, double tlTY, int tPx) {
        towerScreenX = new double[cellTowers.size()];
        towerScreenY = new double[cellTowers.size()];

        for (int i = 0; i < cellTowers.size(); i++) {
            CellTower t = cellTowers.get(i);
            double sx = (lonToTileX(t.longitude, zoom) - tlTX) * tPx;
            double sy = (latToTileY(t.latitude,  zoom) - tlTY) * tPx;
            towerScreenX[i] = sx;
            towerScreenY[i] = sy;
            if (sx < -80 || sx > W + 80 || sy < -80 || sy > H + 80) continue;

            boolean hovered = (i == hoveredTower);

            // ── Coverage radius ring ──────────────────────────────────────────
            // Skipped while the dataset cell-site layer is also showing: its
            // directional sector wedge (drawDbCellSymbol) draws the same
            // rangeMetres estimate, more precisely (per sector, not a full
            // circle), at this exact coordinate. Drawing both meant the big
            // translucent orange disc visually swallowed the thinner green
            // wedge — the wedge only became visible once this layer was
            // switched off, even though it had been rendered correctly all
            // along. Leaving this ring out when redundant fixes that without
            // needing to toggle anything.
            if (zoom >= 12 && t.rangeMetres > 100 && !dbCellLayerEnabled) {
                // Convert metres to pixels at current zoom/latitude
                double metresToPixels = metresToPixels(t.latitude, zoom, tPx);
                double radiusPx = t.rangeMetres * metresToPixels;
                gc.setFill(TOWER_RANGE_COLOR);
                gc.fillOval(sx - radiusPx, sy - radiusPx, radiusPx * 2, radiusPx * 2);
                gc.setStroke(TOWER_COLOR.deriveColor(0, 1, 1, 0.4));
                gc.setLineWidth(1.0);
                gc.strokeOval(sx - radiusPx, sy - radiusPx, radiusPx * 2, radiusPx * 2);
            }

            // ── Tower symbol ─────────────────────────────────────────────────
            // Draw a simple antenna glyph: vertical mast + 3 arcs
            drawTowerSymbol(gc, sx, sy, hovered);
        }
    }

    /**
     * Draws a minimal antenna symbol at (cx, cy).
     * <pre>
     *    )   (   arcs
     *   /|\      mast + struts
     *    |
     * </pre>
     */
    private void drawTowerSymbol(GraphicsContext gc, double cx, double cy, boolean hovered) {
        Color c = hovered ? TOWER_COLOR.brighter() : TOWER_COLOR;
        gc.setStroke(c);
        gc.setLineWidth(hovered ? 2.0 : 1.5);

        // Vertical mast
        gc.strokeLine(cx, cy - 12, cx, cy + 8);

        // Two diagonal struts
        gc.strokeLine(cx, cy - 6, cx - 6, cy + 6);
        gc.strokeLine(cx, cy - 6, cx + 6, cy + 6);

        // Three signal arcs (concentric semicircles above)
        for (int a = 1; a <= 3; a++) {
            double r = a * 4.5;
            gc.strokeArc(cx - r, cy - 12 - r, r * 2, r * 2, 20, 140, javafx.scene.shape.ArcType.OPEN);
        }

        // Filled dot at base
        gc.setFill(c);
        gc.fillOval(cx - 3, cy - 3, 6, 6);

        // Hover glow
        if (hovered) {
            gc.setFill(c.deriveColor(0, 1, 1, 0.25));
            gc.fillOval(cx - 16, cy - 16, 32, 32);
        }
    }

    /**
     * Converts metres on the ground to pixels on screen at the given zoom.
     * Uses the Mercator scale factor: pixels_per_metre = tPx * 2^zoom / (C * cos(lat))
     * where C = 40_075_016.686 m (Earth circumference).
     */
    private static double metresToPixels(double latDeg, int zoom, int tPx) {
        double C = 40_075_016.686;
        double cosLat = Math.cos(Math.toRadians(latDeg));
        double metersPerPixel = C * cosLat / (tPx * Math.pow(2, zoom));
        return 1.0 / metersPerPixel;
    }

    // ── Mouse events for tower layer ──────────────────────────────────────────

    /** Returns the index of the tower under the mouse, or -1. */
    private int hitTestTower(double mx, double my) {
        if (towerScreenX.length == 0) return -1;
        for (int i = 0; i < towerScreenX.length; i++) {
            double dx = mx - towerScreenX[i];
            double dy = my - towerScreenY[i];
            if (Math.sqrt(dx * dx + dy * dy) <= TOWER_HIT_R) return i;
        }
        return -1;
    }

    /** Returns the index of the dataset cell site under the mouse, or -1. */
    private int hitTestDbCell(double mx, double my) {
        if (dbCellScreenX.length == 0) return -1;

        // Several sectors of the same mast share one apex coordinate (see
        // rebuildDbCellSites()), so a plain "nearest within radius" test
        // would always return whichever sector happens to be first in the
        // list — the others could never be hovered. Wedge sites are instead
        // matched by the angle from the apex to the mouse, so each sector's
        // own slice of the cone is independently hoverable; only the apex
        // area itself (where the angle is undefined/noisy) falls back to
        // "closest by angle" so it isn't a dead zone.
        int    best       = -1;
        double bestAngDiff = Double.MAX_VALUE;
        double bestDist    = Double.MAX_VALUE;
        final double CORE_R = 6.0;

        for (int i = 0; i < dbCellScreenX.length; i++) {
            double dx   = mx - dbCellScreenX[i];
            double dy   = my - dbCellScreenY[i];
            double dist = Math.sqrt(dx * dx + dy * dy);
            DbCellSite s = dbCellSites.get(i);

            if (s.azimuthDeg == null) {
                if (dist <= DB_CELL_HIT_R && dist < bestDist) {
                    best = i;
                    bestAngDiff = -1; // diamonds have no angle; always preferred over a wedge tie
                    bestDist = dist;
                }
                continue;
            }

            // Same base length used for rendering (schematic or real-range
            // estimate — see wedgeBaseLenPx), plus the hover growth factor
            // and a slack margin so the clickable area matches what's drawn.
            double maxLen = wedgeBaseLenPx(s) * 1.25 + DB_CELL_HIT_R;
            if (dist > maxLen) continue;

            double bearing = Math.toDegrees(Math.atan2(dx, -dy)); // dx=sin(az), -dy=cos(az)
            if (bearing < 0) bearing += 360.0;
            double diff = Math.abs(bearing - s.azimuthDeg);
            diff = Math.min(diff, 360.0 - diff);

            boolean inCore  = dist <= CORE_R;
            boolean inWedge = diff <= DB_CELL_WEDGE_HALF_SPREAD_DEG + 8.0; // small tolerance margin
            if (!inCore && !inWedge) continue;

            double effDiff = inCore ? Math.min(diff, 1.0) : diff; // core hits sort ahead of any wedge match
            if (effDiff < bestAngDiff || (effDiff == bestAngDiff && dist < bestDist)) {
                best = i;
                bestAngDiff = effDiff;
                bestDist = dist;
            }
        }
        return best;
    }

    /** Builds a tooltip for a dataset-reported cell site. */
    private String buildDbCellTooltip(DbCellSite s) {
        StringBuilder sb = new StringBuilder();
        sb.append("🗼  FUNKZELLE (aus Datensatz)  —  Kennung ").append(s.label).append("\n");
        sb.append("─".repeat(36)).append("\n");
        row(sb, "Lat",         String.format("%.6f°", s.lat));
        row(sb, "Lon",         String.format("%.6f°", s.lon));
        if (s.azimuthDeg != null) {
            row(sb, "Azimuth", String.format("%.0f° (%s)", s.azimuthDeg, compassAbbrev(s.azimuthDeg)));
        }
        if (s.rangeMetres != null) {
            row(sb, "Reichweite", formatRange(s.rangeMetres) + "  (Schätzung, OpenCelliD/BeaconDB)");
        }
        row(sb, "Datenpunkte", String.valueOf(s.count));

        // Decoded user_location_info (ULI), if any contributing record had
        // a decodable value — same fields/labels as the OpenCelliD tower
        // tooltip (buildTowerTooltip) so both layers read consistently.
        UliDecoder.CellInfo ci = s.cellInfo;
        if (ci != null) {
            sb.append("\n── Zellenidentität (ULI) ────────────\n");
            row(sb, "Typ",  ci.networkType);
            row(sb, "MCC",  String.valueOf(ci.mcc));
            row(sb, "MNC",  String.valueOf(ci.mnc));
            String lacLabel = (ci.networkType != null
                    && (ci.networkType.contains("LTE") || ci.networkType.contains("NR"))) ? "TAC" : "LAC";
            if (ci.lac >= 0)    row(sb, lacLabel,   String.valueOf(ci.lac));
            if (ci.cellId >= 0) row(sb, "Cell-ID", String.valueOf(ci.cellId));
            if (ci.eci >= 0)    row(sb, "ECI",     ci.eci + "  (0x" + Integer.toHexString(ci.eci).toUpperCase() + ")");
        }

        sb.append("\n");
        sb.append("Position des Funkmasts laut Datensatz —\n");
        sb.append("nicht die tatsächliche Geräteposition.");
        return sb.toString().stripTrailing();
    }

    /**
     * Builds a structured multi-line tooltip for a cell tower.
     *
     * <p>Shows all available identity and quality fields in a fixed-width
     * monospace layout so values align vertically.</p>
     */
    private String buildTowerTooltip(CellTower t) {
        // Determine network generation label
        String radio = t.networkType != null ? t.networkType : "?";
        String gen   = radio.contains("LTE") || radio.contains("ECGI") ? "4G/LTE"
                : radio.contains("NR")                             ? "5G/NR"
                : radio.contains("UMTS") || radio.contains("3G")  ? "3G/UMTS"
                : radio.contains("GSM")  || radio.contains("2G")  ? "2G/GSM"
                : radio;

        // Match-type suffix: extract how the tower was found
        String matchType = "";
        if (radio.contains("eNB-centroid"))       matchType = " (eNB-Schwerpunkt)";
        else if (radio.contains("gps-fallback"))  matchType = " (GPS-Nächster)";
        else if (radio.contains("exact"))         matchType = " (Exakt)";

        StringBuilder sb = new StringBuilder();

        // ── Header ───────────────────────────────────────────────────────────
        sb.append("📡  FUNKZELLMAST").append(matchType).append("\n");
        sb.append("─".repeat(36)).append("\n");

        // ── Betreiber ─────────────────────────────────────────────────────────
        if (t.operatorName != null)
            row(sb, "Betreiber",  t.operatorName);
        row(sb, "Netz",       gen);
        row(sb, "Land",       t.country != null ? t.country : "DE");
        sb.append("\n");

        // ── Zellenidentität ───────────────────────────────────────────────────
        sb.append("── Zellenidentität ──────────────────\n");
        row(sb, "MCC",        String.valueOf(t.mcc));
        row(sb, "MNC",        String.valueOf(t.mnc));

        // LAC/TAC hängt von Netztyp ab
        String lacLabel = (radio.contains("LTE") || radio.contains("NR")) ? "TAC" : "LAC";
        if (t.lac >= 0)    row(sb, lacLabel, String.valueOf(t.lac));
        if (t.cellId >= 0) {
            // eNB-centroid: cellId = eNB-ID, CI unbekannt
            if (radio.contains("eNB-centroid"))
                row(sb, "eNB-ID", String.valueOf(t.cellId));
            else
                row(sb, "Cell-ID", t.cellId + (t.eci >= 0
                        ? "  (ECI: " + t.eci + " = 0x" + Integer.toHexString(t.eci).toUpperCase() + ")"
                        : ""));
        }
        sb.append("\n");

        // ── Standort ─────────────────────────────────────────────────────────
        sb.append("── Standort ─────────────────────────\n");
        row(sb, "Lat",  String.format("%.5f°", t.latitude));
        row(sb, "Lon",  String.format("%.5f°", t.longitude));
        if (t.rangeMetres > 0)
            row(sb, "Reichweite", formatRange(t.rangeMetres));
        sb.append("\n");

        // ── Datenqualität ─────────────────────────────────────────────────────
        sb.append("── Datenqualität ───────────────────\n");
        if (t.samples > 0)
            row(sb, "Messungen", t.samples + (t.isReliable ? "  ✓ zuverlässig" : "  ⚠ gering"));
        row(sb, "Quelle", "OpenCelliD");

        return sb.toString().stripTrailing();
    }

    /** Appends a label-value row with fixed label width. */
    private static void row(StringBuilder sb, String label, String value) {
        sb.append(String.format("  %-12s %s%n", label + ":", value));
    }

    /** Formats a range in metres, switching to km above 1000 m. */
    private static String formatRange(int metres) {
        if (metres >= 1000)
            return String.format("~%.1f km", metres / 1000.0);
        return "~" + metres + " m";
    }

    // ── Hover / click overrides to handle both layers ─────────────────────────

    private void onMouseMoved(MouseEvent e) {
        // Check tower layer first (it renders on top)
        int towerHit = cellTowerLayerEnabled ? hitTestTower(e.getX(), e.getY()) : -1;
        if (towerHit != hoveredTower) {
            hoveredTower = towerHit;
            if (!cellTowers.isEmpty()) render();
        }

        int dbCellHit = dbCellLayerEnabled ? hitTestDbCell(e.getX(), e.getY()) : -1;
        if (dbCellHit != hoveredDbCell) {
            hoveredDbCell = dbCellHit;
            if (!dbCellSites.isEmpty()) render();
        }

        int seqHit = !sequenceStops.isEmpty() ? hitTestSeqStop(e.getX(), e.getY()) : -1;
        if (seqHit != hoveredSeqStop) {
            hoveredSeqStop = seqHit;
            if (!sequenceStops.isEmpty()) render();
        }

        int hit = hitTest(e.getX(), e.getY());
        if (hit != hoveredMarker) {
            hoveredMarker = hit;
            canvas.setCursor((hit >= 0 || towerHit >= 0 || dbCellHit >= 0 || seqHit >= 0) ? Cursor.HAND : Cursor.OPEN_HAND);
            render();
        }

        if (towerHit >= 0) {
            tooltip.setText(buildTowerTooltip(cellTowers.get(towerHit)));
            Tooltip.install(canvas, tooltip);
        } else if (seqHit >= 0) {
            tooltip.setText(buildSeqStopTooltip(seqHit));
            Tooltip.install(canvas, tooltip);
        } else if (dbCellHit >= 0) {
            tooltip.setText(buildDbCellTooltip(dbCellSites.get(dbCellHit)));
            Tooltip.install(canvas, tooltip);
        } else if (hit >= 0) {
            tooltip.setText(buildClusterTooltip(clusters.get(hit)));
            Tooltip.install(canvas, tooltip);
        } else {
            Tooltip.uninstall(canvas, tooltip);
        }
    }

    // ── Scroll ────────────────────────────────────────────────────────────────

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

    // ── Hover / click on markers ──────────────────────────────────────────────

    // onMouseMoved is defined in the cell-tower section above.

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
