package fqlite.timemap;

import fqlite.timemap.DataAnalyzer.DataPoint;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.control.*;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.ContextMenuEvent;
import javafx.scene.input.MouseEvent;
import javafx.scene.input.ScrollEvent;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Interactive <b>vertical</b> timeline with subscriber labels and a live filter bar.
 * A toggle above the filter bar switches the label (and the identifier used by
 * the filter, the pick list, the tooltip and the copy action) between
 * IMSI, IMEI and phone number (MSISDN).
 *
 * <h3>Layout (top → bottom)</h3>
 * <pre>
 *  ┌─────────────────────────────┐
 *  │ TIMELINE  ● Timestamp  ● +Geo │  ← header with legend
 *  ├─────────────────────────────┤
 *  │ [IMSI ▼]   [ filter text  ] │  ← filter bar
 *  ├─────────────────────────────┤
 *  │  ←timestamp   ●             │
 *  │             ●  imsi→        │  ← canvas (virtual scrolling)
 *  │  ←timestamp   ●             │
 *  │    …                        │
 *  └─────────────────────────────┘  ← scrollbar on the right
 * </pre>
 *
 * <h3>Each dot shows two lines</h3>
 * <ul>
 *   <li>Line 1: timestamp string (e.g. {@code 09-27 00:06:32})</li>
 *   <li>Line 2: IMSI, rendered in {@link Theme#accent1} colour</li>
 * </ul>
 *
 * <h3>Filter bar</h3>
 * A {@link ComboBox} selects which field to filter on; a {@link TextField}
 * holds the search term. Matching is case-insensitive substring.  The
 * dropdown is pre-populated with all distinct IMSI values found in the
 * data so the user can also pick a subscriber from a list instead of typing.
 *
 * <h3>Context menu</h3>
 * Right-clicking a data point opens a context menu with a single entry to
 * copy that point's IMSI to the system clipboard. Disabled when the point
 * has no IMSI.
 */
public class TimelineView extends BorderPane {

    // ── Layout constants ─────────────────────────────────────────────────────

    /** Fraction of canvas width at which the central axis is drawn. */
    private static final double AXIS_X_FRAC    = 0.38;
    private static final double PADDING_TOP    = 40;
    private static final double PADDING_BOTTOM = 40;
    private static final double DOT_R          = 6;
    /** Minimum logical pixels between adjacent dot centres. */
    private static final double MIN_DOT_SPACING = 46;   // slightly more room for two label lines
    /** Horizontal distance from axis to dot centre. */
    private static final double DOT_OFFSET_X   = 54;

    // ── State ────────────────────────────────────────────────────────────────

    private Theme theme = Theme.DARK;

    private final Canvas    canvas;
    private final Label     titleLabel;
    private final Label     legend1;
    private final Label     legend2;
    private final HBox      header;
    private final ScrollBar vScrollBar;
    private final StackPane canvasWrapper;
    private final Tooltip   tooltip = new Tooltip();

    // Filter bar widgets
    private final ComboBox<String> filterField;  // which field to search in
    private final TextField        filterText;   // search term
    private final Label            filterCount;  // "42 / 128 points"

    /** All points that have a timestamp (unfiltered). */
    private List<DataPoint> allTimestamped = new ArrayList<>();
    /** Currently visible subset after applying the active filter. */
    private List<DataPoint> visible = new ArrayList<>();

    private Instant minTs, maxTs;

    private int hoveredIdx  = -1;
    private int selectedIdx = -1;

    private Consumer<DataPoint>       selectionListener;
    /**
     * Called whenever the active filter changes, with the current filtered
     * list of {@link DataPoint}s. {@link MapViewPane} uses this to keep the
     * map in sync with the timeline filter.
     */
    private Consumer<List<DataPoint>> filterListener;

    private double virtualHeight = 600;
    private double scrollOffset  = 0;

    /** Screen coordinates of each dot (for hit testing). Parallel to {@link #visible}. */
    private double[] dotX;
    private double[] dotY;

    /**
     * Logical (pre-scroll) Y position of each point in {@link #visible},
     * indexed the same way as {@code visible} itself (i.e. {@code
     * dotLogicalYCache[i]} belongs to {@code visible.get(i)}).
     *
     * <p>Computed once per {@link #updateVirtualHeight()} call rather than
     * from a plain time-fraction formula at render time: when two records
     * are far apart in time, a pure proportional mapping squeezes every
     * point that falls in between into a near-identical Y position (they
     * render on top of each other). This cache instead walks the points in
     * timestamp order and pushes any point that would land closer than
     * {@link #MIN_DOT_SPACING} to its temporal predecessor further down,
     * so points stay visually distinguishable regardless of how the
     * overall time range is distributed.</p>
     */
    private double[] dotLogicalYCache = new double[0];

    // ── Filter field options ─────────────────────────────────────────────────

    private static final String FF_TABLE     = "Table";
    private static final String FF_TIMESTAMP = "Timestamp";
    private static final String FF_ALL       = "All fields";

    // ── Identifier mode (IMSI / IMEI / phone number) ──────────────────────────

    /** Which subscriber identifier is shown under each dot and used for filtering. */
    public enum IdMode {
        IMSI("IMSI", "IMSI", "IMSI"),
        IMEI("IMEI", "IMEI", "IMEI"),
        MSISDN("MSISDN", "Rufnummer", "Number");

        /** Language-neutral name, used as filter-field entry. */
        final String filterLabel;
        final String labelDe, labelEn;
        IdMode(String filterLabel, String labelDe, String labelEn) {
            this.filterLabel = filterLabel; this.labelDe = labelDe; this.labelEn = labelEn;
        }
        String uiLabel() { return Lang.t(labelDe, labelEn); }
    }

    private IdMode idMode = IdMode.IMSI;
    private final ToggleGroup  idGroup = new ToggleGroup();
    private final ToggleButton idImsiBtn   = new ToggleButton();
    private final ToggleButton idImeiBtn   = new ToggleButton();
    private final ToggleButton idMsisdnBtn = new ToggleButton();
    private final HBox         idBar;
    private final Label        idBarLabel = new Label();
    /** Last full data set passed to {@link #setData}, needed to rebuild the pick list. */
    private List<DataPoint> allPointsCache = new ArrayList<>();

    /** Identifier of {@code dp} according to the active {@link IdMode}, or {@code null}. */
    private String idOf(DataPoint dp) {
        return switch (idMode) {
            case IMSI   -> dp.getImsi();
            case IMEI   -> dp.getImei();
            case MSISDN -> dp.getMsisdn();
        };
    }

    /** Filter-field entry for the active identifier ("IMSI", "IMEI" or "MSISDN"). */
    private String ffId() { return idMode.filterLabel; }

    // ── Context menu ─────────────────────────────────────────────────────────

    private final ContextMenu contextMenu  = new ContextMenu();
    private final MenuItem    copyImsiItem = new MenuItem("IMSI kopieren");   // text follows idMode
    /** Index (into {@link #visible}) the context menu was last opened for. */
    private int contextMenuIdx = -1;

    // ── Construction ─────────────────────────────────────────────────────────

    public TimelineView() {

        // Header row
        titleLabel = new Label(Lang.t("ZEITLEISTE", "TIMELINE"));
        Lang.addListener(() -> titleLabel.setText(Lang.t("ZEITLEISTE", "TIMELINE")));
        titleLabel.setFont(Font.font("Monospace", FontWeight.BOLD, 17));
        titleLabel.setPadding(new Insets(10, 12, 6, 12));

        legend1 = new Label();
        legend2 = new Label();

        header = new HBox(12, titleLabel, legend1, legend2);
        header.setAlignment(Pos.CENTER_LEFT);
        setTop(header);

        // Filter bar
        filterField = new ComboBox<>();
        filterField.getItems().addAll(ffId(), FF_TABLE, FF_TIMESTAMP, FF_ALL);
        filterField.setValue(ffId());
        filterField.setMaxWidth(110);
        filterField.setStyle("-fx-font-family: monospace; -fx-font-size: 14px;");

        filterText = new TextField();
        filterText.setPromptText("Filter…");
        filterText.setStyle("-fx-font-family: monospace; -fx-font-size: 14px;");
        HBox.setHgrow(filterText, Priority.ALWAYS);

        Button clearBtn = new Button("\u2715");
        clearBtn.setFont(Font.font("Monospace", 14));
        Tooltip clearBtnTooltip = new Tooltip(Lang.t("Filter leeren", "Clear filter"));
        clearBtn.setTooltip(clearBtnTooltip);
        Lang.addListener(() -> clearBtnTooltip.setText(Lang.t("Filter leeren", "Clear filter")));
        clearBtn.setOnAction(e -> filterText.clear());

        filterCount = new Label();
        filterCount.setFont(Font.font("Monospace", 13));

        HBox filterBar = new HBox(6, filterField, filterText, clearBtn, filterCount);
        filterBar.setAlignment(Pos.CENTER_LEFT);
        filterBar.setPadding(new Insets(5, 8, 5, 8));

        // Identifier toggle: IMSI | IMEI | Rufnummer
        idImsiBtn.setToggleGroup(idGroup);
        idImeiBtn.setToggleGroup(idGroup);
        idMsisdnBtn.setToggleGroup(idGroup);
        idImsiBtn.setUserData(IdMode.IMSI);
        idImeiBtn.setUserData(IdMode.IMEI);
        idMsisdnBtn.setUserData(IdMode.MSISDN);
        idImsiBtn.setSelected(true);
        updateIdBarTexts();
        Lang.addListener(this::updateIdBarTexts);
        idGroup.selectedToggleProperty().addListener((obs, o, n) -> {
            if (n == null) { idGroup.selectToggle(o); return; }   // always keep one selected
            setIdMode((IdMode) n.getUserData());
        });
        idBarLabel.setFont(Font.font("Monospace", 13));
        idBar = new HBox(0, idBarLabel, idImsiBtn, idImeiBtn, idMsisdnBtn);
        HBox.setMargin(idBarLabel, new Insets(0, 8, 0, 0));
        idBar.setAlignment(Pos.CENTER_LEFT);
        idBar.setPadding(new Insets(5, 8, 0, 8));

        // Canvas
        canvas = new Canvas(240, 600);
        canvasWrapper = new StackPane(canvas);
        canvas.widthProperty().bind(canvasWrapper.widthProperty());
        canvas.heightProperty().bind(canvasWrapper.heightProperty());
        canvas.widthProperty().addListener(e -> render());
        canvas.heightProperty().addListener(e -> { updateVirtualHeight(); render(); });

        // Let the SplitPane shrink this pane freely.
        canvasWrapper.setMinSize(0, 0);
        setMinSize(0, 0);

        // Vertical scrollbar
        vScrollBar = new ScrollBar();
        vScrollBar.setOrientation(javafx.geometry.Orientation.VERTICAL);
        vScrollBar.setMin(0);
        vScrollBar.setMax(0);
        vScrollBar.setValue(0);
        vScrollBar.setVisibleAmount(600);
        vScrollBar.setUnitIncrement(MIN_DOT_SPACING);
        vScrollBar.setBlockIncrement(300);
        vScrollBar.valueProperty().addListener((obs, o, n) -> {
            scrollOffset = n.doubleValue();
            render();
        });

        // Layout assembly: filterBar on top, then canvas+scrollbar
        BorderPane inner = new BorderPane();
        inner.setTop(new VBox(idBar, filterBar));
        inner.setCenter(canvasWrapper);
        inner.setRight(vScrollBar);
        setCenter(inner);

        // Mouse handlers
        canvas.setOnMouseMoved(this::onMouseMoved);
        canvas.setOnMouseClicked(this::onMouseClicked);
        canvas.setOnMouseExited(e -> { hoveredIdx = -1; render(); });
        canvas.setOnScroll(this::onScroll);
        canvas.setOnContextMenuRequested(this::onContextMenuRequested);

        // Right-click context menu: copy the IMSI of the clicked data point.
        copyImsiItem.setOnAction(e -> copySelectedImsi());
        contextMenu.getItems().add(copyImsiItem);

        // Filter listeners – re-apply filter on any change
        filterField.valueProperty().addListener((obs, o, n) -> applyFilter());
        filterText.textProperty().addListener((obs, o, n) -> applyFilter());

        applyTheme(Theme.DARK);
    }

    // ── Public API ───────────────────────────────────────────────────────────

    public void setData(List<DataPoint> allPoints) {
        this.allTimestamped = allPoints.stream()
                .filter(p -> p.getTimestamp() != null)
                .collect(Collectors.toCollection(ArrayList::new));

        // Populate identifier quick-pick list in the ComboBox
        this.allPointsCache = allPoints;
        populateImsiPicker(allPoints);

        hoveredIdx   = -1;
        selectedIdx  = -1;
        scrollOffset = 0;
        applyFilter();   // sets visible, minTs/maxTs, virtualHeight, renders
    }

    /**
     * Switches the identifier shown under each dot (and used by filter, pick
     * list, tooltip and copy action) between IMSI, IMEI and phone number.
     */
    public void setIdMode(IdMode mode) {
        if (mode == null || mode == idMode) return;
        idMode = mode;
        for (Toggle t : idGroup.getToggles()) {
            if (t.getUserData() == mode && !t.isSelected()) idGroup.selectToggle(t);
        }
        styleIdButtons();
        // Rebuild pick list for the new identifier and fall back to the plain id filter
        filterField.setValue(null);
        populateImsiPicker(allPointsCache);
        filterField.setValue(ffId());
        applyFilter();
    }

    public IdMode getIdMode() { return idMode; }

    private void updateIdBarTexts() {
        idBarLabel.setText(Lang.t("Anzeige:", "Label:"));
        idImsiBtn.setText(IdMode.IMSI.uiLabel());
        idImeiBtn.setText(IdMode.IMEI.uiLabel());
        idMsisdnBtn.setText(IdMode.MSISDN.uiLabel());
    }

    private void styleIdButtons() {
        if (theme == null) return;
        ToggleButton[] btns = {idImsiBtn, idImeiBtn, idMsisdnBtn};
        for (int i = 0; i < btns.length; i++) {
            boolean sel = btns[i].isSelected();
            String radius = i == 0 ? "3 0 0 3" : i == btns.length - 1 ? "0 3 3 0" : "0";
            btns[i].setStyle(
                    "-fx-font-family: monospace; -fx-font-size: 13px; -fx-padding: 2 10 2 10;" +
                    "-fx-background-color: " + Theme.hex(sel ? theme.accent1 : theme.bgAlt) + ";" +
                    "-fx-text-fill: " + Theme.hex(sel ? theme.bg : theme.labelStrong) + ";" +
                    "-fx-border-color: " + Theme.hex(theme.border) + ";" +
                    "-fx-background-radius: " + radius + "; -fx-border-radius: " + radius + ";");
        }
    }

    public void setSelectionListener(Consumer<DataPoint> listener) {
        this.selectionListener = listener;
    }

    /**
     * Registers a callback that is invoked whenever the filter changes.
     * The argument is the current filtered list (may be all points when no
     * filter is active).
     */
    public void setFilterListener(Consumer<List<DataPoint>> listener) {
        this.filterListener = listener;
    }

    /**
     * Programmatically selects a dot (e.g. triggered by a map marker click)
     * and scrolls it into view. If the point is currently filtered out, the
     * filter is cleared first.
     */
    public void selectPoint(DataPoint dp) {
        // If dp is not in the current visible list, clear the filter so it appears.
        if (!visible.contains(dp) && allTimestamped.contains(dp)) {
            filterText.clear();          // triggers applyFilter() via listener
        }
        int idx = visible.indexOf(dp);
        if (idx < 0) return;
        selectedIdx = idx;
        scrollToIndex(idx);
        render();
    }

    public void applyTheme(Theme t) {
        this.theme = t;
        setStyle(t.bgStyle());
        header.setStyle(t.bgStyle() + t.borderBottomStyle());
        canvasWrapper.setStyle(t.bgStyle());
        vScrollBar.setStyle("-fx-background-color: " + Theme.hex(t.bg) + ";");
        titleLabel.setTextFill(t.accent1);
        filterCount.setTextFill(t.label);

        String inputCss =
                "-fx-font-family: monospace; -fx-font-size: 14px;" +
                "-fx-background-color: " + Theme.hex(t.bgAlt) + ";" +
                "-fx-text-fill: " + Theme.hex(t.labelStrong) + ";" +
                "-fx-border-color: " + Theme.hex(t.border) + ";" +
                "-fx-border-radius: 3; -fx-background-radius: 3;";
        filterText.setStyle(inputCss);
        filterField.setStyle(inputCss);

        // Style the filter bar background
        // (inner BorderPane top node)
        if (getCenter() instanceof BorderPane inner && inner.getTop() instanceof VBox top) {
            top.setStyle(t.bgStyle() + t.borderBottomStyle());
        }
        idBarLabel.setTextFill(t.label);
        styleIdButtons();

        rebuildLegend();
        tooltip.setStyle(
                "-fx-font-family: monospace; -fx-font-size: 14px;" +
                "-fx-background-color: " + Theme.hex(t.bgAlt) + ";" +
                "-fx-text-fill: " + Theme.hex(t.labelStrong) + ";" +
                "-fx-border-color: " + Theme.hex(t.border) + "; -fx-border-width: 1px;");
        render();
    }

    // ── Filtering ────────────────────────────────────────────────────────────

    /**
     * Fills the ComboBox with the fixed field names plus all distinct IMSI
     * values found in the dataset, so users can select a subscriber directly.
     */
    private void populateImsiPicker(List<DataPoint> allPoints) {
        List<String> items = new ArrayList<>(List.of(ffId(), FF_TABLE, FF_TIMESTAMP, FF_ALL));
        // Separator-style entry
        items.add("── Pick " + ffId() + " ──");
        allPoints.stream()
                .map(this::idOf)
                .filter(m -> m != null && !m.isBlank())
                .distinct()
                .sorted()
                .forEach(items::add);

        String current = filterField.getValue();
        filterField.getItems().setAll(items);
        // Restore selection if still valid, else default to the identifier filter
        filterField.setValue(items.contains(current) ? current : ffId());
    }

    /**
     * Applies the active filter to {@link #allTimestamped} and stores the
     * result in {@link #visible}.  Also recalculates min/maxTs and triggers
     * a full re-render.
     */
    private void applyFilter() {
        String field = filterField.getValue();
        String term  = filterText.getText().trim().toLowerCase();

        List<DataPoint> filtered;

        // If a specific IMSI was picked from the dropdown list, filter by it directly.
        boolean isImsiPickValue = field != null
                                    && !field.equals(ffId()) && !field.equals(FF_TABLE)
                                    && !field.equals(FF_TIMESTAMP) && !field.equals(FF_ALL)
                                    && !field.startsWith("──");

        if (isImsiPickValue) {
            // User chose a specific IMSI from the pick list
            final String picked = field;
            filtered = allTimestamped.stream()
                    .filter(p -> picked.equals(idOf(p)))
                    .toList();
        } else if (term.isEmpty()) {
            filtered = allTimestamped;
        } else {
            filtered = allTimestamped.stream().filter(p -> {
                String f = field == null ? FF_ALL : field;
                if (f.equals(ffId())) {
                    String m = idOf(p);
                    return m != null && m.toLowerCase().contains(term);
                }
                return switch (f) {
                    case FF_TABLE -> p.getTableName().toLowerCase().contains(term);
                    case FF_TIMESTAMP -> p.getFormattedTimestamp().toLowerCase().contains(term);
                    default -> {  // FF_ALL
                        String m = idOf(p);
                        yield p.getTableName().toLowerCase().contains(term)
                              || p.getFormattedTimestamp().toLowerCase().contains(term)
                              || (m != null && m.toLowerCase().contains(term));
                    }
                };
            }).toList();
        }

        this.visible = filtered;

        if (visible.isEmpty()) {
            minTs = maxTs = null;
        } else {
            minTs = visible.stream().map(DataPoint::getTimestamp).min(Instant::compareTo).orElse(null);
            maxTs = visible.stream().map(DataPoint::getTimestamp).max(Instant::compareTo).orElse(null);
        }

        // Keep selection valid
        if (selectedIdx >= visible.size()) selectedIdx = -1;
        hoveredIdx = -1;
        contextMenuIdx = -1;
        scrollOffset = 0;

        // Update count label
        filterCount.setText(visible.size() + " / " + allTimestamped.size());

        updateVirtualHeight();

        // Notify map (and any other listener) about the new filtered set.
        if (filterListener != null) filterListener.accept(List.copyOf(this.visible));

        render();
    }

    // ── Virtual scrolling ────────────────────────────────────────────────────

    private void updateVirtualHeight() {
        double viewH = canvas.getHeight() > 0 ? canvas.getHeight() : 600;
        int    n     = visible.size();

        if (n == 0 || minTs == null) {
            virtualHeight    = viewH;
            dotLogicalYCache = new double[0];
        } else {
            // Lower bound on the available drawing height: enough room for
            // every point to get at least MIN_DOT_SPACING if they were
            // evenly distributed (matches the previous, count-only formula).
            double countBasedNeeded = PADDING_TOP + PADDING_BOTTOM + (long) n * MIN_DOT_SPACING;
            double baseHeight       = Math.max(viewH, countBasedNeeded);

            long minMs   = minTs.toEpochMilli();
            long rangeMs = maxTs.toEpochMilli() - minMs;

            // Process points in timestamp order so a minimum pixel gap can
            // be enforced between temporally adjacent points -- otherwise,
            // if two records are far apart in time, the proportional
            // mapping below squeezes any points in between to (almost) the
            // same Y and they render on top of each other.
            Integer[] order = new Integer[n];
            for (int i = 0; i < n; i++) order[i] = i;
            Arrays.sort(order, Comparator.comparing(i -> visible.get(i).getTimestamp()));

            double[] y = new double[n];
            double prevY = Double.NEGATIVE_INFINITY;
            for (int k = 0; k < n; k++) {
                int idx = order[k];
                double frac = rangeMs == 0 ? 0.5
                        : (double) (visible.get(idx).getTimestamp().toEpochMilli() - minMs) / rangeMs;
                double idealY = PADDING_TOP + (baseHeight - PADDING_TOP - PADDING_BOTTOM) * frac;
                double yy = (k == 0) ? idealY : Math.max(idealY, prevY + MIN_DOT_SPACING);
                y[idx] = yy;
                prevY  = yy;
            }

            dotLogicalYCache = y;
            virtualHeight    = Math.max(baseHeight, prevY + PADDING_BOTTOM);
        }

        double maxScroll = Math.max(0, virtualHeight - viewH);
        scrollOffset = Math.min(scrollOffset, maxScroll);

        vScrollBar.setMax(maxScroll);
        vScrollBar.setVisibleAmount(viewH);
        vScrollBar.setValue(scrollOffset);
    }

    private double toScreen(double logicalY) { return logicalY - scrollOffset; }

    private void scrollToIndex(int idx) {
        double viewH    = canvas.getHeight();
        double logicalY = dotLogicalY(idx);
        double screenY  = logicalY - scrollOffset;
        if (screenY < DOT_R * 3)
            scrollOffset = Math.max(0, logicalY - DOT_R * 3);
        else if (screenY > viewH - DOT_R * 3)
            scrollOffset = Math.min(virtualHeight - viewH, logicalY - viewH + DOT_R * 3);
        vScrollBar.setValue(scrollOffset);
    }

    /** Pre-computed, collision-resolved logical Y position for {@code visible.get(i)}. */
    private double dotLogicalY(int i) {
        if (dotLogicalYCache == null || i < 0 || i >= dotLogicalYCache.length) return PADDING_TOP;
        return dotLogicalYCache[i];
    }

    // ── Rendering ────────────────────────────────────────────────────────────

    private void render() {
        GraphicsContext gc = canvas.getGraphicsContext2D();
        double W     = canvas.getWidth();
        double H     = canvas.getHeight();
        double axisX = W * AXIS_X_FRAC;

        gc.setFill(theme.bg);
        gc.fillRect(0, 0, W, H);

        if (visible.isEmpty()) {
            gc.setFill(theme.label);
            gc.setFont(Font.font("Monospace", 14));
            String msg = allTimestamped.isEmpty()
                    ? "No timestamps found."
                    : "No matches for current filter.";
            gc.fillText(msg, 10, H / 2);
            return;
        }

        drawGrid(gc, W, H);
        drawAxis(gc, H, axisX);
        drawTicks(gc, W, H, axisX);
        drawDots(gc, W, H, axisX);
    }

    private void drawGrid(GraphicsContext gc, double W, double H) {
        gc.setStroke(theme.grid);
        gc.setLineWidth(0.5);
        int ticks = numTicks(H);
        for (int i = 0; i <= ticks; i++) {
            double scrY = toScreen(PADDING_TOP + (virtualHeight - PADDING_TOP - PADDING_BOTTOM) * i / ticks);
            if (scrY < 0 || scrY > H) continue;
            gc.strokeLine(0, scrY, W, scrY);
        }
    }

    private void drawAxis(GraphicsContext gc, double H, double axisX) {
        gc.setStroke(theme.axis);
        gc.setLineWidth(2);
        double axisStart = Math.max(0,  toScreen(PADDING_TOP));
        double axisEnd   = Math.min(H,  toScreen(virtualHeight - PADDING_BOTTOM));
        if (axisEnd > axisStart)
            gc.strokeLine(axisX, axisStart, axisX, axisEnd);

        double arrowY = toScreen(virtualHeight - PADDING_BOTTOM);
        if (arrowY > 0 && arrowY <= H + 10) {
            gc.setFill(theme.axis);
            gc.fillPolygon(
                    new double[]{axisX - 5, axisX, axisX + 5},
                    new double[]{arrowY,    arrowY + 10, arrowY}, 3);
        }
    }

    private void drawTicks(GraphicsContext gc, double W, double H, double axisX) {
        if (minTs == null) return;
        long rangeMs = maxTs.toEpochMilli() - minTs.toEpochMilli();
        int  ticks   = numTicks(H);
        DateTimeFormatter fmt = chooseFmt(rangeMs);

        gc.setStroke(theme.tick);
        gc.setFill(theme.label);
        gc.setFont(Font.font("Monospace", 12));
        gc.setLineWidth(1);

        for (int i = 0; i <= ticks; i++) {
            double scrY = toScreen(PADDING_TOP + (virtualHeight - PADDING_TOP - PADDING_BOTTOM) * i / ticks);
            if (scrY < 0 || scrY > H) continue;
            long ms = minTs.toEpochMilli() + rangeMs * i / ticks;
            gc.strokeLine(axisX - 4, scrY, axisX + 4, scrY);
            gc.fillText(fmt.format(Instant.ofEpochMilli(ms).atZone(ZoneOffset.UTC)), axisX + 7, scrY + 4);
        }
    }

    private void drawDots(GraphicsContext gc, double W, double H, double axisX) {
        if (visible.isEmpty()) {
            // Drop any stale hit-test arrays from a previous (larger) data set —
            // otherwise hitTest() can still return an index that's valid against
            // these old arrays but out of bounds for the now-empty `visible` list,
            // crashing onMouseMoved()/onMouseClicked() with an IndexOutOfBoundsException.
            dotX = new double[0];
            dotY = new double[0];
            return;
        }

        dotX = new double[visible.size()];
        dotY = new double[visible.size()];

        for (int i = 0; i < visible.size(); i++) {
            DataPoint dp   = visible.get(i);
            double scrY    = toScreen(dotLogicalY(i));
            boolean left   = (i % 2 == 0);
            double  scrX   = axisX + (left ? -DOT_OFFSET_X : DOT_OFFSET_X);

            dotX[i] = scrX;
            dotY[i] = scrY;

            if (scrY < -DOT_R * 6 || scrY > H + DOT_R * 6) continue;

            // Connector line axis → dot
            gc.setStroke(theme.connector);
            gc.setLineWidth(1);
            gc.strokeLine(axisX, scrY, scrX, scrY);

            // Dot colour and radius
            Color  fill;
            double r = DOT_R;
            if (i == selectedIdx)     { fill = theme.selected; r = DOT_R * 1.5; }
            else if (i == hoveredIdx) { fill = theme.hover;    r = DOT_R * 1.4; }
            else { fill = dp.getCoordinate() != null ? theme.accent2 : theme.accent1; }

            // Glow ring
            if (i == hoveredIdx || i == selectedIdx) {
                gc.setFill(fill.deriveColor(0, 1, 1, 0.25));
                gc.fillOval(scrX - r * 2.5, scrY - r * 2.5, r * 5, r * 5);
            }

            gc.setFill(fill);
            gc.fillOval(scrX - r, scrY - r, r * 2, r * 2);
            gc.setStroke(fill.brighter());
            gc.setLineWidth(1);
            gc.strokeOval(scrX - r, scrY - r, r * 2, r * 2);

            // ── Labels ───────────────────────────────────────────────────────
            // Line 1: timestamp  (grey)
            String timeStr   = DateTimeFormatter.ofPattern("MM-dd HH:mm:ss")
                    .withZone(ZoneOffset.UTC).format(dp.getTimestamp());
            // Line 2: IMSI / IMEI / phone number (accent colour, see idMode); empty when unavailable
            String idVal   = idOf(dp);
            String imsiStr = idVal != null ? idVal : "";

            gc.setFont(Font.font("Monospace", 12));

            // Vertical anchor: centre between the two text lines
            double lineH   = 11;           // approximate line height at size 9
            double textTop = scrY - lineH / 2.0 + 1;

            if (left) {
                // Left-side dots: labels to the LEFT of the dot, right-aligned
                double twTime = timeStr.length() * 5.5;
                double twImsi = imsiStr.length() * 5.5;
                double textX  = scrX - DOT_R - 4;

                gc.setFill(theme.label);
                gc.fillText(timeStr, textX - twTime, textTop);
                if (!imsiStr.isEmpty()) {
                    gc.setFill(theme.accent1);
                    gc.fillText(imsiStr, textX - twImsi, textTop + lineH);
                }
            } else {
                // Right-side dots: labels to the RIGHT of the dot, left-aligned
                double textX = scrX + DOT_R + 4;

                gc.setFill(theme.label);
                gc.fillText(timeStr, textX, textTop);
                if (!imsiStr.isEmpty()) {
                    gc.setFill(theme.accent1);
                    gc.fillText(imsiStr, textX, textTop + lineH);
                }
            }
        }
    }

    // ── Mouse / scroll ───────────────────────────────────────────────────────

    private void onMouseMoved(MouseEvent e) {
        int hit = hitTest(e.getX(), e.getY());
        if (hit >= visible.size()) hit = -1;   // guard against a stale hit-test index
        if (hit != hoveredIdx) { hoveredIdx = hit; render(); }
        if (hit >= 0) {
            tooltip.setText(buildTooltip(visible.get(hit)));
            Tooltip.install(canvas, tooltip);
        } else {
            Tooltip.uninstall(canvas, tooltip);
        }
    }

    private void onMouseClicked(MouseEvent e) {
        int hit = hitTest(e.getX(), e.getY());
        if (hit >= 0) {
            selectedIdx = hit;
            render();
            if (selectionListener != null) selectionListener.accept(visible.get(hit));
        }
    }

    /**
     * Right-click on a data point: select it, remember its index, and show a
     * context menu offering to copy its IMSI. The menu item is disabled when
     * the point has no IMSI. Right-clicking empty canvas space shows nothing.
     */
    private void onContextMenuRequested(ContextMenuEvent e) {
        int hit = hitTest(e.getX(), e.getY());
        if (hit < 0) {
            contextMenu.hide();
            return;
        }

        contextMenuIdx = hit;
        selectedIdx = hit;
        render();

        String imsi = idOf(visible.get(hit));
        String name = idMode.uiLabel();
        copyImsiItem.setDisable(imsi == null);
        copyImsiItem.setText(imsi != null
                ? Lang.t(name + " kopieren (", "Copy " + name + " (") + imsi + ")"
                : Lang.t(name + " kopieren (nicht verfügbar)", "Copy " + name + " (not available)"));

        contextMenu.show(canvas, e.getScreenX(), e.getScreenY());
    }

    /** Copies the IMSI of the data point the context menu was opened for. */
    private void copySelectedImsi() {
        if (contextMenuIdx < 0 || contextMenuIdx >= visible.size()) return;
        String imsi = idOf(visible.get(contextMenuIdx));
        if (imsi == null) return;
        ClipboardContent content = new ClipboardContent();
        content.putString(imsi);
        Clipboard.getSystemClipboard().setContent(content);
    }

    private void onScroll(ScrollEvent e) {
        double delta     = e.getDeltaY() != 0 ? -e.getDeltaY() : -e.getDeltaX();
        double maxScroll = Math.max(0, virtualHeight - canvas.getHeight());
        scrollOffset = Math.clamp(scrollOffset + delta, 0, maxScroll);
        vScrollBar.setValue(scrollOffset);
        e.consume();
    }

    private int hitTest(double mx, double my) {
        if (dotX == null) return -1;
        for (int i = 0; i < dotX.length; i++) {
            double dx = mx - dotX[i], dy = my - dotY[i];
            if (Math.sqrt(dx * dx + dy * dy) <= DOT_R * 2.5) return i;
        }
        return -1;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private int numTicks(double viewH) { return Math.max(2, (int)(viewH / 80)); }

    private DateTimeFormatter chooseFmt(long rangeMs) {
        if (rangeMs < 2L * 60_000)       return DateTimeFormatter.ofPattern("HH:mm:ss");
        if (rangeMs < 2L * 3_600_000)    return DateTimeFormatter.ofPattern("HH:mm");
        if (rangeMs < 2L * 86_400_000)   return DateTimeFormatter.ofPattern("MM-dd HH:mm");
        if (rangeMs < 365L * 86_400_000) return DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return DateTimeFormatter.ofPattern("yyyy-MM");
    }

    private String buildTooltip(DataPoint dp) {
        StringBuilder sb = new StringBuilder();
        sb.append("Table: ").append(dp.getTableName())
                .append("  (row ").append(dp.getRowIndex()).append(")\n");
        sb.append("Time:  ").append(dp.getFormattedTimestamp()).append(" UTC\n");
        String imsi = idOf(dp);
        if (imsi != null) sb.append(idMode.filterLabel).append(":  ").append(imsi).append("\n");
        if (dp.getCoordinate() != null) sb.append("Geo:   ").append(dp.getCoordinate()).append("\n");
        List<String> cols = dp.getColumnNames();
        List<String> row  = dp.getRawRow();
        for (int i = 0; i < Math.min(cols.size(), row.size()); i++) {
            String v = row.get(i);
            if (v != null && !v.isBlank()) sb.append(cols.get(i)).append(": ").append(v).append("\n");
        }
        return sb.toString().trim();
    }

    private void rebuildLegend() {
        buildLegendDot(legend1, theme.accent1, "Timestamp");
        buildLegendDot(legend2, theme.accent2, "Timestamp + Geo");
    }

    private void buildLegendDot(Label lbl, Color c, String text) {
        Canvas dot = new Canvas(10, 10);
        dot.getGraphicsContext2D().setFill(c);
        dot.getGraphicsContext2D().fillOval(0, 0, 10, 10);
        lbl.setGraphic(dot);
        lbl.setText(text);
        lbl.setTextFill(theme.label);
        lbl.setFont(Font.font("Monospace", 13));
        lbl.setGraphicTextGap(5);
    }
}
