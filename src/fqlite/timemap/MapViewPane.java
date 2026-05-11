package fqlite.timemap;

import fqlite.base.ThemeManager;
import fqlite.timemap.DataAnalyzer.DataPoint;
import fqlite.timemap.DataAnalyzer.ResponseRecordDataPoint;
import javafx.application.Platform;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.scene.text.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Root panel for the geo/time analysis feature.
 *
 * <h3>Layout</h3>
 * <pre>
 * ┌─────────────────────────────────────────────────────────────┐
 * │  Header bar                                                 │
 * ├──────────────────┬───────────────────────┬──────────────────┤
 * │  TimelineView    │  MapView              │  DetailPane      │
 * │  vertical,       │  fills remaining      │  scrollable      │
 * │  ~260 px wide    │  space                │  metadata        │
 * └──────────────────┴───────────────────────┴──────────────────┘
 * </pre>
 *
 * <h3>Interaction</h3>
 * <ul>
 *   <li>Timeline dot clicked → map centres on that point and highlights
 *       the matching marker; detail panel shows the row data.</li>
 *   <li>Map marker clicked → detail panel updates; timeline scrolls to
 *       that dot and highlights it.</li>
 * </ul>
 */
public class MapViewPane extends BorderPane {

    // Sub-views
    private final TimelineView timelineView = new TimelineView();
    private final MapView      mapView      = new MapView();
    private final DetailPane   detailPane   = new DetailPane();

    // State
    private final DataAnalyzer    analyzer  = new DataAnalyzer();
    private       Theme           theme     = Theme.DARK;
    private       List<DataPoint> allPoints       = List.of();
    /** The map's current point set (may be filtered by the timeline filter). */
    private       List<DataPoint> currentMapPoints = List.of();

    // Header
    private final Label appTitle;
    private final Label subtitle;
    private final HBox  titleBox;

    // Construction
    public MapViewPane() {
        appTitle = new Label("\u2B21 GEO\u00B7TIME ANALYZER");
        appTitle.setFont(Font.font("Monospace", FontWeight.BOLD, 15));

        subtitle = new Label("Timestamps & geo-coordinates from SQLite tables");
        subtitle.setFont(Font.font("Monospace", 11));

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        titleBox = new HBox(16, appTitle, subtitle, spacer);
        titleBox.setAlignment(Pos.CENTER_LEFT);
        titleBox.setPadding(new Insets(14, 20, 14, 20));
        setTop(titleBox);

        // ── Three-column layout via SplitPane ────────────────────────────────
        //
        // SplitPane respects minWidth on all children and allows the user to
        // drag the dividers freely. Initial positions are set as fractions;
        // after the first layout pass the dividers become fully interactive.

        timelineView.setMinWidth(210);
        timelineView.setPrefWidth(270);

        detailPane.setMinWidth(180);
        detailPane.setPrefWidth(220);

        SplitPane splitPane = new SplitPane(timelineView, mapView, detailPane);
        splitPane.setOrientation(javafx.geometry.Orientation.HORIZONTAL);

        // Set initial divider positions (fractions 0..1).
        // 270 / 1280 ≈ 0.21  |  1 - 220/1280 ≈ 0.83  (assumes ~1280 px start width)
        splitPane.setDividerPositions(0.21, 0.83);

        setCenter(splitPane);

        // Wire selection events:
        // Timeline dot → centre map + update detail
        timelineView.setSelectionListener(this::onTimelinePointSelected);
        // Map marker → update detail + scroll timeline
        mapView.setSelectionListener(this::onMapPointSelected);
        // Filter change in timeline → update map with same filtered set
        timelineView.setFilterListener(filtered -> {
            currentMapPoints = filtered;
            mapView.setData(filtered);
        });

        if (ThemeManager.isDark()) setTheme(Theme.DARK);
        else                       setTheme(Theme.LIGHT);

        applyThemeToSelf();
    }

    // Public API
    public void setData(
            ConcurrentHashMap<String, ObservableList<ObservableList<String>>> resultlist,
            Map<String, List<String>> headers) {

        if (!Platform.isFxApplicationThread()) {
            Platform.runLater(() -> setData(resultlist, headers));
            return;
        }

        Map<String, List<String>> safeHeaders = new HashMap<>(headers);
        for (Map.Entry<String, ObservableList<ObservableList<String>>> e : resultlist.entrySet()) {
            String key = e.getKey();
            if (!safeHeaders.containsKey(key)) {
                int colCount = e.getValue().isEmpty() ? 0 : e.getValue().get(0).size();
                List<String> auto = new ArrayList<>();
                for (int i = 0; i < colCount; i++) auto.add("Col_" + i);
                safeHeaders.put(key, auto);
            }
        }

        allPoints        = analyzer.analyze(resultlist, safeHeaders);
        currentMapPoints = allPoints;
        timelineView.setData(allPoints);
        mapView.setData(allPoints);
        detailPane.clear(theme);
    }

    public void setData(
            ConcurrentHashMap<String, ObservableList<ObservableList<String>>> resultlist) {
        setData(resultlist, Map.of());
    }

    public void setTheme(Theme t) {
        this.theme = t;
        applyThemeToSelf();
        timelineView.applyTheme(t);
        mapView.applyTheme(t);
        detailPane.applyTheme(t);
    }

    public Theme getTheme() { return theme; }

    // Selection handlers

    /**
     * Timeline dot clicked: show only this single point on the map so it is
     * immediately visible without being hidden in a cluster, then update the
     * detail panel.  If the point has no coordinate the map is left unchanged.
     */
    private void onTimelinePointSelected(DataPoint dp) {
        detailPane.show(dp, theme);
        if (dp.getCoordinate() != null) {
            mapView.setData(List.of(dp));
            mapView.focusPoint(dp);
        }
    }

    /**
     * Map marker clicked: restore the full filtered point set on the map,
     * update the detail panel and scroll the timeline to that dot.
     */
    private void onMapPointSelected(DataPoint dp) {
        // Restore all currently-filtered points so the map is not stuck
        // showing a single marker after a timeline selection.
        mapView.setData(currentMapPoints);
        detailPane.show(dp, theme);
        timelineView.selectPoint(dp);
    }

    // Theme (own widgets)
    private void applyThemeToSelf() {
        setStyle(theme.bgStyle());
        titleBox.setStyle(theme.bgStyle() + theme.borderBottomStyle());
        appTitle.setTextFill(theme.labelStrong);
        subtitle.setTextFill(theme.label);
    }

    // -------------------------------------------------------------------------
    // Detail pane
    // -------------------------------------------------------------------------

    static class DetailPane extends ScrollPane {

        private final VBox content = new VBox(8);

        DetailPane() {
            setFitToWidth(true);
            content.setPadding(new Insets(14));
            setContent(content);
            // Allow SplitPane to shrink this pane; ScrollPane's content
            // would otherwise impose a large implicit minWidth.
            setMinSize(0, 0);
            clear(Theme.DARK);
        }

        void applyTheme(Theme t) {
            String bg = Theme.hex(t.bgAlt);
            setStyle("-fx-background: " + bg + "; -fx-background-color: " + bg + ";");
            content.setStyle("-fx-background-color: " + bg + ";");
        }

        void clear(Theme t) {
            content.getChildren().clear();
            applyTheme(t);
            Label placeholder = new Label("\u2190 Select a dot\non the timeline\nor a map marker");
            placeholder.setFont(Font.font("Monospace", 11));
            placeholder.setTextFill(t.axis);
            placeholder.setAlignment(Pos.CENTER);
            content.getChildren().add(placeholder);
        }

        void show(DataPoint dp, Theme t) {
            content.getChildren().clear();
            applyTheme(t);

            addRow("TABLE", dp.getTableName(),                Theme.hex(t.accent1), t);
            addRow("ROW",   String.valueOf(dp.getRowIndex()), Theme.hex(t.label),   t);
            if (dp.getTimestamp() != null)
                addRow("TIME", dp.getFormattedTimestamp() + " UTC", Theme.hex(t.accent2), t);
            if (dp.getCoordinate() != null)
                addRow("GEO",  dp.getCoordinate().toString(), Theme.hex(t.hover), t);

            addSeparator(t);

            if (dp instanceof ResponseRecordDataPoint rr) {
                addSectionHeader("RESPONSE RECORD", t);
                addOptional("END TIME",    rr.getEndTime() != null ? rr.getEndTime() + " UTC" : null, Theme.hex(t.accent2), t);
                addOptional("PARTY TYPE",  rr.getPartyType(),        Theme.hex(t.labelStrong), t);
                addOptional("NW ACCESS",   rr.getNwAccessType(),     Theme.hex(t.label), t);
                addOptional("DEVICE ID",   rr.getNaDeviceId(),       Theme.hex(t.labelStrong), t);
                addOptional("MSISDN",      rr.getMsisdn(),           Theme.hex(t.accent1), t);
                addOptional("IMSI",        rr.getImsi(),             Theme.hex(t.accent1), t);
                addOptional("DURATION",    formatDuration(rr.getDurationSeconds()), Theme.hex(t.label), t);
                addOptional("APN",         rr.getApn(),              Theme.hex(t.label), t);
                addOptional("AZIMUTH",     rr.getAzimuth() != null ? rr.getAzimuth() + "\u00b0" : null, Theme.hex(t.label), t);
                addOptional("MAP DATUM",   rr.getMapDatum(),         Theme.hex(t.label), t);
                addOptional("LT RAW",      rr.getLtRaw(),            Theme.hex(t.tick), t);
                addOptional("LO RAW",      rr.getLoRaw(),            Theme.hex(t.tick), t);
                addOptional("USER LOC",    rr.getUserLocationInfo(), Theme.hex(t.tick), t);
                addOptional("CALL IND.",   rr.getCallIndicator(),    Theme.hex(t.label), t);
                addOptional("CALL ACTION", rr.getCallActionCode(),   Theme.hex(t.label), t);
                addOptional("CALL SUBTYPE",rr.getCallSubtype(),      Theme.hex(t.label), t);
                addOptional("SESSION ID",  rr.getSessionId(),        Theme.hex(t.label), t);
                addOptional("TYPE EXTRA",  rr.getTypeOfDataExtra(),  Theme.hex(t.label), t);
                if (rr.getOtherInformation() != null && !rr.getOtherInformation().isBlank()) {
                    addSectionHeader("OTHER INFO", t);
                    for (String kv : rr.getOtherInformation().split(";\\s*"))
                        if (!kv.isBlank()) addRow("", kv.trim(), Theme.hex(t.detailValue), t);
                }
                addSeparator(t);
                addSectionHeader("RAW COLUMNS", t);
            }

            List<String> cols = dp.getColumnNames();
            List<String> row  = dp.getRawRow();
            for (int i = 0; i < Math.min(cols.size(), row.size()); i++) {
                String v = row.get(i);
                if (v != null && !v.isBlank())
                    addRow(cols.get(i), v, Theme.hex(t.detailValue), t);
            }
        }

        private void addSectionHeader(String title, Theme t) {
            Label h = new Label(title);
            h.setFont(Font.font("Monospace", FontWeight.BOLD, 10));
            h.setTextFill(t.accent1);
            h.setPadding(new Insets(4, 0, 2, 0));
            content.getChildren().add(h);
        }

        private void addSeparator(Theme t) {
            Separator sep = new Separator();
            sep.setStyle("-fx-background-color: " + Theme.hex(t.border) + ";");
            content.getChildren().add(sep);
        }

        private void addOptional(String key, String value, String hex, Theme t) {
            if (value != null && !value.isBlank()) addRow(key, value, hex, t);
        }

        private static String formatDuration(String raw) {
            if (raw == null || raw.isBlank()) return null;
            try {
                long s = Long.parseLong(raw.trim());
                if (s < 60)   return s + " s";
                if (s < 3600) return String.format("%d min %02d s", s / 60, s % 60);
                return String.format("%d h %02d min", s / 3600, (s % 3600) / 60);
            } catch (NumberFormatException e) { return raw; }
        }

        private void addRow(String key, String value, String valueHex, Theme t) {
            Label k = new Label(key);
            k.setFont(Font.font("Monospace", FontWeight.BOLD, 10));
            k.setTextFill(t.detailMuted);
            k.setPrefWidth(82);

            Label v = new Label(value);
            v.setFont(Font.font("Monospace", 10));
            v.setTextFill(Color.web(valueHex));
            v.setWrapText(true);
            v.setMaxWidth(Double.MAX_VALUE);

            HBox rowBox = new HBox(6, k, v);
            rowBox.setAlignment(Pos.TOP_LEFT);
            HBox.setHgrow(v, Priority.ALWAYS);
            content.getChildren().add(rowBox);
        }
    }
}
