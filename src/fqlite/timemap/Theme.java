package fqlite.timemap;

import javafx.scene.paint.Color;

/**
 * Colour palette for dark and light modes.
 *
 * Every UI component receives a {@link Theme} instance and must read colours
 * exclusively from here so that switching themes requires only one call to
 * {@link MapViewPane#setTheme(Theme)}.
 */
public enum Theme {

    // -------------------------------------------------------------------------
    // Dark theme  (default)
    // -------------------------------------------------------------------------
    DARK(
        /* bg          */ "#0f1117",
        /* bgAlt       */ "#0a0d14",
        /* border      */ "#1e293b",
        /* axis        */ "#334155",
        /* tick        */ "#475569",
        /* label       */ "#94a3b8",
        /* labelStrong */ "#e2e8f0",
        /* grid        */ "#1e293b",
        /* connector   */ "#334155",
        /* accent1     */ "#38bdf8",   // timestamp-only dot / map title
        /* accent2     */ "#34d399",   // timestamp + geo dot / map title
        /* hover       */ "#f472b6",
        /* selected    */ "#facc15",
        /* detailMuted */ "#64748b",
        /* detailValue */ "#e2e8f0",
        /* tileUrl     */
          "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
    ),

    // -------------------------------------------------------------------------
    // Light theme
    // -------------------------------------------------------------------------
    LIGHT(
        /* bg          */ "#f8fafc",
        /* bgAlt       */ "#f1f5f9",
        /* border      */ "#cbd5e1",
        /* axis        */ "#94a3b8",
        /* tick        */ "#64748b",
        /* label       */ "#475569",
        /* labelStrong */ "#0f172a",
        /* grid        */ "#e2e8f0",
        /* connector   */ "#cbd5e1",
        /* accent1     */ "#0284c7",   // timestamp-only dot
        /* accent2     */ "#059669",   // timestamp + geo dot
        /* hover       */ "#db2777",
        /* selected    */ "#d97706",
        /* detailMuted */ "#64748b",
        /* detailValue */ "#0f172a",
        /* tileUrl     */
          "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
    );

    // -------------------------------------------------------------------------
    // Fields
    // -------------------------------------------------------------------------

    public final Color  bg;
    public final Color  bgAlt;
    public final Color  border;
    public final Color  axis;
    public final Color  tick;
    public final Color  label;
    public final Color  labelStrong;
    public final Color  grid;
    public final Color  connector;
    public final Color  accent1;
    public final Color  accent2;
    public final Color  hover;
    public final Color  selected;
    public final Color  detailMuted;
    public final Color  detailValue;
    public final String tileUrl;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    Theme(String bg, String bgAlt, String border, String axis, String tick,
          String label, String labelStrong, String grid, String connector,
          String accent1, String accent2, String hover, String selected,
          String detailMuted, String detailValue, String tileUrl) {

        this.bg          = Color.web(bg);
        this.bgAlt       = Color.web(bgAlt);
        this.border      = Color.web(border);
        this.axis        = Color.web(axis);
        this.tick        = Color.web(tick);
        this.label       = Color.web(label);
        this.labelStrong = Color.web(labelStrong);
        this.grid        = Color.web(grid);
        this.connector   = Color.web(connector);
        this.accent1     = Color.web(accent1);
        this.accent2     = Color.web(accent2);
        this.hover       = Color.web(hover);
        this.selected    = Color.web(selected);
        this.detailMuted = Color.web(detailMuted);
        this.detailValue = Color.web(detailValue);
        this.tileUrl     = tileUrl;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Returns the hex string (with #) of a Color. */
    public static String hex(Color c) {
        return String.format("#%02x%02x%02x",
            (int) Math.round(c.getRed()   * 255),
            (int) Math.round(c.getGreen() * 255),
            (int) Math.round(c.getBlue()  * 255));
    }

    /** Inline style for a solid background fill. */
    public String bgStyle()    { return "-fx-background-color: " + hex(bg)    + ";"; }
    public String bgAltStyle() { return "-fx-background-color: " + hex(bgAlt) + ";"; }

    /** Panel border on the bottom edge only. */
    public String borderBottomStyle() {
        return "-fx-border-color: " + hex(border) + "; -fx-border-width: 0 0 1 0;";
    }

    /** Panel border on the top edge only. */
    public String borderTopStyle() {
        return "-fx-border-color: " + hex(border) + "; -fx-border-width: 1 0 0 0;";
    }
}
