package fqlite.timemap;

/**
 * Represents a cell tower resolved from OpenCelliD or a local cell database.
 *
 * <p>Instances are produced by {@link CellTowerResolver} and attached to
 * {@link DataAnalyzer.ResponseRecordDataPoint} objects after the ULI hex field
 * has been decoded by {@link UliDecoder}.</p>
 *
 * <p>All numeric fields default to {@code -1} / {@code Double.NaN} when the
 * value is not available from the data source.</p>
 */
public final class CellTower {

    // ── Cell identity ────────────────────────────────────────────────────────

    /** Network technology derived from the ULI, e.g. "CGI/2G", "ECGI/LTE". */
    public final String networkType;

    /** Mobile Country Code. */
    public final int mcc;

    /** Mobile Network Code. */
    public final int mnc;

    /** Location Area Code (2G/3G) or Tracking Area Code (4G). */
    public final int lac;

    /** Cell Identity (CI) for 2G/3G, or sector index (CI = ECI & 0xFF) for LTE. */
    public final int cellId;

    /**
     * Full 28-bit E-UTRAN Cell Identifier (raw {@code cell} column from CSV).
     * {@code -1} for non-LTE or when not available.
     */
    public final int eci;

    // ── Geographic position of the tower ─────────────────────────────────────

    /** Latitude of the cell tower antenna (WGS-84), or {@code Double.NaN}. */
    public final double latitude;

    /** Longitude of the cell tower antenna (WGS-84), or {@code Double.NaN}. */
    public final double longitude;

    /**
     * Estimated coverage radius in metres, or {@code -1} if unknown.
     * Sourced from the {@code range} field in the OpenCelliD CSV.
     */
    public final int rangeMetres;

    // ── Operator information ──────────────────────────────────────────────────

    /**
     * Human-readable operator name derived from MCC+MNC, or {@code null}.
     */
    public final String operatorName;

    /**
     * ISO 3166-1 alpha-2 country code resolved from MCC, or {@code null}.
     */
    public final String country;

    // ── Data-quality indicators ───────────────────────────────────────────────

    /**
     * Number of independent measurements in the OpenCelliD database.
     * {@code -1} if unknown.
     */
    public final int samples;

    /**
     * {@code true} if the position was reported by multiple independent
     * sources and is therefore considered reliable.
     */
    public final boolean isReliable;

    // =========================================================================

    /** Full constructor including ECI. */
    public CellTower(
            String networkType, int mcc, int mnc, int lac, int cellId, int eci,
            double latitude, double longitude, int rangeMetres,
            String operatorName, String country,
            int samples, boolean isReliable) {
        this.networkType  = networkType;
        this.mcc          = mcc;
        this.mnc          = mnc;
        this.lac          = lac;
        this.cellId       = cellId;
        this.eci          = eci;
        this.latitude     = latitude;
        this.longitude    = longitude;
        this.rangeMetres  = rangeMetres;
        this.operatorName = operatorName;
        this.country      = country;
        this.samples      = samples;
        this.isReliable   = isReliable;
    }

    /** Convenience constructor without ECI (sets eci = -1). */
    public CellTower(
            String networkType, int mcc, int mnc, int lac, int cellId,
            double latitude, double longitude, int rangeMetres,
            String operatorName, String country,
            int samples, boolean isReliable) {
        this(networkType, mcc, mnc, lac, cellId, -1,
             latitude, longitude, rangeMetres,
             operatorName, country, samples, isReliable);
    }

    /** Returns {@code true} when geographic coordinates are available. */
    public boolean hasPosition() {
        return !Double.isNaN(latitude) && !Double.isNaN(longitude);
    }

    @Override
    public String toString() {
        return String.format(
                "CellTower[%s MCC=%d MNC=%d LAC=%d CI=%d ECI=%d → %.5f,%.5f %dm %s]",
                networkType, mcc, mnc, lac, cellId, eci,
                latitude, longitude, rangeMetres,
                operatorName != null ? operatorName : "?");
    }
}
