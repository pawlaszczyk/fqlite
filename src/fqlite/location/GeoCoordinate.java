package fqlite.location;

// -------------------------------------------------------------------------
// Public data structures
// -------------------------------------------------------------------------

/** Immutable WGS-84 coordinate pair. */
public class GeoCoordinate {
    private final double latitude;
    private final double longitude;

    public GeoCoordinate(double latitude, double longitude) {
        this.latitude  = latitude;
        this.longitude = longitude;
    }

    public double getLatitude()  { return latitude;  }
    public double getLongitude() { return longitude; }

    @Override
    public String toString() {
        return String.format("%.6f, %.6f", latitude, longitude);
    }
}
