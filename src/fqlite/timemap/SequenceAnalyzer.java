package fqlite.timemap;

import fqlite.timemap.DataAnalyzer.DataPoint;
import fqlite.timemap.DataAnalyzer.GeoCoordinate;
import fqlite.timemap.DataAnalyzer.ResponseRecordDataPoint;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Movement / sequence analysis for a single subscriber identity (IMSI).
 *
 * <p>Forensic use case: ETSI retained-data records carry an IMSI, a
 * timestamp and the position of the cell tower the subscriber was attached
 * to (see {@link MapView}'s "Funkzellen (Datensatz)" layer — these
 * coordinates denote the tower, not the device). Grouping all records that
 * share one IMSI and ordering them by time reveals whether the subscriber's
 * device was registered at more than one site, and in which order — i.e. a
 * coarse movement timeline.</p>
 *
 * <p>Consecutive records at the same physical site (within ~50 m, the same
 * threshold {@link MapView} uses for its other dedup logic) are collapsed
 * into a single {@link Stop} spanning from the first to the last record
 * seen there, instead of producing one entry per raw row.</p>
 */
public final class SequenceAnalyzer {

    private SequenceAnalyzer() {}

    /** Same "same physical site" threshold used elsewhere in MapView (~50 m). */
    private static final double SAME_SITE_DEG = 0.0005;

    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

    /**
     * Returns all distinct, non-blank IMSI values found in {@code points},
     * sorted ascending — intended for populating a selection dropdown.
     */
    public static List<String> listImsis(List<DataPoint> points) {
        return points.stream()
                .filter(p -> p instanceof ResponseRecordDataPoint)
                .map(p -> ((ResponseRecordDataPoint) p).getImsi())
                .filter(s -> s != null && !s.isBlank())
                .distinct()
                .sorted()
                .collect(Collectors.toList());
    }

    /**
     * Returns all distinct, non-blank device identities (IMEI, exposed as
     * {@link ResponseRecordDataPoint#getNaDeviceId()}) found in {@code points},
     * sorted ascending — same role as {@link #listImsis} but keyed by the
     * handset's own identity instead of the subscriber's, for the IMEI
     * storyboard animation in {@link MapView}.
     */
    public static List<String> listImeis(List<DataPoint> points) {
        return points.stream()
                .filter(p -> p instanceof ResponseRecordDataPoint)
                .map(p -> ((ResponseRecordDataPoint) p).getNaDeviceId())
                .filter(s -> s != null && !s.isBlank())
                .distinct()
                .sorted()
                .collect(Collectors.toList());
    }

    /**
     * Builds the chronological movement sequence for one IMSI.
     *
     * @param points all loaded data points (any table); rows that are not a
     *               {@link ResponseRecordDataPoint}, lack a coordinate, or
     *               don't match {@code imsi} are ignored
     * @param imsi   the IMSI to analyse (exact match)
     * @return        ordered list of stops, oldest first; empty if the IMSI
     *                has no geo-tagged records
     */
    public static List<Stop> buildSequence(List<DataPoint> points, String imsi) {
        if (imsi == null || imsi.isBlank()) return List.of();

        List<ResponseRecordDataPoint> records = points.stream()
                .filter(p -> p instanceof ResponseRecordDataPoint)
                .map(p -> (ResponseRecordDataPoint) p)
                .filter(p -> imsi.equals(p.getImsi()))
                .filter(p -> p.getCoordinate() != null)
                .sorted(Comparator.comparing(
                        DataPoint::getTimestamp,
                        Comparator.nullsLast(Comparator.naturalOrder())))
                .collect(Collectors.toList());

        List<Stop> stops = new ArrayList<>();
        for (ResponseRecordDataPoint rr : records) {
            GeoCoordinate c = rr.getCoordinate();
            Stop last = stops.isEmpty() ? null : stops.get(stops.size() - 1);
            if (last != null
                    && Math.abs(last.lat - c.getLatitude())  < SAME_SITE_DEG
                    && Math.abs(last.lon - c.getLongitude()) < SAME_SITE_DEG) {
                // Same site as the previous stop — extend its time range
                // instead of creating a redundant new entry.
                last.lastSeen = rr.getTimestamp();
                last.recordCount++;
                if (last.cellInfo == null)  last.cellInfo  = rr.getCellInfo();
                if (last.cellTower == null) last.cellTower = rr.getCellTower();
                continue;
            }
            Stop s = new Stop();
            s.lat         = c.getLatitude();
            s.lon         = c.getLongitude();
            s.firstSeen   = rr.getTimestamp();
            s.lastSeen    = rr.getTimestamp();
            s.recordCount = 1;
            s.cellInfo    = rr.getCellInfo();
            s.cellTower   = rr.getCellTower();
            if (last != null) {
                s.distanceFromPreviousKm = haversineKm(last.lat, last.lon, s.lat, s.lon);
            }
            stops.add(s);
        }
        return stops;
    }

    /**
     * Builds the chronological <b>activity</b> sequence for one IMEI (handset
     * identity, {@link ResponseRecordDataPoint#getNaDeviceId()}), for the
     * IMEI storyboard animation in {@link MapView}.
     *
     * <p>Unlike {@link #buildSequence}, which collapses consecutive same-site
     * records into one dwell {@link Stop} because it answers "where did this
     * subscriber go", this method keeps exactly one {@link Stop} per raw
     * record (recordCount is always 1) and copies the record's call-type
     * fields ({@code call_indicator}/{@code call_action_code}/
     * {@code call_subtype}) onto the stop. The storyboard is answering
     * "what did this handset do, and in what order" — collapsing same-site
     * activities together would hide exactly what an analyst wants to step
     * or play through, e.g. a GPRS session's First/Interim/Interim/Last
     * records all attached to the same mast.</p>
     *
     * @param points all loaded data points (any table); rows that are not a
     *               {@link ResponseRecordDataPoint}, lack a coordinate, or
     *               don't match {@code imei} are ignored
     * @param imei   the device id (IMEI) to analyse (exact match)
     * @return        ordered list of one-record activity stops, oldest
     *                first; empty if the IMEI has no geo-tagged records
     */
    public static List<Stop> buildImeiActivitySequence(List<DataPoint> points, String imei) {
        if (imei == null || imei.isBlank()) return List.of();

        List<ResponseRecordDataPoint> records = points.stream()
                .filter(p -> p instanceof ResponseRecordDataPoint)
                .map(p -> (ResponseRecordDataPoint) p)
                .filter(p -> imei.equals(p.getNaDeviceId()))
                .filter(p -> p.getCoordinate() != null)
                .sorted(Comparator.comparing(
                        DataPoint::getTimestamp,
                        Comparator.nullsLast(Comparator.naturalOrder())))
                .collect(Collectors.toList());

        List<Stop> stops = new ArrayList<>();
        Stop last = null;
        for (ResponseRecordDataPoint rr : records) {
            GeoCoordinate c = rr.getCoordinate();
            Stop s = new Stop();
            s.lat            = c.getLatitude();
            s.lon            = c.getLongitude();
            s.firstSeen      = rr.getTimestamp();
            s.lastSeen       = rr.getTimestamp();
            s.recordCount    = 1;
            s.cellInfo       = rr.getCellInfo();
            s.cellTower      = rr.getCellTower();
            s.callIndicator  = rr.getCallIndicator();
            s.callActionCode = rr.getCallActionCode();
            s.callSubtype    = rr.getCallSubtype();
            if (last != null) {
                s.distanceFromPreviousKm = haversineKm(last.lat, last.lon, s.lat, s.lon);
            }
            stops.add(s);
            last = s;
        }
        return stops;
    }

    /** Great-circle distance in km between two WGS-84 points. */
    public static double haversineKm(double lat1, double lon1, double lat2, double lon2) {
        double R = 6371.0;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    /** One contiguous stay at a single physical site within an IMSI's timeline. */
    public static final class Stop {
        public double  lat, lon;
        public Instant firstSeen;
        public Instant lastSeen;
        public int     recordCount;
        public double  distanceFromPreviousKm = 0.0;
        public UliDecoder.CellInfo cellInfo;
        public CellTower cellTower;

        /**
         * Raw Call Indicator / Action Code / Subtype of the record this stop
         * was built from. Populated only by {@link SequenceAnalyzer#buildImeiActivitySequence}
         * (IMEI storyboard); always {@code null} for stops built by
         * {@link SequenceAnalyzer#buildSequence} (plain IMSI movement path),
         * since those can span several records with different call types.
         */
        public String callIndicator;
        public String callActionCode;
        public String callSubtype;

        /** Duration between this stop's first and last record (zero if only one). */
        public Duration dwell() {
            if (firstSeen == null || lastSeen == null) return Duration.ZERO;
            return Duration.between(firstSeen, lastSeen);
        }

        public String formattedFirstSeen() {
            return firstSeen == null ? "–" : TS_FMT.format(firstSeen);
        }

        public String formattedLastSeen() {
            return lastSeen == null ? "–" : TS_FMT.format(lastSeen);
        }

        public String formattedDwell() {
            Duration d = dwell();
            long h = d.toHours(), m = d.toMinutesPart(), s = d.toSecondsPart();
            if (h == 0 && m == 0 && s == 0) return "–";
            return String.format("%02d:%02d:%02d", h, m, s);
        }

        /** Short cell-identity label, e.g. "LAC 1234 / CI 56", or "–" if unknown. */
        public String cellLabel() {
            if (cellTower != null) {
                String op = cellTower.operatorName != null ? cellTower.operatorName + " " : "";
                return op + "LAC/TAC " + cellTower.lac
                        + (cellTower.cellId >= 0 ? " / CI " + cellTower.cellId : "");
            }
            if (cellInfo != null) {
                return "LAC " + cellInfo.lac + " / CI " + cellInfo.cellId;
            }
            return "–";
        }

        /**
         * Human-readable meaning of {@link #callIndicator}, based on
         * forensic analysis of an ETSI-TS-102-232-style lawful-interception
         * export (Session GPRS/packet-data and SMS originated/terminated are
         * the high-confidence mappings observed so far); returns the raw
         * code unchanged if it isn't one of the known values, or {@code null}
         * if no call indicator is present on this stop.
         */
        public String callIndicatorLabel() {
            if (callIndicator == null) return null;
            return switch (callIndicator) {
                case "SGP" -> "Session GPRS / Packetdaten";
                case "MOM" -> "Mobile Originated Message (gesendete SMS)";
                case "MTM" -> "Mobile Terminated Message (empfangene SMS)";
                default    -> callIndicator;
            };
        }

        /** Human-readable meaning of {@link #callActionCode}, or {@code null} if absent. */
        public String callActionCodeLabel() {
            if (callActionCode == null) return null;
            return switch (callActionCode) {
                case "1" -> "terminierend (eingehend)";
                case "2" -> "originierend (ausgehend)";
                default  -> callActionCode;
            };
        }

        /** Human-readable meaning of {@link #callSubtype}, or {@code null} if absent. */
        public String callSubtypeLabel() {
            if (callSubtype == null) return null;
            return switch (callSubtype) {
                case "S" -> "Single Accounting Record (vollständiger Vorgang)";
                case "F" -> "First Record (Sitzungsbeginn)";
                case "I" -> "Interim Record (Zwischenintervall)";
                case "L" -> "Last Record (Sitzungsende)";
                default  -> callSubtype;
            };
        }

        /**
         * One-line activity summary combining {@link #callIndicatorLabel},
         * {@link #callSubtypeLabel} and {@link #callActionCodeLabel} for
         * display in the storyboard's result table/tooltip, or {@code null}
         * if this stop carries no call-type metadata at all (plain IMSI
         * movement stops).
         */
        public String activityLabel() {
            if (callIndicator == null && callActionCode == null && callSubtype == null) return null;
            StringBuilder sb = new StringBuilder();
            if (callIndicatorLabel() != null) sb.append(callIndicatorLabel());
            if (callSubtypeLabel() != null)   sb.append(sb.length() > 0 ? " · " : "").append(callSubtypeLabel());
            if (callActionCodeLabel() != null) sb.append(sb.length() > 0 ? " · " : "").append(callActionCodeLabel());
            return sb.length() > 0 ? sb.toString() : null;
        }

        @Override
        public String toString() {
            return String.format("Stop[%.5f,%.5f from=%s to=%s n=%d]",
                    lat, lon, formattedFirstSeen(), formattedLastSeen(), recordCount);
        }
    }
}
