package fqlite.importer;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import fqlite.timemap.TacLocalDatabase;
import fqlite.timemap.UliDecoder;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// Imports an ETSI Retained Data XML file (TS 102 657 v1.28.1) into a new
/// SQLite database.
///
/// This is a Java port of the original Python prototype
/// {@code etsi_xml_import.py} that was used to do this conversion outside
/// of FQLite. The resulting database schema (tables {@code request_metadata}
/// and {@code response_records}) is identical to the one produced by the
/// Python script, so existing SQL queries / reports keep working unchanged.
///
/// @author D. Pawlaszczyk
public class EtsiXmlImporter {

    /** Namespace of the ETSI RetainedData XML schema (TS 102 657 v1.28.1). */
    public static final String NS_URI = "http://uri.etsi.org/02657/v1.28.1#/RetainedData";

    private static final Logger LOG = Logger.getLogger(EtsiXmlImporter.class.getName());

    private EtsiXmlImporter() { /* utility class */ }

    /** Reports import progress in terms of processed / total response records. */
    @FunctionalInterface
    public interface ProgressCallback {
        void onProgress(int current, int total);
    }

    // ------------------------------------------------------------------
    // Database schema (identical to the Python reference implementation)
    // ------------------------------------------------------------------

    private static final String[] SCHEMA = {
            "CREATE TABLE IF NOT EXISTS request_metadata (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                    "country_code TEXT," +
                    "organisation_id TEXT," +
                    "request_number TEXT UNIQUE," +
                    "csp_id TEXT," +
                    "timestamp TEXT," +
                    "imported_at TEXT NOT NULL)",

            "CREATE TABLE IF NOT EXISTS response_records (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                    "request_id INTEGER REFERENCES request_metadata(id)," +
                    "record_number INTEGER," +
                    "party_type TEXT," +
                    "national_country_code TEXT," +
                    "national_header_id TEXT," +
                    "nw_access_type TEXT," +
                    "start_time DATETIME," +
                    "end_time DATETIME," +
                    "duration_seconds INTEGER," +
                    "na_device_id TEXT," +
                    "latitude_raw TEXT," +
                    "longitude_raw TEXT," +
                    "latitude_dec REAL," +
                    "longitude_dec REAL," +
                    "map_datum TEXT," +
                    "azimuth INTEGER," +
                    "user_location_info TEXT," +
                    "imsi TEXT," +
                    "msisdn TEXT," +
                    "apn TEXT," +
                    "other_information TEXT," +
                    "call_indicator TEXT," +
                    "call_action_code TEXT," +
                    "call_subtype TEXT," +
                    "session_id TEXT," +
                    "type_of_data_extra TEXT," +
                    "nat_country_code TEXT," +
                    "nat_header_id TEXT," +
                    "type_of_data TEXT," +
                    "party_role TEXT," +
                    "cell_mcc INTEGER," +
                    "cell_mnc INTEGER," +
                    "cell_lac_tac INTEGER," +
                    "cell_ci INTEGER," +
                    "device_brand TEXT," +
                    "device_model TEXT)",

            "CREATE INDEX IF NOT EXISTS idx_msisdn ON response_records(msisdn)",
            "CREATE INDEX IF NOT EXISTS idx_imsi ON response_records(imsi)",
            "CREATE INDEX IF NOT EXISTS idx_na_device_id ON response_records(na_device_id)",
            "CREATE INDEX IF NOT EXISTS idx_start_time ON response_records(start_time)",
            "CREATE INDEX IF NOT EXISTS idx_end_time ON response_records(end_time)",
            "CREATE INDEX IF NOT EXISTS idx_apn ON response_records(apn)",
            "CREATE INDEX IF NOT EXISTS idx_cell ON response_records(cell_mcc, cell_mnc, cell_lac_tac, cell_ci)"
    };

    private static final String INSERT_RECORD_SQL =
            "INSERT INTO response_records (" +
                    "request_id, record_number, party_type, national_country_code, national_header_id, " +
                    "nw_access_type, start_time, end_time, duration_seconds, na_device_id, " +
                    "latitude_raw, longitude_raw, latitude_dec, longitude_dec, map_datum, azimuth, " +
                    "user_location_info, imsi, msisdn, apn, other_information, " +
                    "call_indicator, call_action_code, call_subtype, session_id, type_of_data_extra, " +
                    "nat_country_code, nat_header_id, type_of_data, party_role, " +
                    "cell_mcc, cell_mnc, cell_lac_tac, cell_ci, device_brand, device_model" +
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final int BATCH_SIZE = 500;

    // ------------------------------------------------------------------
    // Public entry point
    // ------------------------------------------------------------------

    /**
     * Reads the given ETSI RetainedData XML file and imports all
     * {@code ResponseRecord} entries into a freshly created SQLite database.
     *
     * @param xmlFile    the ETSI XML file to import
     * @param dbFile     target SQLite database (will be created/overwritten)
     * @param onProgress optional progress callback, may be {@code null}
     * @throws Exception on any parsing / database error
     */
    public static void importXmlToSqlite(File xmlFile, File dbFile, ProgressCallback onProgress) throws Exception {

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(xmlFile);
        Element root = doc.getDocumentElement();

        Class.forName("org.sqlite.JDBC");

        if (dbFile.exists()) {
            dbFile.delete();
        }

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile.getAbsolutePath())) {

            try (Statement st = conn.createStatement()) {
                st.execute("PRAGMA journal_mode=WAL;");
                st.execute("PRAGMA synchronous=NORMAL;");
                for (String ddl : SCHEMA) {
                    st.execute(ddl);
                }
            }

            conn.setAutoCommit(false);

            long requestId = importRequestMetadata(conn, root);

            NodeList records = root.getElementsByTagNameNS(NS_URI, "ResponseRecord");
            int total = records.getLength();

            try (PreparedStatement ps = conn.prepareStatement(INSERT_RECORD_SQL)) {
                int pending = 0;
                for (int i = 0; i < total; i++) {
                    // A single ResponseRecord can yield more than one row: voice-call/
                    // SMS records carry one row per party leg (A-/B-Teilnehmer), see
                    // bindRecord(). addBatch() is called inside bindRecord() itself for
                    // each row produced, not here.
                    pending += bindRecord(ps, requestId, (Element) records.item(i));

                    if (pending >= BATCH_SIZE) {
                        ps.executeBatch();
                        pending = 0;
                        if (onProgress != null) {
                            onProgress.onProgress(i + 1, total);
                        }
                    }
                }
                if (pending > 0) {
                    ps.executeBatch();
                }
            }

            conn.commit();
            if (onProgress != null) {
                onProgress.onProgress(total, total);
            }
        }
    }

    // ------------------------------------------------------------------
    // Header / request metadata
    // ------------------------------------------------------------------

    private static long importRequestMetadata(Connection conn, Element root) throws SQLException {

        Element header = findElement(root, "retainedDataHeader");
        String countryCode = findText(header, "requestID/countryCode");
        String orgId = findText(header, "requestID/authorisedOrganisationID");
        String requestNumber = findText(header, "requestID/requestNumber");
        String cspId = findText(header, "cSPID");
        String timestamp = findText(header, "timeStamp");

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT OR IGNORE INTO request_metadata " +
                        "(country_code, organisation_id, request_number, csp_id, timestamp, imported_at) " +
                        "VALUES (?,?,?,?,?,?)")) {
            setNullableString(ps, 1, countryCode);
            setNullableString(ps, 2, orgId);
            setNullableString(ps, 3, requestNumber);
            setNullableString(ps, 4, cspId);
            setNullableString(ps, 5, timestamp);
            ps.setString(6, OffsetDateTime.now().toString());
            ps.executeUpdate();
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id FROM request_metadata WHERE request_number = ?")) {
            setNullableString(ps, 1, requestNumber);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }
        throw new SQLException("Could not resolve request_metadata.id for request number " + requestNumber);
    }

    // ------------------------------------------------------------------
    // Per-record extraction (mirrors the field paths of the ETSI schema)
    // ------------------------------------------------------------------

    /**
     * Binds and batches all rows for one {@code ResponseRecord} and returns
     * how many rows were added (almost always 1, but see below).
     *
     * <p>Real ETSI exports use (at least) two structurally different shapes
     * for {@code telephonyServiceUsage}, confirmed against an actual export
     * ({@code Vorgang_001.xml}) rather than the XSD alone:</p>
     * <ul>
     *   <li><b>GPRS / data sessions</b> — a single party, with the session
     *   details (device id, location, timing, identifiers) nested under a
     *   sibling {@code nationalTelephonyServiceUsage/internetAccess}
     *   element. Handled by {@link #bindNationalUsageRow}.</li>
     *   <li><b>Voice calls / SMS</b> — no {@code nationalTelephonyServiceUsage}
     *   at all. Instead {@code partyInformation} directly contains one
     *   {@code TelephonyPartyInformation} per party leg (e.g.
     *   originating-/terminating-Party for a call, smsOriginator/
     *   smsRecipient for an SMS), each with its own
     *   {@code communicationTime}, {@code iMSI}/{@code iMEI} and
     *   {@code detailedLocation}. This shape was not present in the sample
     *   data ({@code sample_500.xml}, GPRS-only) this importer was originally
     *   built/verified against, so it previously fell through to the GPRS
     *   path and silently produced rows with no location or identifiers at
     *   all for every call/SMS record — handled by
     *   {@link #bindPartyLegRow}, one row per party leg.</li>
     * </ul>
     */
    private static int bindRecord(PreparedStatement ps, long requestId, Element rec) throws SQLException {

        final String tsuPath = "recordPayload/telephonyRecord/telephonyServiceUsage";

        Integer recordNumber = parseIntOrNull(findText(rec, "recordNumber"));

        String otherInfo = findText(rec, "additionalInformation/otherInformation");
        Map<String, String> oi = parseAdditionalInfo(otherInfo);
        String callIndicator = oi.get("Call Indikator");
        String callActionCode = oi.get("Call Action Code");
        String callSubtype = oi.get("Call Subtype");
        String sessionId = oi.get("Session Id");
        String typeOfDataExtra = oi.get("TypeOfData");

        String natCc2 = findText(rec, "nationalRecordPayload/countryCode");
        String natHid2 = findText(rec, "nationalRecordPayload/headerID");
        Element todElem = findElement(rec, "nationalRecordPayload/typeOfData");
        String typeOfData = firstChildLocalName(todElem);

        Element ntsuElem = findElement(rec, tsuPath + "/nationalTelephonyServiceUsage");
        if (ntsuElem != null) {
            bindNationalUsageRow(ps, requestId, rec, tsuPath, ntsuElem, recordNumber, otherInfo,
                    callIndicator, callActionCode, callSubtype, sessionId, typeOfDataExtra,
                    natCc2, natHid2, typeOfData);
            return 1;
        }

        Element partyInfoElem = findElement(rec, tsuPath + "/partyInformation");
        int rows = 0;
        if (partyInfoElem != null) {
            NodeList children = partyInfoElem.getChildNodes();
            for (int i = 0; i < children.getLength(); i++) {
                Node n = children.item(i);
                if (n.getNodeType() != Node.ELEMENT_NODE) continue;
                Element party = (Element) n;
                if (!"TelephonyPartyInformation".equals(party.getLocalName())) continue;
                bindPartyLegRow(ps, requestId, party, recordNumber, otherInfo,
                        callIndicator, callActionCode, callSubtype, sessionId, typeOfDataExtra,
                        natCc2, natHid2, typeOfData);
                rows++;
            }
        }

        if (rows == 0) {
            // Neither known shape matched (e.g. a third telephonyServiceUsage
            // variant we haven't seen yet). Emit one near-empty row instead of
            // silently dropping the ResponseRecord, so record counts stay
            // correct and the gap is visible in the data instead of invisible.
            LOG.warning("EtsiXmlImporter: record " + recordNumber + " matched neither the GPRS "
                    + "(nationalTelephonyServiceUsage) nor the voice/SMS (partyInformation) shape "
                    + "— importing it with no location/identifiers.");
            writeRow(ps, requestId, recordNumber, null, null, null, null, null, null, null, null,
                    null, null, null, null, null, null, null,
                    null, null, null, otherInfo, callIndicator, callActionCode, callSubtype,
                    sessionId, typeOfDataExtra, natCc2, natHid2, typeOfData, null);
            rows = 1;
        }
        return rows;
    }

    /** GPRS / data-session shape: one row, sourced from {@code nationalTelephonyServiceUsage}. */
    private static void bindNationalUsageRow(PreparedStatement ps, long requestId, Element rec, String tsuPath,
            Element ntsuElem, Integer recordNumber, String otherInfo, String callIndicator, String callActionCode,
            String callSubtype, String sessionId, String typeOfDataExtra, String natCc2, String natHid2,
            String typeOfData) throws SQLException {

        Element partyElem = findElement(rec, tsuPath + "/partyInformation/TelephonyPartyInformation/partyType");
        String partyType = firstChildLocalName(partyElem);
        if ("other".equals(partyType)) {
            partyType = findText(rec, tsuPath + "/partyInformation/TelephonyPartyInformation/partyType/other");
        }

        final String ntsu = tsuPath + "/nationalTelephonyServiceUsage";
        String natCc = findText(rec, ntsu + "/countryCode");
        String natHid = findText(rec, ntsu + "/headerID");

        // nationalTelephonyServiceUsage is itself an XSD "choice": internetAccess
        // (GPRS) is the only branch this importer used to assume, but real ETSI
        // exports also contain voice-call / SMS branches under a different
        // element name. Resolve whichever branch is actually present instead of
        // hardcoding "internetAccess" — but skip the two fixed leading siblings
        // (countryCode, headerID) that real exports place before the actual
        // choice element. A plain firstChildLocalName() would otherwise return
        // "countryCode" itself, silently resolving every "ia"-prefixed path
        // below (start/end time, naDeviceId, location, imsi/msisdn/apn) to
        // nothing — which is exactly the bug reported against Vorgang_001.xml:
        // those columns came back NULL even though party_type/national_country_code/
        // national_header_id (read directly off ntsuElem, not via "ia") were fine.
        String branchName = firstNonMetaChildLocalName(ntsuElem);
        final String ia = (branchName != null) ? (ntsu + "/" + branchName) : (ntsu + "/internetAccess");
        Element branchElem = (branchName != null) ? firstChildByTag(ntsuElem, branchName) : null;

        Element nwElem = findElement(rec, ia + "/nwAccessType");
        String nwAccess = firstChildLocalName(nwElem);

        String start = parseEtsiTimestamp(findText(rec, ia + "/interval/startTime"));
        String end = parseEtsiTimestamp(findText(rec, ia + "/interval/endTime"));
        Integer duration = parseIntOrNull(findText(rec, ia + "/interval/durationTime"));
        String deviceId = findText(rec, ia + "/naDeviceId");

        String latRaw = findText(rec, ia + "/location/extendedLocation/spot/gsmLocation/geoCoordinates/latitude");
        String lonRaw = findText(rec, ia + "/location/extendedLocation/spot/gsmLocation/geoCoordinates/longitude");
        Element datumElem = findElement(rec, ia + "/location/extendedLocation/spot/gsmLocation/geoCoordinates/mapDatum");
        String datum = firstChildLocalName(datumElem);
        Integer azimuth = parseIntOrNull(findText(rec, ia + "/location/extendedLocation/spot/gsmLocation/geoCoordinates/azimuth"));
        String uli = findText(rec, ia + "/location/userLocationInformation");

        // iMSI/mSISDN/aPN live under a branch-specific identity container
        // (gprsInformation for internetAccess). No real-world voice/SMS sample
        // of this shape was available to confirm a different branch's
        // container name, so after trying the known GPRS path, fall back to a
        // depth-first search for the leaf element by name anywhere inside the
        // chosen branch.
        String imsi = findText(rec, ia + "/gprsInformation/iMSI");
        String msisdn = findText(rec, ia + "/gprsInformation/mSISDN");
        String apn = findText(rec, ia + "/gprsInformation/aPN");
        if (imsi == null) imsi = findDescendantText(branchElem, "iMSI");
        if (msisdn == null) msisdn = findDescendantText(branchElem, "mSISDN");
        if (apn == null) apn = findDescendantText(branchElem, "aPN");

        Double latDec = parseDmsCoordinate(latRaw);
        Double lonDec = parseDmsCoordinate(lonRaw);

        writeRow(ps, requestId, recordNumber, partyType, natCc, natHid, nwAccess, start, end, duration, deviceId,
                latRaw, lonRaw, latDec, lonDec, datum, azimuth, uli, imsi, msisdn, apn, otherInfo,
                callIndicator, callActionCode, callSubtype, sessionId, typeOfDataExtra, natCc2, natHid2,
                typeOfData, /* partyRole */ null);
    }

    /**
     * Voice-call / SMS shape: one row per party leg ({@code party} is a
     * single {@code TelephonyPartyInformation} taken directly from
     * {@code telephonyServiceUsage/partyInformation}, not from a
     * {@code nationalTelephonyServiceUsage} sibling).
     */
    private static void bindPartyLegRow(PreparedStatement ps, long requestId, Element party, Integer recordNumber,
            String otherInfo, String callIndicator, String callActionCode, String callSubtype, String sessionId,
            String typeOfDataExtra, String natCc2, String natHid2, String typeOfData) throws SQLException {

        String partyRole = firstChildLocalName(findElement(party, "partyRole"));

        String partyType = firstChildLocalName(findElement(party, "partyType"));
        if ("other".equals(partyType)) {
            partyType = findText(party, "partyType/other");
        }

        String msisdn = findText(party, "partyNumber");
        String imsi = findText(party, "iMSI");
        String deviceId = findText(party, "iMEI");

        String start = parseEtsiTimestamp(findText(party, "communicationTime/startTime"));
        String end = parseEtsiTimestamp(findText(party, "communicationTime/endTime"));
        Integer duration = parseIntOrNull(findText(party, "communicationTime/durationTime"));

        final String loc = "detailedLocation/cellInformation";
        String latRaw = findText(party, loc + "/extendedLocation/spot/gsmLocation/geoCoordinates/latitude");
        String lonRaw = findText(party, loc + "/extendedLocation/spot/gsmLocation/geoCoordinates/longitude");
        Element datumElem = findElement(party, loc + "/extendedLocation/spot/gsmLocation/geoCoordinates/mapDatum");
        String datum = firstChildLocalName(datumElem);
        Integer azimuth = parseIntOrNull(findText(party, loc + "/extendedLocation/spot/gsmLocation/geoCoordinates/azimuth"));
        String uli = findText(party, loc + "/userLocationInformation");

        Double latDec = parseDmsCoordinate(latRaw);
        Double lonDec = parseDmsCoordinate(lonRaw);

        // No nationalTelephonyServiceUsage/nwAccessType exists for this shape;
        // derive a readable equivalent from the party role instead so
        // nw_access_type isn't just empty. (call_indicator, e.g. MOC/MTC/MOM/MTM,
        // remains the authoritative field for Telefonie/SMS/GPRS classification —
        // see PoliceAnalysisQueries#CONNECTION_TYPE — this is purely informational.)
        String nwAccess;
        if ("originating-Party".equals(partyRole) || "terminating-Party".equals(partyRole)) {
            nwAccess = "voiceCall";
        } else if (partyRole != null && partyRole.toLowerCase(Locale.ROOT).startsWith("sms")) {
            nwAccess = "sms";
        } else {
            nwAccess = partyRole;
        }

        writeRow(ps, requestId, recordNumber, partyType, /* natCc */ null, /* natHid */ null, nwAccess, start, end,
                duration, deviceId, latRaw, lonRaw, latDec, lonDec, datum, azimuth, uli, imsi, msisdn,
                /* apn */ null, otherInfo, callIndicator, callActionCode, callSubtype, sessionId, typeOfDataExtra,
                natCc2, natHid2, typeOfData, partyRole);
    }

    /**
     * Binds one {@code response_records} row and adds it to the batch. Shared by all three shapes above.
     *
     * <p>As a side effect, decodes the raw {@code uli} hex string (see {@link UliDecoder}) into its
     * structured cell identity (MCC, MNC, LAC/TAC, CI) and persists those four values into the
     * {@code cell_mcc}/{@code cell_mnc}/{@code cell_lac_tac}/{@code cell_ci} columns, so that
     * forensic SQL (filters, aggregations, the LLM-driven query templates) can match on cell
     * identity directly without having to re-decode {@code user_location_info} at query time.
     * Any field {@link UliDecoder} could not determine (sentinel {@code -1}, or decoding failed
     * entirely) is stored as {@code NULL}.</p>
     *
     * <p>Also resolves the device (brand/model) encoded in {@code deviceId}'s TAC (the IMEI's
     * first 8 digits) against {@link TacLocalDatabase} — the analyst's local, offline reference
     * file (Settings → Location → "Lokale TAC-Datenbank") — and persists the result into
     * {@code device_brand}/{@code device_model}. Deliberately uses only the local database, never
     * {@link fqlite.timemap.TacLookupService}'s online HiCellTek fallback: a bulk import must not
     * make a network call (or block on one) per record. {@link TacLocalDatabase#lookup} already
     * returns {@code null} immediately, with no I/O, when no local file is configured — so both
     * columns simply stay {@code NULL} when the analyst hasn't set one up ("falls vorhanden").</p>
     */
    private static void writeRow(PreparedStatement ps, long requestId, Integer recordNumber, String partyType,
            String natCc, String natHid, String nwAccess, String start, String end, Integer duration,
            String deviceId, String latRaw, String lonRaw, Double latDec, Double lonDec, String datum,
            Integer azimuth, String uli, String imsi, String msisdn, String apn, String otherInfo,
            String callIndicator, String callActionCode, String callSubtype, String sessionId,
            String typeOfDataExtra, String natCc2, String natHid2, String typeOfData, String partyRole)
            throws SQLException {

        UliDecoder.CellInfo cellInfo = UliDecoder.decode(uli);
        Integer cellMcc    = (cellInfo != null && cellInfo.mcc    >= 0) ? cellInfo.mcc    : null;
        Integer cellMnc    = (cellInfo != null && cellInfo.mnc    >= 0) ? cellInfo.mnc    : null;
        Integer cellLacTac = (cellInfo != null && cellInfo.lac    >= 0) ? cellInfo.lac    : null;
        Integer cellCi     = (cellInfo != null && cellInfo.cellId >= 0) ? cellInfo.cellId : null;

        TacLocalDatabase.Entry deviceEntry = TacLocalDatabase.getInstance().lookup(deviceId);
        String deviceBrand = deviceEntry != null ? deviceEntry.brand : null;
        String deviceModel = deviceEntry != null ? deviceEntry.specs : null;

        int idx = 1;
        ps.setLong(idx++, requestId);
        setNullableInt(ps, idx++, recordNumber);
        setNullableString(ps, idx++, partyType);
        setNullableString(ps, idx++, natCc);
        setNullableString(ps, idx++, natHid);
        setNullableString(ps, idx++, nwAccess);
        setNullableString(ps, idx++, start);
        setNullableString(ps, idx++, end);
        setNullableInt(ps, idx++, duration);
        setNullableString(ps, idx++, deviceId);
        setNullableString(ps, idx++, latRaw);
        setNullableString(ps, idx++, lonRaw);
        setNullableDouble(ps, idx++, latDec);
        setNullableDouble(ps, idx++, lonDec);
        setNullableString(ps, idx++, datum);
        setNullableInt(ps, idx++, azimuth);
        setNullableString(ps, idx++, uli);
        setNullableString(ps, idx++, imsi);
        setNullableString(ps, idx++, msisdn);
        setNullableString(ps, idx++, apn);
        setNullableString(ps, idx++, otherInfo);
        setNullableString(ps, idx++, callIndicator);
        setNullableString(ps, idx++, callActionCode);
        setNullableString(ps, idx++, callSubtype);
        setNullableString(ps, idx++, sessionId);
        setNullableString(ps, idx++, typeOfDataExtra);
        setNullableString(ps, idx++, natCc2);
        setNullableString(ps, idx++, natHid2);
        setNullableString(ps, idx++, typeOfData);
        setNullableString(ps, idx++, partyRole);
        setNullableInt(ps, idx++, cellMcc);
        setNullableInt(ps, idx++, cellMnc);
        setNullableInt(ps, idx++, cellLacTac);
        setNullableInt(ps, idx++, cellCi);
        setNullableString(ps, idx++, deviceBrand);
        setNullableString(ps, idx, deviceModel);
        ps.addBatch();
    }

    // ------------------------------------------------------------------
    // XML helpers (mirrors the small XPath-like helpers of the Python script)
    // ------------------------------------------------------------------

    /**
     * Resolves a "/"-separated path of local element names, following only
     * direct child elements at each step (equivalent to Python's
     * {@code ElementTree.find()} semantics).
     */
    private static Element findElement(Element parent, String path) {
        if (parent == null) return null;
        Element current = parent;
        for (String part : path.split("/")) {
            current = firstChildByTag(current, part);
            if (current == null) return null;
        }
        return current;
    }

    private static String findText(Element parent, String path) {
        Element e = findElement(parent, path);
        if (e == null) return null;
        String text = e.getTextContent();
        if (text == null) return null;
        text = text.trim();
        return text.isEmpty() ? null : text;
    }

    private static Element firstChildByTag(Element parent, String localName) {
        if (parent == null) return null;
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node n = children.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
                Element e = (Element) n;
                if (localName.equals(e.getLocalName()) && NS_URI.equals(e.getNamespaceURI())) {
                    return e;
                }
            }
        }
        return null;
    }

    /**
     * Depth-first search for the first descendant element with the given
     * local name, at any depth, regardless of intermediate container names.
     *
     * <p>Used as a fallback for fields (iMSI/mSISDN/aPN) whose container
     * element is known for the {@code internetAccess} branch
     * ({@code gprsInformation}) but unverified for other
     * {@code telephonyServiceUsage} branches (voice call / SMS) — see
     * {@link #bindRecord}.</p>
     */
    private static Element findDescendant(Element root, String localName) {
        if (root == null) return null;
        NodeList children = root.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node n = children.item(i);
            if (n.getNodeType() != Node.ELEMENT_NODE) continue;
            Element e = (Element) n;
            if (localName.equals(e.getLocalName()) && NS_URI.equals(e.getNamespaceURI())) {
                return e;
            }
            Element found = findDescendant(e, localName);
            if (found != null) return found;
        }
        return null;
    }

    private static String findDescendantText(Element root, String localName) {
        Element e = findDescendant(root, localName);
        if (e == null) return null;
        String text = e.getTextContent();
        if (text == null) return null;
        text = text.trim();
        return text.isEmpty() ? null : text;
    }

    /** Returns the local name of the first child element (used for XSD "choice" elements). */
    private static String firstChildLocalName(Element elem) {
        if (elem == null) return null;
        NodeList children = elem.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node n = children.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
                return ((Element) n).getLocalName();
            }
        }
        return null;
    }

    /**
     * Like {@link #firstChildLocalName}, but skips the {@code countryCode}/
     * {@code headerID} pair that real ETSI exports place before the actual
     * XSD "choice" element inside {@code nationalTelephonyServiceUsage}
     * (confirmed against {@code Vorgang_001.xml}). Used only to detect that
     * choice's branch name — plain {@code firstChildLocalName} would return
     * "countryCode" instead.
     */
    private static String firstNonMetaChildLocalName(Element elem) {
        if (elem == null) return null;
        NodeList children = elem.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node n = children.item(i);
            if (n.getNodeType() != Node.ELEMENT_NODE) continue;
            String name = ((Element) n).getLocalName();
            if ("countryCode".equals(name) || "headerID".equals(name)) continue;
            return name;
        }
        return null;
    }

    // ------------------------------------------------------------------
    // Value parsing helpers
    // ------------------------------------------------------------------

    private static final Pattern TIMESTAMP_PATTERN =
            Pattern.compile("(\\d{4})(\\d{2})(\\d{2})(\\d{2})(\\d{2})(\\d{2})(Z|[+-]\\d{4})");

    /**
     * Converts an ETSI timestamp (e.g. {@code 20240927020503+0200} or
     * {@code 20240927020503Z}) into an ISO-8601 UTC string suitable for a
     * SQLite {@code DATETIME} column (e.g. {@code 2024-09-27 00:05:03}).
     */
    static String parseEtsiTimestamp(String raw) {
        if (raw == null) return null;
        raw = raw.trim();
        Matcher m = TIMESTAMP_PATTERN.matcher(raw);
        if (!m.matches()) return null;

        int year = Integer.parseInt(m.group(1));
        int month = Integer.parseInt(m.group(2));
        int day = Integer.parseInt(m.group(3));
        int hour = Integer.parseInt(m.group(4));
        int minute = Integer.parseInt(m.group(5));
        int second = Integer.parseInt(m.group(6));
        String tz = m.group(7);

        LocalDateTime naive = LocalDateTime.of(year, month, day, hour, minute, second);

        Duration offset;
        if ("Z".equals(tz)) {
            offset = Duration.ZERO;
        } else {
            int sign = tz.charAt(0) == '+' ? 1 : -1;
            int oh = Integer.parseInt(tz.substring(1, 3));
            int om = Integer.parseInt(tz.substring(3, 5));
            offset = Duration.ofHours((long) sign * oh).plusMinutes((long) sign * om);
        }

        LocalDateTime utc = naive.minus(offset);
        return utc.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT));
    }

    /**
     * Converts an ETSI DMS coordinate (e.g. {@code N515715} or
     * {@code E0144130}) into decimal degrees, rounded to 6 decimal places.
     */
    static Double parseDmsCoordinate(String raw) {
        if (raw == null) return null;
        raw = raw.trim();
        if (raw.isEmpty()) return null;

        char hemi = Character.toUpperCase(raw.charAt(0));
        String digits = raw.substring(1);
        int deg, min, sec;

        if (hemi == 'N' || hemi == 'S') {
            if (digits.length() != 6) return null;
            deg = Integer.parseInt(digits.substring(0, 2));
            min = Integer.parseInt(digits.substring(2, 4));
            sec = Integer.parseInt(digits.substring(4, 6));
        } else if (hemi == 'E' || hemi == 'W') {
            while (digits.length() < 7) {
                digits = "0" + digits;
            }
            if (digits.length() != 7) return null;
            deg = Integer.parseInt(digits.substring(0, 3));
            min = Integer.parseInt(digits.substring(3, 5));
            sec = Integer.parseInt(digits.substring(5, 7));
        } else {
            return null;
        }

        double decimal = deg + min / 60.0 + sec / 3600.0;
        if (hemi == 'S' || hemi == 'W') {
            decimal = -decimal;
        }
        return Math.round(decimal * 1_000_000.0) / 1_000_000.0;
    }

    private static final Pattern KEY_VALUE_PATTERN = Pattern.compile("\"([^\"]+)\"\\s*=\\s*\"([^\"]*)\"");

    /** Parses {@code "Key1"="Value1";"Key2"="Value2"} style strings into a map. */
    static Map<String, String> parseAdditionalInfo(String raw) {
        Map<String, String> result = new HashMap<>();
        if (raw == null) return result;
        for (String pair : raw.split(";")) {
            pair = pair.trim();
            Matcher m = KEY_VALUE_PATTERN.matcher(pair);
            if (m.lookingAt()) {
                result.put(m.group(1), m.group(2));
            }
        }
        return result;
    }

    private static Integer parseIntOrNull(String raw) {
        if (raw == null) return null;
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    // ------------------------------------------------------------------
    // JDBC null-safe binding helpers
    // ------------------------------------------------------------------

    private static void setNullableString(PreparedStatement ps, int idx, String v) throws SQLException {
        if (v == null) ps.setNull(idx, Types.VARCHAR);
        else ps.setString(idx, v);
    }

    private static void setNullableInt(PreparedStatement ps, int idx, Integer v) throws SQLException {
        if (v == null) ps.setNull(idx, Types.INTEGER);
        else ps.setInt(idx, v);
    }

    private static void setNullableDouble(PreparedStatement ps, int idx, Double v) throws SQLException {
        if (v == null) ps.setNull(idx, Types.REAL);
        else ps.setDouble(idx, v);
    }
}
