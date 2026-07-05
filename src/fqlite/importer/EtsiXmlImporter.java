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
import java.util.List;
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

    /**
     * Namespace of the ETSI RetainedData XML schema (TS 102 657 v1.28.1).
     *
     * <p><b>Not used to restrict element matching</b> (see
     * {@link #firstChildByTag}/{@link #findDescendant}): real-world exports
     * have also been seen under {@code v1.26.1} with an otherwise identical
     * structure, so all lookups in this class match on local element name
     * only, regardless of namespace/schema version. Kept around purely for
     * documentation/reference.</p>
     */
    public static final String NS_URI = "http://uri.etsi.org/02657/v1.28.1#/RetainedData";

    private static final Logger LOG = Logger.getLogger(EtsiXmlImporter.class.getName());

    private EtsiXmlImporter() { /* utility class */ }

    /** Reports import progress in terms of processed / total response records (per file). */
    @FunctionalInterface
    public interface ProgressCallback {
        void onProgress(int current, int total);
    }

    /**
     * Reports that import of a new file (out of a batch) has started, so a
     * caller importing several files into one database can show which file
     * is currently being read.
     */
    @FunctionalInterface
    public interface FileProgressCallback {
        void onFileStart(int fileIndex, int fileCount, String fileName);
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
                    "device_model TEXT," +
                    // The following columns cover the tKG96-style otherInformation shape
                    // (e.g. "A_PRIVATE_IPV4"="...";"DATENSATZART"="2";"CTT"="63";"type"="9";
                    // "serviceName"="Datendienst"), seen alongside the older, smaller
                    // Call-Indikator/Call-Action-Code/Call-Subtype/Session-ID key set that
                    // call_indicator/call_action_code/call_subtype/session_id above already
                    // cover. See bindRecord() for the extraction.
                    "a_private_ipv4 TEXT," +
                    "a_public_ipv6 TEXT," +
                    "datensatzart TEXT," +
                    "ctt TEXT," +
                    "service_type_code TEXT," +
                    "data_type TEXT," +
                    "suppl_services TEXT," +
                    "service_name TEXT," +
                    "a_party_cell_other_hsrs TEXT," +
                    "offset_beginn INTEGER," +
                    "offset_ende INTEGER," +
                    "quelle TEXT," +
                    "beam_width INTEGER," +
                    "source_file TEXT)",

            "CREATE INDEX IF NOT EXISTS idx_msisdn ON response_records(msisdn)",
            "CREATE INDEX IF NOT EXISTS idx_imsi ON response_records(imsi)",
            "CREATE INDEX IF NOT EXISTS idx_na_device_id ON response_records(na_device_id)",
            "CREATE INDEX IF NOT EXISTS idx_start_time ON response_records(start_time)",
            "CREATE INDEX IF NOT EXISTS idx_end_time ON response_records(end_time)",
            "CREATE INDEX IF NOT EXISTS idx_apn ON response_records(apn)",
            "CREATE INDEX IF NOT EXISTS idx_cell ON response_records(cell_mcc, cell_mnc, cell_lac_tac, cell_ci)",
            "CREATE INDEX IF NOT EXISTS idx_source_file ON response_records(source_file)"
    };

    private static final String INSERT_RECORD_SQL =
            "INSERT INTO response_records (" +
                    "request_id, record_number, party_type, national_country_code, national_header_id, " +
                    "nw_access_type, start_time, end_time, duration_seconds, na_device_id, " +
                    "latitude_raw, longitude_raw, latitude_dec, longitude_dec, map_datum, azimuth, " +
                    "user_location_info, imsi, msisdn, apn, other_information, " +
                    "call_indicator, call_action_code, call_subtype, session_id, type_of_data_extra, " +
                    "nat_country_code, nat_header_id, type_of_data, party_role, " +
                    "cell_mcc, cell_mnc, cell_lac_tac, cell_ci, device_brand, device_model, " +
                    "a_private_ipv4, a_public_ipv6, datensatzart, ctt, service_type_code, data_type, " +
                    "suppl_services, service_name, a_party_cell_other_hsrs, offset_beginn, offset_ende, quelle, " +
                    "beam_width, source_file" +
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final int BATCH_SIZE = 500;

    // ------------------------------------------------------------------
    // Public entry point
    // ------------------------------------------------------------------

    /**
     * Reads the given ETSI RetainedData XML file and imports all
     * {@code ResponseRecord} entries into a freshly created SQLite database.
     *
     * <p>Convenience wrapper around {@link #importXmlFilesToSqlite} for the
     * common single-file case.</p>
     *
     * @param xmlFile    the ETSI XML file to import
     * @param dbFile     target SQLite database (will be created/overwritten)
     * @param onProgress optional progress callback, may be {@code null}
     * @throws Exception on any parsing / database error
     */
    public static void importXmlToSqlite(File xmlFile, File dbFile, ProgressCallback onProgress) throws Exception {
        importXmlFilesToSqlite(List.of(xmlFile), dbFile, null, onProgress);
    }

    /**
     * Reads one or more ETSI RetainedData XML files and imports all of their
     * {@code ResponseRecord} entries into a single, freshly created SQLite
     * database (rather than one database per file).
     *
     * <p>Each imported row's {@code source_file} column is set to the name
     * of the XML file it came from (without its file extension), so the
     * origin of every single record stays identifiable even after merging
     * several requests into one database.</p>
     *
     * @param xmlFiles    the ETSI XML files to import, in order
     * @param dbFile      target SQLite database (will be created/overwritten)
     * @param onFileStart optional callback fired once per file, before it is
     *                    read; may be {@code null}
     * @param onProgress  optional progress callback, fired per file with
     *                     that file's own record count; may be {@code null}
     * @throws Exception on any parsing / database error
     */
    public static void importXmlFilesToSqlite(List<File> xmlFiles, File dbFile, FileProgressCallback onFileStart,
            ProgressCallback onProgress) throws Exception {

        if (xmlFiles == null || xmlFiles.isEmpty()) {
            throw new IllegalArgumentException("xmlFiles must not be empty");
        }

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

            int fileCount = xmlFiles.size();
            for (int fi = 0; fi < fileCount; fi++) {
                File xmlFile = xmlFiles.get(fi);
                if (onFileStart != null) {
                    onFileStart.onFileStart(fi + 1, fileCount, xmlFile.getName());
                }

                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                factory.setNamespaceAware(true);
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document doc = builder.parse(xmlFile);
                Element root = doc.getDocumentElement();

                long requestId = importRequestMetadata(conn, root);
                String sourceFile = stripExtension(xmlFile.getName());

                // "*" rather than NS_URI: real-world exports use different
                // schema versions (e.g. v1.26.1 vs. v1.28.1) under different
                // namespace URIs -- see firstChildByTag()/findDescendant()
                // below for why matching is namespace-agnostic throughout.
                NodeList records = root.getElementsByTagNameNS("*", "ResponseRecord");
                int total = records.getLength();

                try (PreparedStatement ps = conn.prepareStatement(INSERT_RECORD_SQL)) {
                    int pending = 0;
                    for (int i = 0; i < total; i++) {
                        // A single ResponseRecord can yield more than one row: voice-call/
                        // SMS records carry one row per party leg (A-/B-Teilnehmer), see
                        // bindRecord(). addBatch() is called inside bindRecord() itself for
                        // each row produced, not here.
                        pending += bindRecord(ps, requestId, (Element) records.item(i), sourceFile);

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
    }

    /** Strips the last {@code "."}-delimited extension off a file name, if any. */
    private static String stripExtension(String fileName) {
        int dot = fileName.lastIndexOf('.');
        return dot > 0 ? fileName.substring(0, dot) : fileName;
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

        if (requestNumber == null || cspId == null || timestamp == null) { return -1; }

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
    private static int bindRecord(PreparedStatement ps, long requestId, Element rec, String sourceFile) throws SQLException {

        final String tsuPath = "recordPayload/telephonyRecord/telephonyServiceUsage";

        Integer recordNumber = parseIntOrNull(findText(rec, "recordNumber"));

        String otherInfo = findText(rec, "additionalInformation/otherInformation");
        Map<String, String> oi = parseAdditionalInfo(otherInfo);
        String callIndicator = oi.get("Call Indikator");
        String callActionCode = oi.get("Call Action Code");
        String callSubtype = oi.get("Call Subtype");
        // Key spelling varies between export versions: "Session Id" (space,
        // seen in v1.28.1 exports) vs. "Session-ID" (hyphen, seen in
        // v1.26.1 exports) -- accept either so the column isn't silently
        // left NULL depending on which schema version produced the file.
        String sessionId = oi.containsKey("Session Id") ? oi.get("Session Id") : oi.get("Session-ID");
        String typeOfDataExtra = oi.get("TypeOfData");

        // tKG96-style otherInformation carries a larger key set on top of the
        // four above (seen in real-world exports, e.g. mobile-data/roaming
        // sessions: APN/domain, private/public IP, internal record-type
        // codes, and a human-readable service name). Not every export uses
        // these — oi.get() returns null for keys that aren't present, which
        // is exactly what's wanted here.
        String aDomainSpvApn = oi.get("A_DOMAIN_SPV_APN");
        String aPrivateIpv4 = oi.get("A_PRIVATE_IPV4");
        String aPublicIpv6 = oi.get("A_PUPLIC_IPV6");
        String datensatzart = oi.get("DATENSATZART");
        String ctt = oi.get("CTT");
        String serviceTypeCode = oi.get("type");
        String dataType = oi.get("dataType");
        String supplServices = oi.get("suppl_Services");
        String serviceName = oi.get("serviceName");
        String aPartyCellOtherHSRs = oi.get("aPartyCellOtherHSRs");
        Integer offsetBeginn = parseIntOrNull(oi.get("Offset Beginn"));
        Integer offsetEnde = parseIntOrNull(oi.get("Offset Ende"));
        String quelle = oi.get("Quelle");

        String natCc2 = findText(rec, "nationalRecordPayload/countryCode");
        String natHid2 = findText(rec, "nationalRecordPayload/headerID");
        Element todElem = findElement(rec, "nationalRecordPayload/typeOfData");
        String typeOfData = firstChildLocalName(todElem);

        Element ntsuElem = findElement(rec, tsuPath + "/nationalTelephonyServiceUsage");
        if (ntsuElem != null) {
            bindNationalUsageRow(ps, requestId, rec, tsuPath, ntsuElem, recordNumber, otherInfo,
                    callIndicator, callActionCode, callSubtype, sessionId, typeOfDataExtra,
                    natCc2, natHid2, typeOfData, aDomainSpvApn, aPrivateIpv4, aPublicIpv6, datensatzart, ctt,
                    serviceTypeCode, dataType, supplServices, serviceName, aPartyCellOtherHSRs,
                    offsetBeginn, offsetEnde, quelle, sourceFile);
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
                        natCc2, natHid2, typeOfData, aDomainSpvApn, aPrivateIpv4, aPublicIpv6, datensatzart, ctt,
                        serviceTypeCode, dataType, supplServices, serviceName, aPartyCellOtherHSRs,
                        offsetBeginn, offsetEnde, quelle, sourceFile);
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
                    null, null, aDomainSpvApn, otherInfo, callIndicator, callActionCode, callSubtype,
                    sessionId, typeOfDataExtra, natCc2, natHid2, typeOfData, null,
                    aPrivateIpv4, aPublicIpv6, datensatzart, ctt, serviceTypeCode, dataType, supplServices,
                    serviceName, aPartyCellOtherHSRs, offsetBeginn, offsetEnde, quelle, /* beamWidth */ null, sourceFile);
            rows = 1;
        }
        return rows;
    }

    /** GPRS / data-session shape: one row, sourced from {@code nationalTelephonyServiceUsage}. */
    private static void bindNationalUsageRow(PreparedStatement ps, long requestId, Element rec, String tsuPath,
            Element ntsuElem, Integer recordNumber, String otherInfo, String callIndicator, String callActionCode,
            String callSubtype, String sessionId, String typeOfDataExtra, String natCc2, String natHid2,
            String typeOfData, String aDomainSpvApn, String aPrivateIpv4, String aPublicIpv6, String datensatzart,
            String ctt, String serviceTypeCode, String dataType, String supplServices, String serviceName,
            String aPartyCellOtherHSRs, Integer offsetBeginn, Integer offsetEnde, String quelle, String sourceFile)
            throws SQLException {

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
        Integer azimuth = parseIntOrNull(findText(rec, ia + "/location/extendedLocation/spot/gsmLocation/geoCoordinates/azimuth"));
        // Fallback: some providers encode location in decimal degrees (<geoCoordinatesDec>) instead of DMS (<geoCoordinates>)
        if (latRaw == null) {
            final String locDec = ia + "/location/extendedLocation/spot/gsmLocation/geoCoordinatesDec";
            latRaw = findText(rec, locDec + "/latitudeDec");
            lonRaw = findText(rec, locDec + "/longitudeDec");
            if (datumElem == null) datumElem = findElement(rec, locDec + "/mapDatum");
            if (azimuth == null) azimuth = parseIntOrNull(findText(rec, locDec + "/azimuth"));
        }
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
        // tKG96-style exports carry the APN/domain in otherInformation
        // ("A_DOMAIN_SPV_APN") instead of (or in addition to) the XML
        // gprsInformation/aPN element -- only used when the XML element
        // itself didn't supply a value, never overwrites it.
        if (apn == null) apn = aDomainSpvApn;

        // Hybrid shape confirmed in real-world v1.26.1 exports: the
        // internetAccess branch only ever carries naDeviceId/gprsInformation
        // (iMSI/aPN) and has NO interval/location children at all (verified:
        // 1327/1327 internetAccess elements in a real sample file had zero
        // inline <location>). The actual session timing and cell location for
        // these records instead live in a sibling partyInformation/
        // TelephonyPartyInformation element under the same telephonyServiceUsage
        // -- the very same element/paths bindPartyLegRow() below reads for the
        // voice/SMS shape. Without this fallback, start_time/end_time/duration/
        // lat/lon/uli (and the derived cell_* columns) stayed NULL for ~94% of
        // that file's records even after the namespace fix, with no exception
        // to flag it. Only fills in what internetAccess didn't already provide.
        Element hybridParty = findElement(rec, tsuPath + "/partyInformation/TelephonyPartyInformation");
        if (hybridParty != null) {
            final String hloc = "detailedLocation/cellInformation";
            if (start == null) start = parseEtsiTimestamp(findText(hybridParty, "communicationTime/startTime"));
            if (end == null) end = parseEtsiTimestamp(findText(hybridParty, "communicationTime/endTime"));
            if (duration == null) duration = parseIntOrNull(findText(hybridParty, "communicationTime/durationTime"));
            if (latRaw == null) latRaw = findText(hybridParty, hloc + "/extendedLocation/spot/gsmLocation/geoCoordinates/latitude");
            if (lonRaw == null) lonRaw = findText(hybridParty, hloc + "/extendedLocation/spot/gsmLocation/geoCoordinates/longitude");
            if (datumElem == null) datumElem = findElement(hybridParty, hloc + "/extendedLocation/spot/gsmLocation/geoCoordinates/mapDatum");
            if (azimuth == null) azimuth = parseIntOrNull(findText(hybridParty, hloc + "/extendedLocation/spot/gsmLocation/geoCoordinates/azimuth"));
            // Fallback: decimal degrees
            if (latRaw == null) {
                final String hlocDec = hloc + "/extendedLocation/spot/gsmLocation/geoCoordinatesDec";
                latRaw = findText(hybridParty, hlocDec + "/latitudeDec");
                lonRaw = findText(hybridParty, hlocDec + "/longitudeDec");
                if (datumElem == null) datumElem = findElement(hybridParty, hlocDec + "/mapDatum");
                if (azimuth == null) azimuth = parseIntOrNull(findText(hybridParty, hlocDec + "/azimuth"));
            }
            if (uli == null) uli = findText(hybridParty, hloc + "/userLocationInformation");
            if (deviceId == null) deviceId = findText(hybridParty, "iMEI");
            if (imsi == null) imsi = findText(hybridParty, "iMSI");
            if (msisdn == null) msisdn = findText(hybridParty, "partyNumber");
        }
        String datum = firstChildLocalName(datumElem);
        // beamWidth lives under detailedLocation/transmitterDetails, a sibling of
        // cellInformation. Present only in the hybrid/partyInformation shape;
        // the pure internetAccess-only shape has no detailedLocation at all.
        Integer beamWidth = (hybridParty != null)
                ? parseIntOrNull(findText(hybridParty, "detailedLocation/transmitterDetails/beamWidth"))
                : null;

        Double latDec = parseAnyCoordinate(latRaw);
        Double lonDec = parseAnyCoordinate(lonRaw);

        writeRow(ps, requestId, recordNumber, partyType, natCc, natHid, nwAccess, start, end, duration, deviceId,
                latRaw, lonRaw, latDec, lonDec, datum, azimuth, uli, imsi, msisdn, apn, otherInfo,
                callIndicator, callActionCode, callSubtype, sessionId, typeOfDataExtra, natCc2, natHid2,
                typeOfData, /* partyRole */ null,
                aPrivateIpv4, aPublicIpv6, datensatzart, ctt, serviceTypeCode, dataType, supplServices,
                serviceName, aPartyCellOtherHSRs, offsetBeginn, offsetEnde, quelle, beamWidth, sourceFile);
    }

    /**
     * Voice-call / SMS shape: one row per party leg ({@code party} is a
     * single {@code TelephonyPartyInformation} taken directly from
     * {@code telephonyServiceUsage/partyInformation}, not from a
     * {@code nationalTelephonyServiceUsage} sibling).
     */
    private static void bindPartyLegRow(PreparedStatement ps, long requestId, Element party, Integer recordNumber,
            String otherInfo, String callIndicator, String callActionCode, String callSubtype, String sessionId,
            String typeOfDataExtra, String natCc2, String natHid2, String typeOfData, String aDomainSpvApn,
            String aPrivateIpv4, String aPublicIpv6, String datensatzart, String ctt, String serviceTypeCode,
            String dataType, String supplServices, String serviceName, String aPartyCellOtherHSRs,
            Integer offsetBeginn, Integer offsetEnde, String quelle, String sourceFile) throws SQLException {

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
        Integer azimuth = parseIntOrNull(findText(party, loc + "/extendedLocation/spot/gsmLocation/geoCoordinates/azimuth"));
        // Fallback: decimal degrees
        if (latRaw == null) {
            final String locDec = loc + "/extendedLocation/spot/gsmLocation/geoCoordinatesDec";
            latRaw = findText(party, locDec + "/latitudeDec");
            lonRaw = findText(party, locDec + "/longitudeDec");
            if (datumElem == null) datumElem = findElement(party, locDec + "/mapDatum");
            if (azimuth == null) azimuth = parseIntOrNull(findText(party, locDec + "/azimuth"));
        }
        String datum = firstChildLocalName(datumElem);
        String uli = findText(party, loc + "/userLocationInformation");

        Double latDec = parseAnyCoordinate(latRaw);
        Double lonDec = parseAnyCoordinate(lonRaw);
        Integer beamWidth = parseIntOrNull(findText(party, "detailedLocation/transmitterDetails/beamWidth"));

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

        // No gprsInformation/aPN element exists for this shape, but tKG96-style
        // exports can still carry the APN/domain in otherInformation
        // ("A_DOMAIN_SPV_APN") -- use it the same way bindNationalUsageRow does.
        String apn = aDomainSpvApn;

        writeRow(ps, requestId, recordNumber, partyType, /* natCc */ null, /* natHid */ null, nwAccess, start, end,
                duration, deviceId, latRaw, lonRaw, latDec, lonDec, datum, azimuth, uli, imsi, msisdn,
                apn, otherInfo, callIndicator, callActionCode, callSubtype, sessionId, typeOfDataExtra,
                natCc2, natHid2, typeOfData, partyRole,
                aPrivateIpv4, aPublicIpv6, datensatzart, ctt, serviceTypeCode, dataType, supplServices,
                serviceName, aPartyCellOtherHSRs, offsetBeginn, offsetEnde, quelle, beamWidth, sourceFile);
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
            String typeOfDataExtra, String natCc2, String natHid2, String typeOfData, String partyRole,
            String aPrivateIpv4, String aPublicIpv6, String datensatzart, String ctt, String serviceTypeCode,
            String dataType, String supplServices, String serviceName, String aPartyCellOtherHSRs,
            Integer offsetBeginn, Integer offsetEnde, String quelle, Integer beamWidth, String sourceFile)
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
        setNullableString(ps, idx++, deviceModel);
        setNullableString(ps, idx++, aPrivateIpv4);
        setNullableString(ps, idx++, aPublicIpv6);
        setNullableString(ps, idx++, datensatzart);
        setNullableString(ps, idx++, ctt);
        setNullableString(ps, idx++, serviceTypeCode);
        setNullableString(ps, idx++, dataType);
        setNullableString(ps, idx++, supplServices);
        setNullableString(ps, idx++, serviceName);
        setNullableString(ps, idx++, aPartyCellOtherHSRs);
        setNullableInt(ps, idx++, offsetBeginn);
        setNullableInt(ps, idx++, offsetEnde);
        setNullableString(ps, idx++, quelle);
        setNullableInt(ps, idx++, beamWidth);
        setNullableString(ps, idx, sourceFile);
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

    /**
     * Matches purely on the element's local name, ignoring its namespace
     * URI. Real-world ETSI exports have been observed under at least two
     * different schema versions/namespaces ({@code v1.26.1} and
     * {@code v1.28.1}, confirmed against actual exports rather than the
     * XSD alone), with otherwise-identical element structure. Since every
     * lookup here already follows an explicit "/"-separated path of local
     * element names (see {@link #findElement}), matching on local name
     * alone is enough to resolve the right element regardless of which
     * schema version produced the document — and avoids every field
     * silently coming back {@code null} (as happened for {@code v1.26.1}
     * exports while this matched strictly against the {@code v1.28.1}
     * {@link #NS_URI}).
     */
    private static Element firstChildByTag(Element parent, String localName) {
        if (parent == null) return null;
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node n = children.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
                Element e = (Element) n;
                if (localName.equals(e.getLocalName())) {
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
            if (localName.equals(e.getLocalName())) {
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

        // Some real-world exports (v1.26.1, hybrid NTSU+partyInformation shape)
        // append decimal fractional seconds to the integer DMS string, e.g.
        // "N513746.02" (= 51°37'46.02") or "E0133743.91" (= 13°37'43.91").
        // Split on the decimal point so the length check below only sees the
        // integer digits, then fold the fraction back in during degree conversion.
        double fracSec = 0.0;
        int dotIdx = digits.indexOf('.');
        if (dotIdx >= 0) {
            String fracStr = digits.substring(dotIdx); // ".02"
            try { fracSec = Double.parseDouble("0" + fracStr); } catch (NumberFormatException ignored) { }
            digits = digits.substring(0, dotIdx);
        }

        if (hemi == 'N' || hemi == 'S') {
            if (digits.length() != 6) return null;
            deg = Integer.parseInt(digits.substring(0, 2));
            min = Integer.parseInt(digits.substring(2, 4));
            sec = Integer.parseInt(digits.substring(4, 6));
        } else if (hemi == 'E' || hemi == 'W') {
            // Allow exactly one optional leading-zero omission (6 → 7 digits).
            // Do NOT pad shorter strings: "013" from "E013.7655" (decimal degrees)
            // must fall through to parseDecDegCoordinate, not be mis-parsed as DMS.
            if (digits.length() == 6) digits = "0" + digits;
            if (digits.length() != 7) return null;
            deg = Integer.parseInt(digits.substring(0, 3));
            min = Integer.parseInt(digits.substring(3, 5));
            sec = Integer.parseInt(digits.substring(5, 7));
        } else {
            return null;
        }

        double decimal = deg + min / 60.0 + (sec + fracSec) / 3600.0;
        if (hemi == 'S' || hemi == 'W') {
            decimal = -decimal;
        }
        return Math.round(decimal * 1_000_000.0) / 1_000_000.0;
    }

    /**
     * Converts a decimal-degree coordinate string as used by
     * {@code <geoCoordinatesDec>} (e.g. {@code N51.5046}, {@code E013.7854})
     * into a signed decimal degree value, rounded to 6 decimal places.
     * Returns {@code null} if the input cannot be parsed.
     */
    static Double parseDecDegCoordinate(String raw) {
        if (raw == null) return null;
        raw = raw.trim();
        if (raw.isEmpty()) return null;
        char hemi = Character.toUpperCase(raw.charAt(0));
        if (hemi != 'N' && hemi != 'S' && hemi != 'E' && hemi != 'W') return null;
        try {
            double val = Double.parseDouble(raw.substring(1));
            if (hemi == 'S' || hemi == 'W') val = -val;
            return Math.round(val * 1_000_000.0) / 1_000_000.0;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Tries DMS parsing first ({@link #parseDmsCoordinate}); if that returns
     * {@code null} (e.g. for a decimal-degree value like {@code N51.5046}),
     * falls back to {@link #parseDecDegCoordinate}. Handles both
     * {@code <geoCoordinates>} and {@code <geoCoordinatesDec>} raw values.
     */
    static Double parseAnyCoordinate(String raw) {
        if (raw == null) return null;
        Double dms = parseDmsCoordinate(raw);
        return (dms != null) ? dms : parseDecDegCoordinate(raw);
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
                result.put(m.group(1), normalizeLeer(m.group(2)));
            }
        }
        return result;
    }

    /**
     * Some tKG96-style exports use the literal placeholder text {@code <leer>}
     * ("empty") for a key instead of omitting it entirely (e.g.
     * {@code "dataType"="<leer>"}, {@code "suppl_Services"="<leer>"}).
     * Normalize that to {@code null} so it isn't stored as literal placeholder
     * text in the database.
     */
    private static String normalizeLeer(String value) {
        return (value == null || value.trim().equalsIgnoreCase("<leer>")) ? null : value;
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
