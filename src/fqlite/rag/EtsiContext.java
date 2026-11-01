package fqlite.rag;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Java-Port von {@code etsi_context.py} (SQLiteForensicsModel).
 *
 * <p>Das Modell forensic_sql_ETSI_CDR_V3 wurde mit einem festen Schema-Kontext
 * (annotierte DDL von request_metadata + response_records) plus einem
 * automatisch aus der DB ermittelten Anreicherungsblock trainiert und evaluiert.
 * Damit die Genauigkeit aus der Evaluation im Plugin erreicht wird, muss der
 * Prompt <b>zeichengenau</b> denselben Kontext enthalten. Diese Klasse erzeugt
 * ihn und legt die vier Hilfs-Views an.</p>
 *
 * <p>Die Views werden ausschliesslich in der In-Memory-Arbeitskopie angelegt;
 * die Original-Beweisdatei wird nie veraendert.</p>
 *
 * <p>WICHTIG: TRAINING_CONTEXT, VIEW_DDL und das Format von
 * {@link #enrichmentBlock} nicht "aufraeumen" — jede Abweichung gegenueber
 * etsi_context.py verschlechtert die Modellleistung. Generiert aus
 * lighthouse_etsi_cdr_training_dataset.json / etsi_context.py.</p>
 */
public final class EtsiContext {

    private EtsiContext() { }

    /** Wortgleich der "context"-Eintrag des Trainingsdatensatzes. */
    public static final String TRAINING_CONTEXT =
            "            -- Database: case_merged.db (SQLite, importiert ueber Lighthouse/FQLite EtsiXmlImporter)\n" +
            "            -- Quelle: ETSI TS 102 657 RetainedData-XML-Exporte (Vorratsdaten-/CDR-Auskunft) eines\n" +
            "            -- Netzbetreibers; ein Datensatz je importierter Quelldatei in request_metadata,\n" +
            "            -- je Partei-Bein (Anruf/SMS) bzw. je Session (GPRS) eine Zeile in response_records.\n" +
            "            CREATE TABLE request_metadata (\n" +
            "                id INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
            "                country_code TEXT,\n" +
            "                organisation_id TEXT,\n" +
            "                request_number TEXT UNIQUE,\n" +
            "                csp_id TEXT,\n" +
            "                timestamp TEXT,\n" +
            "                imported_at TEXT NOT NULL\n" +
            "            );\n" +
            "\n" +
            "            CREATE TABLE response_records (\n" +
            "                id INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
            "                request_id INTEGER,                 -- FK -> request_metadata.id\n" +
            "                record_number INTEGER,\n" +
            "                party_type TEXT,                     -- 'GPRS' | 'MOC' (abgehender Anruf) | 'MTC' (ankommender Anruf) | 'MOM' (SMS)\n" +
            "                national_country_code TEXT,\n" +
            "                national_header_id TEXT,\n" +
            "                nw_access_type TEXT,                 -- z.B. 'mobilePacketData', 'voiceCall'\n" +
            "                start_time DATETIME,                 -- 'YYYY-MM-DD HH:MM:SS'\n" +
            "                end_time DATETIME,\n" +
            "                duration_seconds INTEGER,\n" +
            "                na_device_id TEXT,                    -- IMEI des Endgeraets\n" +
            "                latitude_raw TEXT,                    -- Grad/Minuten/Sekunden, z.B. 'N505638'\n" +
            "                longitude_raw TEXT,                   -- z.B. 'E0140849'\n" +
            "                latitude_dec REAL,                     -- WGS84, dezimal\n" +
            "                longitude_dec REAL,                    -- WGS84, dezimal\n" +
            "                map_datum TEXT,\n" +
            "                azimuth INTEGER,                       -- Ausrichtung der Sendeantenne in Grad (0-359)\n" +
            "                user_location_info TEXT,               -- roher 3GPP ULI-Hexstring\n" +
            "                imsi TEXT,\n" +
            "                msisdn TEXT,                            -- Rufnummer\n" +
            "                apn TEXT,\n" +
            "                other_information TEXT,\n" +
            "                call_indicator TEXT,\n" +
            "                call_action_code TEXT,\n" +
            "                call_subtype TEXT,\n" +
            "                session_id TEXT,\n" +
            "                type_of_data_extra TEXT,\n" +
            "                nat_country_code TEXT,\n" +
            "                nat_header_id TEXT,\n" +
            "                type_of_data TEXT,\n" +
            "                party_role TEXT,\n" +
            "                cell_mcc INTEGER,                       -- Mobile Country Code der Funkzelle\n" +
            "                cell_mnc INTEGER,                       -- Mobile Network Code der Funkzelle\n" +
            "                cell_lac_tac INTEGER,                   -- Location/Tracking Area Code\n" +
            "                cell_ci INTEGER,                        -- Cell-ID\n" +
            "                device_brand TEXT,                      -- per TAC-Lookup ermittelt\n" +
            "                device_model TEXT,\n" +
            "                source_file TEXT                        -- Name der importierten Quelldatei (Mehrquellen-Zusammenfuehrung)\n" +
            "            );";

    public static final Map<String, String> VIEW_DDL = new LinkedHashMap<>();
    public static final Map<String, String> VIEW_DOC = new LinkedHashMap<>();
    static {
        VIEW_DDL.put("call_pairs", "CREATE VIEW call_pairs AS\nSELECT a.source_file, a.record_number,\n       a.msisdn AS caller, b.msisdn AS callee,\n       a.imsi AS caller_imsi, b.imsi AS callee_imsi,\n       a.na_device_id AS caller_imei, b.na_device_id AS callee_imei,\n       a.start_time, a.end_time, a.duration_seconds,\n       a.cell_lac_tac AS caller_lac, a.cell_ci AS caller_ci,\n       b.cell_lac_tac AS callee_lac, b.cell_ci AS callee_ci\nFROM response_records a JOIN response_records b\n  ON a.record_number = b.record_number AND a.source_file = b.source_file\nWHERE a.party_type = 'MOC' AND b.party_type = 'MTC'");
        VIEW_DDL.put("sms_pairs", "CREATE VIEW sms_pairs AS\nSELECT a.source_file, a.record_number,\n       a.msisdn AS sender, b.msisdn AS recipient,\n       a.imsi AS sender_imsi, a.start_time,\n       a.cell_lac_tac AS sender_lac, a.cell_ci AS sender_ci\nFROM response_records a JOIN response_records b\n  ON a.record_number = b.record_number AND a.source_file = b.source_file\nWHERE a.party_type = 'MOM' AND a.party_role = 'smsOriginator'\n  AND b.party_type = 'MOM' AND b.party_role = 'smsRecipient'");
        VIEW_DDL.put("contacts", "CREATE VIEW contacts AS\nSELECT caller AS msisdn, callee AS partner, 'call' AS kind, 'out' AS direction,\n       start_time, duration_seconds, source_file FROM call_pairs\nUNION ALL\nSELECT callee, caller, 'call', 'in', start_time, duration_seconds, source_file FROM call_pairs\nUNION ALL\nSELECT sender, recipient, 'sms', 'out', start_time, NULL, source_file FROM sms_pairs\nUNION ALL\nSELECT recipient, sender, 'sms', 'in', start_time, NULL, source_file FROM sms_pairs");
        VIEW_DDL.put("record_gaps", "CREATE VIEW record_gaps AS\nSELECT msisdn, prev_start_time, start_time,\n       (julianday(start_time) - julianday(prev_start_time)) * 86400 AS gap_seconds\nFROM (SELECT msisdn, start_time,\n             LAG(start_time) OVER (PARTITION BY msisdn ORDER BY start_time) AS prev_start_time\n      FROM response_records WHERE msisdn IS NOT NULL)\nWHERE prev_start_time IS NOT NULL");
        VIEW_DOC.put("call_pairs", "ein Anruf = eine Zeile (Anrufer + Angerufener)");
        VIEW_DOC.put("sms_pairs", "eine SMS = eine Zeile (Absender + Empfaenger)");
        VIEW_DOC.put("contacts", "alle Kommunikationspartner je Rufnummer; kind='call'|'sms', direction='out'|'in'");
        VIEW_DOC.put("record_gaps", "Abstand in Sekunden zum vorherigen Datensatz derselben Rufnummer");
    }

    private static final String[] CATEGORICAL = {"source_file", "party_type", "party_role", "nw_access_type"};
    private static final String IND = "            ";

    /** True, wenn die DB das ETSI-CDR-Schema des Lighthouse-Importers enthaelt. */
    public static boolean isEtsiCdrDatabase(Connection conn) {
        if (conn == null) return false;
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
                     + "AND name IN ('response_records','request_metadata')")) {
            return rs.next() && rs.getInt(1) == 2;
        } catch (SQLException e) {
            return false;
        }
    }

    /** Legt die Hilfs-Views an (idempotent). Nur auf der In-Memory-Kopie aufrufen! */
    public static void createViews(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            for (Map.Entry<String, String> e : VIEW_DDL.entrySet()) {
                st.execute("DROP VIEW IF EXISTS " + e.getKey());
                st.execute(e.getValue());
            }
        }
    }

    /** Vollstaendiger Schema-Kontext wie in Training/Evaluation (enrich_context). */
    public static String buildContext(Connection conn) throws SQLException {
        return rstrip(TRAINING_CONTEXT) + "\n" + enrichmentBlock(conn);
    }

    /** Port von etsi_context.enrichment_block(). */
    public static String enrichmentBlock(Connection conn) throws SQLException {
        List<String> L = new ArrayList<>();
        String lo = null, hi = null;
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT MIN(start_time), MAX(start_time) FROM response_records")) {
            if (rs.next()) { lo = rs.getString(1); hi = rs.getString(2); }
        }
        List<String> days = column(conn,
                "SELECT DISTINCT date(start_time) FROM response_records WHERE start_time IS NOT NULL ORDER BY 1");
        L.add("");
        L.add(IND + "-- \u2500\u2500 Datenbestand dieses Falls (automatisch aus der DB ermittelt) \u2500\u2500");
        L.add(IND + "-- start_time von " + py(lo) + " bis " + py(hi) + "; Kalendertage: " + String.join(", ", days));
        for (String col : CATEGORICAL) {
            List<String> vals = column(conn, "SELECT DISTINCT " + col + " FROM response_records WHERE "
                    + col + " IS NOT NULL ORDER BY 1 LIMIT 25");
            List<String> q = new ArrayList<>();
            for (String v : vals) q.add("'" + v.replace("'", "''") + "'");
            L.add(IND + "-- " + col + ": " + String.join(", ", q));
        }
        L.add(IND + "-- \u2500\u2500 Hinweise \u2500\u2500");
        L.add(IND + "-- Uhrzeit unabhaengig vom Datum: time(start_time) > 'HH:MM:SS'");
        L.add(IND + "-- Kalendertag: date(start_time) = 'YYYY-MM-DD'; Anzahl Tage: COUNT(DISTINCT date(start_time))");
        L.add(IND + "-- Zeitpunkt/Zeitfenster an einem Tag: start_time BETWEEN 'YYYY-MM-DD HH:MM:SS' AND 'YYYY-MM-DD HH:MM:SS'");
        L.add(IND + "-- Ein Anruf = zwei Zeilen (MOC + MTC) mit gleicher record_number und source_file;");
        L.add(IND + "-- eine SMS = zwei MOM-Zeilen (party_role smsOriginator + smsRecipient), ebenso verknuepft.");
        L.add(IND + "-- Rang N (z.B. zweithaeufigste): ORDER BY n DESC LIMIT 1 OFFSET N-1");
        Set<String> present = new HashSet<>(column(conn, "SELECT name FROM sqlite_master WHERE type='view'"));
        present.retainAll(VIEW_DDL.keySet());
        if (!present.isEmpty()) {
            L.add(IND + "-- \u2500\u2500 Hilfs-Views (read-only, in der DB vorhanden) \u2500\u2500");
            for (String name : VIEW_DDL.keySet()) {
                if (!present.contains(name)) continue;
                List<String> cols = new ArrayList<>();
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("PRAGMA table_info(" + name + ")")) {
                    while (rs.next()) cols.add(rs.getString(2));
                }
                L.add(IND + "CREATE VIEW " + name + " (" + String.join(", ", cols) + ");  -- " + VIEW_DOC.get(name));
            }
        }
        return String.join("\n", L) + "\n";
    }

    private static List<String> column(Connection conn, String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String v = rs.getString(1);
                if (v != null) out.add(v);
            }
        }
        return out;
    }

    /** Python-str()-Aequivalent fuer NULL-Werte. */
    private static String py(String s) { return s == null ? "None" : s; }

    private static String rstrip(String s) {
        int i = s.length();
        while (i > 0 && Character.isWhitespace(s.charAt(i - 1))) i--;
        return s.substring(0, i);
    }
}
