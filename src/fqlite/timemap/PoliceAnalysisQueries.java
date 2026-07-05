package fqlite.timemap;

/**
 * Fixed, reviewed SQL templates for the police investigative focus areas
 * ("Untersuchungsschwerpunkte der Polizei zu Funkmastdaten") that analysts
 * commonly need when working a retained-data export.
 *
 * <p>Mirrors the {@code buildColocationSql}/{@code buildColocationDetailSql}
 * pattern in {@link fqlite.rag.RAGPipeline}: every query here is a fixed,
 * hand-reviewed Java template — never LLM-generated — so results stay
 * deterministic and forensically auditable. {@link PoliceAnalysisPane} is
 * the UI that lets an analyst pick one of these and run it.</p>
 *
 * <p>All queries run against the fixed {@code response_records} schema (see
 * {@link fqlite.importer.EtsiXmlImporter}). Two schema facts shape several
 * of the templates below:</p>
 * <ul>
 *   <li><b>"Funkzelle" / cell site</b> is represented by the resolved
 *       {@code (latitude_dec, longitude_dec)} pair — the schema has no
 *       separate cell/site-ID column, so any "which participants share a
 *       cell" question groups by that coordinate pair (same convention
 *       already used by {@code buildColocationSql}).</li>
 *   <li>{@code na_device_id} is the handset's own id (IMEI in real
 *       exports), distinct from {@code imsi} (the SIM's identity) — so
 *       "device swap" means same IMSI / different {@code na_device_id},
 *       and "SIM swap" means same {@code na_device_id} / different IMSI.</li>
 * </ul>
 *
 * <p><b>Known limitation:</b> {@code party_type} (used for the A-/B-/C-
 * Teilnehmer view) and the country-code heuristic (used for the in-/
 * ausländisch view) are both best-effort: no real ETSI export or XSD for
 * the non-GPRS {@code telephonyServiceUsage} branches was available to
 * confirm the exact value vocabulary, so those two views surface the raw
 * values/heuristic rather than asserting a mapping that hasn't been
 * verified against real data. The country *name* shown for foreign numbers
 * (via {@link CountryCallingCodeLookup}) is itself a hand-maintained table
 * of ITU-T E.164 calling codes — covers essentially all assigned codes, but
 * numbers under the shared NANP ("1") or Russia/Kasachstan ("7") codes only
 * resolve to the umbrella region, not the specific country.</p>
 */
public final class PoliceAnalysisQueries {

    private PoliceAnalysisQueries() { }

    /** The "Untersuchungsschwerpunkte" an analyst can pick from in {@link PoliceAnalysisPane}. */
    public enum AnalysisType {

        ALL_NUMBERS(
                "Alle Rufnummern",
                "Listet alle in den Datensätzen vorkommenden Rufnummern (MSISDN) auf, "
                        + "mit Anzahl der Treffer und erstem/letztem Auftreten."),

        DOMESTIC_FOREIGN(
                "In-/ausländische Rufnummern",
                "Unterscheidet deutsche von ausländischen Rufnummern. Heuristik: eine "
                        + "Rufnummer beginnt im Rohformat mit der Landesvorwahl ohne '+' "
                        + "(z. B. '49…' für Deutschland) — bei erkannter ausländischer "
                        + "Vorwahl wird der Ländername angezeigt, sonst nur der rohe Präfix."),

        PARTY_ROLE(
                "Teilnehmerrolle (A-/B-/C-Teilnehmer)",
                "Gruppiert nach dem importierten party_type-Rohwert je Rufnummer/IMSI. "
                        + "Hinweis: die genaue A-/B-/C-Zuordnung dieses Feldes ist anhand der "
                        + "vorliegenden Testdaten nicht zweifelsfrei verifiziert — die Rohwerte "
                        + "werden daher unverändert angezeigt."),

        PARTICIPANTS_PER_CELL(
                "Teilnehmer in der Funkzelle",
                "Zeigt je Funkzelle (Standort-Koordinate) die Anzahl und Liste der "
                        + "beteiligten Rufnummern/IMSIs im gewählten Zeitraum."),

        CONNECTION_TYPE(
                "Verbindungstyp (Telefon/SMS/GPRS)",
                "Klassifiziert jeden Datensatz anhand von call_indicator/nw_access_type "
                        + "als Telefonie, Kurzmitteilung (SMS) oder GPRS/Datenverbindung."),

        DEVICE_SIM_SWAP(
                "Wechsler (Endgeräte/SIM-Karten)",
                "Findet IMSIs, die mit mehreren unterschiedlichen Geräte-IDs (IMEI) "
                        + "auftreten (Geräte-Wechsel), sowie Geräte-IDs, die mit mehreren "
                        + "unterschiedlichen IMSIs auftreten (SIM-Wechsel)."),

        CROSS_HITS(
                "Kreuztreffer über mehrere Quellen",
                "Findet IMSI/Rufnummer/IMEI, die in mehr als einer Anfrage "
                        + "(request_id, z. B. unterschiedliche Datenquellen/Funkzellenabfragen) "
                        + "auftauchen."),

        ROAMERS(
                "Wanderer (mehrere Funkzellen)",
                "Findet Rufnummern/IMSIs, die im Zeitraum an mehr als einer Funkzelle "
                        + "(Standort-Koordinate) registriert waren."),

        NUMBER_BLOCKS(
                "Ähnliche Rufnummern (Rufnummernblöcke)",
                "Gruppiert Rufnummern, die sich nur in den letzten beiden Ziffern "
                        + "unterscheiden, als möglichen gemeinsam vergebenen Rufnummernblock."),

        RECURRING_PAIRS(
                "Wiederkehrende Rufnummernpaare",
                "Findet Rufnummernpaare, die wiederholt (>1×) in derselben Funkzelle "
                        + "innerhalb von 30 Minuten registriert wurden."),

        TWINS(
                "Twins (identische Funkzellen-Menge)",
                "Findet Rufnummernpaare, bei denen beide Teilnehmer exakt dieselbe Menge "
                        + "an Funkzellen im Zeitraum besucht haben."),

        // ---------------------------------------------------------------
        // Erweiterung um die 14 konkreten Ermittlungsfragen — einige davon
        // benötigen eine vom Analysten eingegebene Kennung (requiresIdentifier)
        // und/oder ein Gebiet (requiresArea), siehe PoliceAnalysisPane.
        // ---------------------------------------------------------------

        CELL_SITES(
                "Funkzellen im Datensatz",
                "Listet alle im Datensatz vorkommenden Funkzellen (Standort-"
                        + "Koordinaten) auf, mit Anzahl der Datensätze sowie "
                        + "erstem/letztem Auftreten. Beantwortet 'Welche Gebiete/"
                        + "Funkmasten liegen im Datensatz vor?'."),

        IDENTIFIER_INVENTORY(
                "Alle Kennungen (Rufnummern/IMSI/IMEI)",
                "Listet alle im Datensatz vorkommenden Rufnummern, IMSIs und "
                        + "Geräte-IDs (IMEI) getrennt nach Kennungstyp auf, mit Anzahl "
                        + "der Datensätze sowie erstem/letztem Auftreten. Beantwortet "
                        + "'Welche Telefonnummern/IMEIs/IMSIs liegen im Datensatz vor?'."),

        IDENTIFIER_TIMELINE(
                "Verbindungen einer Kennung (wer/wann/wo)",
                "Zeigt für die eingegebene Rufnummer/IMSI/IMEI alle Datensätze "
                        + "chronologisch mit Funkzelle, Zeit und Verbindungstyp. "
                        + "Beantwortet 'Mit welchen Funkmasten war x zu welcher Zeit "
                        + "verbunden?' sowie 'Gibt es einen Eintrag zu x im Zeitraum y?' "
                        + "(leere Tabelle = kein Treffer).",
                true, false),

        COMMUNICATION_PARTNERS_DETAIL(
                "Gesprächs-/SMS-Partner einer Kennung",
                "Findet zu von der eingegebenen Kennung ausgehenden Telefonaten/"
                        + "SMS den jeweils zugehörigen Gegenpart über die gemeinsame "
                        + "session_id, inkl. dessen Funkzelle und Zeitpunkt. Hinweis: "
                        + "setzt voraus, dass beide Seiten eines Gesprächs im Export "
                        + "dieselbe session_id tragen — ungeprüft, da kein reales "
                        + "Telefonie-Sample vorlag.",
                true, false),

        IDENTIFIER_CO_OCCURRENCE(
                "Weitere Kennungen im Umfeld einer Kennung",
                "Findet weitere Rufnummern/IMSIs/IMEIs, die zur eingegebenen "
                        + "Kennung in Beziehung stehen: im selben Datensatz erfasst, "
                        + "über dieselbe session_id (Gesprächspartner) oder zur "
                        + "gleichen Zeit (±30 Min.) in derselben Funkzelle.",
                true, false),

        TRANSMISSION_COUNT(
                "Anzahl Übertragungen einer Kennung",
                "Zählt die Datensätze der eingegebenen Kennung im gewählten "
                        + "Zeitraum, aufgeschlüsselt nach Verbindungstyp (Telefonie/"
                        + "SMS/GPRS).",
                true, false),

        DEVICE_INFO_LOOKUP(
                "Gerätetyp zu einer IMEI",
                "Zeigt zunächst alle lokal vorliegenden Datensätze zur "
                        + "eingegebenen IMEI und versucht zusätzlich einen Online-"
                        + "Abgleich der TAC (erste 8 Ziffern der IMEI) über die "
                        + "HiCellTek-API, um Hersteller/Modell zu ermitteln. Erfordert "
                        + "einen konfigurierten API-Key (System-Property "
                        + "'fqlite.hicelltek.apikey') und Internetzugriff — ohne beides "
                        + "wird nur der lokale Datensatzbefund angezeigt.",
                true, false),

        FILTER_BY_COUNTRY(
                "Rufnummern aus einem Land",
                "Filtert Rufnummern nach einer eingegebenen Landesvorwahl (z. B. "
                        + "'49' für Deutschland, ohne führendes '+') — Vorwahl als "
                        + "Kennung eingeben; der erkannte Ländername wird in der "
                        + "Ergebnisspalte 'Land' angezeigt. Gleiche Heuristik/"
                        + "Einschränkung wie bei 'In-/ausländische Rufnummern'.",
                true, false),

        COUNTRY_HISTOGRAM(
                "Länder-Übersicht aller Rufnummern",
                "Gruppiert alle Rufnummern nach erkanntem Land (anhand der "
                        + "Landesvorwahl) und zeigt die Anzahl je Land. Gleiche "
                        + "Heuristik/Einschränkung wie bei 'In-/ausländische "
                        + "Rufnummern' — Vorwahlen ohne Eintrag in der Vorwahltabelle "
                        + "erscheinen weiterhin nur mit rohem Präfix."),

        PRESENCE_IN_AREA(
                "Anwesenheit einer Kennung in einem Gebiet",
                "Prüft, ob die eingegebene Kennung im gewählten Zeitraum innerhalb "
                        + "eines Umkreises (Radius in Metern, Standard 500 m) um den "
                        + "eingegebenen Gebiets-Mittelpunkt ('Breitengrad,Längengrad') "
                        + "registriert war. Hinweis: der Umkreis wird als einfache "
                        + "Grad-Bounding-Box angenähert (1° ≈ 111.320 m, ohne "
                        + "Breitengrad-Korrektur der Längengrad-Distanz) — für eine "
                        + "grobe forensische Einordnung ausreichend, aber keine exakte "
                        + "geodätische Distanz.",
                true, true),

        FREQUENT_COMPANION(
                "Häufigster Begleiter einer Kennung",
                "Findet zur eingegebenen Kennung die anderen Rufnummern/IMSIs/"
                        + "IMEIs, die am häufigsten zur gleichen Zeit (±30 Min.) in "
                        + "derselben Funkzelle registriert waren — sortiert nach "
                        + "Häufigkeit.",
                true, false),

        COMM_SAME_CELL(
                "Kommunikationspartner in derselben Funkzelle",
                "Findet Paare von Rufnummern, die über dieselbe session_id als "
                        + "Gesprächs-/SMS-Partner verknüpft sind UND dabei in "
                        + "derselben Funkzelle eingeloggt waren.");

        public final String label;
        public final String description;
        /** True if this analysis needs an analyst-supplied Rufnummer/IMSI/IMEI (or, for FILTER_BY_COUNTRY, a country prefix) in {@link Params#identifier}. */
        public final boolean requiresIdentifier;
        /** True if this analysis additionally needs {@link Params#areaCenter} (and optionally {@link Params#radiusMeters}). */
        public final boolean requiresArea;

        AnalysisType(String label, String description) {
            this(label, description, false, false);
        }

        AnalysisType(String label, String description, boolean requiresIdentifier, boolean requiresArea) {
            this.label = label;
            this.description = description;
            this.requiresIdentifier = requiresIdentifier;
            this.requiresArea = requiresArea;
        }

        @Override
        public String toString() { return label; }
    }

    // =========================================================================
    // Parameters beyond the analysis type itself: an optional time range
    // (item 12) plus, for the identifier-/area-based analyses added later,
    // an analyst-supplied Kennung and/or Gebiet.
    // =========================================================================

    /**
     * Bundles every optional input an analysis may need. Which fields are
     * actually required depends on {@link AnalysisType#requiresIdentifier}
     * and {@link AnalysisType#requiresArea} — {@link PoliceAnalysisPane}
     * enables/validates the corresponding UI fields accordingly.
     */
    public static final class Params {
        /** Inclusive lower time bound ({@code yyyy-MM-dd HH:mm:ss}), or {@code null}/blank for no filter. */
        public String fromIso;
        /** Inclusive upper time bound, same format, or {@code null}/blank for no filter. */
        public String toIso;
        /**
         * Rufnummer, IMSI or IMEI entered by the analyst (matched against
         * {@code msisdn}/{@code imsi}/{@code na_device_id}). For
         * {@link AnalysisType#FILTER_BY_COUNTRY} this is instead a country
         * prefix (e.g. {@code "49"}).
         */
        public String identifier;
        /** Area centre as {@code "Breitengrad,Längengrad"} — only used by {@link AnalysisType#PRESENCE_IN_AREA}. */
        public String areaCenter;
        /** Radius in meters around {@link #areaCenter}; defaults to 500 if {@code null}. */
        public Double radiusMeters;
    }

    // =========================================================================
    // Time filter (item 12: "Filterung vom Datum bis einschließlich Stunde/
    // Minute/Sekunde") — applied as an optional WHERE-clause add-on to *any*
    // of the analyses above, rather than as a standalone query.
    // =========================================================================

    /**
     * Builds an optional {@code AND <prefix>start_time BETWEEN … AND …}
     * clause. Returns {@code ""} (no filter) if either bound is {@code null}
     * or blank.
     *
     * @param fromIso      inclusive lower bound, format {@code yyyy-MM-dd HH:mm:ss}
     * @param toIso        inclusive upper bound, same format
     * @param columnPrefix table alias prefix including the trailing dot
     *                     (e.g. {@code "a."}), or {@code ""} for an
     *                     unqualified column reference
     */
    public static String timeFilterClause(String fromIso, String toIso, String columnPrefix) {
        if (fromIso == null || fromIso.isBlank() || toIso == null || toIso.isBlank()) return "";
        String prefix = columnPrefix == null ? "" : columnPrefix;
        return " AND " + prefix + "start_time BETWEEN '" + escape(fromIso) + "' AND '" + escape(toIso) + "' ";
    }

    private static String escape(String s) {
        return s.replace("'", "''");
    }

    /**
     * Builds an {@code OR}-ed equality match of the given identifier against
     * all three identifier columns (msisdn/imsi/na_device_id), since the
     * analyst may type in any of a Rufnummer, IMSI or IMEI.
     *
     * @param identifier   value entered by the analyst (must not be blank —
     *                     callers gate on {@link AnalysisType#requiresIdentifier})
     * @param columnPrefix table alias prefix including the trailing dot
     *                     (e.g. {@code "a."}), or {@code ""}
     */
    private static String identifierMatchClause(String identifier, String columnPrefix) {
        String p = columnPrefix == null ? "" : columnPrefix;
        String esc = escape(identifier);
        return "(" + p + "msisdn = '" + esc + "' OR " + p + "imsi = '" + esc + "' OR " + p + "na_device_id = '" + esc + "')";
    }

    /**
     * Validates that {@code params.identifier} was actually supplied for an
     * analysis that {@link AnalysisType#requiresIdentifier requires} one.
     */
    private static String requireIdentifier(Params params, AnalysisType type) {
        if (params == null || params.identifier == null || params.identifier.isBlank()) {
            throw new IllegalArgumentException(
                    "Die Auswertung '" + type.label + "' benötigt eine eingegebene Rufnummer/IMSI/IMEI.");
        }
        return params.identifier.trim();
    }

    /**
     * Builds a simple flat-earth bounding box around {@code params.areaCenter}
     * ("lat,lon") with radius {@code params.radiusMeters} (default 500 m).
     * This is a deliberate approximation — see
     * {@link AnalysisType#PRESENCE_IN_AREA}'s Javadoc for the caveat — good
     * enough for a rough forensic "was x roughly here" check, not an exact
     * geodesic distance.
     */
    private static String areaBoundingBoxClause(Params params, AnalysisType type) {
        if (params == null || params.areaCenter == null || params.areaCenter.isBlank()) {
            throw new IllegalArgumentException(
                    "Die Auswertung '" + type.label + "' benötigt ein Gebiet (Format 'Breitengrad,Längengrad').");
        }
        String[] parts = params.areaCenter.split(",");
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "Gebiet bitte im Format 'Breitengrad,Längengrad' angeben, z. B. '52.5200,13.4050'.");
        }
        double lat, lon;
        try {
            lat = Double.parseDouble(parts[0].trim());
            lon = Double.parseDouble(parts[1].trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                    "Gebiet bitte im Format 'Breitengrad,Längengrad' angeben, z. B. '52.5200,13.4050'.");
        }
        double radius = (params.radiusMeters != null) ? params.radiusMeters : 500.0;
        double deltaDeg = radius / 111320.0;
        return " AND latitude_dec BETWEEN " + (lat - deltaDeg) + " AND " + (lat + deltaDeg)
                + " AND longitude_dec BETWEEN " + (lon - deltaDeg) + " AND " + (lon + deltaDeg) + " ";
    }

    // =========================================================================
    // SQL builder
    // =========================================================================

    /**
     * Builds the SQL for the given analysis type using only a time filter.
     * Kept for callers that only ever ran the original 11 (non-identifier,
     * non-area) analyses; delegates to {@link #buildSql(AnalysisType, Params)}.
     *
     * @param type    which analysis to run (must not require an identifier or area)
     * @param fromIso inclusive lower time bound ({@code yyyy-MM-dd HH:mm:ss}),
     *                or {@code null}/blank for "kein Zeitfilter"
     * @param toIso   inclusive upper time bound, same format, or
     *                {@code null}/blank for "kein Zeitfilter"
     */
    public static String buildSql(AnalysisType type, String fromIso, String toIso) {
        Params p = new Params();
        p.fromIso = fromIso;
        p.toIso = toIso;
        return buildSql(type, p);
    }

    /**
     * Builds the SQL for the given analysis type and parameters.
     *
     * @param type   which of the fixed analyses to run
     * @param params time range plus, where {@link AnalysisType#requiresIdentifier}
     *               and/or {@link AnalysisType#requiresArea} are true, the
     *               analyst-supplied Kennung/Gebiet
     * @return a fixed SELECT statement, ready to execute against
     *         {@code response_records} via {@link fqlite.sql.InMemoryDatabase}
     * @throws IllegalArgumentException if a required identifier/area is missing or malformed
     */
    public static String buildSql(AnalysisType type, Params params) {
        String fromIso = params != null ? params.fromIso : null;
        String toIso = params != null ? params.toIso : null;
        String t   = timeFilterClause(fromIso, toIso, "");
        String ta  = timeFilterClause(fromIso, toIso, "a.");
        String tb  = timeFilterClause(fromIso, toIso, "b.");

        if (type.requiresIdentifier) {
            requireIdentifier(params, type);
        }
        if (type.requiresArea) {
            areaBoundingBoxClause(params, type); // validate eagerly so bad input fails before building SQL below
        }

        switch (type) {

            case ALL_NUMBERS:
                return "SELECT msisdn AS Rufnummer, COUNT(*) AS Anzahl_Datensaetze, "
                        + "MIN(start_time) AS Erstmals, MAX(start_time) AS Zuletzt "
                        + "FROM response_records "
                        + "WHERE msisdn IS NOT NULL AND msisdn <> '' " + t
                        + "GROUP BY msisdn "
                        + "ORDER BY Rufnummer;";

            case DOMESTIC_FOREIGN:
                return "SELECT msisdn AS Rufnummer, "
                        + CountryCallingCodeLookup.buildSqlCaseExpression(
                                "msisdn", "'Ausland (Präfix ' || SUBSTR(msisdn,1,3) || '…)'")
                        + " AS Herkunft, "
                        + "COUNT(*) AS Anzahl_Datensaetze "
                        + "FROM response_records "
                        + "WHERE msisdn IS NOT NULL AND msisdn <> '' " + t
                        + "GROUP BY msisdn "
                        + "ORDER BY Herkunft, Rufnummer;";

            case PARTY_ROLE:
                return "SELECT party_type AS Teilnehmerrolle, msisdn AS Rufnummer, imsi AS IMSI, "
                        + "COUNT(*) AS Anzahl_Datensaetze "
                        + "FROM response_records "
                        + "WHERE party_type IS NOT NULL AND party_type <> '' " + t
                        + "GROUP BY party_type, msisdn, imsi "
                        + "ORDER BY Teilnehmerrolle, Rufnummer;";

            case PARTICIPANTS_PER_CELL:
                return "SELECT ROUND(latitude_dec,5) || ', ' || ROUND(longitude_dec,5) AS Funkzelle, "
                        + "COUNT(DISTINCT imsi) AS Anzahl_IMSI, "
                        + "COUNT(DISTINCT msisdn) AS Anzahl_Rufnummern, "
                        + "GROUP_CONCAT(DISTINCT msisdn) AS Rufnummern, "
                        + "MIN(start_time) AS Von, MAX(start_time) AS Bis "
                        + "FROM response_records "
                        + "WHERE latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL " + t
                        + "GROUP BY latitude_dec, longitude_dec "
                        + "ORDER BY Anzahl_Rufnummern DESC;";

            case CONNECTION_TYPE:
                return "SELECT CASE "
                        + "WHEN call_indicator IN ('MOC','MTC') THEN 'Telefonie' "
                        + "WHEN call_indicator IN ('MOM','MTM') THEN 'Kurzmitteilung (SMS)' "
                        + "WHEN call_indicator = 'SGP' OR nw_access_type IN ('mobilePacketData','GPRS') THEN 'GPRS/Datenverbindung' "
                        + "ELSE 'Unbekannt (' || COALESCE(call_indicator, nw_access_type, '?') || ')' "
                        + "END AS Verbindungstyp, "
                        + "COUNT(*) AS Anzahl_Datensaetze "
                        + "FROM response_records "
                        + "WHERE 1=1 " + t
                        + "GROUP BY Verbindungstyp "
                        + "ORDER BY Anzahl_Datensaetze DESC;";

            case DEVICE_SIM_SWAP:
                return "SELECT 'Geräte-Wechsel (gleiche SIM, mehrere Geräte)' AS Wechseltyp, "
                        + "imsi AS Kennung, COUNT(DISTINCT TRIM(na_device_id)) AS Anzahl_Wechsel, "
                        + "GROUP_CONCAT(DISTINCT TRIM(na_device_id)) AS Details "
                        + "FROM response_records "
                        + "WHERE imsi IS NOT NULL AND TRIM(imsi) <> '' AND na_device_id IS NOT NULL AND TRIM(na_device_id) <> '' " + t
                        + "GROUP BY imsi "
                        + "HAVING COUNT(DISTINCT TRIM(na_device_id)) > 1 "
                        + "UNION ALL "
                        + "SELECT 'SIM-Wechsel (gleiches Gerät, mehrere SIM-Karten)' AS Wechseltyp, "
                        + "na_device_id AS Kennung, COUNT(DISTINCT TRIM(imsi)) AS Anzahl_Wechsel, "
                        + "GROUP_CONCAT(DISTINCT TRIM(imsi)) AS Details "
                        + "FROM response_records "
                        + "WHERE na_device_id IS NOT NULL AND TRIM(na_device_id) <> '' AND imsi IS NOT NULL AND TRIM(imsi) <> '' " + t
                        + "GROUP BY na_device_id "
                        + "HAVING COUNT(DISTINCT TRIM(imsi)) > 1 "
                        + "ORDER BY Anzahl_Wechsel DESC;";

            case CROSS_HITS:
                return "SELECT 'IMSI' AS Kennungstyp, imsi AS Kennung, "
                        + "COUNT(DISTINCT request_id) AS Anzahl_Quellen, GROUP_CONCAT(DISTINCT request_id) AS Quellen "
                        + "FROM response_records WHERE imsi IS NOT NULL AND imsi <> '' " + t
                        + "GROUP BY imsi HAVING COUNT(DISTINCT request_id) > 1 "
                        + "UNION ALL "
                        + "SELECT 'Rufnummer' AS Kennungstyp, msisdn AS Kennung, "
                        + "COUNT(DISTINCT request_id) AS Anzahl_Quellen, GROUP_CONCAT(DISTINCT request_id) AS Quellen "
                        + "FROM response_records WHERE msisdn IS NOT NULL AND msisdn <> '' " + t
                        + "GROUP BY msisdn HAVING COUNT(DISTINCT request_id) > 1 "
                        + "UNION ALL "
                        + "SELECT 'IMEI/Geräte-ID' AS Kennungstyp, na_device_id AS Kennung, "
                        + "COUNT(DISTINCT request_id) AS Anzahl_Quellen, GROUP_CONCAT(DISTINCT request_id) AS Quellen "
                        + "FROM response_records WHERE na_device_id IS NOT NULL AND na_device_id <> '' " + t
                        + "GROUP BY na_device_id HAVING COUNT(DISTINCT request_id) > 1 "
                        + "ORDER BY Anzahl_Quellen DESC;";

            case ROAMERS:
                // "Funkzellen" lists every visited site as "<lat>|<lon>" pairs
                // joined by ',' (GROUP_CONCAT's only separator option together
                // with DISTINCT in SQLite) — '|' inside each pair avoids
                // ambiguity with that outer comma. The column is named
                // "Funkzellen" (not "Standorte") so showSelectedRowOnMap()'s
                // startsWith("Funkzelle") convention picks it up; that method
                // also knows to parse this '|'-delimited multi-site format
                // and jump to the first listed site.
                return "SELECT imsi AS IMSI, MIN(msisdn) AS Rufnummer, "
                        + "COUNT(DISTINCT latitude_dec || ',' || longitude_dec) AS Anzahl_Funkzellen, "
                        + "GROUP_CONCAT(DISTINCT ROUND(latitude_dec,5) || '|' || ROUND(longitude_dec,5)) AS Funkzellen, "
                        + "MIN(start_time) AS Von, MAX(start_time) AS Bis "
                        + "FROM response_records "
                        + "WHERE imsi IS NOT NULL AND TRIM(imsi) <> '' AND latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL " + t
                        + "GROUP BY imsi "
                        + "HAVING COUNT(DISTINCT latitude_dec || ',' || longitude_dec) > 1 "
                        + "ORDER BY Anzahl_Funkzellen DESC;";

            case NUMBER_BLOCKS:
                return "SELECT SUBSTR(msisdn,1,LENGTH(msisdn)-2) AS Rufnummernblock, "
                        + "COUNT(DISTINCT msisdn) AS Anzahl_Rufnummern, "
                        + "GROUP_CONCAT(DISTINCT msisdn) AS Rufnummern "
                        + "FROM response_records "
                        + "WHERE msisdn IS NOT NULL AND msisdn <> '' AND LENGTH(msisdn) > 4 " + t
                        + "GROUP BY Rufnummernblock "
                        + "HAVING COUNT(DISTINCT msisdn) > 1 "
                        + "ORDER BY Anzahl_Rufnummern DESC;";

            case RECURRING_PAIRS:
                return "SELECT a.msisdn AS Rufnummer_A, b.msisdn AS Rufnummer_B, "
                        + "COUNT(*) AS Anzahl_gemeinsamer_Funkzellen "
                        + "FROM response_records a "
                        + "JOIN response_records b "
                        + "  ON a.latitude_dec = b.latitude_dec AND a.longitude_dec = b.longitude_dec "
                        + "  AND a.msisdn < b.msisdn "
                        + "  AND ABS(strftime('%s', a.start_time) - strftime('%s', b.start_time)) <= 1800 "
                        + "WHERE a.msisdn IS NOT NULL AND b.msisdn IS NOT NULL "
                        + "  AND a.latitude_dec IS NOT NULL AND a.longitude_dec IS NOT NULL " + ta + tb
                        + "GROUP BY a.msisdn, b.msisdn "
                        + "HAVING COUNT(*) > 1 "
                        + "ORDER BY Anzahl_gemeinsamer_Funkzellen DESC;";

            case TWINS:
                return "WITH cells AS ("
                        + "  SELECT DISTINCT msisdn, latitude_dec, longitude_dec FROM response_records "
                        + "  WHERE msisdn IS NOT NULL AND msisdn <> '' "
                        + "    AND latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL " + t
                        + ") "
                        + "SELECT a.msisdn AS Rufnummer_A, b.msisdn AS Rufnummer_B, "
                        + "COUNT(*) AS Gemeinsame_Funkzellen "
                        + "FROM cells a "
                        + "JOIN cells b ON a.latitude_dec = b.latitude_dec AND a.longitude_dec = b.longitude_dec "
                        + "  AND a.msisdn < b.msisdn "
                        + "GROUP BY a.msisdn, b.msisdn "
                        + "HAVING COUNT(*) = (SELECT COUNT(DISTINCT latitude_dec || ',' || longitude_dec) FROM cells WHERE msisdn = a.msisdn) "
                        + "   AND COUNT(*) = (SELECT COUNT(DISTINCT latitude_dec || ',' || longitude_dec) FROM cells WHERE msisdn = b.msisdn) "
                        + "ORDER BY Gemeinsame_Funkzellen DESC;";

            case CELL_SITES:
                return "SELECT ROUND(latitude_dec,5) || ', ' || ROUND(longitude_dec,5) AS Funkzelle, "
                        + "COUNT(*) AS Anzahl_Datensaetze, MIN(start_time) AS Erstmals, MAX(start_time) AS Zuletzt "
                        + "FROM response_records "
                        + "WHERE latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL " + t
                        + "GROUP BY latitude_dec, longitude_dec "
                        + "ORDER BY Anzahl_Datensaetze DESC;";

            case IDENTIFIER_INVENTORY:
                return "SELECT 'Rufnummer' AS Kennungstyp, msisdn AS Kennung, COUNT(*) AS Anzahl_Datensaetze, "
                        + "MIN(start_time) AS Erstmals, MAX(start_time) AS Zuletzt "
                        + "FROM response_records WHERE msisdn IS NOT NULL AND msisdn <> '' " + t
                        + "GROUP BY msisdn "
                        + "UNION ALL "
                        + "SELECT 'IMSI' AS Kennungstyp, imsi AS Kennung, COUNT(*) AS Anzahl_Datensaetze, "
                        + "MIN(start_time) AS Erstmals, MAX(start_time) AS Zuletzt "
                        + "FROM response_records WHERE imsi IS NOT NULL AND imsi <> '' " + t
                        + "GROUP BY imsi "
                        + "UNION ALL "
                        + "SELECT 'IMEI/Geräte-ID' AS Kennungstyp, na_device_id AS Kennung, COUNT(*) AS Anzahl_Datensaetze, "
                        + "MIN(start_time) AS Erstmals, MAX(start_time) AS Zuletzt "
                        + "FROM response_records WHERE na_device_id IS NOT NULL AND na_device_id <> '' " + t
                        + "GROUP BY na_device_id "
                        + "ORDER BY Kennungstyp, Kennung;";

            case IDENTIFIER_TIMELINE:
                return "SELECT start_time AS Zeit, end_time AS Ende, "
                        + "ROUND(latitude_dec,5) || ', ' || ROUND(longitude_dec,5) AS Funkzelle, "
                        + "CASE WHEN call_indicator IN ('MOC','MTC') THEN 'Telefonie' "
                        + "WHEN call_indicator IN ('MOM','MTM') THEN 'Kurzmitteilung (SMS)' "
                        + "WHEN call_indicator = 'SGP' OR nw_access_type IN ('mobilePacketData','GPRS') THEN 'GPRS/Datenverbindung' "
                        + "ELSE 'Unbekannt (' || COALESCE(call_indicator, nw_access_type, '?') || ')' END AS Verbindungstyp, "
                        + "msisdn AS Rufnummer, imsi AS IMSI, na_device_id AS IMEI, request_id AS Quelle "
                        + "FROM response_records "
                        + "WHERE " + identifierMatchClause(params.identifier, "") + t
                        + "ORDER BY start_time;";

            case COMMUNICATION_PARTNERS_DETAIL:
                return "SELECT a.start_time AS Zeit, a.call_indicator AS Richtung, "
                        + "ROUND(a.latitude_dec,5) || ', ' || ROUND(a.longitude_dec,5) AS Funkzelle, "
                        + "b.msisdn AS Gegenpart_Rufnummer, b.imsi AS Gegenpart_IMSI, b.na_device_id AS Gegenpart_IMEI, "
                        + "ROUND(b.latitude_dec,5) || ', ' || ROUND(b.longitude_dec,5) AS Funkzelle_Gegenpart, "
                        + "b.start_time AS Zeit_Gegenpart "
                        + "FROM response_records a "
                        + "LEFT JOIN response_records b "
                        + "  ON a.session_id = b.session_id AND b.session_id IS NOT NULL AND b.session_id <> '' "
                        + "  AND COALESCE(b.msisdn,'')||'|'||COALESCE(b.imsi,'')||'|'||COALESCE(b.na_device_id,'') "
                        + "      <> COALESCE(a.msisdn,'')||'|'||COALESCE(a.imsi,'')||'|'||COALESCE(a.na_device_id,'') "
                        + "WHERE " + identifierMatchClause(params.identifier, "a.")
                        + "  AND a.call_indicator IN ('MOC','MTC','MOM','MTM') " + ta
                        + "ORDER BY a.start_time;";

            case IDENTIFIER_CO_OCCURRENCE:
                return "SELECT 'Im selben Datensatz' AS Beziehung, msisdn AS Rufnummer, imsi AS IMSI, "
                        + "na_device_id AS IMEI, start_time AS Zeit "
                        + "FROM response_records "
                        + "WHERE " + identifierMatchClause(params.identifier, "") + t
                        + "UNION ALL "
                        + "SELECT 'Gesprächspartner (gleiche session_id)' AS Beziehung, "
                        + "b.msisdn AS Rufnummer, b.imsi AS IMSI, b.na_device_id AS IMEI, b.start_time AS Zeit "
                        + "FROM response_records a JOIN response_records b ON a.session_id = b.session_id "
                        + "WHERE " + identifierMatchClause(params.identifier, "a.")
                        + "  AND a.session_id IS NOT NULL AND a.session_id <> '' "
                        + "  AND COALESCE(b.msisdn,'')||'|'||COALESCE(b.imsi,'')||'|'||COALESCE(b.na_device_id,'') "
                        + "      <> COALESCE(a.msisdn,'')||'|'||COALESCE(a.imsi,'')||'|'||COALESCE(a.na_device_id,'') " + ta
                        + "UNION ALL "
                        + "SELECT 'Gleiche Funkzelle (±30 Min.)' AS Beziehung, "
                        + "b.msisdn AS Rufnummer, b.imsi AS IMSI, b.na_device_id AS IMEI, b.start_time AS Zeit "
                        + "FROM response_records a JOIN response_records b "
                        + "  ON a.latitude_dec = b.latitude_dec AND a.longitude_dec = b.longitude_dec "
                        + "  AND ABS(strftime('%s', a.start_time) - strftime('%s', b.start_time)) <= 1800 "
                        + "WHERE " + identifierMatchClause(params.identifier, "a.")
                        + "  AND a.latitude_dec IS NOT NULL AND a.longitude_dec IS NOT NULL "
                        + "  AND COALESCE(b.msisdn,'')||'|'||COALESCE(b.imsi,'')||'|'||COALESCE(b.na_device_id,'') "
                        + "      <> COALESCE(a.msisdn,'')||'|'||COALESCE(a.imsi,'')||'|'||COALESCE(a.na_device_id,'') " + ta
                        + "ORDER BY Beziehung, Zeit;";

            case TRANSMISSION_COUNT:
                return "SELECT CASE "
                        + "WHEN call_indicator IN ('MOC','MTC') THEN 'Telefonie' "
                        + "WHEN call_indicator IN ('MOM','MTM') THEN 'Kurzmitteilung (SMS)' "
                        + "WHEN call_indicator = 'SGP' OR nw_access_type IN ('mobilePacketData','GPRS') THEN 'GPRS/Datenverbindung' "
                        + "ELSE 'Unbekannt (' || COALESCE(call_indicator, nw_access_type, '?') || ')' "
                        + "END AS Verbindungstyp, COUNT(*) AS Anzahl "
                        + "FROM response_records "
                        + "WHERE " + identifierMatchClause(params.identifier, "") + t
                        + "GROUP BY Verbindungstyp "
                        + "ORDER BY Anzahl DESC;";

            case DEVICE_INFO_LOOKUP:
                // The online TAC→Hersteller/Modell lookup (TacLookupService) is
                // performed separately by PoliceAnalysisPane; this SQL only
                // surfaces what the local dataset itself knows about the IMEI.
                return "SELECT na_device_id AS IMEI, imsi AS IMSI, msisdn AS Rufnummer, start_time AS Zeit, "
                        + "ROUND(latitude_dec,5) || ', ' || ROUND(longitude_dec,5) AS Funkzelle "
                        + "FROM response_records "
                        + "WHERE na_device_id = '" + escape(params.identifier.trim()) + "' " + t
                        + "ORDER BY start_time;";

            case FILTER_BY_COUNTRY: {
                String prefix = params.identifier.trim();
                String countryName = CountryCallingCodeLookup.getCountryForNumber(prefix);
                String landLiteral = escape(countryName != null ? countryName : "Unbekannt (Präfix " + prefix + "…)");
                return "SELECT msisdn AS Rufnummer, '" + landLiteral + "' AS Land, "
                        + "COUNT(*) AS Anzahl_Datensaetze, "
                        + "MIN(start_time) AS Erstmals, MAX(start_time) AS Zuletzt "
                        + "FROM response_records "
                        + "WHERE msisdn LIKE '" + escape(prefix) + "%' " + t
                        + "GROUP BY msisdn "
                        + "ORDER BY Rufnummer;";
            }

            case COUNTRY_HISTOGRAM:
                return "SELECT "
                        + CountryCallingCodeLookup.buildSqlCaseExpression(
                                "msisdn", "'Unbekannt/Ausland (Präfix ' || SUBSTR(msisdn,1,3) || '…)'")
                        + " AS Land, "
                        + "COUNT(DISTINCT msisdn) AS Anzahl_Rufnummern, COUNT(*) AS Anzahl_Datensaetze "
                        + "FROM response_records "
                        + "WHERE msisdn IS NOT NULL AND msisdn <> '' " + t
                        + "GROUP BY Land "
                        + "ORDER BY Anzahl_Rufnummern DESC;";

            case PRESENCE_IN_AREA:
                return "SELECT start_time AS Zeit, "
                        + "ROUND(latitude_dec,5) || ', ' || ROUND(longitude_dec,5) AS Funkzelle, "
                        + "msisdn AS Rufnummer, imsi AS IMSI, na_device_id AS IMEI "
                        + "FROM response_records "
                        + "WHERE " + identifierMatchClause(params.identifier, "")
                        + "  AND latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL "
                        + areaBoundingBoxClause(params, type) + t
                        + "ORDER BY start_time;";

            case FREQUENT_COMPANION:
                return "SELECT b.msisdn AS Begleiter_Rufnummer, b.imsi AS Begleiter_IMSI, "
                        + "b.na_device_id AS Begleiter_IMEI, COUNT(*) AS Anzahl_gemeinsamer_Funkzellen "
                        + "FROM response_records a "
                        + "JOIN response_records b "
                        + "  ON a.latitude_dec = b.latitude_dec AND a.longitude_dec = b.longitude_dec "
                        + "  AND ABS(strftime('%s', a.start_time) - strftime('%s', b.start_time)) <= 1800 "
                        + "  AND COALESCE(b.msisdn,'')||'|'||COALESCE(b.imsi,'')||'|'||COALESCE(b.na_device_id,'') "
                        + "      <> COALESCE(a.msisdn,'')||'|'||COALESCE(a.imsi,'')||'|'||COALESCE(a.na_device_id,'') "
                        + "WHERE " + identifierMatchClause(params.identifier, "a.")
                        + "  AND a.latitude_dec IS NOT NULL AND a.longitude_dec IS NOT NULL " + ta + tb
                        + "GROUP BY b.msisdn, b.imsi, b.na_device_id "
                        + "ORDER BY Anzahl_gemeinsamer_Funkzellen DESC;";

            case COMM_SAME_CELL:
                return "SELECT a.msisdn AS Rufnummer_A, b.msisdn AS Rufnummer_B, a.session_id AS Session, "
                        + "ROUND(a.latitude_dec,5) || ', ' || ROUND(a.longitude_dec,5) AS Funkzelle, "
                        + "a.start_time AS Zeit "
                        + "FROM response_records a "
                        + "JOIN response_records b "
                        + "  ON a.session_id = b.session_id AND a.msisdn < b.msisdn "
                        + "  AND a.latitude_dec = b.latitude_dec AND a.longitude_dec = b.longitude_dec "
                        + "WHERE a.session_id IS NOT NULL AND a.session_id <> '' "
                        + "  AND a.msisdn IS NOT NULL AND b.msisdn IS NOT NULL "
                        + "  AND a.latitude_dec IS NOT NULL " + ta + tb
                        + "ORDER BY a.start_time DESC;";

            default:
                throw new IllegalArgumentException("Unbekannter Auswertungstyp: " + type);
        }
    }
}
