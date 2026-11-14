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
 *
 * <p>Every analysis label/description and every SQL column alias below is
 * bilingual (German/English), selected at build time via
 * {@link Lang#isGerman()} — this follows the same runtime "DE | EN" toggle
 * already used elsewhere in the Lighthouse window, so the result table's
 * headers switch language along with the rest of the UI.</p>
 */
public final class PoliceAnalysisQueries {

    private PoliceAnalysisQueries() { }

    /** Returns {@code de} if German is currently active, otherwise {@code en} — shorthand for {@link Lang#t}. */
    private static String c(String de, String en) {
        return Lang.t(de, en);
    }

    /**
     * True if {@code columnName} is one of the (German or English) cell-site
     * columns that every coordinate-bearing analysis below names with this
     * shared prefix convention (see e.g. {@link PoliceAnalysisPane}'s
     * "Auf Karte anzeigen"/"Show on map" handler, which looks the column up
     * by this prefix rather than a fixed name, since the "Funkzelle"/
     * "Cell_Site" alias appears under a few different exact names above).
     */
    public static boolean isCellSiteColumn(String columnName) {
        return columnName != null
                && (columnName.startsWith("Funkzelle") || columnName.startsWith("Cell_Site"));
    }

    /** The "Untersuchungsschwerpunkte" an analyst can pick from in {@link PoliceAnalysisPane}. */
    public enum AnalysisType {

        ALL_NUMBERS(
                "Alle Rufnummern", "All phone numbers",
                "Listet alle in den Datensätzen vorkommenden Rufnummern (MSISDN) auf, "
                        + "mit Anzahl der Treffer und erstem/letztem Auftreten.",
                "Lists every phone number (MSISDN) that occurs in the records, "
                        + "with the number of hits and its first/last occurrence."),

        DOMESTIC_FOREIGN(
                "In-/ausländische Rufnummern", "Domestic/foreign phone numbers",
                "Unterscheidet deutsche von ausländischen Rufnummern. Heuristik: eine "
                        + "Rufnummer beginnt im Rohformat mit der Landesvorwahl ohne '+' "
                        + "(z. B. '49…' für Deutschland) — bei erkannter ausländischer "
                        + "Vorwahl wird der Ländername angezeigt, sonst nur der rohe Präfix.",
                "Distinguishes German from foreign phone numbers. Heuristic: a raw "
                        + "phone number starts with the country calling code without a "
                        + "leading '+' (e.g. '49…' for Germany) — a recognised foreign "
                        + "calling code shows the country name, otherwise just the raw prefix."),

        PARTY_ROLE(
                "Teilnehmerrolle (A-/B-/C-Teilnehmer)", "Party role (A/B/C party)",
                "Gruppiert nach dem importierten party_type-Rohwert je Rufnummer/IMSI. "
                        + "Hinweis: die genaue A-/B-/C-Zuordnung dieses Feldes ist anhand der "
                        + "vorliegenden Testdaten nicht zweifelsfrei verifiziert — die Rohwerte "
                        + "werden daher unverändert angezeigt.",
                "Groups by the imported raw party_type value per phone number/IMSI. "
                        + "Note: the exact A-/B-/C-party mapping of this field could not be "
                        + "conclusively verified against the available test data — the raw "
                        + "values are therefore shown unchanged."),

        PARTICIPANTS_PER_CELL(
                "Teilnehmer in der Funkzelle", "Participants in a cell site",
                "Zeigt je Funkzelle (Standort-Koordinate) die Anzahl und Liste der "
                        + "beteiligten Rufnummern/IMSIs im gewählten Zeitraum.",
                "Shows, per cell site (location coordinate), the count and list of "
                        + "phone numbers/IMSIs involved during the selected time range."),

        CONNECTION_TYPE(
                "Verbindungstyp (Telefon/SMS/GPRS)", "Connection type (phone/SMS/GPRS)",
                "Klassifiziert jeden Datensatz anhand von call_indicator/nw_access_type "
                        + "als Telefonie, Kurzmitteilung (SMS) oder GPRS/Datenverbindung.",
                "Classifies every record, based on call_indicator/nw_access_type, as "
                        + "telephony, short message (SMS), or GPRS/data connection."),

        DEVICE_SIM_SWAP(
                "Wechsler (Endgeräte/SIM-Karten)", "Swappers (devices/SIM cards)",
                "Findet IMSIs, die mit mehreren unterschiedlichen Geräte-IDs (IMEI) "
                        + "auftreten (Geräte-Wechsel), sowie Geräte-IDs, die mit mehreren "
                        + "unterschiedlichen IMSIs auftreten (SIM-Wechsel).",
                "Finds IMSIs seen with more than one distinct device ID (IMEI) — a "
                        + "device swap — as well as device IDs seen with more than one "
                        + "distinct IMSI — a SIM swap."),

        CROSS_HITS(
                "Kreuztreffer über mehrere Quellen", "Cross-hits across multiple sources",
                "Findet IMSI/Rufnummer/IMEI, die in mehr als einer Anfrage "
                        + "(request_id, z. B. unterschiedliche Datenquellen/Funkzellenabfragen) "
                        + "auftauchen.",
                "Finds IMSI/phone number/IMEI values that occur in more than one "
                        + "request (request_id, e.g. different data sources/cell-site queries)."),

        ROAMERS(
                "Wanderer (mehrere Funkzellen)", "Roamers (multiple cell sites)",
                "Findet Rufnummern/IMSIs, die im Zeitraum an mehr als einer Funkzelle "
                        + "(Standort-Koordinate) registriert waren.",
                "Finds phone numbers/IMSIs that were registered at more than one cell "
                        + "site (location coordinate) during the time range."),

        NUMBER_BLOCKS(
                "Ähnliche Rufnummern (Rufnummernblöcke)", "Similar phone numbers (number blocks)",
                "Gruppiert Rufnummern, die sich nur in den letzten beiden Ziffern "
                        + "unterscheiden, als möglichen gemeinsam vergebenen Rufnummernblock.",
                "Groups phone numbers that differ only in their last two digits as a "
                        + "possible jointly issued block of numbers."),

        RECURRING_PAIRS(
                "Wiederkehrende Rufnummernpaare", "Recurring phone-number pairs",
                "Findet Rufnummernpaare, die sich mindestens zweimal begegnet sind: "
                        + "Datensätze beider Nummern in derselben Funkzelle innerhalb von 30 Minuten; "
                        + "eine Lücke von mehr als 30 Minuten beginnt eine neue Begegnung.",
                "Finds pairs of phone numbers that met at least twice: records of both "
                        + "numbers at the same cell site within 30 minutes of each other; a gap "
                        + "of more than 30 minutes starts a new meeting."),

        TWINS(
                "Twins (identische Funkzellen-Menge)", "Twins (identical set of cell sites)",
                "Findet Rufnummernpaare, bei denen beide Teilnehmer exakt dieselbe Menge "
                        + "an Funkzellen im Zeitraum besucht haben.",
                "Finds pairs of phone numbers where both parties visited exactly the "
                        + "same set of cell sites during the time range."),

        // ---------------------------------------------------------------
        // Erweiterung um die 14 konkreten Ermittlungsfragen — einige davon
        // benötigen eine vom Analysten eingegebene Kennung (requiresIdentifier)
        // und/oder ein Gebiet (requiresArea), siehe PoliceAnalysisPane.
        // ---------------------------------------------------------------

        CELL_SITES(
                "Funkzellen im Datensatz", "Cell sites in the dataset",
                "Listet alle im Datensatz vorkommenden Funkzellen (Standort-"
                        + "Koordinaten) auf, mit Anzahl der Datensätze sowie "
                        + "erstem/letztem Auftreten. Beantwortet 'Welche Gebiete/"
                        + "Funkmasten liegen im Datensatz vor?'.",
                "Lists every cell site (location coordinate) present in the "
                        + "dataset, with the record count and first/last occurrence. "
                        + "Answers 'Which areas/masts appear in the dataset?'."),

        IDENTIFIER_INVENTORY(
                "Alle Kennungen (Rufnummern/IMSI/IMEI)", "All identifiers (phone numbers/IMSI/IMEI)",
                "Listet alle im Datensatz vorkommenden Rufnummern, IMSIs und "
                        + "Geräte-IDs (IMEI) getrennt nach Kennungstyp auf, mit Anzahl "
                        + "der Datensätze sowie erstem/letztem Auftreten. Beantwortet "
                        + "'Welche Telefonnummern/IMEIs/IMSIs liegen im Datensatz vor?'.",
                "Lists every phone number, IMSI and device ID (IMEI) present in "
                        + "the dataset, separated by identifier type, with the record "
                        + "count and first/last occurrence. Answers 'Which phone "
                        + "numbers/IMEIs/IMSIs appear in the dataset?'."),

        IDENTIFIER_TIMELINE(
                "Verbindungen einer Kennung (wer/wann/wo)", "Connections of an identifier (who/when/where)",
                "Zeigt für die eingegebene Rufnummer/IMSI/IMEI alle Datensätze "
                        + "chronologisch mit Funkzelle, Zeit und Verbindungstyp. "
                        + "Beantwortet 'Mit welchen Funkmasten war x zu welcher Zeit "
                        + "verbunden?' sowie 'Gibt es einen Eintrag zu x im Zeitraum y?' "
                        + "(leere Tabelle = kein Treffer).",
                "Shows, for the entered phone number/IMSI/IMEI, every record in "
                        + "chronological order with cell site, time and connection "
                        + "type. Answers 'Which masts was x connected to and when?' "
                        + "as well as 'Is there an entry for x in period y?' (an "
                        + "empty table means no hit).",
                true, false),

        COMMUNICATION_PARTNERS_DETAIL(
                "Gesprächs-/SMS-Partner einer Kennung", "Call/SMS partners of an identifier",
                "Findet zu von der eingegebenen Kennung ausgehenden Telefonaten/"
                        + "SMS den jeweils zugehörigen Gegenpart über die gemeinsame "
                        + "session_id, inkl. dessen Funkzelle und Zeitpunkt. Hinweis: "
                        + "setzt voraus, dass beide Seiten eines Gesprächs im Export "
                        + "dieselbe session_id tragen — ungeprüft, da kein reales "
                        + "Telefonie-Sample vorlag.",
                "Finds, for calls/SMS originating from the entered identifier, the "
                        + "corresponding counterpart via the shared session_id, "
                        + "including that counterpart's cell site and time. Note: "
                        + "this assumes both sides of a call carry the same "
                        + "session_id in the export — unverified, since no real "
                        + "telephony sample was available.",
                true, false),

        IDENTIFIER_CO_OCCURRENCE(
                "Weitere Kennungen im Umfeld einer Kennung", "Other identifiers near an identifier",
                "Findet weitere Rufnummern/IMSIs/IMEIs, die zur eingegebenen "
                        + "Kennung in Beziehung stehen: im selben Datensatz erfasst, "
                        + "über dieselbe session_id (Gesprächspartner) oder zur "
                        + "gleichen Zeit (±30 Min.) in derselben Funkzelle.",
                "Finds other phone numbers/IMSIs/IMEIs related to the entered "
                        + "identifier: recorded in the same record, linked via the "
                        + "same session_id (call partner), or present at the same "
                        + "time (±30 min.) in the same cell site.",
                true, false),

        TRANSMISSION_COUNT(
                "Anzahl Übertragungen einer Kennung", "Number of transmissions of an identifier",
                "Zählt die Datensätze der eingegebenen Kennung im gewählten "
                        + "Zeitraum, aufgeschlüsselt nach Verbindungstyp (Telefonie/"
                        + "SMS/GPRS).",
                "Counts the records of the entered identifier within the "
                        + "selected time range, broken down by connection type "
                        + "(telephony/SMS/GPRS).",
                true, false),

        DEVICE_INFO_LOOKUP(
                "Gerätetyp zu einer IMEI", "Device type for an IMEI",
                "Zeigt zunächst alle lokal vorliegenden Datensätze zur "
                        + "eingegebenen IMEI und versucht zusätzlich einen Online-"
                        + "Abgleich der TAC (erste 8 Ziffern der IMEI) über die "
                        + "HiCellTek-API, um Hersteller/Modell zu ermitteln. Erfordert "
                        + "einen konfigurierten API-Key (System-Property "
                        + "'fqlite.hicelltek.apikey') und Internetzugriff — ohne beides "
                        + "wird nur der lokale Datensatzbefund angezeigt.",
                "First shows every locally available record for the entered "
                        + "IMEI and additionally attempts an online lookup of the TAC "
                        + "(the IMEI's first 8 digits) via the HiCellTek API to "
                        + "determine make/model. Requires a configured API key "
                        + "(system property 'fqlite.hicelltek.apikey') and internet "
                        + "access — without both, only the local record findings are shown.",
                true, false),

        FILTER_BY_COUNTRY(
                "Rufnummern aus einem Land", "Phone numbers from a country",
                "Filtert Rufnummern nach einer eingegebenen Landesvorwahl (z. B. "
                        + "'49' für Deutschland, ohne führendes '+') — Vorwahl als "
                        + "Kennung eingeben; der erkannte Ländername wird in der "
                        + "Ergebnisspalte 'Land' angezeigt. Gleiche Heuristik/"
                        + "Einschränkung wie bei 'In-/ausländische Rufnummern'.",
                "Filters phone numbers by an entered country calling code (e.g. "
                        + "'49' for Germany, without a leading '+') — enter the "
                        + "calling code as the identifier; the recognised country "
                        + "name is shown in the result's 'Country' column. Same "
                        + "heuristic/limitation as 'Domestic/foreign phone numbers'.",
                true, false),

        COUNTRY_HISTOGRAM(
                "Länder-Übersicht aller Rufnummern", "Country overview of all phone numbers",
                "Gruppiert alle Rufnummern nach erkanntem Land (anhand der "
                        + "Landesvorwahl) und zeigt die Anzahl je Land. Gleiche "
                        + "Heuristik/Einschränkung wie bei 'In-/ausländische "
                        + "Rufnummern' — Vorwahlen ohne Eintrag in der Vorwahltabelle "
                        + "erscheinen weiterhin nur mit rohem Präfix.",
                "Groups all phone numbers by recognised country (based on the "
                        + "calling code) and shows the count per country. Same "
                        + "heuristic/limitation as 'Domestic/foreign phone numbers' — "
                        + "calling codes with no entry in the lookup table still "
                        + "appear only with their raw prefix."),

        PRESENCE_IN_AREA(
                "Anwesenheit einer Kennung in einem Gebiet", "Presence of an identifier in an area",
                "Prüft, ob die eingegebene Kennung im gewählten Zeitraum innerhalb "
                        + "eines Umkreises (Radius in Metern, Standard 500 m) um den "
                        + "eingegebenen Gebiets-Mittelpunkt ('Breitengrad,Längengrad') "
                        + "registriert war. Hinweis: der Umkreis wird als einfache "
                        + "Grad-Bounding-Box angenähert (1° ≈ 111.320 m, ohne "
                        + "Breitengrad-Korrektur der Längengrad-Distanz) — für eine "
                        + "grobe forensische Einordnung ausreichend, aber keine exakte "
                        + "geodätische Distanz.",
                "Checks whether the entered identifier was registered, during "
                        + "the selected time range, within a radius (in meters, "
                        + "default 500 m) around the entered area centre "
                        + "('latitude,longitude'). Note: the radius is approximated "
                        + "as a simple degree bounding box (1° ≈ 111,320 m, without "
                        + "latitude correction of the longitude distance) — good "
                        + "enough for a rough forensic assessment, but not an exact "
                        + "geodesic distance.",
                true, true),

        FREQUENT_COMPANION(
                "Häufigster Begleiter einer Kennung", "Most frequent companion of an identifier",
                "Findet zur eingegebenen Kennung die anderen Rufnummern/IMSIs/"
                        + "IMEIs, die ihr am häufigsten begegnet sind (Datensätze in "
                        + "derselben Funkzelle innerhalb von 30 Min.; eine Lücke von mehr "
                        + "als 30 Min. beginnt eine neue Begegnung) — sortiert nach "
                        + "Anzahl der Begegnungen.",
                "Finds, for the entered identifier, the other phone numbers/"
                        + "IMSIs/IMEIs that met it most often (records at the same "
                        + "cell site within 30 min.; a gap of more than 30 min. starts "
                        + "a new meeting) — sorted by number of meetings.",
                true, false),

        COMM_SAME_CELL(
                "Kommunikationspartner in derselben Funkzelle", "Communication partners in the same cell site",
                "Findet Paare von Rufnummern, die über dieselbe session_id als "
                        + "Gesprächs-/SMS-Partner verknüpft sind UND dabei in "
                        + "derselben Funkzelle eingeloggt waren.",
                "Finds pairs of phone numbers linked as call/SMS partners via "
                        + "the same session_id AND registered in the same cell "
                        + "site at the time.");

        private final String labelDe;
        private final String labelEn;
        private final String descriptionDe;
        private final String descriptionEn;
        /** True if this analysis needs an analyst-supplied Rufnummer/IMSI/IMEI (or, for FILTER_BY_COUNTRY, a country prefix) in {@link Params#identifier}. */
        public final boolean requiresIdentifier;
        /** True if this analysis additionally needs {@link Params#areaCenter} (and optionally {@link Params#radiusMeters}). */
        public final boolean requiresArea;

        AnalysisType(String labelDe, String labelEn, String descriptionDe, String descriptionEn) {
            this(labelDe, labelEn, descriptionDe, descriptionEn, false, false);
        }

        AnalysisType(String labelDe, String labelEn, String descriptionDe, String descriptionEn,
                     boolean requiresIdentifier, boolean requiresArea) {
            this.labelDe = labelDe;
            this.labelEn = labelEn;
            this.descriptionDe = descriptionDe;
            this.descriptionEn = descriptionEn;
            this.requiresIdentifier = requiresIdentifier;
            this.requiresArea = requiresArea;
        }

        /** The display label in the currently active UI language (see {@link Lang#isGerman()}). */
        public String label() {
            return Lang.isGerman() ? labelDe : labelEn;
        }

        /** The explanatory description in the currently active UI language. */
        public String description() {
            return Lang.isGerman() ? descriptionDe : descriptionEn;
        }

        @Override
        public String toString() { return label(); }
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
     * CTE {@code ident}: all records of the analysed identifier (msisdn, imsi
     * or IMEI), i.e. the rows of {@code WHERE }{@link #identifierMatchClause},
     * read once for the whole query (with the importer's indexes as a
     * MULTI-INDEX OR lookup). Used as the {@code a} side of the self-joins.
     */
    private static String identCte(String identifier) {
        return "ident AS MATERIALIZED ("
                + "SELECT * FROM response_records WHERE " + identifierMatchClause(identifier, "") + ")";
    }

    /**
     * CTE {@code partners} (requires {@link #identCte}): all records whose
     * {@code session_id} occurs in a record of the identifier. In a join on
     * {@code a.session_id = b.session_id} with {@code a} from {@code ident},
     * every record that can match is contained, so using it instead of
     * {@code response_records b} gives the same result; the table is read once
     * instead of SQLite building an automatic index on {@code session_id}
     * over the whole table.
     */
    private static String sessionPartnersCte() {
        return "partners AS MATERIALIZED ("
                + "SELECT * FROM response_records WHERE session_id IN ("
                + "SELECT session_id FROM ident WHERE session_id IS NOT NULL AND session_id <> ''))";
    }

    /**
     * CTE {@code samecell} (requires {@link #identCte}): all records at a cell
     * site (exact coordinates) where the identifier has a record within the
     * time filter {@code timeFilter} (unqualified form of the filter applied
     * to the identifier side). In a join on equal coordinates with {@code a}
     * from {@code ident}, every record that can match is contained, so using
     * it instead of {@code response_records b} gives the same result, while
     * only the cells of the identifier are joined.
     */
    private static String sameCellCte(String timeFilter) {
        return "samecell AS MATERIALIZED ("
                + "SELECT * FROM response_records WHERE (latitude_dec, longitude_dec) IN ("
                + "SELECT latitude_dec, longitude_dec FROM ident "
                + "WHERE latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL " + timeFilter + "))";
    }

    /**
     * Validates that {@code params.identifier} was actually supplied for an
     * analysis that {@link AnalysisType#requiresIdentifier requires} one.
     */
    private static String requireIdentifier(Params params, AnalysisType type) {
        if (params == null || params.identifier == null || params.identifier.isBlank()) {
            throw new IllegalArgumentException(Lang.t(
                    "Die Auswertung '" + type.label() + "' benötigt eine eingegebene Rufnummer/IMSI/IMEI.",
                    "The analysis '" + type.label() + "' requires a phone number/IMSI/IMEI to be entered."));
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
            throw new IllegalArgumentException(Lang.t(
                    "Die Auswertung '" + type.label() + "' benötigt ein Gebiet (Format 'Breitengrad,Längengrad').",
                    "The analysis '" + type.label() + "' requires an area (format 'latitude,longitude')."));
        }
        String[] parts = params.areaCenter.split(",");
        if (parts.length != 2) {
            throw new IllegalArgumentException(Lang.t(
                    "Gebiet bitte im Format 'Breitengrad,Längengrad' angeben, z. B. '52.5200,13.4050'.",
                    "Please enter the area in the format 'latitude,longitude', e.g. '52.5200,13.4050'."));
        }
        double lat, lon;
        try {
            lat = Double.parseDouble(parts[0].trim());
            lon = Double.parseDouble(parts[1].trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(Lang.t(
                    "Gebiet bitte im Format 'Breitengrad,Längengrad' angeben, z. B. '52.5200,13.4050'.",
                    "Please enter the area in the format 'latitude,longitude', e.g. '52.5200,13.4050'."));
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
     *         {@code response_records} via {@link fqlite.sql.QueryableDatabase}
     * @throws IllegalArgumentException if a required identifier/area is missing or malformed
     */
    // Note on "GROUP BY +column": databases written by the current ETSI
    // importer have indexes on msisdn/imsi/na_device_id. Without the unary
    // "+", SQLite walks such an index to group the whole table and then reads
    // every record in index order, i.e. in random file order - for millions
    // of records many times slower than one sequential scan plus sorting
    // (measured: 96 s instead of 5 s for 5.7 million records). The "+" keeps
    // the grouping exactly the same but lets SQLite group by sorting, as it
    // always did on the in-memory copy, which has no indexes. A time filter
    // can still use the start_time index.
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
                return "SELECT msisdn AS " + c("Rufnummer", "Phone_Number") + ", COUNT(*) AS " + c("Anzahl_Datensaetze", "Record_Count") + ", "
                        + "MIN(start_time) AS " + c("Erstmals", "First_Seen") + ", MAX(start_time) AS " + c("Zuletzt", "Last_Seen") + " "
                        + "FROM response_records "
                        + "WHERE msisdn IS NOT NULL AND msisdn <> '' " + t
                        + "GROUP BY +msisdn "
                        + "ORDER BY " + c("Rufnummer", "Phone_Number") + ";";

            case DOMESTIC_FOREIGN:
                return "SELECT msisdn AS " + c("Rufnummer", "Phone_Number") + ", "
                        + CountryCallingCodeLookup.buildSqlCaseExpression(
                                "msisdn", "'" + c("Ausland (Präfix ", "Abroad (prefix ") + "' || SUBSTR(msisdn,1,3) || '…)'")
                        + " AS " + c("Herkunft", "Origin") + ", "
                        + "COUNT(*) AS " + c("Anzahl_Datensaetze", "Record_Count") + " "
                        + "FROM response_records "
                        + "WHERE msisdn IS NOT NULL AND msisdn <> '' " + t
                        + "GROUP BY msisdn "
                        + "ORDER BY " + c("Herkunft", "Origin") + ", " + c("Rufnummer", "Phone_Number") + ";";

            case PARTY_ROLE:
                return "SELECT party_type AS " + c("Teilnehmerrolle", "Party_Role") + ", msisdn AS " + c("Rufnummer", "Phone_Number") + ", imsi AS IMSI, "
                        + "COUNT(*) AS " + c("Anzahl_Datensaetze", "Record_Count") + " "
                        + "FROM response_records "
                        + "WHERE party_type IS NOT NULL AND party_type <> '' " + t
                        + "GROUP BY party_type, msisdn, imsi "
                        + "ORDER BY " + c("Teilnehmerrolle", "Party_Role") + ", " + c("Rufnummer", "Phone_Number") + ";";

            case PARTICIPANTS_PER_CELL:
                return "SELECT ROUND(latitude_dec,5) || ', ' || ROUND(longitude_dec,5) AS " + c("Funkzelle", "Cell_Site") + ", "
                        + "COUNT(DISTINCT imsi) AS " + c("Anzahl_IMSI", "IMSI_Count") + ", "
                        + "COUNT(DISTINCT msisdn) AS " + c("Anzahl_Rufnummern", "Phone_Number_Count") + ", "
                        + "GROUP_CONCAT(DISTINCT msisdn) AS " + c("Rufnummern", "Phone_Numbers") + ", "
                        + "MIN(start_time) AS " + c("Von", "From_Time") + ", MAX(start_time) AS " + c("Bis", "To_Time") + " "
                        + "FROM response_records "
                        + "WHERE latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL " + t
                        + "GROUP BY latitude_dec, longitude_dec "
                        + "ORDER BY " + c("Anzahl_Rufnummern", "Phone_Number_Count") + " DESC;";

            case CONNECTION_TYPE:
                return "SELECT CASE "
                        + "WHEN call_indicator IN ('MOC','MTC') THEN '" + c("Telefonie", "Telephony") + "' "
                        + "WHEN call_indicator IN ('MOM','MTM') THEN '" + c("Kurzmitteilung (SMS)", "Short message (SMS)") + "' "
                        + "WHEN call_indicator = 'SGP' OR nw_access_type IN ('mobilePacketData','GPRS') THEN '" + c("GPRS/Datenverbindung", "GPRS/data connection") + "' "
                        + "ELSE '" + c("Unbekannt (", "Unknown (") + "' || COALESCE(call_indicator, nw_access_type, '?') || ') ' "
                        + "END AS " + c("Verbindungstyp", "Connection_Type") + ", "
                        + "COUNT(*) AS " + c("Anzahl_Datensaetze", "Record_Count") + " "
                        + "FROM response_records "
                        + "WHERE 1=1 " + t
                        + "GROUP BY " + c("Verbindungstyp", "Connection_Type") + " "
                        + "ORDER BY " + c("Anzahl_Datensaetze", "Record_Count") + " DESC;";

            case DEVICE_SIM_SWAP:
                return "SELECT '" + c("Geräte-Wechsel (gleiche SIM, mehrere Geräte)", "Device swap (same SIM, multiple devices)") + "' AS " + c("Wechseltyp", "Swap_Type") + ", "
                        + "imsi AS " + c("Kennung", "Identifier") + ", COUNT(DISTINCT TRIM(na_device_id)) AS " + c("Anzahl_Wechsel", "Swap_Count") + ", "
                        + "GROUP_CONCAT(DISTINCT TRIM(na_device_id)) AS " + c("Details", "Details") + " "
                        + "FROM response_records "
                        + "WHERE imsi IS NOT NULL AND TRIM(imsi) <> '' AND na_device_id IS NOT NULL AND TRIM(na_device_id) <> '' " + t
                        + "GROUP BY +imsi "
                        + "HAVING COUNT(DISTINCT TRIM(na_device_id)) > 1 "
                        + "UNION ALL "
                        + "SELECT '" + c("SIM-Wechsel (gleiches Gerät, mehrere SIM-Karten)", "SIM swap (same device, multiple SIM cards)") + "' AS " + c("Wechseltyp", "Swap_Type") + ", "
                        + "na_device_id AS " + c("Kennung", "Identifier") + ", COUNT(DISTINCT TRIM(imsi)) AS " + c("Anzahl_Wechsel", "Swap_Count") + ", "
                        + "GROUP_CONCAT(DISTINCT TRIM(imsi)) AS " + c("Details", "Details") + " "
                        + "FROM response_records "
                        + "WHERE na_device_id IS NOT NULL AND TRIM(na_device_id) <> '' AND imsi IS NOT NULL AND TRIM(imsi) <> '' " + t
                        + "GROUP BY +na_device_id "
                        + "HAVING COUNT(DISTINCT TRIM(imsi)) > 1 "
                        + "ORDER BY " + c("Anzahl_Wechsel", "Swap_Count") + " DESC;";

            case CROSS_HITS:
                return "SELECT 'IMSI' AS " + c("Kennungstyp", "Identifier_Type") + ", imsi AS " + c("Kennung", "Identifier") + ", "
                        + "COUNT(DISTINCT request_id) AS " + c("Anzahl_Quellen", "Source_Count") + ", GROUP_CONCAT(DISTINCT request_id) AS " + c("Quellen", "Sources") + " "
                        + "FROM response_records WHERE imsi IS NOT NULL AND imsi <> '' " + t
                        + "GROUP BY +imsi HAVING COUNT(DISTINCT request_id) > 1 "
                        + "UNION ALL "
                        + "SELECT '" + c("Rufnummer", "Phone_Number") + "' AS " + c("Kennungstyp", "Identifier_Type") + ", msisdn AS " + c("Kennung", "Identifier") + ", "
                        + "COUNT(DISTINCT request_id) AS " + c("Anzahl_Quellen", "Source_Count") + ", GROUP_CONCAT(DISTINCT request_id) AS " + c("Quellen", "Sources") + " "
                        + "FROM response_records WHERE msisdn IS NOT NULL AND msisdn <> '' " + t
                        + "GROUP BY +msisdn HAVING COUNT(DISTINCT request_id) > 1 "
                        + "UNION ALL "
                        + "SELECT '" + c("IMEI/Geräte-ID", "IMEI/Device_ID") + "' AS " + c("Kennungstyp", "Identifier_Type") + ", na_device_id AS " + c("Kennung", "Identifier") + ", "
                        + "COUNT(DISTINCT request_id) AS " + c("Anzahl_Quellen", "Source_Count") + ", GROUP_CONCAT(DISTINCT request_id) AS " + c("Quellen", "Sources") + " "
                        + "FROM response_records WHERE na_device_id IS NOT NULL AND na_device_id <> '' " + t
                        + "GROUP BY +na_device_id HAVING COUNT(DISTINCT request_id) > 1 "
                        + "ORDER BY " + c("Anzahl_Quellen", "Source_Count") + " DESC;";

            case ROAMERS:
                // "Funkzellen"/"Cell_Sites" lists every visited site as
                // "<lat>|<lon>" pairs joined by ',' (GROUP_CONCAT's only
                // separator option together with DISTINCT in SQLite) — '|'
                // inside each pair avoids ambiguity with that outer comma.
                // The column is named via isCellSiteColumn()'s prefix
                // convention (not e.g. "Standorte") so
                // showSelectedRowOnMap() picks it up; that method also
                // knows to parse this '|'-delimited multi-site format and
                // jump to the first listed site.
                return "SELECT imsi AS IMSI, MIN(msisdn) AS " + c("Rufnummer", "Phone_Number") + ", "
                        + "COUNT(DISTINCT latitude_dec || ',' || longitude_dec) AS " + c("Anzahl_Funkzellen", "Cell_Site_Count") + ", "
                        + "GROUP_CONCAT(DISTINCT ROUND(latitude_dec,5) || '|' || ROUND(longitude_dec,5)) AS " + c("Funkzellen", "Cell_Sites") + ", "
                        + "MIN(start_time) AS " + c("Von", "From_Time") + ", MAX(start_time) AS " + c("Bis", "To_Time") + " "
                        + "FROM response_records "
                        + "WHERE imsi IS NOT NULL AND TRIM(imsi) <> '' AND latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL " + t
                        + "GROUP BY +imsi "
                        + "HAVING COUNT(DISTINCT latitude_dec || ',' || longitude_dec) > 1 "
                        + "ORDER BY " + c("Anzahl_Funkzellen", "Cell_Site_Count") + " DESC;";

            case NUMBER_BLOCKS:
                return "SELECT SUBSTR(msisdn,1,LENGTH(msisdn)-2) AS " + c("Rufnummernblock", "Phone_Number_Block") + ", "
                        + "COUNT(DISTINCT msisdn) AS " + c("Anzahl_Rufnummern", "Phone_Number_Count") + ", "
                        + "GROUP_CONCAT(DISTINCT msisdn) AS " + c("Rufnummern", "Phone_Numbers") + " "
                        + "FROM response_records "
                        + "WHERE msisdn IS NOT NULL AND msisdn <> '' AND LENGTH(msisdn) > 4 " + t
                        + "GROUP BY " + c("Rufnummernblock", "Phone_Number_Block") + " "
                        + "HAVING COUNT(DISTINCT msisdn) > 1 "
                        + "ORDER BY " + c("Anzahl_Rufnummern", "Phone_Number_Count") + " DESC;";

            case RECURRING_PAIRS:
                // A pair of numbers counts once per meeting, not once per pair
                // of records: two numbers with several records each during
                // one stay at a cell site would otherwise reach a count > 1
                // with a single encounter. A meeting is a period in which the
                // two numbers had records within 30 minutes of each other at
                // the same cell site; a gap of more than 30 minutes between
                // such record pairs starts a new meeting (moving together
                // through several cells is one meeting). Pairs with at least
                // two meetings are listed.
                //
                // Record pairs: records in 30-minute buckets, each record only
                // compared with the records of its own and the two
                // neighbouring buckets of the same cell (equality join on
                // cell + bucket, served by an automatic index); every pair with
                // |dt| <= 1800 s lies in adjacent buckets and is matched by
                // exactly one of the offsets -1/0/+1. Numbers and cells are
                // replaced by integers numbered in sort order (n, cl): the
                // record pairs (tens of millions on a dense cell dump) are
                // then grouped by integer keys, about twice as fast as by
                // text and coordinates.
                return "WITH r0 AS MATERIALIZED ("
                        + "  SELECT msisdn, latitude_dec AS lat, longitude_dec AS lon, ts FROM ("
                        + "    SELECT msisdn, latitude_dec, longitude_dec, CAST(strftime('%s', start_time) AS INTEGER) AS ts "
                        + "    FROM response_records "
                        + "    WHERE msisdn IS NOT NULL AND msisdn <> '' AND latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL " + t
                        + "  ) WHERE ts IS NOT NULL"
                        + "), n AS MATERIALIZED ("
                        + "  SELECT msisdn, ROW_NUMBER() OVER (ORDER BY msisdn) AS id FROM (SELECT DISTINCT msisdn FROM r0)"
                        + "), cl AS MATERIALIZED ("
                        + "  SELECT lat, lon, ROW_NUMBER() OVER (ORDER BY lat, lon) AS cid FROM (SELECT DISTINCT lat, lon FROM r0)"
                        + "), r AS MATERIALIZED ("
                        + "  SELECT n.id, cl.cid, r0.ts, r0.ts / 1800 AS bkt "
                        + "  FROM r0 JOIN n ON n.msisdn = r0.msisdn JOIN cl ON cl.lat = r0.lat AND cl.lon = r0.lon"
                        // stored in cell/time order: the lookups of neighbouring
                        // records then hit neighbouring pages
                        + "  ORDER BY cl.cid, r0.ts"
                        + "), d(o) AS (VALUES (-1), (0), (1)), "
                        + "p AS ("
                        + "  SELECT a.id AS x, b.id AS y, MIN(a.ts, b.ts) AS t, a.cid AS cid "
                        + "  FROM r a CROSS JOIN d "
                        // CROSS JOIN keeps this order: b is looked up by (cid, bkt)
                        + "  CROSS JOIN r b ON b.cid = a.cid AND b.bkt = a.bkt + d.o "
                        + "    AND a.id < b.id "
                        + "    AND ABS(a.ts - b.ts) <= 1800"
                        // the record pairs of a pair of numbers within one
                        // 30-minute window at one cell as one interval: no
                        // gap inside can exceed 30 minutes, so the meetings
                        // follow from the intervals (far fewer rows to sort
                        // than the record pairs)
                        + "), q AS ("
                        + "  SELECT x, y, cid, MIN(t) AS t0, MAX(t) AS t1 FROM p GROUP BY x, y, t / 1800, cid"
                        + "), g AS ("
                        + "  SELECT x, y, cid, t0, t1, "
                        + "    CASE WHEN t0 - MAX(t1) OVER (PARTITION BY x, y ORDER BY t0 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) <= 1800 "
                        + "      THEN 0 ELSE 1 END AS nw "
                        + "  FROM q"
                        + "), s AS ("
                        + "  SELECT x, y, SUM(nw) AS meetings, COUNT(DISTINCT cid) AS cells, MIN(t0) AS first, MAX(t1) AS last "
                        + "  FROM g GROUP BY x, y HAVING SUM(nw) > 1"
                        + ") "
                        + "SELECT na.msisdn AS " + c("Rufnummer_A", "Phone_Number_A") + ", nb.msisdn AS " + c("Rufnummer_B", "Phone_Number_B") + ", "
                        + "s.meetings AS " + c("Anzahl_Begegnungen", "Meeting_Count") + ", "
                        + "s.cells AS " + c("Gemeinsame_Funkzellen", "Shared_Cell_Sites") + ", "
                        + "datetime(s.first, 'unixepoch') AS " + c("Erste_Begegnung", "First_Meeting") + ", "
                        + "datetime(s.last, 'unixepoch') AS " + c("Letzte_Begegnung", "Last_Meeting") + " "
                        + "FROM s JOIN n na ON na.id = s.x JOIN n nb ON nb.id = s.y "
                        + "ORDER BY s.meetings DESC, s.cells DESC, s.x, s.y;";

            case TWINS:
                // Twins are numbers with exactly the same set of cell sites.
                // Instead of joining every number with every other number of
                // each cell (quadratic in the numbers per cell), every number
                // gets a signature = its sorted list of distinct cell sites
                // (coordinates in full precision), and numbers with equal
                // signatures are paired. The count is the number of cell sites,
                // which is what the shared-cell count of an identical set is.
                return "WITH cells AS ("
                        + "  SELECT DISTINCT msisdn, latitude_dec, longitude_dec FROM response_records "
                        + "  WHERE msisdn IS NOT NULL AND msisdn <> '' "
                        + "    AND latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL " + t
                        + "), sig AS MATERIALIZED ("
                        + "  SELECT msisdn, COUNT(*) AS n, "
                        + "    group_concat(printf('%.17g,%.17g', latitude_dec, longitude_dec), ';' ORDER BY latitude_dec, longitude_dec) AS cellset "
                        + "  FROM cells GROUP BY msisdn"
                        + ") "
                        + "SELECT a.msisdn AS " + c("Rufnummer_A", "Phone_Number_A") + ", b.msisdn AS " + c("Rufnummer_B", "Phone_Number_B") + ", "
                        + "a.n AS " + c("Gemeinsame_Funkzellen", "Shared_Cell_Sites") + " "
                        + "FROM sig a "
                        + "JOIN sig b ON b.cellset = a.cellset AND a.msisdn < b.msisdn "
                        + "ORDER BY " + c("Gemeinsame_Funkzellen", "Shared_Cell_Sites") + " DESC;";

            case CELL_SITES:
                return "SELECT ROUND(latitude_dec,5) || ', ' || ROUND(longitude_dec,5) AS " + c("Funkzelle", "Cell_Site") + ", "
                        + "COUNT(*) AS " + c("Anzahl_Datensaetze", "Record_Count") + ", MIN(start_time) AS " + c("Erstmals", "First_Seen") + ", MAX(start_time) AS " + c("Zuletzt", "Last_Seen") + " "
                        + "FROM response_records "
                        + "WHERE latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL " + t
                        + "GROUP BY latitude_dec, longitude_dec "
                        + "ORDER BY " + c("Anzahl_Datensaetze", "Record_Count") + " DESC;";

            case IDENTIFIER_INVENTORY:
                return "SELECT '" + c("Rufnummer", "Phone_Number") + "' AS " + c("Kennungstyp", "Identifier_Type") + ", msisdn AS " + c("Kennung", "Identifier") + ", COUNT(*) AS " + c("Anzahl_Datensaetze", "Record_Count") + ", "
                        + "MIN(start_time) AS " + c("Erstmals", "First_Seen") + ", MAX(start_time) AS " + c("Zuletzt", "Last_Seen") + " "
                        + "FROM response_records WHERE msisdn IS NOT NULL AND msisdn <> '' " + t
                        + "GROUP BY +msisdn "
                        + "UNION ALL "
                        + "SELECT 'IMSI' AS " + c("Kennungstyp", "Identifier_Type") + ", imsi AS " + c("Kennung", "Identifier") + ", COUNT(*) AS " + c("Anzahl_Datensaetze", "Record_Count") + ", "
                        + "MIN(start_time) AS " + c("Erstmals", "First_Seen") + ", MAX(start_time) AS " + c("Zuletzt", "Last_Seen") + " "
                        + "FROM response_records WHERE imsi IS NOT NULL AND imsi <> '' " + t
                        + "GROUP BY +imsi "
                        + "UNION ALL "
                        + "SELECT '" + c("IMEI/Geräte-ID", "IMEI/Device_ID") + "' AS " + c("Kennungstyp", "Identifier_Type") + ", na_device_id AS " + c("Kennung", "Identifier") + ", COUNT(*) AS " + c("Anzahl_Datensaetze", "Record_Count") + ", "
                        + "MIN(start_time) AS " + c("Erstmals", "First_Seen") + ", MAX(start_time) AS " + c("Zuletzt", "Last_Seen") + " "
                        + "FROM response_records WHERE na_device_id IS NOT NULL AND na_device_id <> '' " + t
                        + "GROUP BY +na_device_id "
                        + "ORDER BY " + c("Kennungstyp", "Identifier_Type") + ", " + c("Kennung", "Identifier") + ";";

            case IDENTIFIER_TIMELINE:
                return "SELECT start_time AS " + c("Zeit", "Time") + ", end_time AS " + c("Ende", "End_Time") + ", "
                        + "ROUND(latitude_dec,5) || ', ' || ROUND(longitude_dec,5) AS " + c("Funkzelle", "Cell_Site") + ", "
                        + "CASE WHEN call_indicator IN ('MOC','MTC') THEN '" + c("Telefonie", "Telephony") + "' "
                        + "WHEN call_indicator IN ('MOM','MTM') THEN '" + c("Kurzmitteilung (SMS)", "Short message (SMS)") + "' "
                        + "WHEN call_indicator = 'SGP' OR nw_access_type IN ('mobilePacketData','GPRS') THEN '" + c("GPRS/Datenverbindung", "GPRS/data connection") + "' "
                        + "ELSE '" + c("Unbekannt (", "Unknown (") + "' || COALESCE(call_indicator, nw_access_type, '?') || ')' END AS " + c("Verbindungstyp", "Connection_Type") + ", "
                        + "msisdn AS " + c("Rufnummer", "Phone_Number") + ", imsi AS IMSI, na_device_id AS IMEI, request_id AS " + c("Quelle", "Source") + " "
                        + "FROM response_records "
                        + "WHERE " + identifierMatchClause(params.identifier, "") + t
                        + "ORDER BY start_time;";

            case COMMUNICATION_PARTNERS_DETAIL:
                // a = records of the identifier, b = only records of the same
                // sessions (see identCte / sessionPartnersCte); same result as
                // the self-join over the whole table.
                return "WITH " + identCte(params.identifier) + ", " + sessionPartnersCte() + " "
                        + "SELECT a.start_time AS " + c("Zeit", "Time") + ", a.call_indicator AS " + c("Richtung", "Direction") + ", "
                        + "ROUND(a.latitude_dec,5) || ', ' || ROUND(a.longitude_dec,5) AS " + c("Funkzelle", "Cell_Site") + ", "
                        + "b.msisdn AS " + c("Gegenpart_Rufnummer", "Counterpart_Phone_Number") + ", b.imsi AS " + c("Gegenpart_IMSI", "Counterpart_IMSI") + ", b.na_device_id AS " + c("Gegenpart_IMEI", "Counterpart_IMEI") + ", "
                        + "ROUND(b.latitude_dec,5) || ', ' || ROUND(b.longitude_dec,5) AS " + c("Funkzelle_Gegenpart", "Counterpart_Cell_Site") + ", "
                        + "b.start_time AS " + c("Zeit_Gegenpart", "Counterpart_Time") + " "
                        + "FROM ident a "
                        + "LEFT JOIN partners b "
                        + "  ON a.session_id = b.session_id AND b.session_id IS NOT NULL AND b.session_id <> '' "
                        + "  AND COALESCE(b.msisdn,'')||'|'||COALESCE(b.imsi,'')||'|'||COALESCE(b.na_device_id,'') "
                        + "      <> COALESCE(a.msisdn,'')||'|'||COALESCE(a.imsi,'')||'|'||COALESCE(a.na_device_id,'') "
                        + "WHERE " + identifierMatchClause(params.identifier, "a.")
                        + "  AND a.call_indicator IN ('MOC','MTC','MOM','MTM') " + ta
                        + "ORDER BY a.start_time;";

            case IDENTIFIER_CO_OCCURRENCE: {
                // Session and cell-site parts join the identifier's records
                // only with the records of its sessions / its cell sites; same
                // result as the self-joins over the whole table. Cell-site
                // part: ia = the identifier's records with a time, sc = the
                // records at its cell sites within 30 minutes of its first and
                // last record, only the columns needed (before: every column
                // of every record at those cells, on a cell dump with few
                // cell sites nearly the whole table); compared only within
                // the same and the neighbouring 30-minute buckets of the same
                // cell (every pair with |dt| <= 1800 s lies in adjacent
                // buckets, and exactly one offset matches it) instead of with
                // every record of the cell.
                String key = "COALESCE(%1$s.msisdn,'')||'|'||COALESCE(%1$s.imsi,'')||'|'||COALESCE(%1$s.na_device_id,'')";
                return "WITH " + identCte(params.identifier) + ", " + sessionPartnersCte() + ", "
                        + "ia AS MATERIALIZED ("
                        + "  SELECT *, ts / 1800 AS bkt FROM ("
                        + "    SELECT msisdn, imsi, na_device_id, latitude_dec AS lat, longitude_dec AS lon, "
                        + "      CAST(strftime('%s', start_time) AS INTEGER) AS ts "
                        + "    FROM ident WHERE latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL " + t
                        + "  ) WHERE ts IS NOT NULL"
                        + "), sc AS MATERIALIZED ("
                        + "  SELECT *, ts / 1800 AS bkt FROM ("
                        + "    SELECT msisdn, imsi, na_device_id, start_time, latitude_dec AS lat, longitude_dec AS lon, "
                        + "      CAST(strftime('%s', start_time) AS INTEGER) AS ts "
                        + "    FROM response_records WHERE (latitude_dec, longitude_dec) IN (SELECT lat, lon FROM ia)"
                        + "  ) WHERE ts BETWEEN (SELECT MIN(ts) FROM ia) - 1800 AND (SELECT MAX(ts) FROM ia) + 1800"
                        + "), d(o) AS (VALUES (-1), (0), (1)) "
                        + "SELECT '" + c("Im selben Datensatz", "In the same record") + "' AS " + c("Beziehung", "Relationship") + ", msisdn AS " + c("Rufnummer", "Phone_Number") + ", imsi AS IMSI, "
                        + "na_device_id AS IMEI, start_time AS " + c("Zeit", "Time") + " "
                        + "FROM ident "
                        + "WHERE " + identifierMatchClause(params.identifier, "") + t
                        + "UNION ALL "
                        + "SELECT '" + c("Gesprächspartner (gleiche session_id)", "Call partner (same session_id)") + "' AS " + c("Beziehung", "Relationship") + ", "
                        + "b.msisdn AS " + c("Rufnummer", "Phone_Number") + ", b.imsi AS IMSI, b.na_device_id AS IMEI, b.start_time AS " + c("Zeit", "Time") + " "
                        + "FROM ident a JOIN partners b ON a.session_id = b.session_id "
                        + "WHERE " + identifierMatchClause(params.identifier, "a.")
                        + "  AND a.session_id IS NOT NULL AND a.session_id <> '' "
                        + "  AND COALESCE(b.msisdn,'')||'|'||COALESCE(b.imsi,'')||'|'||COALESCE(b.na_device_id,'') "
                        + "      <> COALESCE(a.msisdn,'')||'|'||COALESCE(a.imsi,'')||'|'||COALESCE(a.na_device_id,'') " + ta
                        + "UNION ALL "
                        + "SELECT '" + c("Gleiche Funkzelle (±30 Min.)", "Same cell site (±30 min.)") + "' AS " + c("Beziehung", "Relationship") + ", "
                        + "b.msisdn AS " + c("Rufnummer", "Phone_Number") + ", b.imsi AS IMSI, b.na_device_id AS IMEI, b.start_time AS " + c("Zeit", "Time") + " "
                        + "FROM ia a CROSS JOIN d "
                        // CROSS JOIN keeps this order: b is looked up by (lat, lon, bkt)
                        + "CROSS JOIN sc b ON b.lat = a.lat AND b.lon = a.lon AND b.bkt = a.bkt + d.o "
                        + "  AND ABS(a.ts - b.ts) <= 1800 "
                        + "  AND " + String.format(key, "b") + " <> " + String.format(key, "a") + " "
                        + "ORDER BY " + c("Beziehung", "Relationship") + ", " + c("Zeit", "Time") + ";";
            }

            case TRANSMISSION_COUNT:
                return "SELECT CASE "
                        + "WHEN call_indicator IN ('MOC','MTC') THEN '" + c("Telefonie", "Telephony") + "' "
                        + "WHEN call_indicator IN ('MOM','MTM') THEN '" + c("Kurzmitteilung (SMS)", "Short message (SMS)") + "' "
                        + "WHEN call_indicator = 'SGP' OR nw_access_type IN ('mobilePacketData','GPRS') THEN '" + c("GPRS/Datenverbindung", "GPRS/data connection") + "' "
                        + "ELSE '" + c("Unbekannt (", "Unknown (") + "' || COALESCE(call_indicator, nw_access_type, '?') || ') ' "
                        + "END AS " + c("Verbindungstyp", "Connection_Type") + ", COUNT(*) AS " + c("Anzahl", "Count") + " "
                        + "FROM response_records "
                        + "WHERE " + identifierMatchClause(params.identifier, "") + t
                        + "GROUP BY " + c("Verbindungstyp", "Connection_Type") + " "
                        + "ORDER BY " + c("Anzahl", "Count") + " DESC;";

            case DEVICE_INFO_LOOKUP:
                // The online TAC→Hersteller/Modell lookup (TacLookupService) is
                // performed separately by PoliceAnalysisPane; this SQL only
                // surfaces what the local dataset itself knows about the IMEI.
                return "SELECT na_device_id AS IMEI, imsi AS IMSI, msisdn AS " + c("Rufnummer", "Phone_Number") + ", start_time AS " + c("Zeit", "Time") + ", "
                        + "ROUND(latitude_dec,5) || ', ' || ROUND(longitude_dec,5) AS " + c("Funkzelle", "Cell_Site") + " "
                        + "FROM response_records "
                        + "WHERE na_device_id = '" + escape(params.identifier.trim()) + "' " + t
                        + "ORDER BY start_time;";

            case FILTER_BY_COUNTRY: {
                String prefix = params.identifier.trim();
                String countryName = CountryCallingCodeLookup.getCountryForNumber(prefix);
                String landLiteral = escape(countryName != null ? countryName : c("Unbekannt (Präfix ", "Unknown (prefix ") + prefix + "…)");
                return "SELECT msisdn AS " + c("Rufnummer", "Phone_Number") + ", '" + landLiteral + "' AS " + c("Land", "Country") + ", "
                        + "COUNT(*) AS " + c("Anzahl_Datensaetze", "Record_Count") + ", "
                        + "MIN(start_time) AS " + c("Erstmals", "First_Seen") + ", MAX(start_time) AS " + c("Zuletzt", "Last_Seen") + " "
                        + "FROM response_records "
                        + "WHERE msisdn LIKE '" + escape(prefix) + "%' " + t
                        + "GROUP BY +msisdn "
                        + "ORDER BY " + c("Rufnummer", "Phone_Number") + ";";
            }

            case COUNTRY_HISTOGRAM:
                // Counted per distinct number first (served by the msisdn
                // index), so the long country CASE runs once per number instead
                // of once per record. Every number maps to exactly one country,
                // so COUNT(*) over the numbers equals COUNT(DISTINCT msisdn) and
                // SUM(n) equals the record count.
                return "WITH m AS ("
                        + "  SELECT msisdn, COUNT(*) AS n FROM response_records "
                        + "  WHERE msisdn IS NOT NULL AND msisdn <> '' " + t
                        + "  GROUP BY msisdn"
                        + ") "
                        + "SELECT "
                        + CountryCallingCodeLookup.buildSqlCaseExpression(
                                "msisdn", "'" + c("Unbekannt/Ausland (Präfix ", "Unknown/abroad (prefix ") + "' || SUBSTR(msisdn,1,3) || '…)'")
                        + " AS " + c("Land", "Country") + ", "
                        + "COUNT(*) AS " + c("Anzahl_Rufnummern", "Phone_Number_Count") + ", SUM(n) AS " + c("Anzahl_Datensaetze", "Record_Count") + " "
                        + "FROM m "
                        + "GROUP BY " + c("Land", "Country") + " "
                        + "ORDER BY " + c("Anzahl_Rufnummern", "Phone_Number_Count") + " DESC;";

            case PRESENCE_IN_AREA:
                return "SELECT start_time AS " + c("Zeit", "Time") + ", "
                        + "ROUND(latitude_dec,5) || ', ' || ROUND(longitude_dec,5) AS " + c("Funkzelle", "Cell_Site") + ", "
                        + "msisdn AS " + c("Rufnummer", "Phone_Number") + ", imsi AS IMSI, na_device_id AS IMEI "
                        + "FROM response_records "
                        + "WHERE " + identifierMatchClause(params.identifier, "")
                        + "  AND latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL "
                        + areaBoundingBoxClause(params, type) + t
                        + "ORDER BY start_time;";

            case FREQUENT_COMPANION: {
                // Counted are meetings, not pairs of records (as in
                // RECURRING_PAIRS): a meeting is a period in which the
                // identifier and the companion had records within 30 minutes
                // of each other at the same cell site; a gap of more than 30
                // minutes starts a new one. a = records of the identifier,
                // b = only the records at its cell sites, compared within
                // the same and the neighbouring 30-minute buckets (every
                // pair with |dt| <= 1800 s lies in adjacent buckets).
                String cols = "msisdn, imsi, na_device_id, latitude_dec AS lat, longitude_dec AS lon, "
                        + "CAST(strftime('%s', start_time) AS INTEGER) AS ts";
                String key = "COALESCE(%1$s.msisdn,'')||'|'||COALESCE(%1$s.imsi,'')||'|'||COALESCE(%1$s.na_device_id,'')";
                return "WITH ident AS MATERIALIZED ("
                        + "  SELECT *, ts / 1800 AS bkt FROM (SELECT " + cols + " FROM response_records "
                        + "    WHERE " + identifierMatchClause(params.identifier, "")
                        + "    AND latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL " + t + ") WHERE ts IS NOT NULL"
                        + "), samecell AS MATERIALIZED ("
                        + "  SELECT *, ts / 1800 AS bkt FROM (SELECT " + cols + " FROM response_records "
                        + "    WHERE (latitude_dec, longitude_dec) IN (SELECT lat, lon FROM ident) " + t + ") WHERE ts IS NOT NULL"
                        + "), d(o) AS (VALUES (-1), (0), (1)), "
                        + "p AS ("
                        + "  SELECT b.msisdn AS m, b.imsi AS i, b.na_device_id AS e, MIN(a.ts, b.ts) AS t, a.lat AS lat, a.lon AS lon "
                        + "  FROM ident a CROSS JOIN d "
                        // CROSS JOIN keeps this order: b is looked up by (lat, lon, bkt)
                        + "  CROSS JOIN samecell b ON b.lat = a.lat AND b.lon = a.lon AND b.bkt = a.bkt + d.o "
                        + "    AND ABS(a.ts - b.ts) <= 1800 "
                        + "    AND " + String.format(key, "b") + " <> " + String.format(key, "a")
                        + "), q AS ("
                        + "  SELECT m, i, e, lat, lon, MIN(t) AS t0, MAX(t) AS t1 FROM p GROUP BY m, i, e, t / 1800, lat, lon"
                        + "), g AS ("
                        + "  SELECT m, i, e, lat, lon, t0, t1, "
                        + "    CASE WHEN t0 - MAX(t1) OVER (PARTITION BY m, i, e ORDER BY t0 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) <= 1800 "
                        + "      THEN 0 ELSE 1 END AS nw "
                        + "  FROM q"
                        + ") "
                        + "SELECT m AS " + c("Begleiter_Rufnummer", "Companion_Phone_Number") + ", i AS " + c("Begleiter_IMSI", "Companion_IMSI") + ", "
                        + "e AS " + c("Begleiter_IMEI", "Companion_IMEI") + ", "
                        + "SUM(nw) AS " + c("Anzahl_Begegnungen", "Meeting_Count") + ", "
                        + "COUNT(DISTINCT printf('%.17g,%.17g', lat, lon)) AS " + c("Gemeinsame_Funkzellen", "Shared_Cell_Sites") + ", "
                        + "datetime(MIN(t0), 'unixepoch') AS " + c("Erste_Begegnung", "First_Meeting") + ", "
                        + "datetime(MAX(t1), 'unixepoch') AS " + c("Letzte_Begegnung", "Last_Meeting") + " "
                        + "FROM g GROUP BY m, i, e "
                        + "ORDER BY " + c("Anzahl_Begegnungen", "Meeting_Count") + " DESC, " + c("Gemeinsame_Funkzellen", "Shared_Cell_Sites") + " DESC, m, i, e;";
            }

            case COMM_SAME_CELL: {
                // A match needs two records with different numbers in the same
                // session at the same cell site. So first the (session, cell
                // site) groups with at least two different numbers are
                // determined (one sorted pass), and only their records are
                // joined - instead of an automatic index over every record of
                // the table. Filters and the time filter (on both sides, as
                // before) are applied to every record that is joined.
                String f = "session_id IS NOT NULL AND session_id <> '' AND msisdn IS NOT NULL "
                        + "AND latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL " + t;
                return "WITH g AS MATERIALIZED ("
                        + "  SELECT session_id, latitude_dec, longitude_dec FROM response_records WHERE " + f
                        + "  GROUP BY session_id, latitude_dec, longitude_dec HAVING MIN(msisdn) < MAX(msisdn)"
                        + "), r AS MATERIALIZED ("
                        + "  SELECT session_id, msisdn, latitude_dec, longitude_dec, start_time FROM response_records WHERE " + f
                        + "  AND (session_id, latitude_dec, longitude_dec) IN (SELECT session_id, latitude_dec, longitude_dec FROM g)"
                        + ") "
                        + "SELECT a.msisdn AS " + c("Rufnummer_A", "Phone_Number_A") + ", b.msisdn AS " + c("Rufnummer_B", "Phone_Number_B") + ", a.session_id AS " + c("Session", "Session") + ", "
                        + "ROUND(a.latitude_dec,5) || ', ' || ROUND(a.longitude_dec,5) AS " + c("Funkzelle", "Cell_Site") + ", "
                        + "a.start_time AS " + c("Zeit", "Time") + " "
                        + "FROM r a "
                        + "JOIN r b "
                        + "  ON a.session_id = b.session_id AND a.msisdn < b.msisdn "
                        + "  AND a.latitude_dec = b.latitude_dec AND a.longitude_dec = b.longitude_dec "
                        + "ORDER BY a.start_time DESC;";
            }

            default:
                throw new IllegalArgumentException(Lang.t("Unbekannter Auswertungstyp: ", "Unknown analysis type: ") + type);
        }
    }
}
