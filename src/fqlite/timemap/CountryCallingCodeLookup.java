package fqlite.timemap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Static lookup table mapping ITU-T E.164 international country calling
 * codes (the digits a Rufnummer starts with after stripping the leading
 * {@code '+'}/{@code '00'}, e.g. {@code "49"} for Germany) to a
 * human-readable country name.
 *
 * <p>Used by {@link PoliceAnalysisQueries} to show an actual country name
 * for foreign numbers instead of just the raw numeric prefix. Covers the
 * calling codes of essentially all assigned countries/territories, but —
 * like {@link MncLookup} — is a hand-maintained table, not a verified
 * ITU/E.164 data feed. Two known limitations carry over from the ITU plan
 * itself, not from this table being incomplete:</p>
 * <ul>
 *   <li>The single-digit codes {@code "1"} (NANP: USA, Kanada und diverse
 *       karibische Staaten) and {@code "7"} (Russland und Kasachstan) are
 *       shared by several countries that are only distinguished by
 *       subsequent area-code digits — this table reports the umbrella
 *       region name for those two codes.</li>
 *   <li>Numbers that don't start with any known calling code (e.g.
 *       malformed or anonymised entries) resolve to {@code null}/no match.</li>
 * </ul>
 */
public final class CountryCallingCodeLookup {

    private CountryCallingCodeLookup() {}

    // ── Calling code → country/region name (German) ───────────────────────────
    private static final Map<String, String> CODE_TO_COUNTRY = new LinkedHashMap<>();

    // Prefixes sorted longest-first so a CASE/WHEN ladder built from this
    // list never lets a shorter prefix shadow a more specific longer one.
    private static final List<String> PREFIXES_BY_LENGTH_DESC = new ArrayList<>();

    static {
        // ── 1-digit codes (NANP / Russia-Kasachstan; shared by multiple countries) ──
        add("1", "USA/Kanada (NANP)");
        add("7", "Russland/Kasachstan");

        // ── 2-digit codes ───────────────────────────────────────────────────────
        add("20", "Ägypten");
        add("27", "Südafrika");
        add("30", "Griechenland");
        add("31", "Niederlande");
        add("32", "Belgien");
        add("33", "Frankreich");
        add("34", "Spanien");
        add("36", "Ungarn");
        add("39", "Italien");
        add("40", "Rumänien");
        add("41", "Schweiz");
        add("43", "Österreich");
        add("44", "Vereinigtes Königreich");
        add("45", "Dänemark");
        add("46", "Schweden");
        add("47", "Norwegen");
        add("48", "Polen");
        add("49", "Deutschland");
        add("51", "Peru");
        add("52", "Mexiko");
        add("53", "Kuba");
        add("54", "Argentinien");
        add("55", "Brasilien");
        add("56", "Chile");
        add("57", "Kolumbien");
        add("58", "Venezuela");
        add("60", "Malaysia");
        add("61", "Australien");
        add("62", "Indonesien");
        add("63", "Philippinen");
        add("64", "Neuseeland");
        add("65", "Singapur");
        add("66", "Thailand");
        add("81", "Japan");
        add("82", "Südkorea");
        add("84", "Vietnam");
        add("86", "China");
        add("90", "Türkei");
        add("91", "Indien");
        add("92", "Pakistan");
        add("93", "Afghanistan");
        add("94", "Sri Lanka");
        add("95", "Myanmar");
        add("98", "Iran");

        // ── 3-digit codes ───────────────────────────────────────────────────────
        add("211", "Südsudan");
        add("212", "Marokko");
        add("213", "Algerien");
        add("216", "Tunesien");
        add("218", "Libyen");
        add("220", "Gambia");
        add("221", "Senegal");
        add("222", "Mauretanien");
        add("223", "Mali");
        add("224", "Guinea");
        add("225", "Elfenbeinküste");
        add("226", "Burkina Faso");
        add("227", "Niger");
        add("228", "Togo");
        add("229", "Benin");
        add("230", "Mauritius");
        add("231", "Liberia");
        add("232", "Sierra Leone");
        add("233", "Ghana");
        add("234", "Nigeria");
        add("235", "Tschad");
        add("236", "Zentralafrikanische Republik");
        add("237", "Kamerun");
        add("238", "Kap Verde");
        add("239", "São Tomé und Príncipe");
        add("240", "Äquatorialguinea");
        add("241", "Gabun");
        add("242", "Kongo (Republik)");
        add("243", "Kongo (Demokratische Republik)");
        add("244", "Angola");
        add("245", "Guinea-Bissau");
        add("248", "Seychellen");
        add("249", "Sudan");
        add("250", "Ruanda");
        add("251", "Äthiopien");
        add("252", "Somalia");
        add("253", "Dschibuti");
        add("254", "Kenia");
        add("255", "Tansania");
        add("256", "Uganda");
        add("257", "Burundi");
        add("258", "Mosambik");
        add("260", "Sambia");
        add("261", "Madagaskar");
        add("262", "Réunion/Mayotte");
        add("263", "Simbabwe");
        add("264", "Namibia");
        add("265", "Malawi");
        add("266", "Lesotho");
        add("267", "Botswana");
        add("268", "Eswatini");
        add("269", "Komoren");
        add("291", "Eritrea");
        add("297", "Aruba");
        add("298", "Färöer-Inseln");
        add("299", "Grönland");
        add("350", "Gibraltar");
        add("351", "Portugal");
        add("352", "Luxemburg");
        add("353", "Irland");
        add("354", "Island");
        add("355", "Albanien");
        add("356", "Malta");
        add("357", "Zypern");
        add("358", "Finnland");
        add("359", "Bulgarien");
        add("370", "Litauen");
        add("371", "Lettland");
        add("372", "Estland");
        add("373", "Moldau");
        add("374", "Armenien");
        add("375", "Belarus");
        add("376", "Andorra");
        add("377", "Monaco");
        add("378", "San Marino");
        add("380", "Ukraine");
        add("381", "Serbien");
        add("382", "Montenegro");
        add("383", "Kosovo");
        add("385", "Kroatien");
        add("386", "Slowenien");
        add("387", "Bosnien und Herzegowina");
        add("389", "Nordmazedonien");
        add("420", "Tschechien");
        add("421", "Slowakei");
        add("423", "Liechtenstein");
        add("500", "Falklandinseln");
        add("501", "Belize");
        add("502", "Guatemala");
        add("503", "El Salvador");
        add("504", "Honduras");
        add("505", "Nicaragua");
        add("506", "Costa Rica");
        add("507", "Panama");
        add("509", "Haiti");
        add("590", "Guadeloupe");
        add("591", "Bolivien");
        add("592", "Guyana");
        add("593", "Ecuador");
        add("594", "Französisch-Guayana");
        add("595", "Paraguay");
        add("596", "Martinique");
        add("597", "Suriname");
        add("598", "Uruguay");
        add("670", "Osttimor");
        add("673", "Brunei");
        add("674", "Nauru");
        add("675", "Papua-Neuguinea");
        add("676", "Tonga");
        add("677", "Salomonen");
        add("678", "Vanuatu");
        add("679", "Fidschi");
        add("680", "Palau");
        add("685", "Samoa");
        add("686", "Kiribati");
        add("687", "Neukaledonien");
        add("689", "Französisch-Polynesien");
        add("691", "Mikronesien");
        add("692", "Marshallinseln");
        add("850", "Nordkorea");
        add("852", "Hongkong");
        add("853", "Macau");
        add("855", "Kambodscha");
        add("856", "Laos");
        add("880", "Bangladesch");
        add("886", "Taiwan");
        add("960", "Malediven");
        add("961", "Libanon");
        add("962", "Jordanien");
        add("963", "Syrien");
        add("964", "Irak");
        add("965", "Kuwait");
        add("966", "Saudi-Arabien");
        add("967", "Jemen");
        add("968", "Oman");
        add("970", "Palästinensische Gebiete");
        add("971", "Vereinigte Arabische Emirate");
        add("972", "Israel");
        add("973", "Bahrain");
        add("974", "Katar");
        add("975", "Bhutan");
        add("976", "Mongolei");
        add("977", "Nepal");
        add("992", "Tadschikistan");
        add("993", "Turkmenistan");
        add("994", "Aserbaidschan");
        add("995", "Georgien");
        add("996", "Kirgisistan");
        add("998", "Usbekistan");

        PREFIXES_BY_LENGTH_DESC.sort(Comparator.comparingInt(String::length).reversed());
    }

    private static void add(String prefix, String country) {
        CODE_TO_COUNTRY.put(prefix, country);
        PREFIXES_BY_LENGTH_DESC.add(prefix);
    }

    // ── colloquial/alternate country names → calling code ─────────────────────
    // Covers names an analyst is likely to type that don't match the official
    // (German) name stored in CODE_TO_COUNTRY verbatim. Hand-maintained, like
    // the rest of this table — not an exhaustive alias list.
    private static final Map<String, String> ALIAS_TO_PREFIX = new LinkedHashMap<>();

    static {
        aliasPrefix("usa", "1");
        aliasPrefix("amerika", "1");
        aliasPrefix("vereinigte staaten", "1");
        aliasPrefix("vereinigte staaten von amerika", "1");
        aliasPrefix("kanada", "1");
        aliasPrefix("grossbritannien", "44");
        aliasPrefix("uk", "44");
        aliasPrefix("england", "44");
        aliasPrefix("schottland", "44");
        aliasPrefix("wales", "44");
        aliasPrefix("nordirland", "44");
        aliasPrefix("holland", "31");
        aliasPrefix("tschechische republik", "420");
        aliasPrefix("vae", "971");
        aliasPrefix("emirate", "971");
        aliasPrefix("elfenbeinkueste", "225");
        aliasPrefix("suedkorea", "82");
        aliasPrefix("nordkorea", "850");
        aliasPrefix("russland", "7");
        aliasPrefix("kasachstan", "7");

        // ── adjectival forms ("französische Rufnummern", "deutsche Nummern", …) ──
        // Bare adjective stems (no inflection ending) — matched via prefix in
        // findPrefixForCountryName() below, so "französische", "französischen"
        // etc. all resolve through the "franzosisch" entry.
        aliasPrefix("deutsch", "49");
        aliasPrefix("franzosisch", "33");
        aliasPrefix("polnisch", "48");
        aliasPrefix("spanisch", "34");
        aliasPrefix("italienisch", "39");
        aliasPrefix("britisch", "44");
        aliasPrefix("englisch", "44");
        aliasPrefix("niederlandisch", "31");
        aliasPrefix("hollandisch", "31");
        aliasPrefix("osterreichisch", "43");
        aliasPrefix("schweizerisch", "41");
        aliasPrefix("russisch", "7");
        aliasPrefix("amerikanisch", "1");
        aliasPrefix("turkisch", "90");
        aliasPrefix("chinesisch", "86");
        aliasPrefix("japanisch", "81");
        aliasPrefix("belgisch", "32");
        aliasPrefix("portugiesisch", "351");
        aliasPrefix("griechisch", "30");
        aliasPrefix("ungarisch", "36");
        aliasPrefix("tschechisch", "420");
        aliasPrefix("schwedisch", "46");
        aliasPrefix("norwegisch", "47");
        aliasPrefix("danisch", "45");
        aliasPrefix("finnisch", "358");
        aliasPrefix("ukrainisch", "380");
        aliasPrefix("kroatisch", "385");
        aliasPrefix("serbisch", "381");
        aliasPrefix("rumanisch", "40");
        aliasPrefix("bulgarisch", "359");
        aliasPrefix("slowakisch", "421");
        aliasPrefix("slowenisch", "386");
        aliasPrefix("irisch", "353");
        aliasPrefix("indisch", "91");
        aliasPrefix("pakistanisch", "92");
        aliasPrefix("brasilianisch", "55");
        aliasPrefix("mexikanisch", "52");
        aliasPrefix("kanadisch", "1");
        aliasPrefix("australisch", "61");
        aliasPrefix("agyptisch", "20");
        aliasPrefix("marokkanisch", "212");
        aliasPrefix("israelisch", "972");
        aliasPrefix("syrisch", "963");
        aliasPrefix("iranisch", "98");
        aliasPrefix("irakisch", "964");
    }

    private static void aliasPrefix(String alias, String prefix) {
        ALIAS_TO_PREFIX.put(normalize(alias), prefix);
    }

    /** Lower-cases and strips German umlauts/ß so look-ups are accent-insensitive. */
    private static String normalize(String s) {
        return s.trim().toLowerCase()
                .replace("ä", "a").replace("ö", "o").replace("ü", "u").replace("ß", "ss");
    }

    /**
     * Returns the country/region name for the given Rufnummer (without
     * leading {@code '+'}/{@code '00'}), matched longest-prefix-first, or
     * {@code null} if no known calling code matches.
     */
    public static String getCountryForNumber(String msisdn) {
        if (msisdn == null) return null;
        String digits = msisdn.trim();
        for (String prefix : PREFIXES_BY_LENGTH_DESC) {
            if (digits.startsWith(prefix)) return CODE_TO_COUNTRY.get(prefix);
        }
        return null;
    }

    /** Returns the stored country/region name for an exact calling-code prefix, or {@code null}. */
    public static String getCountryNameByPrefix(String prefix) {
        return prefix == null ? null : CODE_TO_COUNTRY.get(prefix.trim());
    }

    /**
     * Reverse look-up: resolves a (German, possibly colloquial) country name
     * mentioned in a natural-language request back to its E.164 calling-code
     * prefix, for building a {@code msisdn LIKE 'prefix%'} filter.
     *
     * <p>Tries, in order: (1) the alias table, (2) an exact match against a
     * stored country/region name, (3) a loose substring match in either
     * direction (so e.g. "Kongo" matches "Kongo (Republik)"; if several
     * stored names contain the input, the longest stored name wins, since
     * that's the more specific match). Returns {@code null} if nothing
     * matches — the caller should treat that as "country not recognised"
     * rather than guessing.</p>
     */
    public static String findPrefixForCountryName(String countryName) {
        if (countryName == null || countryName.isBlank()) return null;
        String norm = normalize(countryName);

        if (ALIAS_TO_PREFIX.containsKey(norm)) return ALIAS_TO_PREFIX.get(norm);

        for (Map.Entry<String, String> e : CODE_TO_COUNTRY.entrySet()) {
            if (normalize(e.getValue()).equals(norm)) return e.getKey();
        }

        // Adjective stems in ALIAS_TO_PREFIX (e.g. "franzosisch") are stored
        // without an inflection ending, so match them as a prefix of the
        // (normalized) input — this covers "französische", "französischen",
        // "französischer" etc. all resolving via the one "franzosisch" entry.
        // Longest stem wins in case one stem is itself a prefix of another.
        String bestAlias = null;
        int bestAliasLen = -1;
        for (Map.Entry<String, String> e : ALIAS_TO_PREFIX.entrySet()) {
            if (norm.startsWith(e.getKey()) && e.getKey().length() > bestAliasLen) {
                bestAliasLen = e.getKey().length();
                bestAlias = e.getValue();
            }
        }
        if (bestAlias != null) return bestAlias;

        String bestPrefix = null;
        int bestLen = -1;
        for (Map.Entry<String, String> e : CODE_TO_COUNTRY.entrySet()) {
            String countryNorm = normalize(e.getValue());
            if (countryNorm.contains(norm) || norm.contains(countryNorm)) {
                if (countryNorm.length() > bestLen) {
                    bestLen = countryNorm.length();
                    bestPrefix = e.getKey();
                }
            }
        }
        return bestPrefix;
    }

    private static final Pattern WORD_PATTERN = Pattern.compile("[a-zA-Z]+");

    /**
     * Scans an entire free-form request (not just an already-isolated
     * country field) for any known country name/alias/adjective and
     * returns its E.164 calling-code prefix, or {@code null} if none is
     * mentioned.
     *
     * <p>Used as a deterministic safety net underneath the small local
     * model's intent classification ({@code RAGPipeline#classifyIntent}):
     * that model occasionally fails to recognise a country_filter request
     * for an unusual phrasing or a typo (e.g. "Telefonummern" instead of
     * "Telefonnummern") and falls back to {@code intent="filter"}, which
     * then routes to the free-form SQL generator — a model with no notion
     * of E.164 calling codes at all, observed to invent filters on
     * {@code national_country_code} (the requesting agency's country, not
     * the subscriber's). Since the set of recognisable country names is
     * small, fixed, and enumerable (unlike e.g. a time window), it can be
     * checked directly against the raw text instead of trusting the model
     * to extract it.</p>
     *
     * <p>Multi-word aliases/country names (e.g. "vereinigte staaten",
     * "Vereinigtes Königreich") are matched as a substring of the whole
     * normalized text. Single-word adjective stems (e.g. "franzosisch")
     * are matched per word-token, requiring either an exact match or — for
     * stems of 5+ characters, to avoid short false positives like "uk"/
     * "vae" matching inside unrelated words — a prefix match, so inflected
     * forms ("französischen", "polnischer") still resolve.</p>
     */
    public static String findPrefixMentionedInText(String text) {
        if (text == null || text.isBlank()) return null;
        String norm = normalize(text);

        String bestPrefix = null;
        int bestLen = -1;
        for (Map.Entry<String, String> e : ALIAS_TO_PREFIX.entrySet()) {
            if (norm.contains(e.getKey()) && e.getKey().length() > bestLen) {
                bestLen = e.getKey().length();
                bestPrefix = e.getValue();
            }
        }
        for (Map.Entry<String, String> e : CODE_TO_COUNTRY.entrySet()) {
            String countryNorm = normalize(e.getValue());
            if (norm.contains(countryNorm) && countryNorm.length() > bestLen) {
                bestLen = countryNorm.length();
                bestPrefix = e.getKey();
            }
        }
        if (bestPrefix != null) return bestPrefix;

        Matcher m = WORD_PATTERN.matcher(norm);
        while (m.find()) {
            String word = m.group();
            for (Map.Entry<String, String> e : ALIAS_TO_PREFIX.entrySet()) {
                String key = e.getKey();
                if (word.equals(key) || (key.length() >= 5 && word.startsWith(key))) {
                    return e.getValue();
                }
            }
        }
        return null;
    }

    /**
     * Builds a SQL {@code CASE WHEN <column> LIKE 'xxx%' THEN '<Land>' … END}
     * ladder covering every calling code in this table, longest prefix
     * first so e.g. {@code "212"} (Marokko) is tested before any shorter
     * prefix could shadow it. The caller supplies the {@code ELSE} branch
     * (e.g. a generic "Ausland (Präfix …)" fallback) since that varies by
     * analysis.
     *
     * @param column     the SQL column/expression to match against (e.g. {@code "msisdn"})
     * @param elseBranch raw SQL expression used as the {@code ELSE} result (already quoted if a literal)
     */
    public static String buildSqlCaseExpression(String column, String elseBranch) {
        StringBuilder sb = new StringBuilder("CASE ");
        for (String prefix : PREFIXES_BY_LENGTH_DESC) {
            sb.append("WHEN ").append(column).append(" LIKE '").append(prefix).append("%' THEN '")
              .append(CODE_TO_COUNTRY.get(prefix).replace("'", "''")).append("' ");
        }
        sb.append("ELSE ").append(elseBranch).append(" END");
        return sb.toString();
    }
}
