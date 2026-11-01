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
 * human-readable country name, in both German and English (selected at
 * lookup time via {@link Lang#isGerman()} so this table follows the same
 * runtime DE/EN toggle as the rest of the Lighthouse UI).
 *
 * <p>Used by {@link PoliceAnalysisQueries} to show an actual country name
 * for foreign numbers instead of just the raw numeric prefix. Covers the
 * calling codes of essentially all assigned countries/territories, but —
 * like {@link MncLookup} — is a hand-maintained table, not a verified
 * ITU/E.164 data feed. Two known limitations carry over from the ITU plan
 * itself, not from this table being incomplete:</p>
 * <ul>
 *   <li>The single-digit codes {@code "1"} (NANP: USA, Canada and several
 *       Caribbean states) and {@code "7"} (Russia and Kazakhstan) are
 *       shared by several countries that are only distinguished by
 *       subsequent area-code digits — this table reports the umbrella
 *       region name for those two codes.</li>
 *   <li>Numbers that don't start with any known calling code (e.g.
 *       malformed or anonymised entries) resolve to {@code null}/no match.</li>
 * </ul>
 */
public final class CountryCallingCodeLookup {

    private CountryCallingCodeLookup() {}

    // ── Calling code → country/region name (German and English) ───────────────
    private static final Map<String, String> CODE_TO_COUNTRY_DE = new LinkedHashMap<>();
    private static final Map<String, String> CODE_TO_COUNTRY_EN = new LinkedHashMap<>();

    /** Returns the language-appropriate name map for the currently active UI language. */
    private static Map<String, String> activeMap() {
        return Lang.isGerman() ? CODE_TO_COUNTRY_DE : CODE_TO_COUNTRY_EN;
    }

    // Prefixes sorted longest-first so a CASE/WHEN ladder built from this
    // list never lets a shorter prefix shadow a more specific longer one.
    private static final List<String> PREFIXES_BY_LENGTH_DESC = new ArrayList<>();

    static {
        // ── 1-digit codes (NANP / Russia-Kazakhstan; shared by multiple countries) ──
        add("1", "USA/Kanada (NANP)", "USA/Canada (NANP)");
        add("7", "Russland/Kasachstan", "Russia/Kazakhstan");

        // ── 2-digit codes ───────────────────────────────────────────────────────
        add("20", "Ägypten", "Egypt");
        add("27", "Südafrika", "South Africa");
        add("30", "Griechenland", "Greece");
        add("31", "Niederlande", "Netherlands");
        add("32", "Belgien", "Belgium");
        add("33", "Frankreich", "France");
        add("34", "Spanien", "Spain");
        add("36", "Ungarn", "Hungary");
        add("39", "Italien", "Italy");
        add("40", "Rumänien", "Romania");
        add("41", "Schweiz", "Switzerland");
        add("43", "Österreich", "Austria");
        add("44", "Vereinigtes Königreich", "United Kingdom");
        add("45", "Dänemark", "Denmark");
        add("46", "Schweden", "Sweden");
        add("47", "Norwegen", "Norway");
        add("48", "Polen", "Poland");
        add("49", "Deutschland", "Germany");
        add("51", "Peru", "Peru");
        add("52", "Mexiko", "Mexico");
        add("53", "Kuba", "Cuba");
        add("54", "Argentinien", "Argentina");
        add("55", "Brasilien", "Brazil");
        add("56", "Chile", "Chile");
        add("57", "Kolumbien", "Colombia");
        add("58", "Venezuela", "Venezuela");
        add("60", "Malaysia", "Malaysia");
        add("61", "Australien", "Australia");
        add("62", "Indonesien", "Indonesia");
        add("63", "Philippinen", "Philippines");
        add("64", "Neuseeland", "New Zealand");
        add("65", "Singapur", "Singapore");
        add("66", "Thailand", "Thailand");
        add("81", "Japan", "Japan");
        add("82", "Südkorea", "South Korea");
        add("84", "Vietnam", "Vietnam");
        add("86", "China", "China");
        add("90", "Türkei", "Turkey");
        add("91", "Indien", "India");
        add("92", "Pakistan", "Pakistan");
        add("93", "Afghanistan", "Afghanistan");
        add("94", "Sri Lanka", "Sri Lanka");
        add("95", "Myanmar", "Myanmar");
        add("98", "Iran", "Iran");

        // ── 3-digit codes ───────────────────────────────────────────────────────
        add("211", "Südsudan", "South Sudan");
        add("212", "Marokko", "Morocco");
        add("213", "Algerien", "Algeria");
        add("216", "Tunesien", "Tunisia");
        add("218", "Libyen", "Libya");
        add("220", "Gambia", "Gambia");
        add("221", "Senegal", "Senegal");
        add("222", "Mauretanien", "Mauritania");
        add("223", "Mali", "Mali");
        add("224", "Guinea", "Guinea");
        add("225", "Elfenbeinküste", "Ivory Coast");
        add("226", "Burkina Faso", "Burkina Faso");
        add("227", "Niger", "Niger");
        add("228", "Togo", "Togo");
        add("229", "Benin", "Benin");
        add("230", "Mauritius", "Mauritius");
        add("231", "Liberia", "Liberia");
        add("232", "Sierra Leone", "Sierra Leone");
        add("233", "Ghana", "Ghana");
        add("234", "Nigeria", "Nigeria");
        add("235", "Tschad", "Chad");
        add("236", "Zentralafrikanische Republik", "Central African Republic");
        add("237", "Kamerun", "Cameroon");
        add("238", "Kap Verde", "Cape Verde");
        add("239", "São Tomé und Príncipe", "São Tomé and Príncipe");
        add("240", "Äquatorialguinea", "Equatorial Guinea");
        add("241", "Gabun", "Gabon");
        add("242", "Kongo (Republik)", "Congo (Republic)");
        add("243", "Kongo (Demokratische Republik)", "Congo (Democratic Republic)");
        add("244", "Angola", "Angola");
        add("245", "Guinea-Bissau", "Guinea-Bissau");
        add("248", "Seychellen", "Seychelles");
        add("249", "Sudan", "Sudan");
        add("250", "Ruanda", "Rwanda");
        add("251", "Äthiopien", "Ethiopia");
        add("252", "Somalia", "Somalia");
        add("253", "Dschibuti", "Djibouti");
        add("254", "Kenia", "Kenya");
        add("255", "Tansania", "Tanzania");
        add("256", "Uganda", "Uganda");
        add("257", "Burundi", "Burundi");
        add("258", "Mosambik", "Mozambique");
        add("260", "Sambia", "Zambia");
        add("261", "Madagaskar", "Madagascar");
        add("262", "Réunion/Mayotte", "Réunion/Mayotte");
        add("263", "Simbabwe", "Zimbabwe");
        add("264", "Namibia", "Namibia");
        add("265", "Malawi", "Malawi");
        add("266", "Lesotho", "Lesotho");
        add("267", "Botswana", "Botswana");
        add("268", "Eswatini", "Eswatini");
        add("269", "Komoren", "Comoros");
        add("291", "Eritrea", "Eritrea");
        add("297", "Aruba", "Aruba");
        add("298", "Färöer-Inseln", "Faroe Islands");
        add("299", "Grönland", "Greenland");
        add("350", "Gibraltar", "Gibraltar");
        add("351", "Portugal", "Portugal");
        add("352", "Luxemburg", "Luxembourg");
        add("353", "Irland", "Ireland");
        add("354", "Island", "Iceland");
        add("355", "Albanien", "Albania");
        add("356", "Malta", "Malta");
        add("357", "Zypern", "Cyprus");
        add("358", "Finnland", "Finland");
        add("359", "Bulgarien", "Bulgaria");
        add("370", "Litauen", "Lithuania");
        add("371", "Lettland", "Latvia");
        add("372", "Estland", "Estonia");
        add("373", "Moldau", "Moldova");
        add("374", "Armenien", "Armenia");
        add("375", "Belarus", "Belarus");
        add("376", "Andorra", "Andorra");
        add("377", "Monaco", "Monaco");
        add("378", "San Marino", "San Marino");
        add("380", "Ukraine", "Ukraine");
        add("381", "Serbien", "Serbia");
        add("382", "Montenegro", "Montenegro");
        add("383", "Kosovo", "Kosovo");
        add("385", "Kroatien", "Croatia");
        add("386", "Slowenien", "Slovenia");
        add("387", "Bosnien und Herzegowina", "Bosnia and Herzegovina");
        add("389", "Nordmazedonien", "North Macedonia");
        add("420", "Tschechien", "Czechia");
        add("421", "Slowakei", "Slovakia");
        add("423", "Liechtenstein", "Liechtenstein");
        add("500", "Falklandinseln", "Falkland Islands");
        add("501", "Belize", "Belize");
        add("502", "Guatemala", "Guatemala");
        add("503", "El Salvador", "El Salvador");
        add("504", "Honduras", "Honduras");
        add("505", "Nicaragua", "Nicaragua");
        add("506", "Costa Rica", "Costa Rica");
        add("507", "Panama", "Panama");
        add("509", "Haiti", "Haiti");
        add("590", "Guadeloupe", "Guadeloupe");
        add("591", "Bolivien", "Bolivia");
        add("592", "Guyana", "Guyana");
        add("593", "Ecuador", "Ecuador");
        add("594", "Französisch-Guayana", "French Guiana");
        add("595", "Paraguay", "Paraguay");
        add("596", "Martinique", "Martinique");
        add("597", "Suriname", "Suriname");
        add("598", "Uruguay", "Uruguay");
        add("670", "Osttimor", "East Timor");
        add("673", "Brunei", "Brunei");
        add("674", "Nauru", "Nauru");
        add("675", "Papua-Neuguinea", "Papua New Guinea");
        add("676", "Tonga", "Tonga");
        add("677", "Salomonen", "Solomon Islands");
        add("678", "Vanuatu", "Vanuatu");
        add("679", "Fidschi", "Fiji");
        add("680", "Palau", "Palau");
        add("685", "Samoa", "Samoa");
        add("686", "Kiribati", "Kiribati");
        add("687", "Neukaledonien", "New Caledonia");
        add("689", "Französisch-Polynesien", "French Polynesia");
        add("691", "Mikronesien", "Micronesia");
        add("692", "Marshallinseln", "Marshall Islands");
        add("850", "Nordkorea", "North Korea");
        add("852", "Hongkong", "Hong Kong");
        add("853", "Macau", "Macao");
        add("855", "Kambodscha", "Cambodia");
        add("856", "Laos", "Laos");
        add("880", "Bangladesch", "Bangladesh");
        add("886", "Taiwan", "Taiwan");
        add("960", "Malediven", "Maldives");
        add("961", "Libanon", "Lebanon");
        add("962", "Jordanien", "Jordan");
        add("963", "Syrien", "Syria");
        add("964", "Irak", "Iraq");
        add("965", "Kuwait", "Kuwait");
        add("966", "Saudi-Arabien", "Saudi Arabia");
        add("967", "Jemen", "Yemen");
        add("968", "Oman", "Oman");
        add("970", "Palästinensische Gebiete", "Palestinian Territories");
        add("971", "Vereinigte Arabische Emirate", "United Arab Emirates");
        add("972", "Israel", "Israel");
        add("973", "Bahrain", "Bahrain");
        add("974", "Katar", "Qatar");
        add("975", "Bhutan", "Bhutan");
        add("976", "Mongolei", "Mongolia");
        add("977", "Nepal", "Nepal");
        add("992", "Tadschikistan", "Tajikistan");
        add("993", "Turkmenistan", "Turkmenistan");
        add("994", "Aserbaidschan", "Azerbaijan");
        add("995", "Georgien", "Georgia");
        add("996", "Kirgisistan", "Kyrgyzstan");
        add("998", "Usbekistan", "Uzbekistan");

        PREFIXES_BY_LENGTH_DESC.sort(Comparator.comparingInt(String::length).reversed());
    }

    private static void add(String prefix, String countryDe, String countryEn) {
        CODE_TO_COUNTRY_DE.put(prefix, countryDe);
        CODE_TO_COUNTRY_EN.put(prefix, countryEn);
        PREFIXES_BY_LENGTH_DESC.add(prefix);
    }

    // ── colloquial/alternate country names → calling code ─────────────────────
    // Covers names an analyst is likely to type that don't match the stored
    // (German or English) name verbatim. Hand-maintained, like the rest of
    // this table — not an exhaustive alias list. Kept language-agnostic
    // (mixes German and English colloquialisms) since an analyst may type
    // either regardless of the UI's current display language.
    private static final Map<String, String> ALIAS_TO_PREFIX = new LinkedHashMap<>();

    static {
        aliasPrefix("usa", "1");
        aliasPrefix("amerika", "1");
        aliasPrefix("america", "1");
        aliasPrefix("vereinigte staaten", "1");
        aliasPrefix("vereinigte staaten von amerika", "1");
        aliasPrefix("united states", "1");
        aliasPrefix("united states of america", "1");
        aliasPrefix("kanada", "1");
        aliasPrefix("canada", "1");
        aliasPrefix("grossbritannien", "44");
        aliasPrefix("great britain", "44");
        aliasPrefix("uk", "44");
        aliasPrefix("england", "44");
        aliasPrefix("schottland", "44");
        aliasPrefix("scotland", "44");
        aliasPrefix("wales", "44");
        aliasPrefix("nordirland", "44");
        aliasPrefix("northern ireland", "44");
        aliasPrefix("holland", "31");
        aliasPrefix("tschechische republik", "420");
        aliasPrefix("czech republic", "420");
        aliasPrefix("vae", "971");
        aliasPrefix("emirate", "971");
        aliasPrefix("emirates", "971");
        aliasPrefix("elfenbeinkueste", "225");
        aliasPrefix("cote divoire", "225");
        aliasPrefix("suedkorea", "82");
        aliasPrefix("south korea", "82");
        aliasPrefix("nordkorea", "850");
        aliasPrefix("north korea", "850");
        aliasPrefix("russland", "7");
        aliasPrefix("russia", "7");
        aliasPrefix("kasachstan", "7");
        aliasPrefix("kazakhstan", "7");

        // ── adjectival/demonym forms ("französische Rufnummern", "German
        // numbers", …) ── Bare stems (no inflection ending) — matched via
        // prefix in findPrefixForCountryName() below, so "französische",
        // "französischen", "German", "Germany's" etc. all resolve through
        // the corresponding stem entry.
        aliasPrefix("deutsch", "49");
        aliasPrefix("german", "49");
        aliasPrefix("franzosisch", "33");
        aliasPrefix("french", "33");
        aliasPrefix("polnisch", "48");
        aliasPrefix("polish", "48");
        aliasPrefix("spanisch", "34");
        aliasPrefix("spanish", "34");
        aliasPrefix("italienisch", "39");
        aliasPrefix("italian", "39");
        aliasPrefix("britisch", "44");
        aliasPrefix("british", "44");
        aliasPrefix("englisch", "44");
        aliasPrefix("english", "44");
        aliasPrefix("niederlandisch", "31");
        aliasPrefix("hollandisch", "31");
        aliasPrefix("dutch", "31");
        aliasPrefix("osterreichisch", "43");
        aliasPrefix("austrian", "43");
        aliasPrefix("schweizerisch", "41");
        aliasPrefix("swiss", "41");
        aliasPrefix("russisch", "7");
        aliasPrefix("amerikanisch", "1");
        aliasPrefix("american", "1");
        aliasPrefix("turkisch", "90");
        aliasPrefix("turkish", "90");
        aliasPrefix("chinesisch", "86");
        aliasPrefix("chinese", "86");
        aliasPrefix("japanisch", "81");
        aliasPrefix("japanese", "81");
        aliasPrefix("belgisch", "32");
        aliasPrefix("belgian", "32");
        aliasPrefix("portugiesisch", "351");
        aliasPrefix("portuguese", "351");
        aliasPrefix("griechisch", "30");
        aliasPrefix("greek", "30");
        aliasPrefix("ungarisch", "36");
        aliasPrefix("hungarian", "36");
        aliasPrefix("tschechisch", "420");
        aliasPrefix("czech", "420");
        aliasPrefix("schwedisch", "46");
        aliasPrefix("swedish", "46");
        aliasPrefix("norwegisch", "47");
        aliasPrefix("norwegian", "47");
        aliasPrefix("danisch", "45");
        aliasPrefix("danish", "45");
        aliasPrefix("finnisch", "358");
        aliasPrefix("finnish", "358");
        aliasPrefix("ukrainisch", "380");
        aliasPrefix("ukrainian", "380");
        aliasPrefix("kroatisch", "385");
        aliasPrefix("croatian", "385");
        aliasPrefix("serbisch", "381");
        aliasPrefix("serbian", "381");
        aliasPrefix("rumanisch", "40");
        aliasPrefix("romanian", "40");
        aliasPrefix("bulgarisch", "359");
        aliasPrefix("bulgarian", "359");
        aliasPrefix("slowakisch", "421");
        aliasPrefix("slovak", "421");
        aliasPrefix("slowenisch", "386");
        aliasPrefix("slovenian", "386");
        aliasPrefix("irisch", "353");
        aliasPrefix("irish", "353");
        aliasPrefix("indisch", "91");
        aliasPrefix("indian", "91");
        aliasPrefix("pakistanisch", "92");
        aliasPrefix("pakistani", "92");
        aliasPrefix("brasilianisch", "55");
        aliasPrefix("brazilian", "55");
        aliasPrefix("mexikanisch", "52");
        aliasPrefix("mexican", "52");
        aliasPrefix("kanadisch", "1");
        aliasPrefix("canadian", "1");
        aliasPrefix("australisch", "61");
        aliasPrefix("australian", "61");
        aliasPrefix("agyptisch", "20");
        aliasPrefix("egyptian", "20");
        aliasPrefix("marokkanisch", "212");
        aliasPrefix("moroccan", "212");
        aliasPrefix("israelisch", "972");
        aliasPrefix("israeli", "972");
        aliasPrefix("syrisch", "963");
        aliasPrefix("syrian", "963");
        aliasPrefix("iranisch", "98");
        aliasPrefix("iranian", "98");
        aliasPrefix("irakisch", "964");
        aliasPrefix("iraqi", "964");
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
     * Returns the country/region name (in the currently active UI language,
     * see {@link Lang#isGerman()}) for the given Rufnummer (without leading
     * {@code '+'}/{@code '00'}), matched longest-prefix-first, or
     * {@code null} if no known calling code matches.
     */
    public static String getCountryForNumber(String msisdn) {
        if (msisdn == null) return null;
        String digits = msisdn.trim();
        for (String prefix : PREFIXES_BY_LENGTH_DESC) {
            if (digits.startsWith(prefix)) return activeMap().get(prefix);
        }
        return null;
    }

    /** Returns the stored country/region name (current UI language) for an exact calling-code prefix, or {@code null}. */
    public static String getCountryNameByPrefix(String prefix) {
        return prefix == null ? null : activeMap().get(prefix.trim());
    }

    /**
     * Reverse look-up: resolves a (German or English, possibly colloquial)
     * country name mentioned in a natural-language request back to its
     * E.164 calling-code prefix, for building a {@code msisdn LIKE
     * 'prefix%'} filter.
     *
     * <p>Tries, in order: (1) the alias table, (2) an exact match against a
     * stored German or English country/region name, (3) a loose substring
     * match in either direction against either language (so e.g. "Kongo"
     * or "Congo" matches "Kongo (Republik)"/"Congo (Republic)"; if several
     * stored names contain the input, the longest stored name wins, since
     * that's the more specific match). Returns {@code null} if nothing
     * matches — the caller should treat that as "country not recognised"
     * rather than guessing.</p>
     */
    public static String findPrefixForCountryName(String countryName) {
        if (countryName == null || countryName.isBlank()) return null;
        String norm = normalize(countryName);

        if (ALIAS_TO_PREFIX.containsKey(norm)) return ALIAS_TO_PREFIX.get(norm);

        for (Map.Entry<String, String> e : CODE_TO_COUNTRY_DE.entrySet()) {
            if (normalize(e.getValue()).equals(norm)) return e.getKey();
        }
        for (Map.Entry<String, String> e : CODE_TO_COUNTRY_EN.entrySet()) {
            if (normalize(e.getValue()).equals(norm)) return e.getKey();
        }

        // Adjective/demonym stems in ALIAS_TO_PREFIX (e.g. "franzosisch",
        // "french") are stored without an inflection ending, so match them
        // as a prefix of the (normalized) input — this covers "französische",
        // "französischen", "Germany's" etc. all resolving via the
        // corresponding stem entry. Longest stem wins in case one stem is
        // itself a prefix of another.
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
        for (Map.Entry<String, String> e : CODE_TO_COUNTRY_DE.entrySet()) {
            String countryNorm = normalize(e.getValue());
            if (countryNorm.contains(norm) || norm.contains(countryNorm)) {
                if (countryNorm.length() > bestLen) {
                    bestLen = countryNorm.length();
                    bestPrefix = e.getKey();
                }
            }
        }
        for (Map.Entry<String, String> e : CODE_TO_COUNTRY_EN.entrySet()) {
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
     * country field) for any known country name/alias/adjective — in
     * either German or English — and returns its E.164 calling-code
     * prefix, or {@code null} if none is mentioned.
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
     * "Vereinigtes Königreich", "United Kingdom") are matched as a
     * substring of the whole normalized text. Single-word adjective/demonym
     * stems (e.g. "franzosisch"/"french") are matched per word-token,
     * requiring either an exact match or — for stems of 5+ characters, to
     * avoid short false positives like "uk"/"vae" matching inside unrelated
     * words — a prefix match, so inflected forms ("französischen",
     * "polnischer") still resolve.</p>
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
        for (Map.Entry<String, String> e : CODE_TO_COUNTRY_DE.entrySet()) {
            String countryNorm = normalize(e.getValue());
            if (norm.contains(countryNorm) && countryNorm.length() > bestLen) {
                bestLen = countryNorm.length();
                bestPrefix = e.getKey();
            }
        }
        for (Map.Entry<String, String> e : CODE_TO_COUNTRY_EN.entrySet()) {
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
     * Builds a SQL {@code CASE WHEN <column> LIKE 'xxx%' THEN '<name>' … END}
     * ladder covering every calling code in this table (names in the
     * currently active UI language, see {@link Lang#isGerman()}), longest
     * prefix first so e.g. {@code "212"} (Morocco) is tested before any
     * shorter prefix could shadow it. The caller supplies the {@code ELSE}
     * branch (e.g. a generic "Abroad (prefix …)" fallback) since that
     * varies by analysis.
     *
     * @param column     the SQL column/expression to match against (e.g. {@code "msisdn"})
     * @param elseBranch raw SQL expression used as the {@code ELSE} result (already quoted if a literal)
     */
    public static String buildSqlCaseExpression(String column, String elseBranch) {
        Map<String, String> names = activeMap();
        StringBuilder sb = new StringBuilder("CASE ");
        for (String prefix : PREFIXES_BY_LENGTH_DESC) {
            sb.append("WHEN ").append(column).append(" LIKE '").append(prefix).append("%' THEN '")
              .append(names.get(prefix).replace("'", "''")).append("' ");
        }
        sb.append("ELSE ").append(elseBranch).append(" END");
        return sb.toString();
    }
}
