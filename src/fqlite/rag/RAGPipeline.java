package fqlite.rag;

import de.kherud.llama.*;
import fqlite.erm.SchemaRetriever;
import fqlite.timemap.CountryCallingCodeLookup;
import fqlite.timemap.PoliceAnalysisQueries;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class contains RAG + SQL Generator + DB Integration
 *  author: @pawlaszc
 */
public class RAGPipeline {

    private SchemaRetriever schemaRetriever = null;
    private final SQLGenerator sqlGenerator;

    /**
     * Constructor for the pipeline.
     *
     * @param modelPath  we need the path to the .guff file
     * @throws Exception thrown if something goes wrong during initialization.
     */
    public RAGPipeline(String modelPath) {

        // make sure, that modell is loaded and ready
        this.sqlGenerator = new SQLGenerator(modelPath);

    }

        /**
         * Constructor for the pipeline.
         *
         * @param modelPath  we need the path to the .guff file
         * @param schema all CREATE TABLE statement from the schema definition as String
         * @throws Exception thrown if something goes wrong during initialization.
         */
    public RAGPipeline(String modelPath, String schema) {

        System.out.println("Initializing ForensicSQL with RAG...");

        String fullSchema = "";

        if (schema.isEmpty()) {

            // just for testing
            fullSchema = """
                    CREATE TABLE messages (
                        _id INTEGER PRIMARY KEY,
                        thread_id INTEGER,
                        address TEXT,
                        body TEXT,
                        date INTEGER,
                        read INTEGER,
                        FOREIGN KEY (thread_id) REFERENCES thread(_id)
                    );
                    
                    CREATE TABLE thread (
                        _id INTEGER PRIMARY KEY,
                        recipient_id INTEGER,
                        snippet TEXT
                    );
                    
                    CREATE TABLE contacts (
                        _id INTEGER PRIMARY KEY,
                        name TEXT,
                        phone TEXT
                    );
                    
                    CREATE TABLE call_log (
                        _id INTEGER PRIMARY KEY,
                        number TEXT,
                        duration INTEGER
                    );
                    """;
        }

        System.out.println("✅ Extracted schema: " + countTables(fullSchema) + " tables");

        // 3. Initialize RAG retriever
       // this.schemaRetriever = new RAGSchemaRetriever(fullSchema);

        // 4. Initialize SQL generator
        this.sqlGenerator = new SQLGenerator(modelPath);

        System.out.println("✅ ForensicSQL with RAG ready!");
    }

    public void initializeRetriever(Connection dbConnection) {
         this.schemaRetriever = new SchemaRetriever(dbConnection);
    }


    /**
     * Generate SQL for prompt
     */
    public String generateSQL(String request) {
        return generateSQL(request, 200);
    }

    /**
     * This method is used to combine schema and SQL statement
     * @param request the SQL statement
     * @param maxTables how many table should be involved
     * @return the result of the query
     */
    public String generateSQL(String request, int maxTables) {
        System.out.println("\n📝 Request: " + request);

        // Step 1: RAG - Retrieve relevant schema
        String relevantSchema = schemaRetriever.retrieveRelevantSchema(request, maxTables);

        // Step 2: Generate SQL
        String sql = sqlGenerator.generateSQL(relevantSchema, request);

        System.out.println("✅ Generated SQL: " + sql);

        return sql;
    }

    /**
     * Generate SQL and analyze validity.
     */
    public QueryResult analyzeRequest(String request) {

        try {
            String sql = generateSQL(request);

            if (!isValidSQL(sql)) {
                return new QueryResult(false, "Invalid SQL generated", null, sql);
            }
            else
                return null;

        } catch (Exception e) {
            return new QueryResult(false, "SQL Error: " + e.getMessage(), null, null);
        }
    }


    // ========================================================================
    // Co-Location queries: intent classification + fixed SQL template
    // ========================================================================

    /**
     * Classifies a natural-language request as either a "co-location" query
     * ("welche IMSIs waren gemeinsam in einer Funkzelle?") or a plain filter
     * query, and extracts the time window if one is mentioned.
     *
     * <p>Unlike {@link #generateSQL}, the model is NOT asked to invent the
     * (error-prone) self-join SQL itself — it only has to return a small
     * JSON object with intent + time window. The actual co-location SQL is
     * a fixed, reviewed template built by {@link #buildColocationSql}, so
     * the forensic result stays deterministic and auditable.</p>
     */
    public QueryIntent classifyIntent(String request) {
        return classifyIntent(request, null);
    }

    /**
     * Like {@link #classifyIntent(String)}, but passes a {@code dataDateContext}
     * string (e.g. {@code "2024-09-01 00:00:00 bis 2024-10-31 23:59:59"}) into
     * the prompt so the model uses the actual year of the dataset rather than
     * the current calendar year when the user omits the year in date expressions
     * like "02.07. zwischen 5 und 7 Uhr".
     *
     * @param dataDateContext  "MIN bis MAX" string from {@code response_records.start_time},
     *                         or {@code null} to fall back to the current-year assumption
     */
    public QueryIntent classifyIntent(String request, String dataDateContext) {
        String normalized = normalizeDateExpressions(request);
        // Only inject the data-date-range context when the request actually
        // contains a date expression (ISO date, dot-separated, European hyphen,
        // or German month name). For pure keyword queries like "Zeige mir alle
        // Wechsler an." the extra hint changes prompt length and can shift the
        // model's attention away from intent keywords — causing misclassification.
        boolean hasDate = DATE_HINT_TRIGGER.matcher(request).find();
        // Additional step: if the data date context is available and the request
        // contains a partial dot-separated date without a year (e.g. "02.07."),
        // inject the most likely year directly into the normalized text so the
        // LLM doesn't have to guess — small models reliably misread the year hint.
        if (hasDate && dataDateContext != null && !dataDateContext.isBlank()) {
            normalized = injectYearIntoPartialDates(normalized, dataDateContext);
        }
        String raw = sqlGenerator.classifyIntent(normalized, hasDate ? dataDateContext : null);
        QueryIntent qi = QueryIntent.parse(raw);
        qi.normalizedRequest = normalized;   // make date-normalized text available to callers
        return qi;
    }

    /**
     * Replaces partial dot-separated dates of the form {@code DD.MM.} or
     * {@code DD.MM} (no 4-digit year) with {@code YYYY-MM-DD} using the year
     * extracted from {@code dataDateContext} ("MIN bis MAX" string from
     * {@code response_records.start_time}).  This is called after
     * {@link #normalizeDateExpressions} so that German month-name dates
     * ({@code "2. Juli 2023"} → {@code "2023-07-02"}) are already handled;
     * only the remaining bare {@code DD.MM.} patterns need a year prefix.
     *
     * <p>Example: {@code "02.07. zwischen 5 und 7 Uhr"} with
     * {@code dataDateContext="2023-02-07 ... bis 2023-09-30 ..."} →
     * {@code "2023-07-02 zwischen 5 und 7 Uhr"}</p>
     *
     * <p>Ambiguous month/day order: if the first part &gt; 12 it must be the
     * day; otherwise German convention (first = day, second = month) is used,
     * matching the convention already applied by {@link #normalizeDateExpressions}
     * for hyphen-separated dates.</p>
     */
    static String injectYearIntoPartialDates(String text, String dataDateContext) {
        if (text == null || dataDateContext == null) return text;

        // Extract the most prominent year from the context string
        // (first 4-digit year found after "bis " or at the start).
        String year = null;
        java.util.regex.Matcher ym = Pattern.compile("\\b(\\d{4})-\\d{2}-\\d{2}").matcher(dataDateContext);
        if (ym.find()) year = ym.group(1);        // year of the MIN timestamp
        if (year == null) return text;

        // Match "DD.MM." or "DD.MM" that is NOT already preceded by a year
        // (i.e. not already part of "YYYY-MM-DD").  Negative look-behind
        // prevents matching dates that normalizeDateExpressions already fixed.
        Pattern partial = Pattern.compile(
                "(?<![\\d-])(\\d{1,2})\\.(\\d{1,2})\\.?(?!\\d)");
        java.util.regex.Matcher m = partial.matcher(text);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            int a = Integer.parseInt(m.group(1));
            int b = Integer.parseInt(m.group(2));
            int day, mon;
            if (a > 12 && b <= 12)      { day = a; mon = b; }
            else if (b > 12 && a <= 12) { day = b; mon = a; }
            else                         { day = a; mon = b; }  // German: DD.MM.
            if (mon < 1 || mon > 12 || day < 1 || day > 31) {
                m.appendReplacement(sb, java.util.regex.Matcher.quoteReplacement(m.group()));
                continue;
            }
            m.appendReplacement(sb, java.util.regex.Matcher.quoteReplacement(
                    String.format("%s-%02d-%02d", year, mon, day)));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /**
     * Pre-processes the user's natural-language request and replaces German
     * long-form date expressions like "2. Juli 2023" or "15. März" with the
     * numeric ISO-style equivalent ("02.07.2023" / "15.03.") <em>before</em>
     * the text reaches the LLM.  Small local models are unreliable at month-
     * name conversion; doing it deterministically in Java is far more robust.
     *
     * <p>Recognised patterns (case-insensitive):
     * <ul>
     *   <li>{@code <day>. <MonthName> <year>}  → {@code DD.MM.YYYY}</li>
     *   <li>{@code <day>. <MonthName>}          → {@code DD.MM.}  (year omitted)</li>
     * </ul>
     */
    static String normalizeDateExpressions(String text) {
        if (text == null) return null;
        // Map German month names (and common abbreviated forms) to zero-padded month numbers.
        String[][] months = {
            {"januar",    "01"}, {"february",  "02"}, {"februar",   "02"},
            {"märz",      "03"}, {"maerz",     "03"}, {"april",     "04"},
            {"mai",       "05"}, {"juni",      "06"}, {"juli",      "07"},
            {"august",    "08"}, {"september", "09"}, {"oktober",   "10"},
            {"november",  "11"}, {"dezember",  "12"},
        };
        // Pattern: optional whitespace, day (1-2 digits), dot, whitespace,
        // month name (captured by alternation below), optional comma/whitespace,
        // optional 4-digit year.
        // We build one combined pattern so we can replace in a single pass.
        StringBuilder monthAlt = new StringBuilder();
        for (String[] m : months) {
            if (monthAlt.length() > 0) monthAlt.append('|');
            monthAlt.append(Pattern.quote(m[0]));
        }
        // Build lookup map for fast replacement inside the replacer.
        java.util.Map<String, String> lookup = new java.util.LinkedHashMap<>();
        for (String[] m : months) lookup.put(m[0], m[1]);

        Pattern p = Pattern.compile(
                "(\\d{1,2})\\.\\s*(" + monthAlt + ")[.,]?\\s*(\\d{4})?",
                Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);

        Matcher m = p.matcher(text);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String day   = String.format("%02d", Integer.parseInt(m.group(1)));
            String mon   = lookup.get(m.group(2).toLowerCase());
            if (mon == null) { m.appendReplacement(sb, Matcher.quoteReplacement(m.group())); continue; }
            String year  = m.group(3);
            // With year: use unambiguous ISO format (YYYY-MM-DD) so the LLM
            // cannot confuse day and month.  Without year: keep the German
            // DD.MM. dot format so that injectYearIntoPartialDates() can
            // complete it via the data date context rather than relying on
            // the LLM to infer the year from the hint (less reliable with
            // small models). "MM-DD" (hyphen without year) was previously
            // used here but is NOT picked up by injectYearIntoPartialDates.
            String replacement = (year != null)
                    ? year + "-" + mon + "-" + day          // → YYYY-MM-DD  (ISO, unambiguous)
                    : day  + "." + mon + ".";               // → DD.MM.  (injectYearIntoPartialDates handles this)
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        String result = sb.toString();

        // ── Step 2: European hyphen-separated dates (DD-MM-YYYY or MM-DD-YYYY) ──
        // Handles inputs like "02-07-2023", "07-02-2023" that the LLM has no
        // examples for.  The pattern only matches when the YEAR is the LAST
        // 4-digit group (distinguishes from ISO "YYYY-MM-DD" where year is first).
        // Disambiguation rule: if one part > 12 it must be the day; otherwise
        // use German convention (first number = day, second = month).
        // Negative look-around prevents matching inside an already-correct ISO date.
        Pattern euroHyphen = Pattern.compile(
                "(?<![\\d-])(\\d{1,2})-(\\d{1,2})-(\\d{4})(?![\\d-])");
        Matcher m2 = euroHyphen.matcher(result);
        StringBuffer sb2 = new StringBuffer();
        while (m2.find()) {
            int a    = Integer.parseInt(m2.group(1));
            int b    = Integer.parseInt(m2.group(2));
            String y = m2.group(3);
            int day2, mon2;
            if (a > 12 && b <= 12)      { day2 = a; mon2 = b; }   // a can only be day
            else if (b > 12 && a <= 12) { day2 = b; mon2 = a; }   // b can only be day
            else                         { day2 = a; mon2 = b; }   // both ≤ 12 → German convention: DD-MM
            if (mon2 < 1 || mon2 > 12 || day2 < 1 || day2 > 31) {
                m2.appendReplacement(sb2, Matcher.quoteReplacement(m2.group()));
                continue;
            }
            m2.appendReplacement(sb2, Matcher.quoteReplacement(
                    String.format("%s-%02d-%02d", y, mon2, day2)));
        }
        m2.appendTail(sb2);
        String result2 = sb2.toString();

        // ── Step 3: German dot-separated full dates (DD.MM.YYYY) ──
        // The most common German date format — e.g. "02.07.2023", "2.7.2023".
        // Steps 1 and 2 leave these untouched (step 1 needs a month name, step 2
        // needs hyphens), so we handle them here. Applied after steps 1/2 so
        // there is no interference with already-converted ISO results.
        // German convention: first number = day, second = month; if one part > 12
        // it must be the day regardless of position.
        Pattern dotFull = Pattern.compile(
                "(?<![\\d-])(\\d{1,2})\\.(\\d{1,2})\\.(\\d{4})(?![\\d.])");
        Matcher m3 = dotFull.matcher(result2);
        StringBuffer sb3 = new StringBuffer();
        while (m3.find()) {
            int a    = Integer.parseInt(m3.group(1));
            int b    = Integer.parseInt(m3.group(2));
            String y = m3.group(3);
            int day3, mon3;
            if (a > 12 && b <= 12)      { day3 = a; mon3 = b; }
            else if (b > 12 && a <= 12) { day3 = b; mon3 = a; }
            else                         { day3 = a; mon3 = b; }  // German: DD.MM.YYYY
            if (mon3 < 1 || mon3 > 12 || day3 < 1 || day3 > 31) {
                m3.appendReplacement(sb3, Matcher.quoteReplacement(m3.group()));
                continue;
            }
            m3.appendReplacement(sb3, Matcher.quoteReplacement(
                    String.format("%s-%02d-%02d", y, mon3, day3)));
        }
        m3.appendTail(sb3);
        return sb3.toString();
    }

    /**
     * Detects whether a user request string contains any date expression that
     * would benefit from a data-date-range hint in the LLM prompt.  Only
     * inject the hint when there really is a date, so non-date queries like
     * "Zeige mir alle Wechsler an." keep exactly the same prompt length as
     * before — a longer prompt can shift the model's attention and cause it
     * to misclassify keyword-based intents.
     *
     * <p>Recognised patterns:</p>
     * <ul>
     *   <li>German month names: Januar, Februar, …, Dezember</li>
     *   <li>Dot-separated dates: {@code 02.07.} / {@code 02.07.2023}</li>
     *   <li>ISO dates: {@code 2023-07-02}</li>
     *   <li>European hyphen dates: {@code 02-07-2023}</li>
     * </ul>
     */
    private static final Pattern DATE_HINT_TRIGGER = Pattern.compile(
            "\\b\\d{1,2}[./]\\d{1,2}([./]\\d{2,4})?\\b"          // 02.07. / 02.07.2023
            + "|\\b\\d{1,2}-\\d{1,2}-\\d{4}\\b"                   // 02-07-2023 (European)
            + "|\\b\\d{4}-\\d{2}-\\d{2}\\b"                       // 2023-07-02 (ISO)
            + "|\\b(januar|j[aä]nner|februar|m[aä]rz|april|mai"
            + "|juni|juli|august|september|oktober|november|dezember)\\b",
            Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);

    private static final Pattern TIMESTAMP_PATTERN =
            Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");

    private static boolean isValidTimestamp(String s) {
        return s != null && TIMESTAMP_PATTERN.matcher(s).matches();
    }

    /**
     * Builds the fixed, hand-reviewed SQL for a co-location query: finds
     * pairs of distinct IMSIs whose records fall within
     * [{@code startTime}, {@code endTime}] and share the exact same
     * reported cell-site coordinate (latitude_dec/longitude_dec are rounded
     * deterministically on import, so equality is sufficient — no distance
     * threshold needed).
     *
     * <p>{@link fqlite.sql.InMemoryDatabase#execute(String)} runs the
     * statement via {@code prepareStatement(sql).executeQuery()} without
     * binding any {@code ?} parameters, so the time window is embedded as a
     * literal here rather than bound afterwards.</p>
     *
     * @param startTime "yyyy-MM-dd HH:mm:ss"
     * @param endTime   "yyyy-MM-dd HH:mm:ss"
     * @return the SELECT statement, or {@code null} if either timestamp
     *         doesn't match the expected format
     */
    /**
     * Columns that {@code identifierColumn} is allowed to resolve to. This
     * is a hand-picked whitelist, not a trust boundary around user input
     * directly — but {@code identifierColumn} ultimately originates from the
     * LLM's JSON answer (see {@link QueryIntent}), so it's validated before
     * being embedded in the SQL string rather than assumed safe.
     */
    private static final Set<String> ALLOWED_IDENTIFIER_COLUMNS =
            Set.of("imsi", "msisdn", "na_device_id");

    private static String safeIdentifierColumn(String identifierColumn) {
        return ALLOWED_IDENTIFIER_COLUMNS.contains(identifierColumn) ? identifierColumn : "imsi";
    }

    /**
     * Boolean SQL expression deciding whether two {@code response_records}
     * rows ({@code left}/{@code right} table refs, e.g. {@code "a"}/{@code "b"}
     * or {@code "o"}/{@code "response_records"}) represent the same physical
     * radio cell.
     *
     * <p>Prefers the structured cell identity decoded at import time from
     * {@code user_location_info} ({@code cell_mcc}/{@code cell_mnc}/
     * {@code cell_lac_tac}/{@code cell_ci} — see {@link fqlite.timemap.UliDecoder}
     * and {@code EtsiXmlImporter#writeRow}): two records match only if all
     * four fields are present on both sides and equal. This is strictly more
     * precise than comparing {@code latitude_dec}/{@code longitude_dec} —
     * multi-sector sites can report the very same rounded coordinate for
     * several distinct cells, which would otherwise produce false
     * co-location matches.</p>
     *
     * <p>Falls back to the coordinate comparison only when cell identity is
     * missing on either side (older databases imported before the cell_*
     * columns existed, or a ULI that {@code UliDecoder} couldn't decode), so
     * existing databases keep working.</p>
     */
    private static String sameCellExpr(String left, String right) {
        return String.format(
                "(("
                + "%1$s.cell_mcc IS NOT NULL AND %2$s.cell_mcc IS NOT NULL "
                + "AND %1$s.cell_mcc = %2$s.cell_mcc "
                + "AND %1$s.cell_mnc = %2$s.cell_mnc "
                + "AND %1$s.cell_lac_tac = %2$s.cell_lac_tac "
                + "AND %1$s.cell_ci = %2$s.cell_ci"
                + ") OR ("
                + "(%1$s.cell_mcc IS NULL OR %2$s.cell_mcc IS NULL) "
                + "AND %1$s.latitude_dec = %2$s.latitude_dec "
                + "AND %1$s.longitude_dec = %2$s.longitude_dec"
                + "))",
                left, right);
    }

    /** {@link #buildColocationSql(String, String, String)} defaulting to IMSI. */
    public static String buildColocationSql(String startTime, String endTime) {
        return buildColocationSql(startTime, endTime, "imsi");
    }

    /**
     * @param identifierColumn which subscriber identifier to group by:
     *                         {@code "imsi"}, {@code "msisdn"}, or
     *                         {@code "na_device_id"} (the IMEI-equivalent
     *                         field in {@code response_records}). Falls back
     *                         to {@code "imsi"} for any other value.
     */
    public static String buildColocationSql(String startTime, String endTime, String identifierColumn) {
        if (!isValidTimestamp(startTime) || !isValidTimestamp(endTime)) return null;
        String col = safeIdentifierColumn(identifierColumn);
        String sameCell = sameCellExpr("a", "b");

        return String.format(
                """
                SELECT a.%3$s AS id_a, b.%3$s AS id_b,
                       a.latitude_dec AS lat, a.longitude_dec AS lon,
                       a.cell_mcc AS mcc, a.cell_mnc AS mnc, a.cell_lac_tac AS lac_tac, a.cell_ci AS ci,
                       a.start_time AS start_a, b.start_time AS start_b
                FROM response_records a
                JOIN response_records b ON a.id < b.id
                WHERE a.%3$s IS NOT NULL AND b.%3$s IS NOT NULL
                  AND a.%3$s <> b.%3$s
                  AND a.latitude_dec IS NOT NULL AND a.longitude_dec IS NOT NULL
                  AND %4$s
                  AND a.start_time BETWEEN '%1$s' AND '%2$s'
                  AND b.start_time BETWEEN '%1$s' AND '%2$s'
                ORDER BY a.latitude_dec, a.longitude_dec, a.start_time
                """,
                startTime, endTime, col, sameCell);
    }

    /**
     * Builds the fixed SQL that returns the individual {@code response_records}
     * rows backing a co-location match (every record that has at least one
     * other IMSI at the exact same site within the same time window).
     *
     * <p>Unlike {@link #buildColocationSql}, this returns plain
     * {@code response_records}-shaped rows (one row per record, not a pair),
     * so the result can be fed straight into the existing
     * {@code setData(...)} pipeline to draw the matched records on the map
     * and timeline — the same way a plain filter query's result is drawn.</p>
     *
     * @param startTime "yyyy-MM-dd HH:mm:ss"
     * @param endTime   "yyyy-MM-dd HH:mm:ss"
     * @return the SELECT statement, or {@code null} if either timestamp
     *         doesn't match the expected format
     */
    /** {@link #buildColocationDetailSql(String, String, String)} defaulting to IMSI. */
    public static String buildColocationDetailSql(String startTime, String endTime) {
        return buildColocationDetailSql(startTime, endTime, "imsi");
    }

    /**
     * @param identifierColumn see {@link #buildColocationSql(String, String, String)}
     */
    public static String buildColocationDetailSql(String startTime, String endTime, String identifierColumn) {
        if (!isValidTimestamp(startTime) || !isValidTimestamp(endTime)) return null;
        String col = safeIdentifierColumn(identifierColumn);
        String sameCell = sameCellExpr("o", "response_records");

        // NOTE: deliberately not aliasing response_records in the outer
        // query (no "SELECT r.*" / "FROM response_records r"). Aliasing the
        // table being SELECT *'d can make the JDBC driver report
        // alias-qualified column names (e.g. "r.start_time") in
        // ResultSetMetaData, which breaks DataAnalyzer.isResponseRecordsSchema's
        // exact-name match against "start_time"/"latitude_dec"/"longitude_dec"
        // and silently drops the table into the generic heuristic fallback
        // (timestamps still resolve there, but coordinates don't reliably).
        // The correlated subquery aliases only its own inner reference ("o")
        // and refers back to the outer, un-aliased table by its real name.
        return String.format(
                """
                SELECT *
                FROM response_records
                WHERE %3$s IS NOT NULL
                  AND latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL
                  AND start_time BETWEEN '%1$s' AND '%2$s'
                  AND EXISTS (
                      SELECT 1 FROM response_records o
                      WHERE o.id <> response_records.id
                        AND o.%3$s IS NOT NULL
                        AND o.%3$s <> response_records.%3$s
                        AND %4$s
                        AND o.start_time BETWEEN '%1$s' AND '%2$s'
                  )
                ORDER BY latitude_dec, longitude_dec, start_time
                """,
                startTime, endTime, col, sameCell);
    }

    // ========================================================================
    // Calls/SMS direction queries: intent classification + fixed SQL template
    // ========================================================================

    /**
     * Maps the four directions the model can extract to the
     * {@code call_indicator} value writes for each
     * (see project background: MOC/MTC for calls, MOM/MTM for SMS, from the
     * monitored target's perspective — MO* = target-initiated/outgoing,
     * MT* = target-received/incoming).
     */
    private static final Map<String, List<String>> CALL_SMS_INDICATOR = Map.of(
            "ausgehender_anruf", List.of("MOC"),
            "eingehender_anruf", List.of("MTC"),
            "gesendete_sms", List.of("MOM"),
            "empfangene_sms", List.of("MTM"),
            // No direction given ("alle Anrufe" / "alle SMS") — covers both legs.
            "alle_anrufe", List.of("MOC", "MTC"),
            "alle_sms", List.of("MOM", "MTM"));

    /**
     * Builds the fixed, hand-reviewed SQL for an incoming/outgoing-call or
     * sent/received-SMS query. Like co-location, this is never generated
     * freely by the model — {@code call_indicator} values (MOC/MTC/MOM/MTM)
     * are internal abbreviations the small local model has no way to know,
     * so the mapping lives in {@link #CALL_SMS_INDICATOR}, reviewed once,
     * rather than being invented per-request.
     *
     * <p>Each call/SMS event is stored as two {@code response_records} rows
     * (one per party leg, see {@code EtsiXmlImporter.bindPartyLegRow}).
     * Only the leg belonging to the network's own monitored subscriber
     * carries {@code latitude_dec}/{@code longitude_dec}; the remote
     * counterpart's leg only has a bare {@code msisdn} and no location. This
     * filters to the located leg, so every returned row can be drawn as a
     * marker/timeline dot via the existing {@code setData} pipeline — the
     * remote party's stub row (no coordinates, nothing to plot) is dropped.</p>
     *
     * @param callSmsType one of {@code "eingehender_anruf"},
     *                    {@code "ausgehender_anruf"}, {@code "gesendete_sms"},
     *                    {@code "empfangene_sms"}
     * @param startTime   "yyyy-MM-dd HH:mm:ss", or {@code null}/invalid to
     *                    leave the time window unrestricted
     * @param endTime     "yyyy-MM-dd HH:mm:ss", or {@code null}/invalid to
     *                    leave the time window unrestricted
     * @return the SELECT statement, or {@code null} if {@code callSmsType}
     *         isn't one of the four recognised values
     */
    public static String buildCallsSmsSql(String callSmsType, String startTime, String endTime) {
        List<String> indicators = callSmsType == null ? null : CALL_SMS_INDICATOR.get(callSmsType.trim().toLowerCase());
        if (indicators == null || indicators.isEmpty()) return null;

        String indicatorClause = indicators.size() == 1
                ? String.format("call_indicator = '%s'", indicators.get(0))
                : "call_indicator IN (" + String.join(", ", indicators.stream().map(i -> "'" + i + "'").toList()) + ")";

        String timeClause = "";
        if (isValidTimestamp(startTime) && isValidTimestamp(endTime)) {
            timeClause = String.format(" AND start_time BETWEEN '%s' AND '%s'", startTime, endTime);
        }

        return String.format(
                """
                SELECT *
                FROM response_records
                WHERE %s
                  AND latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL%s
                ORDER BY start_time
                """,
                indicatorClause, timeClause);
    }

    // ========================================================================
    // Country-filter queries: intent classification + fixed SQL template
    // ========================================================================

    /**
     * Builds the fixed SQL for "alle Rufnummern aus Land X" queries. The
     * model never builds this SQL itself — it only extracts the country
     * name as written; resolving that to an E.164 calling-code prefix is
     * done deterministically by {@link CountryCallingCodeLookup}, and the
     * prefix is then embedded as a {@code msisdn LIKE 'prefix%'} filter.
     *
     * <p>Like {@link #buildCallsSmsSql}, this requires
     * {@code latitude_dec}/{@code longitude_dec} to be present so every
     * returned row can be drawn as a marker via {@code setData} — rows for
     * a remote party's bare {@code msisdn} stub (no location) are dropped.</p>
     *
     * @param country   country name as mentioned in the request (German,
     *                  colloquial names allowed — see
     *                  {@link CountryCallingCodeLookup#findPrefixForCountryName})
     * @param startTime "yyyy-MM-dd HH:mm:ss", or {@code null}/invalid to
     *                  leave the time window unrestricted
     * @param endTime   "yyyy-MM-dd HH:mm:ss", or {@code null}/invalid to
     *                  leave the time window unrestricted
     * @return the SELECT statement, or {@code null} if the country name
     *         couldn't be resolved to a known calling code
     */
    public static String buildCountryFilterSql(String country, String startTime, String endTime) {
        String prefix = CountryCallingCodeLookup.findPrefixForCountryName(country);
        if (prefix == null) return null;
        // Defensive: prefix only ever comes from the lookup table above (digits
        // only), but strip anything non-numeric before embedding it anyway.
        prefix = prefix.replaceAll("[^0-9]", "");
        if (prefix.isEmpty()) return null;

        String timeClause = "";
        if (isValidTimestamp(startTime) && isValidTimestamp(endTime)) {
            timeClause = String.format(" AND start_time BETWEEN '%s' AND '%s'", startTime, endTime);
        }

        return String.format(
                """
                SELECT *
                FROM response_records
                WHERE msisdn LIKE '%s%%'
                  AND latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL%s
                ORDER BY start_time
                """,
                prefix, timeClause);
    }

    // ========================================================================
    // Roamers ("Wanderer") queries: intent classification + fixed SQL template
    // ========================================================================

    /** {@link #buildRoamersSql(String, String, String)} defaulting to IMSI. */
    public static String buildRoamersSql(String startTime, String endTime) {
        return buildRoamersSql(startTime, endTime, "imsi");
    }

    /**
     * Builds the fixed SQL for "alle Wanderer" queries (subscribers seen at
     * more than one distinct cell-site coordinate, optionally within a time
     * window) — the LLM-driven counterpart of
     * {@link PoliceAnalysisQueries}, but returning
     * plain {@code response_records} rows (one row per record, not the
     * aggregated per-subscriber summary that analysis produces) so the
     * result can be fed straight into the existing {@code setData(...)}
     * pipeline and drawn as points on the map/timeline, the same way a
     * plain filter or calls/SMS query's result is drawn.
     *
     * <p>Like {@link #buildColocationDetailSql}, the outer table is
     * deliberately left un-aliased (no {@code "FROM response_records r"})
     * so {@code SELECT *}'s {@code ResultSetMetaData} still reports bare
     * column names — aliasing it would break
     * {@code DataAnalyzer.isResponseRecordsSchema}'s exact-name match.</p>
     *
     * @param identifierColumn which subscriber identifier to group by — see
     *                         {@link #buildColocationSql(String, String, String)}
     * @param startTime        "yyyy-MM-dd HH:mm:ss", or {@code null}/invalid
     *                          to leave the time window unrestricted
     * @param endTime           same format, or {@code null}/invalid to leave
     *                          the time window unrestricted
     * @return the SELECT statement (never {@code null} — unlike co-location,
     *         a time window isn't required for this query)
     */
    public static String buildRoamersSql(String startTime, String endTime, String identifierColumn) {
        String col = safeIdentifierColumn(identifierColumn);

        String timeClause = "";
        if (isValidTimestamp(startTime) && isValidTimestamp(endTime)) {
            timeClause = String.format(" AND start_time BETWEEN '%s' AND '%s'", startTime, endTime);
        }

        return String.format(
                """
                SELECT *
                FROM response_records
                WHERE %1$s IS NOT NULL AND %1$s <> ''
                  AND latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL%2$s
                  AND %1$s IN (
                      SELECT %1$s FROM response_records
                      WHERE %1$s IS NOT NULL AND %1$s <> ''
                        AND latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL%2$s
                      GROUP BY %1$s
                      HAVING COUNT(DISTINCT latitude_dec || ',' || longitude_dec) > 1
                  )
                ORDER BY %1$s, start_time
                """,
                col, timeClause);
    }

    // ========================================================================
    // Wechsler (device/SIM swap) queries: intent classification + fixed SQL
    // ========================================================================

    /**
     * Builds the fixed SQL for "alle Wechsler" queries — subscribers seen
     * with more than one device (Geräte-Wechsel, same IMSI/multiple
     * na_device_id) as well as devices seen with more than one subscriber
     * (SIM-Wechsel, same na_device_id/multiple IMSI), optionally within a
     * time window. This is the LLM-driven, map-point counterpart
     * PoliceAnalysisQueries.AnalysisType#DEVICE_SIM_SWAP: that
     * analysis produces an aggregated per-subscriber/per-device summary,
     * whereas this returns plain {@code response_records} rows (one row per
     * record) so the result can be fed straight into the existing
     * {@code setData(...)} pipeline and drawn as points on the map/timeline.
     *
     * <p>Like {@link #buildRoamersSql}, the outer table is deliberately left
     * un-aliased (no {@code "FROM response_records r"}) so {@code SELECT *}'s
     * {@code ResultSetMetaData} still reports bare column names — aliasing it
     * would break {@code DataAnalyzer.isResponseRecordsSchema}'s exact-name
     * match.</p>
     *
     * @param startTime "yyyy-MM-dd HH:mm:ss", or {@code null}/invalid to
     *                  leave the time window unrestricted
     * @param endTime   same format, or {@code null}/invalid to leave the
     *                  time window unrestricted
     * @return the SELECT statement (never {@code null} — a time window
     *         isn't required for this query)
     */
    public static String buildWechslerSql(String startTime, String endTime) {
        String timeClause = "";
        if (isValidTimestamp(startTime) && isValidTimestamp(endTime)) {
            timeClause = String.format(" AND start_time BETWEEN '%s' AND '%s'", startTime, endTime);
        }

        return String.format(
                """
                SELECT *
                FROM response_records
                WHERE latitude_dec IS NOT NULL AND longitude_dec IS NOT NULL%1$s
                  AND (
                      imsi IN (
                          SELECT imsi FROM response_records
                          WHERE imsi IS NOT NULL AND imsi <> ''
                            AND na_device_id IS NOT NULL AND na_device_id <> ''%1$s
                          GROUP BY imsi
                          HAVING COUNT(DISTINCT na_device_id) > 1
                      )
                      OR na_device_id IN (
                          SELECT na_device_id FROM response_records
                          WHERE na_device_id IS NOT NULL AND na_device_id <> ''
                            AND imsi IS NOT NULL AND imsi <> ''%1$s
                          GROUP BY na_device_id
                          HAVING COUNT(DISTINCT imsi) > 1
                      )
                  )
                ORDER BY imsi, na_device_id, start_time
                """,
                timeClause);
    }

    /** Result of {@link #classifyIntent}. */
    public static final class QueryIntent {
        public final String intent;            // "colocation" | "calls_sms" | "country_filter" | "roamers" | "wechsler" | "filter"
        public final String startTime;         // "yyyy-MM-dd HH:mm:ss" or null
        public final String endTime;           // "yyyy-MM-dd HH:mm:ss" or null
        public final String identifierColumn;  // "imsi" | "msisdn" | "na_device_id"
        public final String identifierLabel;   // "IMSI" | "MSISDN" | "IMEI" — for UI display
        public final String callSmsType;       // "eingehender_anruf" | "ausgehender_anruf" | "gesendete_sms" | "empfangene_sms" | null
        public final String country;           // country name as extracted by the model, or null
        /**
         * The date-normalized form of the original user request: German month
         * names and dot/hyphen date formats are converted to ISO 8601, and
         * years missing from partial dates are injected from the DB date range.
         * Callers that pass text on to a secondary LLM step (e.g. the SQL
         * generator for {@code intent="filter"}) should use this field instead
         * of the raw request so the downstream model sees unambiguous ISO dates
         * and doesn't have to guess partial-date years.
         */
        public String normalizedRequest;       // set by RAGPipeline.classifyIntent after normalization

        private QueryIntent(String intent, String startTime, String endTime,
                             String identifierColumn, String identifierLabel, String callSmsType,
                             String country) {
            this.intent = intent;
            this.startTime = startTime;
            this.endTime = endTime;
            this.identifierColumn = identifierColumn;
            this.identifierLabel = identifierLabel;
            this.callSmsType = callSmsType;
            this.country = country;
        }

        public boolean isColocation() {
            return "colocation".equalsIgnoreCase(intent) && startTime != null && endTime != null;
        }

        /**
         * True when the model recognised a co-location request but couldn't
         * pin down a clean time window. The caller should ask the user to
         * clarify the time range rather than silently falling back to the
         * free-form SQL generator, which can't reliably express the
         * self-join a co-location query needs and tends to answer such
         * requests with degenerate SQL.
         */
        public boolean isAmbiguousColocation() {
            return "colocation".equalsIgnoreCase(intent) && (startTime == null || endTime == null);
        }

        /**
         * True when the model recognised a request about incoming/outgoing
         * calls or sent/received SMS AND was able to pin down which of the
         * four directions is meant. The fixed SQL template
         * ({@link #buildCallsSmsSql}) keys off {@code call_indicator}
         * (MOC/MTC/MOM/MTM) — a value the small local model has no reliable
         * way to invent on its own — so, like co-location, this is never
         * left to the free-form SQL generator.
         */
        public boolean isCallsOrSms() {
            return "calls_sms".equalsIgnoreCase(intent) && callSmsType != null;
        }

        /**
         * True when the model recognised a "Rufnummern aus Land X" request
         * AND a country name was extracted. The actual prefix resolution
         * (and rejection of unrecognised country names) happens in
         * {@link #buildCountryFilterSql}/{@link CountryCallingCodeLookup} —
         * this only gates whether that fixed-template path is attempted at
         * all, instead of falling back to the free-form SQL generator
         * (which has no notion of E.164 calling codes).
         */
        public boolean isCountryFilter() {
            return "country_filter".equalsIgnoreCase(intent) && country != null && !country.isBlank();
        }

        /**
         * True when the model recognised an "alle Wanderer" request
         * (subscribers seen at more than one cell site). Unlike co-location,
         * no time window or extra extracted field is required — the fixed
         * template ({@link #buildRoamersSql}) works with or without one —
         * so this only gates on the intent value itself.
         */
        public boolean isRoamers() {
            return "roamers".equalsIgnoreCase(intent);
        }

        /**
         * True when the model recognised an "alle Wechsler" request
         * (subscribers/devices linked to more than one device/subscriber —
         * Geräte- oder SIM-Wechsel). Like roamers, no time window or extra
         * extracted field is required — the fixed template
         * ({@link #buildWechslerSql}) works with or without one — so this
         * only gates on the intent value itself.
         */
        public boolean isWechsler() {
            return "wechsler".equalsIgnoreCase(intent);
        }

        /**
         * Tolerantly parses the model's JSON answer. Falls back to
         * {@code intent="filter"} if the model didn't return recognizable
         * JSON, so the existing filter pipeline handles the request as
         * before — the co-location/calls_sms/country_filter paths only ever
         * trigger when the intent and the data needed to build their fixed
         * SQL template were both extracted.
         */
        static QueryIntent parse(String raw) {
            if (raw == null) return new QueryIntent("filter", null, null, "imsi", "IMSI", null, null);

            Matcher m = Pattern.compile("\\{.*}", Pattern.DOTALL).matcher(raw);
            String json = m.find() ? m.group() : raw;

            String intent = extractField(json, "intent");
            String start = extractField(json, "start_time");
            String end = extractField(json, "end_time");
            String identifier = extractField(json, "identifier");
            String callSmsType = extractField(json, "call_sms_type");
            String country = extractField(json, "country");

            if (intent == null) intent = "filter";
            if (callSmsType != null) {
                callSmsType = callSmsType.trim().toLowerCase();
                if (!CALL_SMS_INDICATOR.containsKey(callSmsType)) callSmsType = null;
            }
            if (country != null && country.isBlank()) country = null;

            String identifierColumn;
            String identifierLabel;
            String normalizedIdentifier = identifier == null ? "" : identifier.trim().toLowerCase();
            switch (normalizedIdentifier) {
                case "msisdn" -> { identifierColumn = "msisdn"; identifierLabel = "MSISDN"; }
                case "imei"   -> { identifierColumn = "na_device_id"; identifierLabel = "IMEI"; }
                default       -> { identifierColumn = "imsi"; identifierLabel = "IMSI"; }
            }

            return new QueryIntent(intent.trim().toLowerCase(), start, end, identifierColumn, identifierLabel,
                    callSmsType, country);
        }

        private static String extractField(String json, String key) {
            // Accept BOTH quoted string values AND the literal null so that the
            // first occurrence of the key (always the top-level schema field)
            // is returned even when its value is null.  Without this, the regex
            // skips "key": null (not quoted) and accidentally picks up the same
            // key name buried inside LLM-hallucinated extra arrays/objects that
            // happen to contain a quoted value — causing e.g. a null start_time
            // to be replaced with a fabricated timestamp.
            Matcher m = Pattern.compile(
                    "\"" + key + "\"\\s*:\\s*(?:\"([^\"]*)\"|null)")
                    .matcher(json);
            if (m.find()) return m.group(1); // null when literal null, String otherwise
            return null;
        }
    }

    private int countTables(String schema) {
        return (int) Arrays.stream(schema.split(";"))
                .filter(s -> s.trim().toUpperCase().startsWith("CREATE TABLE"))
                .count();
    }

    /**
     * Sanity check applied to model-generated SQL before it is ever executed
     * against {@link fqlite.sql.InMemoryDatabase}. The local SQL-generation
     * model is small and occasionally answers with degenerate output (e.g.
     * a bare {@code "SELECT;"}) for requests it can't translate — this is
     * a fast rejection so the caller can show a clear error instead of
     * silently running garbage SQL.
     */
    public boolean isValidSQL(String sql) {
        if (sql == null || sql.isEmpty()) return false;
        String upper = sql.toUpperCase().trim();
        return upper.startsWith("SELECT") && upper.contains("FROM");
    }




    // ========================================================================
    // INNER CLASS: SQLGenerator (using llama.cpp)
    // ========================================================================

    static class SQLGenerator {
        private static LlamaModel model;

        SQLGenerator(String modelPath) {
            if (null == model) {
                // llama.cpp defaults --threads to -1 ("use all logical cores") and
                // by default also runs a warm-up inference pass while the model
                // is being constructed (see java-llama.cpp's ModelParameters#skipWarmup).
                // Both together can peg every CPU core for the whole duration of
                // model loading. Even though this load already runs on a background
                // Thread (never the FX Application Thread directly), saturating all
                // cores still starves the FX thread of CPU time, so it can't pump
                // its event loop quickly enough — which is exactly what the OS
                // reports as "Application Not Responding" (hourglass), even though
                // no thread is actually deadlocked.
                //
                // Reserve a couple of cores for the FX thread / GC / everything else
                // and skip the warm-up pass, which isn't needed before the first
                // real generateSQL() call anyway.
                int reservedThreads = Math.max(1, Runtime.getRuntime().availableProcessors() - 2);
                // 4096-token context: the classifyIntent prompt is ~3350 tokens
                // (instructions + 20 examples + request) — at 2048 the model
                // only sees the last ~1300 tokens, cutting off all intent
                // definitions and early examples.  4096 fits the full prompt
                // and eliminates intent misclassifications caused by truncation.
                ModelParameters params = new ModelParameters().
                        setGpuLayers(0).setCtxSize(4096).setModel(modelPath).
                        setThreads(reservedThreads).skipWarmup();
                model = new LlamaModel(params);
                System.out.println("✅ Modell has been loaded");
            }
            else
                System.out.println("✅ Modell is already loaded. ");

        }

        String generateSQL(String schema, String request) {
            String prompt = String.format(
                    """
                            Generate a valid SQLite query for this forensic database request.

                            IMPORTANT RULES:
                            - The time column is called "start_time" (never "timestamp", "time", "date", or "end_time").
                            - Use only column names that appear in the schema below. Do NOT invent column names.
                            - For date/time filtering always use: start_time BETWEEN 'YYYY-MM-DD HH:MM:SS' AND 'YYYY-MM-DD HH:MM:SS'

                            Database Schema:
                            %s

                            Request: %s

                            SQLite Query:
                            """,
                    schema, request
            );

            InferenceParameters inferParams = new InferenceParameters(prompt)
                    .setTemperature(0.0f)
                    .setTopP(0.9f)
                    .setTopK(40)
                    .setPresencePenalty(1.1f)
                    .setNPredict(1024)
                    .setStopStrings("\n", ";");

            LlamaIterable outputs = model.generate(inferParams);
            StringBuilder sql = new StringBuilder();
            for (LlamaOutput output : outputs) {
                sql.append(output.text);
            }

            return cleanSQL(sql.toString());
        }

        private String cleanSQL(String sql) {
            sql = sql.trim().replace("```sql", "").replace("```", "");
            sql = sql.split("\n")[0].trim();
            if (!sql.endsWith(";")) sql += ";";
            return sql;
        }

        /**
         * Asks the model to classify the request as "colocation" or
         * "filter" and to extract a time window, returning the raw model
         * output (parsed by {@link QueryIntent#parse}).
         */
        String classifyIntent(String request) {
            return classifyIntent(request, null);
        }

        /**
         * Like {@link #classifyIntent(String)}, but injects {@code dataDateContext}
         * (e.g. "2024-09-01 00:00:00 bis 2024-09-30 23:59:59") into the prompt
         * so the model uses the dataset's actual year rather than the current
         * calendar year when dates without a year are mentioned.
         */
        String classifyIntent(String request, String dataDateContext) {
            // Build the date-hint line that is embedded in the prompt.
            // When the caller supplies the actual data date range (i.e. the
            // request contains a date expression), we tell the model to derive
            // missing years from that range rather than from the current
            // calendar year. For requests without any date expression the hint
            // is intentionally empty so the prompt length and content stay
            // identical to the pre-Task-55 baseline — any extra text here
            // shifts the model's attention and can cause it to invent a year
            // filter even for queries like "Zeige mir alle Wechsler an."
            // where no time window was intended.
            String dateHint = (dataDateContext != null && !dataDateContext.isBlank())
                    ? "Die Datensätze in der Datenbank liegen im Zeitraum " + dataDateContext + ". "
                      + "Fehlt bei einer Datumsangabe in der Anfrage das Jahr, leite das Jahr "
                      + "aus diesem Datenbestand ab (nicht das aktuelle Kalenderjahr)."
                    : "";

            String prompt = String.format(
                    """
                    Du bist ein Assistent für die forensische Auswertung von Funkzellendaten.
                    Lies die folgende Anfrage und antworte AUSSCHLIESSLICH mit einem JSON-Objekt,
                    ohne Erklärung, ohne Markdown.

                    Erkenne, ob nach Teilnehmern gefragt wird, die sich gemeinsam,
                    gleichzeitig oder in derselben Funkzelle aufgehalten haben
                    (intent="colocation"), ob nach ein- oder ausgehenden Anrufen
                    bzw. gesendeten oder empfangenen SMS gefragt wird
                    (intent="calls_sms"), ob nach Rufnummern aus einem bestimmten
                    Land gefragt wird (intent="country_filter"), ob nach
                    Teilnehmern/"Wanderern" gefragt wird, die an mehr als einer
                    Funkzelle/an mehreren Standorten registriert waren
                    (intent="roamers"; Stichworte: "Wanderer", "mehrere
                    Funkzellen", "mehrere Standorte", "bewegt/bewegen sich"),
                    ob nach "Wechslern" gefragt wird, also Teilnehmern, die
                    mit mehreren unterschiedlichen Geräten (Geräte-Wechsel)
                    oder Geräten, die mit mehreren unterschiedlichen SIM-
                    Karten/IMSIs (SIM-Wechsel) aufgetreten sind
                    (intent="wechsler"; Stichworte: "Wechsler", "Gerätewechsel",
                    "Geräte-Wechsel", "SIM-Wechsel", "Handy gewechselt",
                    "SIM-Karte gewechselt", "mehrere Geräte", "mehrere
                    SIM-Karten"), oder ob es eine einfache Filterabfrage auf
                    Datensätze ist (intent="filter").
                    Extrahiere außerdem den genannten Zeitraum als
                    start_time/end_time im Format "YYYY-MM-DD HH:MM:SS".
                    Fehlt die Uhrzeit, nimm 00:00:00 bzw. 23:59:59. Ist kein Zeitraum
                    erkennbar, setze beide Felder auf null. %s

                    Bei intent="calls_sms" setze zusätzlich das Feld call_sms_type auf
                    genau einen der folgenden Werte: "eingehender_anruf" (eingehende /
                    erhaltene Anrufe), "ausgehender_anruf" (ausgehende / getätigte
                    Anrufe), "gesendete_sms" (gesendete / verschickte SMS/Nachrichten),
                    "empfangene_sms" (empfangene / erhaltene SMS/Nachrichten),
                    "alle_anrufe" (es wird nach Anrufen gefragt, OHNE dass eine Richtung
                    genannt wird — z.B. "alle Anrufe", "Anrufliste") oder "alle_sms"
                    (es wird nach SMS/Nachrichten gefragt, OHNE dass eine Richtung
                    genannt wird — z.B. "alle SMS", "alle Nachrichten"). Wird nicht
                    klar zwischen Anruf und SMS unterschieden, ordne dem genannten
                    Begriff zu (z.B. "SMS" → alle_sms, "Anrufe" → alle_anrufe), rate
                    NIEMALS auf intent="filter" nur weil keine Richtung erkennbar ist.
                    Bei den anderen intent-Werten setze call_sms_type auf null.
                    Für intent="calls_sms" ist ein Zeitraum optional — ist keiner
                    genannt, setze start_time/end_time auf null und liefere trotzdem
                    intent="calls_sms".

                    Bei intent="country_filter" setze zusätzlich das Feld country auf
                    den im Text genannten Ländernamen, unverändert in der Schreibweise
                    der Anfrage (z.B. "Polen", "Frankreich", "USA") — übersetze oder
                    normalisiere ihn nicht. Das Land kann auch als Adjektiv genannt sein
                    (z.B. "französische", "polnische", "deutsche" Rufnummern/Telefonnummern)
                    — übernimm in diesem Fall genau dieses Adjektiv unverändert in das
                    Feld country, wandle es NICHT in die Landesbezeichnung um. Anfragen
                    nach Rufnummern/Telefonnummern "aus" einem Land oder mit einem
                    Länder-Adjektiv sind IMMER intent="country_filter", niemals
                    intent="filter". Bei den anderen intent-Werten setze country
                    auf null. Für intent="country_filter" ist ein Zeitraum ebenfalls
                    optional.

                    Datumsangaben können in unterschiedlicher Reihenfolge vorliegen
                    (z.B. TT.MM.JJJJ oder MM.TT.JJJJ). Ist eine der ersten beiden Zahlen
                    größer als 12, ist sie eindeutig der Tag, unabhängig von ihrer
                    Position. Beispiel: "09.27.2024" kann nur der 27.09.2024 sein, da
                    es keinen Monat 27 gibt. Rate niemals "kein Zeitraum erkennbar" nur
                    wegen einer ungewöhnlichen Zahlenreihenfolge — löse die Mehrdeutigkeit
                    immer so auf, dass ein gültiges Datum entsteht.

                    Datumsangaben können auch ausgeschriebene deutsche Monatsnamen
                    enthalten (z.B. "2. Juli 2023", "15. März 2024", "1. Januar 2025").
                    Monatsnamen: Januar=01, Februar=02, März=03, April=04, Mai=05,
                    Juni=06, Juli=07, August=08, September=09, Oktober=10,
                    November=11, Dezember=12. Wandle diese immer ins Format
                    YYYY-MM-DD um.

                    Erkenne außerdem, welche Teilnehmerkennung gemeint ist:
                    "imsi" (IMSI / SIM-Karte, das ist der Standardfall, wenn nichts
                    anderes genannt wird), "msisdn" (MSISDN / Rufnummer / Telefonnummer)
                    oder "imei" (IMEI / Geräte-/Gerätenummer). Gib genau einen dieser
                    drei Werte im Feld identifier zurück.

                    Antwortschema (immer exakt, genau eine Zeile, keine weiteren Felder, kein Markdown):
                    {"intent": "colocation" | "calls_sms" | "country_filter" | "roamers" | "wechsler" | "filter", "start_time": "YYYY-MM-DD HH:MM:SS" | null, "end_time": "YYYY-MM-DD HH:MM:SS" | null, "identifier": "imsi" | "msisdn" | "imei", "call_sms_type": "eingehender_anruf" | "ausgehender_anruf" | "gesendete_sms" | "empfangene_sms" | "alle_anrufe" | "alle_sms" | null, "country": "<Ländername>" | null}
                    Gib NUR dieses JSON zurück — keine Erläuterungen, keine Beispiele, keine zusätzlichen Felder, kein weiterer Text.

                    Beispiel 1
                    Anfrage: Zeige mir die IMSI Nummern an, die sich im Zeitfenster von 27.09.2024 02:00 bis 27.09.2024 04:00 gemeinsam in Funkzellen aufgehalten haben
                    Antwort: {"intent": "colocation", "start_time": "2024-09-27 02:00:00", "end_time": "2024-09-27 04:00:00", "identifier": "imsi", "call_sms_type": null, "country": null}

                    Beispiel 2
                    Anfrage: Welche Rufnummern (MSISDN) waren am 01.01.2025 zwischen 10 und 11 Uhr in derselben Funkzelle?
                    Antwort: {"intent": "colocation", "start_time": "2025-01-01 10:00:00", "end_time": "2025-01-01 11:00:00", "identifier": "msisdn", "call_sms_type": null, "country": null}

                    Beispiel 3
                    Anfrage: Zeige mir alle Datensätze von IMSI 12345 nach 18 Uhr
                    Antwort: {"intent": "filter", "start_time": null, "end_time": null, "identifier": "imsi", "call_sms_type": null, "country": null}

                    Beispiel 4
                    Anfrage: Welche IMEI-Nummern waren zwischen dem 03.03.2025 08:00 und 09:00 gemeinsam in einer Funkzelle angemeldet?
                    Antwort: {"intent": "colocation", "start_time": "2025-03-03 08:00:00", "end_time": "2025-03-03 09:00:00", "identifier": "imei", "call_sms_type": null, "country": null}

                    Beispiel 5
                    Anfrage: Welche Teilnehmer waren am 09.27.2024 zwischen 0 und 20 Uhr in derselben Funkzelle?
                    Antwort: {"intent": "colocation", "start_time": "2024-09-27 00:00:00", "end_time": "2024-09-27 20:00:00", "identifier": "imsi", "call_sms_type": null, "country": null}

                    Beispiel 6
                    Anfrage: Zeige mir alle eingehenden Anrufe am 27.09.2024
                    Antwort: {"intent": "calls_sms", "start_time": "2024-09-27 00:00:00", "end_time": "2024-09-27 23:59:59", "identifier": "imsi", "call_sms_type": "eingehender_anruf", "country": null}

                    Beispiel 7
                    Anfrage: Welche SMS wurden versendet?
                    Antwort: {"intent": "calls_sms", "start_time": null, "end_time": null, "identifier": "imsi", "call_sms_type": "gesendete_sms", "country": null}

                    Beispiel 8
                    Anfrage: Zeige ausgehende Anrufe zwischen 8 und 9 Uhr am 01.01.2025
                    Antwort: {"intent": "calls_sms", "start_time": "2025-01-01 08:00:00", "end_time": "2025-01-01 09:00:00", "identifier": "imsi", "call_sms_type": "ausgehender_anruf", "country": null}

                    Beispiel 9
                    Anfrage: Welche Nachrichten wurden empfangen?
                    Antwort: {"intent": "calls_sms", "start_time": null, "end_time": null, "identifier": "imsi", "call_sms_type": "empfangene_sms", "country": null}

                    Beispiel 10
                    Anfrage: Zeige mir alle Rufnummern aus Polen auf der Karte
                    Antwort: {"intent": "country_filter", "start_time": null, "end_time": null, "identifier": "imsi", "call_sms_type": null, "country": "Polen"}

                    Beispiel 11
                    Anfrage: Welche Nummern aus Frankreich gibt es zwischen dem 01.01.2025 und 02.01.2025?
                    Antwort: {"intent": "country_filter", "start_time": "2025-01-01 00:00:00", "end_time": "2025-01-02 23:59:59", "identifier": "imsi", "call_sms_type": null, "country": "Frankreich"}

                    Beispiel 12
                    Anfrage: Zeige mir alle Französischen Telefonnummern an
                    Antwort: {"intent": "country_filter", "start_time": null, "end_time": null, "identifier": "msisdn", "call_sms_type": null, "country": "Französischen"}

                    Beispiel 13
                    Anfrage: Zeige mir alle SMS an
                    Antwort: {"intent": "calls_sms", "start_time": null, "end_time": null, "identifier": "msisdn", "call_sms_type": "alle_sms", "country": null}

                    Beispiel 14
                    Anfrage: Zeige mir alle Anrufe am 27.09.2024
                    Antwort: {"intent": "calls_sms", "start_time": "2024-09-27 00:00:00", "end_time": "2024-09-27 23:59:59", "identifier": "msisdn", "call_sms_type": "alle_anrufe", "country": null}

                    Beispiel 15
                    Anfrage: Zeige mir alle Wanderer an
                    Antwort: {"intent": "roamers", "start_time": null, "end_time": null, "identifier": "imsi", "call_sms_type": null, "country": null}

                    Beispiel 16
                    Anfrage: Welche Rufnummern waren zwischen dem 01.01.2025 und 02.01.2025 an mehreren Funkzellen registriert?
                    Antwort: {"intent": "roamers", "start_time": "2025-01-01 00:00:00", "end_time": "2025-01-02 23:59:59", "identifier": "msisdn", "call_sms_type": null, "country": null}

                    Beispiel 17
                    Anfrage: Zeige mir alle Wechsler an
                    Antwort: {"intent": "wechsler", "start_time": null, "end_time": null, "identifier": "imsi", "call_sms_type": null, "country": null}

                    Beispiel 18
                    Anfrage: Welche IMSIs haben am 27.09.2024 das Gerät gewechselt?
                    Antwort: {"intent": "wechsler", "start_time": "2024-09-27 00:00:00", "end_time": "2024-09-27 23:59:59", "identifier": "imsi", "call_sms_type": null, "country": null}

                    Beispiel 19
                    Anfrage: Zeige mir alle französischen Telefonnummern an vom 2. Juli 2023
                    Antwort: {"intent": "country_filter", "start_time": "2023-07-02 00:00:00", "end_time": "2023-07-02 23:59:59", "identifier": "msisdn", "call_sms_type": null, "country": "französischen"}

                    Beispiel 20
                    Anfrage: Welche Rufnummern waren am 15. März 2024 zwischen 8 und 10 Uhr in derselben Funkzelle?
                    Antwort: {"intent": "colocation", "start_time": "2024-03-15 08:00:00", "end_time": "2024-03-15 10:00:00", "identifier": "msisdn", "call_sms_type": null, "country": null}

                    Beispiel 21
                    Anfrage: Zeige mir alle Teilnehmer vom 2023-07-02 zwischen 5 und 7 Uhr an
                    Antwort: {"intent": "filter", "start_time": "2023-07-02 05:00:00", "end_time": "2023-07-02 07:00:00", "identifier": "imsi", "call_sms_type": null, "country": null}

                    Beispiel 22
                    Anfrage: Welche Datensätze liegen am 2024-09-27 vor?
                    Antwort: {"intent": "filter", "start_time": "2024-09-27 00:00:00", "end_time": "2024-09-27 23:59:59", "identifier": "imsi", "call_sms_type": null, "country": null}

                    Anfrage: %s
                    Antwort:
                    """,
                    dateHint, request
            );

            InferenceParameters inferParams = new InferenceParameters(prompt)
                    .setTemperature(0.0f)
                    .setTopP(0.9f)
                    .setTopK(40)
                    .setNPredict(150)
                    .setStopStrings("\n\n");

            LlamaIterable outputs = model.generate(inferParams);
            StringBuilder out = new StringBuilder();
            for (LlamaOutput output : outputs) {
                out.append(output.text);
            }
            return out.toString();
        }


    }

    // ========================================================================
    // RESULT CLASS
    // ========================================================================

    public static class QueryResult {
        public final boolean success;
        public final String message;
        public final List<Map<String, Object>> data;
        public final String sql;

        public QueryResult(boolean success, String message,
                           List<Map<String, Object>> data, String sql) {
            this.success = success;
            this.message = message;
            this.data = data;
            this.sql = sql;
        }

        public void print() {
            System.out.println("\n" + "=".repeat(80));
            System.out.println("QUERY RESULT");
            System.out.println("=".repeat(80));
            System.out.println("Success: " + success);
            System.out.println("SQL: " + sql);
            System.out.println("Message: " + message);

            if (data != null && !data.isEmpty()) {
                System.out.println("\nResults: " + data.size() + " rows");
                for (int i = 0; i < Math.min(5, data.size()); i++) {
                    System.out.println("  Row " + (i+1) + ": " + data.get(i));
                }
                if (data.size() > 5) {
                    System.out.println("  ... (" + (data.size() - 5) + " more)");
                }
            }
            System.out.println("=".repeat(80));
        }
    }



    // ========================================================================
    // MAIN - Example Usage
    // ========================================================================

    public static void main(String[] args) {
        String modelPath = "/Users/pawel/llm_models/forensic-sqlite-llama-3.2-3b-Q4_K_M.gguf";
        String schema = "";

        try {
            RAGPipeline analyzer = new RAGPipeline(modelPath, schema);

            String[] requests = {
                    "Find all unread messages from yesterday",
                    "Show me messages with media attachments",
                    "List all group conversations",
                    "Find deleted messages"
            };

            System.out.println("\n" + "=".repeat(80));
            System.out.println("FORENSIC SQL ANALYSIS WITH RAG");
            System.out.println("=".repeat(80));

            for (String request : requests) {
                analyzer.analyzeRequest(request);
                Arrays.stream(requests).toList().stream().toString();
            }

        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
