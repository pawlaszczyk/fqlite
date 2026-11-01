package fqlite.rag;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Datenbankgestuetzte Reparatur und feste Vorlagen fuer den freien SQL-Pfad
 * (ETSI-CDR). Ergaenzt {@link SqlAutoFix} (rein syntaktisch) um Pruefungen,
 * die die In-Memory-Arbeitskopie brauchen.
 *
 * <ul>
 *   <li><b>FIX_ID_COLUMN</b>: {@code msisdn|imsi|na_device_id = 'wert'}, der Wert kommt in
 *       dieser Spalte nicht vor, aber in genau einer der anderen Kennungsspalten → Spalte
 *       tauschen (typisch: IMSI 26000… wurde als msisdn abgefragt).</li>
 *   <li><b>FIX_COUNT_ALIAS</b>: SQLite meldet {@code no such column: count|n|cnt|anzahl…}
 *       in ORDER BY/HAVING, das SELECT enthaelt genau ein COUNT(…) → durch diesen Ausdruck
 *       ersetzen.</li>
 *   <li><b>Vorlage TOP_CELLS</b>: „In welchen (drei) Zellen war X am häufigsten/öftesten …“
 *       wird ohne Modell beantwortet (Gruppierung nach MCC/MNC/LAC/CI).</li>
 * </ul>
 * Alle Aenderungen sind konservativ: im Zweifel bleibt das SQL unveraendert.
 */
public final class SqlRepair {

    private SqlRepair() { }

    public static final class Result {
        public final String sql;
        public final List<String> fixes;
        Result(String sql, List<String> fixes) { this.sql = sql; this.fixes = fixes; }
        public boolean changed() { return !fixes.isEmpty(); }
    }

    private static final int F = Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
    private static final String[] ID_COLS = {"msisdn", "imsi", "na_device_id"};
    private static final Pattern ID_EQ = Pattern.compile(
            "\\b((?:\\w+\\.)?)(msisdn|imsi|na_device_id)(\\s*=\\s*)'([^']*)'", F);
    private static final Pattern OTHER_REL = Pattern.compile(
            "\\b(contacts|call_pairs|sms_pairs|record_gaps)\\b", F);
    private static final Pattern NO_SUCH_COL = Pattern.compile("no such column:\\s*\"?([\\w.]+)\"?", F);
    private static final Pattern COUNT_EXPR = Pattern.compile("\\bcount\\s*\\(\\s*(?:distinct\\s+)?[^()]*\\)", F);
    private static final List<String> COUNT_ALIASES = List.of(
            "count", "cnt", "n", "num", "number", "anzahl", "total", "freq", "frequency", "haeufigkeit", "häufigkeit", "c");

    // ── Reparatur ───────────────────────────────────────────────────────────

    public static Result repair(Connection conn, String sql) {
        List<String> fixes = new ArrayList<>();
        if (conn == null || sql == null || sql.isBlank()) return new Result(sql, fixes);

        // FIX_ID_COLUMN (nur auf response_records, nicht auf den Views mit eigener msisdn-Spalte)
        if (!OTHER_REL.matcher(sql).find()) {
            Matcher m = ID_EQ.matcher(sql);
            StringBuilder sb = new StringBuilder();
            int last = 0;
            while (m.find()) {
                String col = m.group(2).toLowerCase(Locale.ROOT), val = m.group(4);
                String repl = null;
                if (!valueExists(conn, col, val)) {
                    String found = null; int hits = 0;
                    for (String other : ID_COLS) {
                        if (other.equals(col)) continue;
                        if (valueExists(conn, other, val)) { found = other; hits++; }
                    }
                    if (hits == 1) repl = m.group(1) + found + m.group(3) + "'" + val + "'";
                }
                sb.append(sql, last, m.start()).append(repl != null ? repl : m.group());
                last = m.end();
                if (repl != null && !fixes.contains("FIX_ID_COLUMN")) fixes.add("FIX_ID_COLUMN");
            }
            sb.append(sql.substring(last));
            sql = sb.toString();
        }

        // FIX_COUNT_ALIAS (nur wenn SQLite die Spalte tatsaechlich nicht kennt)
        String err = prepareError(conn, sql);
        if (err != null) {
            Matcher nm = NO_SUCH_COL.matcher(err);
            if (nm.find()) {
                String bad = nm.group(1);
                Matcher cm = COUNT_EXPR.matcher(sql);
                List<String> counts = new ArrayList<>();
                while (cm.find()) if (!counts.contains(cm.group())) counts.add(cm.group());
                if (COUNT_ALIASES.contains(bad.toLowerCase(Locale.ROOT)) && counts.size() == 1) {
                    Pattern p = Pattern.compile("\\b(ORDER\\s+BY|HAVING)(\\s+)" + Pattern.quote(bad) + "\\b", F);
                    String fixed = p.matcher(sql).replaceAll(r -> Matcher.quoteReplacement(
                            r.group(1) + r.group(2) + counts.get(0)));
                    if (!fixed.equals(sql)) { sql = fixed; fixes.add("FIX_COUNT_ALIAS"); }
                }
            }
        }
        return new Result(sql, fixes);
    }

    /** Fehlermeldung von SQLite beim Vorbereiten, oder {@code null}, wenn das SQL gueltig ist. */
    public static String prepareError(Connection conn, String sql) {
        if (conn == null || sql == null) return null;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            return null;
        } catch (SQLException e) {
            return e.getMessage();
        }
    }

    private static boolean valueExists(Connection conn, String col, String val) {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM response_records WHERE " + col + " = ? LIMIT 1")) {
            ps.setString(1, val);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        } catch (SQLException e) {
            return true;   // im Zweifel nichts aendern
        }
    }

    // ── Vorlage TOP_CELLS ───────────────────────────────────────────────────

    private static final Pattern CELL_WORD = Pattern.compile(
            "\\b(funk)?zellen?\\b|\\bcells?\\b|\\bstandort(e|en)?\\b|\\bsites?\\b", F);
    private static final Pattern CELL_PLURAL = Pattern.compile(
            "\\b(funk)?zellen\\b|\\bcells\\b|\\bstandorte[n]?\\b|\\bsites\\b", F);
    private static final Pattern MOST = Pattern.compile(
            "\\b(am\\s+(h[äa]ufigsten|[öo]e?ftesten|meisten)|h[äa]ufigste\\w*|meistgenutzte\\w*|most\\s+(often|frequent\\w*|used)|top)\\b", F);
    private static final Pattern IDENT = Pattern.compile("\\b(\\d{8,})\\b");
    private static final Map<String, Integer> NUM_WORDS = Map.ofEntries(
            Map.entry("zwei", 2), Map.entry("drei", 3), Map.entry("vier", 4), Map.entry("fünf", 5),
            Map.entry("fuenf", 5), Map.entry("sechs", 6), Map.entry("sieben", 7), Map.entry("acht", 8),
            Map.entry("neun", 9), Map.entry("zehn", 10), Map.entry("two", 2), Map.entry("three", 3),
            Map.entry("four", 4), Map.entry("five", 5), Map.entry("six", 6), Map.entry("seven", 7),
            Map.entry("eight", 8), Map.entry("nine", 9), Map.entry("ten", 10));
    private static final Pattern TOP_N = Pattern.compile(
            "\\b(\\d{1,2}|zwei|drei|vier|f[üu]e?nf|sechs|sieben|acht|neun|zehn|two|three|four|five|six|seven|eight|nine|ten)\\s+"
            + "(?:\\w+\\s+)?(funk)?(zellen|cells|standorte[n]?|sites)\\b", F);
    private static final Pattern DATE_OR_TIME = Pattern.compile(
            "\\b\\d{4}-\\d{2}-\\d{2}\\b|\\b\\d{1,2}\\.\\d{1,2}\\.|\\buhr\\b|\\b\\d{1,2}:\\d{2}\\b", F);

    /**
     * Liefert fertiges SQL fuer „In welchen N Zellen war X am häufigsten …“, sonst {@code null}.
     * Greift nur ohne Datums-/Uhrzeitangabe (dann bleibt es beim freien Pfad) und nur, wenn
     * genau eine Kennung genannt ist und sie in genau einer Kennungsspalte vorkommt.
     */
    public static String topCellsSql(Connection conn, String request) {
        if (conn == null || request == null) return null;
        String r = request.toLowerCase(Locale.ROOT);
        if (!CELL_WORD.matcher(r).find() || !MOST.matcher(r).find() || DATE_OR_TIME.matcher(r).find()) return null;
        Matcher im = IDENT.matcher(r);
        List<String> ids = new ArrayList<>();
        while (im.find()) ids.add(im.group(1));
        if (ids.size() != 1) return null;
        String val = ids.get(0);
        String col = null; int hits = 0;
        for (String c : ID_COLS) if (valueExists(conn, c, val)) { col = c; hits++; }
        if (hits != 1) return null;

        int n = CELL_PLURAL.matcher(r).find() ? 5 : 1;
        Matcher nm = TOP_N.matcher(r);
        if (nm.find()) {
            String w = nm.group(1);
            Integer v = w.chars().allMatch(Character::isDigit) ? Integer.valueOf(w)
                    : NUM_WORDS.get(w);
            if (v == null && w.startsWith("f")) v = 5;
            if (v != null && v > 0 && v <= 50) n = v;
        }
        // Gleichstaende am Rang N werden mit ausgegeben (forensisch: kein willkuerliches Abschneiden
        // durch LIMIT); bei weniger als N Zellen werden alle gezeigt.
        return "WITH z AS (SELECT cell_mcc, cell_mnc, cell_lac_tac, cell_ci, COUNT(*) AS anzahl, "
                + "MIN(start_time) AS erster, MAX(start_time) AS letzter "
                + "FROM response_records WHERE " + col + " = '" + val.replace("'", "''") + "' "
                + "AND cell_ci IS NOT NULL "
                + "GROUP BY cell_mcc, cell_mnc, cell_lac_tac, cell_ci) "
                + "SELECT * FROM z WHERE anzahl >= COALESCE("
                + "(SELECT anzahl FROM z ORDER BY anzahl DESC LIMIT 1 OFFSET " + (n - 1) + "), 0) "
                + "ORDER BY anzahl DESC, erster;";
    }
}
