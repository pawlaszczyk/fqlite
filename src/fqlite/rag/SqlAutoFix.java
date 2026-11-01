package fqlite.rag;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Deterministische Korrektur von Uhrzeit-/Zeitstempel-Typfehlern in vom LLM
 * generiertem SQL (ETSI-CDR). Java-Port von {@code plugin_autofix.py}
 * (SQLiteForensicsModel), dort ist auch die Paritaet getestet.
 *
 * <p>Hintergrund: start_time ist ein Zeitstempel 'YYYY-MM-DD HH:MM:SS'. Ein
 * Vergleich wie {@code start_time >= '21:00:00'} ist in SQLite ein
 * String-Vergleich und liefert stillschweigend falsche Ergebnisse (meist 0
 * Zeilen). Das quantisierte Modell (Q4_K_M) erzeugt diesen Fehler haeufiger
 * als das HF-Modell.</p>
 *
 * <p>Nur Typ-/Syntaxkorrekturen, keine inhaltlichen Aenderungen; braucht
 * weder DB noch Referenz. Regeln:</p>
 * <ol>
 *   <li>FIX_TIME_FULL  time(col) &lt;op&gt; 'YYYY-MM-DD HH:MM[:SS]' → col &lt;op&gt; '...'</li>
 *   <li>FIX_TIME_FULL  time(col) BETWEEN 'full' AND 'full' → col BETWEEN ...</li>
 *   <li>FIX_TIME_ONLY  col BETWEEN 'HH:MM' AND 'HH:MM' → time(col) BETWEEN ...</li>
 *   <li>FIX_TIME_ONLY  col &lt;op&gt; 'HH:MM[:SS]' → time(col) &lt;op&gt; 'HH:MM:SS'</li>
 *   <li>FIX_TS_SECONDS col &lt;op&gt; 'YYYY-MM-DD HH:MM' → '... HH:MM:00'</li>
 *   <li>FIX_TIME_WRAP  time(col) BETWEEN '21:00:00' AND '05:00:00' (ueber Mitternacht)
 *       → (time(col) &gt;= '21:00:00' OR time(col) &lt;= '05:00:00')</li>
 * </ol>
 */
public final class SqlAutoFix {

    private SqlAutoFix() { }

    /** Ergebnis: korrigiertes SQL und die angewandten Regel-Codes. */
    public static final class Result {
        public final String sql;
        public final List<String> fixes;
        Result(String sql, List<String> fixes) { this.sql = sql; this.fixes = fixes; }
        public boolean changed() { return !fixes.isEmpty(); }
    }

    private static final String TIME_COLS = "(?:\\w+\\.)?(?:start_time|end_time|prev_start_time)";
    private static final String OPS = "(?:<=|>=|<>|!=|=|<|>)";
    private static final String T = "\\d{1,2}:\\d{2}(?::\\d{2})?";
    /** Anfang eines Bezeichners, aber nicht Argument eines Funktionsaufrufs wie time(...). */
    private static final String NOT_FN = "(?<![\\w.])(?<!\\w\\()";
    private static final int F = Pattern.CASE_INSENSITIVE;

    private static final Pattern P1 = Pattern.compile(
            "\\btime\\(\\s*(" + TIME_COLS + ")\\s*\\)\\s*(" + OPS + ")\\s*'(\\d{4}-\\d{2}-\\d{2}) (" + T + ")'", F);
    private static final Pattern P2 = Pattern.compile(
            "\\btime\\(\\s*(" + TIME_COLS + ")\\s*\\)\\s+BETWEEN\\s+'(\\d{4}-\\d{2}-\\d{2} [\\d:]+)'\\s+AND\\s+'(\\d{4}-\\d{2}-\\d{2} [\\d:]+)'", F);
    private static final Pattern P3 = Pattern.compile(
            NOT_FN + "(" + TIME_COLS + ")\\s+BETWEEN\\s+'(" + T + ")'\\s+AND\\s+'(" + T + ")'", F);
    private static final Pattern P4 = Pattern.compile(
            NOT_FN + "(" + TIME_COLS + ")\\s*(" + OPS + ")\\s*'(" + T + ")'", F);
    private static final Pattern P5 = Pattern.compile(
            NOT_FN + "(" + TIME_COLS + ")\\s*(" + OPS + ")\\s*'(\\d{4}-\\d{2}-\\d{2}) (\\d{1,2}:\\d{2})'", F);
    private static final Pattern P6 = Pattern.compile(
            "\\btime\\(\\s*(" + TIME_COLS + ")\\s*\\)\\s+BETWEEN\\s+'(" + T + ")'\\s+AND\\s+'(" + T + ")'", F);

    public static Result autofix(String sql) {
        List<String> fixes = new ArrayList<>();
        boolean[] wrap = {false};
        if (sql == null) return new Result(null, fixes);

        sql = sub(P1, m -> m.group(1) + " " + m.group(2) + " '" + m.group(3) + " " + padTime(m.group(4)) + "'",
                sql, "FIX_TIME_FULL", fixes);
        sql = sub(P2, m -> m.group(1) + " BETWEEN '" + m.group(2) + "' AND '" + m.group(3) + "'",
                sql, "FIX_TIME_FULL", fixes);
        sql = sub(P3, m -> betweenTime(m.group(1), m.group(2), m.group(3), wrap),
                sql, "FIX_TIME_ONLY", fixes);
        sql = sub(P4, m -> "time(" + m.group(1) + ") " + m.group(2) + " '" + padTime(m.group(3)) + "'",
                sql, "FIX_TIME_ONLY", fixes);
        sql = sub(P5, m -> m.group(1) + " " + m.group(2) + " '" + m.group(3) + " " + padTime(m.group(4)) + "'",
                sql, "FIX_TS_SECONDS", fixes);
        sql = sub(P6, m -> betweenTime(m.group(1), m.group(2), m.group(3), wrap), sql, null, fixes);
        if (wrap[0]) fixes.add("FIX_TIME_WRAP");
        return new Result(sql, fixes);
    }

    private static String sub(Pattern p, Function<MatchResult, String> repl, String s,
                              String code, List<String> fixes) {
        Matcher m = p.matcher(s);
        if (!m.find()) return s;
        m.reset();
        String out = m.replaceAll(r -> Matcher.quoteReplacement(repl.apply(r)));
        if (code != null && !fixes.contains(code)) fixes.add(code);
        return out;
    }

    private static String betweenTime(String col, String a, String b, boolean[] wrap) {
        a = padTime(a);
        b = padTime(b);
        if (a.compareTo(b) > 0) {
            wrap[0] = true;
            return "(time(" + col + ") >= '" + a + "' OR time(" + col + ") <= '" + b + "')";
        }
        return "time(" + col + ") BETWEEN '" + a + "' AND '" + b + "'";
    }

    static String padTime(String t) {
        String[] p = t.split(":");
        String[] out = {"00", "00", "00"};
        for (int i = 0; i < Math.min(3, p.length); i++) {
            out[i] = p[i].length() < 2 ? "0".repeat(2 - p[i].length()) + p[i] : p[i];
        }
        return out[0] + ":" + out[1] + ":" + out[2];
    }
}
