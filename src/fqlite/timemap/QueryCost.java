package fqlite.timemap;

import fqlite.base.Limits;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.control.Alert;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.layout.GridPane;
import javafx.stage.Window;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Estimates how long a query on a large Lighthouse database will take, before
 * it runs, and asks the analyst whether to run it when the estimate exceeds
 * {@link Limits#LIGHTHOUSE_CONFIRM_SECONDS}.
 *
 * <p>The estimate counts the records the query has to read - all records, or
 * those of its {@code start_time} range, counted through the index in a
 * fraction of a second. Every SELECT (also in subqueries) reads them once
 * inside SQLite; the records of the first query may in addition all be
 * returned and loaded. The rates come from this computer: the overview build
 * measures the records per second Lighthouse loads (kept in
 * {@code ~/.fqlite/lighthouse-cache/scan-rate.txt}); a pass inside SQLite is
 * about {@link #SQL_FACTOR} times as fast. How many records a query returns
 * (Wanderer, Wechsler) is only known afterwards, so the estimate is an upper
 * bound: a query with few results finishes sooner.</p>
 */
public final class QueryCost {

    private QueryCost() { }

    /** a pass inside SQLite (grouping, counting) compared to loading records */
    private static final double SQL_FACTOR = 3;

    /** records per second when no measurement exists yet */
    private static final double DEFAULT_RATE = 350_000;
    private static volatile double rate = -1;

    private static final Pattern SELECT = Pattern.compile("(?i)\\bSELECT\\b");
    private static final Pattern JOIN = Pattern.compile("(?i)\\bJOIN\\b");
    private static final Pattern RANGE = Pattern.compile(
            "(?i)start_time\\s+BETWEEN\\s+'([^']*)'\\s+AND\\s+'([^']*)'");
    /** a literal identifier lookup the importer's indexes answer directly */
    private static final Pattern ID_LOOKUP = Pattern.compile(
            "(?i)\\b(?:imsi|msisdn|na_device_id)\\s*(?:=\\s*'|IN\\s*\\(\\s*')");

    /** result of {@link #estimate} */
    public record Estimate(long records, int passes, double seconds, String from, String to) {
        /** e.g. "ca. 12,3 Mio. Datensätze (01.04.–30.04.)" */
        String scope() {
            String n = records >= 1_000_000 ? String.format(Lang.t("%,.1f Mio.", "%,.1f million"), records / 1e6)
                    : String.format("%,d", records);
            String range = from != null ? " (" + from + " – " + to + ")" : Lang.t(" (gesamter Zeitraum)", " (whole period)");
            return Lang.t("ca. ", "about ") + n + Lang.t(" Datensätze", " records") + range;
        }

        /** e.g. "bis zu ca. 3 Minuten" (upper bound, see the class description) */
        String duration() {
            String up = Lang.t("bis zu ca. ", "up to about ");
            if (seconds < 90) return up + String.format(Lang.t("%d Sekunden", "%d seconds"), Math.round(seconds));
            long min = Math.round(seconds / 60);
            if (min < 90) return up + String.format(Lang.t("%d Minuten", "%d minutes"), min);
            return up + String.format(Lang.t("%.1f Stunden", "%.1f hours"), seconds / 3600);
        }
    }

    // ------------------------------------------------------------ read rate

    private static File rateFile() {
        return new File(new File(new File(System.getProperty("user.home"), ".fqlite"), "lighthouse-cache"), "scan-rate.txt");
    }

    /** records per second this computer reads during a pass over a table */
    public static double rate() {
        if (rate < 0) {
            double r = DEFAULT_RATE;
            try {
                File f = rateFile();
                if (f.isFile()) r = Double.parseDouble(Files.readString(f.toPath(), StandardCharsets.UTF_8).trim());
            } catch (Exception ignored) {
                // keep the default
            }
            rate = r > 1000 ? r : DEFAULT_RATE;
        }
        return rate;
    }

    /** Records a measured pass: {@code records} read in {@code seconds}. */
    public static void record(long records, double seconds) {
        if (records < 100_000 || seconds <= 0.5) return;   // too short to be meaningful
        double r = records / seconds;
        rate = rate < 0 ? r : 0.5 * rate + 0.5 * r;
        try {
            File f = rateFile();
            File dir = f.getParentFile();
            if (dir.isDirectory() || dir.mkdirs())
                Files.writeString(f.toPath(), String.valueOf(Math.round(rate)), StandardCharsets.UTF_8);
        } catch (Exception ignored) {
            // the rate is only a convenience
        }
    }

    // ------------------------------------------------------------- estimate

    /**
     * Estimates the queries {@code sqls}, run one after the other.
     *
     * @param c         connection to the database (counts through the index only)
     * @param tableRows number of records of the table
     * @param ranges    cache of range counts ("from\0to" -&gt; records), may be shared
     */
    public static Estimate estimate(Connection c, List<String> sqls, long tableRows, Map<String, Long> ranges)
            throws SQLException {
        long maxRecords = 0;
        int passes = 0;
        double seconds = 0;
        String from = null, to = null;
        boolean first = true;
        for (String sql : sqls) {
            if (sql == null) continue;
            int p = count(SELECT, sql) + count(JOIN, sql);
            long hits;        // records matching the time range (upper bound of the result)
            double read;      // records read per pass
            Matcher m = RANGE.matcher(sql);
            if (m.find()) {
                from = m.group(1);
                to = m.group(2);
                hits = rangeCount(c, from, to, ranges);
                // a large range is read sequentially, a small one through the index
                read = hits * 20 > tableRows ? tableRows : hits * 4.0;
            } else if (ID_LOOKUP.matcher(sql).find() && count(SELECT, sql) == 1) {
                hits = 0;     // answered by an index
                read = 0;
            } else {
                hits = tableRows;
                read = tableRows;
            }
            if (read > 0) {
                passes += p;
                seconds += p * read / (SQL_FACTOR * rate());
                if (first) seconds += hits / rate();   // the result may be all of them
            }
            maxRecords = Math.max(maxRecords, hits);
            first = false;
        }
        return new Estimate(maxRecords, passes, seconds, from, to);
    }

    private static int count(Pattern p, String s) {
        Matcher m = p.matcher(s);
        int n = 0;
        while (m.find()) n++;
        return n;
    }

    private static long rangeCount(Connection c, String from, String to, Map<String, Long> cache) throws SQLException {
        String key = from + "\u0000" + to;
        Long n = cache != null ? cache.get(key) : null;
        if (n != null) return n;
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT COUNT(*) FROM response_records WHERE start_time BETWEEN ? AND ?")) {
            ps.setString(1, from);
            ps.setString(2, to);
            try (ResultSet rs = ps.executeQuery()) {
                n = rs.next() ? rs.getLong(1) : 0L;
            }
        }
        if (cache != null) cache.put(key, n);
        return n;
    }

    /** number of records of the table */
    public static long tableRows(Connection c) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM response_records")) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    // ------------------------------------------------------------- question

    /** answer of {@link #ask}: run, cancel, or run with this time range */
    public sealed interface Decision permits Run, Cancel, Restrict { }
    public record Run() implements Decision { }
    public record Cancel() implements Decision { }
    public record Restrict(String from, String to) implements Decision { }

    public static final Decision RUN = new Run();
    public static final Decision CANCEL = new Cancel();

    /** true if {@code e} needs a confirmation (setting, 0 = never) */
    public static boolean needsConfirmation(Estimate e) {
        int limit = Limits.LIGHTHOUSE_CONFIRM_SECONDS;
        return limit > 0 && e != null && e.seconds() >= limit;
    }

    /**
     * Asks whether to run a query estimated as {@code e}. Can be called from
     * any thread; a worker thread waits for the answer.
     *
     * @param restrictRange preset of "Restrict time range" (from, to: yyyy-MM-dd HH:mm:ss),
     *                      or {@code null} if the query cannot be restricted
     *                      (then the button is not offered)
     */
    public static Decision ask(Window owner, String what, Estimate e, String[] restrictRange) {
        if (Platform.isFxApplicationThread()) return askOnFx(owner, what, e, restrictRange);
        CompletableFuture<Decision> f = new CompletableFuture<>();
        Platform.runLater(() -> {
            try {
                f.complete(askOnFx(owner, what, e, restrictRange));
            } catch (RuntimeException ex) {
                f.complete(CANCEL);
            }
        });
        return f.join();
    }

    private static Decision askOnFx(Window owner, String what, Estimate e, String[] restrictRange) {
        ButtonType run = new ButtonType(Lang.t("Trotzdem ausführen", "Run anyway"), ButtonBar.ButtonData.OK_DONE);
        ButtonType restrict = new ButtonType(Lang.t("Zeitraum einschränken…", "Restrict time range…"), ButtonBar.ButtonData.OTHER);
        ButtonType cancel = new ButtonType(Lang.t("Abbrechen", "Cancel"), ButtonBar.ButtonData.CANCEL_CLOSE);
        Alert a = restrictRange != null
                ? new Alert(Alert.AlertType.CONFIRMATION, "", run, restrict, cancel)
                : new Alert(Alert.AlertType.CONFIRMATION, "", run, cancel);
        if (owner != null) a.initOwner(owner);
        a.setTitle("Lighthouse");
        a.setHeaderText(Lang.t("Diese Abfrage dauert voraussichtlich lange.", "This query will probably take a long time."));
        Label text = new Label((what != null && !what.isBlank() ? what + "\n\n" : "")
                + Lang.t("Betroffen: ", "Affected: ") + e.scope()
                + (e.passes() > 1 ? String.format(Lang.t(", %d Durchläufe", ", %d passes"), e.passes()) : "") + "\n"
                + Lang.t("Geschätzte Dauer: ", "Estimated duration: ") + e.duration()
                + Lang.t(" (weniger, wenn die Abfrage nur wenige Treffer hat)", " (less if the query has few results)") + "\n\n"
                + Lang.t("Eine laufende Abfrage lässt sich mit \"■ Abbrechen\" beenden.",
                         "A running query can be stopped with \"■ Cancel\"."));
        text.setWrapText(true);
        text.setPrefWidth(560);
        a.getDialogPane().setContent(text);
        Optional<ButtonType> r = a.showAndWait();
        if (r.isEmpty() || r.get() == cancel) return CANCEL;
        if (r.get() == run) return RUN;
        String[] range = askRange(owner, restrictRange);
        return range == null ? CANCEL : new Restrict(range[0], range[1]);
    }

    private static final DateTimeFormatter FULL = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** time range dialog (preset), or null when cancelled */
    private static String[] askRange(Window owner, String[] preset) {
        Dialog<String[]> d = new Dialog<>();
        if (owner != null) d.initOwner(owner);
        d.setTitle("Lighthouse");
        d.setHeaderText(Lang.t("Zeitraum der Abfrage", "Time range of the query"));
        DateTimeField from = new DateTimeField(), to = new DateTimeField();
        LocalDateTime a = parse(preset == null ? null : preset[0]), z = parse(preset == null ? null : preset[1]);
        from.setRange(a, z);
        to.setRange(a, z);
        from.setValue(a);
        to.setValue(z);
        GridPane g = new GridPane();
        g.setHgap(10);
        g.setVgap(8);
        g.setPadding(new Insets(12));
        g.addRow(0, new Label(Lang.t("Von:", "From:")), from);
        g.addRow(1, new Label(Lang.t("Bis:", "To:")), to);
        d.getDialogPane().setContent(g);
        ButtonType ok = new ButtonType(Lang.t("Ausführen", "Run"), ButtonBar.ButtonData.OK_DONE);
        d.getDialogPane().getButtonTypes().addAll(ok, ButtonType.CANCEL);
        d.setResultConverter(b -> b == ok ? new String[] { from.getText(), to.getText() } : null);
        Optional<String[]> r = d.showAndWait();
        if (r.isEmpty() || r.get()[0].isEmpty() || r.get()[1].isEmpty()) return null;
        return r.get();
    }

    private static LocalDateTime parse(String s) {
        if (s == null) return null;
        try {
            return LocalDateTime.parse(s.trim(), FULL);
        } catch (Exception e) {
            return null;
        }
    }

    // ------------------------------------------------------------- cancel

    /**
     * Stops the statements running on {@code c} (sqlite3_interrupt); they end
     * with an SQLException, the connection stays usable.
     */
    public static void interrupt(Connection c) {
        if (c == null) return;
        try {
            c.unwrap(org.sqlite.SQLiteConnection.class).getDatabase().interrupt();
        } catch (Exception ignored) {
            // closed or no SQLite connection
        }
    }

    /** true if {@code e} (or a cause) reports an interrupted statement */
    public static boolean isInterrupt(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            String m = t.getMessage();
            if (m != null && (m.contains("SQLITE_INTERRUPT") || m.contains("interrupted"))) return true;
        }
        return false;
    }

    /** shared cache for range counts of one database */
    public static Map<String, Long> newRangeCache() {
        return new ConcurrentHashMap<>();
    }
}
