import fqlite.base.GUI;
import fqlite.base.Job;
import javafx.application.Platform;
import javafx.collections.ObservableList;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.*;

/**
 * Dumps EVERY row FQLite puts into Job.resultlist (live, deleted, freelist, unmatched)
 * as tab-separated lines:  bucket \t cell0 \t cell1 \t ...   (\t,\n,\\ escaped)
 * Scoring/filtering is done afterwards in score.py, so the emission policy of the
 * benchmark's HeadlessTap can be reproduced AND alternatives compared.
 * Usage: java FQLiteDump <db> <out.tsv>
 *        java FQLiteDump --startup                         (JVM + JavaFX start only, then exit)
 *        java FQLiteDump --timing <times.tsv> <outdir> <rounds> <db>...
 *            analyses all databases in ONE JVM, <rounds> times, and writes per database and
 *            round the analysis time (Job.run + journal + WAL + writing the dump) - this separates
 *            the per-process start-up cost from the actual scan time.
 */
public final class FQLiteDump {
    public static void main(String[] a) {
        try {
            if (a.length > 0 && a[0].equals("--startup")) startup(a);
            else if (a.length > 0 && a[0].equals("--timing")) timing(a);
            else dump(a);
        } catch (Throwable t) {
            // an uncaught exception would end main() while the (non-daemon) JavaFX thread
            // keeps the JVM alive -> the benchmark run hangs. Always terminate.
            t.printStackTrace();
            System.exit(1);
        }
    }

    /** starts JavaFX once (FQLite's Job reports via Platform.runLater) */
    static void startFx() throws Exception {
        File base = Files.createTempDirectory("fqlite-dump-").toFile();
        GUI.baseDir = base;
        CountDownLatch l = new CountDownLatch(1);
        try { Platform.startup(l::countDown); } catch (IllegalStateException e) { l.countDown(); }
        l.await(30, TimeUnit.SECONDS);
    }

    static void startup(String[] a) throws Exception {
        long jvmStart = java.lang.management.ManagementFactory.getRuntimeMXBean().getStartTime();
        long main = System.currentTimeMillis();
        long t = System.nanoTime();
        startFx();
        long fx = (System.nanoTime() - t) / 1_000_000;
        System.out.println("jvm_to_main_ms\t" + (main - jvmStart) + "\tjavafx_startup_ms\t" + fx);
        Platform.exit();
        System.exit(0);
    }

    static void timing(String[] a) throws Exception {
        long jvmStart = java.lang.management.ManagementFactory.getRuntimeMXBean().getStartTime();
        long main = System.currentTimeMillis();
        File times = new File(a[1]), outdir = new File(a[2]);
        int rounds = Integer.parseInt(a[3]);
        outdir.mkdirs();
        long t = System.nanoTime();
        startFx();
        long fx = (System.nanoTime() - t) / 1_000_000;
        ExecutorService ex = Executors.newSingleThreadExecutor(r -> { Thread th = new Thread(r); th.setDaemon(true); return th; });
        try (PrintWriter w = new PrintWriter(new OutputStreamWriter(new FileOutputStream(times), StandardCharsets.UTF_8))) {
            w.println("# jvm_to_main_ms=" + (main - jvmStart) + " javafx_startup_ms=" + fx);
            w.println("round\tdb\tbytes\trows\tms\tstatus");
            for (int r = 1; r <= rounds; r++) {
                for (int i = 4; i < a.length; i++) {
                    File db = new File(a[i]);
                    String n = db.getName().replaceAll("\\.(db|sqlite)$", "");
                    long bytes = db.length() + new File(a[i] + "-wal").length() + new File(a[i] + "-journal").length();
                    File out = new File(outdir, n + ".tsv");
                    long t0 = System.nanoTime();
                    String status = "ok";
                    Future<?> f = ex.submit(() -> { analyse(db, out); return null; });
                    try {
                        f.get(180, TimeUnit.SECONDS);
                    } catch (TimeoutException te) {
                        status = "timeout"; f.cancel(true);
                        ex.shutdownNow();
                        ex = Executors.newSingleThreadExecutor(rr -> { Thread th = new Thread(rr); th.setDaemon(true); return th; });
                    } catch (ExecutionException ee) {
                        status = "error:" + ee.getCause();
                    }
                    long ms = (System.nanoTime() - t0) / 1_000_000;
                    long rows = 0;
                    if (out.isFile()) try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(out), StandardCharsets.UTF_8))) {
                        while (br.readLine() != null) rows++;
                    }
                    w.println(r + "\t" + n + "\t" + bytes + "\t" + rows + "\t" + ms + "\t" + status.replace('\t', ' ').replace('\n', ' '));
                    w.flush();
                }
            }
        }
        Platform.exit();
        System.exit(0);
    }

    static void dump(String[] a) throws Exception {
        startFx();
        analyse(new File(a[0]), new File(a[1]));
        Platform.exit();
        System.exit(0);
    }

    /** one database (+ -journal / -wal next to it) -> dump file */
    static void analyse(File db, File outFile) throws Exception {
        PrintStream real = System.out;
        System.setOut(new PrintStream(OutputStream.nullOutputStream()));
        Job job = new Job();
        job.setPath(db.getAbsolutePath());
        File walFile = new File(db.getAbsolutePath() + "-wal");
        File rjFile = new File(db.getAbsolutePath() + "-journal");
        fqlite.base.WALReader wal = null;
        fqlite.base.RollbackJournalReader rol = null;
        try {
            if (walFile.isFile()) job.readWAL = true;          // same as GUI.java for a companion -wal
            if (rjFile.isFile() && rjFile.length() > 0) job.readRollbackJournal = true;
            job.run(db.getAbsolutePath());
            if (rjFile.isFile() && rjFile.length() > 0) {        // same as ImportDBTask
                rol = new fqlite.base.RollbackJournalReader(rjFile.getAbsolutePath(), job);
                rol.ps = job.ps;
                rol.parse();
                job.rol = rol;
            }
            if (walFile.isFile()) {                              // same as ImportDBTask
                wal = new fqlite.base.WALReader(walFile.getAbsolutePath(), job);
                job.wal = wal;
                wal.parse();                                     // fills wal.resultlist
                // wal.output() is not needed headless (it only writes a CSV into the cwd)
            }
        } finally { System.setOut(real); }
        try (Writer w = new OutputStreamWriter(new FileOutputStream(outFile), StandardCharsets.UTF_8)) {
            for (String t : job.resultlist.keySet()) {
                for (ObservableList<String> row : job.resultlist.get(t)) {
                    StringBuilder sb = new StringBuilder(esc(t));
                    if (row != null) for (String c : row) sb.append('\t').append(esc(c));
                    w.write(sb.append('\n').toString());
                }
            }
            if (rol != null) {                                   // journal records, bucket prefixed "RJ:"
                for (String t : rol.resultlist.keySet()) {
                    for (ObservableList<String> row : rol.resultlist.get(t)) {
                        StringBuilder sb = new StringBuilder(esc("RJ:" + t));
                        if (row != null) for (String c : row) sb.append('\t').append(esc(c));
                        w.write(sb.append('\n').toString());
                    }
                }
            }
            if (wal != null) {                                   // WAL records, bucket prefixed "WAL:"
                for (String t : wal.resultlist.keySet()) {
                    for (ObservableList<String> row : wal.resultlist.get(t)) {
                        StringBuilder sb = new StringBuilder(esc("WAL:" + t));
                        if (row != null) for (String c : row) sb.append('\t').append(esc(c));
                        w.write(sb.append('\n').toString());
                    }
                }
            }
        }
    }
    static String esc(String s) {
        if (s == null) return "\\N";
        return s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r");
    }
}
