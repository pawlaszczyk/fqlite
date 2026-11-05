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
 */
public final class FQLiteDump {
    public static void main(String[] a) {
        try {
            dump(a);
        } catch (Throwable t) {
            // an uncaught exception would end main() while the (non-daemon) JavaFX thread
            // keeps the JVM alive -> the benchmark run hangs. Always terminate.
            t.printStackTrace();
            System.exit(1);
        }
    }

    static void dump(String[] a) throws Exception {
        File db = new File(a[0]);
        File base = Files.createTempDirectory("fqlite-dump-").toFile();
        GUI.baseDir = base;
        CountDownLatch l = new CountDownLatch(1);
        try { Platform.startup(l::countDown); } catch (IllegalStateException e) { l.countDown(); }
        l.await(30, TimeUnit.SECONDS);
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
        try (Writer w = new OutputStreamWriter(new FileOutputStream(a[1]), StandardCharsets.UTF_8)) {
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
        Platform.exit();
        System.exit(0);
    }
    static String esc(String s) {
        if (s == null) return "\\N";
        return s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r");
    }
}
