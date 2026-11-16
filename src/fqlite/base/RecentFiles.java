package fqlite.base;

import fqlite.log.AppLog;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * List of the most recently opened databases for "File > Open Recent",
 * kept in {@code ~/.fqlite/recent.txt} (one line per entry:
 * {@code mode<TAB>path}). The mode tells how the file was opened: as a
 * database analysed by FQLite or directly in Lighthouse.
 */
public final class RecentFiles {

    /** how a recent file was opened */
    public enum Mode { DATABASE, LIGHTHOUSE }

    /** one entry of the list */
    public record Entry(Mode mode, File file) { }

    /** entries kept */
    public static final int MAX = 10;

    private RecentFiles() { }

    private static final List<Runnable> LISTENERS = new java.util.concurrent.CopyOnWriteArrayList<>();

    /** Called after every change of the list (e.g. to rebuild a menu). */
    public static void addListener(Runnable listener) {
        LISTENERS.add(listener);
    }

    private static File store() {
        return new File(new File(System.getProperty("user.home"), ".fqlite"), "recent.txt");
    }

    /** the entries, most recent first */
    public static synchronized List<Entry> list() {
        List<Entry> out = new ArrayList<>();
        File f = store();
        if (!f.isFile()) return out;
        try {
            for (String line : Files.readAllLines(f.toPath(), StandardCharsets.UTF_8)) {
                int tab = line.indexOf('\t');
                if (tab <= 0) continue;
                try {
                    out.add(new Entry(Mode.valueOf(line.substring(0, tab)), new File(line.substring(tab + 1))));
                } catch (IllegalArgumentException ignored) {
                    // unknown mode: skip the line
                }
                if (out.size() >= MAX) break;
            }
        } catch (IOException e) {
            AppLog.error("Recent files could not be read: " + e.getMessage());
        }
        return out;
    }

    /** Puts {@code file} at the top of the list (an older entry for the same file and mode is removed). */
    public static synchronized void add(Mode mode, File file) {
        if (file == null) return;
        File abs;
        try {
            abs = file.getCanonicalFile();
        } catch (IOException e) {
            abs = file.getAbsoluteFile();
        }
        List<Entry> entries = list();
        final File key = abs;
        entries.removeIf(e -> e.mode() == mode && e.file().equals(key));
        entries.add(0, new Entry(mode, abs));
        while (entries.size() > MAX) entries.remove(entries.size() - 1);
        write(entries);
    }

    /** Empties the list. */
    public static synchronized void clear() {
        write(List.of());
    }

    private static void write(List<Entry> entries) {
        File f = store();
        File dir = f.getParentFile();
        if (!dir.isDirectory() && !dir.mkdirs()) return;
        List<String> lines = new ArrayList<>();
        for (Entry e : entries) lines.add(e.mode().name() + "\t" + e.file().getPath());
        try {
            Files.write(f.toPath(), lines, StandardCharsets.UTF_8);
        } catch (IOException e) {
            AppLog.error("Recent files could not be written: " + e.getMessage());
        }
        for (Runnable l : LISTENERS) l.run();
    }
}
