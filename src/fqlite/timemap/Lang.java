package fqlite.timemap;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Tiny UI-language helper for the Lighthouse window.
 * <p>
 * Most of the Lighthouse UI text was always written in German, but a
 * handful of labels/buttons/tooltips were left in English, leaving the
 * window visibly mixed-language. This picks German when the JVM's default
 * locale is German and English otherwise as a starting point, and every
 * affected call site goes through {@link #t}.
 * <p>
 * On top of that automatic default, {@link #setGerman(boolean)} lets the
 * window offer an explicit German/English switch at runtime (see the
 * "DE | EN" toggle in {@code MapViewPane}'s title bar). Widgets whose text
 * depends on the language register a refresh callback via
 * {@link #addListener(Runnable)}; every call to {@link #setGerman} re-runs
 * all of them so already-visible text updates immediately, without
 * requiring the window to be reopened.
 * <p>
 * This intentionally does not attempt to externalise every string in the
 * window into a resource bundle — most of it was already consistently
 * German and complaint-free; this only covers the handful of call sites
 * that were actually mixed (or otherwise chose to opt into the switch).
 */
final class Lang {

    private Lang() {}

    /**
     * True while German text should be shown. Starts out based on the JVM's
     * default locale (any German-speaking region — {@code de}, {@code de_DE},
     * {@code de_AT}, {@code de_CH}, …) and can be changed at runtime via
     * {@link #setGerman(boolean)}.
     */
    private static volatile boolean german =
            Locale.getDefault().getLanguage().equalsIgnoreCase("de");

    /** Callbacks that re-apply {@link #t} to already-created widgets. */
    private static final List<Runnable> listeners = new ArrayList<>();

    static boolean isGerman() {
        return german;
    }

    /**
     * Switches the active language and immediately notifies every
     * registered listener so visible text updates without reopening the
     * window. A listener that throws is logged and skipped rather than
     * aborting the remaining ones.
     */
    static void setGerman(boolean newGerman) {
        if (german == newGerman) return;
        german = newGerman;
        for (Runnable r : List.copyOf(listeners)) {
            try {
                r.run();
            } catch (Exception e) {
                System.out.println("⚠️ Lang-Listener fehlgeschlagen: " + e.getMessage());
            }
        }
    }

    /** Flips between German and English (used by the "DE | EN" toggle). */
    static void toggleGerman() {
        setGerman(!german);
    }

    /**
     * Registers a callback to run whenever the language changes, so a
     * widget created with {@link #t} can refresh its text. Not removable —
     * all call sites so far live for the lifetime of the window.
     */
    static void addListener(Runnable onLanguageChanged) {
        listeners.add(onLanguageChanged);
    }

    /**
     * Returns {@code de} if German is currently active, otherwise
     * {@code en}. Both arguments are required (no implicit fallback), so a
     * call site always makes clear what the English text should be.
     */
    static String t(String de, String en) {
        return german ? de : en;
    }
}
