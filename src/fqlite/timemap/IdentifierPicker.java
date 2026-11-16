package fqlite.timemap;

import javafx.animation.PauseTransition;
import javafx.scene.control.ComboBox;
import javafx.util.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Makes a {@link ComboBox} usable for very many identifiers (IMSI, IMEI).
 * A combo box popup measures all of its items when it opens: with 200,000
 * IMSIs that blocks the application for seconds. This picker keeps the full
 * sorted list aside and puts at most {@link #MAX_SHOWN} entries into the combo
 * box. The combo box becomes editable: typing filters the full list (part of
 * the identifier is enough), Enter takes an exact or unique match.
 */
final class IdentifierPicker {

    /** entries shown in the popup at most */
    static final int MAX_SHOWN = 1000;

    private final ComboBox<String> combo;
    private final Supplier<String> noSelection;
    private final Consumer<String> onPick;
    private final PauseTransition pause = new PauseTransition(Duration.millis(250));
    /** all identifiers, sorted */
    private List<String> all = List.of();
    private boolean updating;
    /** last identifier handed to onPick, null if none is selected */
    private String picked;

    /**
     * @param combo       the combo box to manage
     * @param noSelection text of the "no selection" entry (first item)
     * @param onPick      called with the chosen identifier, or with the
     *                    "no selection" text / {@code null} when cleared
     */
    IdentifierPicker(ComboBox<String> combo, Supplier<String> noSelection, Consumer<String> onPick) {
        this.combo = combo;
        this.noSelection = noSelection;
        this.onPick = onPick;
        combo.setEditable(true);
        combo.setVisibleRowCount(15);
        combo.setOnAction(e -> onAction());
        pause.setOnFinished(e -> refilter(combo.getEditor().getText(), true));
        combo.getEditor().textProperty().addListener((o, was, now) -> {
            if (updating || !combo.getEditor().isFocused()) return;
            if (now != null && now.equals(combo.getValue())) return;
            pause.playFromStart();
        });
    }

    private static boolean isHint(String v) {
        return v != null && v.startsWith("… ");
    }

    private String hint(int more) {
        return String.format(Lang.t("… %,d weitere – Kennung eintippen zum Suchen",
                "… %,d more – type to search"), more);
    }

    /** Replaces the identifiers ({@code sorted}: ascending, without duplicates). */
    void setAll(List<String> sorted) {
        pause.stop();
        all = sorted == null ? List.of() : sorted;
        picked = null;
        refilter("", false);
    }

    /** true if {@code v} is one of the identifiers */
    boolean contains(String v) {
        return v != null && Collections.binarySearch(all, v) >= 0;
    }

    /** number of identifiers */
    int size() { return all.size(); }

    private List<String> matches(String text, int limit) {
        List<String> out = new ArrayList<>(Math.min(limit, all.size()));
        String t = text == null ? "" : text.trim().toLowerCase(Locale.ROOT);
        for (String s : all) {
            if (t.isEmpty() || s.toLowerCase(Locale.ROOT).contains(t)) {
                out.add(s);
                if (out.size() >= limit) break;
            }
        }
        return out;
    }

    private int countMatches(String text) {
        String t = text == null ? "" : text.trim().toLowerCase(Locale.ROOT);
        if (t.isEmpty()) return all.size();
        int n = 0;
        for (String s : all) if (s.toLowerCase(Locale.ROOT).contains(t)) n++;
        return n;
    }

    /** Puts the identifiers matching {@code text} (at most MAX_SHOWN) into the combo box. */
    private void refilter(String text, boolean showPopup) {
        List<String> shown = matches(text, MAX_SHOWN);
        List<String> items = new ArrayList<>(shown.size() + 2);
        items.add(noSelection.get());
        items.addAll(shown);
        if (shown.size() >= MAX_SHOWN) {
            int total = countMatches(text);
            if (total > shown.size()) items.add(hint(total - shown.size()));
        }
        String value = combo.getValue();
        int caret = combo.getEditor().getCaretPosition();
        updating = true;
        try {
            if (combo.isShowing()) combo.hide();   // the popup re-measures when shown again
            combo.getItems().setAll(items);
            if (showPopup) {
                // keep what is being typed (setting the items may reset the editor)
                combo.getEditor().setText(text);
                combo.getEditor().positionCaret(Math.min(caret, text == null ? 0 : text.length()));
            } else {
                combo.setValue(value);
            }
        } finally {
            updating = false;
        }
        if (showPopup && combo.getEditor().isFocused() && text != null && !text.isBlank()) combo.show();
    }

    private void onAction() {
        if (updating) return;
        String v = combo.getValue();
        if (isHint(v)) {
            // not an identifier: back to the text typed so far
            updating = true;
            try { combo.setValue(null); } finally { updating = false; }
            return;
        }
        if (v == null || v.isBlank()) {
            // the combo box empties its value when the list changes while
            // typing: only an emptied input field clears the selection
            // (and leaving the empty field commits "" - nothing to clear then)
            String typed = combo.getEditor().getText();
            if (typed != null && !typed.isBlank() || picked == null) return;
            picked = null;
            onPick.accept(v);
            return;
        }
        if (v.equals(noSelection.get())) {
            picked = null;
            onPick.accept(v);
            return;
        }
        String t = v.trim();
        if (contains(t)) {
            pause.stop();
            picked = t;
            onPick.accept(t);
            return;
        }
        // part of an identifier: take it if it is unique, otherwise show the matches
        List<String> m = matches(t, 2);
        if (m.size() == 1) {
            combo.setValue(m.get(0));   // fires onAction again with the full identifier
        } else if (!m.isEmpty()) {
            refilter(t, true);
        }
    }
}
