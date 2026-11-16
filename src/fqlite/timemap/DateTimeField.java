package fqlite.timemap;

import javafx.geometry.Pos;
import javafx.scene.control.DateCell;
import javafx.scene.control.DatePicker;
import javafx.scene.control.Label;
import javafx.scene.control.Spinner;
import javafx.scene.control.SpinnerValueFactory;
import javafx.scene.layout.HBox;
import javafx.scene.text.Font;
import javafx.util.StringConverter;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Date and time input without typing: a calendar (date) and three spinners
 * (hour, minute, second). The text value has the format
 * {@code yyyy-MM-dd HH:mm:ss} used by the analyses. The date can still be
 * typed into the calendar field; days outside the range of the data (see
 * {@link #setRange}) are greyed out in the calendar but not forbidden.
 */
public final class DateTimeField extends HBox {

    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter FULL = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final DatePicker date = new DatePicker();
    private final Spinner<Integer> hour = spinner(23);
    private final Spinner<Integer> minute = spinner(59);
    private final Spinner<Integer> second = spinner(59);
    private LocalDate min, max;

    public DateTimeField() {
        super(3);
        setAlignment(Pos.CENTER_LEFT);
        date.setPrefWidth(135);
        date.setConverter(new StringConverter<>() {
            @Override public String toString(LocalDate d) { return d == null ? "" : DATE.format(d); }
            @Override public LocalDate fromString(String s) {
                if (s == null || s.isBlank()) return null;
                try { return LocalDate.parse(s.trim(), DATE); } catch (DateTimeParseException e) { return date.getValue(); }
            }
        });
        date.setDayCellFactory(p -> new DateCell() {
            @Override
            public void updateItem(LocalDate d, boolean empty) {
                super.updateItem(d, empty);
                boolean outside = d != null && (min != null && d.isBefore(min) || max != null && d.isAfter(max));
                setStyle(outside ? "-fx-opacity: 0.4;" : "");
            }
        });
        // a typed date counts when the field loses focus, not only on Enter
        date.getEditor().focusedProperty().addListener((o, was, now) -> {
            if (!now) date.setValue(date.getConverter().fromString(date.getEditor().getText()));
        });
        Label c1 = new Label(":"), c2 = new Label(":");
        getChildren().addAll(date, hour, c1, minute, c2, second);
    }

    private static Spinner<Integer> spinner(int maxValue) {
        Spinner<Integer> s = new Spinner<>();
        SpinnerValueFactory.IntegerSpinnerValueFactory f = new SpinnerValueFactory.IntegerSpinnerValueFactory(0, maxValue, 0);
        f.setWrapAround(true);
        f.setConverter(new StringConverter<>() {
            @Override public String toString(Integer v) { return v == null ? "00" : String.format("%02d", v); }
            @Override public Integer fromString(String t) {
                try {
                    int v = Integer.parseInt(t.trim());
                    return Math.max(0, Math.min(maxValue, v));
                } catch (NumberFormatException e) {
                    return f.getValue();
                }
            }
        });
        s.setValueFactory(f);
        s.setEditable(true);
        s.setPrefWidth(76);
        s.setMinWidth(76);
        s.getEditor().setPrefColumnCount(2);
        // typed values count when the field loses focus, not only on Enter
        s.getEditor().focusedProperty().addListener((o, was, now) -> {
            if (!now) s.getValueFactory().setValue(f.getConverter().fromString(s.getEditor().getText()));
        });
        return s;
    }

    /** Range of the data: marks the days outside in the calendar. */
    public void setRange(LocalDateTime from, LocalDateTime to) {
        min = from == null ? null : from.toLocalDate();
        max = to == null ? null : to.toLocalDate();
    }

    /** Sets date and time ({@code null} clears the date). */
    public void setValue(LocalDateTime v) {
        date.setValue(v == null ? null : v.toLocalDate());
        hour.getValueFactory().setValue(v == null ? 0 : v.getHour());
        minute.getValueFactory().setValue(v == null ? 0 : v.getMinute());
        second.getValueFactory().setValue(v == null ? 0 : v.getSecond());
    }

    /** true if no date is set */
    public boolean isEmpty() { return date.getValue() == null; }

    /** {@code yyyy-MM-dd HH:mm:ss}, or "" without a date */
    public String getText() {
        commit();
        LocalDate d = date.getValue();
        if (d == null) return "";
        return FULL.format(d.atTime(hour.getValue(), minute.getValue(), second.getValue()));
    }

    /** takes over values still being typed */
    private void commit() {
        date.setValue(date.getConverter().fromString(date.getEditor().getText()));
        for (Spinner<Integer> s : java.util.List.of(hour, minute, second)) {
            @SuppressWarnings("unchecked")
            SpinnerValueFactory.IntegerSpinnerValueFactory f = (SpinnerValueFactory.IntegerSpinnerValueFactory) s.getValueFactory();
            f.setValue(f.getConverter().fromString(s.getEditor().getText()));
        }
    }

    public void setPromptText(String text) { date.setPromptText(text); }

    public void setFieldFont(Font f) {
        date.getEditor().setFont(f);
        hour.getEditor().setFont(f);
        minute.getEditor().setFont(f);
        second.getEditor().setFont(f);
    }

    /** style of the input fields (theme) */
    public void setFieldStyle(String css) {
        date.getEditor().setStyle(css);
        hour.getEditor().setStyle(css);
        minute.getEditor().setStyle(css);
        second.getEditor().setStyle(css);
    }

    /** colour of the ':' separators */
    public void setLabelColor(javafx.scene.paint.Color c) {
        for (javafx.scene.Node n : getChildren())
            if (n instanceof Label l) l.setTextFill(c);
    }
}
