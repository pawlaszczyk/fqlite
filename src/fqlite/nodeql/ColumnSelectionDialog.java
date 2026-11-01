package fqlite.nodeql;

import javafx.beans.property.BooleanProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.TextField;
import javafx.scene.control.cell.CheckBoxListCell;
import javafx.scene.layout.VBox;
import javafx.stage.Modality;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/** An explicit, searchable multi-picker; editing is committed only with Apply. */
final class ColumnSelectionDialog extends Dialog<List<SchemaInput.Suggestion>> {
    private record Choice(SchemaInput.Suggestion suggestion, boolean custom, BooleanProperty selected) {
        @Override public String toString() { return (custom ? "Custom SQL: " : "") + suggestion.label(); }
    }

    private final ObservableList<Choice> choices = FXCollections.observableArrayList();
    private final FilteredList<Choice> filteredChoices = new FilteredList<>(choices);
    private final LinkedHashSet<Choice> selectionOrder = new LinkedHashSet<>();
    private final TextField filter = new TextField();
    private final Button clear = new Button("Clear selection");

    ColumnSelectionDialog(List<SchemaInput.Suggestion> available, List<String> existing) {
        setTitle("Select columns");
        setHeaderText("Check multiple columns, then Apply");
        initModality(Modality.WINDOW_MODAL);
        setResizable(true);
        getDialogPane().getButtonTypes().addAll(ButtonType.APPLY, ButtonType.CANCEL);

        for (String sql : existing) {
            String key = ColumnSuggestions.referenceKey(sql);
            SchemaInput.Suggestion suggestion = key == null ? null : available.stream()
                    .filter(entry -> Objects.equals(key, ColumnSuggestions.referenceKey(entry.sql()))).findFirst().orElse(null);
            boolean custom = suggestion == null;
            if (custom) suggestion = new SchemaInput.Suggestion(sql, sql);
            SchemaInput.Suggestion value = suggestion;
            if (choices.stream().noneMatch(choice -> choice.suggestion().sql().equals(value.sql()))) {
                choices.add(new Choice(value, custom, new SimpleBooleanProperty(true)));
            }
        }
        for (SchemaInput.Suggestion entry : available) {
            if (choices.stream().noneMatch(choice -> choice.suggestion().sql().equals(entry.sql()))) {
                choices.add(new Choice(entry, false, new SimpleBooleanProperty(false)));
            }
        }
        choices.stream().filter(choice -> choice.selected().get()).forEach(selectionOrder::add);
        Button apply = (Button) getDialogPane().lookupButton(ButtonType.APPLY);
        Runnable updateCount = () -> {
            apply.setText("Apply (" + selectionOrder.size() + ")");
            apply.setDisable(selectionOrder.isEmpty());
        };
        for (Choice choice : choices) {
            choice.selected().addListener((observable, wasSelected, selected) -> {
                if (selected) {
                    selectionOrder.add(choice);
                    if (!choice.custom()) {
                        for (Choice other : choices) {
                            if (other != choice && !other.custom() && (wildcard(choice) || wildcard(other))) {
                                other.selected().set(false);
                            }
                        }
                    }
                } else selectionOrder.remove(choice);
                updateCount.run();
            });
        }
        updateCount.run();
        filter.setPromptText("Search columns");
        filter.setStyle("-fx-prompt-text-fill: -fqlite-muted;");
        filter.textProperty().addListener((observable, old, text) -> filteredChoices.setPredicate(choice ->
                choice.toString().toLowerCase(Locale.ROOT).contains(text.strip().toLowerCase(Locale.ROOT))));
        ListView<Choice> list = new ListView<>(filteredChoices);
        list.setCellFactory(CheckBoxListCell.forListView(Choice::selected));
        list.setPrefHeight(320);
        list.setPlaceholder(new Label("No matching columns"));
        clear.setOnAction(event -> choices.forEach(choice -> choice.selected().set(false)));
        Label help = new Label("Selections survive filtering. Existing custom SQL can be kept or unchecked.");
        help.setWrapText(true);
        VBox content = new VBox(10, help, filter, list, clear);
        content.setPadding(new Insets(8));
        content.setPrefWidth(460);
        getDialogPane().setContent(content);
        setResultConverter(button -> button == ButtonType.APPLY
                ? selectionOrder.stream().map(Choice::suggestion).toList() : null);
    }

    private static boolean wildcard(Choice choice) {
        String sql = choice.suggestion().sql();
        return sql.equals("*") || sql.endsWith(".*");
    }
}
