package fqlite.fts;

import fqlite.base.GUI;
import fqlite.base.ThemeManager;
import javafx.animation.PauseTransition;
import javafx.application.Platform;
import javafx.collections.*;
import javafx.geometry.Insets;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.stage.*;
import javafx.util.Duration;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;


public class SearchDialog extends Dialog<Void> {

    private TextField searchField;
    private final Consumer<SearchResult> onSelect;

    private final ConcurrentHashMap<String, ObservableList<ObservableList<String>>> data;
    private final ObservableList<SearchResult> results = FXCollections.observableArrayList();
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private String filename;

    // remember last search term
    private static String lastSearchTerm = "";

    public SearchDialog(GUI parent, String filename, ConcurrentHashMap<String, ObservableList<ObservableList<String>>> data, Consumer<SearchResult> onSelect) {
        this.onSelect = onSelect;
        this.data = data;
        this.filename = filename;
        setTitle("Full-text Search [" + filename + "]");
        setHeaderText(null);
        setResizable(true);
        getDialogPane().setContent(buildContent());
        getDialogPane().getButtonTypes().add(ButtonType.CLOSE);
        getDialogPane().setPrefWidth(580);
        setOnCloseRequest(e -> executor.shutdownNow());
        setOnShown(e -> {
            ThemeManager.register(getDialogPane().getScene());
            searchField.requestFocus();
            // set cursor to the end of the search field in case there was a former run
            searchField.end();
        });
    }

    private VBox buildContent() {
        // --- Suchfeld ---
        searchField = new TextField();
        searchField.setStyle("-fx-prompt-text-fill: gray;");

        Label countLabel = new Label("0 matches");
        countLabel.setStyle("-fx-text-fill: gray; -fx-font-size: 12px;");

        HBox searchBar = new HBox(8, searchField, countLabel);
        HBox.setHgrow(searchField, Priority.ALWAYS);
        searchField.setPromptText("Enter word to search...");
        searchBar.setAlignment(javafx.geometry.Pos.CENTER_LEFT);

        // --- Ergebnisliste ---
        ListView<SearchResult> listView = new ListView<>(results);
        listView.setPrefHeight(340);
        listView.setCellFactory(lv -> new ListCell<>() {
            @Override
            protected void updateItem(SearchResult item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setText(null);
                    setGraphic(null);
                } else {
                    Label cell = new Label("\"" + item.cellValue() + "\"");
                    cell.setStyle("-fx-font-weight: bold;");

                    Label meta = new Label(
                            "table: " + item.tableName() +
                            "   row: " + (item.rowIndex()+1) +
                            "   column: " + (item.colIndex())
                    );
                    meta.setStyle("-fx-text-fill: grey; -fx-font-size: 12px;");
                    VBox box = new VBox(2, cell, meta);
                    box.setPadding(new Insets(4, 0, 4, 0));
                    setGraphic(box);
                    setText(null);
                }
            }
        });

        // Einfachklick
        listView.setOnMouseClicked(event -> {
            if (event.getClickCount() == 1) {
                fireSelection(listView.getSelectionModel().getSelectedItem());
            }
        });

        // Enter-Taste
        listView.setOnKeyPressed(event -> {
            if (event.getCode() == javafx.scene.input.KeyCode.ENTER) {
                fireSelection(listView.getSelectionModel().getSelectedItem());
            }
        });

        // --- Live-Search with Debounce ---
        searchField.textProperty().addListener((obs, oldVal, newVal) -> {
            lastSearchTerm = newVal.trim(); // remember last search word
            String term = newVal.trim();
            results.clear();
            countLabel.setText("Searching...");
            executor.shutdownNow();
            executor = Executors.newSingleThreadExecutor();
            executor.submit(() -> search(term, countLabel));
        });

        // restore last search term - trigger new search automatically
        // via textProperty-Listener
        if (!lastSearchTerm.isEmpty()) {
            searchField.setText(lastSearchTerm);
        }

        VBox layout = new VBox(10, searchBar, listView);
        layout.setPadding(new Insets(12));
        return layout;
    }

    private void search(String term, Label countLabel) {
        if (term.isEmpty()) {
            Platform.runLater(() -> countLabel.setText("0 matches"));
            return;
        }

        String lowerTerm = term.toLowerCase(Locale.ROOT);
        List<SearchResult> found = new ArrayList<>();

        for (Map.Entry<String, ObservableList<ObservableList<String>>> entry : data.entrySet()) {
            String tableName = entry.getKey();
            ObservableList<ObservableList<String>> rows = entry.getValue();

            for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
                ObservableList<String> row = rows.get(rowIdx);
                for (int colIdx = 0; colIdx < row.size(); colIdx++) {
                    String cell = row.get(colIdx);
                    if (cell != null && cell.toLowerCase(Locale.ROOT).contains(lowerTerm)) {
                        found.add(new SearchResult(tableName, rowIdx, colIdx, cell));
                    }
                    if (Thread.currentThread().isInterrupted()) return;
                }
            }
        }

        Platform.runLater(() -> {
            results.setAll(found);
            countLabel.setText(found.size() + " match" + (found.size() == 1 ? "" : "es"));
        });
    }

    private void fireSelection(SearchResult result) {
        if (result != null && onSelect != null) {
            close();
            PauseTransition pause = new PauseTransition(Duration.millis(300));
            pause.setOnFinished(e -> onSelect.accept(result));
            pause.play();
        }
    }

}
