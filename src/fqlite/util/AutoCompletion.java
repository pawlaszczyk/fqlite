package fqlite.util;

import javafx.application.Platform;
import javafx.geometry.Bounds;
import javafx.geometry.Point2D;
import javafx.scene.Node;
import javafx.scene.control.ListView;
import javafx.scene.control.TextArea;
import javafx.scene.input.KeyCode;
import javafx.stage.Popup;
import java.util.List;
import java.util.stream.Collectors;

/**
 *  Implements text AutoCompletion for TextAreas.
 *  It supports a user defined word list.
 *
 *  @author pawel
 */
public class AutoCompletion {


    /**
     * Appends autocompletion to any TextArea component.
     *
     * @param textArea  the target TextArea
     * @param wordList  the word list with completion words
     */
    public static void installAutoComplete(TextArea textArea, List<String> wordList) {

        Popup popup = new Popup();
        popup.setAutoHide(true);

        ListView<String> suggestionList = new ListView<>();
        suggestionList.setPrefWidth(250);
        suggestionList.setMaxHeight(150);
        suggestionList.setStyle(
                "-fx-background-color: white;" +
                "-fx-border-color: #aaa;" +
                "-fx-border-radius: 4;" +
                "-fx-background-radius: 4;"
        );

        popup.getContent().add(suggestionList);

        // Cache the caret node once the TextArea receives focus
        final Node[] caretNodeHolder = {null};
        textArea.focusedProperty().addListener((obs, wasFocused, isFocused) -> {
            if (isFocused && caretNodeHolder[0] == null) {
                Platform.runLater(() -> {
                    caretNodeHolder[0] = textArea.lookupAll(".caret")
                            .stream()
                            .findFirst()
                            .orElse(null);
                });
            }
        });

        // Typing → calculate suggestions
        textArea.textProperty().addListener((obs, oldText, newText) -> {
            String currentWord = getCurrentWord(textArea);

            if (currentWord.isEmpty()) {
                popup.hide();
                return;
            }

            List<String> matches = wordList.stream()
                    .filter(w -> w.toLowerCase().startsWith(currentWord.toLowerCase()))
                    .filter(w -> !w.equalsIgnoreCase(currentWord))
                    .collect(Collectors.toList());

            if (matches.isEmpty()) {
                popup.hide();
                return;
            }

            suggestionList.getItems().setAll(matches);
            suggestionList.setPrefHeight(Math.min(matches.size(), 5) * 26.0 + 2);

            // Position popup below the cursor
            Platform.runLater(() -> {
                Point2D caretPos = getCaretScreenPosition(textArea);

                if (caretPos != null) {
                    popup.show(textArea, caretPos.getX(), caretPos.getY() + 2);
                } else {
                    // Fallback: show popup below the TextArea
                    Bounds textAreaBounds = textArea.localToScreen(textArea.getBoundsInLocal());
                    if (textAreaBounds != null) {
                        popup.show(textArea,
                                textAreaBounds.getMinX() + 10,
                                textAreaBounds.getMaxY() + 2);
                    }
                }
            });
        });


        // Keyboard controls inside the TextArea
        textArea.setOnKeyPressed(e -> {
            if (!popup.isShowing()) return;

            if (e.getCode() == KeyCode.DOWN) {
                suggestionList.requestFocus();
                suggestionList.getSelectionModel().selectFirst();
                e.consume();
            } else if (e.getCode() == KeyCode.ESCAPE) {
                popup.hide();
                e.consume();
            }
        });

        // Accept selection via Enter or Tab
        suggestionList.setOnKeyPressed(e -> {
            if (e.getCode() == KeyCode.ENTER || e.getCode() == KeyCode.TAB) {
                applyCompletion(textArea, suggestionList.getSelectionModel().getSelectedItem());
                popup.hide();
                e.consume();
            } else if (e.getCode() == KeyCode.ESCAPE) {
                popup.hide();
                textArea.requestFocus();
                e.consume();
            }
        });

        // Accept selection via mouse click
        suggestionList.setOnMouseClicked(e -> {
            String selected = suggestionList.getSelectionModel().getSelectedItem();
            if (selected != null) {
                applyCompletion(textArea, selected);
                popup.hide();
                Platform.runLater(textArea::requestFocus);
            }
        });
    }


    /**
     * Return the word for the current position of the cursor.
     */
    private static String getCurrentWord(TextArea textArea) {
        int caret = textArea.getCaretPosition();
        String text = textArea.getText();
        if (text.isEmpty() || caret == 0) return "";

        int start = caret;
        while (start > 0 && start < text.length() && !Character.isWhitespace(text.charAt(start - 1))) {
            start--;
        }

        if (start >= text.length() || (caret >= text.length())) return "";

        return text.substring(start, caret);
    }


    /**
     * Replaces the actual word with the proposed word
     *
     */
    private static void applyCompletion(TextArea textArea, String word) {
        if (word == null) return;

        int caret = textArea.getCaretPosition();
        String text = textArea.getText();

        int start = caret;
        while (start > 0 && !Character.isWhitespace(text.charAt(start - 1))) {
            start--;
        }

        textArea.replaceText(start, caret, word);
        textArea.positionCaret(start + word.length());
    }


    private static Point2D getCaretScreenPosition(TextArea textArea) {
        int caretPos = textArea.getCaretPosition();
        String text = textArea.getText();

        // Count line and column of caret
        int line = 0;
        int col = 0;
        for (int i = 0; i < caretPos && i < text.length(); i++) {
            if (text.charAt(i) == '\n') {
                line++;
                col = 0;
            } else {
                col++;
            }
        }

        // Approximate character size via a temporary Text node
        javafx.scene.text.Text textNode = new javafx.scene.text.Text("W");
        textNode.setFont(textArea.getFont());
        new javafx.scene.Scene(new javafx.scene.layout.StackPane(textNode));
        double charWidth  = textNode.getLayoutBounds().getWidth();
        double lineHeight = textNode.getLayoutBounds().getHeight();

        // TextArea internal padding (approx.)
        double paddingTop  = 7;
        double paddingLeft = 7;

        // Pixel position relative to TextArea
        double localX = paddingLeft + col  * (charWidth/2.5);
        double localY = paddingTop  + line * lineHeight;

        // Convert to screen coordinates
        Bounds textAreaScreen = textArea.localToScreen(textArea.getBoundsInLocal());
        if (textAreaScreen == null) return null;

        return new Point2D(
                textAreaScreen.getMinX() + localX,
                textAreaScreen.getMinY() + localY + lineHeight
        );
    }


}
