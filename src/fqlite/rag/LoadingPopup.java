package fqlite.rag;

import javafx.application.Platform;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.layout.VBox;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.StageStyle;

/**
 *  During download process of the HuggingFace Model this
 *  dialog is shown.
 */
public class LoadingPopup {

    private Stage popupStage;

    /**
     * Show an animated popup window.
     *
     * @param owner The main window
     * @param message The message to show during download
     */
    public void show(Stage owner, String message) {
        Platform.runLater(() -> {
            popupStage = new Stage();

            popupStage.initModality(Modality.APPLICATION_MODAL);
            popupStage.initStyle(StageStyle.UNDECORATED);
            if (owner != null) {
                popupStage.initOwner(owner);
            }

            // create layout
            VBox vbox = new VBox(15);
            vbox.setAlignment(Pos.CENTER);
            vbox.setStyle("-fx-background-color: white; -fx-padding: 30; -fx-border-color: #ccc; -fx-border-width: 1;");

            // hour clock animator (ProgressIndicator)
            ProgressIndicator progressIndicator = new ProgressIndicator();
            progressIndicator.setPrefSize(60, 60);

            // message
            Label label = new Label(message);
            label.setStyle("-fx-font-size: 14px;");

            vbox.getChildren().addAll(progressIndicator, label);

            Scene scene = new Scene(vbox);
            popupStage.setScene(scene);
            popupStage.show();
        });
    }

    /**
     * Close the popup.
     */
    public void close() {
        Platform.runLater(() -> {
            if (popupStage != null) {
                popupStage.close();
            }
        });
    }
}
