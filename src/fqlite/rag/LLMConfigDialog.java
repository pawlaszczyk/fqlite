package fqlite.rag;

import fqlite.base.Global;
import fqlite.base.ThemeManager;
import javafx.application.Application;
import javafx.scene.control.*;
import javafx.stage.Stage;

import java.nio.file.Path;

/**
 *  This class realises a Property dialogue window.
 *  The LLM needs to be configured before we can use it.
 *
 *  <p>The controls are those of {@link LlmSettingsPane}, which is also the
 *  "LLM" tab of File › Settings…; both write {@link LlmSettings}.</p>
 *
 *  Original Author:  Dirk Pawlaszczyk
 *
 */
public class LLMConfigDialog extends Application {

    /** the configuration file ({@code ~/.fqlite/fqlitellm-config.properties}) */
    public static Path configPath = Global.llmconfig;

    @Override
    public void start(Stage primaryStage) {
        configPath = LlmSettings.file();

        Dialog<ButtonType> dialog = new Dialog<>();
        dialog.setTitle("LLM Configuration");
        dialog.setHeaderText("Configuration Settings for the local LLM (Text2SQL)");
        dialog.setResizable(true);

        LlmSettingsPane pane = new LlmSettingsPane();
        pane.setPrefWidth(560);
        dialog.getDialogPane().setContent(pane);

        ButtonType applyButton = new ButtonType("Apply", ButtonBar.ButtonData.OK_DONE);
        ButtonType cancelButton = new ButtonType("Cancel", ButtonBar.ButtonData.CANCEL_CLOSE);
        dialog.getDialogPane().getButtonTypes().addAll(applyButton, cancelButton);

        // Register with ThemeManager once the dialog's scene is available
        dialog.setOnShown(e ->
            ThemeManager.register(dialog.getDialogPane().getScene())
        );

        dialog.showAndWait().ifPresent(response -> {
            if (response == applyButton && pane.save()) {
                showConfirmation();
            }
        });

        primaryStage.close();
    }

    private void showConfirmation() {
        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle("Success");
        alert.setHeaderText(null);
        alert.setContentText("Configuration has been successfully saved to\n" + configPath);
        alert.showAndWait();
    }

    public static void main(String[] args) {
        launch(args);
    }
}
