package fqlite.log;

import java.nio.file.FileSystems;
import java.util.Objects;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import fqlite.base.GUI;
import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.image.ImageView;
import javafx.scene.input.ClipboardContent;
import javafx.scene.layout.VBox;
import javafx.scene.text.Font;
import javafx.stage.Stage;

/// Log window of the GUI mode and the log file in the .fqlite folder.
/// Split off from {@link AppLog} so that the logger itself needs no JavaFX
/// controls (headless mode, see {@code fqlite.headless}).
///
/// @author pawlaszc
public class AppLogWindow extends Application {

    public static TextArea textArea;
    public static Scene myScene;
    private static boolean initialised = false;

    /** creates the log window and the log file handler - called once by {@link AppLog} */
    static synchronized void setup() {
        if (initialised)
            return;
        initialised = true;
        try { 
        	VBox layout = new VBox();
            layout.setPadding(new Insets(5, 5, 5, 5));
            layout.setSpacing(10);
            layout.setAlignment(Pos.CENTER);

            textArea = new TextArea();
          	textArea.setEditable(false);
            textArea.setFont(Font.font("Monospaced", 13));
            textArea.setPrefHeight(550);
            
            // This block configures the logger with a handler and a formatter
			FileHandler fh = new FileHandler(GUI.baseDir.getAbsolutePath() + FileSystems.getDefault().getSeparator() + "fqlite.log",4096*1000,3,true);
            //System.out.println("Log File: " + fh.toString());
            AppLog.LOGGER.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();  
            fh.setFormatter(formatter);
            AppLog.LOGGER.addHandler(new TextAreaHandler(textArea));
            AppLog.LOGGER.severe("<FQLite message log>");

            ButtonBar buttonBar = new ButtonBar();
            buttonBar.setPadding( new Insets(10) );

            Button copyButton = new Button("Copy");

            String s = Objects.requireNonNull(GUI.class.getResource("/icon24_copy.png")).toExternalForm();
            ImageView iv = new ImageView(s);
            iv.setFitHeight(16);
            iv.setFitWidth(16);
            copyButton.setGraphic(iv);
            copyButton.setTooltip(new Tooltip("copy log to clipboard"));
            copyButton.setOnAction(new EventHandler<ActionEvent>() {

                @Override
                @SuppressWarnings("rawtypes")
                public void handle(ActionEvent event) {

                    StringBuilder sb = new StringBuilder();
                    final javafx.scene.input.Clipboard clipboard = javafx.scene.input.Clipboard.getSystemClipboard();
                    final ClipboardContent content = new ClipboardContent();
                    content.putString(textArea.getText());
                    clipboard.setContent(content);
                }
            });

            Button backButton = new Button("Back");
            s = Objects.requireNonNull(GUI.class.getResource("/icon32_exit.png")).toExternalForm();
            iv = new ImageView(s);
            iv.setFitWidth(16);
            iv.setFitHeight(16);
            backButton.setGraphic(iv);
            backButton.setTooltip(new Tooltip("Go back to main window"));

            ButtonBar.setButtonData(copyButton, ButtonBar.ButtonData.OK_DONE);
            ButtonBar.setButtonData(backButton, ButtonBar.ButtonData.CANCEL_CLOSE);
            buttonBar.getButtons().addAll(copyButton, backButton);

            backButton.setOnAction(
                    new EventHandler<ActionEvent>() {
                        @Override
                        public void handle(final ActionEvent e) {
                            // get a handle on the stage
                            Stage stage = (Stage) backButton.getScene().getWindow();
                            // do what you have to do
                            stage.close();
                        }
                    });

            layout.getChildren().addAll(textArea,buttonBar);

            myScene = new Scene(layout, 600, 400);
            
        } catch (Exception e) {  
            e.printStackTrace();  
        }
    }

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("FQLite - Log");
        primaryStage.setScene(myScene);
        primaryStage.show();
    }
}
