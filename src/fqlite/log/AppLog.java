package fqlite.log;

import java.nio.file.FileSystems;
import java.util.Objects;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import fqlite.base.GUI;
import javafx.application.Application;
import javafx.collections.ObservableList;
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

/// This is the central Logger class for FQLite.
/// We are using <code>java.util.logging.Logger</code> functionality
/// to enable rotating logging.
/// A total of 3 log files are kept in the <user directory>/.fqlite folder.
/// The maximum size of each log file is 4 MB.
/// A text area is also updated with the log messages so that
/// the last entries in the log file can be viewed via the interface
/// if required.
///
/// @author pawlaszc
public class AppLog extends Application {

    public static final Logger LOGGER = Logger.getLogger(AppLog.class.getName());
    public static TextArea textArea;
    public static Scene myScene;
    
    static {
    	
        LOGGER.setUseParentHandlers(false);
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
            LOGGER.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();  
            fh.setFormatter(formatter);
            LOGGER.addHandler(new TextAreaHandler(textArea));
            LOGGER.severe("<FQLite message log>");


            ButtonBar buttonBar = new ButtonBar();
            buttonBar.setPadding( new Insets(10) );

            Button copyButton = new Button("Copy");

            String s = Objects.requireNonNull(GUI.class.getResource("/edit-copy_small.png")).toExternalForm();
            ImageView iv = new ImageView(s);
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
            s = Objects.requireNonNull(GUI.class.getResource("/analyzer-exit.png")).toExternalForm();
            iv = new ImageView(s);
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

    public static void debug(String log){
    	LOGGER.finest(log);
    }
    
    public static void info(String log){
    	LOGGER.info(log);
    }
   
    public static void warning(String log){
    	LOGGER.warning(log);
    }
    
    public static void error(String log){
    	LOGGER.severe(log);
    }
    
    public static void setLevel(Level newLevel){
    	LOGGER.setLevel(newLevel);
    }
    
    @Override
    public void start(Stage primaryStage) {
       
    	primaryStage.setTitle("FQLite - Log");
        primaryStage.setScene(myScene);
        primaryStage.show();

        
    }
}
