package fqlite.viewer;

import fqlite.viewer.ui.BinViewer;
import javafx.application.Application;
import javafx.stage.Stage;

/**
 * Application entry point.
 * Run with:  mvn javafx:run
 * or after building fat-jar:  java -jar target/protobuf-viewer-1.0.0.jar
 */
public class Main extends Application {

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {
        new BinViewer(primaryStage);
    }
}
