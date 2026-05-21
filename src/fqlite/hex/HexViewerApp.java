package fqlite.hex;


import fqlite.base.ThemeManager;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.scene.Scene;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;
import javafx.stage.WindowEvent;
import javafx.util.Duration;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 *
 *
 */
public class HexViewerApp extends Application {

    public Stage stage;
    public static Stage parent;
    public HexViewerPane viewerPane;
    public boolean closed = false;

    public void toFront(){
        stage.toFront();
    }

    public void setVisible() {
        stage.show();
    }

    public void go2Position(long pos)
    {
        if(null != viewerPane)
            viewerPane.gotoOffset(pos);
    }

    public void loadNewHexFile(String filename){
        Path p = Paths.get(filename);
        String fname = p.getFileName().toString();
        viewerPane.loadFile(Paths.get(filename));
        stage.setTitle("Hex View <" + fname + ">");
        stage.setAlwaysOnTop(true);
    }

    @Override
    public void start(Stage primaryStage) {
        stage = primaryStage;
        viewerPane = new HexViewerPane();
        BorderPane root = new BorderPane(viewerPane);
        Scene scene = new Scene(root, 680, 600);
        fqlite.base.ThemeManager.register(scene);
        primaryStage.setTitle("Hex-View");
        primaryStage.setMinWidth(680);
        primaryStage.setMinHeight(600);
        primaryStage.setScene(scene);
        primaryStage.setOnCloseRequest(e -> ThemeManager.unregister(scene));

        primaryStage.addEventHandler(WindowEvent.WINDOW_SHOWN, e -> {
            new Timeline(new KeyFrame(Duration.millis(50), ev -> {
                primaryStage.setMaximized(false);
                primaryStage.setWidth(680);
                primaryStage.setHeight(600);
                primaryStage.centerOnScreen();
            })).play();
        });


        primaryStage.show();


    }

    public static void main(String[] args) {
        launch(args);
    }

    public void clear(){}
}
