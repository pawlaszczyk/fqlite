package fqlite.ui;

import com.dlsc.pdfviewfx.PDFView;

import fr.brouillard.oss.cssfx.CSSFX;
import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.stage.Screen;
import javafx.stage.Stage;

public class UserGuideWindow extends Application {

	    private PDFView pdfView = new PDFView();
		
	      
	    @Override
	    public void start(Stage primaryStage) {
	   
	    	try {
	             pdfView.load(getClass().getResourceAsStream("/FQLite_UserGuide.pdf"));
	        } catch (Exception e) {
	            e.printStackTrace();
	        }


	        VBox.setVgrow(pdfView, Priority.ALWAYS);
	        VBox box = new VBox(pdfView);
	    	
	        box.setFillWidth(true);

	        Scene scene = new Scene(box);

	        CSSFX.start(primaryStage);

	        primaryStage.setTitle("FQLite User Guide");
	        primaryStage.setWidth(Screen.getPrimary().getVisualBounds().getWidth()*0.6);
	        primaryStage.setHeight(Screen.getPrimary().getVisualBounds().getHeight()*0.7);
	        primaryStage.setScene(scene);
	        primaryStage.centerOnScreen();
	        primaryStage.setAlwaysOnTop(true);
	        primaryStage.show();
	    }
}