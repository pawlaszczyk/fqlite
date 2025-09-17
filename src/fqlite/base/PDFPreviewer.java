package fqlite.base;

import com.dlsc.pdfviewfx.PDFView;
import fr.brouillard.oss.cssfx.CSSFX;
import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import java.io.File;

public class PDFPreviewer extends Application {

	    private String filename;
	    private PDFView pdfView = new PDFView();
	 	
	    public PDFPreviewer(String filename){
	    	this.filename = filename;
	 	    pdfView.load(new File(this.filename));
	    }
	 
	   
	    @Override
	    public void start(Stage primaryStage) {
	        VBox.setVgrow(pdfView, Priority.ALWAYS);
	        VBox box = new VBox(pdfView);
	        box.setFillWidth(true);
	        Scene scene = new Scene(box);
	        CSSFX.start(primaryStage);
	        primaryStage.setTitle("PDF Preview");
	        primaryStage.setWidth(800);
	        primaryStage.setHeight(600);
	        primaryStage.setScene(scene);
	        primaryStage.centerOnScreen();
	        primaryStage.show();
	    }
}
	
	

