package fqlite.ui;

import java.io.File;
import javafx.geometry.HPos;
import javafx.geometry.VPos;
import javafx.scene.Node;

import javafx.scene.image.ImageView;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.web.WebEngine;
import javafx.scene.web.WebView;

public class SchemaBrowser extends Region {
	 
		private HBox toolBar = new HBox();
	 
	    final ImageView selectedImage = new ImageView();
	    final WebView browser = new WebView();
	    final WebEngine webEngine = browser.getEngine();
	 
	    public SchemaBrowser(String schema) {       
	        //apply the styles
	        getStyleClass().add("browser");
	        
	      //  try {
	        
	        	// OutputStream htmlfile= new FileOutputStream(new File("temp.html"));
	            // PrintStream printhtml = new PrintStream(htmlfile);
	            // printhtml.println(schema);
	        	// printhtml.close();
	       // }
	        //catch(Exception err)
	        //{ }
	        
	        // load the schema page        
	        File f = new File("temp.html");
            //webEngine.load(f.toURI().toString());
	        webEngine.loadContent(schema);
	        getChildren().add(browser); 
	       
	    }
	 
	    private Node createSpacer() {
	        Region spacer = new Region();
	        HBox.setHgrow(spacer, Priority.ALWAYS);
	        return spacer;
	    }
	 
	    @Override protected void layoutChildren() {
	        double w = getWidth();
	        double h = getHeight();
	        double tbHeight = toolBar.prefHeight(w);
	        layoutInArea(browser,0,0,w,h-tbHeight,0, HPos.CENTER, VPos.CENTER);
	        layoutInArea(toolBar,0,h-tbHeight,w,tbHeight,0,HPos.CENTER,VPos.CENTER);
	    }
	 
	    @Override protected double computePrefWidth(double height) {
	        return 750;
	    }
	 
	    @Override protected double computePrefHeight(double width) {
	        return 500;
	    }
	}

