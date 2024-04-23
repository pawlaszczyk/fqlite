package fqlite.ui;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.FileSystems;

import javafx.geometry.HPos;
import javafx.geometry.VPos;
import javafx.scene.image.ImageView;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Region;
import javafx.scene.web.WebEngine;
import javafx.scene.web.WebView;

public class SchemaBrowser extends Region {
	 
		private HBox toolBar = new HBox();
	 
	    final ImageView selectedImage = new ImageView();
	    final WebView browser = new WebView();
	    WebEngine webEngine = browser.getEngine();
	   
	 
	    public SchemaBrowser(String schema) {       
	    	 webEngine.setJavaScriptEnabled(true);

	    	//apply the styles
	        getStyleClass().add("browser");
	        
	        try {
				File baseDir = new File(System.getProperty("user.home"), ".fqlite");
				String pfad = baseDir.getAbsolutePath() + FileSystems.getDefault().getSeparator() + "schema.html";  
	        
	        	OutputStream htmlfile= new FileOutputStream(new File(pfad));
	        	PrintStream printhtml = new PrintStream(htmlfile);
	        	printhtml.println(schema);
	        	printhtml.close();
	        
	        // load the schema page        
	        File f = new File(pfad);
	        webEngine.load(f.toURI().toString());
	        getChildren().add(browser); 
	        }
	        catch(Exception err)
	        { 
	        	System.out.println(err);
	        }
	        
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

