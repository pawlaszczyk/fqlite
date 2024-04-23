package fqlite.ui;

import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.HPos;
import javafx.geometry.VPos;
import javafx.scene.control.Hyperlink;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Region;
import javafx.scene.web.WebEngine;
import javafx.scene.web.WebView;

public class Browser extends Region {
	   private HBox toolBar;
	 
	   private static String[] imageFiles = new String[]{
	        "product.png",
	        "blog.png",
	        "documentation.png",
	        "partners.png"
	    };
	    private static String[] captions = new String[]{
	        "UserGuide",
	        "Download",
	        "Github",
	    };
	 
	    private static String[] urls = new String[]{
	    	"https://www.staff.hs-mittweida.de/~pawlaszc/fqlite/userguide/",
	    	"https://www.staff.hs-mittweida.de/~pawlaszc/fqlite/",
	        "https://github.com/pawlaszczyk/fqlite"        
	    };
	 
	    final ImageView selectedImage = new ImageView();
	    final Hyperlink[] hpls = new Hyperlink[captions.length];
	    final Image[] images = new Image[imageFiles.length];
	    final WebView browser = new WebView();
	    final WebEngine webEngine = browser.getEngine();
	 
	    public Browser() {       
	        //apply the styles
	        getStyleClass().add("browser");
	        
	        for (int i = 0; i < captions.length; i++) {
	            final Hyperlink hpl = hpls[i] = new Hyperlink(captions[i]);
	           // Image image = images[i] =
	           //     new Image(getClass().getResourceAsStream(imageFiles[i]));
	           // hpl.setGraphic(new ImageView (image));
	            final String url = urls[i];
	 
	            hpl.setOnAction(new EventHandler<ActionEvent>() {
	                @Override
	                public void handle(ActionEvent e) {
	                    webEngine.load(url);   
	                }
	            });
	        }        
	 
	        // load the home page        
	        webEngine.load("https://www.staff.hs-mittweida.de/~pawlaszc/fqlite/userguide/");
	        
	     
	        // create the toolbar
	        toolBar = new HBox();
	        toolBar.getStyleClass().add("browser-toolbar");
	        toolBar.getChildren().addAll(hpls);        
	    
	        //add components
	        getChildren().add(toolBar);
	        getChildren().add(browser); 
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
