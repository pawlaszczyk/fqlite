package fqlite.ui;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Properties;
import java.util.logging.Level;

import fqlite.base.Global;
import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Label;
import javafx.scene.control.RadioButton;
import javafx.scene.control.ToggleGroup;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.scene.text.TextAlignment;
import javafx.stage.Stage;


public class SettingsDialog extends Application{

		final	RadioButton r1 = new RadioButton("don't export any BLOBs"); 
		final   RadioButton r2 = new RadioButton("export BLOB values to .csv"); 
		final   RadioButton r3 = new RadioButton("export BLOB values as separate files"); 
        final   ChoiceBox<String> loglevel = new ChoiceBox<String>();


    @Override
	    public void start(final Stage stage) {
	        stage.setTitle("FQLite Settings");
	 
	 
	        VBox fileexportproperties = new VBox();
	          
	        ToggleGroup toggleGroup = new ToggleGroup();
	        String cssLayout = "-fx-border-color: black;\n" +
	                   "-fx-border-insets: 20;\n" +
	        		   "    -fx-padding: 10;\n" +
	                   "    -fx-spacing: 10;\n" +
	                   "-fx-border-width: 2;\n";
	        fileexportproperties.setStyle(cssLayout);     
	        
	        
	        
	        r1.setToggleGroup(toggleGroup);
	        r2.setToggleGroup(toggleGroup);
	        r3.setToggleGroup(toggleGroup);
	        
	        fileexportproperties.getChildren().addAll(r1,r2,r3);
	    
	 	    if (Global.EXPORT_MODE == Global.EXPORT_MODES.DONTEXPORT)
	 	    	r1.setSelected(true);
	 	    else if (Global.EXPORT_MODE == Global.EXPORT_MODES.TOCSV)
	 	    	r2.setSelected(true);
	 	    else if (Global.EXPORT_MODE == Global.EXPORT_MODES.TOSEPARATEFILES)
	 	    	r3.setSelected(true);
	 	  
	 	    
	        final VBox rootGroup = new VBox();
	        javafx.scene.control.Label heading = new javafx.scene.control.Label("BLOB Settings");
	        heading.setTextAlignment(TextAlignment.CENTER);
	        heading.setFont(Font.font("Verdana", FontWeight.BOLD, 12));
	        
	        rootGroup.setPadding(new Insets(5, 5, 5, 5));
	        rootGroup.setSpacing(10);
	        rootGroup.setAlignment(Pos.CENTER);
	        rootGroup.getChildren().addAll(heading,fileexportproperties);

	        final VBox exportBox = new VBox();
	        exportBox.setPadding(new Insets(5, 5, 5, 5));
	        exportBox.setSpacing(10);
	        
	        javafx.scene.control.Label heading2 = new javafx.scene.control.Label("CSV Settings");
	        heading2.setFont(Font.font("Verdana", FontWeight.BOLD, 12));

	  
	        CheckBox exportTblHeader = new CheckBox("always write table header to .csv"); 
	        if (Global.EXPORTTABLEHEADER) 
	        	exportTblHeader.setSelected(true); 
	        else
	        	exportTblHeader.setSelected(false);
		       
	        
	        ChoiceBox<String> choiceBox = new ChoiceBox<String>();

	        choiceBox.getItems().add(",");
	        choiceBox.getItems().add(";");
	        choiceBox.getItems().add("[TAB]");
	        choiceBox.setTooltip(new Tooltip("Select a separator for .csv export"));
	        choiceBox.getSelectionModel().select(Global.CSV_SEPARATOR);
	        
	        exportBox.getChildren().add(exportTblHeader);
	        exportBox.setStyle(cssLayout);    
	        Label text = new Label("Separator: ");
	        
	        //Adding the choice box to the group
	        //Group newgrp = new Group(choiceBox,text);
	        HBox newgrp = new HBox(text, choiceBox);
	        newgrp.setPadding(new Insets(5, 5, 5, 0));
	        newgrp.setSpacing(10);
	       
	        exportBox.getChildren().add(newgrp);  
	        rootGroup.getChildren().addAll(heading2, exportBox);

            Label text2 = new Label("Level: ");
            javafx.scene.control.Label heading3 = new javafx.scene.control.Label("Log Settings");
            heading.setTextAlignment(TextAlignment.CENTER);
            heading3.setFont(Font.font("Verdana", FontWeight.BOLD, 12));
            loglevel.getItems().add(Level.SEVERE.toString());
            loglevel.getItems().add(Level.WARNING.toString());
            loglevel.getItems().add(Level.INFO.toString());
            loglevel.getItems().add(Level.FINEST.toString());
            loglevel.setTooltip(new Tooltip("Select a LOG Level"));
            loglevel.getSelectionModel().select(Global.LOGLEVEL.toString());

            HBox loggrp = new HBox(text2, loglevel);
            loggrp.setPadding(new Insets(5, 5, 5, 0));
            loggrp.setSpacing(10);
            loggrp.setStyle(cssLayout);


            //exportBox.getChildren().add(loggrp);
            rootGroup.getChildren().addAll(heading3, loggrp);

	        /**
	         * button bar definition.
	         * 
	         */
	        
	        ButtonBar buttonBar = new ButtonBar();
	        buttonBar.setPadding( new Insets(10) );

	        Button applyButton = new Button("Apply");
	        Button cancelButton = new Button("Cancel");

	        applyButton.setOnAction(
		            new EventHandler<ActionEvent>() {
		                @Override
		                public void handle(final ActionEvent e) {
		                   // read and set log-level

                            switch (loglevel.getValue()){

                                case "SEVERE": Global.LOGLEVEL = Level.SEVERE; break;
                                case "WARNING": Global.LOGLEVEL = Level.WARNING; break;
                                case "INFO": Global.LOGLEVEL = Level.INFO; break;
                                case "FINE": Global.LOGLEVEL = Level.FINE; break;

                            }

		                	if(r1.isSelected())
		                		Global.EXPORT_MODE = Global.EXPORT_MODES.DONTEXPORT;
		                	else if (r2.isSelected()) 
		                		Global.EXPORT_MODE = Global.EXPORT_MODES.TOCSV;
		                	else if (r3.isSelected()) 
			                		Global.EXPORT_MODE = Global.EXPORT_MODES.TOSEPARATEFILES;

		                	File baseDir = new File(System.getProperty("user.home"), ".fqlite");
		                	String path = baseDir.getAbsolutePath()+ File.separator + "fqlite.conf";
		            
		                	Properties appProps = new Properties();
		            		try {
		            			
		            			appProps.load(new FileInputStream(path));
		            	        appProps.setProperty("EXPORTMODE",Global.EXPORT_MODE.name());
		            	        Global.EXPORTTABLEHEADER = exportTblHeader.isSelected();
		            	        appProps.setProperty("EXPORT_THEADER", Global.EXPORTTABLEHEADER?"true": "false");
		            	        Global.CSV_SEPARATOR = choiceBox.getSelectionModel().getSelectedItem();
		            	        appProps.setProperty("CSV_SEPARATOR",Global.CSV_SEPARATOR);
		            	        appProps.setProperty("LOG-LEVEL",Global.LOGLEVEL.toString());

		            	        appProps.store(new FileOutputStream(path), null);

		            		} catch (Exception err) {
		            		
		            		}
		                		
		                	// get a handle on the stage
		                    Stage stage = (Stage) applyButton.getScene().getWindow();
		                    // do what you have to do
		                    stage.close();
		                }
		    });
	        
	        cancelButton.setOnAction(
		            new EventHandler<ActionEvent>() {
		                @Override
		                public void handle(final ActionEvent e) {
		                	// get a handle on the stage
		                    Stage stage = (Stage) applyButton.getScene().getWindow();
		                    // do what you have to do
		                    stage.close();
		                }
		    });
	        
	        
	        ButtonBar.setButtonData(applyButton, ButtonBar.ButtonData.APPLY);
	        ButtonBar.setButtonData(cancelButton, ButtonBar.ButtonData.CANCEL_CLOSE);
	        buttonBar.getButtons().addAll(applyButton, cancelButton);

	        
	  
	        rootGroup.getChildren().add(buttonBar);
	        
	        stage.setScene(new Scene(rootGroup,400,550));
	        stage.setAlwaysOnTop(true);
	        stage.show();

	    }

}
