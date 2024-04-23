package fqlite.ui;


import java.awt.Desktop;
import java.io.IOException;
import java.net.URI;

import fqlite.base.GUI;
import fqlite.base.Global;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.ButtonType;
import javafx.scene.control.DialogPane;
import javafx.scene.control.Hyperlink;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.Background;
import javafx.scene.layout.BackgroundFill;
import javafx.scene.layout.CornerRadii;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;

/*
 ---------------
 AboutDialog.java
 ---------------
 (C) Copyright 2020.

 Original Author:  Dirk Pawlaszczyk
 Contributor(s):   -;


 Project Info:  http://www.hs-mittweida.de

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.

 Dieses Programm ist Freie Software: Sie können es unter den Bedingungen
 der GNU General Public License, wie von der Free Software Foundation,
 Version 3 der Lizenz oder (nach Ihrer Wahl) jeder neueren
 veröffentlichten Version, weiterverbreiten und/oder modifizieren.

 Dieses Programm wird in der Hoffnung, dass es nützlich sein wird, aber
 OHNE JEDE GEWÄHRLEISTUNG, bereitgestellt; sogar ohne die implizite
 Gewährleistung der MARKTFÄHIGKEIT oder EIGNUNG FÜR EINEN BESTIMMTEN ZWECK.
 Siehe die GNU General Public License für weitere Details.

 Sie sollten eine Kopie der GNU General Public License zusammen mit diesem
 Programm erhalten haben. Wenn nicht, siehe <http://www.gnu.org/licenses/>.

 */

/**
 * Brings up a simple About-FontDialog with application information.
 * 
 * @author Dirk Pawlaszczyk
 *
 */
public class AboutDialog extends javafx.scene.control.Dialog<Object>{

    javafx.scene.Node root;
    DialogPane dialogPane = getDialogPane();
    VBox content = new VBox();
    
    
	public AboutDialog(javafx.scene.Node rootelement) {
		
		root = rootelement;
      
		Image img = new Image(GUI.class.getResource("/fqlite_logo_small.png").toExternalForm());
	    ImageView view = new ImageView(img);
		
		dialogPane.setGraphic(view);
		
		dialogPane.getButtonTypes().addAll(ButtonType.OK);
	    
		createLayout();		
		  
		setTitle("About this Program");
		
		
		show();
	}

	private void createLayout() {

		dialogPane.setBackground(new Background(new BackgroundFill(Color.WHITE, new CornerRadii(5), Insets.EMPTY)));
		 
        this.setContentText("FQLite Retrieval Tool, Version " + Global.FQLITE_VERSION + "\n" 
        		+ "Author: Dirk Pawlaszczyk \n\n"
        		+ "Mittweida University of Applied Sciences\n"
        		+ "Germany\n"
        		+ "\u00A9 "+ Global.YEAR);
        
        
        Hyperlink link = new Hyperlink();
        link.setText("https://www.staff.hs-mittweida.de/~pawlaszc/fqlite/");
        link.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent e) {
            	
            	try {
                    if (Desktop.isDesktopSupported()) {
                        Desktop desktop = Desktop.getDesktop();
                        if (desktop.isSupported(Desktop.Action.BROWSE)) {
                            desktop.browse(URI.create("https://www.staff.hs-mittweida.de/~pawlaszc/fqlite/"));
                        }
                    }
                } catch (IOException | InternalError err) {
                    err.printStackTrace();
                }
            
            }
        });
        
	
		String license =  
				
				 "GNU GENERAL PUBLIC LICENSE \n Version 3, 29 June 2007 \n"
				+"----------------------------------------------------- \n"
				+"This program is free software:  you  can  redistribute  "
				+ "it and/or modify it under the terms of the GNU General "
				+ "Public License as published by the Free Software "
				+ "Foundation, either version 3 of the License, or (at your "
				+ "option) any later version. This program is distributed in "
				+ "without even the implied warranty of MERCHANTABILITY or "
				+ "FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General "
				+ "Public License for more details. You should have received "
				+ "a copy of the GNU General Public License along with this "
				+ "program.  If not, see <http://www.gnu.org/licenses/>.";
		
		
		Label label = new Label("License Information");

		TextArea textArea = new TextArea(license);
		textArea.setEditable(false);
		textArea.setWrapText(true);

		//textArea.setMaxWidth(Double.MAX_VALUE);
		//textArea.setMaxHeight(Double.MAX_VALUE);
		GridPane.setVgrow(textArea, Priority.ALWAYS);
		GridPane.setHgrow(textArea, Priority.ALWAYS);

		GridPane expContent = new GridPane();
		expContent.setMaxWidth(0.8);
		expContent.add(label, 0, 0);
		expContent.add(textArea, 0, 1);

		
		
		
		//content.getChildren().add(dialogPane);
		TextArea ta = new TextArea();
		ta.setText("FQLite Retrieval Tool, Version " + Global.FQLITE_VERSION + "\n" 
        		+ "Author: Dirk Pawlaszczyk \n\n"
        		+ "Mittweida University of Applied Sciences\n"
        		+ "Germany\n"
        		+ "\u00A9 "+ Global.YEAR
        		+ "\n"
        		+ "\n"
        		+ "FQLite for SQLite is bi-licensed under the Mozilla Public License Version 2, \n"
        		+ "as well as the GNU General Public License Version 3 or later. \n"
				+ "You can modify or redistribute it under the conditions of these licenses.\n"
				);
   
		
		Label lb = new Label("Find us on Web: ");
		
		
		content.getChildren().add(ta);
		
		HBox fl = new HBox();
		fl.setAlignment(Pos.CENTER);
		
		fl.getChildren().add(lb);
		fl.getChildren().add(link);
		
		content.getChildren().add(fl);
		
		getDialogPane().contentProperty().set(content);
		
		
		// Set expandable Exception into the dialog pane.
		getDialogPane().setExpandableContent(expContent);
    
		//final Window window = getDialogPane().getScene().getWindow();
	}
	
}


	
	

