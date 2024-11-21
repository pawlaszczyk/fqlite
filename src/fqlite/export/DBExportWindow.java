package fqlite.export;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import fqlite.base.GUI;
import fqlite.ui.FQTableView;
import javafx.application.Application;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.ToolBar;
import javafx.scene.control.Tooltip;
import javafx.scene.image.ImageView;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.stage.Screen;
import javafx.stage.Stage;

public class DBExportWindow extends Application{

	List<String> dbnames; 
	final ComboBox<String> dbBox = new ComboBox<>();
	GUI app;  
	VBox root = new VBox();
	public Hashtable<String,ObservableList<ObservableList<String>>> datasets;
	public TextArea log;
	
	public DBExportWindow(GUI app){

	    	this.app = app;
	    	this.dbnames = app.dbnames;
	    	this.datasets = app.datasets;
	    	log = new TextArea();
	}
	
	@Override
	public void start(Stage primaryStage) throws Exception {

		
		   ToolBar toolBar = new ToolBar();

	        Button btnGo = new Button();
	        String s = GUI.class.getResource("/start.png").toExternalForm();
			ImageView iv = new ImageView(s);
			btnGo.setGraphic(iv);
			btnGo.setTooltip(new Tooltip("click her to start databse export"));
			btnGo.setOnAction(new EventHandler<ActionEvent>() {
	 
	            @Override
	            public void handle(ActionEvent event) {
	            	
	            	Enumeration<String> keys =   datasets.keys();

        			String dbname = dbBox.getSelectionModel().getSelectedItem();
	            	String signature = "Databases/" + dbname + "/";
	            	
	            	log.appendText("Start export... \n");
	            	
	            	DBExporter exp = new DBExporter(dbname + "_saved.sqlite");
	    	            		
	            	while(keys.hasMoreElements()){
	            		
	            		String next = keys.nextElement();
	           
	            		if(next.startsWith(signature)){
	            			log.appendText(next + "\n");
	            		
	            			     // we've found a table which belongs to this database
	            				 
	            				String tbname = next.substring(signature.length());
	            				log.appendText(tbname + "\n");
	            				app.tables.get(next);
	            				
	            				Node jnd = app.tables.get(next);

								if (jnd instanceof VBox) {

									VBox jvb = (VBox) jnd;
									FQTableView jtbl = (FQTableView) jvb.getChildren().get(1);
								
									List<String> columns = jtbl.columns;
									List<String> types = jtbl.columntypes;
									exp.createTable(tbname, columns, types);
								}

	            		}
	            		
	            		
	            	}
	            	
	            	
	            	//String result = p.parse(dbBox.getSelectionModel().getSelectedItem(),app,primaryStage,resultview,statusline);
	            	//Platform.runLater( () -> {});
	            }
	        });
			
		    Button btnExit = new Button();
	        s = GUI.class.getResource("/analyzer-exit.png").toExternalForm();
			iv = new ImageView(s);
			btnExit.setGraphic(iv);
			btnExit.setTooltip(new Tooltip("Quit SQL Analyzer"));
			btnExit.setOnAction(new EventHandler<ActionEvent>() {
	 
	            @Override
	            public void handle(ActionEvent event) {
	            	primaryStage.close();
	            }
	        });
			
			toolBar.getItems().addAll(btnGo, btnExit);

		    
	
			dbBox.getItems().addAll(dbnames);
		        
		    Label dblabel = new Label("Choose database: ");
		    dbBox.getSelectionModel().selectFirst();   
		    
		    ToolBar dbbar = new ToolBar();

	        dbbar.getItems().addAll(dblabel,dbBox);
	        
	     
		    VBox.setVgrow(root,Priority.ALWAYS);
			root.getChildren().addAll(dbbar,toolBar,log);
			
			Scene scene = new Scene(root,Screen.getPrimary().getVisualBounds().getWidth()*0.4,Screen.getPrimary().getVisualBounds().getHeight()*0.5);
	        
	        primaryStage.setScene(scene);
	        primaryStage.sizeToScene();
	        primaryStage.show();
	        primaryStage.setAlwaysOnTop(true);
	        
	}

}
