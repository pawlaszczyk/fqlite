package fqlite.base;

import java.awt.AWTException;
import java.awt.Font;
import java.awt.SystemTray;
import java.awt.Taskbar;
import java.awt.TrayIcon;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import javax.swing.ImageIcon;
import javax.swing.UIManager;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreePath;

import org.apache.commons.lang3.StringUtils;

import fqlite.analyzer.ConverterFactory;
import fqlite.analyzer.Names;
import fqlite.analyzer.avro.Avro;
import fqlite.analyzer.javaserial.Deserializer;
import fqlite.analyzer.pblist.BPListParser;
import fqlite.descriptor.TableDescriptor;
import fqlite.export.DBExportWindow;
import fqlite.log.AppLog;
import fqlite.sql.SQLWindow;
import fqlite.types.BLOBElement;
import fqlite.types.CtxTypes;
import fqlite.types.ExportType;
import fqlite.types.FileTypes;
import fqlite.ui.AboutDialog;
import fqlite.ui.DBPropertyPanel;
import fqlite.ui.FQTableView;
import fqlite.ui.FileInfo;
import fqlite.ui.FontDialog;
import fqlite.ui.Importer;
import fqlite.ui.NodeObject;
import fqlite.ui.SettingsDialog;
import fqlite.ui.RollbackPropertyPanel;
import fqlite.ui.TooltippedTableCell;
import fqlite.ui.UserGuideWindow;
import fqlite.ui.WALPropertyPanel;
import fqlite.ui.hexviewer.HexViewManager;
import fqlite.util.Auxiliary;
import javafx.animation.Animation;
import javafx.animation.FadeTransition;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.collections.transformation.SortedList;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.Button;
import javafx.scene.control.CheckMenuItem;
import javafx.scene.control.ComboBox;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.Label;
import javafx.scene.control.Menu;
import javafx.scene.control.MenuBar;
import javafx.scene.control.MenuItem;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.SelectionMode;
import javafx.scene.control.SeparatorMenuItem;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableColumn.CellDataFeatures;
import javafx.scene.control.TablePosition;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.ToolBar;
import javafx.scene.control.Tooltip;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;
import javafx.scene.control.cell.TextFieldTableCell;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.ContextMenuEvent;
import javafx.scene.input.DragEvent;
import javafx.scene.input.Dragboard;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.scene.input.KeyEvent;
import javafx.scene.input.MouseButton;
import javafx.scene.input.TransferMode;
import javafx.scene.layout.Background;
import javafx.scene.layout.BackgroundFill;
import javafx.scene.layout.CornerRadii;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.text.FontPosture;
import javafx.stage.FileChooser;
import javafx.stage.Modality;
import javafx.stage.Screen;
import javafx.stage.Stage;
import javafx.util.Callback;
import javafx.util.Duration;
import javafx.util.converter.DefaultStringConverter;

/*
    ---------------
    GUI.java
    ---------------
    (C) Copyright 2024.

    Original Author:  Dirk Pawlaszczyk
    Contributor(s):   -;

   
    Project Info:  https://github.com/pawlaszczyk/fqlite

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	
	    http://www.apache.org/licenses/LICENSE-2.0
	
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
 
*/

/**
 * This class offers a basic graphical user interface. It is based on JavaFX 
 * and extends the class <code>Application</code>.
 * 
 * Most of the decoration code was generated manually.
 * 
 * If you want to start FQLite in aphic-mode you have to call this class. 
 * 
 * It contains a main()-function.
 * 
 *     __________    __    _ __     
 *    / ____/ __ \  / /   (_) /____ 
 *   / /_  / / / / / /   / / __/ _ \
 *  / __/ / /_/ / / /___/ / /_/  __/
 * /_/    \___\_\/_____/_/\__/\___/ 
 * 
 * 
 * 
 * @author Dirk Pawlaszczyk
 *
 *
 */

public class GUI extends Application {

	
	public static File baseDir;
	
	public static int pos = 0;
	
	public static final CountDownLatch latch = new CountDownLatch(1);
   	public static GUI mainwindow;
	public ConcurrentHashMap<String, javafx.scene.Node> tables = new ConcurrentHashMap<String, javafx.scene.Node>();
	private Hashtable<Object, String> rowcolors = new Hashtable<Object, String>();

	public Hashtable<String,ObservableList<ObservableList<String>>> datasets = new Hashtable<>(); 
	
	protected ContextMenu cm = null;
	protected TextArea logwindow;
	protected MenuBar menuBar;
	
	public List<String> dbnames = new ArrayList<String>();
	
	public static ScrollPane CellDetailsScrollPane;
	VBox table_panel_with_filter;
	SplitPane splitPane;
	final StackPane leftSide = new StackPane();
    final VBox rightSide = new VBox();
    public static HexViewManager HEXVIEW = HexViewManager.getInstance();
    
	/* search filter */
	TextField currentFilter = null;
	HBox head;
    int datacounter = 0;
	
	static GUI app;
	static TreeView<NodeObject> tree;
	TreeItem<NodeObject>  walNode;
	TreeItem<NodeObject>  rjNode;

	public static Font ttfFont = null;

	TreeItem<NodeObject>  root = new TreeItem<NodeObject>(new NodeObject("Databases",true));
    
	ConcurrentHashMap<String, TreeItem<NodeObject>> treeitems  = new ConcurrentHashMap<String, TreeItem<NodeObject>>();
	
	String lasthit = "";
	int lasthitrow = 0;
	int lasthitcol = 0;

	File lastDir = null;

	ImageIcon facewink;
	ImageIcon findIcon;
	ImageIcon errorIcon;
	ImageIcon infoIcon;
	ImageIcon questionIcon;
	ImageIcon warningIcon;

	StackPane rootPane = new StackPane();

	Stage stage;
	Scene scene;
	public static VBox  topContainer;
	
	SQLWindow sqlAnalyzer = null;
	
	Font appFont = null;
	
	/**
	 * Launch the graphic front-end with this method.
	 */
    public static void main(String[] args) {
   	
		/**
		  * This is needed because only one main class can be called in an
		  * executable jar archive. 
		  * 
		  **/
		if (args.length > 0)
		{			     
			// There is a least one parameter -> check, if nogui-option is set
            String option = args[0];
			
            // take the first argument - if there is one - an put to global variables
            Global.WORKINGDIRECTORY = option;
            
		}
		
	
		ImageIcon img = new ImageIcon(GUI.class.getResource("/logo.png"));
		
		
	 	SystemTray st = SystemTray.getSystemTray();
		
		if (Taskbar.isTaskbarSupported())
	    {
			try {
				final Taskbar taskbar = Taskbar.getTaskbar();
				taskbar.setIconImage(img.getImage());
				taskbar.setIconBadge("FQLite");
			}catch(Exception err) {
				// do nothing 
			}
			
	    }
		else if(SystemTray.isSupported()){
			try {
				TrayIcon ti = new java.awt.TrayIcon(img.getImage());						
				st.add(ti);
			} catch (AWTException e) {
				// do nothing - no logo - no problem ;-)
			}
		}
	
	
		Application.launch(args);
		
    }
    
    

	/**
	 * Set the TTF-font for the user interface. 
	 * @param f the font we want to set. 
	 */
	public static void setUIFont(javax.swing.plaf.FontUIResource f) {
		java.util.Enumeration<Object> keys = UIManager.getDefaults().keys();
		while (keys.hasMoreElements()) {
			Object key = keys.nextElement();
			Object value = UIManager.get(key);
			if (value instanceof javax.swing.plaf.FontUIResource)
				UIManager.put(key, f);
		}
	}
	
	
	

	/**
	 * Constructor of the user interface class. 
	 * @throws IOException
	 */
	@Override
	public void start(Stage stage) throws Exception {

		stage.setOnCloseRequest(e->{
				Platform.exit(); System.exit(0);}
		);

		
	    HexViewManager.setParent(stage);
	
		baseDir = new File(System.getProperty("user.home"), ".fqlite");

		//Attach the icon to the stage/window
	    stage.getIcons().add(new Image(GUI.class.getResourceAsStream("/logo.png")));
	 	  
		
		/* create hidden directory inside users home */
		Path pp = Path.of(baseDir.getAbsolutePath());
        System.out.println("FQlite home::" + baseDir.getAbsolutePath());
		if (!Files.exists(pp)) {
			// path does not exist in the moment -> create a new hidden folder
			baseDir.mkdir();
		}
		
		clearCacheFromPreviousRun();
	
		this.stage = stage;									
		mainwindow = this;
		
		stage.setTitle("FQLite Carving Tool");
	
		rootPane.setBackground(new Background(new BackgroundFill(Color.WHITE, CornerRadii.EMPTY, Insets.EMPTY)));

		
		String s = GUI.class.getResource("/leaf.jpg").toExternalForm();
		ImageView iv = new ImageView(s);
		root.setGraphic(iv);
        root.setExpanded(true);
			
		URL url = GUI.class.getResource("/find.png");
		findIcon = new ImageIcon(url);
		

		MenuItem mntopen = new MenuItem("Open Database...");
		mntopen.setAccelerator(KeyCombination.keyCombination("Ctrl+O"));
		mntopen.setOnAction(e -> open_db(null));

		MenuItem mntmExport = new MenuItem("Export Node to CSV...");
		mntmExport.setAccelerator(KeyCombination.keyCombination("Ctrl+X"));
		mntmExport.setOnAction(e -> doExport());
		
		MenuItem mntmExportDB = new MenuItem("Export db to SQLite...");
		mntmExportDB.setOnAction( e -> {
			showDBExportWindow();
		});
		
		MenuItem mntclose = new MenuItem("Close All");
		mntclose.setAccelerator(KeyCombination.keyCombination("Ctrl+D"));
		mntclose.setOnAction(e -> closeAll());

		MenuItem mntmExit = new MenuItem("Exit");
		mntmExit.setAccelerator(KeyCombination.keyCombination("Alt+F4"));
		mntmExit.setOnAction(e -> {Platform.exit(); System.exit(0);});

		MenuItem mntAbout = new MenuItem("About...");
		mntAbout.setOnAction(e -> new AboutDialog(topContainer));
		
		MenuItem mntFont= new MenuItem("Font...");
		mntFont.setOnAction(e -> {
				javafx.scene.text.Font fff = javafx.scene.text.Font.font(Global.font_name, FontPosture.findByName(Global.font_style),Double.valueOf(Global.font_size));
				System.out.println(fff.toString());
				
				FontDialog fdia = new FontDialog(fff,topContainer);
				fdia.show();
			}
		);

		MenuItem mntmLog = new MenuItem("View Log...");
		mntmLog.setOnAction( e -> {
			showLog();
		});
		
		
		MenuItem mntmProp = new MenuItem("Settings...");
		mntmProp.setOnAction( e -> {
			showPropertyWindow();
		});
		
		MenuItem mntmSQL = new MenuItem("SQL Analyzer...");
		mntmSQL.setOnAction( e -> {
			showSqlWindow();
		});
		
	
		
		
		
		MenuItem mntmHelp = new MenuItem("Help");
		mntmHelp.setOnAction(e -> {
				showHelp();
			}
		);
		
	
		SeparatorMenuItem sep = new SeparatorMenuItem();
		SeparatorMenuItem sep2 = new SeparatorMenuItem();

		Menu mnFiles = new Menu("File");
		Menu mnAnalyze = new Menu("Analyze");
		Menu mnInfo = new Menu("Info");

		mnFiles.getItems().addAll(mntopen,mntmExport,sep,mntclose,sep2,mntmProp,mntmExit);
        mnAnalyze.getItems().addAll(mntmSQL);
		mnInfo.getItems().addAll(mntmHelp,mntmLog,mntFont,mntAbout);
		
		
		/* MenuBar */
		menuBar = new MenuBar();
		menuBar.getMenus().addAll(mnFiles,mnAnalyze,mnInfo);
	    	
		splitPane = new SplitPane();

		s = GUI.class.getResource("/root.png").toExternalForm();
		Button starthere = new Button();
		starthere.setMaxSize(200, 200);
		
		iv = new ImageView(s);
		starthere.setGraphic(iv);
		starthere.setOnAction(e->open_db(null));
		rootPane.setAlignment(Pos.CENTER);
		rootPane.getChildren().add(starthere);
		rootPane.setPrefHeight(4000);

		prepare_tree();
        leftSide.getChildren().add(tree);
                
		splitPane.getItems().add(leftSide);
	    splitPane.getItems().add(rightSide);
	    SplitPane.setResizableWithParent(leftSide, true);
	    SplitPane.setResizableWithParent(rightSide, true);

		ToolBar toolBar = new ToolBar();
		
	    s = GUI.class.getResource("/openDB_gray.png").toExternalForm();
		Button btnOeffne = new Button();
		iv = new ImageView(s);
		btnOeffne.setGraphic(iv);
		btnOeffne.setOnAction(e->open_db(null));
		btnOeffne.setTooltip(new Tooltip("open database file"));
		toolBar.getItems().add(btnOeffne);

		s = GUI.class.getResource("/export_gray.png").toExternalForm();
		Button btnExport = new Button();
		iv = new ImageView(s);
		btnExport.setGraphic(iv);
		btnExport.setOnAction(e->doExport());
		btnExport.setTooltip(new Tooltip("export data base to file"));
		toolBar.getItems().add(btnExport);

		s = GUI.class.getResource("/closeDB_gray.png").toExternalForm();
		Button btnClose = new Button();
		iv = new ImageView(s);
		btnClose.setGraphic(iv);
		btnClose.setTooltip(new Tooltip("close All"));
		toolBar.getItems().add(btnClose);
		btnClose.setOnAction(e->closeAll());

		s = GUI.class.getResource("/helpcontent_gray.png").toExternalForm();
		Button about = new Button();
		iv = new ImageView(s);
		about.setGraphic(iv);
		about.setOnAction(e->
		{
			showHelp();
		});
		
		s = GUI.class.getResource("/sql.png").toExternalForm();
		Button btnSQL = new Button();
		iv = new ImageView(s);
		btnSQL.setGraphic(iv);
		btnSQL.setTooltip(new Tooltip("open SQL Analyzer"));
		toolBar.getItems().add(btnSQL);
		btnSQL.setOnAction(e->showSqlWindow());

		
		s = GUI.class.getResource("/properties.png").toExternalForm();
		Button btnProp = new Button();
		iv = new ImageView(s);
		btnProp.setGraphic(iv);
		btnProp.setTooltip(new Tooltip("open setting window"));
		toolBar.getItems().add(btnProp);
		btnProp.setOnAction(e->showPropertyWindow());

		about.setTooltip(new Tooltip("help"));
		toolBar.getItems().add(about);

		s = GUI.class.getResource("/exit3_gray.png").toExternalForm();
		Button btnexit = new Button();
		iv = new ImageView(s);
		btnexit.setGraphic(iv);
		btnexit.setTooltip(new Tooltip("exit"));
		btnexit.setOnAction(e->{Platform.exit(); System.exit(0);});
		toolBar.getItems().add(btnexit);

		s = GUI.class.getResource("/hex-32.png").toExternalForm();
		Button hexViewBtn = new Button();
		iv = new ImageView(s);
		hexViewBtn.setGraphic(iv);
		hexViewBtn.setTooltip(new Tooltip("open database in HexView"));
		hexViewBtn.setOnAction(e->{	
			openHexViewer();	
		});
		toolBar.getItems().add(hexViewBtn);	
		
		url = GUI.class.getResource("/facewink.png");
		facewink = new ImageIcon(url);

		url = GUI.class.getResource("/error-48.png");
		errorIcon = new ImageIcon(url);

		url = GUI.class.getResource("/information-48.png");
		infoIcon = new ImageIcon(url);

		url = GUI.class.getResource("/question-48.png");
		questionIcon = new ImageIcon(url);

		url = GUI.class.getResource("/warning-48.png");
		warningIcon = new ImageIcon(url);

		
		/**
		    Bring together all components:
		 */
		topContainer = new VBox();
		topContainer.getChildren().add(menuBar);
		topContainer.getChildren().add(toolBar);
		topContainer.getChildren().add(splitPane);
		scene = new Scene(topContainer,Screen.getPrimary().getVisualBounds().getWidth()*0.9,Screen.getPrimary().getVisualBounds().getHeight()*0.9);	
	    VBox.setVgrow(splitPane, Priority.ALWAYS);
        
		
		stage.showingProperty().addListener(new ChangeListener<Boolean>() {

		    @Override
		    public void changed(ObservableValue<? extends Boolean> observable, Boolean oldValue, Boolean newValue) {
		        if (newValue) {
		            splitPane.setDividerPositions(0.25f);
		            observable.removeListener(this);
		    	    //rightSide.autosize();

		        }
		    }
		});
		
		
		tree.setOnContextMenuRequested(new
				EventHandler<ContextMenuEvent>() {
				 @Override
				 public void handle(ContextMenuEvent event) {
				 
					 hideContextMenu();
					 cm = createContextMenu(CtxTypes.TABLE);
					 tree.setContextMenu(cm);
					 cm.show(tree,event.getScreenX(), event.getScreenY());
					 cm.show(tree.getScene().getWindow(), event.getScreenX(), event.getScreenY());

				 }
		});
		
		// OnDrag a file Over
		scene.setOnDragOver(new EventHandler<DragEvent>() {
            @Override
            public void handle(DragEvent event) {
                Dragboard db = event.getDragboard();
                if (db.hasFiles()) {
                    event.acceptTransferModes(TransferMode.COPY);
                } else {
                    event.consume();
                }
            }
        });
        
        // Dropping over surface
        scene.setOnDragDropped(new EventHandler<DragEvent>() {
            @Override
            public void handle(DragEvent event) {
                Dragboard db = event.getDragboard();
                boolean success = false;
                if (db.hasFiles()) {
                    success = true;
                    String filePath = null;
                    for (File file:db.getFiles()) {
                        filePath = file.getAbsolutePath();
                        System.out.println(filePath);
                        open_db(file);
                    }
                }
                event.setDropCompleted(success);
                event.consume();
            }
        });
        
       
        tree.autosize(); 

		stage.setScene(scene);
		
		loadConfiguration();
		
        stage.centerOnScreen();
        stage.sizeToScene();
        stage.show();
        stage.toFront();
        stage.requestFocus();
     
      
	}
	
	private void showPropertyWindow() {
		
		SettingsDialog pd = new SettingsDialog();
		pd.start(new Stage());
	}

	private void showDBExportWindow(){	
		
		if(dbnames.size() == 0){
		
			Alert alert = new Alert(AlertType.INFORMATION);
			alert.setTitle("Information");
			alert.setContentText("You must open at least one database before you can use the export function.");
			alert.showAndWait();	
			
			return;
		}
	
		DBExportWindow exp = new DBExportWindow(this);
		Stage expstage = new Stage();
		expstage.initModality(Modality.APPLICATION_MODAL);
		try {
			exp.start(expstage);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
}
		
	
	private void showSqlWindow(){
		
		if(dbnames.size() == 0){
		
			Alert alert = new Alert(AlertType.INFORMATION);
			alert.setTitle("Information");
			alert.setContentText("You must open at least one database before you can use the analyzer.");
			alert.showAndWait();	
			
			return;
		}
		
		if (!Global.SQLWARNING_SEEN) {
			showWarning(stage,"Important note: Please note that the SQL Analyzer is a beta version. Use the results with care. ");
			Global.SQLWARNING_SEEN = true;
		}
		
			SQLWindow sql = new SQLWindow(this);
			Stage sqlstage = new Stage();
			sqlstage.initModality(Modality.APPLICATION_MODAL);
			sql.start(sqlstage);
			sqlAnalyzer = sql;
			
	}
	
	private static void showWarning(Stage stage, String details) {
		Alert alert = new Alert(AlertType.INFORMATION);
		alert.setTitle("user infomation");
		alert.setContentText(details);
		alert.initOwner(stage);
		alert.showAndWait();
	}
	
	private void openHexViewer(){
		TreeItem<NodeObject> node = (TreeItem<NodeObject>) tree.getSelectionModel().getSelectedItem();
		if (node == null || node == root) {
			
		} else if (null != node.getValue()) {
			NodeObject no = (NodeObject) node.getValue();
		
			// prepare Hex-View of DB-File
			if (no.type == FileTypes.SQLiteDB)
			{			
				if (null != no.job) {
					
					HEXVIEW.load(no.job.path);
				}
			}
			// prepare Hex-View of WAL-File
			else if (no.type == FileTypes.WriteAheadLog)
			{
				
				if (null != no.job.wal) {
					
					HEXVIEW.load(no.job.wal.path);

				}
			}	
			// prepare Hex-View of Rollback-Journal
			else if (no.type == FileTypes.RollbackJournalLog)
			{
				
				if (null != no.job.rol) {
					
					HEXVIEW.load(no.job.rol.path);
				}
				

			}

		}	
		
		
		Platform.runLater(new Runnable() {
			       public void run() {             
			           try {
			        	   HEXVIEW.show();
			        	   
			           } catch (Exception e) {
						e.printStackTrace();
					}
			       }
		});			    
		
		
			
	}
	
	
	private void showHelp()
	{
		Platform.runLater(new Runnable() {
			 
			   public void run() {     
				   
			    	new UserGuideWindow().start(new Stage());									   	
			   }  
		});
		
	}
	
	final GUI gui = this;
	
	private void showLocation(double latitude, double longitude){
		Platform.runLater(new Runnable() {
			 
			   public void run() {     	   
				   gui.getHostServices().showDocument("https://www.openstreetmap.org/?mlat="+ latitude +"&mlon="+ longitude);	   	
			   }  
		});
	}
	
	private void hideContextMenu()
	{
		 if (null != cm)
			   cm.hide();
	}
	
	private void showLog(){
		AppLog logwindow = new AppLog();
		
		Stage stage = new Stage();
	    try {
			logwindow.start(stage);
		} catch (Exception e) {
			e.printStackTrace();
		}
	  
	}
	
	@SuppressWarnings("unlikely-arg-type")
	private ContextMenu createContextMenu(CtxTypes type){
		
		final ContextMenu contextMenu = new ContextMenu();
	
		MenuItem mntclosesingle = new MenuItem("Close Database");
		String s = GUI.class.getResource("/closeDB_gray.png").toExternalForm();
		ImageView iv = new ImageView(s);
		mntclosesingle.setGraphic(iv);
		mntclosesingle.setOnAction(e->{
			TreeItem<NodeObject> node = tree.getSelectionModel().getSelectedItem();
			if (node == null || node.getValue().isRoot) {
				return;
			} 
			else if (null != node.getValue()) {
				NodeObject no = (NodeObject) node.getValue();
	            boolean remove = node.getParent().getChildren().remove(node);
	            if (remove) {
	            	AppLog.info(" Database " + no.name + " closed.");
	            	this.tables.remove(no); 
	        	    this.treeitems.remove(no);
	            }
	            if (null == no.job) {}
			}	
		});
    
       
		MenuItem mntopen = new MenuItem("Open Database...");
		s = GUI.class.getResource("/openDB_gray.png").toExternalForm();
		iv = new ImageView(s);
		mntopen.setGraphic(iv);
		mntopen.setOnAction(e->open_db(null));

		MenuItem mntmExport = new MenuItem("Export Node...");
		mntmExport.setAccelerator(KeyCombination.keyCombination("Ctrl+X"));
		mntmExport.setOnAction(e ->{ doExport();});
		
		MenuItem mnthex = new MenuItem("Open HexViewer");
		mnthex.setAccelerator(KeyCombination.keyCombination("Ctrl+H"));
		s = GUI.class.getResource("/hex-32.png").toExternalForm();
		iv = new ImageView(s);
		mnthex.setOnAction(e ->{openHexViewer();});
		mnthex.setGraphic(iv);
			
		SeparatorMenuItem sepA = new SeparatorMenuItem();
		SeparatorMenuItem sepB = new SeparatorMenuItem();
	
		
	    contextMenu.getItems().addAll(sepA,mntopen,mntmExport,mntclosesingle,sepB,mnthex);
		
	    return contextMenu;
	}


	
	/**
	 *  Delete all Files from a previous run of the program. 
	 */
	private void clearCacheFromPreviousRun(){
		
		if(baseDir != null){
			
			try {
				File [] cache = baseDir.listFiles();
				
				if(null == cache)
					return;
				
				for(File file: baseDir.listFiles()) 
				    if (!file.isDirectory() && !file.getName().endsWith(".conf")) 
				    	file.delete();
			}
			catch(Exception err){
				// do nothing - no cache directory	
			}
		}
		
		
		
	}
	
	/**
	 *  This method loads the file fqlite.conf from .fqlite directory. 
	 *  The directory is hidden and normally resides in the users home directory. 
	 */
	private void loadConfiguration(){
		
				//check, if settings.conf file exists
				String path = baseDir.getAbsolutePath()+ File.separator + "fqlite.conf";
				Properties appProps = new Properties();
				
				/**
				 * Example: configuration file
				 * 
				 * #Mon Aug 12 19:01:12 CEST 2024
				 * CSV_SEPARATOR=;
				 * EXPORTMODE=DONTEXPORT
				 * EXPORT_THEADER=true
				 * font_name=Tamil Sangam MN
				 * font_size=14.0
				 * font_style=Bold
				*/
				try {
					appProps.load(new FileInputStream(path));
			
					Object value = appProps.get("EXPORTMODE");
					if (null != value)
						Global.EXPORT_MODE = Global.EXPORT_MODES.valueOf((String)value);
				
					Object svalue = appProps.get("SEPARATOR");
					if (null != svalue)
						Global.CSV_SEPARATOR = (String)svalue;
					
					Object tvalue = appProps.get("EXPORT_THEADER");
					if (null != tvalue){
						if(tvalue.equals("true"))
							Global.EXPORTTABLEHEADER = true;
						else
							Global.EXPORTTABLEHEADER = false;
					}
				
					Object cvalue = appProps.get("CSV_SEPARATOR");
					if (null != cvalue){
						Global.CSV_SEPARATOR = (String)cvalue;
					}
					
					
					Object fn = appProps.get("font_name");
					if (null != fn){
						Global.font_name = (String)fn;
					}
					
					Object fs = appProps.get("font_size");
					if (null != fs){
						Global.font_size = (String)fs;
					}
					
					System.out.println("-fx-font-size: " + fs + "pt; -fx-font-family: \""+ fn + "\"; "); 
					topContainer.setStyle("-fx-font-size: " + fs + "pt; -fx-font-family: \""+ fn + "\"; ");
				} catch (Exception e) {
					System.out.println(" Couldn't find settings.conf file. Create a new one.");
					FileOutputStream firsttime;
					try {
						firsttime = new FileOutputStream(path);
						firsttime.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}
		
	}
	
	
	/**
	 *  Close all database nodes that are currently open. 
	 */
	public void closeAll() {

		TreeItem<NodeObject> root = tree.getRoot();
		Iterator<TreeItem<NodeObject>> it = root.getChildren().iterator();
	
	
		while (it.hasNext()) {
			
			TreeItem<NodeObject> node = it.next();
			if (null != node.getValue()) {
				NodeObject no = (NodeObject) node.getValue();
				if (null != no.job) {		
					no.job.checkpointlist = null;
					no.job.FileCache = null;
					if(no.job.db != null)	
						no.job.db.clear();
					if(no.job.freelistpages != null)
						no.job.freelistpages.clear();
					if(no.job.tasklist != null)
						no.job.tasklist.clear();
					if (no.job.guiroltab != null)
						no.job.guiroltab.clear();
					if (no.job.rol != null)
						no.job.rol = null;
					if (no.job.wal != null)
						no.job.wal = null;
					no.job = null;
					no.tablePane = null;
				}
				
				
			}
		}
		
		// remove all nodes -> simply create a new TreeView 
		if (root != null) {
			 root.getChildren().clear();
		}

		this.tables.clear(); 
	    this.treeitems.clear(); 
	    this.dbnames.clear();
	    HEXVIEW.close();
	
	    //long free = Runtime.getRuntime().freeMemory();
        //long total = Runtime.getRuntime().totalMemory();
        //long max = Runtime.getRuntime().maxMemory();
        
        //System.out.println("free memory " + free);
        //System.out.println("total memory " + total);
        //System.out.println("max memory " + max);
   
	    System.gc();	
	    
	    

	}

	/**
	 * Start a data export.
	 */
	public void doExport() {
		NodeObject no = null;
		
		/* Do we really have a database node currently selected ? */
		TreeItem<NodeObject> node = tree.getSelectionModel().getSelectedItem();
		if (node == null || node.getValue().isRoot) {
			return;
		} 
		else if (null != node.getValue()) {
			no = (NodeObject) node.getValue();
			if (null == no.job) {}
		}

		/* it is indeed a valid node (database, table, journal, WAL archive) -> begin with export */
		export_table(no);
	}
	 
	private static final String CSS = """
	           .invalid {
	               -fx-text-fill: red;
	           }
	           """;
	
	/**
	 * Add a new table header to the database tree.
	 * 
	 * @param job
	 * @param tablename
	 * @param columns
	 * @return tree path
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	String add_table(Job job, String tablename, List<String> columns, List<String> columntypes, List<String> PK, List<String> BoolColumns, boolean walnode,
			boolean rjnode, int db_object) {

		
		NodeObject o = null;
		
		Path p = Paths.get(job.path);
		
		FQTableView<Object> table = new FQTableView<Object>(tablename,p.getFileName().toString(),job, columns,columntypes);
		
 		
		table.getSelectionModel().setSelectionMode(
			    SelectionMode.MULTIPLE
		);
		
		
		Image img = new Image(GUI.class.getResource("/icon_status.png").toExternalForm());
	    ImageView view = new ImageView(img);
	  
	
		// normal table ?
		if (!walnode) {
	    //yes		
		//add the standard columns (index 0 <>'line number', 2 <> 'status',3 <> 'offset' - '1' <>is the table name 
	    //Label NoLabel = new Label("No.");
	    //NoLabel.setTooltip(new Tooltip("row number")); 
		TableColumn numbercolumn = new TableColumn<>("No.");
		//numbercolumn.setGraphic(NoLabel);
		numbercolumn.setCellValueFactory(new Callback<CellDataFeatures<ObservableList,String>,ObservableValue<String>>(){                    
			public ObservableValue<String> call(CellDataFeatures<ObservableList, String> param) {                                                                                              
					return new SimpleStringProperty(param.getValue().get(0).toString());               //line number index   
			}                    
		});  
		
		numbercolumn.setStyle( "-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");
//		numbercolumn.setCellFactory(param -> new TextFieldTableCell<>(new DefaultStringConverter()) {
//			   @Override
//			   public void updateItem(String item, boolean empty) {
//			      super.updateItem(item, empty);
//
//			       if (item != null && !empty) {
//			          if (!getStyleClass().contains(CSS)) {
//			             getStyleClass().add(CSS);
//			          }
//			       } else {
//			          getStyleClass().remove(CSS);
//			       }
//			       
//			       if (super.getIndex() % 2 == 0)
//			    	   this.setStyle("-fx-font-style: italic; -fx-alignment: TOP-RIGHT; -fx-background-color: honeydew;");
//			       else
//			    	   this.setStyle("-fx-font-style: italic; -fx-alignment: TOP-RIGHT; -fx-background-color: lavender;");
//
//			   }
//
//			 
//		});

		//Label PLLLabel = new Label("PLL");
		//PLLLabel.setTooltip(new Tooltip("shows payload length in bytes")); 
		TableColumn pllcolumn = new TableColumn<>("PLL|HL");
		//pllcolumn.setGraphic(PLLLabel);
		pllcolumn.setCellFactory(TooltippedTableCell.forTableColumn(tablename,job,this.stage));
     	pllcolumn.setCellValueFactory(new Callback<CellDataFeatures<ObservableList,String>,ObservableValue<String>>(){                    
            public ObservableValue<String> call(CellDataFeatures<ObservableList, String> param) {                                                                                              
                return new SimpleStringProperty(param.getValue().get(2).toString());                        
            }                    
		});
     	
		pllcolumn.setStyle( "-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");

//		pllcolumn.setCellFactory(param -> new TextFieldTableCell<>(new DefaultStringConverter()) {
//			   @Override
//			   public void updateItem(String item, boolean empty) {
//			      super.updateItem(item, empty);
//
//			       if (item != null && !empty) {
//			          if (!getStyleClass().contains(CSS)) {
//			             getStyleClass().add(CSS);
//			          }
//			       } else {
//			          getStyleClass().remove(CSS);
//			       }
//			       
//			       if (super.getIndex() % 2 == 0)
//			    	   this.setStyle("-fx-font-style: italic; -fx-alignment: TOP-RIGHT; -fx-background-color: honeydew;");
//			       else
//			    	   this.setStyle("-fx-font-style: italic; -fx-alignment: TOP-RIGHT; -fx-background-color: lavender;");
//
//			   }
//
//			 
//		});
//		
     
     	
    	//Label HLLabel = new Label("HL");
		//HLLabel.setTooltip(new Tooltip("shows the header length in bytes")); 
		TableColumn hlcolumn = new TableColumn<>("ROWID");
		//hlcolumn.setGraphic(HLLabel);
		hlcolumn.setCellFactory(TooltippedTableCell.forTableColumn(tablename,job,this.stage));
     	hlcolumn.setCellValueFactory(new Callback<CellDataFeatures<ObservableList,String>,ObservableValue<String>>(){                    
            public ObservableValue<String> call(CellDataFeatures<ObservableList, String> param) {                                                                                              
                return new SimpleStringProperty(param.getValue().get(3).toString());                        
            }                    
		});
     	
		hlcolumn.setStyle( "-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");
//		hlcolumn.setCellFactory(param -> new TextFieldTableCell<>(new DefaultStringConverter()) {
//			   @Override
//			   public void updateItem(String item, boolean empty) {
//			      super.updateItem(item, empty);
//
//			       if (item != null && !empty) {
//			          if (!getStyleClass().contains(CSS)) {
//			             getStyleClass().add(CSS);
//			          }
//			       } else {
//			          getStyleClass().remove(CSS);
//			       }
//			       
//			       if (super.getIndex() % 2 == 0)
//			    	   this.setStyle("-fx-font-style: italic; -fx-alignment: TOP-RIGHT; -fx-background-color: honeydew;");
//			       else
//			    	   this.setStyle("-fx-font-style: italic; -fx-alignment: TOP-RIGHT; -fx-background-color: lavender;");
//
//			   }
//
//			 
//		});
     	 
     	
     	Label statusLabel = new Label(Global.STATUS_CLOMUN);
		
     	statusLabel.setTooltip(new Tooltip("indicates if data record is deleted or not")); 
     	TableColumn statuscolumn = new TableColumn<>();
        statuscolumn.setGraphic(statusLabel);
		statuscolumn.setCellFactory(TooltippedTableCell.forTableColumn(tablename,job,this.stage));
     	statuscolumn.setCellValueFactory(new Callback<CellDataFeatures<ObservableList,String>,ObservableValue<String>>(){                    
			public ObservableValue<String> call(CellDataFeatures<ObservableList, String> param) {                                                                                              
                return new SimpleStringProperty(param.getValue().get(4).toString());                        
            } 
     	});
     	statuscolumn.setGraphic(view);
		statuscolumn.setStyle( "-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");
//		statuscolumn.setCellFactory(param -> new TextFieldTableCell<>(new DefaultStringConverter()) {
//			   @Override
//			   public void updateItem(String item, boolean empty) {
//			      super.updateItem(item, empty);
//
//			       if (item != null && !empty) {
//			          if (!getStyleClass().contains(CSS)) {
//			             getStyleClass().add(CSS);
//			          }
//			       } else {
//			          getStyleClass().remove(CSS);
//			       }
//			       
//			       if (super.getIndex() % 2 == 0)
//			    	   this.setStyle("-fx-font-style: italic; -fx-alignment: TOP-RIGHT; -fx-background-color: honeydew;");
//			       else
//			    	   this.setStyle("-fx-font-style: italic; -fx-alignment: TOP-RIGHT; -fx-background-color: lavender;");
//
//			   }
//
//			 
//		});
	  	
     	TableColumn offsetcolumn = new TableColumn<>("Offset");
		offsetcolumn.setCellFactory(TooltippedTableCell.forTableColumn(tablename,job,this.stage));
     	offsetcolumn.setCellValueFactory(new Callback<CellDataFeatures<ObservableList,String>,ObservableValue<String>>(){                    
			public ObservableValue<String> call(CellDataFeatures<ObservableList, String> param) {                                                                                              
                return new SimpleStringProperty(param.getValue().get(5).toString());                        
            }                    
		});
     		
		offsetcolumn.setStyle( "-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");
//		offsetcolumn.setCellFactory(param -> new TextFieldTableCell<>(new DefaultStringConverter()) {
//			   @Override
//			   public void updateItem(String item, boolean empty) {
//			      super.updateItem(item, empty);
//
//			       if (item != null && !empty) {
//			          if (!getStyleClass().contains(CSS)) {
//			             getStyleClass().add(CSS);
//			          }
//			       } else {
//			          getStyleClass().remove(CSS);
//			       }
//			       
//			       if (super.getIndex() % 2 == 0)
//			    	   this.setStyle("-fx-font-style: italic; -fx-alignment: TOP-RIGHT; -fx-background-color: honeydew;");
//			       else
//			    	   this.setStyle("-fx-font-style: italic; -fx-alignment: TOP-RIGHT; -fx-background-color: lavender;");
//
//			   }
//
//			 
//		});
     		
     	//[no,pll,hl,tabname,status,...]
		table.getColumns().addAll(numbercolumn,statuscolumn,offsetcolumn,pllcolumn,hlcolumn);
		
		}
		else {
			/**
			 * Attention: Table is a WAL-Table and has some extra columns !!!
			 */
		
			//add the standard columns (index 0 <>'line number', 2 <> 'status',3 <> 'offset' - '1' <>is the table name 
		    //Label NoLabel = new Label("No.");
		    //NoLabel.setTooltip(new Tooltip("row number")); 
			TableColumn numbercolumn = new TableColumn<>("No.");
			//numbercolumn.setGraphic(NoLabel);
			numbercolumn.setComparator(new CustomComparator());
			numbercolumn.setCellValueFactory(new Callback<CellDataFeatures<ObservableList,String>,ObservableValue<String>>(){                    
				public ObservableValue<String> call(CellDataFeatures<ObservableList, String> param) {                                                                                              
						return new SimpleStringProperty(param.getValue().get(0).toString());               //line number index   
				}                    
			});  
			numbercolumn.setStyle( "-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");


			//Label PLLLabel = new Label("PLL");
			//PLLLabel.setTooltip(new Tooltip("shows payload length in bytes")); 
			TableColumn pllcolumn = new TableColumn<>("PLL|HL");
			pllcolumn.setCellFactory(TooltippedTableCell.forTableColumn(tablename,job,this.stage));
			//pllcolumn.setGraphic(PLLLabel);
			pllcolumn.setComparator(new CustomComparator());
	     	pllcolumn.setCellValueFactory(new Callback<CellDataFeatures<ObservableList,String>,ObservableValue<String>>(){                    
	            public ObservableValue<String> call(CellDataFeatures<ObservableList, String> param) {                                                                                              
	                return new SimpleStringProperty(param.getValue().get(2).toString());                        
	            }                    
			});
			//pllcolumn.setStyle( "-fx-alignment: CENTER-RIGHT;");
			pllcolumn.setStyle( "-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");
			
		
	     	
	    	//Label HLLabel = new Label("HL");
			//HLLabel.setTooltip(new Tooltip("shows the header length in bytes")); 
			TableColumn hlcolumn = new TableColumn<>("ROWID");
			hlcolumn.setCellFactory(TooltippedTableCell.forTableColumn(tablename,job,this.stage));
			//hlcolumn.setGraphic(HLLabel);
			hlcolumn.setComparator(new CustomComparator());
	     	hlcolumn.setCellValueFactory(new Callback<CellDataFeatures<ObservableList,String>,ObservableValue<String>>(){                    
	            public ObservableValue<String> call(CellDataFeatures<ObservableList, String> param) {                                                                                              
	                return new SimpleStringProperty(param.getValue().get(3).toString());                        
	            }                    
			});
			hlcolumn.setStyle( "-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");

	     	
	     	Label statusLabel = new Label(Global.STATUS_CLOMUN);
			statusLabel.setTooltip(new Tooltip("indicates if data record is deleted or not")); 
	     	TableColumn statuscolumn = new TableColumn<>();
	        statuscolumn.setGraphic(statusLabel);
			statuscolumn.setCellFactory(TooltippedTableCell.forTableColumn(tablename,job,this.stage));
	        statuscolumn.setCellValueFactory(new Callback<CellDataFeatures<ObservableList,String>,ObservableValue<String>>(){                    
	            public ObservableValue<String> call(CellDataFeatures<ObservableList, String> param) {                                                                                              
	                return new SimpleStringProperty(param.getValue().get(4).toString());                        
	            } 
	     	});
	     	statuscolumn.setGraphic(view);
			//statuscolumn.setStyle( "-fx-alignment: TOP-RIGHT;");

		  	
	     	TableColumn offsetcolumn = new TableColumn<>("Offset");
			offsetcolumn.setComparator(new CustomComparator());
			offsetcolumn.setCellFactory(TooltippedTableCell.forTableColumn(tablename,job,this.stage));
	     	offsetcolumn.setCellValueFactory(new Callback<CellDataFeatures<ObservableList,String>,ObservableValue<String>>(){                    
	            public ObservableValue<String> call(CellDataFeatures<ObservableList, String> param) {                                                                                              
	                return new SimpleStringProperty(param.getValue().get(5).toString());                        
	            }                    
			});
			offsetcolumn.setStyle( "-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");

	     		
	     	//[no,pll,hl,tabname,status,...]
			table.getColumns().addAll(numbercolumn,statuscolumn,offsetcolumn,pllcolumn,hlcolumn);
		
		
		
		
		}	// end of else (walnode = true)	
		
		datacounter = 0;
		
		
	   /**********************************
        * ADD TABLE COLUMNS DYNAMICALLY *
        **********************************/
		
		
		for (int i = 0; i < columns.size(); i++) {
            String colname = columns.get(i);
            final int j = walnode ? i + 6 : i + 6;                
			TableColumn col = new TableColumn(colname);		
			col.setCellFactory(TooltippedTableCell.forTableColumn(tablename,job,this.stage));
			col.setComparator(new CustomComparator());
			
		
			col.setCellValueFactory(new Callback<CellDataFeatures<ObservableList,String>,ObservableValue<String>>(){                    
	  
			public ObservableValue<String> call(CellDataFeatures<ObservableList, String> param) {                                                                                              
            	
				if (param.getValue().size() <= j)
            		return new SimpleStringProperty("");
            	Object o = param.getValue().get(j);
            	if (null != o)
            		return new SimpleStringProperty(o.toString());                        
            	else
            		return new SimpleStringProperty("");                        
                
            	
            }      
			});
			
		
			if (columntypes.size()>i && !columntypes.get(i).equals("BLOB") && !columntypes.get(i).equals("TEXT")) {				
								col.setStyle( "-fx-alignment: TOP-RIGHT;");	
			}
			else {
			    if (columntypes.size()>i)
			    				col.setStyle( "-fx-alignment: TOP-LEFT;");	    	
			}
			
			/* add icon to PRIMARYKEY columns */
			if (null != PK)
			{
				if(PK.contains(colname))
				{
					img = new Image(GUI.class.getResource("/key-icon.png").toExternalForm());
				    view = new ImageView(img);
				    col.setGraphic(view);
				}	
			}
			
			if(colname.equals("salt2") || colname.equals("salt1") ||
					colname.equals("walframe") 	|| colname.equals("dbpage") || colname.equals("commit"))
			{			col.setStyle( "-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");

				
			}	
			
			
			table.getColumns().add(col);
		}
						
			
	    VBox tablePane = new VBox();
	    
	    
		Image img2 = new Image(GUI.class.getResource("/closeDB_gray.png").toExternalForm());
	  
		ImageView view2 = new ImageView(img2);
	    
	    Button clearFilter = new Button();
	    clearFilter.setGraphic(view2);
		view2.setFitHeight(14);
	    view2.setFitWidth(14);
	    view2.preserveRatioProperty();
	    clearFilter.setGraphic(view2);
	    clearFilter.setTooltip(new Tooltip("set back filter"));
		   
	    ComboBox<String> columnselection = new ComboBox<String>();

	    columnselection.getItems().add("All Columns (Filter) -> ");
	    columnselection.getItems().add("No.");
	    columnselection.getItems().add("Status");
	    columnselection.getItems().add("Offset");
	    columnselection.getItems().add("PLL|HL");
	    columnselection.getItems().add("ROWID");
		    
	    
	    Iterator<String> cli = columns.iterator();
	    
	    while(cli.hasNext()) {
	        String choice = cli.next();
	    	if(choice != null)
	        	columnselection.getItems().add(choice);
	    }
	    
	    // Select All Columns as default 
	    columnselection.getSelectionModel().select(0);
	    
	    TextField filter = new TextField();
	    filter.setPrefWidth(300);
	    
	    HBox filterpane = new HBox();
	    filterpane.getChildren().add(0,columnselection);
	    filterpane.getChildren().add(1,filter);
	    filterpane.getChildren().add(2,clearFilter);
		 
	    
		tablePane.getChildren().add(filterpane);
		tablePane.getChildren().add(table);
		table.setPrefHeight(4000);
		VBox.setVgrow(table, Priority.ALWAYS);
	    Label l = new Label("Table: " + tablename);
	    tablePane.getChildren().add(l);
	
		if (walnode)
      		o = new NodeObject(tablename, tablePane, columns.size(), FileTypes.WriteAheadLog, db_object); // wal file
		if (rjnode)
			o = new NodeObject(tablename, tablePane, columns.size(), FileTypes.RollbackJournalLog, db_object); // rollback																									// journal file
		if (!walnode && !rjnode)
			o = new NodeObject(tablename, tablePane, columns.size(), FileTypes.SQLiteDB, db_object); // normal db

		o.job = job;
		TreeItem<NodeObject> dmtn = new TreeItem<NodeObject>(o);    
	    dmtn.setExpanded(true);
	    
		
		String s = null;
		
		switch(o.tabletype)
		{
			case 0   :	s = GUI.class.getResource("/table_icon_empty.png").toExternalForm(); // /.png
						break;
			case 1   : 	s = GUI.class.getResource("/table-key-icon-reddot.png").toExternalForm();
						break;
			case 99  :  s = GUI.class.getResource("/database-small-icon.png").toExternalForm();
						break;						
			case 100 :  s = GUI.class.getResource("/journal-icon.png").toExternalForm();  
						break; 			
			case 101 :  s = GUI.class.getResource("/wal-icon.png").toExternalForm();
						break;
		}
		
	    ImageView iv = new ImageView(s);
		dmtn.setGraphic(iv);
 		
		String tp = null;
		
		if (walnode) {
				
			/* WAL-tree node - add child node of table */
			walNode.getChildren().add(dmtn);
		
			tp = getPath(dmtn);
			
			// save assignment between tree item's path an a tree item
			treeitems.put(tp, dmtn);
	
			
		}
		else if (rjnode) {
			/* Rollback Journal */
			rjNode.getChildren().add(dmtn);
			tp = getPath(dmtn);
			
			// save assignment between tree item's path an a tree item
			treeitems.put(tp, dmtn);
			
		}
		else{
			 /* main db */
		    job.getTreeItem().getChildren().add(dmtn);
			tp = getPath(dmtn);

			// save assignment between tree item's path an a tree item
			treeitems.put(tp, dmtn);
			
		}
		
		ContextMenu tcm = createContextMenu(CtxTypes.TABLE,tablename,table,job); 
	

		table.setOnKeyPressed(new EventHandler<KeyEvent>(){
			
		    @Override
		    public void handle(KeyEvent event) {
			    if (!table.getSelectionModel().isEmpty())
			    {   	
			    	KeyCodeCombination copylineCombination = new KeyCodeCombination(KeyCode.L, KeyCombination.SHORTCUT_DOWN);
			    	KeyCodeCombination copycellCombination = new KeyCodeCombination(KeyCode.C, KeyCombination.SHORTCUT_DOWN);
					 
			    	if(copylineCombination.match(event))
			    	{	
				    	copyLineAction(table);
				    	event.consume();
				    }
			    	else if(copycellCombination.match(event))
			    	{
			    		copyCellAction(table);
			    		event.consume();
			    	}
			    	
			    }
		    }
		});
		
		table.setOnMouseClicked(new EventHandler<javafx.scene.input.MouseEvent>() {
			@Override
			   public void handle(javafx.scene.input.MouseEvent event) {

				   //System.out.println("Quelle : " + event.getTarget());
				   if(event.getTarget().toString().startsWith("TableColumnHeader"))
					   return;
				   
				   int row = -1;
				   TablePosition pos = null;
				   try 
				   {
				     pos = (TablePosition) table.getSelectionModel().getSelectedCells().get(0);
			         row = pos.getRow();

				   }catch(Exception err) {
					   return;
				   }
				      
				   
				   // Item here is the table view type:
				   Object item = table.getItems().get(row);
				   
				
				   if(event.getButton() == MouseButton.SECONDARY) {
						  
					   //ContextMenu tcm = createContextMenu(CtxTypes.TABLE,table); 
					   tcm.show(table.getScene().getWindow(), event.getScreenX(), event.getScreenY());
					   return;
				   }
				   
				   
				   TableColumn col = pos.getTableColumn();

				   if(col == null)
					   	return;
				   
				   // this gives the value in the selected cell:
				   Object data = col.getCellObservableValue(item).getValue();

				   // get the relative virtual address (offset) from the table
			       TableColumn toff = (TableColumn) table.getColumns().get(2);
			       
				   // get the actual value of the currently selected cell
			       ObservableValue off =  toff.getCellObservableValue(row);
			      
				   
				   if (col.getText().equals("Offset"))
				   {
					   if(row >= 0)
					   {   
						   // get currently selected database
						   NodeObject no = getSelectedNode();
						   						   
						   String model = null;
						   
						   if (no.type == FileTypes.SQLiteDB)
							model = no.job.path;
						   else if (no.type == FileTypes.WriteAheadLog)
							model = no.job.wal.path;
					       else if (no.type == FileTypes.RollbackJournalLog)
					    	model = no.job.rol.path;
						   
										   
						   long position = -1;
						   try {
							   position = Long.parseLong((String)data);

							   HEXVIEW.go2(model, position);
							   
						   }catch(Exception err) {
							   
						   }
						   
					       
					   	}
				   }
				   else  //another column was clicked
				   {
					   boolean doubleclicked = false;
					   
					   if(event.getButton().equals(MouseButton.PRIMARY)){
				            if(event.getClickCount() == 2){
				                doubleclicked = true;
				            }
				        }	  
					   
					   if(data != null && doubleclicked){
					   	
					   		   String cellvalue = (String)data;
				    		   /* Has the user double-clicked on a BLOB column ? */ 
					   		   if (cellvalue.startsWith("[BLOB-"))
				   			   {
					   			    int from = cellvalue.indexOf("BLOB-");
					   	        	int to = cellvalue.indexOf("]");
					   	        	String number = cellvalue.substring(from+5, to);			
					   				int start = cellvalue.indexOf("<");
					   				int end   = cellvalue.indexOf(">");
					   				
					   				/* extract the BLOB type information from cell value */
					   				String type;
					   				if (end > 0) {
					   					type = cellvalue.substring(start+1,end);
					   				}
					   				else 
					   					type = "";
					   				
					   				/* note: there are only a few supported file formats in the moment */
					   				
					   				/* Is the BLOB a PDF ? */
					   				if(type.equals("pdf"))
					   				{
					   					
					   					Platform.runLater(new Runnable() {
											 
											   public void run() {     
												   
													String path = GUI.baseDir + Global.separator + table.dbname + "_" + off.getValue() + "-" + number + "." + type;
											    	BLOBElement e = job.bincache.get(path);
											    	try {
											    		BufferedOutputStream buffer = new BufferedOutputStream(new FileOutputStream(path));
														buffer.write(e.binary);
														buffer.close();
														/* open the pdf-file by using our internal viewer */
														new PDFPreviewer(path).start(new Stage());									   	
													}
													catch(Exception err){
														// simply do nothing, if opening the viewer fails
													}
													
											   }  
										});
					   					
					   					
					   				}
					   				/* Is it a common picture format ? */
					   				if(type.equals("gif") || type.equals("bmp") || type.equals("png") || type.equals("jpg")|| type.equals("heic") || type.equals("tiff"))
					   				{		
					   					String uri = "file:" + Global.separator + Global.separator + GUI.baseDir + Global.separator + table.dbname + "_" + off.getValue() + "-" + number + "." + type;
										String path = GUI.baseDir + Global.separator + table.dbname + "_" + off.getValue() + "-" + number + "." + type;
					   					BLOBElement e = job.bincache.get(path);
								    	try {
								    		/* before we can open it, we need to create a file on the file system for the BLOB */
								    		BufferedOutputStream buffer = new BufferedOutputStream(new FileOutputStream(path));
											buffer.write(e.binary);
											buffer.close();
											/* open picture with the default viewer from the operating system associated with this file extension*/
						   					getHostServices().showDocument(uri);
										}
										catch(Exception err){
											
										}
					   				}
				   			   }
						   
					   	   }
				   	  
				   }
				   
				   
				   
				  
				   
			   }
			});
		
		
	
		tables.put(tp, tablePane);
		return tp;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private ContextMenu createContextMenu(CtxTypes type,String tablename, FQTableView table,Job job){
		
		final ContextMenu contextMenu = new ContextMenu();
	    FQTableView mytable = table;
		
		// copy a single table line
		MenuItem mntcopyline = new MenuItem("Copy Line(s)");
		String s = GUI.class.getResource("/edit-copy.png").toExternalForm();
	    ImageView iv = new ImageView(s); 

    	KeyCodeCombination copylineCombination = new KeyCodeCombination(KeyCode.L, KeyCombination.SHORTCUT_DOWN);
    	KeyCodeCombination copycellCombination = new KeyCodeCombination(KeyCode.C, KeyCombination.SHORTCUT_DOWN);
    	KeyCodeCombination copyttCombination = new KeyCodeCombination(KeyCode.T, KeyCombination.SHORTCUT_DOWN);

    	
	    mntcopyline.setAccelerator(copylineCombination);
	    mntcopyline.setGraphic(iv);
		mntcopyline.setOnAction(e ->{
			copyLineAction(mytable);     		
			e.consume();
		}
		);

		
		// copy the complete table line (with all cells)
		MenuItem mntcopycell= new MenuItem("Copy Cell");
	    s = GUI.class.getResource("/edit-copy.png").toExternalForm();
		iv = new ImageView(s);
		mntcopycell.setGraphic(iv);
	    mntcopycell.setAccelerator(copycellCombination);

		mntcopycell.setOnAction(e ->{
			copyCellAction(mytable);
			e.consume();
		}
		);
		
		// copy the complete table line (with all cells)
		MenuItem mntcopytt = new MenuItem("Copy Tooltip");
		s = GUI.class.getResource("/edit-copy.png").toExternalForm();
		iv = new ImageView(s);
		mntcopytt.setGraphic(iv);
	    mntcopytt.setAccelerator(copyttCombination);

		mntcopytt.setOnAction(e ->{
					copyToolTipAction(mytable);
					e.consume();
		}
		);
		
		
        SeparatorMenuItem sepA = new SeparatorMenuItem();
    
        s = GUI.class.getResource("/edit-find.png").toExternalForm();
        iv = new ImageView(s);
     	
        Menu analyze = new Menu("Convert...");
        analyze.setGraphic(iv);
       
        Menu blob = new Menu("BLOB to ..");
        blob.setGraphic(iv);
     
        MenuItem location = new MenuItem("Show Location (in browser)");
        location.setOnAction(e -> { 
        	
    		
            ObservableList<TablePosition> selection = mytable.getSelectionModel().getSelectedCells();
            // nothing selected -> leave copy action
            if (selection.size() == 0)
            	return;
           
            // where am I?
            TablePosition tp = selection.get(0); 
            int row = tp.getRow();
    		int col = tp.getColumn();
    		
    		try {
            
	            TableColumn tc = (TableColumn) table.getColumns().get(col);
	            ObservableValue observableValue =  tc.getCellObservableValue(row);
	            String cellvalue1 = (String)observableValue.getValue();
	            TableColumn tc2 = (TableColumn) table.getColumns().get(col+1);
	            ObservableValue observableValue2 =  tc2.getCellObservableValue(row);
	            String cellvalue2 = (String)observableValue2.getValue();
	            
      	
				cellvalue1 = cellvalue1.replace(",",".");
				cellvalue2 = cellvalue2.replace(",",".");
				double latitude = Double.valueOf(cellvalue1);
				double longitude = Double.valueOf(cellvalue2);
				System.out.println(" Coordinates: " + latitude + " " + longitude);
				showLocation(latitude,longitude);
				
			}catch(Exception err) {
				Alert alert = new Alert(AlertType.ERROR);
				alert.setTitle("Error");
				alert.setContentText("No valid gps coordinates.");
				alert.showAndWait();		
				return;
			}
        		
        });
        
        
        analyze.getItems().add(blob);
        
        
        EventHandler<ActionEvent> event = new EventHandler<ActionEvent>() { 
            public void handle(ActionEvent e) 
            { 
            	CheckMenuItem selected = ((CheckMenuItem)e.getSource());
            	String text = selected.getText();
            	
            	List<MenuItem> items = blob.getItems();
            	for(int i=0; i < items.size();i++){
            		CheckMenuItem cmi = (CheckMenuItem)items.get(i);
            		if(cmi.isSelected() && !cmi.getText().equals(selected.getText()))
            		{
            			cmi.setSelected(false);
            		}
            
            	}
            	
            	switch(text) 
            	{
            	    case Names.DEFAULT:
            	    	 job.convertto.remove(tablename);
            	    	break;
            		case Names.BSON: 
            			job.convertto.put(tablename,Names.BSON);
            			break;
            		case Names.Fleece: 
        				job.convertto.put(tablename,Names.Fleece);
        			break;
            		case Names.MessagePack: 
        				job.convertto.put(tablename,Names.MessagePack);
        			break;
            		case Names.ThriftBinary: 
        				job.convertto.put(tablename,Names.ThriftBinary);
        			break;
            		case Names.ThriftCompact: 
        				job.convertto.put(tablename,Names.ThriftCompact);
        			break;
            		case Names.ProtoBuffer: 
            			job.convertto.put(tablename,Names.ProtoBuffer);
        			break;
        			
        				
            	}
            	table.refresh();
    			
            	
            } 
        }; 
        
        CheckMenuItem defaults = new CheckMenuItem(Names.DEFAULT);
        defaults.setOnAction(event);
        blob.getItems().add(defaults);
        defaults.setSelected(true);

        CheckMenuItem protob = new CheckMenuItem(Names.ProtoBuffer);
        protob.setOnAction(event);
        blob.getItems().add(protob);

        CheckMenuItem bson = new CheckMenuItem(Names.BSON);
        bson.setOnAction(event);
        blob.getItems().add(bson);

        CheckMenuItem flatbuffer = new CheckMenuItem(Names.FlatBuffer);
        flatbuffer.setOnAction(event);
        blob.getItems().add(flatbuffer);
        
        CheckMenuItem fleece = new CheckMenuItem(Names.Fleece);
        fleece.setOnAction(event);
        blob.getItems().add(fleece);
        
        CheckMenuItem msgpack = new CheckMenuItem(Names.MessagePack);
        msgpack.setOnAction(event);
        blob.getItems().add(msgpack);
        
        CheckMenuItem tbp = new CheckMenuItem(Names.ThriftBinary);
        tbp.setOnAction(event);
        blob.getItems().add(tbp);
    
        CheckMenuItem tcp = new CheckMenuItem(Names.ThriftCompact);
        tcp.setOnAction(event);
        blob.getItems().add(tcp);
        
        
        /* This section is used to create a check item to BASE64 support for table cells (experimental)*/
        
        EventHandler<ActionEvent> eventBASE64 = new EventHandler<ActionEvent>() { 
            public void handle(ActionEvent e) 
            { 
                if (((CheckMenuItem)e.getSource()).isSelected()) 
                {
                	/* enable BASE64 for this table */
                	System.out.println("BASE64 support enabled." + tablename);
                	job.inspectBASE64.add(tablename);
                	table.refresh();
                }
                else
                {
                	/* disable protobuffer inspection for this table */
                	System.out.println("Off.");
                	job.inspectBASE64.remove(tablename);
                	table.refresh();
                }
            } 
        }; 
        
        CheckMenuItem base64 = new CheckMenuItem("BASE64 to..");
        base64.setOnAction(eventBASE64);
        analyze.getItems().add(base64);
        
        
		contextMenu.getItems().addAll(mntcopyline,mntcopycell,mntcopytt,sepA,analyze,location);
			    
	    return contextMenu;
	}
	
	
	
	/**
	 * This method is an action handler. It provides a copy of
	 * the tool tip content to clip board. Note: The tool tip content
	 * can significantly differ from the cell value. BLOB values
	 * like property lists or protobuffers are decoded and displayed
	 * within the tool tip info. 
	 *  
	 * @param table
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void copyToolTipAction(FQTableView table){
	 
		String coltype = null;
		Job job = table.job;
		String tablename = table.tablename;
		
        ObservableList<TablePosition> selection = table.getSelectionModel().getSelectedCells();
        // nothing selected -> leave copy action
        if (selection.size() == 0)
        	return;
       
        // where am I?
        TablePosition tp = selection.get(0); 
        int row = tp.getRow();
		int col = tp.getColumn();

        
        TableColumn tc = (TableColumn) table.getColumns().get(col);
        ObservableValue observableValue =  tc.getCellObservableValue(row);
        String columnname =  tc.getText();
        // get the relative virtual address (offset) from the table
        TableColumn toff = (TableColumn) table.getColumns().get(2);
        // get the actual value of the currently selected cell
        ObservableValue ov =  toff.getCellObservableValue(row);
        
        String off = (String)ov.getValue();
        
        String cellvalue = "";
       
        Iterator<TableDescriptor> tbls = table.job.headers.iterator();
    	while(tbls.hasNext()){
    		TableDescriptor td = tbls.next();
    		
    		// What is the SQLType of the selected cell?
    		if(td.tblname.equals(table.tablename)) {
        		coltype = td.getSqlTypeForColumn(columnname);
            	
    		}	
    	}    	
    
    	if (null != coltype)
    		coltype = coltype.toUpperCase();
    	
      
        // not null-check: provide empty string for nulls
		if (observableValue != null) {			
			
			cellvalue = (String)observableValue.getValue();
			
			/**
			 *  Logic is exactly the same as in  CustomToolTip.setToolTipText():
			 */
		
	    	if(coltype.equals("REAL") || coltype.equals("DOUBLE") || coltype.equals("FLOAT")) {

	        	int point = -1;
	        	
	        	point = cellvalue.indexOf(",");
	        	
	        	if (point < 0)
	        		point = cellvalue.indexOf(".");

	        	
	        	String firstpart;
	        	if (point > 0)
	            	firstpart = cellvalue.substring(0, point);
	            else
	            	firstpart = cellvalue;
	           
	        	String value = Auxiliary.int2Timestamp(firstpart);
	        	setContent("[" + coltype + "] " +  cellvalue + "\n" + value );
	        	return;
	    	}
	    	
	    	if(coltype.equals("INTEGER") || coltype.equals("INT") || coltype.equals("BIGINT") || coltype.equals("LONG") || coltype.equals("TINYINT") || coltype.equals("INTUNSIGNED") || coltype.equals("INTSIGNED") || coltype.equals("MEDIUMINT")) {
	    		        	
	    		String value = Auxiliary.int2Timestamp(cellvalue);
	    		setContent("[" + coltype + "] " +  value + "\n" + value );
	    		return;
	    	}
	    	
	    	String s = cellvalue;
	    	
	    	if(s.contains("[BLOB")){
	    		
	         	int from = s.indexOf("BLOB-");
	    	    int to = s.indexOf("]");
	    	    String number = s.substring(from+5, to);
	    	    String shash = off + "-" + number;
	    	    
	    	    if(s.contains("jpg"))
	    	    	shash += ".jpg";
	    	    else if(s.contains("png"))
	    	    	shash += ".png";
	    	    else if(s.contains("gif"))
	    	    	shash += ".gif";
	    	    else if(s.contains("bmp"))
	    	    	shash += ".bmp";
	    	    
	    	    String key = GUI.baseDir + Global.separator + job.filename + "_" +  shash;
	    	    System.out.println(key);
	    	    
	    	    
	    	    boolean bson 	 = false;
	    	    boolean fleece 	 = false;
	    	    boolean msgpack  = false;
	    	    boolean thriftb  = false;
	    	    boolean thriftc  = false;
	    	    boolean protobuf = false;
	    	    
	    	    /* check, wether the user has activated a converter for binary 
	    	     * columns for this table 
	    	     */
	    	    if (job.convertto.containsKey(tablename)){
	    	    	
	    	    	String con = job.convertto.get(tablename);
	    	    	
	    	    	switch(con){
	    	    	
		    	    	case Names.BSON   : 	 	bson = true;
		    	    					    	 	break;
		    	    	case Names.Fleece : 	 	fleece = true;
						  						 	break;
		    	    	case Names.MessagePack  : 	msgpack = true;
						  						 	break;
		    	    	case Names.ProtoBuffer	: 	protobuf = true;
		    	    							 	break;
		    	    	case Names.ThriftBinary : 	thriftb = true;
							 					 	break;
		    	    	case Names.ThriftCompact : 	thriftc = true;
						 						 	break;
		    	    	
	    	    	}
	    	    }
	    	    
	    	   
	    	    {	
	    	    	
	    	    	if(s.contains("java"))
	    	    	{
	    	    		String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".bin";
	    	    		
	    	    		if (Auxiliary.writeBLOB2Disk(job, path)) {
	    	    			
	    	    			String javaclass = Deserializer.decode(path); 
		    	           	setContent(javaclass);
		    	  			return;
	    	    		}
		    	            
	    	    	}    	
	    	    	else if(s.contains("plist"))
	    	    	{
	    	            String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".plist";
	    	           	
	    	            	String plist = BPListParser.parse(job,path); 
	    	      	
	    	            	setContent(plist);
	    	            	return;
	    	          
	    	    	}
	    	    	else if(s.contains("avro")) {
	    	    		
	    	    		  try {
	    	    			  
	    	    			  String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".avro";
	    	    		     
	    	    			  if (Auxiliary.writeBLOB2Disk(job, path)) {
	    	    	    			
		    	      	    		
		    	    			  String buffer = Avro.decode(path);
		     	    	          setContent(buffer);
		     	    	          return;
	    	    			  }
		     	    	          
	    	    		  } catch (Exception e) {
	    	    		    throw new RuntimeException(e);
	    	    		  }
	    	    		
	    	    		
	    	    	}
	    	    	
	    	    	else if(fleece)
	    	    	{
	    	        	String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".bin";
	    	            System.out.println("offset :" + path);

	    	            if (Auxiliary.writeBLOB2Disk(job, path)) {
		    	        	/* inspection is enabled */
		    	        	String result = ConverterFactory.build(Names.Fleece).decode(job,path);
		    	            
		    	            System.out.println(result);
		    	        	if (null != result) {
		    	                setContent(result);
				    	        return;
		    	        	}
				    	    else {
				    	        setContent("invalid value");   
				    	        return;
				    	    }
	    	            }
	    	    	}
	    	    	
	    	    	else if(protobuf)
	    	    	{
	    	        	String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".bin";
	    	
	    	        	if (Auxiliary.writeBLOB2Disk(job, path)) {    
		    	        	/* inspection is enabled */
		    	        	String buffer = ConverterFactory.build(Names.ProtoBuffer).decode(job,path);
		    	        	setContent(buffer);
		 	    	        return;
	    	        	}
	    	    	}
	    	    	
	    	    	else if(thriftb)
	    	    	{
	    	        	String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".bin";
	    	        	
	    	        	if (Auxiliary.writeBLOB2Disk(job, path)) {    
		    	        	/* inspection is enabled */
		    	        	String buffer = ConverterFactory.build(Names.ThriftBinary).decode(job,path);
		    	        	setContent(buffer);
		 	    	 	    return;
	    	        	}
	    	    	}
	    	    	
	    	    	else if(thriftc)
	    	    	{        	    	
	    	        	String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".bin";
	    	        	
	    	        	if (Auxiliary.writeBLOB2Disk(job, path)) {        
		    	        	/* inspection is enabled */
		    	        	String buffer = ConverterFactory.build(Names.ThriftCompact).decode(job,path);      
			    	        setContent(buffer);
			    	        return;
	    	        	}
	    	    	}
	    	    	
	    	    	else if(msgpack)
	    	    	{
	    	        	String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".bin";
	    	        	
	    	        	if (Auxiliary.writeBLOB2Disk(job, path)) {        
		    	        	/* inspection is enabled */
		    	        	String buffer = ConverterFactory.build(Names.MessagePack).decode(job,path);
		    	            setContent(buffer);
		    	            return;
	    	        	}
	    	        }
	    	    	
	    	    	else if(bson) {
	    	    		String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".bin";
	    	        	
	    	        	if (Auxiliary.writeBLOB2Disk(job, path)) {        
		    	        	/* inspection is enabled */
		    	        	String buffer = ConverterFactory.build(Names.BSON).decode(job,path);
		    	        	setContent(buffer);
		    	        	return;
	    	        	}
			    	  
	    	       }
	    	    	/* this binary formats cannot be viewed inside a ToolTip. We need to call the viewer from
	    	    	 * the operating system.
	    	    	 */
	    	        else if(s.contains("pdf") || s.contains("heic") || s.contains("tiff")) {
	    	    	       
		    	        setContent("double-click to preview.");
		    	        return;
			    	  
	    	    	}
	    	     	else
	    	    	{
	    	            
	    	            String fext = ".bin";
	    	         
	    	            if(s.contains("<tiff>"))
	    	            		fext = ".tiff";
	    	            else if(s.contains("<pdf>"))
	    	            		fext = ".pdf";
	    	            else if(s.contains("<heic>"))
		            		fext = ".heic";
	    	            else if(s.contains("<gzip>"))
		            		fext = ".gzip";
	    	            else if(s.contains("<avro>"))
	    	            	fext = ".avro";
	    	            else if(s.contains("<jpg>"))
	    	            	fext = ".jpg";
	    	            else if(s.contains("<bmp>"))
	    	            	fext = ".bmp";
	    	            else if(s.contains("<png>"))
	    	            	fext = ".png";
	    	            else if(s.contains("<gif>"))
	    	            	fext = ".gif"; 
	    	            else if(s.contains("<gzip>"))
	    	            	fext = ".gzip"; 
	    	            else if(s.contains("<plist>"))
	    	            	fext = ".plist"; 
	    	       
	             
	    	            String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + fext;
	    	            //String text = job.bincache.getHexString(path);
	    	    		String text = job.bincache.getASCII(path);
	    	            setContent(text);
	     	            return;
	    	    	}
	    	    
	    	    }

	    	} // end of BLOB branch
	    	
	    	setContent("[" +coltype + "] " + tc.getText());
	    	return;			
		} // end of value != null condition
        
	    setContent("null");
	}
	
	private void setContent(String data){
		//System.out.println(" Data::" + data);
	 	content.putString(data);
		clipboard.setContent(content);
	}
	
	
	/**
	 * Action handler method.   
	 * @param table
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void copyCellAction(FQTableView table){

	 	final javafx.scene.input.Clipboard clipboard = javafx.scene.input.Clipboard.getSystemClipboard();
        final ClipboardContent content = new ClipboardContent();
                 
        ObservableList<TablePosition> selection = table.getSelectionModel().getSelectedCells();
        if (selection.size() == 0)
        	return;
        TablePosition tp = selection.get(0); 
        int row = tp.getRow();
		int col = tp.getColumn();

        
        TableColumn tc = (TableColumn) table.getColumns().get(col);
        ObservableValue observableValue =  tc.getCellObservableValue(row);
		
        String cellvalue = "";
        
		// not null-check: provide empty string for nulls
		if (observableValue != null) {			
			cellvalue = (String)observableValue.getValue();
			
			// handle binary values like protocol buffers, java serials or property lists
			if (cellvalue.startsWith("[BLOB-"))
			{
			    int from = cellvalue.indexOf("BLOB-");
	        	int to = cellvalue.indexOf("]");
	        	String number = cellvalue.substring(from+5, to);
	        	 
				
				int start = cellvalue.indexOf("<");
				int end   = cellvalue.indexOf(">");
				
				String type;
				if (end > 0) {
					type = cellvalue.substring(start+1,end);
				}
				else 
					type = "bin";
				
				if(type.equals("java"))
					type = "bin";
				
			    tc = (TableColumn) table.getColumns().get(2);
				ObservableValue off =  tc.getCellObservableValue(row);
					
				String path = GUI.baseDir + Global.separator + table.dbname + "_" + off.getValue() + "-" + number + "." + type;
				System.out.println("Clipboard-Path<1>:: " + path);
				String data = table.job.bincache.getHexString(path);
				System.out.println(" Data" + data);				
				content.putString(data.toUpperCase());
				clipboard.setContent(content);
				return;
			}
			
		}
    
        content.putString(cellvalue);
        clipboard.setContent(content);
       
        
	}
	
	final javafx.scene.input.Clipboard clipboard = javafx.scene.input.Clipboard.getSystemClipboard();
    final ClipboardContent content = new ClipboardContent();

	
	@SuppressWarnings("rawtypes")
	public void cellvalue2clipboard(String cellvalue, FQTableView table, int row){
		
	    
		int from = cellvalue.indexOf("BLOB-");
      	int to = cellvalue.indexOf("]");
      	String number = cellvalue.substring(from+5, to);
      	 
			
			int start = cellvalue.indexOf("<");
			int end   = cellvalue.indexOf(">");
			
			String type;
			if (end > 0) {
				type = cellvalue.substring(start+1,end);
			}
			else 
				type = "bin";
			
			if(type.equals("java"))
				type = "bin";
		
		    TableColumn tc = (TableColumn) table.getColumns().get(2);
			ObservableValue off =  tc.getCellObservableValue(row);
				
			String path = GUI.baseDir + Global.separator + table.dbname + "_" + off.getValue() + "-" + number + "." + type;
			System.out.println("Clipboard-Path<2>:: " + path);
			String data = table.job.bincache.getHexString(path);
			System.out.println(" Data" + data);				
			content.putString(data.toUpperCase());
			clipboard.setContent(content);
			return;
	}
	
	
	/**
	 * This method handles the "copy lines" action. All data of all selected lines
	 * is copied to the system clipboard. BLOBs will be completely extracted from
	 * the file cache.   
	 * @param table
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void copyLineAction(FQTableView table){
		
		StringBuffer sb = new StringBuffer();			
	 	final javafx.scene.input.Clipboard clipboard = javafx.scene.input.Clipboard.getSystemClipboard();
        final ClipboardContent content = new ClipboardContent();
        ObservableList<TablePosition> selection = table.getSelectionModel().getSelectedCells();	        
        Iterator<TablePosition> iter = selection.iterator();
        
    	String token = Global.CSV_SEPARATOR;
		
		if(token.equals("[TAB]"))
			token = "\t";
	
          
        while(iter.hasNext()) {
        	
        	TablePosition pos = iter.next();	        	
        	ObservableList<String> hl = (ObservableList<String>)table.getItems().get(pos.getRow());
        	
        	Iterator<String> s = hl.iterator();
        	
        	int current = 0;
        	String offset = null;
        	
        	// BLOB handling
        	while (s.hasNext()){ 
        	
        		/* column for offset found ? */
        		if(current == 5)
        		{
        			offset = s.next();
        
        			sb.append(token);
        			sb.append(offset);
        			current++;
        			continue;
        		}	
        		
        		String cellvalue = s.next();

    		    /* BLOB-value found ? */     		
        		if(cellvalue.length()>7) {
        			
	        		
        			int from = cellvalue.indexOf("BLOB-");
	        	    int to = cellvalue.indexOf("]");
	        	    
	        	    if(from > 0 && to > 0)
	        	    {
	        	    
		        	    String number = cellvalue.substring(from+5, to);			
		        		int start = cellvalue.indexOf("<");
		        		int end   = cellvalue.indexOf(">");
		        				
		        		String type;
						if (end > 0) {
							type = cellvalue.substring(start+1,end);
						}
						else 
		        			type = "bin";
		        				
		        		if(type.equals("java"))
		        			type = "bin";
		        			
		        		String path = GUI.baseDir + Global.separator + table.dbname + "_" + offset + "-" + number + "." + type;
		    			System.out.println("Clipboard-Path<3>:: " + path);
		        		String data = table.job.bincache.getHexString(path);
		        		System.out.println(" Data" + data);				
		        		cellvalue = data.toUpperCase();
	        	    }
	        	}
        		
        		if(current > 0)
        			sb.append(token);
        		sb.append(cellvalue);	
                current++;
        	}
        	sb.append("\n");
        }
        content.putString(sb.toString());
        clipboard.setContent(content);

	}
	

	/**
	 * Returns the Path object of for a given treeNode.
	 * @param treeNode
	 * @return TreePath 
	 */
	public static TreePath getPath(TreeNode treeNode) {
		List<Object> nodes = new ArrayList<Object>();
		if (treeNode != null) {
			nodes.add(treeNode);
			treeNode = treeNode.getParent();
			while (treeNode != null) {
				nodes.add(0, treeNode);
				treeNode = treeNode.getParent();
			}
		}

		return nodes.isEmpty() ? null : new TreePath(nodes.toArray());
	}

	/**
	 * Returns the tree-path for a given node item. 
	 * @param item
	 * @return path as string
	 */
	private String getPath(TreeItem<NodeObject> item)
	{
		

		// create the entire path to the selected item.
        String path = item.getValue().name;
        TreeItem<NodeObject> tmp = item.getParent();

        
        if(tmp == null)
        {	
            try {
            	int i = 1/0;
            	System.out.println("" + i);
            }catch(Exception err) {
            	err.printStackTrace();
            }
            
        }	
        	
        while (tmp != null) {
           path = tmp.getValue().name + "/" + path;
           tmp = tmp.getParent();
        }

		return path;
		
	}
	
	/**
	 * Returns the currently selected tree node.
	 * @return
	 */
	public NodeObject getSelectedNode()
	{
	   return tree.getSelectionModel().getSelectedItem().getValue();	
	}
	
	/**
	 * Sometimes we need an empty table as placeholder.
	 * 
	 */
	@SuppressWarnings("rawtypes")
	protected void prepare_tree() {

		if (tree == null) {
			tree = new TreeView<NodeObject>(root);
		 	tree.getSelectionModel().setSelectionMode(SelectionMode.SINGLE);
			//tree.setCellRenderer(new CustomTreeCellRenderer());
		}
		
		rightSide.getChildren().add(rootPane);

		tree.getSelectionModel().selectedItemProperty().addListener( new ChangeListener<TreeItem>() {

		        @SuppressWarnings({ "unchecked" })
				@Override
		        public void changed(ObservableValue observable, TreeItem oldValue, TreeItem newValue) {

		       
		            TreeItem<NodeObject> selectedItem = (TreeItem<NodeObject>) newValue;
		            if (null == selectedItem)
		            {
		            	// after closeAll() we have to set back everything
		            	Platform.runLater(new Runnable() {
		            	    @Override
		            	    public void run() {
		            	    	rightSide.getChildren().clear();
				            	rightSide.getChildren().add(rootPane);      	
		            	    }
		            	});
		            	return;
		            }	
		          
		            NodeObject node = selectedItem.getValue();
		            if (null != node.tablePane)
		            {
		            	Platform.runLater(new Runnable() {
		    	            @Override public void run() {
		    	    
		            	
		    	            		rightSide.getChildren().clear();
		    	            		rightSide.getChildren().add(node.tablePane);
		    	            		
		    	            		VBox.setVgrow(node.tablePane,Priority.ALWAYS);

		    	            }
		            	});
		            }
		            else // no table -> show db pane
		            {
		            	
		            	if(node.isRoot){
		            		rightSide.getChildren().clear();
			            	rightSide.getChildren().add(rootPane);
			            }
		            	else{


							    //Platform.runLater(() -> {
								String tp = getPath(selectedItem);
							 	StackPane dbpanel  = (StackPane)tables.get(tp);
							 
							 	rightSide.getChildren().clear();
							 	if(null != dbpanel)
							 	{
							 		rightSide.getChildren().add(dbpanel);		
							 		dbpanel.setPrefHeight(4000);
							 		VBox.setVgrow(dbpanel,Priority.ALWAYS);
							 	}
							
							 
							 	//});
			            }
		            }
		        }

		});
		
	}
	

	/**
	 * Show an open dialog and import <code>sqlite</code>-file.
	 * After selecting a file the import will be automatically
	 * started.
	 *  
	 */
	public synchronized void open_db(File f) {
		File file = f;
		
		if (file == null) {
			FileChooser fileChooser = new FileChooser();
			fileChooser.getExtensionFilters().addAll(
				    new FileChooser.ExtensionFilter("<all>", "*.*")
					,new FileChooser.ExtensionFilter(".sqlite", "*.sqlite")
				    ,new FileChooser.ExtensionFilter(".db", "*.db")
				);
		    file = fileChooser.showOpenDialog(this.stage);
		    if (file != null) {
                fileChooser.setInitialDirectory(file.getParentFile());
            }
		}

			
		if (file == null)
			return;
		
		
			
		/* check file size - the size has to be at least 512 Byte */
		if (file.length() < 512)
		{
			Alert alert = new Alert(AlertType.ERROR);
			alert.setTitle("Error");
			alert.setContentText("File size is smaller than 512 bytes. Import stopped.");
			alert.showAndWait();		
			return;
		}
		else if (file.length()> 8000000000L)
		{
			Alert alert = new Alert(AlertType.ERROR);
			alert.setTitle("Error");
			alert.setContentText("File is too large. Import stopped.");
			alert.showAndWait();		
			return;
		}
	
		RandomAccessFile raf = null;
		boolean abort = false;
		/* check header string for magic number to match */
		try 
		{
			raf = new RandomAccessFile(file,"r");
			byte h[] = new byte[16];
			raf.read(h);
			if (!Auxiliary.bytesToHex3(h).equals(Job.MAGIC_HEADER_STRING)) 
			{
				abort = true;
				Alert alert = new Alert(AlertType.ERROR);
				alert.setTitle("Error");
				alert.setContentText("Couldn't find a valid SQLite3 magic. Import stopped.");
				alert.showAndWait();
				
			}
			

			/**
			 * Compute the entropy for the first bytes of the database.
			 * This is where the schema definition normally resides. If
			 * the entropy is higher than 7.5 is is definitely encrypted. 
			 */
			
			
			byte begin[];
			if (raf.length()>= 4096)
				begin = new byte[4000];
			else
				begin = new byte[400];
					
				raf.read(begin);
				double entropy = Auxiliary.entropy(begin);
				System.out.println("Entropy ::" + entropy);
				if (entropy > 7.5){
					abort = true;
					Alert alert = new Alert(AlertType.ERROR);
					alert.setTitle("Error");
					alert.setContentText("The database file seems to be encrypted! \n Entropy value : " + entropy + " \n Import stopped.");
					alert.showAndWait();
			
				}
			
			
		}
		catch(Exception err)
		{
			Alert alert = new Alert(AlertType.ERROR);
			alert.setTitle("Error");
			alert.setContentText("IO-Exception. Cloud not open file.");
			alert.showAndWait();
			abort = true;
		}
		finally		
		{
			try { raf.close(); } catch(IOException err){}
		}
	    /* no valid file or no permissions -> cancel import */
		if (abort)
			return;
		
		FileInfo info = new FileInfo(file.getAbsolutePath());
		
		DBPropertyPanel panel = new DBPropertyPanel(this, info,file.getName());
		panel.setPrefHeight(4000);
		VBox.setVgrow(panel,Priority.ALWAYS);
		
			
		NodeObject o = new NodeObject(file.getName(), null, -1, FileTypes.SQLiteDB, 99);
		TreeItem<NodeObject> dbNode = new TreeItem<NodeObject>(o); 
		dbNode.setGraphic(createFadeTransition("loading..."));	
		root.getChildren().add(dbNode);
		
		/* insert Panel with general header information for this database */
		String tp = getPath(dbNode);
			
		Job job = new Job();
		tables.put(tp, panel);
		dbnames.add(file.getName());
			
			
		/* Does a companion RollbackJournal exist ? */
		if (doesRollbackJournalExist(file.getAbsolutePath()) > 0) {
				NodeObject ro = new NodeObject(file.getName() + "-journal", null, -1,
						FileTypes.RollbackJournalLog, 100);
				rjNode = new TreeItem<NodeObject>(ro);
				dbnames.add(file.getName()+ "-journal");

				rjNode.setGraphic(createFadeTransition("loading..."));
				
				root.getChildren().add(rjNode);

				/* insert Panel with general header information for this database */
				String tpr = getPath(rjNode);
				FileInfo rinfo = new FileInfo(file.getAbsolutePath()+"-journal");
				RollbackPropertyPanel rpanel = new RollbackPropertyPanel(rinfo);
				tables.put(tpr, rpanel);

				job.rjNode = rjNode;
				job.setRollbackPropertyPanel(rpanel);
				ro.job = job;
				ro.job.readRollbackJournal = true;
				ro.job.readWAL = false;

		}

		/* Does a companion WAL-archive exist ? */
		else if (doesWALFileExist(file.getAbsolutePath()) > 0) {

			NodeObject wo = new NodeObject(file.getName() + "-wal", null, -1, FileTypes.WriteAheadLog, 101);
			walNode = new TreeItem<NodeObject>(wo);
			walNode.setGraphic(createFadeTransition("loading..."));
			dbnames.add(file.getName()+ "-wal");

			root.getChildren().add(walNode);

			/* insert Panel with general header information for this database */
			String tpw = getPath(walNode);
			FileInfo winfo = new FileInfo(file.getAbsolutePath()+"-wal");
			WALPropertyPanel wpanel = new WALPropertyPanel(winfo,this);
			tables.put(tpw, wpanel);
			job.walNode =  walNode;
			
			job.setWALPropertyPanel(wpanel);
			wo.job = job;
			wo.job.readWAL = true;
			wo.job.readRollbackJournal = false;

		}

		tree.refresh();
	
		job.setPropertyPanel(panel);
		job.setGUI(this);
		job.setTreeItem(dbNode);
		job.setPath(file.getAbsolutePath());
		Importer.createAndShowGUI(this, file.getAbsolutePath(), job, dbNode);
		o.job = job;	

		int idx = tree.getRow(dbNode);
	
		tree.getSelectionModel().select(idx);
		tree.scrollTo(idx);
	
	}
	
	/**
	 * During import a fading "loading.." message is shown. This method 
	 * creates an label for this purpose.
	 * @param msg
	 * @return
	 */
	private Label createFadeTransition(String msg) 
	{
		  Label l = new Label(msg);
		  FadeTransition fadeTransition = new FadeTransition(Duration.seconds(1.0),l);
	      fadeTransition.setFromValue(1.0);
	      fadeTransition.setToValue(0.0);
	      fadeTransition.setCycleCount(Animation.INDEFINITE);
	        fadeTransition.play();
	      return l;
	}

	/**
	 * Try to find out, if there is a companion WAL-Archive. This file can be found
	 * in the same directory as the database file and also has the same name as the
	 * database, but with 4 character added to the end – “-wal”
	 * 
	 * 
	 * @param dbfile
	 * @return
	 */
	public static long doesWALFileExist(String dbfile) {
		
		
		String walpath = dbfile + "-wal";
		Path path = Paths.get(walpath);

		if (Files.exists(path, new LinkOption[] { LinkOption.NOFOLLOW_LINKS }))
			try {
				return Files.size(path);
			} catch (IOException e) {
				e.printStackTrace();
			}

		return -1L;

	}

	/**
	 * Try to find out, if there is a companion RollbackJournal-File. This file can
	 * be found in the same directory as the database file and also has the same
	 * name as the database, but with 8 character added to the end – “-journal”
	 * 
	 * 
	 * @param dbfile
	 * @return file size of the rollback-journal archive or -1 if no -journal files exists. 
	 */
	public static long doesRollbackJournalExist(String dbfile) {
		String rolpath = dbfile + "-journal";
		Path path = Paths.get(rolpath);

		if (Files.exists(path, new LinkOption[] { LinkOption.NOFOLLOW_LINKS }))
			try {
				return Files.size(path);
			} catch (IOException e) {
				e.printStackTrace();
			}

		return -1L;

	}

	/**
	 * Print a new message to the trace window on the bottom of the screen. 
	 * @param message
	 */
	protected void doLog(String message) {
		AppLog.info(message);
	}

	/**
	 * This method is called to write the contents of a database to a CSV file. 
	 * 
	 * @param no Database node for export
	 */
	private void export_table(NodeObject no) {

		if (null == no)
			return;
			
		FileChooser fileChooser = new FileChooser();
		fileChooser.setTitle("Export Element");	
        fileChooser.setInitialFileName(prepareDefaultFileName(no.name));
		File f = fileChooser.showSaveDialog(stage);
        
		if(null == f)
			return;
		
		boolean success = false;	
		
		switch(no.tabletype)
		{
			case 0      :	// table 					   
			case 1      :   // index
					
						success = no.job.exportResults2File(f, no.name,	no.type, ExportType.SINGLETABLE);
						break;
						
			case 99  :   // database
				
						 success = no.job.exportResults2File(f, no.name, no.type, ExportType.SQLITEDB);
						 break;
					
			case 100 :   // journal
					 			
						 success = no.job.exportResults2File(f, no.name, no.type, ExportType.ROLLBACKJOURNAL);
						 break;
			
				
			case 101 :   // wal 
					    
						 success = no.job.exportResults2File(f, no.name, no.type, ExportType.WALARCHIVE);
						 break;
						 
			default  :  // root;		
		}
		
		if(success) {
			Alert alert = new Alert(AlertType.INFORMATION);
			alert.setTitle("Success Info");
			alert.setContentText("Data of " + no.name + " exported successfully to \n" + f.getAbsolutePath());
			alert.showAndWait();
		}
		
		
	}
	
	/**
	 * Returns a filename with a time stamp in ISO_DATE_TIME format.
	 * @param nameofnode
	 * @return
	 */
	private String prepareDefaultFileName(String nameofnode){
		LocalDateTime now = LocalDateTime.now();
		DateTimeFormatter df;
		df = DateTimeFormatter.ISO_DATE_TIME; // 2020-01-31T20:07:07.095
		String date = df.format(now);
		date = date.replace(":","_");
		return nameofnode + date + ".csv";
	}
	
	
	/**
	 * Prepares the table row - with all columns - for a TableView.  
	 * @param tp 
	 * @param linenumber the current line number (we put this number in front of the datarecord).
	 * @param data the actual line of data to fill in
	 * @param isWALTable whether or not the table to built is a WAL-Table 
	 * @return
	 */
	@SuppressWarnings("unused")
	private ObservableList<String> prepareRow(int linenumber, LinkedList<String> row,boolean isWALTable)
	{
				
		ObservableList<String> list = FXCollections.observableArrayList(row);
		list.add(0,String.valueOf(++linenumber));	
		return list;
	}
	
	
	/**
	 * This method is used to insert new records into a output table. 
	 * 
	 * @param tp  the table name
	 * @param data  String array with data rows
	 * @param isWALTable whether or not this table to fill is a WAL-Table
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void update_table(String treepath, ObservableList<ObservableList<String>> rows, boolean isWALTable) {
		
		System.out.println("Treepath ::: " + treepath);
		datasets.put(treepath, rows);
		
		int linenumber = 0;
		Iterator<ObservableList<String>> rt = rows.iterator();
		while(rt.hasNext()) {
			rt.next().add(0,String.valueOf(++linenumber));	
		}

		
		// define array list for all table rows 
		ObservableList<ObservableList> obdata = FXCollections.observableArrayList();
		//ObservableList<ObservableList> obdata = FXCollections.observableList(rows);
		for (int i = 0; i < rows.size(); i++){
			obdata.add(rows.get(i));				
		}
		// first get the right table
	    FQTableView tb = null;
		TextField filterField = null;
		ComboBox columnselector;
		Button clearFilter;
		
		try {	
			
			VBox tablepanel = (VBox)tables.get(treepath);	
			VBox.setVgrow(tablepanel,Priority.ALWAYS);
			HBox filterpane = (HBox)tablepanel.getChildren().get(0);
		    columnselector = (ComboBox) filterpane.getChildren().get(0);
			filterField = (TextField) filterpane.getChildren().get(1);
			clearFilter  = (Button) filterpane.getChildren().get(2);
		    tb = (FQTableView)tablepanel.getChildren().get(1);  
		    Label statusline = (Label)tablepanel.getChildren().get(2);
		    String text = statusline.getText();
		    statusline.setText(text + " | rows: " + rows.size());
		} catch (Exception err) {
			System.err.println(err);
			return ;
		}
		
		/* Just in case ;-) */
		if (tb == null) {
			doLog(">>>> Unkown tablename" + treepath);
			return;
		}
			
		// determine the right treeitem for a given treepath from hashtable
		TreeItem<NodeObject> node = treeitems.get(treepath);
		
		if (null != node && rows.size()>0 )
		{

			node.getValue().hasData = true;
			
			Platform.runLater(new Runnable() {
	            @Override public void run() {
	            	String s = null;
	            	if (node.getValue().tabletype == 0) // normal table with rows
	            		s = GUI.class.getResource("/table-icon.png").toExternalForm();  
	            	if (node.getValue().tabletype == 1) // index table with rows
	            		s = GUI.class.getResource("/table-key-icon.png").toExternalForm();  	            	
	            	if (null == s)
	            		return;
	            	ImageView iv = new ImageView(s);
	    			node.setGraphic(null);
	    			node.setGraphic(iv); 
	    			TreeItem.graphicChangedEvent();
	    			TreeItem.valueChangedEvent();
	    			tree.refresh();
	    		}
	        });
		
		}
		
		final TextField ff = filterField;
		final ComboBox cs = columnselector;
		clearFilter.setOnAction(e ->{ ff.clear(); cs.getSelectionModel().select(0); });
		
		
	    // iterate over row array to create table rows  
		//for(int i = 0; i < rows.size(); i++)
		//{
		//	LinkedList<String> data = rows.get(i);
			// rearrange the row cell values for table view
		//	obdata.add(FXCollections.observableList(prepareRow(i,data,isWALTable)));
			
		//}
		
		Iterator it = tb.getColumns().iterator();
		ArrayList<String> cnames = new ArrayList<String>();
		while (it.hasNext()){
			TableColumn tc = (TableColumn) it.next();
			cnames.add(tc.getText().toLowerCase());
		}
		
		final List<String> fnames = cnames;
		
		// 1. Wrap the ObservableList in a FilteredList (initially display all data).
		FilteredList<ObservableList> filteredData = new FilteredList<>(obdata, p -> true);

		final TextField  ffield = filterField; 
		/* if column selection for column filter has changed -> do start new filtering */
		columnselector.getSelectionModel().selectedItemProperty().addListener((options, oldValue, newValue) -> {
			filter(treepath, fnames, columnselector, filteredData, null, ffield.textProperty().getValue());				 
			updatestatusline(treepath,filteredData.size(),rows.size());
		}); 
		
		// 2. Set the filter predicate whenever the filter changes.
		filterField.textProperty().addListener((observable, oldValue, newValue) -> {						
			  filter(treepath, fnames, columnselector, filteredData, oldValue, newValue);
			  updatestatusline(treepath,filteredData.size(),rows.size());	
		});	
				
		// 3. Wrap the FilteredList in a SortedList. 
		SortedList<ObservableList> sortedData = new SortedList<>(filteredData);
				
		// 4. Bind the SortedList comparator to the TableView comparator.
		sortedData.comparatorProperty().bind(tb.comparatorProperty());
		
		// 5. Add sorted (and filtered) data to the table & update TableView with data set
		tb.setItems(sortedData);
		
		final TableView tb2 = tb;
		
		// 6. Update Status line 
		tb.getSelectionModel().getSelectedItems().addListener((ListChangeListener.Change c) -> {
         
			   if (tb2 != null){
		    	   int selecteditems = tb2.getSelectionModel().getSelectedCells().size();
				   VBox tablepanel = (VBox)tables.get(treepath);	
				   Label statusline = (Label)tablepanel.getChildren().get(2);
				   String text = statusline.getText();
				   int idx = text.indexOf(" | rows: ");
				   if (idx > 0)
					   statusline.setText(text.substring(0,idx) + " | rows: " + rows.size() + " | selected rows: " + selecteditems);	
		       }
		    	   
		    }
			
        );
 				
	}

	
	private int filter(String treepath, List<String> fnames, ComboBox<String> columnselector, @SuppressWarnings("rawtypes") FilteredList<ObservableList> filteredData, String oldValue, String newValue){		
		
		filteredData.setPredicate(r -> {
		
			// Compare the column values of all row columns with filter text.
			String lowerCaseFilter = newValue.toLowerCase();
			
			String clvalue = (String)columnselector.getSelectionModel().getSelectedItem();
			
			String searchfor = "";
			String cname = "";
			
			if((clvalue != null && !clvalue.startsWith("All Columns")))
			{
				//case: special column is selected
				
				if (clvalue.equals("Status"))
					clvalue = "";
				
				if (clvalue != null) {
					cname = clvalue.toLowerCase();
					searchfor = lowerCaseFilter;
				}
				
				searchfor = searchfor.trim();
				System.out.println("searchfor " + searchfor);
				System.out.println("cname "+ cname);
				int cnumber = fnames.indexOf(cname);
				cnumber++;		
				
				if (r.size()>cnumber) {
				
					switch(cname) {
						
						case "pll|hl"  : cnumber = 2;
						break;
						
						case "rowid"   : cnumber = 3;
						break;
					
						case ""     : cnumber = 4;
						break;
						
						case "offset"   : cnumber = 5;
						break;
					
					}
					
					String value = (String)r.get(cnumber);
					System.out.println("Value " + value + "Search for " + searchfor);
					if (StringUtils.containsIgnoreCase(value, searchfor))		
					{	
						return true;
					}
				}
			}
			else {
				// case: search all columns in a row
				for (int cc = 0; cc < r.size(); cc++) {
					// get next row
					String value = (String)r.get(cc);
					{
						if (StringUtils.containsIgnoreCase(value, newValue))	
						{
							return true; // Filter matches name
						}
					}
				}
	
			}
			
			return false; // Does not match.
			
		}); // end of setPredicate()
		
		return filteredData.size();
	} // end of filter()

	private void updatestatusline(String treepath,int number, int total){
		   
		    VBox tablepanel1 = (VBox)tables.get(treepath);	
		    Label statusline1 = (Label)tablepanel1.getChildren().get(2);
			String text1 = statusline1.getText();
			int idx1 = text1.indexOf(" | ");
			if (idx1 > 0)
				statusline1.setText(text1.substring(0,idx1) + " | showing " + number + " of "+ total + " rows ");
			else
				statusline1.setText(" | showing " + number + " of "+ total + " rows ");
	}
	
	

	
	
	public Hashtable<Object, String> getRowcolors() {
		return rowcolors;
	}

	public void setRowcolors(Hashtable<Object, String> rowcolors) {
		this.rowcolors = rowcolors;
	}

	

	private class CustomComparator implements Comparator<String>{

	    @Override
	    public int compare(String o1, String o2) {
	    
	    	if (o1 == null && o2 == null) 
	    		return 0;
	        if (o1 == null) 
	        	return -1;
	        if (o2 == null) 
	        	return 1;

	        if (o1.length() == 0)
	        	return -1;
	        
	        char ch = o1.charAt(0);
	       
	        // only if o1 start with a number or a sign
	        if((ch >= '0' && ch <= '9') || ch =='-' || ch =='+')
	        {
	        	Integer i1=null;
	        		try{ i1=Integer.valueOf(o1); } catch(NumberFormatException ignored){}
	        	Integer i2=null;
	        		try{ i2=Integer.valueOf(o2); } catch(NumberFormatException ignored){}

	        	if(i1==null && i2==null) 
	        		return o1.compareTo(o2);
	        	if(i1==null) 
	        		return -1;
	        	if(i2==null) 
	        		return 1;

	        	return i1-i2;
	        }
	        
	        // o1 does not start with a number -> compare String objects as usual 
	        return o1.compareTo(o2);
	    }
	}
	

} // End of class GUI


