package fqlite.base;

import java.awt.AWTException;
import java.awt.SystemTray;
import java.awt.Taskbar;
import java.awt.TrayIcon;
import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import javax.swing.*;

import fqlite.descriptor.TableDescriptor;
import fqlite.export.SQLiteDatabaseCreator;
import fqlite.sqlcipher.*;
import fqlite.timemap.LocationWindow;
import fqlite.erm.MermaidHTMLGenerator;
import fqlite.erm.SchemaRetriever;
import fqlite.erm.SchemaToMermaidConverter;
import fqlite.fts.SearchDialog;
import fqlite.location.GPSParser;
import fqlite.location.GeoCoordinate;
import fqlite.rag.*;
import fqlite.sql.DBManager;
import fqlite.sql.InMemoryDatabase;
import fqlite.types.*;
import fqlite.ui.*;
import fqlite.hex.HexViewManager;
import fqlite.viewer.parser.FormatDetector;
import fqlite.viewer.ui.BinViewer;
import fqlite.viewer.util.HexDump;
import javafx.animation.PauseTransition;
import javafx.application.HostServices;
import javafx.geometry.Orientation;
import javafx.scene.CacheHint;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.text.*;
import fqlite.log.AppLog;
import fqlite.sql.SQLWindow;
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
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.TableColumn.CellDataFeatures;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.Dragboard;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.scene.input.MouseButton;
import javafx.scene.input.TransferMode;
import javafx.stage.FileChooser;
import javafx.stage.Modality;
import javafx.stage.Screen;
import javafx.stage.Stage;
import javafx.util.Callback;
import javafx.util.Duration;

/*
    ---------------
    GUI.java
    ---------------
    (C) Copyright 2026.

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
 * Most of the decoration code was generated manually.
 * If you want to start FQLite in graphic mode, you have to call this class.
 * It contains a main()-function.
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


	private enum Theme {DARK, LIGHT}

	Button themeBtn;
	Button filterBtn;
	private boolean filterActive = false;

	private record ThemeColors(
			String bgRoot, String bgPane, String bgControl, String bgToolbar,
			String bgSelected, String bgHover, String divider, String border,
			String textMain, String textMuted, String textTab, String textTabSel,
			String textArea, String textDropHint,
			String toggleBg, String toggleText, String toggleLabel
	) {
	}

	private static final GUI.ThemeColors DARK = new GUI.ThemeColors(
			"#1e2330", "#181d2a", "#181d2a", "#1a1f2e",
			"#3a4a7a", "#2d3a5e", "#2d3550", "#2d3550",
			"#c8d0e8", "#7080a8", "#8898c0", "#ffffff",
			"#a8c8a0", "#3a4a7a",
			"#2d3a5e", "#c8d0e8", "☀  Light"
	);

	private static final GUI.ThemeColors LIGHT = new GUI.ThemeColors(
			"#f0f2f8", "#ffffff", "#ffffff", "#e4e8f4",
			"#c0d0f0", "#dce4f8", "#b8c4e0", "#c8d0e0",
			"#1a2040", "#6070a0", "#4050a0", "#0a1030",
			"#1a3a1a", "#a0b0d0",
			"#d0d8f0", "#1a2040", "🌙  Dark"
	);


	private static int icon_size_in_pixels = 30;

	private GUI.Theme currentTheme = GUI.Theme.DARK;
	/**
	 * Callbacks notified whenever the user switches the theme.
	 */
	private final List<Runnable> themeListeners = new ArrayList<>();

	public static byte[] currently_selected_hex;

	public static File baseDir;

	public static int pos = 0;

	public static GUI mainwindow;
	public ConcurrentHashMap<String, javafx.scene.Node> tables = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<Object, String> rowcolors = new ConcurrentHashMap<>();

	public ConcurrentHashMap<String, ObservableList<ObservableList<String>>> datasets = new ConcurrentHashMap<>();

	protected ContextMenu cm = null;
	protected MenuBar menuBar;

	public List<String> dbnames = new ArrayList<>();

	SplitPane splitPane;
	final StackPane leftSide = new StackPane();
	final VBox rightSide = new VBox();
	public static HexViewManager HEXVIEW = HexViewManager.getInstance();

	static TreeView<NodeObject> tree;
	TreeItem<NodeObject> walNode;
	TreeItem<NodeObject> rjNode;
	TreeItem<NodeObject> root = new TreeItem<>(new NodeObject("Databases", true));

	ConcurrentHashMap<String, TreeItem<NodeObject>> treeitems = new ConcurrentHashMap<>();

	ImageIcon facewink;
	ImageIcon findIcon;
	ImageIcon errorIcon;
	ImageIcon infoIcon;
	ImageIcon questionIcon;
	ImageIcon warningIcon;

	StackPane rootPane = new StackPane();

	public Stage stage;
	Scene scene;
	public static VBox topContainer;

	/* Buttons for toolbar */
	Button btnSQL;
	Button btnLLM;
	Button btnLocation;
	Button btnExport;
	Button btnExportDB;
	Button hexViewBtn;
	Button btnHTML;
	Button btnSchema;
	Button btnFTS;
	Button btnCASE;

	MenuItem cmExport;
	MenuItem mntmExportDB;
	MenuItem mntmHex;
	MenuItem mntmSQL;
	MenuItem mntmHTML;
	MenuItem mntmSchema;

	TextFlow hexDumpArea;
	String hexDumpText;
	Label formatBadge;
	Button btnBinViewer;
	FormatDetector.Format currentFormat;

	/**
	 * Launch the graphic front-end with this method.
	 */
	public static void main(String[] args) {

		/*
		 * This is needed because only one main class can be called in an
		 * executable jar archive.
		 *
		 */
		if (args.length > 0) {
			// There is a least one parameter -> check if nogui-option is set
			// take the first argument - if there is one - put to the global variables
			Global.WORKINGDIRECTORY = args[0];

		}


		ImageIcon img = new ImageIcon(Objects.requireNonNull(GUI.class.getResource("/logo.png")));


		if (Taskbar.isTaskbarSupported()) {
			try {
				final Taskbar taskbar = Taskbar.getTaskbar();
				taskbar.setIconImage(img.getImage());
				taskbar.setIconBadge("FQLite");
			} catch (Exception err) {
				// do nothing
			}

		} else if (SystemTray.isSupported()) {
			try {
				SystemTray st = SystemTray.getSystemTray();
				TrayIcon ti = new java.awt.TrayIcon(img.getImage());
				st.add(ti);
			} catch (AWTException e) {
				// do nothing - no logo - no problem ;-)
			}
		}


		Application.launch(args);

	}

	/**
	 * Constructor of the user interface class.
	 */
	@Override
	public void start(Stage stage) throws Exception {

		stage.setOnCloseRequest(e -> {
					Platform.exit();
					System.exit(0);
				}
		);


		HexViewManager.setParent(stage);

		baseDir = new File(System.getProperty("user.home"), ".fqlite");

		//Attach the icon to the stage/window
		stage.getIcons().add(new Image(Objects.requireNonNull(GUI.class.getResourceAsStream("/logo.png"))));


		/* create hidden directory inside the user's home */
		Path pp = Path.of(baseDir.getAbsolutePath());
		if (!Files.exists(pp)) {
			// path does not exist at the moment -> create a new hidden folder
			baseDir.mkdir();
		}

		clearCacheFromPreviousRun();

		this.stage = stage;
		mainwindow = this;

		stage.setTitle("FQLite Carving Tool");

		String s = Objects.requireNonNull(GUI.class.getResource("/icon24_root.png")).toExternalForm();
		ImageView iv = new ImageView(s);
		iv.setFitHeight(icon_size_in_pixels-4);
		iv.setFitWidth(icon_size_in_pixels-4);
		root.setGraphic(iv);
		root.setExpanded(true);

		MenuItem mntopen = new MenuItem("Open Database...");
		mntopen.setAccelerator(KeyCombination.keyCombination("Ctrl+O"));
		mntopen.setOnAction(e -> open_db(null));

		cmExport = new MenuItem("Export Node to CSV...");
		cmExport.setAccelerator(KeyCombination.keyCombination("Ctrl+X"));
		cmExport.setDisable(true);
		cmExport.setOnAction(e -> doExport());

		mntmExportDB = new MenuItem("Export to a new SQLite database");
		mntmExportDB.setOnAction(e -> doExportDB());
		mntmExportDB.setDisable(true);

		mntmHTML = new MenuItem("HTML Export...");
		mntmHTML.setOnAction(e -> doExportHTML());
		mntmHTML.setDisable(true);

		mntmSchema = new MenuItem("Schema Analyzer...");
		mntmSchema.setOnAction(e -> doAnalyzeSchema());
		mntmSchema.setDisable(true);

		MenuItem mntclose = new MenuItem("Close All");
		mntclose.setAccelerator(KeyCombination.keyCombination("Ctrl+D"));
		mntclose.setOnAction(e -> closeAll());

		MenuItem mntmExit = new MenuItem("Exit");
		mntmExit.setAccelerator(KeyCombination.keyCombination("Alt+F4"));
		mntmExit.setOnAction(e -> {
			Platform.exit();
			System.exit(0);
		});

		MenuItem mntAbout = new MenuItem("About...");
		mntAbout.setOnAction(e -> new AboutDialog(topContainer));
		MenuItem mntmLog = new MenuItem("View Log...");
		mntmLog.setOnAction(e -> showLog());

		MenuItem mntmProp = new MenuItem("Settings...");
		mntmProp.setOnAction(e -> showPropertyWindow());

		mntmSQL = new MenuItem("SQL-Analyzer...");
		mntmSQL.setDisable(true);
		mntmSQL.setOnAction(e -> showSqlWindow(null, getDatabaseNode()));

		mntmHex = new MenuItem("Hex-Viewer...");
		mntmHex.setDisable(true);
		mntmHex.setOnAction(e -> openHexViewer());

		MenuItem mntmHelp = new MenuItem("Help");
		mntmHelp.setOnAction(e -> showHelp()
		);

		SeparatorMenuItem sep = new SeparatorMenuItem();
		SeparatorMenuItem sep2 = new SeparatorMenuItem();

		Menu mnFiles = new Menu("File");
		Menu mnExport = new Menu("Export");
		Menu mnAnalyze = new Menu("Analyze");
		Menu mnInfo = new Menu("Info");

		mnFiles.getItems().addAll(mntopen, sep, mntclose, sep2, mntmProp, mntmExit);
		mnExport.getItems().addAll(cmExport, mntmExportDB, mntmHTML);
		mnAnalyze.getItems().addAll(mntmSQL, mntmHex, mntmSchema);
		mnInfo.getItems().addAll(mntmHelp, mntmLog, mntAbout);

		/* MenuBar */
		menuBar = new MenuBar();
		menuBar.getMenus().addAll(mnFiles, mnExport, mnAnalyze, mnInfo);

		splitPane = new SplitPane();

		s = Objects.requireNonNull(GUI.class.getResource("/icon24_dragdrop.png")).toExternalForm();
		Label starthere = new Label();
		starthere.setMaxSize(100, 100);
		starthere.setTooltip(new Tooltip("Drag your files here"));

		iv = new ImageView(s);
		iv.setFitWidth(96);
		iv.setFitHeight(96);
		starthere.setGraphic(iv);
		//starthere.setOnAction(e->open_db(null));
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

		//openDB_gray.png
		s = Objects.requireNonNull(GUI.class.getResource("/icon24_openn.png")).toExternalForm();
		Button btnOeffne = new Button();
		iv = new ImageView(s);
		iv.smoothProperty().setValue(true);
		iv.setCache(true);
		iv.preserveRatioProperty().setValue(true);
		iv.setFitHeight(icon_size_in_pixels);
		iv.setFitWidth(icon_size_in_pixels);
		btnOeffne.setGraphic(iv);
		btnOeffne.setOnAction(e -> open_db(null));
		btnOeffne.setTooltip(new Tooltip("Open database file"));
		toolBar.getItems().add(btnOeffne);

		s = Objects.requireNonNull(GUI.class.getResource("/icon24_closee.png")).toExternalForm();
		Button btnClose = new Button();
		iv = new ImageView(s);
		iv.smoothProperty().setValue(true);
		iv.setCache(true);
		iv.preserveRatioProperty().setValue(true);
		iv.setFitHeight(icon_size_in_pixels);
		iv.setFitWidth(icon_size_in_pixels);
		btnClose.setGraphic(iv);
		btnClose.setTooltip(new Tooltip("Close All"));
		toolBar.getItems().add(btnClose);
		btnClose.setOnAction(e -> closeAll());

		s = Objects.requireNonNull(GUI.class.getResource("/icon24_helpp.png")).toExternalForm();
		Button about = new Button();
		iv = new ImageView(s);
		iv.setFitHeight(icon_size_in_pixels);
		iv.setFitWidth(icon_size_in_pixels);
		iv.smoothProperty().setValue(true);
		iv.setCache(true);
		about.setGraphic(iv);
		about.setOnAction(e -> showHelp());


		//properties.png
		s = Objects.requireNonNull(GUI.class.getResource("/icon24_settingss.png")).toExternalForm();
		Button btnProp = new Button();
		iv = new ImageView(s);
		iv.smoothProperty().setValue(true);
		iv.setCache(true);
		iv.preserveRatioProperty().setValue(true);
		iv.setFitHeight(icon_size_in_pixels);
		iv.setFitWidth(icon_size_in_pixels);
		btnProp.setGraphic(iv);
		btnProp.setTooltip(new Tooltip("Open setting window"));
		toolBar.getItems().add(btnProp);
		btnProp.setOnAction(e -> showPropertyWindow());


		about.setTooltip(new Tooltip("Get help"));
		toolBar.getItems().add(about);

		// exit3_gray.png
		s = Objects.requireNonNull(GUI.class.getResource("/icon24_exitt.png")).toExternalForm();
		Button btnexit = new Button();
		iv = new ImageView(s);
		iv.smoothProperty().setValue(true);
		iv.setCache(true);
		iv.preserveRatioProperty().setValue(true);
		iv.setFitHeight(icon_size_in_pixels);
		iv.setFitWidth(icon_size_in_pixels);
		btnexit.setGraphic(iv);
		btnexit.setTooltip(new Tooltip("Exit FQLite"));
		btnexit.setOnAction(e -> {
			Platform.exit();
			System.exit(0);
		});
		toolBar.getItems().add(btnexit);


		toolBar.getItems().add(new Separator());

		s = Objects.requireNonNull(GUI.class.getResource("/icon24_hexx.png")).toExternalForm();
		hexViewBtn = new Button();
		iv = new ImageView(s);
		iv.smoothProperty().setValue(true);
		iv.setCache(true);
		iv.preserveRatioProperty().setValue(true);
		iv.setFitHeight(icon_size_in_pixels);
		iv.setFitWidth(icon_size_in_pixels);
		hexViewBtn.setGraphic(iv);
		hexViewBtn.setDisable(true);
		hexViewBtn.setTooltip(new Tooltip("Show database in HexView"));
		hexViewBtn.setOnAction(e -> openHexViewer());
		toolBar.getItems().add(hexViewBtn);

		s = Objects.requireNonNull(GUI.class.getResource("/icon24_schema.png")).toExternalForm();
		btnSchema = new Button();
		iv = new ImageView(s);
		iv.smoothProperty().setValue(true);
		iv.setCache(true);
		iv.preserveRatioProperty().setValue(true);
		iv.setFitHeight(icon_size_in_pixels);
		iv.setFitWidth(icon_size_in_pixels);
		btnSchema.setGraphic(iv);
		btnSchema.setOnAction(e -> doAnalyzeSchema());
		btnSchema.setDisable(true);
		btnSchema.setTooltip(new Tooltip("Analyze database schema"));
		toolBar.getItems().add(btnSchema);


		s = Objects.requireNonNull(GUI.class.getResource("/icon24_searchh.png")).toExternalForm();
		btnFTS = new Button();
		iv = new ImageView(s);
		iv.smoothProperty().setValue(true);
		iv.setCache(true);
		iv.preserveRatioProperty().setValue(true);
		iv.setFitHeight(icon_size_in_pixels);
		iv.setFitWidth(icon_size_in_pixels);
		btnFTS.setGraphic(iv);
		btnFTS.setDisable(true);
		btnFTS.setTooltip(new Tooltip("Start Free Text Search on database"));
		toolBar.getItems().add(btnFTS);
		btnFTS.setOnAction(e -> {
			showFTSDialog();
		});


		s = Objects.requireNonNull(GUI.class.getResource("/icon24_analyzer.png")).toExternalForm();
		btnSQL = new Button();
		iv = new ImageView(s);
		iv.smoothProperty().setValue(true);
		iv.setCache(true);
		iv.preserveRatioProperty().setValue(true);
		iv.setFitHeight(icon_size_in_pixels);
		iv.setFitWidth(icon_size_in_pixels);
		btnSQL.setGraphic(iv);
		btnSQL.setDisable(true);
		btnSQL.setTooltip(new Tooltip("Open SQL-Analyzer"));
		toolBar.getItems().add(btnSQL);
		btnSQL.setOnAction(e -> showSqlWindow(null, getDatabaseNode()));

		s = Objects.requireNonNull(GUI.class.getResource("/icon24_reasoning.png")).toExternalForm();
		btnLLM = new Button();
		iv = new ImageView(s);
		iv.smoothProperty().setValue(true);
		iv.setCache(true);
		iv.preserveRatioProperty().setValue(true);
		iv.setFitHeight(icon_size_in_pixels);
		iv.setFitWidth(icon_size_in_pixels);
		btnLLM.setGraphic(iv);
		btnLLM.setDisable(true);
		btnLLM.setTooltip(new Tooltip("Open SQL-Agent"));
		toolBar.getItems().add(btnLLM);
		btnLLM.setOnAction(e -> showLLMWindow());


		s = Objects.requireNonNull(GUI.class.getResource("/icon24_location.png")).toExternalForm();
		btnLocation = new Button();
		iv = new ImageView(s);
		iv.smoothProperty().setValue(true);
		iv.setCache(true);
		iv.preserveRatioProperty().setValue(true);
		iv.setFitHeight(icon_size_in_pixels);
		iv.setFitWidth(icon_size_in_pixels);
		btnLocation.setGraphic(iv);
		btnLocation.setDisable(false);
		btnLocation.setTooltip(new Tooltip("Show Locations on Map"));
		//toolBar.getItems().add(btnLocation);
		btnLocation.setOnAction(e -> showMap());

		toolBar.getItems().add(new Separator());

		s = Objects.requireNonNull(GUI.class.getResource("/icon24_csvv.png")).toExternalForm();
		btnExport = new Button();
		iv = new ImageView(s);
		iv.smoothProperty().setValue(true);
		iv.setCache(true);
		iv.preserveRatioProperty().setValue(true);
		iv.setFitHeight(icon_size_in_pixels);
		iv.setFitWidth(icon_size_in_pixels);
		btnExport.setGraphic(iv);
		btnExport.setOnAction(e -> doExport());
		btnExport.setDisable(true);
		btnExport.setTooltip(new Tooltip("Export database to CSV"));
		toolBar.getItems().add(btnExport);


		s = Objects.requireNonNull(GUI.class.getResource("/icon24_htmll.png")).toExternalForm();
		btnHTML = new Button();
		iv = new ImageView(s);
		iv.smoothProperty().setValue(true);
		iv.setCache(true);
		iv.preserveRatioProperty().setValue(true);
		iv.setFitHeight(icon_size_in_pixels);
		iv.setFitWidth(icon_size_in_pixels);
		btnHTML.setGraphic(iv);
		btnHTML.setOnAction(e -> doExportHTML());
		btnHTML.setDisable(true);
		btnHTML.setTooltip(new Tooltip("Export database to HTML"));
		toolBar.getItems().add(btnHTML);


		s = Objects.requireNonNull(GUI.class.getResource("/icon24_case.png")).toExternalForm();
		btnCASE = new Button();
		iv = new ImageView(s);
		iv.smoothProperty().setValue(true);
		iv.setCache(true);
		iv.preserveRatioProperty().setValue(true);
		iv.setFitHeight(icon_size_in_pixels);
		iv.setFitWidth(icon_size_in_pixels);
		btnCASE.setGraphic(iv);
		btnCASE.setOnAction(e -> doExportCASE());
		btnCASE.setDisable(true);
		btnCASE.setTooltip(new Tooltip("Export database to CASE Report"));
		toolBar.getItems().add(btnCASE);

		s = Objects.requireNonNull(GUI.class.getResource("/icon24_export.png")).toExternalForm();
		btnExportDB = new Button();
		iv = new ImageView(s);
		iv.smoothProperty().setValue(true);
		iv.setCache(true);
		iv.preserveRatioProperty().setValue(true);
		iv.setFitHeight(icon_size_in_pixels);
		iv.setFitWidth(icon_size_in_pixels);
		btnExportDB.setGraphic(iv);
		btnExportDB.setDisable(true);
		btnExportDB.setOnAction(e -> doExportDB());
		btnExportDB.setTooltip(new Tooltip("Export database to new database"));
		toolBar.getItems().add(btnExportDB);

		toolBar.getItems().add(new Separator());


		themeBtn = new Button(ThemeManager.toggleLabel());
		themeBtn.setOnAction(e -> toggleTheme());
		themeBtn.setStyle(ThemeManager.toggleBtnStyle());

		filterBtn = new Button("⊘ Show populated tables only");
		filterBtn.setTooltip(new Tooltip("Nur Tabellen mit Datensätzen anzeigen / Alle Tabellen anzeigen"));
		filterBtn.setStyle(
				"-fx-background-color: #2d3a5e;"
				+ "-fx-text-fill: #c8d0e8;"
				+ "-fx-background-radius: 4;"
				+ "-fx-font-size: 12px;"
				+ "-fx-padding: 4 10 4 10;");
		filterBtn.setOnAction(e -> {
			filterActive = !filterActive;
			if (filterActive) {
				filterBtn.setText("✔  Show populated tables only");
				filterBtn.setStyle(
						"-fx-background-color: #1a6b2a;"
						+ "-fx-text-fill: #ffffff;"
						+ "-fx-background-radius: 4;"
						+ "-fx-font-size: 12px;"
						+ "-fx-font-weight: bold;"
						+ "-fx-padding: 4 10 4 10;");
			} else {
				filterBtn.setText("⊘  Show populated tables only");
				filterBtn.setStyle(
						"-fx-background-color: #2d3a5e;"
						+ "-fx-text-fill: #c8d0e8;"
						+ "-fx-background-radius: 4;"
						+ "-fx-font-size: 12px;"
						+ "-fx-padding: 4 10 4 10;");
			}
			applyTableFilter();
		});

		Region spacer = new Region();
		HBox.setHgrow(spacer, Priority.ALWAYS); // let the Spacer fill the remaing place

		toolBar.getItems().add(spacer);
		toolBar.getItems().add(filterBtn);
		toolBar.getItems().add(new Separator());
		toolBar.getItems().add(themeBtn);

		URL url = GUI.class.getResource("/facewink.png");
		assert url != null;
		facewink = new ImageIcon(url);

		url = GUI.class.getResource("/error-48.png");
		assert url != null;
		errorIcon = new ImageIcon(url);

		url = GUI.class.getResource("/information-48.png");
		assert url != null;
		infoIcon = new ImageIcon(url);

		url = GUI.class.getResource("/question-48.png");
		assert url != null;
		questionIcon = new ImageIcon(url);

		url = GUI.class.getResource("/warning-48.png");
		assert url != null;
		warningIcon = new ImageIcon(url);


		/*
		    Bring together all components:
		 */
		topContainer = new VBox();
		topContainer.getChildren().add(menuBar);
		topContainer.getChildren().addAll(toolBar);
		topContainer.getChildren().add(splitPane);
		scene = new Scene(topContainer, Screen.getPrimary().getVisualBounds().getWidth() * 0.9, Screen.getPrimary().getVisualBounds().getHeight() * 0.9);
		ThemeManager.register(scene);   // ← register main window
		VBox.setVgrow(splitPane, Priority.ALWAYS);

		stage.showingProperty().addListener(new ChangeListener<>() {

			@Override
			public void changed(ObservableValue<? extends Boolean> observable, Boolean oldValue, Boolean newValue) {
				if (newValue) {
					splitPane.setDividerPositions(0.25f);
					observable.removeListener(this);
				}
			}
		});


		tree.setOnContextMenuRequested(event -> {
			hideContextMenu();

			TreeItem<NodeObject> node = tree.getSelectionModel().getSelectedItem();
			if (node == null || node == root) {
				cm = createContextMenu(CtxTypes.ROOT);
			} else if (null != node.getValue()) {

				NodeObject no = node.getValue();

				if (node.getValue().isTable)
					cm = createContextMenu(CtxTypes.TABLE);
				else
					cm = createContextMenu(CtxTypes.DATABASE);


			}

			tree.setContextMenu(cm);
			cm.show(tree, event.getScreenX(), event.getScreenY());
			cm.show(tree.getScene().getWindow(), event.getScreenX(), event.getScreenY());

		});

		// OnDrag a file Over
		scene.setOnDragOver(event -> {
			Dragboard db = event.getDragboard();
			if (db.hasFiles()) {
				event.acceptTransferModes(TransferMode.COPY);
			} else {
				event.consume();
			}
		});

		// Dropping over surface
		scene.setOnDragDropped(event -> {
			Dragboard db = event.getDragboard();
			boolean success = false;
			if (db.hasFiles()) {
				success = true;
				String filePath = null;
				for (File file : db.getFiles()) {
					filePath = file.getAbsolutePath();
					open_db(file);
				}
			}
			event.setDropCompleted(success);
			event.consume();
		});


		tree.autosize();
		stage.setScene(scene);
		loadConfiguration();
		stage.centerOnScreen();
		stage.sizeToScene();
		toggleTheme();
		stage.show();
		stage.toFront();
		stage.requestFocus();

	}

	private String themeBtnStyle(GUI.ThemeColors c) {
		return "-fx-background-color: " + c.toggleBg() + ";"
			   + "-fx-text-fill: " + c.toggleText() + ";"
			   + "-fx-background-radius: 4;"
			   + "-fx-font-size: 12px;"
			   + "-fx-padding: 4 10 4 10;";
	}

	// ══════════════════════════════════════════════════════════════════════
	// Theme
	// ══════════════════════════════════════════════════════════════════════

	private void toggleTheme() {
		currentTheme = (currentTheme == GUI.Theme.DARK) ? GUI.Theme.LIGHT : GUI.Theme.DARK;
		// Sync ThemeManager state and repaint ALL registered scenes (main + all dialogs)
		ThemeManager.setDark(currentTheme == GUI.Theme.DARK);

		tree.refresh();
		// Update the toggle button label/style
		if (themeBtn != null) {
			themeBtn.setText(ThemeManager.toggleLabel());
			themeBtn.setStyle(ThemeManager.toggleBtnStyle());
		}
		// Re-color any existing text in the hex dump area
		if (hexDumpArea != null) {
			javafx.scene.paint.Color hexTextColor = javafx.scene.paint.Color.web(
					(currentTheme == GUI.Theme.DARK) ? DARK.textArea() : LIGHT.textArea());
			hexDumpArea.getChildren().forEach(node -> {
				if (node instanceof Text t) t.setFill(hexTextColor);
			});
		}
		// Notify all registered theme-change listeners (e.g. open SQLWindow instances)
		themeListeners.forEach(Runnable::run);
	}

	private void applyTheme() {
		// Delegate entirely to ThemeManager – it handles the main scene and all dialogs
		ThemeManager.setDark(currentTheme == GUI.Theme.DARK);
		if (themeBtn != null) {
			themeBtn.setText(ThemeManager.toggleLabel());
			themeBtn.setStyle(ThemeManager.toggleBtnStyle());
		}
	}

	/**
	 * Returns {@code true} when the dark theme is currently active.
	 */
	public boolean isDarkTheme() {
		return currentTheme == GUI.Theme.DARK;
	}

	/**
	 * Registers a callback invoked whenever the user switches the theme.
	 * Child windows (e.g. SQLWindow) use this to re-style controls that cannot
	 * pick up theme changes via CSS alone.
	 */
	public void addThemeListener(Runnable listener) {
		themeListeners.add(listener);
	}

	/**
	 * Removes a previously registered theme-change callback.
	 */
	public void removeThemeListener(Runnable listener) {
		themeListeners.remove(listener);
	}


	private void showMap(){

		TreeItem<NodeObject> node = tree.getSelectionModel().getSelectedItem();

		int level = tree.getTreeItemLevel(node);

		// case: Do we have the top level?
		if (level == 1) {
			return;
		}

		NodeObject nd = node.getValue();
		String tblname = nd.name;


		ObservableList<ObservableList<String>> result = nd.job.resultlist.get(tblname);

		for (TableDescriptor td : nd.job.headers) {
			if (td.getName().equals(tblname)) {
				LocationWindow mapview = new LocationWindow(td.getName(),result,td.columnnames);
				Stage configstage = new Stage();
				mapview.start(configstage);
			}
		}
	}


	private void showPropertyWindow() {

		SettingsDialog pd = new SettingsDialog();
		pd.start(new Stage());
	}

	private void showLLMWindow() {

		removeLLMDB();

		TreeItem<NodeObject> selected = getDatabaseNode();

		if (selected == null) {
			return;
		}

		String dbname = selected.getValue().name;


		// the configuration file for the LLM is stored in $user.home$/.fqlite/fqlitellm-config.properties
		if (Files.exists(Global.llmconfig)) {
			// file exists
			Properties props = new Properties();
			try (InputStream input = new FileInputStream(Global.llmconfig.toFile())) {
				props.load(input);
				String model_path = props.getProperty("model_path", Global.llmconfig.toString());
				//There is a config file with a path to the model inside
				if (!model_path.isEmpty()) {

					Path p = Paths.get(model_path);

					// model path is valid since the file exists
					if (Files.exists(p)) {

						// prepare autocompletion
						LLMWindow llmw = new LLMWindow(this, selected);
						prepareLLM(llmw, model_path);
						Platform.runLater(llmw::show);
						return;


					}
				}
			} catch (IOException | NumberFormatException e) {
				System.err.println("Error loading configuration: " + e.getMessage());
			}
		}


		Alert alert = new Alert(AlertType.INFORMATION);
		alert.setTitle("Warning Dialog");
		alert.setHeaderText("Couldn't find LLM");
		alert.setContentText("To use this feature, you must first download an LLM model. \n Please check your configuration.");
		alert.showAndWait();
		showConfigDialog();

	}

	/**
	 * Before each run of the LLM-Agent we first delete the selection from the last run.
	 */
	private void removeLLMDB() {
		File file = new File("testtest.json");
		if (file.exists()) {
			Path path = Paths.get("testtest.json");
			try {
				Files.delete(path);
				System.out.println("File deleted successfully");
				Path newFilePath = Paths.get("testtest.json");
				Files.createFile(newFilePath);
			} catch (IOException e) {
				System.out.println("Failed to delete the file: " + e.getMessage());
			}
		}
	}

	public void prepareLLM(LLMWindow llmw, String model_path) {


		// Popup anzeigen
		LoadingPopup popup = new LoadingPopup();
		popup.show(llmw.getPrimaryStage(), "Starting Agent...");

		// Zeitintensive Aufgabe in separatem Thread
		new Thread(() -> {
			try {
				llmw.prepareRAG(model_path);
				// Deine zeitintensive Aufgabe hier
				Platform.runLater(() -> {
					try {
						Stage configstage = new Stage();
						llmw.start(configstage);
					} catch (Exception e) {
						e.printStackTrace();
					}
				});

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				// Popup schließen
				popup.close();
			}
		}).start();
	}

	private void showConfigDialog() {
		LLMConfigDialog dl = new LLMConfigDialog();
		Stage configstage = new Stage();
		configstage.initModality(Modality.APPLICATION_MODAL);
		dl.start(configstage);
	}

	public InMemoryDatabase createInMemoryDB(NodeObject no) {

		ExportType etype = switch (no.tabletype) {
			case 99 -> ExportType.SQLITEDB;
			case 100 -> ExportType.ROLLBACKJOURNAL;
			case 101 -> ExportType.WALARCHIVE;
			default -> null;
		};

		return no.job.createInMemoryDB(no.job.filename, no.name, etype);

	}

	public void showFTSDialog() {

		if (dbnames.isEmpty()) {

			Alert alert = new Alert(AlertType.INFORMATION);
			alert.setTitle("Information");
			alert.setContentText("You must open at least one database before you can use the analyzer.");
			alert.showAndWait();
			return;
		}

		TreeItem<NodeObject> no = getDatabaseNode();

		FileType ft = no.getValue().type;

		ConcurrentHashMap<String, ObservableList<ObservableList<String>>> data = null;

		if (ft == FileType.SQLiteDB) {
			data = no.getValue().job.resultlist;
		}
		else if (ft == FileType.WriteAheadLog) {
			data = no.getValue().job.wal.resultlist;
		}
		else if (ft == FileType.RollbackJournalLog) {
			data = no.getValue().job.rol.resultlist;
		}

		if (data == null) {
			return;
		}

		var dialog = new SearchDialog(this, no.getValue().name, data, result -> {
			selectTableCell(no.getValue().name, result.tableName(), result.rowIndex(), result.colIndex());
		});
		dialog.showAndWait();

	}


	public void showSqlWindow(String statement, TreeItem<NodeObject> node) {

		if (dbnames.isEmpty()) {

			Alert alert = new Alert(AlertType.INFORMATION);
			alert.setTitle("Information");
			alert.setContentText("You must open at least one database before you can use the analyzer.");
			alert.showAndWait();
			return;
		}

		String dbname = node.getValue().name;
		InMemoryDatabase mdb;
		if (DBManager.exists(dbname)) {
			mdb = DBManager.get(dbname);
		} else {
			// 1. load recovered data to Memory (SQLite in Memory DB)
			mdb = createInMemoryDB(node.getValue());
		}


		Platform.runLater(() -> {
			// 2. now open SQL Analyzer
			SQLWindow sql = new SQLWindow(this, node, dbname, mdb, statement);
			Stage sqlstage = new Stage();
			sql.start(sqlstage);
			Platform.runLater(() -> {
				sql.show();
			});
		});

	}

	private static void showWarning(Stage stage, String details) {
		Alert alert = new Alert(AlertType.INFORMATION);
		alert.setTitle("user infomation");
		alert.setContentText(details);
		alert.initOwner(stage);
		alert.showAndWait();
	}

	public void openHexViewer() {
		TreeItem<NodeObject> node = getDatabaseNode();
		if (node == null || node == root) {

		} else if (null != node.getValue()) {
			NodeObject no = node.getValue();

			// prepare Hex-View of DB-File
			if (no.type == FileType.SQLiteDB) {
				if (null != no.job) {

					HEXVIEW.load(no.job.path);
				}
			}
			// prepare Hex-View of WAL-File
			else if (no.type == FileType.WriteAheadLog) {

				if (null != no.job.wal) {

					HEXVIEW.load(no.job.wal.path);

				}
			}
			// prepare Hex-View of Rollback-Journal
			else if (no.type == FileType.RollbackJournalLog) {

				if (null != no.job.rol) {

					HEXVIEW.load(no.job.rol.path);
				}


			}
		}


		Platform.runLater(() -> {
			try {
				HEXVIEW.show();
			} catch (Exception e) {
				e.printStackTrace();
			}
		});


	}


	private void showHelp() {
		Platform.runLater(() -> new UserGuideWindow().start(new Stage()));

	}

	final GUI gui = this;

	private void showLocation(double latitude, double longitude) {
		Platform.runLater(new Runnable() {

			public void run() {
				gui.getHostServices().showDocument("https://www.openstreetmap.org/?mlat=" + latitude + "&mlon=" + longitude);
			}
		});
	}

	private void hideContextMenu() {
		if (null != cm)
			cm.hide();
	}

	private void showLog() {
		AppLog logwindow = new AppLog();

		Stage stage = new Stage();
		try {
			logwindow.start(stage);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@SuppressWarnings("unlikely-arg-type")
	/**
	 * @param the type of node
	 * @return the new ContextMenu
	 */
	private ContextMenu createContextMenu(CtxTypes type) {

		final ContextMenu contextMenu = new ContextMenu();

		MenuItem cmCloseSingle = new MenuItem("Close database file");
		cmCloseSingle.setOnAction(e -> {
			TreeItem<NodeObject> node = tree.getSelectionModel().getSelectedItem();
			if (node == null || node.getValue().isRoot) {
				//root node -> do nothing
			} else if (null != node.getValue()) {
				NodeObject no = node.getValue();
				boolean remove = node.getParent().getChildren().remove(node);
				if (remove) {
					AppLog.info(" Database " + no.name + " closed.");
					this.tables.remove(no);
					this.treeitems.remove(no);
				}
				if (null == no.job) {
				}
			}
		});


		MenuItem mntopen = new MenuItem("Open Database...");
		mntopen.setOnAction(e -> open_db(null));

		MenuItem cmExport;
		if (type == CtxTypes.TABLE)
			cmExport = new MenuItem("Export Table...");
		else
			cmExport = new MenuItem("Export Database to CSV...");

		cmExport.setAccelerator(KeyCombination.keyCombination("Ctrl+X"));
		cmExport.setOnAction(e -> doExport());

		MenuItem cmExport2DB;
		cmExport2DB = new MenuItem("Export to new Database...");
		cmExport2DB.setOnAction(e -> doExportDB());
		MenuItem cmSQLAnalyser;
		cmSQLAnalyser = new MenuItem("Inspect with SQL-Analyzer...");
		cmSQLAnalyser.setOnAction(e -> showSqlWindow(null, getDatabaseNode()));

		MenuItem cmHex = new MenuItem("Open HexViewer");
		cmHex.setAccelerator(KeyCombination.keyCombination("Ctrl+H"));
		cmHex.setOnAction(e -> openHexViewer());

		SeparatorMenuItem sepA = new SeparatorMenuItem();
		SeparatorMenuItem sepB = new SeparatorMenuItem();
		SeparatorMenuItem sepC = new SeparatorMenuItem();


		contextMenu.getItems().addAll(sepA, mntopen, cmCloseSingle, sepB, cmExport, cmExport2DB, sepC, cmSQLAnalyser, cmHex);

		if (type == CtxTypes.ROOT) {
			cmExport.setDisable(true);
			cmExport2DB.setDisable(true);
			cmCloseSingle.setDisable(true);
			cmHex.setDisable(true);
			cmSQLAnalyser.setDisable(true);
		}

		if (type == CtxTypes.DATABASE) {
			cmExport.setDisable(false);
			cmExport2DB.setDisable(false);
			cmCloseSingle.setDisable(false);
			cmHex.setDisable(false);
			cmSQLAnalyser.setDisable(false);
		}

		if (type == CtxTypes.TABLE) {
			cmExport.setDisable(false);
			cmExport2DB.setDisable(true);
			cmHex.setDisable(false);
			cmCloseSingle.setDisable(true);
			cmSQLAnalyser.setDisable(false);
		}

		return contextMenu;
	}


	/**
	 * Delete all Files from a previous run of the program.
	 */
	private void clearCacheFromPreviousRun() {

		if (baseDir != null) {

			try {
				File[] cache = baseDir.listFiles();

				if (null == cache)
					return;

				for (File file : Objects.requireNonNull(baseDir.listFiles()))
					if (!file.isDirectory() && !file.getName().endsWith(".conf") && !file.getName().endsWith(".properties")) {
						file.delete();
					}
			} catch (Exception err) {
				// do nothing - no cache directory
			}
		}


	}

	/**
	 * This method loads the file fqlite.conf from .fqlite directory.
	 * The directory is hidden and normally resides in the user's home directory.
	 */
	private void loadConfiguration() {

		//check if settings.conf file exists
		String path = baseDir.getAbsolutePath() + File.separator + "fqlite.conf";
		Properties appProps = new Properties();

		/*
		 * Example: configuration file
		 *
		 * #Mon Aug 12 19:01:12 CEST 2024
		 * CSV_SEPARATOR=;
		 * EXPORTMODE=DONTEXPORT
		 * EXPORT_THEADER=true
		 * LOG-LEVEL=INFO
		 * font_name=Tamil Sangam MN
		 * font_size=14.0
		 * font_style=Bold
		 */
		try {
			appProps.load(new FileInputStream(path));

			Object value = appProps.get("EXPORTMODE");
			if (null != value)
				Global.EXPORT_MODE = Global.EXPORT_MODES.valueOf((String) value);

			Object svalue = appProps.get("SEPARATOR");
			if (null != svalue)
				Global.CSV_SEPARATOR = (String) svalue;

			Object tvalue = appProps.get("EXPORT_THEADER");
			if (null != tvalue) {
				Global.EXPORTTABLEHEADER = tvalue.equals("true");
			}

			Object cvalue = appProps.get("CSV_SEPARATOR");
			if (null != cvalue) {
				Global.CSV_SEPARATOR = (String) cvalue;
			}

				Object lvalue = appProps.get("LOG-LEVEL");
			if (null != lvalue) {
				Global.LOGLEVEL = Level.parse((String) lvalue);
			}

			if (appProps.containsKey("TIMESTAMP_FORMAT"))
				Global.TIMESTAMP_FORMAT = appProps.getProperty("TIMESTAMP_FORMAT");

			if (appProps.containsKey("TIMESTAMP_USE_UTC"))
				Global.TIMESTAMP_USE_UTC = "true".equals(appProps.getProperty("TIMESTAMP_USE_UTC"));

			//Object fn = appProps.get("font_name");
			//if (null != fn) {
			//	Global.font_name = (String) fn;
			//}

			//Object fs = appProps.get("font_size");
			//if (null != fs) {
			//	Global.font_size = (String) fs;
			//}

			//System.out.println("-fx-font-size: " + fs + "pt; -fx-font-family: \"" + fn + "\"; ");
			//topContainer.setStyle("-fx-font-size: " + fs + "pt; -fx-font-family: \"" + fn + "\"; ");
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
	 * Close all database nodes that are currently open.
	 */
	public void closeAll() {

		TreeItem<NodeObject> root = tree.getRoot();

		this.tables.clear();

		for (TreeItem<NodeObject> node : root.getChildren()) {

			if (null != node.getValue()) {
				NodeObject no = node.getValue();
				if (null != no.job) {
					no.job.checkpointlist = null;
					//no.job.FileCache = null;
					if (no.job.bincache != null){
						no.job.bincache = null;
					}
					if (no.job.hexdumplist != null){
						no.job.hexdumplist.clear();
					}
					if (no.job.db != null)
						no.job.db.clear();
					if (no.job.freelistpages != null)
						no.job.freelistpages.clear();
					if (no.job.tasklist != null)
						no.job.tasklist.clear();
					if (no.job.guiroltab != null)
						no.job.guiroltab.clear();
					if (no.job.rol != null)
						no.job.rol = null;
					if (no.job.wal != null)
						no.job.wal = null;
					no.job = null;
				}
				no.tablePane = null;
				this.datasets.clear();
			}
			root.getChildren().removeAll();
			System.gc();

		}

		// remove all nodes -> simply create a new TreeView
		root.getChildren().clear();

		this.tables.clear();
		this.treeitems.clear();
		this.dbnames.clear();
		HEXVIEW.close();
		System.gc();


	}


	/**
	 * Start a data export.
	 */
	public void doExportDB() {
		NodeObject no = null;

		/* Do we really have a database node currently selected? */
		TreeItem<NodeObject> node = tree.getSelectionModel().getSelectedItem();
		if (node == null || node.getValue().isRoot) {
			return;
		} else if (null != node.getValue()) {
			no = node.getValue();
		}

		/* it is indeed a valid node (database, table, journal, WAL archive) -> begin with export */
		assert no != null;

		if(no.isTable)
		{
			// user has selected a table. Since we need the database node -> come on get it.
			javafx.scene.control.TreeItem<NodeObject> db = getDatabaseNode();
			no = db.getValue();
		}

		export_db(no);
	}

	/**
	 * Start a data export.
	 */
	public void doExport() {
		NodeObject no = null;

		/* Do we really have a database node currently selected? */
		TreeItem<NodeObject> node = tree.getSelectionModel().getSelectedItem();
		if (node == null || node.getValue().isRoot) {
			return;
		} else if (null != node.getValue()) {
			no = node.getValue();
		}

		/* it is indeed a valid node (database, table, journal, WAL archive) -> begin with export */
		export2csv(no);
	}

	/**
	 * The reworked new schema viewer.
	 * The schema is extracted from the internal SQLite database and
	 * a mermaid based html-page is created. The page is automatically
	 * displayed inside the standard browser.
	 *
	 */
	public void doAnalyzeSchema() {

		NodeObject no = null;

		/* Do we really have a database node currently selected? */
		TreeItem<NodeObject> node = getDatabaseNode();
		if (node == null || node.getValue().isRoot) {
			return;
		} else if (null != node.getValue()) {
			no = node.getValue();
		}

		/* it is indeed a valid node (database, table, journal, WAL archive) -> begin with export */
		assert no != null;

		File baseDir = new File(System.getProperty("user.home"), ".fqlite");
		String pfad = baseDir.getAbsolutePath() + FileSystems.getDefault().getSeparator() + "schema.html";

		String dbname = no.name;

		/* Check, if InMemory DB already exists */
		InMemoryDatabase mdb;
		if (DBManager.exists(dbname)) {
			mdb = DBManager.get(dbname);
		} else {
			// 1. load recovered data to Memory (SQLite in Memory DB)
			mdb = createInMemoryDB(node.getValue());
		}

		/* Now we need the SQLite-Schema */
		SchemaRetriever schemaRetriever = new SchemaRetriever(mdb.getConnectionObject());

		try {
			String schema = schemaRetriever.extractFullSchema(mdb.getConnectionObject());

			// 1st step: convert the SQL-Schema to Mermaid
			String mermaidCode = SchemaToMermaidConverter.convertToMermaid(schema);

			File base = new File(System.getProperty("user.home"), ".fqlite");
			String mmpath = base.getAbsolutePath() + File.separator + "mermaid.min.js";
			String pzpath = base.getAbsolutePath() + File.separator + "panzoom.min.js";

			File f = new File(mmpath);
			if (!f.exists()) {
				InputStream is = getClass().getResourceAsStream("/mermaid.min.js");
				Path target = Paths.get(mmpath);
				// Simple copy
				Files.copy(is, target, StandardCopyOption.REPLACE_EXISTING);

				InputStream is2 = getClass().getResourceAsStream("/panzoom.min.js");
				target = Paths.get(pzpath);

				Files.copy(is2, target, StandardCopyOption.REPLACE_EXISTING);
			}

			// Set paths to your local library files
			MermaidHTMLGenerator.setMermaidLibraryPath("./mermaid.min.js");
			MermaidHTMLGenerator.setPanzoomLibraryPath("./panzoom.min.js");

			// 2nd step: generate a local html-file
			MermaidHTMLGenerator.generateHTMLFile(mermaidCode, pfad);

			openInBrowser(pfad, gui.getHostServices());


		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}


	}

	/**
	 * Start a data export.
	 */
	public void doExportHTML() {
		NodeObject no = null;

		/* Do we really have a database node currently selected? */
		TreeItem<NodeObject> node = tree.getSelectionModel().getSelectedItem();
		if (node == null || node.getValue().isRoot) {
			return;
		} else if (null != node.getValue()) {
			no = node.getValue();
		}

		/* it is indeed a valid node (database, table, journal, WAL archive) -> begin with export */
		assert no != null;
		export_html(no);
	}

	/**
	 * Start a data export.
	 */
	public void doExportCASE() {
		NodeObject no = null;

		/* Do we really have a database node currently selected? */
		TreeItem<NodeObject> node = tree.getSelectionModel().getSelectedItem();
		if (node == null || node.getValue().isRoot) {
			return;
		} else if (null != node.getValue()) {
			no = node.getValue();
		}

		/* it is indeed a valid node (database, table, journal, WAL archive) -> begin with export */
		assert no != null;
		export_case(no);
	}



	/**
	 * Add a new table header to the database tree.
	 *
	 * @param job         the import thread
	 * @param tablename   table name
	 * @param columns     list of all columns
	 * @param columntypes list of SQL types
	 * @return tree path
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	CompletableFuture<String> add_table(Job job, String tablename, List<String> columns, List<String> columntypes, List<String> PK, List<String> BoolColumns, List<String> sqltypes, boolean walnode,
										boolean rjnode, int db_object) {

		CompletableFuture<String> future = new CompletableFuture<>();

		NodeObject o = null;

		Path p = Paths.get(job.path);

		FQTableView<Object> table = new FQTableView<>(tablename, p.getFileName().toString(), job, columns, columntypes, sqltypes);

		table.getSelectionModel().setCellSelectionEnabled(true);


		table.getSelectionModel().setSelectionMode(
				SelectionMode.MULTIPLE
				//SelectionMode.SINGLE
		);


		Image img = new Image(Objects.requireNonNull(GUI.class.getResource("/green_info24.png")).toExternalForm());
		ImageView view = new ImageView(img);


		// normal table?
		if (!walnode) {
			//yes
			//add the standard columns (index 0 <>'line number', 2 <> 'status',3 <> 'offset' - '1' <>is the table name
			TableColumn numbercolumn = new TableColumn<>(Global.col_no);
			numbercolumn.setCellValueFactory((Callback<CellDataFeatures<ObservableList, String>, ObservableValue<String>>) param -> {
				return new SimpleStringProperty(param.getValue().get(0).toString());               //line number index
			});

			numbercolumn.setStyle("-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");

			TableColumn pllcolumn = new TableColumn<>(Global.col_pll);
			pllcolumn.setCellFactory(TooltippedTableCell.forTableColumn(tablename, job, this.stage));
			pllcolumn.setCellValueFactory((Callback<CellDataFeatures<ObservableList, String>, ObservableValue<String>>) param -> new SimpleStringProperty(param.getValue().get(2).toString()));

			pllcolumn.setStyle("-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");

			TableColumn hlcolumn = new TableColumn<>(Global.col_rowid);
			hlcolumn.setCellFactory(TooltippedTableCell.forTableColumn(tablename, job, this.stage));
			hlcolumn.setCellValueFactory((Callback<CellDataFeatures<ObservableList, String>, ObservableValue<String>>) param -> new SimpleStringProperty(param.getValue().get(3).toString()));

			hlcolumn.setStyle("-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");

			Label statusLabel = new Label(Global.STATUS_CLOMUN);

			statusLabel.setTooltip(new Tooltip("indicates if data record is deleted or not"));
			TableColumn statuscolumn = new TableColumn<>();
			statuscolumn.setGraphic(statusLabel);
			statuscolumn.setCellFactory(TooltippedTableCell.forTableColumn(tablename, job, this.stage));
			statuscolumn.setCellValueFactory((Callback<CellDataFeatures<ObservableList, String>, ObservableValue<String>>) param -> new SimpleStringProperty(param.getValue().get(4).toString()));
			statuscolumn.setGraphic(view);
			statuscolumn.setStyle("-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");


			TableColumn offsetcolumn = new TableColumn<>(Global.col_offset);
			offsetcolumn.setCellFactory(TooltippedTableCell.forTableColumn(tablename, job, this.stage));
			offsetcolumn.setCellValueFactory((Callback<CellDataFeatures<ObservableList, String>, ObservableValue<String>>) param -> new SimpleStringProperty(param.getValue().get(5).toString()));

			offsetcolumn.setStyle("-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");

			//[no,pll,hl,tabname,status,...]
			table.getColumns().addAll(numbercolumn, statuscolumn, offsetcolumn, pllcolumn, hlcolumn);

		} else {
			/*
			 * Attention: Table is a WAL-Table and has some extra columns !!!
			 */

			//add the standard columns (index 0 <>'line number', 2 <> 'status',3 <> 'offset' - '1' <>is the table name
			TableColumn numbercolumn = new TableColumn<>(Global.col_no);
			numbercolumn.setComparator(new CustomComparator());
			numbercolumn.setCellValueFactory((Callback<CellDataFeatures<ObservableList, String>, ObservableValue<String>>) param -> {
				return new SimpleStringProperty(param.getValue().get(0).toString());               //line number index
			});
			numbercolumn.setStyle("-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");


			TableColumn pllcolumn = new TableColumn<>(Global.col_pll);
			pllcolumn.setCellFactory(TooltippedTableCell.forTableColumn(tablename, job, this.stage));
			pllcolumn.setComparator(new CustomComparator());
			pllcolumn.setCellValueFactory((Callback<CellDataFeatures<ObservableList, String>, ObservableValue<String>>) param -> new SimpleStringProperty(param.getValue().get(2).toString()));
			pllcolumn.setStyle("-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");


			TableColumn rowidcolumn = new TableColumn<>(Global.col_rowid);
			rowidcolumn.setCellFactory(TooltippedTableCell.forTableColumn(tablename, job, this.stage));
			rowidcolumn.setComparator(new CustomComparator());
			rowidcolumn.setCellValueFactory((Callback<CellDataFeatures<ObservableList, String>, ObservableValue<String>>) param -> new SimpleStringProperty(param.getValue().get(3).toString()));
			rowidcolumn.setStyle("-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");


			Label statusLabel = new Label(Global.STATUS_CLOMUN);
			statusLabel.setTooltip(new Tooltip("indicates if data record is deleted or not"));
			TableColumn statuscolumn = new TableColumn<>();
			statuscolumn.setGraphic(statusLabel);
			statuscolumn.setCellFactory(TooltippedTableCell.forTableColumn(tablename, job, this.stage));
			statuscolumn.setCellValueFactory((Callback<CellDataFeatures<ObservableList, String>, ObservableValue<String>>) param -> new SimpleStringProperty(param.getValue().get(4).toString()));
			statuscolumn.setGraphic(view);

			TableColumn offsetcolumn = new TableColumn<>(Global.col_offset);
			offsetcolumn.setComparator(new CustomComparator());
			offsetcolumn.setCellFactory(TooltippedTableCell.forTableColumn(tablename, job, this.stage));
			offsetcolumn.setCellValueFactory((Callback<CellDataFeatures<ObservableList, String>, ObservableValue<String>>) param -> new SimpleStringProperty(param.getValue().get(5).toString()));
			offsetcolumn.setStyle("-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");

			//[no,pll,hl,tabname,status,...]
			table.getColumns().addAll(numbercolumn, statuscolumn, offsetcolumn, pllcolumn, rowidcolumn);


		}    // end of else (walnode = true)


		/*
		 * ADD TABLE COLUMNS DYNAMICALLY
		 */


		for (int i = 0; i < columns.size(); i++) {
			String colname = columns.get(i);
			final int j = i + 6;
			TableColumn col = new TableColumn(colname);
			col.setCellFactory(TooltippedTableCell.forTableColumn(tablename, job, this.stage));
			col.setComparator(new CustomComparator());


			col.setCellValueFactory((Callback<CellDataFeatures<ObservableList, String>, ObservableValue<String>>) param -> {

				if (param.getValue().size() <= j)
					return new SimpleStringProperty("");
				Object o1 = param.getValue().get(j);
				if (null != o1)
					return new SimpleStringProperty(o1.toString());
				else
					return new SimpleStringProperty("");


			});


			if (columntypes.size() > i && !columntypes.get(i).equals("BLOB") && !columntypes.get(i).equals("TEXT") && !columntypes.get(i).startsWith("VARCHAR") && !columntypes.get(i).contains("CHARACTER") && !columntypes.get(i).contains("NCHAR")) {
				col.setStyle("-fx-alignment: TOP-RIGHT;");
			} else {
				if (columntypes.size() > i)
					col.setStyle("-fx-alignment: TOP-LEFT;");
			}

			/* add icon to PRIMARYKEY columns */
			if (null != PK) {
				if (PK.contains(colname)) {
					img = new Image(Objects.requireNonNull(GUI.class.getResource("/key-icon.png")).toExternalForm());
					view = new ImageView(img);
					col.setGraphic(view);
				}
			}

			if (colname.equals(Global.col_salt2) || colname.equals(Global.col_salt1) ||
				colname.equals(Global.col_walframe) || colname.equals(Global.col_dbpage) || colname.equals(Global.col_commit)) {
				col.setStyle("-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");


			}


			table.getColumns().add(col);
		}


		VBox tablePane = new VBox();


		Image img2 = new Image(Objects.requireNonNull(GUI.class.getResource("/green_cancel24.png")).toExternalForm());

		ImageView view2 = new ImageView(img2);

		Button clearFilter = new Button();
		clearFilter.setGraphic(view2);
		view2.setFitHeight(14);
		view2.setFitWidth(14);
		view2.preserveRatioProperty();
		clearFilter.setGraphic(view2);
		clearFilter.setTooltip(new Tooltip("set back filter"));

		ComboBox<String> columnselection = new ComboBox<>();

		columnselection.getItems().add("All Columns (Filter) -> ");
		columnselection.getItems().add(Global.col_no);
		columnselection.getItems().add(Global.col_status);
		columnselection.getItems().add(Global.col_offset);
		columnselection.getItems().add(Global.col_pll);
		columnselection.getItems().add(Global.col_rowid);


		for (String choice : columns) {
			if (choice != null)
				columnselection.getItems().add(choice);
		}

		// Select All Columns as default
		columnselection.getSelectionModel().select(0);

		TextField filter = new TextField();
		filter.setPrefWidth(300);

		HBox filterpane = new HBox();
		filterpane.getChildren().add(0, columnselection);
		filterpane.getChildren().add(1, filter);
		filterpane.getChildren().add(2, clearFilter);


		tablePane.getChildren().add(filterpane);
		tablePane.getChildren().add(table);
		table.setPrefHeight(4000);
		VBox.setVgrow(table, Priority.ALWAYS);
		Label l = new Label("Table: " + tablename);
		tablePane.getChildren().add(l);

		if (walnode)
			o = new NodeObject(tablename, tablePane, columns.size(), FileType.WriteAheadLog, db_object, true); // wal file
		if (rjnode)
			o = new NodeObject(tablename, tablePane, columns.size(), FileType.RollbackJournalLog, db_object, true); // rollback																									// journal file
		if (!walnode && !rjnode)
			o = new NodeObject(tablename, tablePane, columns.size(), FileType.SQLiteDB, db_object, true); // normal db

		o.job = job;
		TreeItem<NodeObject> dmtn = new TreeItem<>(o);
		dmtn.setExpanded(true);


		String s = switch (o.tabletype) {
			case 0 -> Objects.requireNonNull(GUI.class.getResource("/table_icon_empty.png")).toExternalForm(); // /.png
			case 1 -> Objects.requireNonNull(GUI.class.getResource("/table-key-icon-reddot.png")).toExternalForm();
			case 99 -> Objects.requireNonNull(GUI.class.getResource("/gray_database24.png")).toExternalForm();
			case 100 -> Objects.requireNonNull(GUI.class.getResource("/journal-icon.png")).toExternalForm();
			case 101 -> Objects.requireNonNull(GUI.class.getResource("/gray_archive24.png")).toExternalForm();
			default -> null;
		};

		ImageView iv = new ImageView(s);
		dmtn.setGraphic(iv);


		if (walnode) {
			Platform.runLater(() -> {

				/* WAL-tree node - add child node of table */
				walNode.getChildren().add(dmtn);
				String tp = null;
				tp = getPath(dmtn);
				future.complete(tp);

				// save assignment between the tree item's path and a tree item
				treeitems.put(tp, dmtn);
				tables.put(tp, tablePane);
				future.complete(tp);
			});
		} else if (rjnode) {

			Platform.runLater(() -> {

				/* Rollback Journal */
				rjNode.getChildren().add(dmtn);
				String tp = null;
				tp = getPath(dmtn);

				// save assignment between the tree item's path and a tree item
				treeitems.put(tp, dmtn);
				tables.put(tp, tablePane);
				future.complete(tp);
			});
		} else {
			/* main db */
			Platform.runLater(() -> {
				job.getTreeItem().getChildren().add(dmtn);
				String tp = null;
				tp = getPath(dmtn);
				// save assignment between the tree item's path and a tree item
				treeitems.put(tp, dmtn);
				tables.put(tp, tablePane);
				future.complete(tp);
			});

		}



		table.setOnKeyPressed(event -> {
			if (!table.getSelectionModel().isEmpty()) {
				KeyCodeCombination copylineCombination = new KeyCodeCombination(KeyCode.L, KeyCombination.SHORTCUT_DOWN);
				KeyCodeCombination copycellCombination = new KeyCodeCombination(KeyCode.C, KeyCombination.SHORTCUT_DOWN);

				if (copylineCombination.match(event)) {
					copyLineAction(table);
					event.consume();
				} else if (copycellCombination.match(event)) {
					copyCellAction(table);
					event.consume();
				}


				// get detailed position (requires active selection of a cell)
				TablePosition pos = table.getFocusModel().getFocusedCell();
				int row = pos.getRow();
				int colIndex = pos.getColumn();

				// retrieve complete row form currently selected table cell
				ObservableList<String> rowData = (ObservableList<String>) table.getItems().get(row);
				String firstCellValue = rowData.getFirst();

				int delta;
				if (table.sqltypes != null) {
					delta = table.getSelectionModel().getTableView().getColumns().size() - table.sqltypes.size();
					if (pos.getColumn() >= delta) {
						String type = table.sqltypes.get(pos.getColumn() - delta);
						updateHexDump(Integer.parseInt(firstCellValue) - 1, colIndex, job, tablename, type);
					} else
						updateStandardColumns(pos, delta);
				} else if (tablename.equals("sqlite_master")) {
					updateHexDump(Integer.parseInt(firstCellValue) - 1, colIndex, job, tablename, "TEXT");
				} else {
					btnBinViewer.setDisable(true);
					updateStandardColumns(pos, 0);
				}

			}
		});


		table.setOnMouseClicked(event -> {

			ContextMenu tcm = createContextMenu(CtxTypes.TABLE, tablename, table, job);

			//System.out.println("Quelle : " + event.getTarget());
			if (event.getTarget().toString().startsWith("TableColumnHeader"))
				return;

			int row = -1;
			TablePosition pos = null;
			try {
				pos = table.getSelectionModel().getSelectedCells().get(0);
				row = pos.getRow();

			} catch (Exception err) {
				return;
			}


			// Item here is the table view type:
			Object item = table.getItems().get(row);


			if (event.getButton() == MouseButton.SECONDARY) {

				tcm.show(table.getScene().getWindow(), event.getScreenX(), event.getScreenY());
				return;
			}


			TableColumn col = pos.getTableColumn();

			if (col == null)
				return;

			// this gives the value in the selected cell:
			Object data = col.getCellObservableValue(item).getValue();

			// get the relative virtual address (offset) from the table
			TableColumn toff = table.getColumns().get(2);

			// get the actual value of the currently selected cell
			ObservableValue off = toff.getCellObservableValue(row);


			if (col.getText().equals(Global.col_offset)) {
				// get currently selected database
				NodeObject no = getSelectedNode();

				String model = null;

				if (no.type == FileType.SQLiteDB)
					model = no.job.path;
				else if (no.type == FileType.WriteAheadLog)
					model = no.job.wal.path;
				else if (no.type == FileType.RollbackJournalLog)
					model = no.job.rol.path;


				long position = -1;
				try {
					position = Long.parseLong((String) data);
					if (event.getClickCount() == 2)
						HEXVIEW.go2(model, position);

				} catch (Exception err) {
					AppLog.error(err.getMessage());
				}


			} else  //another column was clicked
			{
				boolean doubleclicked = false;

				if (event.getButton().equals(MouseButton.PRIMARY)) {
					if (event.getClickCount() == 2) {
						doubleclicked = true;
					}
				}

				if (data != null) {

					// get detailed position (requires active selection of a cell)
					TablePosition cpos = table.getFocusModel().getFocusedCell();
					int crow = cpos.getRow();
					int colIndex = cpos.getColumn();

					// retrieve complete row form currently selected table cell
					ObservableList<String> rowData = (ObservableList<String>) table.getItems().get(crow);
					String firstCellValue = rowData.getFirst();

					int delta;
					if (table.sqltypes != null) {
						delta = table.getSelectionModel().getTableView().getColumns().size() - table.sqltypes.size();
						if (cpos.getColumn() >= delta) {
							String type = table.sqltypes.get(cpos.getColumn() - delta);
							updateHexDump(Integer.parseInt(firstCellValue) - 1, colIndex, job, tablename, type);
						} else
							updateStandardColumns(cpos, delta);
					} else if (tablename.equals("sqlite_master")) {
						updateHexDump(Integer.parseInt(firstCellValue) - 1, colIndex, job, tablename, "TEXT");
					} else {
						btnBinViewer.setDisable(true);
						updateStandardColumns(cpos, 0);
					}


				}


				if (data != null && doubleclicked) {

					String cellvalue = (String) data;
					/* Has the user double-clicked on a BLOB column? */
					if (cellvalue.startsWith("[BLOB")) {
						int from = cellvalue.indexOf("BLOB-");
						int to = cellvalue.indexOf("]");
						String blobcolidx = cellvalue.substring(from + 5, to);

						String key = off.getValue() + "-" + blobcolidx;
						BLOBElement e = job.bincache.get(key);
						BLOBTYPE type = e.type;

						/* note: there are only a few supported file formats at the moment */

						/* Is the BLOB a PDF? */
						if (type == BLOBTYPE.PDF) {

							Platform.runLater(() -> {

								String path = GUI.baseDir + Global.separator + table.dbname + "_" + off.getValue() + "-" + blobcolidx + "." + "pdf";
								String uri = "file:" + Global.separator + Global.separator + GUI.baseDir + Global.separator + table.dbname + "_" + off.getValue() + "-" + blobcolidx + "." + "pdf";

								try {
									BufferedOutputStream buffer = new BufferedOutputStream(new FileOutputStream(path));
									buffer.write(e.binary);
									buffer.close();
									/* open the PDF-file by using our internal viewer */
									getHostServices().showDocument(uri);
								} catch (Exception err) {
									// simply do nothing, if opening the viewer fails
								}

							});


						}
						/* Is it a common picture format? */
						if (type == BLOBTYPE.GIF || type == BLOBTYPE.BMP || type == BLOBTYPE.PNG || type == BLOBTYPE.JPG || type == BLOBTYPE.HEIC || type == BLOBTYPE.TIFF) {
							String extension;
							switch (type) {
								case GIF:
									extension = ".gif";
									break;
								case BMP:
									extension = ".bmp";
									break;
								case PNG:
									extension = ".png";
									break;
								case JPG:
									extension = ".jpg";
									break;
								case HEIC:
									extension = ".heic";
									break;
								case TIFF:
									extension = ".tiff";
									break;
								default:
									extension = ".bin";
							}


							String uri = "file:" + Global.separator + Global.separator + GUI.baseDir + Global.separator + table.dbname + "_" + off.getValue() + "-" + blobcolidx + "." + extension;
							String path = GUI.baseDir + Global.separator + table.dbname + "_" + off.getValue() + "-" + blobcolidx + "." + extension;

							try {
								/* before we can open it, we need to create a file on the file system for the BLOB */
								BufferedOutputStream buffer = new BufferedOutputStream(new FileOutputStream(path));
								buffer.write(e.binary);
								buffer.close();
								/* open picture with the default viewer from the operating system associated with this file extension*/
								getHostServices().showDocument(uri);
							} catch (Exception err) {
								AppLog.error(err.getMessage());
							}
						}
					}

				}

			}

		});

		return future;
	}

	public void updateStandardColumns(TablePosition pos, int delta) {
		if (pos.getColumn() < delta) {

			switch (pos.getColumn()) {

				case 0:
					updateFormatBadge("INT");
					break;
				case 1:
					updateFormatBadge("TEXT");
					break;
				case 2:
					updateFormatBadge("INT");
					break;
				case 3:
					updateFormatBadge("TEXT");
					break;
				case 4:
					updateFormatBadge("INT");
					break;

			}

		}

		Platform.runLater(() -> {
			hexDumpArea.getChildren().clear();
		});

	}


	@SuppressWarnings({"rawtypes", "unchecked"})
	private ContextMenu createContextMenu(CtxTypes type, String tablename, FQTableView table, Job job) {

		final ContextMenu contextMenu = new ContextMenu();
		FQTableView mytable = table;

		// copy a single table line
		MenuItem mntcopyline = new MenuItem("Copy Line(s)");
		String s = Objects.requireNonNull(GUI.class.getResource("/edit-copy.png")).toExternalForm();
		ImageView iv = new ImageView(s);

		KeyCodeCombination copylineCombination = new KeyCodeCombination(KeyCode.L, KeyCombination.SHORTCUT_DOWN);
		KeyCodeCombination copycellCombination = new KeyCodeCombination(KeyCode.C, KeyCombination.SHORTCUT_DOWN);

		mntcopyline.setAccelerator(copylineCombination);
		mntcopyline.setGraphic(iv);
		mntcopyline.setOnAction(e -> {
					copyLineAction(mytable);
					e.consume();
				}
		);


		// copy the complete table line (with all cells)
		MenuItem mntcopycell = new MenuItem("Copy Cell");
		s = Objects.requireNonNull(GUI.class.getResource("/edit-copy.png")).toExternalForm();
		iv = new ImageView(s);
		mntcopycell.setGraphic(iv);
		mntcopycell.setAccelerator(copycellCombination);

		mntcopycell.setOnAction(e -> {
					copyCellAction(mytable);
					e.consume();
				}
		);


		SeparatorMenuItem sepA = new SeparatorMenuItem();

		s = Objects.requireNonNull(GUI.class.getResource("/edit-find.png")).toExternalForm();
		iv = new ImageView(s);

		MenuItem location = new MenuItem("Show Location (with openstreetmap.org))");
		location.setDisable(true);


		ObservableList<TablePosition> selection = mytable.getSelectionModel().getSelectedCells();
		// nothing selected -> leave copy action
		if (!selection.isEmpty()) {

			// where am I?
			TablePosition tp = selection.get(0);
			int row = tp.getRow();
			int col = tp.getColumn();
			System.out.println(col);
			if(col+1 < table.getColumns().size()) {
				TableColumn tc = (TableColumn) table.getColumns().get(col);
				ObservableValue observableValue = tc.getCellObservableValue(row);
				String cellvalue1 = (String) observableValue.getValue();
				TableColumn tc2 = (TableColumn) table.getColumns().get(col + 1);
				ObservableValue observableValue2 = tc2.getCellObservableValue(row);
				String cellvalue2 = (String) observableValue2.getValue();

				String lat = cellvalue1.replace(",", ".");
				String lon = cellvalue2.replace(",", ".");

				if (GPSParser.looksLikeLatitude(lat) && GPSParser.looksLikeLongitude(lon)) {
					location.setDisable(false);
				} else
					location.setDisable(true);

				location.setOnAction(e -> {

					try {

						if (GPSParser.looksLikeLatitude(lat) && GPSParser.looksLikeLongitude(lon)) {
							GeoCoordinate coo = GPSParser.parseLatLonPair(lat + "," + lon);
							//System.out.println(" Coordinates: " + coo.getLatitude() + " " + coo.getLongitude());
							showLocation(coo.getLatitude(), coo.getLongitude());
						}


					} catch (Exception err) {
						Alert alert = new Alert(AlertType.ERROR);
						alert.setTitle("Error");
						alert.setContentText("No valid gps coordinates.");
						alert.showAndWait();
					}

				});

			}



		}

		contextMenu.getItems().addAll(mntcopyline, mntcopycell , sepA, location);

		return contextMenu;
	}


	private void setContent(String data) {
		content.putString(data);
		clipboard.setContent(content);
	}

	/**
	 * Action handler method.
	 *
	 * @param table table object
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	private void copyCellAction(FQTableView table) {

		final javafx.scene.input.Clipboard clipboard = javafx.scene.input.Clipboard.getSystemClipboard();
		final ClipboardContent content = new ClipboardContent();

		ObservableList<TablePosition> selection = table.getSelectionModel().getSelectedCells();
		if (selection.isEmpty())
			return;
		TablePosition tp = selection.get(0);
		int row = tp.getRow();
		int col = tp.getColumn();


		TableColumn tc = (TableColumn) table.getColumns().get(col);
		ObservableValue observableValue = tc.getCellObservableValue(row);

		String cellvalue = "";

		// not null-check: provide empty string for nulls
		if (observableValue != null) {
			cellvalue = (String) observableValue.getValue();

			// handle binary values like protocol buffers, Java serials or property lists
			if (cellvalue.startsWith("[BLOB-")) {

				tc = (TableColumn) table.getColumns().get(2);
				ObservableValue off = tc.getCellObservableValue(row);

				String path = SQLiteDatabaseCreator.getBLOBKey(cellvalue,table.dbname,(String)off.getValue());
				String data = table.job.bincache.getHexString(path);
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


	/**
	 * This method handles the "copy lines" action. All data of all selected lines
	 * is copied to the system clipboard. BLOBs will be completely extracted from
	 * the file cache.
	 *
	 * @param table the table the action belongs to
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	private void copyLineAction(FQTableView table) {

		StringBuffer sb = new StringBuffer();
		final javafx.scene.input.Clipboard clipboard = javafx.scene.input.Clipboard.getSystemClipboard();
		final ClipboardContent content = new ClipboardContent();
		ObservableList<TablePosition> selection = table.getSelectionModel().getSelectedCells();
		Iterator<TablePosition> iter = selection.iterator();

		String token = Global.CSV_SEPARATOR;

		if (token.equals("[TAB]"))
			token = "\t";


		while (iter.hasNext()) {

			TablePosition pos = iter.next();
			ObservableList<String> hl = (ObservableList<String>) table.getItems().get(pos.getRow());

			Iterator<String> s = hl.iterator();

			int current = 0;
			String offset = null;

			// BLOB handling
			while (s.hasNext()) {

				/* column for offset found? */
				if (current > 7) {
					offset = s.next();

					sb.append(token);
					sb.append(offset);
					current++;
					continue;
				}

				String cellvalue = s.next();

				/* BLOB-value found? */
				if (cellvalue.length() > 5) {

					int from = cellvalue.indexOf("BLOB-");
					int to = cellvalue.indexOf("]");

					if (from > 0 && to > 0) {

						String number = cellvalue.substring(from + 5, to);

						String path = GUI.baseDir + Global.separator + table.dbname + "_" + offset + "-" + number;
						String data = table.job.bincache.getHexString(path);
						cellvalue = data.toUpperCase();
					}
				}

				if (current > 0)
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
	 * Returns the tree-path for a given node item.
	 *
	 * @param item
	 * @return path as string
	 */
	private String getPath(TreeItem<NodeObject> item) {


		// create the entire path to the selected item.
		String path = item.getValue().name;
		TreeItem<NodeObject> tmp = item.getParent();

		while (tmp != null) {
			path = tmp.getValue().name + "/" + path;
			tmp = tmp.getParent();
		}

		return path;

	}

	/**
	 * Returns the currently selected tree node.
	 *
	 * @return node object selected
	 */
	public NodeObject getSelectedNode() {
		return tree.getSelectionModel().getSelectedItem().getValue();
	}

	/**
	 * Sometimes we need an empty table as a placeholder.
	 *
	 */
	@SuppressWarnings("rawtypes")
	protected void prepare_tree() {

		if (tree == null) {
			tree = new TreeView<>(root);
			tree.getSelectionModel().setSelectionMode(SelectionMode.SINGLE);
			tree.setCacheHint(CacheHint.SPEED);
		}

		rightSide.getChildren().add(rootPane);

		tree.getSelectionModel().selectedItemProperty().addListener((ChangeListener<TreeItem>) (observable, oldValue, newValue) -> {


			TreeItem<NodeObject> selectedItem = (TreeItem<NodeObject>) newValue;
			if (null == selectedItem) {
				Platform.runLater(() -> {
					rightSide.getChildren().clear();
					rightSide.getChildren().add(rootPane);
				});
				return;
			}

			NodeObject node = selectedItem.getValue();
			if (null != node.tablePane) {
				Platform.runLater(() -> {

					VBox hexPane = buildFileHexPane();

					formatBadge = new Label("\u2013");
					formatBadge.getStyleClass().add("format-badge");
					formatBadge.setPadding(new Insets(3, 10, 3, 10));

					Region spacer = new Region();
					VBox.setVgrow(spacer, Priority.ALWAYS);

					btnBinViewer = new Button("\uD83D\uDD0E Analyze");
					btnBinViewer.setOnAction(event -> {
						BinViewer.openBytes(currently_selected_hex, "BLOB-View");
					});

					VBox buttonpane = new VBox();
					buttonpane.setSpacing(15);
					buttonpane.setMargin(btnBinViewer, new Insets(10, 10, 10, 10));
					buttonpane.getChildren().addAll(formatBadge, spacer, btnBinViewer);
					btnBinViewer.setDisable(true);

					SplitPane dualPane = new SplitPane(hexPane, buttonpane);
					dualPane.setOrientation(Orientation.HORIZONTAL);
					dualPane.setDividerPositions(0.80);

					if (node.tablePane.getParent() != null) {
						javafx.scene.Parent p = node.tablePane.getParent();
						if (p instanceof javafx.scene.layout.Pane) {
							((javafx.scene.layout.Pane) p).getChildren().remove(node.tablePane);
						} else if (p.getParent() instanceof SplitPane) {
							((SplitPane) p.getParent()).getItems().remove(node.tablePane);
						}
					}

					SplitPane vSplit = new SplitPane();
					vSplit.setOrientation(Orientation.VERTICAL);
					vSplit.setDividerPositions(0.75);
					vSplit.getItems().add(node.tablePane);
					vSplit.getItems().add(dualPane);

					rightSide.getChildren().setAll(vSplit);
					VBox.setVgrow(vSplit, Priority.ALWAYS);
					VBox.setVgrow(node.tablePane, Priority.ALWAYS);

					disableButtons(false);

				});
			}
			else // no table -> show db pane
			{

				if (node.isRoot) {
					rightSide.getChildren().clear();
					rightSide.getChildren().add(rootPane);

					disableButtons(true);

				} else {
					disableButtons(false);

					Platform.runLater(() -> {
						String tp = getPath(selectedItem);
						StackPane dbpanel = (StackPane) tables.get(tp);

						rightSide.getChildren().clear();
						if (null != dbpanel) {
							rightSide.getChildren().add(dbpanel);
							ThemeManager.setDark(ThemeManager.isDark());
							dbpanel.setPrefHeight(4000);
							VBox.setVgrow(dbpanel, Priority.ALWAYS);
						}


					});
				}
			}
		});


		tree.addEventFilter(javafx.scene.input.KeyEvent.KEY_PRESSED, event -> {
			if (event.getCode() != KeyCode.TAB) return;

			TreeItem<NodeObject> selectedItem = tree.getSelectionModel().getSelectedItem();
			if (selectedItem == null) return;

			NodeObject node = selectedItem.getValue();
			if (node == null || node.tablePane == null) return;

			// tablePane ist VBox: Index 0 = filterpane (HBox), Index 1 = FQTableView
			if (node.tablePane.getChildren().size() < 2) return;

			javafx.scene.Node tableNode = node.tablePane.getChildren().get(1);
			if (!(tableNode instanceof FQTableView)) return;

			FQTableView<?> tbl = (FQTableView<?>) tableNode;

			event.consume(); // Tab-Default-Navigation im Tree unterdrücken

			Platform.runLater(() -> {
				tbl.requestFocus();
				// Falls noch keine Zeile selektiert ist, erste Zeile wählen
				if (tbl.getSelectionModel().isEmpty() && !tbl.getItems().isEmpty()) {
					tbl.getSelectionModel().select(0);
					tbl.scrollTo(0);
				}
			});
		});


	}

	/**
	 * Depending on what is selected in the tree (db node or something else), we need to deactivate some of the
	 * menu items and buttons in the toolbar.
	 *
	 * @param active
	 */
	private void disableButtons(boolean active) {

		Platform.runLater(() -> {

			btnLLM.setDisable(active);
			btnSQL.setDisable(active);
			btnFTS.setDisable(active);
			btnExport.setDisable(active);
			btnExportDB.setDisable(active);
			btnHTML.setDisable(active);
			btnCASE.setDisable(active);
			btnSchema.setDisable(active);
			hexViewBtn.setDisable(active);
			cmExport.setDisable(active);
			mntmExportDB.setDisable(active);
			mntmHex.setDisable(active);
			mntmSQL.setDisable(active);
			mntmSchema.setDisable(active);
			mntmHTML.setDisable(active);
		});
	}

	/**
	 * Show an open dialog and import <code>sqlite</code>-file.
	 * After selecting a file, the import will be automatically
	 * started.
	 *
	 */
	public synchronized void open_db(File f){

		/* Prevent concurrent imports: check if any known job is still processing */
		List<TreeItem<NodeObject>> openDbs = TreeHelper.getFirstLevelTreeItems(tree);
		for (TreeItem<NodeObject> item : openDbs) {
			NodeObject no = item.getValue();
			if (no != null && no.job != null && no.job.runningTasks.get() > 0) {
				Alert alert = new Alert(AlertType.INFORMATION);
				alert.setTitle("Import running");
				alert.setContentText("An import is already in progress. Please wait until it finishes before opening another database.");
				alert.showAndWait();
				return;
			}
		}
		File file = f;

		if (file == null) {
			FileChooser fileChooser = new FileChooser();
			fileChooser.getExtensionFilters().addAll(
					new FileChooser.ExtensionFilter("<all>", "*.*")
					, new FileChooser.ExtensionFilter(".sqlite", "*.sqlite")
					, new FileChooser.ExtensionFilter(".db", "*.db")
			);
			file = fileChooser.showOpenDialog(this.stage);
			if (file != null) {
				fileChooser.setInitialDirectory(file.getParentFile());
			}
		}


		if (file == null)
			return;

		/* Check if this database is already loaded */
		final String canonicalPath;
		try {
			canonicalPath = file.getCanonicalPath();
		} catch (IOException e) {
			AppLog.error("Could not resolve canonical path for " + file.getAbsolutePath() + ": " + e.getMessage());
			return;
		}
		for (TreeItem<NodeObject> item : openDbs) {
			NodeObject no = item.getValue();
			if (no != null && no.job != null) {
				try {
					String loadedPath = new File(no.job.path).getCanonicalPath();
					if (loadedPath.equals(canonicalPath)) {
						Alert alert = new Alert(AlertType.WARNING);
						alert.setTitle("Database already loaded");
						alert.setHeaderText(file.getName() + " is already open.");
						alert.setContentText("This database has already been imported. Close it first before importing it again.");
						alert.showAndWait();
						return;
					}
				} catch (IOException ignore) {
					// if we can't resolve the path, don't block the import
				}
			}
		}



		/* check file size - the size has to be at least 512 Byte */
		if (file.length() < 512) {
			Alert alert = new Alert(AlertType.ERROR);
			alert.setTitle("Error");
			alert.setContentText("File size is smaller than 512 bytes. Import stopped.");
			alert.showAndWait();
			return;
		} else if (file.length() > 8000000000L) {
			Alert alert = new Alert(AlertType.ERROR);
			alert.setTitle("Error");
			alert.setContentText("File is too large. Import stopped.");
			alert.showAndWait();
			return;
		}

		File dbFile = file;

		boolean encrypted = looksEncrypted(dbFile);
		AppLog.info("File: " + dbFile.getName() + " | encrypted: " + encrypted);

		if (encrypted) {

			// Show decryption dialog
			Optional<SQLCipherParams> paramsOpt = SQLCipherDecryptDialog.show(this.stage);

			if (paramsOpt.isEmpty()) {
				AppLog.info("User cancelled the decryption dialog.");
			}

			SQLCipherParams params = paramsOpt.get();

			// Forensic page-level decrypt (preserves everything)
			File plainDb = DecryptProgressDialog.show(this.stage, dbFile, params);
			System.out.println(plainDb);
			file = plainDb;

            try {
                SQLCipherForensicExport.Result result =

                        SQLCipherForensicExport.exportAll(
                                dbFile,
                                new File(dbFile.getParent()),
                                params,
                                (current, total) -> {});
            } catch (Exception e) {
                throw new RuntimeException(e);
            }


			// WAL und Journal werden automatisch erkannt und verarbeitet
			//if (result.walDb() != null) {
				// WAL war vorhanden – merged DB öffnen
			//	loadDatabase(result.walDb());
			//} else {
			//	loadDatabase(result.mainDb());
			//}

        }


		RandomAccessFile raf = null;
		boolean abort = false;
		/* check header string for magic number to match */
		try {
			raf = new RandomAccessFile(file, "r");
			byte[] h = new byte[16];
			raf.read(h);
			if (!Auxiliary.bytesToHex3(h).equals(Job.MAGIC_HEADER_STRING)) {
				abort = true;
				Alert alert = new Alert(AlertType.ERROR);
				alert.setTitle("Error");
				alert.setContentText("Couldn't find a valid SQLite3 magic. Import stopped.");
				alert.showAndWait();

			}


			/*
			 * Compute the entropy for the first bytes of the database.
			 * This is where the schema definition normally resides. If
			 * The entropy is higher than 7.5; it is definitely encrypted.
			 */


			byte[] begin;
			if (raf.length() >= 4096)
				begin = new byte[4000];
			else
				begin = new byte[400];

			raf.read(begin);
			double entropy = Auxiliary.entropy(begin);
			System.out.println("Entropy::" + entropy);
			if (entropy > 7.5) {
				abort = true;
				Alert alert = new Alert(AlertType.ERROR);
				alert.setTitle("Error");
				alert.setContentText("The database file seems to be encrypted! \n Entropy value : " + entropy + " \n Import stopped.");
				alert.showAndWait();

			}


		} catch (Exception err) {
			Alert alert = new Alert(AlertType.ERROR);
			alert.setTitle("Error");
			alert.setContentText("IO-Exception. Cloud not open file.");
			alert.showAndWait();
			abort = true;
		} finally {
			try {
				raf.close();
			} catch (IOException err) {
				AppLog.error(err.getMessage());
			}
		}
		/* no valid file or no permissions -> cancel import */
		if (abort)
			return;

		FileInfo info = new FileInfo(file.getAbsolutePath());
		ThemeManager.subscribe(info);

		DBPropertyPanel panel = new DBPropertyPanel(this, info, file.getName());
		panel.setPrefHeight(4000);
		VBox.setVgrow(panel, Priority.ALWAYS);


		NodeObject o = new NodeObject(file.getName(), null, -1, FileType.SQLiteDB, 99, false);
		TreeItem<NodeObject> dbNode = new TreeItem<>(o);
		dbNode.setGraphic(createFadeTransition("loading..."));
		root.getChildren().add(dbNode);

		/* insert Panel with general header information for this database */
		String tp = getPath(dbNode);

		Job job = new Job();
		tables.put(tp, panel);
		dbnames.add(file.getName());


		/* Does a companion RollbackJournal exist? */
		if (doesRollbackJournalExist(file.getAbsolutePath()) > 0) {
			NodeObject ro = new NodeObject(file.getName() + "-journal", null, -1,
					FileType.RollbackJournalLog, 100, false);
			rjNode = new TreeItem<>(ro);
			dbnames.add(file.getName() + "-journal");

			rjNode.setGraphic(createFadeTransition("loading..."));

			root.getChildren().add(rjNode);

			/* insert Panel with general header information for this database */
			String tpr = getPath(rjNode);
			FileInfo rinfo = new FileInfo(file.getAbsolutePath() + "-journal");
			ThemeManager.subscribe(rinfo);
			RollbackPropertyPanel rpanel = new RollbackPropertyPanel(rinfo);
			tables.put(tpr, rpanel);

			job.rjNode = rjNode;
			job.setRollbackPropertyPanel(rpanel);
			ro.job = job;
			ro.job.readRollbackJournal = true;
			ro.job.readWAL = false;

		}

		/* Does a companion WAL-archive exist? */
		else if (doesWALFileExist(file.getAbsolutePath()) > 0) {

			NodeObject wo = new NodeObject(file.getName() + "-wal", null, -1, FileType.WriteAheadLog, 101, false);
			walNode = new TreeItem<>(wo);
			walNode.setGraphic(createFadeTransition("loading..."));
			dbnames.add(file.getName() + "-wal");

			root.getChildren().add(walNode);

			/* insert Panel with general header information for this database */
			String tpw = getPath(walNode);
			FileInfo winfo = new FileInfo(file.getAbsolutePath() + "-wal");
			ThemeManager.subscribe(winfo);
			WALPropertyPanel wpanel = new WALPropertyPanel(winfo, this);
			winfo.setTheme(ThemeManager.isDark());
			tables.put(tpw, wpanel);
			job.walNode = walNode;

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
	 * During import, a fading "loading..." message is shown. This method
	 * creates a label for this purpose.
	 *
	 * @param msg labeltext
	 * @return
	 */
	private Label createFadeTransition(String msg) {
		Label l = new Label(msg);
		FadeTransition fadeTransition = new FadeTransition(Duration.seconds(1.0), l);
		fadeTransition.setFromValue(1.0);
		fadeTransition.setToValue(0.0);
		fadeTransition.setCycleCount(Animation.INDEFINITE);
		fadeTransition.play();
		return l;
	}

	/**
	 * Try to find out if there is a companion WAL-Archive. This file can be found
	 * in the same directory as the database file, and also has the same name as the
	 * database, but with 4 characters added to the end – “-wal”
	 *
	 * @param dbfile database filename
	 * @return true, if file exists.
	 */
	public static long doesWALFileExist(String dbfile) {


		String walpath = dbfile + "-wal";
		Path path = Paths.get(walpath);

		if (Files.exists(path, LinkOption.NOFOLLOW_LINKS))
			try {
				return Files.size(path);
			} catch (IOException e) {
				e.printStackTrace();
			}

		return -1L;

	}

	/**
	 * Try to find out if there is a companion RollbackJournal-File. This file can
	 * be found in the same directory as the database file, and also has the same
	 * name as the database, but with 8 characters added to the end – “-journal”
	 *
	 * @param dbfile database filename
	 * @return file size of the rollback-journal archive or -1 if no journal files exist.
	 */
	public static long doesRollbackJournalExist(String dbfile) {
		String rolpath = dbfile + "-journal";
		Path path = Paths.get(rolpath);

		if (Files.exists(path, LinkOption.NOFOLLOW_LINKS))
			try {
				return Files.size(path);
			} catch (IOException e) {
				e.printStackTrace();
			}

		return -1L;

	}

	/**
	 * Print a new message to the trace window at the bottom of the screen.
	 *
	 * @param message the log message
	 */
	protected void doLog(String message) {
		AppLog.info(message);
	}


	/**
	 * This method is called to transfer the recovered data of a database to a new database.
	 *
	 * @param no Database node for export
	 */
	private void export_db(NodeObject no) {

		boolean success;

		FileChooser fileChooser = new FileChooser();
		fileChooser.setTitle("Export recovered data to a new Database");
		fileChooser.setInitialFileName(prepareDefaultDBName(no.name));
		File f = fileChooser.showSaveDialog(stage);

		if (null == f)
			return;

		ExportType etype = switch (no.tabletype) {
			case 99 -> ExportType.SQLITEDB;
			case 100 -> ExportType.ROLLBACKJOURNAL;
			case 101 -> ExportType.WALARCHIVE;
			default -> null;
		};

		success = no.job.exportDB(no.job.filename, f.getAbsolutePath(), etype);

	}


	/**
	 * This method is called to transfer the recovered data of a database to a new database.
	 *
	 * @param no Database node for export
	 */
	private void export_html(NodeObject no) {

		boolean success;

		FileChooser fileChooser = new FileChooser();
		fileChooser.setTitle("Export recovered data to html report");
		fileChooser.setInitialFileName(prepareDefaultHtmlReportName(no.name));
		File f = fileChooser.showSaveDialog(stage);

		if (null == f)
			return;

		int tp;
		if(no.isTable){

			TreeItem<NodeObject> parent = getDatabaseNode();
			tp = parent.getValue().tabletype;
		}
		else
			tp = no.tabletype;


		ExportType etype = switch (tp) {
			case 99  -> ExportType.SQLITEDB;
			case 100 -> ExportType.ROLLBACKJOURNAL;
			case 101 -> ExportType.WALARCHIVE;
			default -> null;
		};

		try {
			no.job.exportToHtml(no.job.filename, f.getAbsolutePath(), f.getParent(), etype, no.isTable, no.name);
			openInBrowser(f.getAbsolutePath(), gui.getHostServices());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	/**
	 * This method is called to transfer the recovered data of a database to a new database.
	 *
	 * @param no Database node for export
	 */
	private void export_case(NodeObject no) {

		boolean success;

		FileChooser fileChooser = new FileChooser();
		fileChooser.setTitle("Export recovered data to CASE ONTOLOGY file ");
		fileChooser.setInitialFileName(prepareDefaultCASEReportName(no.name));
		File f = fileChooser.showSaveDialog(stage);

		if (null == f)
			return;

		int tp;
		if(no.isTable){

			TreeItem<NodeObject> parent = getDatabaseNode();
			tp = parent.getValue().tabletype;
		}
		else
			tp = no.tabletype;


		ExportType etype = switch (tp) {
			case 99  -> ExportType.SQLITEDB;
			case 100 -> ExportType.ROLLBACKJOURNAL;
			case 101 -> ExportType.WALARCHIVE;
			default -> null;
		};

		try {
			no.job.exportToCASE(no.job.filename, f.getAbsolutePath(), f.getParent(), etype, no.isTable, no.name);
			//openInBrowser(f.getAbsolutePath(), gui.getHostServices());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}





	public void openInBrowser(String htmlFilePath, HostServices hostServices) {
		try {
			File htmlFile = new File(htmlFilePath);
			String url = htmlFile.toURI().toString();
			hostServices.showDocument(url);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("An error occurred while try to open browser: " + e.getMessage());
		}
	}


	/**
	 * This method is called to write the contents of a database to a CSV file.
	 *
	 * @param no Database node for export
	 */
	private void export2csv(NodeObject no) {

		if (null == no)
			return;

		FileChooser fileChooser = new FileChooser();
		fileChooser.setTitle("Export to .csv");
		fileChooser.setInitialFileName(prepareDefaultFileName(no.name));
		File f = fileChooser.showSaveDialog(stage);

		if (null == f)
			return;

		boolean success = false;

		switch (no.tabletype) {
			case 0:    // table
			case 1:   // index

				success = no.job.exportResults2File(f, no.name, no.type, ExportType.SINGLETABLE);
				break;

			case 99:   // database

				success = no.job.exportResults2File(f, no.name, no.type, ExportType.SQLITEDB);
				break;

			case 100:   // journal

				success = no.job.exportResults2File(f, no.name, no.type, ExportType.ROLLBACKJOURNAL);
				break;


			case 101:   // wal

				success = no.job.exportResults2File(f, no.name, no.type, ExportType.WALARCHIVE);
				break;

			default:  // root;
		}

		if (success) {
			Alert alert = new Alert(AlertType.INFORMATION);
			alert.setTitle("Success Info");
			alert.setContentText("Data of " + no.name + " exported successfully to \n" + f.getAbsolutePath());
			alert.showAndWait();
		}


	}

	/**
	 * Returns a filename with a time stamp in ISO_DATE_TIME format.
	 *
	 * @param nameofnode
	 * @return filename with timestamp included.
	 */
	private String prepareDefaultFileName(String nameofnode) {
		LocalDateTime now = LocalDateTime.now();
		DateTimeFormatter df;
		df = DateTimeFormatter.ISO_DATE_TIME; // 2020-01-31T20:07:07.095
		String date = df.format(now);
		date = date.replace(":", "_");
		return nameofnode + date + ".csv";
	}

	/**
	 * Returns a filename with a time stamp in ISO_DATE_TIME format.
	 *
	 * @param nameofnode
	 * @return dbname with timestamp included.
	 */
	private String prepareDefaultDBName(String nameofnode) {
		LocalDateTime now = LocalDateTime.now();
		DateTimeFormatter df;
		df = DateTimeFormatter.ISO_DATE_TIME; // 2020-01-31T20:07:07.095
		String date = df.format(now);
		date = date.replace(":", "_");
		return nameofnode + date + ".sqlite";
	}

	private String prepareDefaultHtmlReportName(String nameofnode) {
		LocalDateTime now = LocalDateTime.now();
		DateTimeFormatter df;
		df = DateTimeFormatter.ISO_DATE_TIME; // 2020-01-31T20:07:07.095
		String date = df.format(now);
		date = date.replace(":", "_");
		return nameofnode + date + ".html";
	}

	private String prepareDefaultCASEReportName(String nameofnode) {
		LocalDateTime now = LocalDateTime.now();
		DateTimeFormatter df;
		df = DateTimeFormatter.ISO_DATE_TIME; // 2020-01-31T20:07:07.095
		String date = df.format(now);
		date = date.replace(":", "_");
		return nameofnode + date + ".jsonld";
	}



	/**
	 * This method is used to insert new records into an output table.
	 *
	 * @param treepath   the table name
	 * @param rows       String array with data rows
	 * @param isWALTable this table to fill is a WAL-Table
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void update_table(String treepath, ObservableList<ObservableList<String>> rows, ObservableList<ObservableList<byte[]>> rawbytes, boolean isWALTable) {

		if (null == treepath)
			return;

		datasets.put(treepath, rows);

		int linenumber = 0;
		int r = 0;
		for (ObservableList<String> row : rows) {
			int c = 0;
			for (String cell : row) {
				if (cell.startsWith("[BLOB")) {
					if (c < rawbytes.get(r).size()) {
						byte[] value = rawbytes.get(r).get(c);
						currentFormat = FormatDetector.detect(value);
						switch (currentFormat) {
							case PNG, GIF, BMP, JPEG, TIFF, HEIC -> {
								String picture = "\uD83D\uDDBC\uFE0F";
								row.set(c, cell += picture);
							}
							case PDF -> {
								String picture = "\uD83D\uDCD6";
								row.set(c, cell += picture);
							}
						}
					}
				}
				c++;
			}
			r++;
			row.add(0, String.valueOf(++linenumber));
		}

		// define an array list for all table rows
		ObservableList<ObservableList> obdata = FXCollections.observableArrayList();
		for (ObservableList<String> row : rows) {
			obdata.add(row);
		}
		// first get the right table
		FQTableView tb;
		TextField filterField;
		ComboBox columnselector;
		Button clearFilter;

		try {

			VBox tablepanel = (VBox) tables.get(treepath);
			VBox.setVgrow(tablepanel, Priority.ALWAYS);
			HBox filterpane = (HBox) tablepanel.getChildren().get(0);
			columnselector = (ComboBox) filterpane.getChildren().get(0);
			filterField = (TextField) filterpane.getChildren().get(1);
			clearFilter = (Button) filterpane.getChildren().get(2);
			tb = (FQTableView) tablepanel.getChildren().get(1);
			Label statusline = (Label) tablepanel.getChildren().get(2);
			String text = statusline.getText();
			statusline.setText(text + " | rows: " + rows.size());
		} catch (Exception err) {
			AppLog.error(err.getMessage());
			return;
		}

		/* Just in case ;-) */
		if (tb == null) {
			AppLog.info(">>>> Unkown tablename" + treepath);
			return;
		}

		// determine the right tree item for a given treepath from the hashtable
		TreeItem<NodeObject> node = treeitems.get(treepath);

		if (null != node && rows.size() > 0) {
			node.getValue().hasData = true;

			Platform.runLater(() -> {
				String s = "";
				if (node.getValue().tabletype == 0) // normal table with rows
					s = Objects.requireNonNull(GUI.class.getResource("/table-icon.png")).toExternalForm();
				if (node.getValue().tabletype == 1) // index table with rows
					s = Objects.requireNonNull(GUI.class.getResource("/table-key-icon.png")).toExternalForm();
				if (null == s)
					return;
				ImageView iv = new ImageView(s);

				node.setGraphic(null);
				node.setGraphic(iv);
				TreeItem.graphicChangedEvent();
				TreeItem.valueChangedEvent();
				tree.refresh();
			});

		}

		final TextField ff = filterField;
		final ComboBox cs = columnselector;
		clearFilter.setOnAction(e -> {
			ff.clear();
			cs.getSelectionModel().select(0);
		});

		Iterator it = tb.getColumns().iterator();
		ArrayList<String> cnames = new ArrayList<String>();
		while (it.hasNext()) {
			TableColumn tc = (TableColumn) it.next();
			cnames.add(tc.getText().toLowerCase());
		}

		final List<String> fnames = cnames;


		FilteredList<ObservableList> filteredData = new FilteredList<>(obdata, p -> true);

		final TextField ffield = filterField;
		columnselector.getSelectionModel().selectedItemProperty().addListener((options, oldValue, newValue) -> {
			filter(treepath, fnames, columnselector, filteredData, null, ffield.textProperty().getValue());
			updatestatusline(treepath, filteredData.size(), rows.size());
		});

		filterField.textProperty().addListener((observable, oldValue, newValue) -> {
			filter(treepath, fnames, columnselector, filteredData, oldValue, newValue);
			updatestatusline(treepath, filteredData.size(), rows.size());
		});

		SortedList<ObservableList> sortedData = new SortedList<>(filteredData);

		// Muss auf FX-Thread laufen - egal ob schon drauf oder nicht
		Runnable applyItems = () -> {
			sortedData.comparatorProperty().bind(tb.comparatorProperty());
			tb.setItems(sortedData);

			final TableView tb2 = tb;
			tb.getSelectionModel().getSelectedItems().addListener((ListChangeListener.Change c) -> {
				if (tb2 != null) {
					int selecteditems = tb2.getSelectionModel().getSelectedCells().size();
					VBox tablepanel = (VBox) tables.get(treepath);
					Label statusline = (Label) tablepanel.getChildren().get(2);
					String text = statusline.getText();
					int idx = text.indexOf(" | rows: ");
					if (idx > 0)
						statusline.setText(text.substring(0, idx) + " | rows: " + rows.size() + " | selected rows: " + selecteditems);
				}
			});

			ObservableList columlist = tb.getColumns();
			for (int i = 0; i < columlist.size(); i++)
				resizeColumnManually((TableColumn) columlist.get(i));
		};

		if (Platform.isFxApplicationThread()) {
			applyItems.run();
		} else {
			Platform.runLater(applyItems);
		}

		final TableView tb2 = tb;

		// 6. Update Status line
		tb.getSelectionModel().getSelectedItems().addListener((ListChangeListener.Change c) -> {

					if (tb2 != null) {
						int selecteditems = tb2.getSelectionModel().getSelectedCells().size();
						VBox tablepanel = (VBox) tables.get(treepath);
						Label statusline = (Label) tablepanel.getChildren().get(2);
						String text = statusline.getText();
						int idx = text.indexOf(" | rows: ");
						if (idx > 0)
							statusline.setText(text.substring(0, idx) + " | rows: " + rows.size() + " | selected rows: " + selecteditems);
					}

				}

		);

		// new: resize columns
		ObservableList columlist = tb.getColumns();
		for (int i = 0; i < columlist.size(); i++)
			resizeColumnManually((TableColumn) columlist.get(i));

	}

	private void filter(String treepath, List<String> fnames, ComboBox<String> columnselector,
						@SuppressWarnings("rawtypes") FilteredList<ObservableList> filteredData,
						String oldValue, String newValue) {

		filteredData.setPredicate(r -> {

			// Bug 2 fix: null-safe guard
			if (newValue == null || newValue.isEmpty()) {
				return true; // leeres Suchfeld → alle Zeilen anzeigen
			}

			String lowerCaseFilter = newValue.toLowerCase();
			String clvalue = columnselector.getSelectionModel().getSelectedItem();
			String cname;

			if (clvalue != null && !clvalue.startsWith("All Columns")) {
				// Fall: bestimmte Spalte ausgewählt
				if (clvalue.equals("Status"))
					clvalue = "";

				cname = clvalue.toLowerCase();
				String searchfor = lowerCaseFilter.trim();  // korrekt aus lowerCaseFilter befüllt
				int cnumber = fnames.indexOf(cname);
				cnumber++;

				if (r.size() > cnumber) {
					cnumber = switch (cname) {
						case Global.col_pll    -> 2;
						case Global.col_rowid  -> 3;
						case Global.col_status -> 4;
						case Global.col_offset -> 5;
						default                -> cnumber;
					};
					String value = (String) r.get(cnumber);
					return value != null && value.toLowerCase().contains(searchfor);
				}
			} else {
				// Bug 1 fix: searchfor auf lowerCaseFilter setzen, nicht auf ""
				String searchfor = lowerCaseFilter;
				for (Object o : r) {
					String value = (String) o;
					if (value != null && value.toLowerCase().contains(searchfor)) {
						return true;
					}
				}
			}

			return false;
		});
	}


	private void updatestatusline(String treepath, int number, int total) {

		VBox tablepanel1 = (VBox) tables.get(treepath);
		Label statusline1 = (Label) tablepanel1.getChildren().get(2);
		String text1 = statusline1.getText();
		int idx1 = text1.indexOf(" | ");
		if (idx1 > 0)
			statusline1.setText(text1.substring(0, idx1) + " | showing " + number + " of " + total + " rows ");
		else
			statusline1.setText(" | showing " + number + " of " + total + " rows ");
	}


	public ConcurrentHashMap<Object, String> getRowcolors() {
		return rowcolors;
	}


	private static class CustomComparator implements Comparator<String> {

		@Override
		public int compare(String o1, String o2) {

			if (o1 == null && o2 == null)
				return 0;
			if (o1 == null)
				return -1;
			if (o2 == null)
				return 1;

			if (o1.isEmpty())
				return -1;

			char ch = o1.charAt(0);

			// only if o1 starts with a number or a sign
			if ((ch >= '0' && ch <= '9') || ch == '-' || ch == '+') {
				Integer i1 = null;
				try {
					i1 = Integer.valueOf(o1);
				} catch (NumberFormatException ignored) {
				}
				Integer i2 = null;
				try {
					i2 = Integer.valueOf(o2);
				} catch (NumberFormatException ignored) {
				}

				if (i1 == null && i2 == null)
					return o1.compareTo(o2);
				if (i1 == null)
					return -1;
				if (i2 == null)
					return 1;

				return i1 - i2;
			}

			// o1 does not start with a number -> compare String objects as usual
			return o1.compareTo(o2);
		}
	}

	private VBox buildFileHexPane() {
		Label title = new Label("Hex Dump");
		title.setFont(Font.font("System", FontWeight.BOLD, 12));
		title.getStyleClass().add("hex-dump-title");

		hexDumpArea = monospace();
		hexDumpArea.setStyle("");
		VBox pane = new VBox(4, title, hexDumpArea);
		pane.setPadding(new Insets(4));
		VBox.setVgrow(hexDumpArea, Priority.ALWAYS);
		return pane;
	}

	private TextFlow monospace() {
		Text t = new Text("");
		t.setFont(Font.font("Monospaced", 12));
		TextFlow ta = new TextFlow(t);
		ta.setCache(true);
		ta.setCacheHint(CacheHint.SPEED);
		ta.setPrefWidth(350);
		ta.setCacheShape(true);
		return ta;
	}

	private void updateHexDump(int row, int column, Job job, String tablename, String type) {

		/* add missing type information for table sqlite_sequence */
		if (tablename.equals("sqlite_sequence")) {

			if (column == 5)
				type = "TEXT";
			if (column == 6)
				type = "INTEGER";
		}
		// add missing sqltypes for the master table
		if (tablename.equals("sqlite_master")) {

			type = switch (column) {
				case 8 -> "INT";
				default -> "TEXT";
			};
		}

		String t = type;
		byte[] value = getBytesForCell(job, tablename, row, column);

		if (value != null) {

			Platform.runLater(() -> {
				currently_selected_hex = value;
				// only write the first bytes to area (performance)
				javafx.scene.paint.Color hexTextColor = javafx.scene.paint.Color.web(
						(currentTheme == Theme.DARK) ? DARK.textArea() : LIGHT.textArea());
				if (value.length > 256) {
					Text content = new Text(HexDump.dump(value, 0, 256, 0) + (value.length - 256) + " bytes more... ");
					content.setFont(Font.font("Monospaced", 12));
					content.setFill(hexTextColor);
					hexDumpArea.getChildren().clear();
					hexDumpArea.getChildren().add(content);
				} else {
					Text content = new Text(HexDump.dump(value));
					content.setFont(Font.font("Monospaced", 12));
					content.setFill(hexTextColor);
					hexDumpArea.getChildren().clear();
					hexDumpArea.getChildren().add(content);
				}

				// now we have to update the badge label
				if (t.equals("TIMESTAMP")) {
					updateFormatBadge("TIMESTAMP");
					btnBinViewer.setDisable(true);
					return;
				}

				// first check the SQL-type
				if ((t.startsWith("STRING")) || (t.contains("VARCHAR")) || (t.contains("CHARACTER")) || (t.contains("TEXT") || (t.contains("NCHAR")) || (t.contains("CLOB")))) {
					currentFormat = FormatDetector.detect(value);

					switch (currentFormat) {
						case PLAINTEXT -> {
							btnBinViewer.setDisable(true);
							updateFormatBadge();
							return;
						}

						case JSON, BASE64_JSON, XML, BASE64_XML -> {
							btnBinViewer.setDisable(false);
							if(currentFormat.equals(FormatDetector.Format.UNKNOWN))
								currentFormat = FormatDetector.Format.PLAINTEXT;
							updateFormatBadge();
							return;
						}
						default -> btnBinViewer.setDisable(true);
					}

					updateFormatBadge(t);

					return;
				}
				if (t.equals("BLOB")) {
					currentFormat = FormatDetector.detect(value);
					switch (currentFormat) {
						case PNG, GIF, BMP, JPEG, TIFF, HEIC, PDF, GZIP, AVRO -> {
							btnBinViewer.setDisable(true);
						}
						default -> btnBinViewer.setDisable(false);
					}
					updateFormatBadge();
				} else {
					// case: simple types
					updateFormatBadge(t);
					btnBinViewer.setDisable(true);
				}


			});
		} else {
			// case: now hex value available
			Platform.runLater(() -> {
				updateFormatBadge("UNKNOWN");
				hexDumpArea.getChildren().clear();
				hexDumpText = "";
			});
		}

	}

	//	Platform.runLater(() -> {
	//		hexDumpArea.getChildren().clear();
	//		this.hexDumpText = "";
	//	});

	//}

	private void updateFormatBadge(String type) {
		record BadgeStyle(String label, String color) {
		}
		BadgeStyle bs = null;
		if (null == type) {
			bs = new BadgeStyle("UNKNOWN", "#7a3a3a");
		} else {
			if (type.contains("(")) {
				type = type.substring(0, type.indexOf("("));
			}
			bs = switch (type) {
				case "INT", "INTEGER", "INTUNSIGNED", "INTSIGNED", "LONG", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT",
					 "UNSIGNEDBIGINT", "INT2", "INT8" -> new BadgeStyle("INT", "#1a4d9e");
				case "STRING", "TEXT", "CHARACTER", "CLOB", "VARCHAR", "VARYINGCHARACTER", "NCHAR",
					 "NATIVE CHARACTER", "NVARCHAR" -> new BadgeStyle("TEXT", "#1a6b2a");
				case "REAL", "DOUBLE", "DOUBLEPRECISION", "FLOAT" -> new BadgeStyle("REAL", "#4a1a6b");
				case "NUMERIC", "DECIMAL", "BOOLEAN", "DATE", "DATETIME" -> new BadgeStyle("REAL", "#6b3a1a");
				case "TIMESTAMP" -> new BadgeStyle("TIMESTAMP", "#1a6b2a");
				default -> new BadgeStyle("UNKNOWN", "#7a3a3a");
			};
		}

		formatBadge.setText(bs.label());
		formatBadge.setStyle(
				"-fx-background-color: " + bs.color() + ";"
				+ "-fx-text-fill: #ffffff;"
				+ "-fx-background-radius: 4;"
				+ "-fx-font-weight: bold; -fx-font-size: 11px;");
	}

	// ══════════════════════════════════════════════════════════════════════
	// UI helpers
	// ══════════════════════════════════════════════════════════════════════

	private void updateFormatBadge() {
		record BadgeStyle(String label, String color) {
		}
		BadgeStyle bs = switch (currentFormat) {
			case PLAINTEXT -> new BadgeStyle("PLAINTEXT", "#7a3a3a");
			case BMP -> new BadgeStyle("BITMAP (.bmp)", "#1a4d9e");
			case GIF -> new BadgeStyle("GIF (.gif)", "#1a6b2a");
			case PDF -> new BadgeStyle("PDF (.pdf)", "#7a4a00");
			case PNG -> new BadgeStyle("PNG (.png)", "#5a1a6a");
			case JPEG -> new BadgeStyle("JPEG (.jpg)", "#1a5a6b");
			case TIFF -> new BadgeStyle("TIFF (.tiff)", "#6b3a1a");
			case HEIC -> new BadgeStyle("HEIC (.heic)", "#4a1a6b");
			case GZIP -> new BadgeStyle("GZIP (.gz)", "#6b1a4a");
			case AVRO -> new BadgeStyle("AVRO (.avro)", "#1a6b5a");
			case PROTOBUF -> new BadgeStyle("PROTOBUF?", "#1a4d9e");
			case BPLIST -> new BadgeStyle("BPLIST?", "#1a6b2a");
			case XML -> new BadgeStyle("XML", "#7a4a00");
			case JSON -> new BadgeStyle("JSON", "#5a1a6a");
			case BSON -> new BadgeStyle("BSON?", "#1a5a6b");
			case MSGPACK -> new BadgeStyle("MSGPACK?", "#6b3a1a");
			case THRIFT_BINARY -> new BadgeStyle("THRIFT-BIN?", "#4a1a6b");
			case THRIFT_COMPACT -> new BadgeStyle("THRIFT-CMP?", "#6b1a4a");
			case FLATBUFFERS -> new BadgeStyle("FLATBUF?", "#1a6b5a");
			case JAVA_SERIAL -> new BadgeStyle("JAVA-SER", "#6b3a6b");
			case BASE64_BPLIST -> new BadgeStyle("B64→BPLIST", "#1a6b2a");
			case BASE64_XML -> new BadgeStyle("B64→XML", "#7a4a00");
			case BASE64_JSON -> new BadgeStyle("B64→JSON", "#5a1a6a");
			case BASE64_BSON -> new BadgeStyle("B64→BSON", "#1a5a6b");
			case BASE64_MSGPACK -> new BadgeStyle("B64→MSGPACK?", "#6b3a1a");
			case BASE64_THRIFT_BINARY -> new BadgeStyle("B64→THRIFT-B?", "#4a1a6b");
			case BASE64_THRIFT_COMPACT -> new BadgeStyle("B64→THRIFT-C?", "#6b1a4a");
			case BASE64_FLATBUFFERS -> new BadgeStyle("B64→FLATBUF?", "#1a6b5a");
			case BASE64_JAVA_SERIAL -> new BadgeStyle("B64→JAVA-SER", "#6b3a6b");
			case BASE64_PROTO -> new BadgeStyle("B64→PROTO?", "#1a4d9e");
			default -> new BadgeStyle("UNKNOWN", "#7a3a3a");
		};
		formatBadge.setText(bs.label());
		formatBadge.setStyle(
				"-fx-background-color: " + bs.color() + ";"
				+ "-fx-text-fill: #ffffff;"
				+ "-fx-background-radius: 4;"
				+ "-fx-font-weight: bold; -fx-font-size: 11px;");
	}


	/**
	 * Maps each table TreeItem to its original parent, populated once on first
	 * filter activation so that the restore path always has a valid parent reference.
	 * (JavaFX sets parent to null when an item is removed from a children list.)
	 */
	private final Map<TreeItem<NodeObject>, TreeItem<NodeObject>> tableParentCache = new LinkedHashMap<>();

	/**
	 * Applies or removes the "non-empty tables only" filter on the TreeView.
	 * When active, table nodes whose {@link NodeObject#hasData} is {@code false}
	 * (i.e. the table contains no records) are hidden.
	 * When deactivated, all previously hidden nodes are restored to their
	 * original parent using the cached parent reference.
	 */
	private void applyTableFilter() {

		List<TreeItem<NodeObject>> databases = TreeHelper.getFirstLevelTreeItems(tree);

		if (filterActive) {
			// Build parent cache BEFORE removing anything (JavaFX nulls parent on removal)
			for (TreeItem<NodeObject> dbNode : databases) {
				for (TreeItem<NodeObject> child : new ArrayList<>(dbNode.getChildren())) {
					NodeObject no = child.getValue();
					if (no != null && no.isTable) {
						tableParentCache.put(child, dbNode);
					} else if (no != null && !no.isTable) {
						// WAL / RollbackJournal container node
						for (TreeItem<NodeObject> tableNode : new ArrayList<>(child.getChildren())) {
							NodeObject tno = tableNode.getValue();
							if (tno != null && tno.isTable) {
								tableParentCache.put(tableNode, child);
							}
						}
					}
				}
			}

			// Remove table nodes that have no data (hasData == false)
			for (TreeItem<NodeObject> dbNode : databases) {
				dbNode.getChildren().removeIf(child -> {
					NodeObject no = child.getValue();
					return no != null && no.isTable && !no.hasData;
				});
				for (TreeItem<NodeObject> child : new ArrayList<>(dbNode.getChildren())) {
					if (child.getValue() != null && !child.getValue().isTable) {
						child.getChildren().removeIf(tableNode -> {
							NodeObject no = tableNode.getValue();
							return no != null && no.isTable && !no.hasData;
						});
					}
				}
			}

		} else {
			// Restore all previously hidden table nodes from the cache
			for (Map.Entry<TreeItem<NodeObject>, TreeItem<NodeObject>> entry : tableParentCache.entrySet()) {
				TreeItem<NodeObject> tableNode = entry.getKey();
				TreeItem<NodeObject> parent    = entry.getValue();
				if (!parent.getChildren().contains(tableNode)) {
					parent.getChildren().add(tableNode);
				}
			}
			tableParentCache.clear();
		}
		tree.refresh();
	}

	/**
	 * Determines whether this node is a database table, a rollbackjournal table
	 * or a WAL table.
	 *
	 * @return FileType
	 */
	public FileType belongsTo() {

		NodeObject no = null;

		/* Do we really have a database node currently selected? */
		TreeItem<NodeObject> node = tree.getSelectionModel().getSelectedItem();

		if (node == null || node.getValue().isRoot) {
			return null;
		} else if (null != node.getValue()) {
			no = node.getValue();
		}

		return no.type;

	}

	public TreeItem<NodeObject> getDatabaseNode() {

		TreeItem<NodeObject> node = tree.getSelectionModel().getSelectedItem();

		int level = tree.getTreeItemLevel(node);

		// case: Do we have the top level?
		if (level == 1) {
			return node;
		}
		// case: We are on the table level?
		else if (level == 2) {
			return node.getParent();
		}

		return null;
	}

	public byte[] getBytesForCell(Job job, String tablename, int row, int col) {
		ObservableList<ObservableList<byte[]>> hvalues;

		// What kind of archive file do we have (db,WAL,Journal)?
		FileType tp = belongsTo();
		switch (tp) {
			case SQLiteDB -> {
				hvalues = job.hexdumplist.get(tablename);
			}
			case WriteAheadLog -> {
				hvalues = job.wal.hexdumplist.get(tablename);
			}
			case RollbackJournalLog -> {
				hvalues = job.rol.hexdumplist.get(tablename);
			}
			default -> {
				hvalues = null;
			}
		}
		if (null == hvalues) {
			return null;
		}

		/* now we are ready to fill the hex-dump area */
		if (row < hvalues.size()) {
			ObservableList<byte[]> hrow = hvalues.get(row);
			if (col < hrow.size()) {
				byte[] value = hrow.get(col);
				return value;
			}

		}
		return null;
	}

	private void resizeColumnManually(TableColumn<?, ?> column) {
		double maxWidth = 10; // Mindestbreite

		// Header-Breite berücksichtigen
		Text headerText = new Text(column.getText());

		maxWidth = Math.max(maxWidth, headerText.getBoundsInLocal().getWidth() + 20);

		// Zellen-Inhalt messen
		for (int i = 0; i < column.getTableView().getItems().size(); i++) {
			if (column.getCellData(i) == null) continue;

			String t = column.getCellData(i).toString();
			if (t.length() < 40) {
				Text cellText = new Text(t);
				maxWidth = Math.max(maxWidth, cellText.getBoundsInLocal().getWidth() + 20);
			}
		}
		column.setPrefWidth(maxWidth);
	}

	public static void selectTableCell(String dbname, String tblname, int row, int column) {

		List<TreeItem<NodeObject>> databases = TreeHelper.getFirstLevelTreeItems(tree);
		if (databases.isEmpty()) return;

		for (TreeItem<NodeObject> item : databases) {
			if (!dbname.equals(item.getValue().name)) continue;

			for (TreeItem<NodeObject> tb : item.getChildren()) {
				if (!tb.getValue().name.equals(tblname)) continue;

				Platform.runLater(() -> {
					tree.getSelectionModel().select(tb);

					FQTableView tbl = (FQTableView) tb.getValue().tablePane.getChildren().get(1);

					if (tbl.getScene() != null) {
						navigateToRow(tbl, row);
					} else {
						// Noch nicht im Scenegraph - auf Scene-Property warten
						tbl.sceneProperty().addListener(new ChangeListener<javafx.scene.Scene>() {
							@Override
							public void changed(ObservableValue<? extends javafx.scene.Scene> obs,
												javafx.scene.Scene oldScene, javafx.scene.Scene newScene) {
								if (newScene != null) {
									obs.removeListener(this);
									Platform.runLater(() -> navigateToRow(tbl, row));
								}
							}
						});
					}
				});
				return;
			}
		}
	}

	private static void navigateToRow(FQTableView tbl, int row) {

		if (tbl == null || tbl.getItems() == null) return;

		final int r = Math.max(0, row - 1);

		Platform.runLater(() -> {
			tbl.refresh();
			int scrollTarget = Math.max(0, r - 2);
			tbl.scrollTo(scrollTarget);
			PauseTransition pause = new PauseTransition(Duration.millis(200));
			pause.setOnFinished(e -> tbl.getSelectionModel().select(r));
			pause.play();
		});

	}


	public static boolean looksEncrypted(File dbFile) {
		try (var is = new java.io.FileInputStream(dbFile)) {
			byte[] header = new byte[16];
			int read = is.read(header);
			if (read < 16) return false;
			String magic = new String(header, java.nio.charset.StandardCharsets.US_ASCII);
			return !magic.startsWith("SQLite format 3");
		} catch (Exception e) {
			return false;
		}
	}


} // End of class GUI
