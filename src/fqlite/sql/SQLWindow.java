package fqlite.sql;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import fqlite.ui.NodeObject;
import fqlite.util.AutoCompletion;
import fqlite.util.WordListCreator;
import javafx.scene.control.*;
import fqlite.base.GUI;
import fqlite.base.ThemeManager;
import javafx.application.Application;
import javafx.beans.value.ObservableValue;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.image.ImageView;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.scene.input.KeyEvent;
import javafx.scene.input.MouseButton;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.stage.Screen;
import javafx.stage.Stage;

/**
 *  This class implements a simple user interface for the
 *  SQL analyzer component.
 *
 *  @author Dirk Pawlaszczyk
 */
public class SQLWindow extends Application {

    ConcurrentHashMap<String, ObservableList<ObservableList<String>>> tabledata;
    TableView<Object> resultview = new TableView<>();
    List<String> dbnames;
    final ComboBox<String> dbBox = new ComboBox<>();
    final ComboBox<String> templateBox = new ComboBox<>();
    final GUI app;
    VBox root = new VBox();
    TextArea codeArea = new TextArea();
    public Label statusline;
    private final String preselection;
    static SQLParser p;
    static Stage primaryStage;
    Button btnGo;
    InMemoryDatabase inMemoryDatabase;
    String initial_statement;
    /** Stored so we can unregister it when the window closes. */
    private final Runnable themeListener = this::applyCodeAreaTheme;
    private static List<String> WORD_LIST = new ArrayList<>();

    private static final String[] KEYWORDS = new String[]{
            "ADD", "ADD CONSTRAINT", "ALL", "ALTER", "ALTER COLUMN", "ALTER TABLE",
            "AND", "ANY", "AS", "ASC", "BACK DATABASE", "BETWEEN", "CASE", "CHECK", "COLUMN",
            "CONSTRAINT", "CREATE", "CREATE DATABASE", "CREATE INDEX", "CREATE OR REPLACE VIEW",
            "CREATE TABLE", "CREATE PROCEDURE", "CREATE UNIQUE INDEX", "CREATE VIEW", "DATABASE",
            "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP", "EXEC", "EXITS", "FOREIGN KEY", "FROM",
            "FULL OUTER JOIN", "GROUP BY", "HAVING", "IN", "INDEX", "INNER JOIN", "INSERT INTO", "IS NULL",
            "IS NOT NULL", "JOIN", "LEFT JOIN", "LIKE", "LIMIT", "NOT", "NOT NULL", "ON", "OR", "ORDER BY", "OUTER JOIN",
            "PRIMARY KEY", "PROCEDURE", "RIGHT JOIN", "ROWNUM", "SELECT", "SELECT DISTINCT", "SELECT INTO", "SELECT TOP",
            "SET", "TABLE", "TOP", "TRUNCATE TABLE", "UNION", "UNION ALL", "UNIQUE", "UPDATE", "VALUES", "VIEW", "WHERE"
    };

    static final ConcurrentHashMap<String, String> templates = new ConcurrentHashMap<>();

    /**
     * Constructor of the SQL Analyser window.
     *
     * @param app        reference to the parent frame
     * @param selectedDB the name of the db for preselection
     */
    public SQLWindow(GUI app, TreeItem<NodeObject> node, String selectedDB, InMemoryDatabase mdb, String statement) {

        this.app = app;
        tabledata = app.datasets;
        List<String> db = new ArrayList<>();
        db.add(selectedDB);
        this.dbnames = db;
        this.preselection = selectedDB;
        this.inMemoryDatabase = mdb;
        this.initial_statement = statement;

        WORD_LIST.addAll(Arrays.asList(KEYWORDS));
        WordListCreator.updateWordList(node, WORD_LIST);

        List<String> listWithoutDuplicates = new ArrayList<>(new LinkedHashSet<>(WORD_LIST));
        AutoCompletion.installAutoComplete(codeArea, listWithoutDuplicates);

        templates.put("SIMPLE SELECT", "-- Place your SELECT statement below this text.\n-- Then click on the Play [>] button to execute. \nSELECT * FROM TABLENAME WHERE <condition>;");
        templates.put("INNER JOIN", "-- Returns records that have matching values in both tables \nSELECT * FROM <TABLE1> AS t1 INNER JOIN <TABLE2> AS t2 ON t1.colX = t2.colY;");
        templates.put("LEFT (OUTER) JOIN", "-- Returns all records from the left table, and the matched records from the right table \n SELECT t1.colX, t2.colY FROM <TABLE1> AS t1 LEFT JOIN <TABLE2> AS t2 ON t1.colX = t2.colY\n ORDER BY e.colname1; ");
        templates.put("RIGHT (OUTER) JOIN", "-- Returns all records from the right table, and the matched records from the left table \n SELECT t1.colX, t2.colY FROM <TABLE1> AS t1 RIGHT JOIN <TABLE2> AS t2 ON t1.colX = t2.colX; ");
        templates.put("FULL (OUTER) JOIN", " -- Returns all records when there is a match in either left or right table \n SELECT t1.colX, t2.colY FROM <table1> AS t1 FULL OUTER JOIN <table2> AS t2 ON t1.colX = t2.colY WHERE <condition>;");
    }

    public void show() {
        primaryStage.show();
        primaryStage.toFront();
    }

    public static void main(String[] args) {
        launch(args);
    }

    @SuppressWarnings("unused")
    @Override
    public void start(Stage primaryStage) {

        SQLWindow.primaryStage = primaryStage;

        primaryStage.setTitle("SQL Analyzer [" + preselection + "]");

        p = SQLParser.getInstance();

        ToolBar toolBar = new ToolBar();

        btnGo = new Button();
        String s = Objects.requireNonNull(GUI.class.getResource("/icon24_run.png")).toExternalForm();
        ImageView iv = new ImageView(s);
        iv.smoothProperty().setValue(true);
        iv.preserveRatioProperty().setValue(true);
        iv.setFitHeight(20);
        iv.setFitWidth(20);
        btnGo.setGraphic(iv);
        btnGo.setTooltip(new Tooltip("click to execute your SELECT statement"));
        btnGo.setOnAction(event -> {
            p.parse(codeArea.getText(), dbBox.getSelectionModel().getSelectedItem(), primaryStage, resultview, statusline);
        });

        Button btnCopy = new Button();
        s = Objects.requireNonNull(GUI.class.getResource("/icon24_copy.png")).toExternalForm();
        iv = new ImageView(s);
        iv.smoothProperty().setValue(true);
        iv.preserveRatioProperty().setValue(true);
        iv.setFitHeight(20);
        iv.setFitWidth(20);
        btnCopy.setGraphic(iv);
        btnCopy.setTooltip(new Tooltip("copy result set to clipboard"));
        btnCopy.setOnAction(event -> {
            resultview.getSelectionModel().selectAll();
            StringBuilder sb = new StringBuilder();
            final javafx.scene.input.Clipboard clipboard = javafx.scene.input.Clipboard.getSystemClipboard();
            final ClipboardContent content = new ClipboardContent();
            ObservableList<TablePosition> selection = resultview.getSelectionModel().getSelectedCells();
            for (TablePosition pos : selection) {
                @SuppressWarnings("unchecked")
                ObservableList<String> hl = (ObservableList<String>) resultview.getItems().get(pos.getRow());
                sb.append(hl.toString()).append("\n");
            }
            content.putString(sb.toString());
            clipboard.setContent(content);
        });

        Button btnExit = new Button();
        s = Objects.requireNonNull(GUI.class.getResource("/icon24_back.png")).toExternalForm();
        iv = new ImageView(s);
        iv.smoothProperty().setValue(true);
        iv.preserveRatioProperty().setValue(true);
        iv.setFitHeight(20);
        iv.setFitWidth(20);

        btnExit.setGraphic(iv);
        btnExit.setTooltip(new Tooltip("Quit SQL Analyzer"));
        btnExit.setOnAction(new EventHandler<>() {
            @Override
            public void handle(ActionEvent event) {
                primaryStage.close();
            }
        });

        toolBar.getItems().addAll(btnGo, btnCopy, btnExit);

        dbBox.getItems().addAll(dbnames);

        Label dblabel = new Label("Choose database: ");
        if (null != preselection)
            dbBox.getSelectionModel().select(preselection);

        ToolBar dbbar = new ToolBar();

        Label statementlabel = new Label("Choose template: ");

        templateBox.getItems().addAll(templates.keySet());
        templateBox.getSelectionModel().selectFirst();

        templateBox.setOnAction(e -> {
            String selection = templateBox.getSelectionModel().getSelectedItem();
            if (selection != null && templates.containsKey(selection)) {
                codeArea.clear();
                codeArea.setText(templates.get(selection));
            }
        });

        dbbar.getItems().addAll(dblabel, dbBox, statementlabel, templateBox);

        // Configure the TextArea as a code editor
        codeArea.setWrapText(false);
        codeArea.setStyle("-fx-font-family: 'Monospaced'; -fx-font-size: 13;");

        if (initial_statement != null) {
            codeArea.clear();
            codeArea.setText(initial_statement);
        } else {
            String txt = """
                    -- Place your SELECT statement below this text.
                    -- Then click on the Play [>] button to execute.\
                    
                    SELECT * FROM TABLENAME;""";
            codeArea.setText(txt);
            int pos = txt.indexOf("TABLENAME");
            codeArea.selectRange(pos, pos + "TABLENAME".length());
        }

        // Auto-indent: insert previous line's indents on Enter
        codeArea.addEventHandler(KeyEvent.KEY_PRESSED, ke -> {
            if (ke.getCode() == KeyCode.ENTER) {
                int caretPos = codeArea.getCaretPosition();
                String text = codeArea.getText();
                // Find the start of the current line
                int lineStart = text.lastIndexOf('\n', caretPos - 1) + 1;
                String currentLine = text.substring(lineStart, caretPos);
                StringBuilder indent = new StringBuilder();
                for (char c : currentLine.toCharArray()) {
                    if (c == ' ' || c == '\t') indent.append(c);
                    else break;
                }
                if (!indent.isEmpty()) {
                    // Insert the indentation after the newline JavaFX will add
                    javafx.application.Platform.runLater(() -> {
                        int newCaret = codeArea.getCaretPosition();
                        codeArea.insertText(newCaret, indent.toString());
                    });
                }
            }
        });

        statusline = new Label();
        statusline.setText("<no rows selected>" + " | rows: " + 0);
        statusline.setStyle("-fx-text-fill: gray; -fx-max-width:200;");

        resultview = new TableView<>();
        resultview.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);

        createContextMenu(resultview);

        statusline.setMaxHeight(30);
        statusline.setMinHeight(30);

        VBox.setVgrow(root, Priority.ALWAYS);
        resultview.setPrefHeight(4000);
        codeArea.setMinHeight(100);
        root.getChildren().addAll(dbbar, codeArea, toolBar, resultview, statusline);

        Scene scene = new Scene(root,
                Screen.getPrimary().getVisualBounds().getWidth() * 0.8,
                Screen.getPrimary().getVisualBounds().getHeight() * 0.8);

        primaryStage.setScene(scene);
        ThemeManager.register(scene);
        scene.getStylesheets().add(Objects.requireNonNull(GUI.class.getResource("/sql-keywords.css")).toExternalForm());
        scene.getStylesheets().add(Objects.requireNonNull(GUI.class.getResource("/sql-editor-theme.css")).toExternalForm());

        applyCodeAreaTheme();
        app.addThemeListener(themeListener);
        primaryStage.setOnHidden(e -> app.removeThemeListener(themeListener));

        primaryStage.sizeToScene();
        codeArea.requestFocus();
        primaryStage.show();
    }

    /**
     * Applies the current theme (dark/light) to the TextArea by swapping CSS classes.
     * The colours are defined in sql-editor-theme.css via .code-area-dark / .code-area-light.
     */
    private void applyCodeAreaTheme() {
        boolean dark = app.isDarkTheme();
        codeArea.getStyleClass().removeIf(c -> c.equals("code-area-dark") || c.equals("code-area-light"));
        codeArea.getStyleClass().add(dark ? "code-area-dark" : "code-area-light");
    }

    ContextMenu createContextMenu(TableView<Object> table) {

        final ContextMenu contextMenu = new ContextMenu();

        table.setOnMouseClicked(event -> {
            if (event.getButton() == MouseButton.SECONDARY) {
                contextMenu.show(table.getScene().getWindow(), event.getScreenX(), event.getScreenY());
            }
        });

        MenuItem mntcopyline = new MenuItem("Copy Line(s)");
        String s = Objects.requireNonNull(GUI.class.getResource("/edit-copy.png")).toExternalForm();
        ImageView iv = new ImageView(s);

        final KeyCodeCombination copylineCombination = new KeyCodeCombination(KeyCode.L, KeyCombination.SHORTCUT_DOWN);
        final KeyCodeCombination copycellCombination = new KeyCodeCombination(KeyCode.C, KeyCombination.SHORTCUT_DOWN);

        table.setOnKeyPressed(event -> {
            if (!table.getSelectionModel().isEmpty()) {
                if (copylineCombination.match(event)) {
                    copyLineAction(table);
                    event.consume();
                } else if (copycellCombination.match(event)) {
                    copyCellAction(table);
                    event.consume();
                }
            }
        });

        MenuItem mntcopycell = new MenuItem("Copy Cell");
        s = Objects.requireNonNull(GUI.class.getResource("/edit-copy.png")).toExternalForm();
        iv = new ImageView(s);
        mntcopycell.setGraphic(iv);
        mntcopycell.setAccelerator(copycellCombination);
        mntcopycell.setOnAction(e -> {
            copyCellAction(table);
            e.consume();
        });

        mntcopyline.setAccelerator(copylineCombination);
        mntcopyline.setGraphic(iv);
        mntcopyline.setOnAction(e -> {
            copyLineAction(table);
            e.consume();
        });

        contextMenu.getItems().addAll(mntcopyline, mntcopycell);
        return contextMenu;
    }

    /**
     * Action handler method.
     *
     * @param table the TableView object where the action takes place.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void copyLineAction(TableView table) {
        StringBuilder sb = new StringBuilder();
        final javafx.scene.input.Clipboard clipboard = javafx.scene.input.Clipboard.getSystemClipboard();
        final ClipboardContent content = new ClipboardContent();
        ObservableList<TablePosition> selection = table.getSelectionModel().getSelectedCells();
        for (TablePosition pos : selection) {
            ObservableList<String> hl = (ObservableList<String>) table.getItems().get(pos.getRow());
            sb.append(hl.toString()).append("\n");
        }
        content.putString(sb.toString());
        clipboard.setContent(content);
    }

    /**
     * Action handler method.
     *
     * @param table the TableView object where the action takes place.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void copyCellAction(TableView table) {
        final javafx.scene.input.Clipboard clipboard = javafx.scene.input.Clipboard.getSystemClipboard();
        final ClipboardContent content = new ClipboardContent();
        ObservableList<TablePosition> selection = table.getSelectionModel().getSelectedCells();
        if (selection.isEmpty())
            return;
        TablePosition tp = selection.getFirst();
        int row = tp.getRow();
        int col = tp.getColumn();

        TableColumn tc = (TableColumn) table.getColumns().get(col);
        ObservableValue observableValue = tc.getCellObservableValue(row);

        String cellvalue = "";
        if (observableValue != null) {
            cellvalue = (String) observableValue.getValue();
        }
        content.putString(cellvalue);
        clipboard.setContent(content);
    }

    @SuppressWarnings("rawtypes")
    public void setOnClickOffset(TableView table) {
        table.setOnMouseClicked(new EventHandler<javafx.scene.input.MouseEvent>() {
            @SuppressWarnings({"unused", "unchecked"})
            @Override
            public void handle(javafx.scene.input.MouseEvent event) {
                if (event.getTarget().toString().startsWith("TableColumnHeader"))
                    return;

                int row;
                TablePosition pos;
                try {
                    pos = (TablePosition) table.getSelectionModel().getSelectedCells().getFirst();
                    row = pos.getRow();
                } catch (Exception err) {
                    return;
                }

                Object item = table.getItems().get(row);
                TableColumn col = pos.getTableColumn();
                if (col == null)
                    return;

                Object data = col.getCellObservableValue(item).getValue();

                TableColumn toff = (TableColumn) table.getColumns().get(1);
                ObservableValue off = toff.getCellObservableValue(row);
            }
        });
    }
}
