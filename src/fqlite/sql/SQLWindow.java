package fqlite.sql;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.fxmisc.richtext.CodeArea;
import org.fxmisc.richtext.GenericStyledArea;
import org.fxmisc.richtext.LineNumberFactory;
import org.fxmisc.richtext.model.Paragraph;
import org.fxmisc.richtext.model.StyleSpans;
import org.fxmisc.richtext.model.StyleSpansBuilder;
import org.reactfx.Subscription;
import org.reactfx.collection.ListModification;

import fqlite.base.GUI;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.beans.value.ObservableValue;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.Label;
import javafx.scene.control.MenuItem;
import javafx.scene.control.SelectionMode;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TablePosition;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.ToolBar;
import javafx.scene.control.Tooltip;
import javafx.scene.image.ImageView;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.scene.input.KeyEvent;
import javafx.scene.input.MouseButton;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

/**
 *  This class implements a simple user interface for the
 *  SQL analyzer component. 
 *  
 *  @author Dirk Pawlaszczyk
 */

public class SQLWindow extends Application {
    
    public TextArea resultTA = new TextArea(); 
    Hashtable<String,ObservableList<ObservableList<String>>> tabledata;
    TableView<Object> resultview = new TableView<Object>();
    List<String> dbnames; 
    final ComboBox<String> dbBox = new ComboBox<>();
    final ComboBox<String> templateBox = new ComboBox<>();
    final GUI app;
    VBox root = new VBox();
    org.fxmisc.richtext.CodeArea codeArea = new CodeArea();
    public Label statusline;
    
    private static final String[] KEYWORDS = new String[] {
            "ADD","ADD CONSTRAINT","ALL","ALTER","ALTER COLUMN","ALTER TABLE",
            "AND","ANY","AS","ASC","BACK DATABASE","BETWEEN","CASE","CHECK","COLUMN",
            "CONSTRAINT","CREATE","CREATE DATABASE","CREATE INDEX", "CREATE OR REPLACE VIEW",
            "CREATE TABLE", "CREATE PROCEDURE", "CREATE UNIQUE INDEX", "CREATE VIEW","DATABASE",
            "DEFAULT","DELETE","DESC","DISTINCT","DROP","EXEC","EXITS","FOREIGN KEY","FROM",
            "FULL OUTER JOIN","GROUP BY","HAVING","IN","INDEX","INNER JOIN","INSERT INTO","IS NULL",
            "IS NOT NULL","JOIN","LEFT JOIN","LIKE","LIMIT","NOT","NOT NULL","ON","OR","ORDER BY","OUTER JOIN",
            "PRIMARY KEY","PROCEDURE","RIGHT JOIN","ROWNUM","SELECT","SELECT DISTINCT","SELECT INTO","SELECT TOP",
            "SET","TABLE","TOP","TRUNCATE TABLE","UNION","UNION ALL","UNIQUE","UPDATE","VALUES","VIEW","WHERE"
    };

    private static final String KEYWORD_PATTERN = "\\b(" + String.join("|", KEYWORDS) + ")\\b";
    private static final String PAREN_PATTERN = "\\(|\\)";
    private static final String BRACE_PATTERN = "\\{|\\}";
    private static final String BRACKET_PATTERN = "\\[|\\]";
    private static final String SEMICOLON_PATTERN = "\\;";
    private static final String STRING_PATTERN = "\"([^\"\\\\]|\\\\.)*\"";
    private static final String COMMENT_PATTERN = "//[^\n]*" + "|" + "/\\*(.|\\R)*?\\*/"   // for whole text processing (text blocks)
    		                          + "|" + "/\\*[^\\v]*" + "|" + "^\\h*\\*([^\\v]*|/)";  // for visible paragraph processing (line by line)

    private static final Pattern PATTERN = Pattern.compile(
            "(?<KEYWORD>" + KEYWORD_PATTERN + ")"
            + "|(?<PAREN>" + PAREN_PATTERN + ")"
            + "|(?<BRACE>" + BRACE_PATTERN + ")"
            + "|(?<BRACKET>" + BRACKET_PATTERN + ")"
            + "|(?<SEMICOLON>" + SEMICOLON_PATTERN + ")"
            + "|(?<STRING>" + STRING_PATTERN + ")"
            + "|(?<COMMENT>" + COMMENT_PATTERN + ")"
    );
    
    static final String sampleCode = String.join("\n", new String[] {
            " SELLECT * FROM ITEMS WHERE A = 'B'; "
        });

    static final Hashtable<String,String> templates = new Hashtable<String,String>();

    
    /**
     * Constructor of the SQL Analyzer window.
     * @param app
     */
    public SQLWindow(GUI app){

    	this.app = app;
    	tabledata = app.datasets;
    	this.dbnames = app.dbnames;
   
    	Enumeration<String> keys = tabledata.keys();
        while(keys.hasMoreElements()) {
        	String key = keys.nextElement();
        	System.out.println("datasets key :: " + key);
        
        	ObservableList<ObservableList<String>> obl =  tabledata.get(key);
        	System.out.println(" first line >> " + obl.get(0));
        }
        
        

        templates.put("SIMPLE SELECT","-- Place your SELECT statement below this text.\n-- Then click on the Play [>] button to execute. \nSELECT * FROM TABLENAME WHERE <condition>;");
        templates.put("INNER JOIN", "-- Returns records that have matching values in both tables \nSELECT * FROM <TABLE1> AS t1 INNER JOIN <TABLE2> AS t2 ON t1.colX = t2.colY;");
        templates.put("LEFT (OUTER) JOIN", "-- Returns all records from the left table, and the matched records from the right table \n SELECT t1.colX, t2.colY FROM <TABLE1> AS t1 LEFT JOIN <TABLE2> AS t2 ON t1.colX = t2.colY\n"
        		+ " ORDER BY e.colname1; ");
        templates.put("RIGHT (OUTER) JOIN", "-- Returns all records from the right table, and the matched records from the left table \n "
        		+ "SELECT t1.colX, t2.colY " 
        		+ "FROM <TABLE1> AS t1 " + "RIGHT JOIN <TABLE2> AS t2 ON t1.colX = t2.colX; ");
        templates.put("FULL (OUTER) JOIN", " -- Returns all records when there is a match in either left or right table \n"
        		+ " SELECT t1.colX, t2.colY " + "FROM <table1> AS t1 FULL OUTER JOIN <table2> AS t2 ON t1.colX = t2.colY "
        		+ "WHERE <condition>;");

      
    }
    
	
	public static void main(String[] args) {
        launch(args);
    }
    
    @SuppressWarnings("unused")
	@Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("SQL Analyzer (Beta)");
		
        ToolBar toolBar = new ToolBar();

        Button btnGo = new Button();
        String s = GUI.class.getResource("/start.png").toExternalForm();
		ImageView iv = new ImageView(s);
		btnGo.setGraphic(iv);
		btnGo.setTooltip(new Tooltip("click her to execute your SELECT statement"));
		btnGo.setOnAction(new EventHandler<ActionEvent>() {
 
            @Override
            public void handle(ActionEvent event) {
            	SQLParser p = new SQLParser();
            	// open connection to calcite 
            	p.connectToInMemoryDatabase();
            	String result = p.parse(codeArea.getText(),dbBox.getSelectionModel().getSelectedItem(),app,primaryStage,resultview,statusline);
            	Platform.runLater( () -> {resultTA.setText(result);});
            }
        });
        

        Button btnCopy = new Button();
        s = GUI.class.getResource("/edit-copy_small.png").toExternalForm();
		iv = new ImageView(s);
		btnCopy.setGraphic(iv);
		btnCopy.setTooltip(new Tooltip("copy result set to clipboard"));
		btnCopy.setOnAction(new EventHandler<ActionEvent>() {
 
            @Override
        	@SuppressWarnings("rawtypes")
            public void handle(ActionEvent event) {
               resultview.getSelectionModel().selectAll();
            	StringBuffer sb = new StringBuffer();			
             	final javafx.scene.input.Clipboard clipboard = javafx.scene.input.Clipboard.getSystemClipboard();
                final ClipboardContent content = new ClipboardContent();
                ObservableList<TablePosition> selection = resultview.getSelectionModel().getSelectedCells();	        
                Iterator<TablePosition> iter = selection.iterator();
                
                while(iter.hasNext()) {
                	
                	TablePosition pos = iter.next();	        	
                	@SuppressWarnings("unchecked")
					ObservableList<String> hl = (ObservableList<String>)resultview.getItems().get(pos.getRow());
                	  sb.append(hl.toString() + "\n");
                }
                content.putString(sb.toString());
                clipboard.setContent(content);

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
        		
		
		
        
        toolBar.getItems().addAll(btnGo, btnCopy,btnExit);

        dbBox.getItems().addAll(dbnames);
        
        Label dblabel = new Label("Choose database: ");
        dbBox.getSelectionModel().selectFirst();
        
        ToolBar dbbar = new ToolBar();
        
        Label statementlabel = new Label("Choose template: ");
        
        templateBox.getItems().addAll(templates.keySet());
        templateBox.getSelectionModel().selectFirst();
        
        templateBox.setOnAction(e -> {

        	String selection = templateBox.getSelectionModel().getSelectedItem();
        	if (selection != null && templates.containsKey(selection))
        		codeArea.clear();
        		codeArea.replaceText(0, 0,templates.get(selection));

        });
        

        dbbar.getItems().addAll(dblabel,dbBox,statementlabel,templateBox);
        
      
        codeArea.setParagraphGraphicFactory(LineNumberFactory.get(codeArea));

        
        Subscription cleanupWhenNoLongerNeedIt = codeArea.multiPlainChanges().successionEnds(Duration.ofMillis(500)).subscribe(ignore -> codeArea.setStyleSpans(0, computeHighlighting(codeArea.getText())));

        // when no longer need syntax highlighting and wish to clean up memory leaks
        // run: `cleanupWhenNoLongerNeedIt.unsubscribe();`
        
        // recompute syntax highlighting only for visible paragraph changes
        // Note that this shows how it can be done but is not recommended for production where multi-
        // line syntax requirements are needed, like comment blocks without a leading * on each line. 
        codeArea.getVisibleParagraphs().addModificationObserver
        (
            new VisibleParagraphStyler<>( codeArea, this::computeHighlighting)
        );
        String txt =  "-- Place your SELECT statement below this text.\n-- Then click on the Play [>] button to execute. "
        //		+ "\nSELECT * FROM TABLE1 AS t1 INNER JOIN TABLE2 AS t2 ON t1.mname = t2.sname;\n";
        //		+ ""
        		+ "\nSELECT * FROM TABLENAME;";
        codeArea.replaceText(0, 0,txt);
        int pos = txt.indexOf("TABLENAME");
        if (pos >=1)
        {	codeArea.selectRange(1, pos);
        	codeArea.selectWord();
        }
        
        statusline = new Label();
	    statusline.setText("<no rows selected>" + " | rows: " + 0);
        
               
        // auto-indent: insert previous line's indents on enter
        final Pattern whiteSpace = Pattern.compile( "^\\s+" );
        codeArea.addEventHandler( KeyEvent.KEY_PRESSED, KE ->
        {
            if ( KE.getCode() == KeyCode.ENTER ) {
            	int caretPosition = codeArea.getCaretPosition();
            	int currentParagraph = codeArea.getCurrentParagraph();
                Matcher m0 = whiteSpace.matcher( codeArea.getParagraph( currentParagraph-1 ).getSegments().get( 0 ) );
                if ( m0.find() ) Platform.runLater( () -> codeArea.insertText( caretPosition, m0.group() ) );              
            }
        });
        
        
    	
        resultview = new TableView<Object>();
  	    
		resultview.getSelectionModel().setSelectionMode(
			    SelectionMode.MULTIPLE
		);
		
		
		createContextMenu(resultview);

		statusline.setMaxHeight(30);
		
		root.getChildren().addAll(dbbar,codeArea,toolBar,resultview,statusline);
		
		
		Scene scene = new Scene(root); //,Screen.getPrimary().getVisualBounds().getWidth()*0.8,Screen.getPrimary().getVisualBounds().getHeight()*0.5);
        
		
        primaryStage.setScene(scene);
        scene.getStylesheets().add(GUI.class.getResource("/sql-keywords.css").toExternalForm());
        primaryStage.sizeToScene();
        codeArea.requestFocus();
        primaryStage.show();
        primaryStage.setAlwaysOnTop(true);
    }


    private StyleSpans<Collection<String>> computeHighlighting(String text) {
    	
        Matcher matcher = PATTERN.matcher(text);
        int lastKwEnd = 0;
        StyleSpansBuilder<Collection<String>> spansBuilder
                = new StyleSpansBuilder<>();
        System.out.println("inside computeHighlithting() " + text);
        while(matcher.find()) {
        	String styleClass =
                    matcher.group("KEYWORD") != null ? "keyword" :
                    matcher.group("PAREN") != null ? "paren" :
                    matcher.group("BRACE") != null ? "brace" :
                    matcher.group("BRACKET") != null ? "bracket" :
                    matcher.group("SEMICOLON") != null ? "semicolon" :
                    matcher.group("STRING") != null ? "string" :
                    matcher.group("COMMENT") != null ? "comment" :
                    null; /* never happens */ assert styleClass != null;
            spansBuilder.add(Collections.emptyList(), matcher.start() - lastKwEnd);
            spansBuilder.add(Collections.singleton(styleClass), matcher.end() - matcher.start());
            lastKwEnd = matcher.end();
        }
        spansBuilder.add(Collections.emptyList(), text.length() - lastKwEnd);
        return spansBuilder.create();
    }
    
    ContextMenu createContextMenu(TableView<Object> table){
    	
    	final ContextMenu contextMenu = new ContextMenu();

    	
    	
    	
    	table.setOnMouseClicked(new EventHandler<javafx.scene.input.MouseEvent>() {
    		
    		@Override
    		public void handle(javafx.scene.input.MouseEvent event) {

    			if(event.getButton() == MouseButton.SECONDARY) {
    				  
    				   //ContextMenu tcm = createContextMenu(CtxTypes.TABLE,table); 
    				   contextMenu.show(table.getScene().getWindow(), event.getScreenX(), event.getScreenY());
    			   }
    			
    		}	
    	});
    	
    	// copy a single table line
    	MenuItem mntcopyline = new MenuItem("Copy Line(s)");
    	String s = GUI.class.getResource("/edit-copy.png").toExternalForm();
        ImageView iv = new ImageView(s); 

    	
     	final KeyCodeCombination copylineCombination = new KeyCodeCombination(KeyCode.L, KeyCombination.SHORTCUT_DOWN);
    	final KeyCodeCombination copycellCombination = new KeyCodeCombination(KeyCode.C, KeyCombination.SHORTCUT_DOWN);

        
    	table.setOnKeyPressed(new EventHandler<KeyEvent>(){
    	
    		
    	    @Override
    	    public void handle(KeyEvent event) {
    		    if (!table.getSelectionModel().isEmpty())
    		    {   	
    		   	 
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
     
    	
    	
    	// copy the complete table line (with all cells)
    	MenuItem mntcopycell= new MenuItem("Copy Cell");
        s = GUI.class.getResource("/edit-copy.png").toExternalForm();
    	iv = new ImageView(s);
    	mntcopycell.setGraphic(iv);
        mntcopycell.setAccelerator(copycellCombination);
    	mntcopycell.setOnAction(e ->{
    		copyCellAction(table);
    		e.consume();
    	}
    	);
    	
    	
    	mntcopyline.setAccelerator(copylineCombination);
    	    mntcopyline.setGraphic(iv);
    		mntcopyline.setOnAction(e ->{
    			copyLineAction(table);     		
    			e.consume();
    	}
    	);

    	
    	contextMenu.getItems().addAll(mntcopyline,mntcopycell);
        return contextMenu;
    }



    /**
     * Action handler method.   
     * @param table
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void copyLineAction(TableView table){
    	
    	StringBuffer sb = new StringBuffer();			
     	final javafx.scene.input.Clipboard clipboard = javafx.scene.input.Clipboard.getSystemClipboard();
        final ClipboardContent content = new ClipboardContent();
        ObservableList<TablePosition> selection = table.getSelectionModel().getSelectedCells();	        
        Iterator<TablePosition> iter = selection.iterator();
        
        while(iter.hasNext()) {
        	
        	TablePosition pos = iter.next();	        	
        	ObservableList<String> hl = (ObservableList<String>)table.getItems().get(pos.getRow());
        	  sb.append(hl.toString() + "\n");
        }
        System.out.println("Write value to clipboard " + sb.toString());
        content.putString(sb.toString());
        clipboard.setContent(content);

    }

    /**
     * Action handler method.   
     * @param table
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void copyCellAction(TableView table){

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
    }

    content.putString(cellvalue);
    clipboard.setContent(content);


    }

    @SuppressWarnings("rawtypes")
	public void setOnClickOffset(TableView table){
    	
    	table.setOnMouseClicked(new EventHandler<javafx.scene.input.MouseEvent>() {
    	
    		@SuppressWarnings({"unused", "unchecked" })
    		@Override
    		   public void handle(javafx.scene.input.MouseEvent event) {
    			
    			
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
    			   
    			
    			   	TableColumn col = pos.getTableColumn();

    			   	if(col == null)
    				   	return;
    			   
    			   	// this gives the value in the selected cell:
    			   	Object data = col.getCellObservableValue(item).getValue();
    			   	
    			    // get the relative virtual address (offset) from the table
    			    TableColumn toff = (TableColumn) table.getColumns().get(1);
    			       
    				// get the actual value of the currently selected cell
    			    ObservableValue off =  toff.getCellObservableValue(row); 	
    		
    			}
    	
    	});
    }
    
   
}

class VisibleParagraphStyler<PS, SEG, S> implements Consumer<ListModification<? extends Paragraph<PS, SEG, S>>>
{
    private final GenericStyledArea<PS, SEG, S> area;
    private final Function<String,StyleSpans<S>> computeStyles;
    private int prevParagraph, prevTextLength;

    public VisibleParagraphStyler( GenericStyledArea<PS, SEG, S> area, Function<String,StyleSpans<S>> computeStyles )
    {
        this.computeStyles = computeStyles;
        this.area = area;
    }

    @Override
    public void accept( ListModification<? extends Paragraph<PS, SEG, S>> lm )
    {
        if ( lm.getAddedSize() > 0 ) Platform.runLater( () ->
        {
            int paragraph = Math.min( area.firstVisibleParToAllParIndex() + lm.getFrom(), area.getParagraphs().size()-1 );
            String text = area.getText( paragraph, 0, paragraph, area.getParagraphLength( paragraph ) );

            if ( paragraph != prevParagraph || text.length() != prevTextLength )
            {
                if ( paragraph < area.getParagraphs().size()-1 )
                {
                    int startPos = area.getAbsolutePosition( paragraph, 0 );
                    area.setStyleSpans( startPos, computeStyles.apply( text ) );
                }
                prevTextLength = text.length();
                prevParagraph = paragraph;
            }
        });
    }


    

}


