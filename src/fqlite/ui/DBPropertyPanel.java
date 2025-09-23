package fqlite.ui;

import java.util.*;

import fqlite.base.GUI;
import fqlite.log.AppLog;
import fqlite.types.FileTypes;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.Label;
import javafx.scene.control.MenuItem;
import javafx.scene.control.SelectionMode;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TablePosition;
import javafx.scene.control.TableColumn.CellDataFeatures;
import javafx.scene.control.TableView;
import javafx.scene.image.ImageView;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.scene.input.KeyEvent;
import javafx.scene.input.MouseButton;
import javafx.scene.layout.Priority;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import javafx.util.Callback;

@SuppressWarnings("rawtypes")
public class DBPropertyPanel extends StackPane{

    private final FileInfo info;
    public String  columnStr = "";
    private final GUI gui;
    
    TabPane tabpane = new TabPane();
    
	public DBPropertyPanel(GUI gui, FileInfo info,String fname)
	{
		this.info = info;
		this.gui = gui;
		VBox base = new VBox();
		
		String s = Objects.requireNonNull(GUI.class.getResource("/gray_schema32.png")).toExternalForm();
		Button btnSchema = new Button("Show Schema Info");
		ImageView iv = new ImageView(s);
		btnSchema.setGraphic(iv);
		btnSchema.setOnAction(e->showColumnInfo());
		//this.columnBtn.setToolTipText("Show Schema Information with Standard Webbrowser");        
		StackPane head = new StackPane();
		head.getChildren().add(btnSchema);
		base.getChildren().addAll(head,tabpane,new Label(fname));
	    tabpane.setPrefHeight(4000);
		VBox.setVgrow(tabpane,Priority.ALWAYS);
		this.getChildren().add(base);
	}
	
	
	
	
	@SuppressWarnings("unchecked")
	public void initHeaderTable(String[][] data)
	{
		
	    javafx.scene.control.TextArea headerinfo = new javafx.scene.control.TextArea();
	    headerinfo.setEditable(false);
	    headerinfo.setStyle("-fx-font-alignment: center");
	    headerinfo.setText(info.toString());
	    StackPane sp =  new StackPane();
	    sp.getChildren().add(headerinfo);
		Tab headerinfotab = new Tab("File Info",sp);
        tabpane.getTabs().add(headerinfotab);
	
        
        String[] column ={"Offset", "Property", "Value"};
		
		TableView table = new TableView<>();
		table.getSelectionModel().setSelectionMode(
			    SelectionMode.MULTIPLE
		);
	
			createContextMenu(table);
		
		for (int i = 0; i < column.length; i++) {
	            String colname = column[i];
	            final int j = i;                
				TableColumn col = new TableColumn(colname);
				col.setCellValueFactory(new Callback<CellDataFeatures<ObservableList,String>,ObservableValue<String>>(){                    
	            public ObservableValue<String> call(CellDataFeatures<ObservableList, String> param) {                                                                                              
	                return new SimpleStringProperty(param.getValue().get(j).toString());                        
	            }                    
				});
				
				switch(i)
				{
				
					case 1:   col.prefWidthProperty().bind(table.widthProperty().multiply(0.4));
				              break;
					case 2:   col.prefWidthProperty().bind(table.widthProperty().multiply(0.5));
							  break;
				    	
				}
				
				/* first column is an integer column */
				if (i == 0) {
					col.setComparator(new Comparator<Object>() {
			            @Override
			            public int compare(Object o1, Object o2) {
			            
			            	long i1 = Long.parseLong((String)o1);
			                long i2 = Long.parseLong((String)o2);
			                return (i1 < i2) ? -1: +1 ;
			            }
			        });
				}
				
				
				table.getColumns().add(col);
		}
		
		fillTable(table,data,false);
		
	
        StackPane fields = new StackPane();
        fields.getChildren().add(table);
		Tab headerfieldstab = new Tab("Header [PRAGMAS]",fields);
        tabpane.getTabs().add(headerfieldstab);

	}
	
	public void initColumnTypesTable(LinkedHashMap<String,String[][]> ht)
	{
	
        String str = "<!DOCTYPE html>" + "<html>"
        		+ " <head> "
        		+ "<title>" + info.filename +" - Schema Information</title>"
        		+ " <style type=\"text/css\">\n"
        		+ "  table, td, tr { border:1px solid black;}\n"
        		+ " </style>"
        		
        	    + "<script>"
                + "function scrollTo(elementId) {"
                + "document.getElementById(elementId).scrollIntoView();"
                + "}"
                + "</script>"
            
                + " </head>"
                + " <body>"                
                + "<h1>Schema Information for database: " + info.filename + "</h1>";	
        
        
        str += "<a id=\"top\"></a>";
        str += "<br />";
        str += "<hr>";
        str += "<h2> TABLES </h2>";
        str += "<hr>";
        
        
        
        boolean firstindex = true;
        
        for (Map.Entry<String, String[][]> entry : ht.entrySet()) {
            String tname = entry.getKey();
         
            if (tname.startsWith("__"))
        		continue;
            if (firstindex && tname.startsWith("idx:"))
            {
                str += "<br />";
                str += "<hr>";
                str += "<h2> INDICES </h2>";
                str += "<hr>";
            	firstindex=false;
            }
            
            str += "<a href='#"+tname+"' onclick=scrollTo('" + tname + "')>"+tname+"</a><br />";
            
        }
        	
        
        str += "<br />";
        str += "<hr>";
        str += "<br />";

        
        for (Map.Entry<String, String[][]> entry : ht.entrySet()) {
            String tname = entry.getKey();
           
       
        	if (tname.startsWith("__"))
        		continue;
        	
        	str += "<a id=\""+tname+"\"></a>";
        	str += "<p>";
        	String bgcolor = "#00008b";
        	
        	if(tname.startsWith("idx:"))
        	{
        		bgcolor = "#DF0101";
        	}
      
        	
        	String [][] tab = entry.getValue();
    	    int cols =	tab[0].length;
         	
        	str	+= "<table rules=groups>";
        	str	+= "<thead bgcolor="+bgcolor+" style=\"color:white;white-space:nowrap;width:100%;\" bordercolor=#000099>";
        	str	+= "<tr>";
        	if(tname.startsWith("idx:"))
        	{
        		str	+= "<th>INDEX </th><th>"+ "\""+ tname + "\"</th>"; 
            }
        	else
        	{
        		str	+= "<th>TABLE	 </th><th>"+ "\""+ tname + "\"</th>";    
            }	
        	for (int i=0; i < cols-1;i++)
            	str	+= "<th></th>";
        	str += "</thead>";
        	str	+= "</tr>";
        	str += "<tbody>";
        	
	       
       
        	
        	for (int i = 0; i < tab.length; i++) {
	            
	        	 str += "<tr>";
	             
        	 	 switch(i)	
            	 { 
            		case 0 :  
            			str+="<td> <b> column name </b> </td>";
            			break;
            		case 1 :  
            			str+="<td> <b> serialtype </b> </td>";
                		break;
            		case 2 :  
            			str+="<td> <b> sqltype </b> </td>";
                		break;
            		case 3 :
            			str+="<td> <b> column constraints </b> </td>";
                		break;                		
            		case 4 : 	
            			str+="<td> <b> table constraint </b> </td>";
            			break;	
                	default :
                		str+="<td>  </td>";
                		
            	 }
        	 
	        	 
	             for(int j=0; j <tab[i].length; j++)
	             {
	            	 str+="<td>" + tab[i][j] + "</td>";
	             }
	             
	             str+="</tr>";
	            
	        }
	        str += "</tbody>";
	        str += "</table>";
            //str += "<a href=\"#top\">[TOP]</a><br/>";
            str += "<a href='#' onclick=scrollTo('top')>[TOP]</a><br/>";
	        str += "</p>";

        }
        
         str += "</body></html>";
        
         columnStr = str;
	}
	
	public void showColumnInfo()
	{
 	    
	    Stage secondStage = new Stage();
        Scene scene = new Scene(new SchemaBrowser(columnStr),750,500, javafx.scene.paint.Color.web("#666970"));			
	    System.out.println(columnStr);
        secondStage.setTitle("Schema Info");
        secondStage.setScene(scene);
        secondStage.show();
	}
	
	@SuppressWarnings("unchecked")
	public void initPagesTable(String[][] data)
	{

		String[] column ={"page", "offset", "type of page", "table", "signature"};
		
		TableView table = new TableView<>();
		table.getSelectionModel().setSelectionMode(
			    SelectionMode.MULTIPLE
		);
		createContextMenu(table);
		
		for (int i = 0; i < column.length; i++) {
	           
				String colname = column[i];
	            final int j = i;                
				TableColumn col = new TableColumn(colname);
				col.setCellValueFactory(new Callback<CellDataFeatures<ObservableList,String>,ObservableValue<String>>(){                    
	            public ObservableValue<String> call(CellDataFeatures<ObservableList, String> param) {                                                                                              
	                return new SimpleStringProperty(param.getValue().get(j)!=null? param.getValue().get(j).toString() :"");                        
	            }                    
				});
				
				/* first 2 columns are integer/long columns */
				if (i == 0 || i == 1) {
					col.setComparator(new Comparator<Object>() {
			            @Override
			            public int compare(Object o1, Object o2) {
			            
			            	long i1 = Long.parseLong((String)o1);
			                long i2 = Long.parseLong((String)o2);
			                return (i1 < i2) ? -1 : +1 ;
			            }
			        });
				}
				
               // col.setSortable(false);
					
				
				switch(i)
				{
					case 0:   col.prefWidthProperty().bind(table.widthProperty().multiply(0.08));
							  break;	
					case 2:   col.prefWidthProperty().bind(table.widthProperty().multiply(0.20));
				              break;
					case 3:   col.prefWidthProperty().bind(table.widthProperty().multiply(0.25));
							  break;
				    case 4:   col.prefWidthProperty().bind(table.widthProperty().multiply(0.30));
							  break;
				}
				
				
				table.getColumns().add(col);
		}
		
		if (null != data)
			fillTable(table,data,true);
		
		// add handler for offset selection -> this will automatically open the hex-viewer
				setOnClickOffset(table);
				
			
				
		Tab pagetab = new Tab("Pages", table);
		
		tabpane.getTabs().add(pagetab);
			

	}
	
	
	@SuppressWarnings("unchecked")
	public void initSchemaTable(String[][] data)
	{
		
		
		String[] column ={"No.", "Type", "Tablename", "Root", "SQL-Statement", "Virtual", "ROWID"};
		
		TableView table = new TableView<>();
		table.getSelectionModel().setSelectionMode(
			    SelectionMode.MULTIPLE
		);
		createContextMenu(table);

		for (int i = 0; i < column.length; i++) {
	           
				String colname = column[i];
	            final int j = i;                
				TableColumn col = new TableColumn(colname);
				col.setCellValueFactory(new Callback<CellDataFeatures<ObservableList,String>,ObservableValue<String>>(){                    
	            public ObservableValue<String> call(CellDataFeatures<ObservableList, String> param) {                                                                                              
	                return new SimpleStringProperty(param.getValue().get(j)!=null? param.getValue().get(j).toString() :"");                        
	            }                    
				});
				
				/* first column is an integer column */
				if (i == 0 || i == 3) {
					col.setComparator(new Comparator<Object>() {
			            @Override
			            public int compare(Object o1, Object o2) {
			            
			            	long i1 = Long.parseLong((String)o1);
			                long i2 = Long.parseLong((String)o2);
			                return (i1 < i2) ? -1: +1 ;
			            }
			        });
				}
				
								
				
				switch(i)
				{
					case 0:   col.prefWidthProperty().bind(table.widthProperty().multiply(0.05));
							  break;	
					case 2:   col.prefWidthProperty().bind(table.widthProperty().multiply(0.15));
				              break;
					case 4:   col.prefWidthProperty().bind(table.widthProperty().multiply(0.5));
							  break;
				    default:  col.prefWidthProperty().bind(table.widthProperty().multiply(0.08));    	
				    	
				}
				
				
				table.getColumns().add(col);
		}
		
		if (null != data)
			fillTable(table,data,true);
		
				
		Tab schematab = new Tab("SQL-Schema", table);
		
		tabpane.getTabs().add(schematab);
			

	}
	
	@SuppressWarnings("unchecked")
	private void fillTable(TableView table, String[][] data, boolean rowno)
	{
		// define an array list for all table rows
	    ObservableList<ObservableList> obdata = FXCollections.observableArrayList();
		
	    int rownumber = 1;
	    
	    // iterate over row array to create a data row 
	 	for(int i = 0; i < data.length; i++)
	 	{
	 		    if(data[i][0]==null)
	 		    	continue;
	 		    String [] s = data[i];
	 			ObservableList<Object> row = FXCollections.observableArrayList();
                if (rowno)
                	row.add(rownumber);
	 		    for(Object o : s) {
	 		    	row.add(o);
	 		    }
                obdata.add(row);
                rownumber++;
	 	}
	 		
	    // finally update TableView with data set
	 	Platform.runLater(()->{
	 		table.setItems(obdata);
	 	});
	 	
	 	
		
	}
	
	
private ContextMenu createContextMenu(TableView<String> table){
		
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
		String s = Objects.requireNonNull(GUI.class.getResource("/edit-copy.png")).toExternalForm();
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
	    s = Objects.requireNonNull(GUI.class.getResource("/edit-copy.png")).toExternalForm();
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
	@SuppressWarnings("unchecked")
	private void copyLineAction(TableView table){
		
		StringBuilder sb = new StringBuilder();
	 	final javafx.scene.input.Clipboard clipboard = javafx.scene.input.Clipboard.getSystemClipboard();
	    final ClipboardContent content = new ClipboardContent();
		ObservableList<TablePosition> selection = table.getSelectionModel().getSelectedCells();

        for (TablePosition pos: selection) {

            ObservableList<String> hl = (ObservableList<String>) table.getItems().get(pos.getRow());
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
	@SuppressWarnings("unchecked")
	private void copyCellAction(TableView table){

 	final javafx.scene.input.Clipboard clipboard = javafx.scene.input.Clipboard.getSystemClipboard();
    final ClipboardContent content = new ClipboardContent();
             
    ObservableList<TablePosition> selection = table.getSelectionModel().getSelectedCells();
    if (selection.isEmpty())
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
	
	public void setOnClickOffset(TableView table){
		
		table.setOnMouseClicked(new EventHandler<javafx.scene.input.MouseEvent>() {
		
			@SuppressWarnings("unchecked")
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
				      
				    System.out.println("Bin drin " + off);
					   
					   if (col.getText().equals("offset"))
					   {
						   if(row >= 0)
						   {   
							   // get currently selected database
							   NodeObject no = gui.getSelectedNode();
							   						   
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

								   GUI.HEXVIEW.go2(model, position);
								   
							   }catch(Exception err) {
                                   AppLog.error(err.getMessage());
							   }
							   
						       
						   	}
					   }

				 
				   	
			
				}
		
		});
	}
	
	
}
