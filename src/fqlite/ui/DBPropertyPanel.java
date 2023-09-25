package fqlite.ui;

import java.util.LinkedHashMap;
import java.util.Map;

import fqlite.base.GUI;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableColumn.CellDataFeatures;
import javafx.scene.control.TableView;
import javafx.scene.image.ImageView;
import javafx.scene.layout.Priority;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import javafx.util.Callback;

public class DBPropertyPanel extends StackPane{

	
	public Label ldbpath;
    public Label lpagesize;
    public Label lencoding;
    public Label ltotalsize;   
    public Label lpagesizeout;
    public Label lencodingout;
    public Label ltotalsizeout;
    private FileInfo info;
    public Button columnBtn;
    public String  columnStr = "";
    
    
    TabPane tabpane = new TabPane();
    
	public DBPropertyPanel(FileInfo info,String fname)
	{
		this.info = info;
		VBox base = new VBox();
		
		String s = GUI.class.getResource("/find.png").toExternalForm();
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
	
	
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
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
	
        
        String column[]={"Offset","Property","Value"};         
		
		TableView table = new TableView<>();
		
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
				
				
				table.getColumns().add(col);
		}
		
		fillTable(table,data);
		
		
        StackPane fields = new StackPane();
        fields.getChildren().add(table);
		Tab headerfieldstab = new Tab("Header Fields",fields);
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
        	str += "<a href=\"#"+tname+"\">"+tname+"</a><br />";
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
            str += "<a href=\"#top\">[TOP]</a><br/>";
	        str += "</p>";

        }
        
         str += "</body></html>";
        
         columnStr = str;
         
         //System.out.println(columnStr);
	}
	
	public void showColumnInfo()
	{
 	    
	    Stage secondStage = new Stage();
        Scene scene = new Scene(new SchemaBrowser(columnStr),750,500, javafx.scene.paint.Color.web("#666970"));			
	    secondStage.setTitle("Schema Info");
        secondStage.setScene(scene);
        secondStage.show();
	}
	
	
	
	
	public void initSchemaTable(String[][] data)
	{
		
		
		String column[]={"Type","Tablename","Root","SQL-Statement","Virtual","ROWID"};         
		
		TableView table = new TableView<>();
		
		for (int i = 0; i < column.length; i++) {
	           
				String colname = column[i];
	            final int j = i;                
				TableColumn col = new TableColumn(colname);
				col.setCellValueFactory(new Callback<CellDataFeatures<ObservableList,String>,ObservableValue<String>>(){                    
	            public ObservableValue<String> call(CellDataFeatures<ObservableList, String> param) {                                                                                              
	                return new SimpleStringProperty(param.getValue().get(j)!=null? param.getValue().get(j).toString() :"");                        
	            }                    
				});
				
				switch(i)
				{
				
					case 1:   col.prefWidthProperty().bind(table.widthProperty().multiply(0.15));
				              break;
					case 3:   col.prefWidthProperty().bind(table.widthProperty().multiply(0.5));
							  break;
				    default:  col.prefWidthProperty().bind(table.widthProperty().multiply(0.08));    	
				    	
				}
				
				
				table.getColumns().add(col);
		}
		
		if (null != data)
			fillTable(table,data);
		
				
		Tab schematab = new Tab("SQL-Schema", table);
		
		tabpane.getTabs().add(schematab);
			

	}
	
	private void fillTable(TableView table, String[][] data)
	{
		// define array list for all table rows 
	    ObservableList<ObservableList> obdata = FXCollections.observableArrayList();
		
	    // iterate over row array to create a data row 
	 	for(int i = 0; i < data.length; i++)
	 	{
	 			String [] s = data[i];
	 			ObservableList<String> row = FXCollections.observableArrayList();
                row.addAll(s);
                obdata.add(row);
	 	}
	 		
	    // finally update TableView with data set
	 	Platform.runLater(()->{
	 		table.setItems(obdata);
	 	});
	 	
	 	
		
	}
	
	
	
}
