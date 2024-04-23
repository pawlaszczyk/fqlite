package fqlite.ui;


import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TableColumn.CellDataFeatures;
import javafx.scene.layout.StackPane;
import javafx.util.Callback;

public class RollbackPropertyPanel extends StackPane{

	
   private FileInfo info;

    
    
    TabPane tabpane = new TabPane();
    

    
	public RollbackPropertyPanel(FileInfo info)
	{
		this.info = info;
		this.getChildren().add(tabpane);

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
					
					table.getColumns().add(col);
			}
			
			fillTable(table,data);
			
			
	        StackPane fields = new StackPane();
	        fields.getChildren().add(table);
			Tab headerfieldstab = new Tab("Rollback Journal Header",fields);
	        tabpane.getTabs().add(headerfieldstab);
		
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void fillTable(TableView table, String[][] data)
	{
		// define array list for all table rows 
	    ObservableList<ObservableList> obdata = FXCollections.observableArrayList();
		
	    // iterate over row array to create a data row 
	 	for(int i = 1; i < data.length; i++)
	 	{
	 			String [] s = data[i];
	 			ObservableList<String> row = FXCollections.observableArrayList();
                if (i == 1 && s[2].equals("0")) {
                	s[2] = " 0 -> zero padded header (transaction committed)";
                }
	 			row.addAll(s);
                obdata.add(row);
	 	}
	 		
	    // finally update TableView with data set
	 	Platform.runLater(()->{
	 		table.setItems(obdata);
	 	});
	 	
	 	
		
	}
	
}
