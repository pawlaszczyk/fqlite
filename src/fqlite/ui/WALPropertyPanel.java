package fqlite.ui;

import javax.swing.JLabel;

import fqlite.base.GUI;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableColumn.CellDataFeatures;
import javafx.scene.control.TableRow;
import javafx.scene.control.TableView;
import javafx.scene.layout.Priority;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.util.Callback;

public class WALPropertyPanel extends StackPane {

	public JLabel ldbpath;
	public JLabel lpagesize;
	public JLabel lencoding;
	public JLabel ltotalsize;
	public JLabel lpagesizeout;
	public JLabel lencodingout;
	public JLabel ltotalsizeout;

	private FileInfo info;
	GUI gui;

    TabPane tabpane = new TabPane();
    VBox container = new VBox();
    

	public WALPropertyPanel(FileInfo info, GUI gui) {
		this.info = info;
		this.gui = gui;
		container.setPrefHeight(4000);
		container.getChildren().add(tabpane);
		container.getChildren().add(new Label("WAL archive"));
		VBox.setVgrow(tabpane,Priority.ALWAYS);
		this.getChildren().add(container);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
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
					TableColumn col = new javafx.scene.control.TableColumn(colname);
					col.setCellValueFactory(new Callback<CellDataFeatures<ObservableList,String>,ObservableValue<String>>(){                    
		            public ObservableValue<String> call(CellDataFeatures<ObservableList, String> param) {                                                                                              
		                return new SimpleStringProperty(param.getValue().get(j).toString());                        
		            }                    
					});
					
					if(i == 1) 
				        col.prefWidthProperty().bind(table.widthProperty().multiply(0.3));
					
					if(i == 2)
				        col.prefWidthProperty().bind(table.widthProperty().multiply(0.5));

					
					table.getColumns().add(col);
			}
			
			fillTable(table,data);
			
			
	        StackPane fields = new StackPane();
	        fields.getChildren().add(table);
			Tab headerfieldstab = new Tab("Write Ahead Log Header",fields);
	        tabpane.getTabs().add(headerfieldstab);
		
	}
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void fillTable(TableView table, String[][] data)
	{
		// define array list for all table rows 
	    ObservableList<ObservableList> obdata = FXCollections.observableArrayList();
		
	    // iterate over row array to create a data row 
	 	for(int i = 1; i < data.length; i++)
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
	
	static int cl = 0;

	String[] bgcolors = new String[]{"-fx-background-color: orange;","-fx-background-color: yellow;","-fx-background-color: lightblue;","-fx-background-color: green;"};
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void initCheckpointTable(String[][] data) {
		

		  String column[]={"salt1", "salt2", "framenumber", "pagenumber", "commit"};         
			
			TableView table = new TableView<>();
			
			for (int i = 0; i < column.length; i++) {
		            String colname = column[i];
		            final int j = i;                
					TableColumn col = new javafx.scene.control.TableColumn(colname);
					col.setCellValueFactory(new Callback<CellDataFeatures<ObservableList,String>,ObservableValue<String>>(){                    
		            public ObservableValue<String> call(CellDataFeatures<ObservableList, String> param) {                                                                                              
		                return new SimpleStringProperty(param.getValue().get(j).toString());                        
		            }                    
					});
					
					table.setRowFactory(tv -> new TableRow<ObservableList<String>>() {
					    @Override
					    public void updateItem(ObservableList<String> item, boolean empty) {
					        super.updateItem(item, empty) ;
					        if (item == null) {
					            setStyle("");
					        } 
					        else{
					        
					        	String salt1 = item.get(0);
								if (!gui.getRowcolors().containsKey(salt1))
								{
									gui.getRowcolors().put(salt1, bgcolors[cl%bgcolors.length]);
									cl++;
								}				
					        	
					        	setStyle(gui.getRowcolors().get(salt1));
					        } 
					      
					    }
					});
					
			
					table.getColumns().add(col);
			}
			
			
			fillTable(table,data);		
	        StackPane fields = new StackPane();
	        fields.getChildren().add(table);
			Tab checkpointtab = new Tab("Checkpoints",fields);
	        tabpane.getTabs().add(checkpointtab);
	}
	

}
