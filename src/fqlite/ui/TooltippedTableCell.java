package fqlite.ui;

import java.util.Iterator;
import fqlite.base.Job;
import fqlite.descriptor.IndexDescriptor;
import fqlite.descriptor.TableDescriptor;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.collections.ObservableList;
import javafx.scene.control.Cell;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.stage.Stage;
import javafx.util.Callback;
import javafx.util.StringConverter;
import javafx.util.converter.DefaultStringConverter;

/**
 * Just like a normal table cell, but each table cell has a tooltip that will display its contents. This makes
 * it easier for the user: they can read the contents without having to expand the table cell.
 * <p>
 * Look it's easy:
 * <code>
 * someColumn.setCellFactory(TooltippedTableCell.forTableColumn());
 * </code>
 */
public class TooltippedTableCell<S, T> extends TableCell<S, T> {
  
	private String tablename = null;
	private Job job = null;
	private Stage s = null;
		
	public static <S> Callback<TableColumn<S, String>, TableCell<S, String>> forTableColumn(String tablename, Job job, Stage s) {
        return forTableColumn(new DefaultStringConverter(), tablename, job, s);
    }

    public static <S, T> Callback<TableColumn<S, T>, TableCell<S, T>> forTableColumn(final StringConverter<T> converter, String tablename, Job job, Stage s) {
        return list -> new TooltippedTableCell<>(converter,tablename, job, s);
    }
	
	public static <S> Callback<TableColumn<S, String>, TableCell<S, String>> forTableColumn() {
        return forTableColumn(new DefaultStringConverter());
    }

    public static <S, T> Callback<TableColumn<S, T>, TableCell<S, T>> forTableColumn(final StringConverter<T> converter) {
        return list -> new TooltippedTableCell<>(converter);
    }

    private static <T> String getItemText(Cell<T> cell, StringConverter<T> converter) {
        return converter == null ? cell.getItem() == null ? "" : cell.getItem()
                .toString() : converter.toString(cell.getItem());
    }

    public void setTablename(String name){
    	this.tablename = name;
    }
    
    public void setJob(Job job) {
    	this.job = job;
    }
    
	CustomTooltip ctt = new CustomTooltip("");
    String coltype = null;
    Long hash = -1L;
    
  
    @SuppressWarnings("unchecked")
	private void updateItem(final Cell<T> cell, final StringConverter<T> converter) {

    	
//    	if (super.getIndex() % 2 == 0)
//	    	   this.setStyle("-fx-font-style: italic; -fx-alignment: TOP-RIGHT; -fx-background-color: honeydew;");
//	       else
//	    	   this.setStyle("-fx-font-style: italic; -fx-alignment: TOP-RIGHT; -fx-background-color: lavender;");
//  

 	   this.setStyle(this.getStyle() + "-fx-font-style: italic;");

    	
    	if(null != s && !s.isFocused())
    		return;
    		
        if (cell.isEmpty()) {
           cell.setText(null);
           return;
        } 
        else{
            
        	/* determine column data type (INT,BLOB,VARCHAR...) */
        	if(null == coltype) {
	           	
	        	Iterator<TableDescriptor> tbls = job.headers.iterator();
	        	while(tbls.hasNext()){
	        		TableDescriptor td = tbls.next();
	        		
	        		if(td.tblname.equals(tablename)) {
		        		coltype = td.getSqlTypeForColumn(this.getTableColumn().getText());
		            	
	        		}	
	        	}
        	}
        	
        	/* not a table ? -> check the index table list */
        	if(coltype == null){
        		
        		Iterator<IndexDescriptor> idxs = job.indices.iterator();
        	    
        		while(idxs.hasNext())
        		{
        			IndexDescriptor id = idxs.next();
        		    if(id.idxname.equals(tablename)){      
        		    	coltype = id.getSqlTypeForColumn(this.getTableColumn().getText());
                    	break;
        		    }
        		}
        		
        		
        	}
        	
        	String s = getItemText(cell, converter);
        	        	
        	/* for each BLOB column we need to know the offset of the data record */
        	if(s.contains("BLOB-")) {
        	
        		int row = -1;
        		// we need the row number
        		try {
        		
        			javafx.scene.control.TableRow<S> tr = this.getTableRow();
        		
        			if (tr == null)
        				return;
        		
        			row = tr.getIndex();
            	
        		}
        		catch(Exception err){
        		   // There is a bug actually under windows -> no idea why	
        		   return;
        		}
        		
        		ObservableList<String> hl = (ObservableList<String>)this.getTableView().getItems().get(row);
        		
        		if (hl.get(5)== null || hl.get(5).trim().equals(""))
        			return;
        		hash = Long.parseLong(hl.get(5));
	
        	}
        	
        	
        	
        	if(true) {
        		synchronized(this) {
        	
		        
        			ctt.addCellText(tablename,hash,job,coltype,s,this,cell,converter);
        			/* there is one tooltip object for the complete table */
        			cell.setTooltip(ctt);
        			cell.setText(s);  
        		}
		        return;
        	}
        	
        }
    }

    private ObjectProperty<StringConverter<T>> converter = new SimpleObjectProperty<>(this, "converter");

    /**
     * The easiest way to get this working is to call this class's static forTableColumn() method:
     * <code>
     * someColumn.setCellFactory(TooltippedTableCell.forTableColumn());
     * </code>
     */
    public TooltippedTableCell() {
        this(null);
    }
    
  
    public TooltippedTableCell(StringConverter<T> converter) {
        this.getStyleClass().add("tooltipped-table-cell");
        setConverter(converter);
    }

    public TooltippedTableCell(StringConverter<T> converter,String tablename, Job job, Stage s) {
        this.getStyleClass().add("tooltipped-table-cell");
        setConverter(converter);
        setTablename(tablename);
        this.s = s;
        setJob(job);
    }

    
    
    public final ObjectProperty<StringConverter<T>> converterProperty() {
        return converter;
    }

    public final void setConverter(StringConverter<T> value) {
        converterProperty().set(value);
    }

    public final StringConverter<T> getConverter() {
        return converterProperty().get();
    }


    @Override
    public void updateItem(T item, boolean empty) {
    	
    	if(null != s && !s.isFocused())
    		return;
    	
    	super.updateItem(item, empty);
        updateItem(this, getConverter());
    }
    
    
    
}