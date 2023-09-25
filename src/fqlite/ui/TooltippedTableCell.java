package fqlite.ui;

import java.util.Iterator;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Base64InputStream;

import fqlite.base.GUI;
import fqlite.base.Job;
import fqlite.descriptor.TableDescriptor;
import fqlite.util.Auxiliary;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.collections.ObservableList;
import javafx.scene.control.Cell;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.Tooltip;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
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
		
	public static <S> Callback<TableColumn<S, String>, TableCell<S, String>> forTableColumn(String tablename, Job job) {
        return forTableColumn(new DefaultStringConverter(), tablename, job);
    }

    public static <S, T> Callback<TableColumn<S, T>, TableCell<S, T>> forTableColumn(final StringConverter<T> converter, String tablename, Job job) {
        return list -> new TooltippedTableCell<>(converter,tablename, job);
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
    
    private void updateItem(final Cell<T> cell, final StringConverter<T> converter) {

    	
        if (cell.isEmpty()) {
            cell.setText(null);
            cell.setTooltip(null);
        } 
        else {
                	
        	//System.out.println(" tooltip>>>" + this.getTableColumn().getText());
        	
        	if(this.getTableColumn().getText().equals("")){
        		Tooltip tooltip = new Tooltip("state (deleted, updated ...) \n empty .. regular dataset");
	            //tooltip.prefWidthProperty().bind(cell.widthProperty());
	            cell.setTooltip(tooltip);	 
	        	String s = getItemText(cell, converter);
	        	cell.setText(s);     
	            return;
        	}
        	
        	if(this.getTableColumn().getText().equals("Offset")){
        		Tooltip tooltip = new Tooltip("byte position");
	            //tooltip.prefWidthProperty().bind(cell.widthProperty());
	            cell.setTooltip(tooltip);	 
	        	String s = getItemText(cell, converter);
	        	cell.setText(s);     
	            return;
        	}
        	
        	if(this.getTableColumn().getText().trim().equals("PLL")){
        		Tooltip tooltip = new Tooltip("payload length");
	            //tooltip.prefWidthProperty().bind(cell.widthProperty());
	            cell.setTooltip(tooltip);	 
	        	String s = getItemText(cell, converter);
	        	cell.setText(s);     
	            return;
        	}
        	
        	if(this.getTableColumn().getText().trim().equals("HL")){
        		Tooltip tooltip = new Tooltip("header length");
	            //tooltip.prefWidthProperty().bind(cell.widthProperty());
	            cell.setTooltip(tooltip);	 
	        	String s = getItemText(cell, converter);
	        	cell.setText(s);     
	            return;
        	}
        	
        	String s = getItemText(cell, converter);
        	cell.setText(s);
        	
        	
        	String tttype = null;
        	/* determine column type for tooltip info panel */
        	
        	Iterator<TableDescriptor> tbls = job.headers.iterator();
        	while(tbls.hasNext()){
        		TableDescriptor td = tbls.next();
        		
        		if(td.tblname.equals(tablename)) {
	        		tttype = td.getToolTypeForColumn(this.getTableColumn().getText());
	            	
        		}	
        	}
        	
        	/* if no table description could be found -> lookup index table */
//        	if(tttype == null){
//        		
//        		Iterator<IndexDescriptor> idxs = job.indices.iterator();
//        		while(idxs.hasNext()){
//        			
//        			IndexDescriptor id = idxs.next();
//        			
//        			tttype = id.getToolTypeForColumn(this.getTableColumn().getText());
//        			
//        		}
//        		
//        		
//        	}
        	
        	if(tttype == null){
        		
        		tttype = "";
        	}
        	
        	
        	if(s.contains("[BLOB")){
        		
        		
        		// we need the column name 
        		// the table name 
        		//System.out.println("Column name >>>>" + this.getTableColumn().getText());
        		int row = this.getTableRow().getIndex();
        		//System.out.println("row " + row);
             	ObservableList<String> hl = (ObservableList<String>)this.getTableView().getItems().get(row);
        	    //System.out.println("Original Size in Byte >>>" + hl.get(2));
        	    //System.out.println("Tablename :" + tablename);
        	    //System.out.println("Job : " + job.path);
        	    
        	    //System.out.println("Get Thumbnail from hashset for keyn" + hl.get(5));
        	    //Long hash = Long.parseLong(hl.get(5)) + this.getTableColumn().getText().hashCode();
        	    int from = s.indexOf("BLOB-");
        	    int to = s.indexOf("]");
        	    String number = s.substring(from+5, to);
        	    //System.out.println("BLOB >>>>" + s);
        	    
        	    int id = Integer.parseInt(number);
        	    Long hash = Long.parseLong(hl.get(5)) + id;
        	    
        	    Image ii = job.Thumbnails.get(hash);
        	    
        	    //if (null != ii && (s.contains("<bmp>") || s.contains("<jpg>") || s.contains("<png>") || s.contains("<gif>")))
        	    if(null != ii)
        	    {
        	    	
        	    	//System.out.println("JPEG|PNG|GIF Gefunden!");
        	    	//Add text as tooltip so that user can read text without editing it.
    	            Tooltip tooltip = new Tooltip();
        	 
      	    		ImageView iv = new ImageView(ii);
      	          	tooltip.setGraphic(iv);   
    	            cell.setTooltip(tooltip);
        	    }
        	    else
        	    {
        	    	String text = Auxiliary.hex2ASCII(getItemText(cell, converter));   
        	    	
        	    	
            	    //Add text as tooltip so that user can read text without editing it.
    	            Tooltip tooltip = new Tooltip(text);
    	            tooltip.setWrapText(true);
    	            tooltip.prefWidthProperty().bind(cell.widthProperty());
    	            if(null!=tttype)
    	            	cell.setTooltip(tooltip);
    	            
    	            s = GUI.class.getResource("/hex-32.png").toExternalForm();
    	    		ImageView iv = new ImageView(s);
    	    		tooltip.setGraphic(iv);   
        	    
        	    
        	    }
        	    	
        	 
        	}
        	else{
        		Tooltip tooltip = null;
        		
        		
        		
        		String bb = (String)cell.getItem();
        		//System.out.println(">>>" + bb + " " + job.timestamps.size());
        		if(job.timestamps.containsKey(bb)){
        			Object value = job.timestamps.get(bb);        			
        			tooltip = new Tooltip(">>>[" + tttype + "] " +  value);
        		    return;
        		}
        		
        		//@SuppressWarnings("deprecation")
    			//boolean isBase64 = Base64.isBase64(s);

                //if(isBase64 && s.length() > 2 ){
                //    try {
                  //  	byte[] decodedBytes = java.util.Base64.getDecoder().decode(s);
                  //  	String decodedString = new String(decodedBytes);
                   
	              //  	if (tttype == null || tttype == "")
	            	//		tooltip = new Tooltip(s);
	            	//	else
	            	//		tooltip = new Tooltip("[" + tttype + "] " + s);
	    	        //    tooltip.setWrapText(true);
	    	        //    tooltip.prefWidthProperty().bind(cell.widthProperty());
	    	        //    cell.setTooltip(tooltip);	            
	                	
	             //   	return;
                 //   }catch(Exception err){
                    	
                 //   }
                //}
                	
        		
        		//Add text as tooltip so that user can read text without editing it.
        		if (tttype == null || tttype == "")
        			tooltip = new Tooltip(getItemText(cell, converter));
        		else
        			tooltip = new Tooltip("[" + tttype + "] " + getItemText(cell, converter));
	            tooltip.setWrapText(true);
	            tooltip.prefWidthProperty().bind(cell.widthProperty());
	            cell.setTooltip(tooltip);	            
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

    public TooltippedTableCell(StringConverter<T> converter,String tablename, Job job) {
        this.getStyleClass().add("tooltipped-table-cell");
        setConverter(converter);
        setTablename(tablename);
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
        super.updateItem(item, empty);
        updateItem(this, getConverter());
    }
}