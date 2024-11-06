package fqlite.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javafx.collections.ObservableList;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MemTableSchema extends AbstractSchema{

	
	Map<String, Table> tables = new HashMap<>();
	
	private final ObjectMapper mapper = new ObjectMapper();
   
    /**
     * Creates a new MemoryTable with the given name, columns and SQL types.
     * @param tablename
     * @param columnnames
     * @param sqltypes
     * @return
     */
    public MemoryTable createTable(String tablename, List<String> columnnames, List<String> sqltypes){
    	
    	MemoryTable mt = new MemoryTable(tablename,columnnames,sqltypes);
    	tables.put(tablename, mt);
    	return mt;
    }
    
    /**
     * Fill a table with a given table name with data.
     *  
     * @param tablename
     * @param data
     */
    public void fill(String tablename, ObservableList<ObservableList<String>> data){
    	
    	Map<Object, ObjectNode> tmap = ((MemoryTable)tables.get(tablename)).getData();
    	// get meta information from the table
    	List<String> fieldnames = ((MemoryTable)tables.get(tablename)).getFieldNames();
    	
    	Iterator<ObservableList<String>> rows = data.iterator();
    
    	// internal access key for the Map
    	int rowid = 0;
    	while(rows.hasNext()){
    		List<?> values = rows.next();
    		ObjectNode row = mapper.createObjectNode();
    	  
    		//System.out.println(" Fieldnames number " + fieldnames.size());
    		for (int i = 0; i < fieldnames.size(); i++){
    	    	// put column name as key and cell value as for each element in a row 
    	        if (values.size() < i)
    	        {
    	        	row.put(fieldnames.get(i),"");
    	        }   	    	
    	        if (values.get(i) == null) {
    	    		row.put(fieldnames.get(i),"");
    	    	}
    	    	else {
    	    		//System.out.println("cell:: " + i +  " " + values.get(i));      
    	    		row.put(fieldnames.get(i),values.get(i).toString());
    	    	}
    		
    		}
    	    //System.out.println(" added row " + rowid);
    	    // save complete row to table map with the "rowid" as key
    	    //System.out.println("ROW:: " + row);
    		tmap.put(rowid, row);	
    	    rowid++;
    	}
    }
   
    public void fill(String tablename, List<List<Object>> data){
    	
    	Map<Object, ObjectNode> tmap = ((MemoryTable)tables.get(tablename)).getData();
    	// get meta information from the table
    	List<String> fieldnames = ((MemoryTable)tables.get(tablename)).getFieldNames();
    	
    	Iterator<List<Object>> rows = data.iterator();
    
    	// internal access key for the Map
    	int rowid = 0;
    	while(rows.hasNext()){
    		List<Object> values = rows.next();
    		ObjectNode row = mapper.createObjectNode();
    	    for (int i = 0; i < fieldnames.size(); i++){
    	    	// put column name as key and cell value as for each element in a row 
    	        
    	    	row.put(fieldnames.get(i),values.get(i).toString());
    	    }
    	    // System.out.println(" added row " + rowid);
    	    // save complete row to table map with the "rowid" as key
    	    tmap.put(rowid, row);	
    	    rowid++;
    	}
    }
    
  

    @Override
    protected Map<String,Table> getTableMap() {
    
    	return tables;
    	//return Collections.singletonMap(tablename, new MemoryTable(table));
    	 
    }


   
    
}