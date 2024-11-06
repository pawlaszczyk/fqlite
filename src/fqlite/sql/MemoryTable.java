package fqlite.sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MemoryTable extends AbstractTable implements ScannableTable {

    private final Map<Object, ObjectNode> data;
    private final List<String> fieldNames;
    private final List<SqlTypeName> fieldTypes;
    private String tblname;

    public MemoryTable(String tblname, List<String> columnnames, List<String> sqltypes) {
    	
    	this.tblname = tblname;
    	this.data = new HashMap<Object, ObjectNode>();
    	this.fieldNames = columnnames;
    	
    	ListIterator<String> sql = sqltypes.listIterator();
    	
    	fieldTypes = new ArrayList<SqlTypeName>(sqltypes.size());
     	while(sql.hasNext()) {
     		sql.next();
     		fieldTypes.add(SqlTypeName.VARCHAR);
    	}    	
    }
  
    
    public MemoryTable(Map<Object, ObjectNode> data, List<String> columnnames, List<String> sqltypes) {
    	
    	this.data = data;
    	this.fieldNames = columnnames;
    	
     	ListIterator<String> sql = sqltypes.listIterator();
    	fieldTypes = new ArrayList<SqlTypeName>(sqltypes.size());
     	while(sql.hasNext()) {
     		fieldTypes.add(SqlTypeName.VARCHAR);
    	}    	
    }
    
    public String getName(){
    	return tblname;
    }
    
    public List<String> getFieldNames(){
    	return fieldNames;
    }
    
    
    public Map<Object, ObjectNode> getData(){
    	
    	return data;
    }
    
    
    public MemoryTable(Map<Object, ObjectNode> data) {

        this.data = data;

        List<String> names = new ArrayList<>();
        names.add("id");
        names.add("name");
        names.add("age");
        this.fieldNames = names;

        List<SqlTypeName> types = new ArrayList<>();
        types.add(SqlTypeName.BIGINT);
        types.add(SqlTypeName.VARCHAR);
        types.add(SqlTypeName.INTEGER);
        this.fieldTypes = types;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {

        // https://github.com/apache/calcite/blob/fa8349069d141d3c75bafa06d5fb8800711ec8d6/example/csv/src/main/java/org/apache/calcite/adapter/csv/CsvEnumerator.java#L111
        List<RelDataType> types = fieldTypes.stream().map(typeFactory::createSqlType).collect(Collectors.toList());
        System.out.println("$$$$ types" + types.toString());
        System.out.println("$$$$ size" + types.size());
        System.out.println("$$$$ fieldNames" + fieldNames.toString());
        System.out.println("$$$$ size" + fieldNames.size());
           
        return typeFactory.createStructType(types, fieldNames);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        Stream<Object[]> dataStream = data.entrySet().stream().map(this::toObjectArray);
        return Linq4j.asEnumerable(new StreamIterable<>(dataStream));
    }

    private Object[] toObjectArray(Map.Entry<Object, ObjectNode> item) {

        int no = fieldNames.size();
    	//System.out.println("number " + no);
        
        Object[] res = new Object[no];
        res[0] = item.getKey();

        for (int i = 0; i < no; i++) {
            JsonNode v = item.getValue().get(fieldNames.get(i));
      //    SqlTypeName type = fieldTypes.get(i);
            
            
            if (v!= null && v.textValue() != null)
    			res[i] = v.textValue();
    		else
    			res[i] = "test";
    		
            
      //      switch (type) {
      //      	case BIGINT:
      //      		res[i] = v.longValue();
      //      		break;
      //      	case VARCHAR:
      //      		if (v.textValue() != null)
      //      			res[i] = v.textValue();
      //      		else
      //      			res[i] = "";
      //      		break;
      //          case INTEGER:
      //              res[i] = v.intValue();
      //              break;
      //          default:
      //              throw new RuntimeException("unsupported sql type: " + type);
       //     }
        }
      //  System.out.println(">>" + Arrays.toString(res));
        
        return res;
    }
}