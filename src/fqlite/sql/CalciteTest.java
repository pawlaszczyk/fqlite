package fqlite.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

public class CalciteTest {

	public static void main(String [] arg) throws Exception {
		
		// load the official calcite jdbc driver
		Class.forName("org.apache.calcite.jdbc.Driver");
	
	    Properties info = new Properties();
	    info.setProperty("lex", "JAVA");
	    
	    // establish connection the calcite framework
	    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
	
	    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
	    
	    // we need add a self defined schema to the root schema
	    SchemaPlus rootSchema = calciteConnection.getRootSchema();
	    
	    
	    List<String> columns = new ArrayList<String>();
	    columns.add("id");
	    columns.add("name");
	    columns.add("age");
	    
	    List<String> sqltypes = new ArrayList<String>();
	    sqltypes.add("BIGINT");
	    sqltypes.add("VARCHAR");
	    sqltypes.add("INTEGER");
	    
	    // this is our schema 
	    MemTableSchema schema = new MemTableSchema();
	    // register the newly defined schema to the root schema
	    // the database name is used as schema name
	    rootSchema.add("testdb", schema);
	
	    schema.createTable("employees", columns, sqltypes);
		
	    List<List<Object>> data = new ArrayList<List<Object>>();
	    for (int i = 0; i < 10000; i++){
	   
	    	List<Object> row = new ArrayList<Object>();
	    	
	    	if(i % 2 == 0) {
		    	row.add(i);
	    		row.add("john");
		    	row.add(30);
		    }
	    	else{
	    		row.add(i);
	    		row.add("cole");
		    	row.add(50);
	    	}
	    	
	    	data.add(row);
	    }
	   
	    /* fill table with data */
	    schema.fill("employees",data);
	    
	    
	    schema.createTable("manager", columns, sqltypes);
		
	    List<List<Object>> mdata = new ArrayList<List<Object>>();
	    for (int i = 0; i < 10; i++){
	   
	    	List<Object> row = new ArrayList<Object>();
	    	
	    	if(i % 2 == 0) {
		    	row.add(i);
	    		row.add("lisa");
		    	row.add(30);
		    }
	    	else{
	    		row.add(i);
	    		row.add("cole");
		    	row.add(50);
	    	}
	    	
	    	mdata.add(row);
	    }
	   
	    /* fill table with data */
	    schema.fill("manager",mdata);
	   
	    long start = System.currentTimeMillis();
	    
	    /* get a statement object */
	    Statement statement = calciteConnection.createStatement();
	    /* execute statement and get result set */
	    //ResultSet rs = statement.executeQuery("select * from testdb.employees as e where e.name = 'cole'");
	    //ResultSet rs = statement.executeQuery("select * from testdb.employees ");
	    ResultSet rs = statement.executeQuery("SELECT * FROM testdb.manager as e INNER JOIN testdb.employees as m ON e.name = m.name");
	    
	    long end = System.currentTimeMillis();
	    
	    System.out.println("duration in ms " + (end-start));
	    
	    int size = 0;
	    while (rs.next()) {
	    	
        //    long id = rs.getLong("id");
              String name = rs.getString("name");
        //    int age = rs.getInt("age");
        //    System.out.println("id: " + id + "; name: " + name + "; age: " + age);
              System.out.println(name);
	    size++;
	    }

	    System.out.println(" result size" + size);
	    
        rs.close();
        statement.close();
        connection.close();
	}
}
