package fqlite.base;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class ViewReader {

    private Connection conn;
	
	/**
     * Connect to a sample database
	 * @throws ClassNotFoundException 
     */
    public void connect() throws ClassNotFoundException {
       
        try {
            // db parameters
        	// create a database connection
        	
            String url = "jdbc:sqlite:/Users/pawlaszc/test.db"; 
            // create a connection to the database
            conn = DriverManager.getConnection(url);
            
            System.out.println("Connection to SQLite has been established.");
            
            createStatement();
            
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }
    
    public void createStatement()
    {
    	Statement statement;
		
    	try {
			statement = conn.createStatement();
	        statement.setQueryTimeout(30); // set timeout to 30 sec.	
	       // String sql = "SELECT * FROM custom_emoji_for_threads";
	        String sql = " SELECT name FROM sqlite_master WHERE type =\'view\' AND name NOT LIKE \'sqlite_%\'";
	        ResultSet rs = statement.executeQuery(sql);
	        
	        List<String> names = new LinkedList<String>();
	        
	        while(rs.next()){
	        	String vn = rs.getString(1);
	        	names.add(vn);
	        }
	        rs.close();
	        
	        Iterator<String> view_names = names.iterator();
	        while (view_names.hasNext())
	        {
	        	String name = view_names.next();
	        	System.out.println(name);
	        	String sql2 = "SELECT * FROM " + name + ";";
	        	ResultSet rs2 = statement.executeQuery(sql2);
	        	
	        	ResultSetMetaData rsMetaData = rs2.getMetaData();
	            System.out.println("List of column names in the current table: ");
	             //Retrieving the list of column names
	             int count = rsMetaData.getColumnCount();
	             List<String> coltypes = new LinkedList<String>();
	             List<String> colnames = new LinkedList<String>();
	             
	             for(int i = 1; i<=count; i++) {
	                System.out.println("column " + rsMetaData.getColumnName(i) + " type " + rsMetaData.getColumnTypeName(i));
	                //String cn = rsMetaData.getColumnName(i);
	                colnames.add(name);
	                String ct = rsMetaData.getColumnTypeName(i);
	                coltypes.add(ct);
	            
	             }
	             //ViewDescriptor vd = new ViewDescriptor(name,coltypes,colnames);
	             
	             
	        	
	        	while(rs2.next())
	        	{	
	        		 for(int i = 1; i<=count; i++) {
	        			 System.out.println(" " + rs2.getRow());
	        			 System.out.print(rs2.getString(i));
	        		 }
	        	}
	        	rs2.close();
	        }
	     
	        
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    	
    }
    
    public static void main(String [] args)
    {
    	try {
			ViewReader v = new ViewReader();
			v.connect();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	
}
