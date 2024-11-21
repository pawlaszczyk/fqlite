package fqlite.export;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.List;

import fqlite.base.GUI;
import javafx.collections.ObservableList;

public class DBExporter {
	private List<String> dbnames;
	private Hashtable<String, ObservableList<ObservableList<String>>> tabledata;
	private Connection connection = null;
	Statement statement = null;

	public DBExporter(String dbname) {

		try 
		{
				// create a database connection with the given name, i.e. jdbc:sqlite:sample.db
				connection = DriverManager.getConnection("jdbc:sqlite:" + dbname);
				statement = connection.createStatement();
				statement.setQueryTimeout(30); // set timeout to 30 sec.

		} catch (SQLException e) {
			// if the error message is "out of memory",
			// it probably means no database file is found
			e.printStackTrace(System.err);
		}

	}

	/**
	 * 
	 * @param tblname
	 * @param colnames
	 * @param sqltypes
	 */
	public void createTable(String tblname, List<String> colnames, List<String> sqltypes) {

		StringBuffer buffer = new StringBuffer();
		buffer.append(" (");
		
		for (int i = 0; i < colnames.size(); i++){
			
			buffer.append(" " + colnames.get(i) + " " + sqltypes.get(i));
			if (i < colnames.size()-1)
				buffer.append(", ");
		}
		buffer.append(")");
		
		try {
			// CREATE TABLE" + tblname + "(id integer, name string)
			statement.executeUpdate("CREATE TABLE " + tblname + buffer.toString());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	

	public static void main(String[] args) {
		// NOTE: Connection and Statement are AutoCloseable.
		// Don't forget to close them both in order to avoid leaks.
		try (
				// create a database connection
				Connection connection = DriverManager.getConnection("jdbc:sqlite:sample.db");
				Statement statement = connection.createStatement();) {
			statement.setQueryTimeout(30); // set timeout to 30 sec.

			statement.executeUpdate("drop table if exists person");
			statement.executeUpdate("create table person (id integer, name string)");
			statement.executeUpdate("insert into person values(1, 'leo')");
			statement.executeUpdate("insert into person values(2, 'yui')");
			ResultSet rs = statement.executeQuery("select * from person");
			while (rs.next()) {
				// read the result set
				System.out.println("name = " + rs.getString("name"));
				System.out.println("id = " + rs.getInt("id"));
			}
		} catch (SQLException e) {
			// if the error message is "out of memory",
			// it probably means no database file is found
			e.printStackTrace(System.err);
		}
	}
}