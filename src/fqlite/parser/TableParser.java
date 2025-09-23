package fqlite.parser;

import fqlite.base.Job;
import fqlite.descriptor.IndexDescriptor;
import fqlite.descriptor.TableDescriptor;


/**
 * This class is just a wrapper class.
 * For the actual parser functionality, see SimpleSQLiteParser.
 * The Backus-Naur form for this statement looks like this:
 * BNF: sql-command ::= CREATE [TEMP | TEMPORARY] TABLE component-name 
 *      				( column-def [, column-def]* [, constraint]* )
 *
 * @author pawlaszc
 *
 */
public class TableParser {

	
	
	/**
	 * Call this method to parse the SQL statement CREATE TABLE.
	 * The result will be a TableDescriptor object that contains the component name, column names
	 * a type. This information is necessary for matching the data records.
	 * An Example statement would look like this:
	 * CREATE TABLE 'users' (
	 *		'name' TEXT,
	 *		'surname' TEXT,
     *		'lastUpdate' TEXT
	 *	);
	 * 
	 * @param stmt the SQL statement to parse
	 * @return a TableDescriptor Object with all the information about the component. 
	 */
	public TableDescriptor parseCREATETABLEStatement(String stmt)
	{
		
		SimpleSQLiteParser parser = new SimpleSQLiteParser();
        return parser.parseTable(stmt);
	}
	
	/**
	 * /**
	 * Call this method to parse the SQL statement CREATE INDEX.
	 * The result will be an IndexDescriptor object that contains the component name, column names
	 * and types. This information is necessary for matching the data records.
	 
	 * @param stmt the SQL statement string
	 * @return an IndexDescriptor Object with all information about the component.
	 */
	public IndexDescriptor parseCREATEIndexStatement(Job job, String stmt)
	{
		
		SimpleSQLiteParser parser = new SimpleSQLiteParser();

        return parser.parseIndex(job, stmt);
	}
	
	
	
	/**
	 * Use this main() method only for testing purposes. 
	 * @param args the parameter list
	 */
	public static void main(String [] args)
	{
		/* test statement */
		String stmt = "CREATE TABLE 'users' (\n"
				+ "      'id' INTEGER,\n"
				+ "      'name' TEXT,\n"
				+ "      'surname' TEXT,\n"
				+ "      'zip' INTEGER,\n"
				+ "      CONSTRAINT constName PRIMARY KEY (id) UNIQUE(name));";
		
		TableParser p = new TableParser();
		
		p.parseCREATETABLEStatement(stmt);
	}
	
	
}
