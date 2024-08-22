package fqlite.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import fqlite.base.Job;
import fqlite.descriptor.IndexDescriptor;
import fqlite.descriptor.TableDescriptor;
import fqlite.log.AppLog;
import fqlite.pattern.HeaderPattern;
import fqlite.pattern.IntegerConstraint;


/**
 * With the help of the present class CREATE TABLE and CREATE INDEX statements can be parsed and decomposed. 
 * 
 * Note: ANTLR (ANother Tool for Language Recognition) is a powerful parser generator for 
 * reading, processing, executing, or translating structured text or binary files. 
 * 
 * This class builds up on the parser classes of ANTLR-library.
 * 
 * 
 * @author pawlaszc
 * 
 * @version 1.0
 *
 */

@SuppressWarnings("deprecation")
public class SimpleSQLiteParser {

	/* determine data type */

	/* INT */
	static String[] inttypes = { "INT", "INTEGER", "INTUNSIGNED", "INTSIGNED", "LONG", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT",
			"UNSIGNEDBIGINT", "INT2", "INT8"};
	/* TEXT */
	static String[] texttypes = { "TEXT", "CHARACTER", "CLOB", "VARCHAR", "VARYINGCHARACTER", "NCHAR",
			"NATIVE CHARACTER", "NVARCHAR" };
	/* BLOB */
	static String[] blobtype = { "BLOB" };
	
	/* REAL */
	static String[] realtype = { "REAL", "DOUBLE", "DOUBLEPRECISION", "FLOAT" };

	/* could by any of the above STORAGE class */
	static String[] numerictype = { "NUMERIC", "DECIMAL", "BOOLEAN", "DATE", "DATETIME" };
	
	
	
	SQLiteLexer lexer;
	SQLiteLexer parser;
	String tablename = null;
	String modulname = null;
	
	/* prepare data fields */
	List<String> coltypes = new ArrayList<String>();
	List<String> sqltypes = new ArrayList<String>();
	List<String> colconstraints = new ArrayList<String>();
	ArrayList<String> colnames = new ArrayList<String>();
	List<String> tableconstraint = new ArrayList<String>();
	Map<Integer,String> constraints = new HashMap<Integer,String>();
	HeaderPattern pattern = new HeaderPattern();

	TableDescriptor tds  = null;
	IndexDescriptor ids  = null;
	int column;
	
	
	/**
	 * Call this method to parse the SQL-statement CREATE TABLE.
	 * The result will be a TableDescriptor object, that contains component name, column names 
	 * an types. This information is necessary for matching the data records.
	 * 
	 * Example statement would look like this:
	 * 	
	 * CREATE TABLE 'users' (
	 *		'name' TEXT,
	 *		'surname' TEXT,
     *		'lastUpdate' TEXT
	 *	);
	 * 
	 * @param stmt
	 * @return a TableDescriptor Object with all the information about the component. 
	 */
	public TableDescriptor parseTable(String stmt)
	{
		
		/* what kind of SQL statement ? */
		
		if ((stmt.indexOf("CREATE TABLE")) >= 0)
		{
			return parseCreateTable(stmt);
		}
		else if ((stmt.indexOf("CREATE TEMP TABLE")) >= 0)
		{
			AppLog.debug("Found CREATE TEMP TABLE statement");
		}
		else if ((stmt.indexOf("CREATE VIRTUAL TABLE")) >= 0)
		{
			return parseCreateVirtualTable(stmt);	
		}
		
		return null;
		
	}
	
	public IndexDescriptor parseIndex(Job job, String stmt)
	{
		if ((stmt.indexOf("CREATE INDEX ") >= 0))
		{
			return parseCreateIndex(job, stmt);
		}
		
		return null;
	}
	
	
	
	
	
	public String trim(String value)
	{
		if (value.startsWith("'") || value.startsWith("\""))
		{
			value = value.substring(1);
		
			if (value.endsWith("'") || value.endsWith("\""))
				value = value.substring(0,value.length()-1);
		
		}
		return value;
	}
	
	/**
	 * Parse a CREATE VIRTUAL TABLE statement and return a TableDescriptor object.
	 * 
	 * @param stmt String with the CREATE VIRTUAL TABLE statement to parse.
	 * @return
	 */
	private TableDescriptor parseCreateVirtualTable(String stmt)
	{
		column = 0;
		modulname = null;
		
		// Create a lexer and parser for the input.
     	SQLiteLexer lexer = new SQLiteLexer(new ANTLRInputStream(stmt));
        SQLiteParser parser = new SQLiteParser(new CommonTokenStream(lexer));

    
        // Invoke the `create_table_stmt` production.
        ParseTree tree = parser.create_virtual_table_stmt();

        /* let's walk through the syntax tree */
        ParseTreeWalker.DEFAULT.walk(new SQLiteBaseListener(){ 	
       
        /* callback methods */
        	
        	@Override public void enterTable_name(SQLiteParser.Table_nameContext ctx) 
        	{
        		tablename = ctx.getText();
        		tablename = trim(tablename);
        		if (tablename.length()==0)
        			tablename = "<no name>";
        		System.out.println("Tablename "  + tablename);
        		
        	}
        	
        	
        	@Override public void enterModule_name(SQLiteParser.Module_nameContext ctx) 
        	{
        		 modulname = ctx.getText();
         		 System.out.println(" name " + modulname);
         		 if (modulname.equals("fts4")|| modulname.equals("fts3"))
         		 {
         			 System.out.println("Found FreeTextSearch Module (fts3/4)");
         			 System.out.println("Table name :" + tablename);
        			

         			 
         		 }
         		 else if (modulname.equals("rtree"))
         		 {
         			 System.out.println("Found rtree Module");

         		 } 
         		 

        	}
        	
        	@Override public void enterModule_argument(SQLiteParser.Module_argumentContext ctx) 
        	{ 
        		String modulargument = ctx.getText();
        		System.out.println(" arg " + modulargument);
        		
        		// FTS3/4 module?
        		if(modulname.equalsIgnoreCase("fts3") || modulname.equalsIgnoreCase("fts4"))
        		{
        			String clname = "";
        			
        			if(modulargument.startsWith("'") || modulargument.startsWith("\""))
        			{
            			int l = 0;
        				//example: 'name' TEXT
        				if(modulargument.startsWith("'"))
        				
        					l = modulargument.lastIndexOf("'");
        					
        				
        				else
        					
        					l = modulargument.lastIndexOf("\"");
    					
        				
        					
        				clname = modulargument.substring(1, l);
            			
        			}
        			else
        			{
        				// name TEXT - skip the type argument -> for FTS we always uses strings
        				String[] tokens = modulargument.split(" ");	
        			    clname = tokens[0];
        			}
        			
        			
    
        			colnames.add(clname);
        			
        			coltypes.add("STRING");
        			column++;
        		}
        		
        		/* RTree module?*/
        		if(modulname.equals("rtree") || modulname.equals("RTREE"))
        		{
        			colnames.add(modulargument);
        			
        			if (column == 0)
        				coltypes.add("INT");
        			else
        				coltypes.add("NUMERIC");
        			column++;
        		}
        	}

        	
        	
        },tree);
	
		tds = new TableDescriptor(tablename,stmt,sqltypes,coltypes,colnames,colconstraints,tableconstraint,null,stmt.contains("WITHOUT ROWID"));
		
		tds.setVirtual(true);
		if (null!=modulname)
			tds.setModulname(modulname);
		return tds;
	}
	
	String idxname;
	
	private IndexDescriptor parseCreateIndex(Job job, String stmt)
	{
		column = 0;
	    // Create a lexer and parser for the input.
     	SQLiteLexer lexer = new SQLiteLexer(new ANTLRInputStream(stmt));
        SQLiteParser parser = new SQLiteParser(new CommonTokenStream(lexer));

    
        // Invoke the `create_index_stmt` production.
        ParseTree tree = parser.create_index_stmt();
        
        /* let's walk through the syntax tree */
        ParseTreeWalker.DEFAULT.walk(new SQLiteBaseListener(){ 	
        	
        
        /**
    	 * {@inheritDoc}
    	 *
    	 * <p>The default implementation does nothing.</p>
    	 */
    	@Override public void enterIndex_name(SQLiteParser.Index_nameContext ctx) 
    	{ 
		
    		idxname = ctx.getText(); //"_IDX_" + ctx.getText();
        	
    		System.out.println("INDEX " + idxname);
        }  
    	
    	
    	/**
    	 * {@inheritDoc}
    	 *
    	 * <p>The default implementation does nothing.</p>
    	 */
    	@Override public void enterIndexed_column(SQLiteParser.Indexed_columnContext ctx) 
    	{
    		
    		
    		String colname = ctx.getText();
    	    
    			
    		/* skip the collate binary asc statement - this is not the column name ;-) */
    		if(colname.contains("COLLATEBINARYASC"))
    		{
    			int idx = colname.indexOf("COLLATEBINARYASC");
    			colname = colname.substring(0,idx);
    		}
    			
    		colname = trim(colname);
    		colnames.add(colname);
    		System.out.println("Columnname " + colname);		
    	}

    	@Override public void enterTable_name(SQLiteParser.Table_nameContext ctx) 
    	{
    		tablename = ctx.getText();
    		tablename = trim(tablename);
    		if (tablename.length()==0)
    			tablename = "<no name>";
    		System.out.println("Tablename "  + tablename);
    	}
    	
    	
    	
    	
        },tree);
		
        
		return new IndexDescriptor(job,idxname,tablename,stmt,colnames);
        
	}
	
	
	/**
	 * Parse a CREATE TABLE statement and return a TableDescriptor object.
	 * 
	 * @param stmt String with the CREATE TABLE statement to parse.
	 * @return
	 */
	private TableDescriptor parseCreateTable(String stmt)
	{
		column = 0;
		
	    // Create a lexer and parser for the input.
      	SQLiteLexer lexer = new SQLiteLexer(new ANTLRInputStream(stmt));
        SQLiteParser parser = new SQLiteParser(new CommonTokenStream(lexer));

    
        // Invoke the `create_table_stmt` production.
        ParseTree tree = parser.create_table_stmt();


        /* let's walk through the syntax tree */
        ParseTreeWalker.DEFAULT.walk(new SQLiteBaseListener(){ 	
        	
        	@Override public void enterTable_name(SQLiteParser.Table_nameContext ctx) 
        	{
        		isTableConstraint = false;
        		tablename = ctx.getText();
        		tablename = trim(tablename);
        		if (tablename.length()==0)
        			tablename = "<no name>";
        		
        		
        	}
        	
        	boolean tblconstraint = false;
        	boolean sqltypes_defined = false;
        	
        	
        	@Override public void enterColumn_name(SQLiteParser.Column_nameContext ctx) 
        	{
        		sqltypes_defined = false;
        		
        		if(inForeignTable)
        		{
        			inForeignTable = false;
            		sqltypes_defined = true;
        			return;
        		}
        		if(isTableConstraint)
        		{
        			return;
        		}
        		cons="";
        		
        		String colname = ctx.getText();
        		
        		//System.out.println("enterColumn_name()::colname =" + colname);
        		
        		if (!colname.equals("CONSTRAINT"))
        		{
        			colname = trim(colname);
        			colnames.add(colname);
        			
        			//System.out.println("Columnname " + colname);
        		}
        		else
        		{
        			tblconstraint = true;
        		}	
        		
        		
        	}
        	
        	boolean inForeignTable = false;
        	
        	/**
        	 * Enter a parse tree produced by {@link SQLiteParser#foreign_table}.
        	 * @param ctx the parse tree
        	 */
        	@Override public void enterForeign_table(SQLiteParser.Foreign_tableContext ctx)
        	{
        		System.out.println("Enter foreign Table!!!");
        		inForeignTable = true;
        	}
        	
        	@Override public void exitForeign_table(SQLiteParser.Foreign_tableContext ctx)
        	{
        		System.out.println("Exit foreign Table!!!");
        		
        	}
        	
        	
        	@Override public void enterType_name(SQLiteParser.Type_nameContext ctx) 
        	{ 
        		
        		String value = ctx.getText();
        	    value = value.trim();
        		
        	    if(value.length()==0)
        	    	value = "BLOB";
        	    	
        		/* the CONSTRAINT key word is mistakenly identified a type */
        		if (tblconstraint)
        		{
        			//System.out.println("Table Constraint: " + value);
        			tableconstraint.add(value);
        			tblconstraint = false;
        		}
        		else
        		{
	        		sqltypes.add(value);
	        		if(value.equalsIgnoreCase("PRIMARYKEY"))
	        			value = "BLOB";
	        		
	        		//System.out.println("SQLType::" + value);
	        		String type = getType(value);
	        		if (type.length()>0)
	        		{
	        			coltypes.add(type);
	        			//System.out.println("Typename " + trim(type));
	            		
	        		}
        		}
        		
        		sqltypes_defined = true;
        	}
        	
        	@Override public void enterKeyword(SQLiteParser.KeywordContext ctx)
        	{
        		System.out.println("Enter keyword ");
        	}
        	
        	@Override public void exitKeyword(SQLiteParser.KeywordContext ctx)
        	{
        		System.out.println("Exit keyword ");
            }
        	
        
        	String cons = "";
        	
        	@Override public void enterColumn_constraint(SQLiteParser.Column_constraintContext ctx)
        	{ 
        		String constraint = ctx.getText().toUpperCase();
        		
        		if (constraint.contains("NOTNULL"))
        			constraints.put(column,constraint);
        		
        	}
        	
        	@Override public void exitColumn_constraint(SQLiteParser.Column_constraintContext ctx)
        	{ 
        		String constraint = ctx.getText();
        		
        		//System.out.println("Columnconstraint " + constraint);
        		
        		if(constraint.contains("UNIQUE"))
        			System.out.println(" UNIQUE CONSTRAINT gefunden.");
        		
        		cons += constraint.toUpperCase() + " ";
        	}
        	
        
        	
        	@Override public void exitColumn_def(SQLiteParser.Column_defContext ctx) 
        	{ 
        		/* this method is passed even if SQLType is not defined */
        		if (!sqltypes_defined)
        		{
        			// the default type is BLOB
	        		sqltypes.add("BLOB");
	        		coltypes.add("BLOB");
        		}
        			
       	       // System.out.println("adding cons:" + cons);
        		colconstraints.add(cons);
        		column++;
        	}
        	
        	boolean isTableConstraint = false;

        	@Override public void enterTable_constraint(SQLiteParser.Table_constraintContext ctx) 
        	{ 
        		isTableConstraint = true;
        		//System.out.println("Table_constraint :: " +ctx.getText());
        		tableconstraint.add(ctx.getText());
        	}
	
        	
        	
        	@Override public void exitCreate_table_stmt(SQLiteParser.Create_table_stmtContext ctx)
        	{ 
        	    /* create a pattern object for constrain matching of records */
    		    HeaderPattern pattern = new HeaderPattern();
    		  
    		    /* the pattern always starts with a header constraint */ 
    		    pattern.addHeaderConstraint(colnames.size()+1,colnames.size()*2);
    		 
    		    /* Bug-fix: the sqlite_XXX columns do not have a type
    		     * https://www.sqlite.org/datatype3.html
                 * 3.1. Determination Of Column Affinity
                 * If the declared type for a column contains the string "BLOB" or if no type is specified then the column has affinity BLOB.
    		     */
    		    if (coltypes.size() == 0)
    		    {
    		    	for(int i =0; i < colnames.size(); i++)
    		    	{
    		    		coltypes.add("BLOB");
    		    		sqltypes.add("BLOB");
    		    	}
    		    }
    		    
    		    /*
				 *	https://www.sqlite.org/datatype3.html
				 *	
			     *	3.1. Determination Of Column Affinity
				 *	
				 *	If the declared type for a column contains the string "BLOB" or if no type is specified then the column has affinity BLOB.
				 *	So we can assume that default type is a BLOB.
    		     */
    		    if (coltypes.size() > sqltypes.size())
    		    {
    		    	for(int i =0; i < (colnames.size()-sqltypes.size()); i++)
    		    	{
    		    		coltypes.add("BLOB");
    		    		sqltypes.add("BLOB");
    		    	}
    		    }
    		    
        		
        		int cc = 0;
        		ListIterator<String> list = coltypes.listIterator();
        		while(list.hasNext())
        		{	
        		
	        		switch (list.next()) {
					case "INT":
						if (constraints.containsKey(cc))
							pattern.add(new IntegerConstraint(true));
						else
							pattern.add(new IntegerConstraint(false));
						
						break;
	
					case "TEXT":
						pattern.addStringConstraint();
						break;
	
					case "BLOB":
						pattern.addBLOBConstraint();
						break;
	
					case "REAL":
						
						pattern.addFloatingConstraint();
						break;
	
					case "NUMERIC":
						pattern.addNumericConstraint();
						break;
					}
	        		cc++;
        		}
	        		
  
        		tds = new TableDescriptor(tablename,stmt,sqltypes,coltypes,colnames,colconstraints,tableconstraint,pattern,stmt.contains("WITHOUT ROWID"));
        		System.out.println("PATTTERN: " + pattern);
        		
        	}
        	
        	
        },tree);
        
		return tds;
	}
	
	
	/**
	 * Find out if there is any type information (key word) in
	 * the substring 
	 * @param s the string, that should be parsed. 
	 * @return  the type (INT, TEXT, REAL etc.)
	 */
	private String getType(String s)
	{
		String type="";
		
		/* Attention: issue 3 */
		s = s.toUpperCase();
				
		if (s.startsWith("TIMESTAMP"))
		{
			type="NUMERIC"; 
 		}	
		else if (stringContainsItemFromList(s, texttypes))
		{
			type="TEXT";
		}
		else if (stringContainsItemFromList(s, inttypes))
		{
			type="INT";
		}
		else if (stringContainsItemFromList(s, blobtype))
		{
			type="BLOB";
		}
		else if (stringContainsItemFromList(s, realtype))
		{
			type="REAL";
		}
		else if (stringContainsItemFromList(s, numerictype))
		{
			type="NUMERIC";	
		}
		
		return type;
	}
	
	public static boolean stringContainsItemFromList(String inputStr, String[] items) {
		return Arrays.stream(items).parallel().anyMatch(inputStr::contains);
	}
	
	
}
