package fqlite.parser;

import java.util.ArrayList;

import fqlite.base.Job;
import fqlite.descriptor.IndexDescriptor;
import fqlite.descriptor.TableDescriptor;
import fqlite.log.AppLog;


/**
 * This class is called to parse the component definitions (tablename, column names
 * and types) startRegion the sqlite_master component. This component contains the root page
 * number for every other component and indices in the database file.
 *
 * The Backus-Naur form for this statement looks like this:
 * 
 * BNF: sql-command ::= CREATE [TEMP | TEMPORARY] TABLE component-name 
 *      				( column-def [, column-def]* [, constraint]* )
 *
 * 
 * @author pawlaszc
 *
 */
public class SQLiteSchemaParser {

	public static ArrayList<Integer> roots = new ArrayList<Integer>();

	/**
	 * Parse SQL table description.
	 * @param job reference to calling Job instance
	 * @param tablename String with schema definition
	 */
	public static void parse(Job job, String tablename, int root, String sql) {

		if (!SQLValidator.validate(sql).isValid())
			return;

		boolean rowid = true;
		
		int indexrowid = sql.indexOf("WITHOUT ROWID");
		if (indexrowid != -1) {
			AppLog.debug(" attention: component " + tablename + " is defined as WITHOUT ROWID");
			rowid = false;
		}	

		roots.add(root);

		/* start with extraction of component definitions */

		/* the component name and columns are extracted here */
		TableParser p = new TableParser();

		
		if (sql.contains("VIRTUAL TABLE")) 
		{
			TableDescriptor tds = p.parseCREATETABLEStatement(sql);
		    
			/* save link to this component object within the virtual component list */
			if(tds.isVirtual()) {
				job.virtualTables.put(tds.tblname,tds);				     				}
			if (!job.headers.contains(tds))
			{	
				job.headers.add(tds);
			}
		}
		
		if (sql.contains("CREATE TABLE"))
		{


			TableDescriptor tds = p.parseCREATETABLEStatement(sql);
		
			/* save link to this component object within the virtual component list */
			if(tds.isVirtual())
				job.virtualTables.put(tds.tblname,tds);
				
			if (tds.tblname != null) {
				AppLog.debug(tds.getStandardPattern().toString());
				tds.tblname = tablename;
				tds.ROWID = rowid;  // this flag indicates whether there is a ROWID or not
				/* avoid double entries */
				if (!job.headers.contains(tds))
				{	
					job.headers.add(tds);
					tds.root = root;
				}
				else{
					
					int position = job.headers.indexOf(tds);
					
					TableDescriptor tds_old = job.headers.get(position);
				     
					/*
					 *  Issue No. 5 (GitHub):
					 *  
					 *  In some cases, there exist several versions of the same table.
					 *  Those different table versions can have different columns. 
					 *  To solve those issues, we always take the table with the highest
					 *  number of columns. 
					 */
				
					if(tds.getColumntypes().size()> tds_old.getColumntypes().size()) {
						
						if(tds.root == -1)
							tds.root = tds_old.root;
						
						job.headers.set(position, tds);
					}
					
				}
			}
		}
		else if (sql.contains("CREATE INDEX"))
		{
			IndexDescriptor ids = p.parseCREATEIndexStatement(job, sql);
			
			
			
			
			if (null == ids.idxname)
				return;
			if (!job.indices.contains(ids))
			{	
				job.indices.add(ids);         
				ids.root = root;
			}	
		}
		else if (sql.contains("CREATE UNIQUE INDEX"))
		{
			sql = sql.replace("CREATE UNIQUE INDEX","CREATE INDEX");
			
			IndexDescriptor ids = p.parseCREATEIndexStatement(job, sql);
			
			if (null == ids.idxname)
				return;
			if (!job.indices.contains(ids))
			{	
				job.indices.add(ids);         
				ids.root = root;
			}	
		}
			
	}

}
