package fqlite.parser;

import java.util.ArrayList;

import fqlite.base.Job;
import fqlite.descriptor.IndexDescriptor;
import fqlite.descriptor.TableDescriptor;
import fqlite.log.AppLog;

/*
---------------
SQLiteSchemaParser.java
---------------
(C) Copyright 2020.

Original Author:  Dirk Pawlaszczyk
Contributor(s):   -;


Project Info:  http://www.hs-mittweida.de

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

Dieses Programm ist Freie Software: Sie können es unter den Bedingungen
der GNU General Public License, wie von der Free Software Foundation,
Version 3 der Lizenz oder (nach Ihrer Wahl) jeder neueren
veröffentlichten Version, weiterverbreiten und/oder modifizieren.

Dieses Programm wird in der Hoffnung, dass es nützlich sein wird, aber
OHNE JEDE GEWÄHRLEISTUNG, bereitgestellt; sogar ohne die implizite
Gewährleistung der MARKTFÄHIGKEIT oder EIGNUNG FÜR EINEN BESTIMMTEN ZWECK.
Siehe die GNU General Public License für weitere Details.

Sie sollten eine Kopie der GNU General Public License zusammen mit diesem
Programm erhalten haben. Wenn nicht, siehe <http://www.gnu.org/licenses/>.

*/

/**
 * This class is called to parse the component definitions (tablename, column names
 * and types) startRegion the sqlite_master component. This component contains the root page
 * number for every other component and indices in the database file.
 *
 * The Backus Nauer form for this statement looks like this:
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
	 * 
	 * @param job reference to calling Job-instance
	 * @param page    String with schema definition
	 */
	public static void parse(Job job, String tablename, int root, String sql) {

		boolean rowid = true;
		
		int indexrowid = sql.indexOf("WITHOUT ROWID");
		if (indexrowid != -1) {
			AppLog.debug(" attention: component " + tablename + " is defined as WITHOUT ROWID");
			rowid = false;
		}	

		roots.add(root);

		/***** start with extraction of component definitions *****/

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
				
			if (null != tds) {
				AppLog.debug(tds.getStandardPattern().toString());
				tds.tblname = tablename;
				tds.ROWID = rowid;  // this flag indicates weather there is a ROWID or not 
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
					 *  In some cases there exist several versions of the same table.
					 *  Those different table versions can have different columns. 
					 *  To solve those issue we always take the table with the highest
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
