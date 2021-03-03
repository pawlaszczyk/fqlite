package fqlite.parser;

import fqlite.descriptor.IndexDescriptor;
import fqlite.descriptor.TableDescriptor;

/*
---------------
TableParser.java
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
 * This class is just a wrapper class.
 * 
 * For the actual parser functionality see SimpleSQLiteParser. 
 * 
 * The Backus Nauer form for this statement looks like this:
 * 
 * BNF: sql-command ::= CREATE [TEMP | TEMPORARY] TABLE component-name 
 *      				( column-def [, column-def]* [, constraint]* )
 *
 * @author pawlaszc
 *
 */
public class TableParser {

	
	
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
	public TableDescriptor parseCREATETABLEStatement(String stmt)
	{
		
		SimpleSQLiteParser parser = new SimpleSQLiteParser();
		TableDescriptor tds = parser.parseTable(stmt);
		
		return tds;
	}
	
	/**
	 * /**
	 * Call this method to parse the SQL-statement CREATE INDEX.
	 * The result will be a IndexDescriptor object, that contains component name, column names 
	 * an types. This information is necessary for matching the data records.
	 
	 * @param stmt
	 * @return a IndexDesriptor Object with all information about the component.
	 */
	public IndexDescriptor parseCREATEIndexStatement(String stmt)
	{
		
		SimpleSQLiteParser parser = new SimpleSQLiteParser();
		IndexDescriptor idx = parser.parseIndex(stmt);
		
		return idx;
	}
	
	
	
	/**
	 * Use this main() method only for testing purposes. 
	 * @param args
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
