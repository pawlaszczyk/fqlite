package fqlite.descriptor;

import java.util.List;

import fqlite.pattern.HeaderPattern;

/**
 * An abstract base class for possible database 
 * objects like index,table, trigger or view.
 * 
 * At the moment FQLite only supports tables and
 * indices.
 * 
 * @author pawlaszc
 *
 */
public abstract class AbstractDescriptor {

	public boolean ROWID = true;
	public int rowid_col = -1;
    public boolean doNotScan = false;
	public List<String> serialtypes;
	public List<String> columnnames;
	public List<String> sqltypes;
	public List<String> columntypes;
    public String tblname;
	
	/**
	 * Return the name of the database object.
	 * @return
	 */
	abstract public String getName();
	
	abstract public boolean checkMatch(String match);
	
	abstract public HeaderPattern getHpattern();

	
}
