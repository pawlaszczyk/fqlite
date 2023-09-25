package fqlite.descriptor;

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

	
	/**
	 * Return the name of the database object.
	 * @return
	 */
	abstract public String getName();
	
	
}
