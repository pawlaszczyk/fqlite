package fqlite.descriptor;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;
import fqlite.base.Job;
import fqlite.pattern.HeaderPattern;
import fqlite.util.Auxiliary;
import fqlite.util.VIntIter;

/**
 * Objects of this class are used to represent an indices. 
 * Besides indices name and column name, matching constrains 
 * are also managed by this class. 
 * 
 * The latter are used to assign a record to an indices. 
 * 
 * @author pawlaszc
 *
 */
@SuppressWarnings("rawtypes")
public class IndexDescriptor extends AbstractDescriptor implements Comparable{

	public List<String> boolcolumns;
	int size = 0;
	public String idxname = "";
    public String tablename = "";
	public int root = -1;
    public HeaderPattern hpattern = null;	
    private String sql = "";
	public Hashtable<String,String> tooltiptypes = new Hashtable<String,String>();
	public TableDescriptor table = null;
	public Job job;
	
	public void addColumtype(String columntype){
		serialtypes.add(columntype);
		columntypes.add(columntype);
	}
       
	public List<String> getColumntypes() {
		return serialtypes;
	}


	public void setColumntypes(List<String> columntypes) {
		this.serialtypes = columntypes;
	}
	
    public boolean checkMatch(String match) {
		
        try
        {
			/* hex-String representation to byte array */
			byte[] bcol = Auxiliary.decode(match);
	
			/* interpret all byte values as a list of varints */
			/* each varint represents a column type */
			VIntIter values = VIntIter.wrap(bcol,4);
			
			
			/*
			 * Normally, the first byte of the match holds the total length of header bytes
			 * including this byte
			 */
			//int headerlength = values.next().length;
			//System.out.println(headerlength);
			values.next();
			
			boolean valid = true;
	        int i = 1;
			// check serialtypes, only if all serialtypes are valid, the match is valid too
			while (values.hasNext()) {
				
				int value = values.next();
				
				/* get next column */
				String type = columntypes.get(i - 1);
	
				switch (type) {
				case "INT":
					/* an INT column always has a value between 0..6 */
					valid = value >= 0 && value <= 6;
					break;
				case "REAL":
					/* a FLOATING POINT COLUMN is always mapped with value 7 */
					valid = value == 7;
					break;
				case "TEXT":
					/* a TEXT COLUMN has always an odd value bigger than 13 or is zero */
					if (value == 0)
						valid = true;
					else if (value % 2 != 0)
						valid = value > 13;
					else
						valid = false;
					break;
				case "BLOB":
					/* a BLOB COLUMN always has an even value bigger than 13 or is zero */
					if (value == 0)
						valid = true;
					else if (value % 2 == 0)
						valid = value > 12;
					else
						valid = false;
					break;
	
				case "NUMERIC":
					valid = value >= 0 && value <= 9;
					break;
				}
	
				/* as soon as one of the serialtypes is not valid -> discard the match */
				if (!valid)
					return false;
			i++;
			}
        }
        catch(Exception err)
        {
        	/* Note: if something went wrong during validation - the match was not valid */
        	return false;
        }

			
		return true;
	}


	 /**
	  * Use this method to initialise a new index table description object.
	  * @param job	The job/database it belongs to
	  * @param idxname name of the index (from the schema)
	  * @param tablename name of the table the index belongs to
	  * @param stmt the concrete SQL statement used to create the index
	  * @param names list of column names 
	  */
	public IndexDescriptor(Job job, String idxname, String tablename, String stmt, ArrayList<String> names) {

		super.ROWID = false;
		columnnames = names;
		this.idxname = idxname;
		this.tablename = tablename;
		this.columntypes = new LinkedList<String>();
		this.serialtypes = new LinkedList<String>();
		
		this.setSql(stmt);
		this.boolcolumns = new LinkedList<String>();
		
		/* assign the correct table description object to the index table */
		Iterator<TableDescriptor> tbls = job.headers.iterator();
		while(tbls.hasNext()) {
		
			TableDescriptor td = tbls.next();
				if(td.tblname.equals(tablename))
					this.table = td;
		}
	}
	

	/**
	 * Returns a regex for only some serial types of the indices.
	 * 
	 * @param startcolumn
	 * @param endcolumn
	 * @return regex
	 */
	public String getPattern(int startcolumn, int endcolumn, boolean multicol) {
		String pat = "";

		for (int i = startcolumn; i < endcolumn; i++) {
			pat += getColumn(columntypes.get(i), multicol);

		}
		return pat;
	}

	@Override
	public String getName()
	{
		return this.idxname;
	}
	
	
	/**
	 * Returns the number of the root data page. 
	 * @return
	 */
	public int getRootOffset() {
		return this.root;
	}

	
	public HeaderPattern getHpattern() {
		return hpattern;
	}


	public void setHpattern(HeaderPattern hpattern) {
		this.hpattern = hpattern;
	}
	
	/**
	 * Returns the indices header length.
	 * The header begins with a single varint, which determines
	 * the total number of bytes in the header. 
	 * The varint value is the size of the header in bytes, including
	 * the size varint itself.
	 * @return
	 */
	public int getLength() {
		return 1 + size;
	}

	/**
	 * Return the number of columns (startRegion the component header). 
	 * @return
	 */
	public int numberofColumns() {
		return columntypes.size();
	}

	public Pattern getPatternMultiCol() {
		return Pattern.compile(getPattern(0, size, true));
	}

	/**
	 * Returns the regular expression for the pattern matching for 
	 * a particular serial type.
	 * 
	 * The header size varint and serial type varints will usually 
	 * consists of a single byte. The serial type varints for large
	 * strings and BLOBs might extend to two or three-byte varints,
	 * But that is the exception rather than the rule.
	 * 
	 * @param serialtype
	 * @param multicol
	 * @return
	 */
	private String getColumn(String serialtype, boolean multicol) {

		switch (serialtype) {
		case "INT":
			return "0[0-6]";

		case "REAL":
			return "07";

		case "TEXT":
			if (multicol)
				return "[0-9a-f][0-9a-f]{0,4}";
			else
				return "[0-9a-f][0-9a-f]";

		case "BLOB":
			if (multicol)
				return "[0-9a-f][0-9a-f]{0,4}";
			else
				return "[0-9a-f][0-9a-f]";

		default:
			return "[0-9a-f][0-9a-f]";
		}

	}


	/**
	 * Outputs indices name and column names to the console. 
	 * 
	 **/
	public void printIndexDefinition() {
		//System.out.println("Index" + idxname);
		//System.out.println("COLUMNS: " + columnnames);
	}

	@Override
	public String toString() {
		String output = "";

		return output;
	}


	public String getSql() {
		return sql;
	}


	public void setSql(String sql) {
		this.sql = sql;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (o instanceof IndexDescriptor)
		{
			IndexDescriptor c = (IndexDescriptor)o;
			return this.idxname.equals(c.idxname);
		}
			
		return false;
		
	}

	public String getSqlTypeForColumn(String column){
		if (column.equals("rowid"))
			return "INTEGER";
		return this.table.getSqlTypeForColumn(column);
	}

	
	public int compareTo(IndexDescriptor o) {
		return tablename.compareTo(o.tablename);
	}


	@Override
	public int compareTo(Object o) {
		return compareTo((IndexDescriptor)o);
	}
	
	
}
