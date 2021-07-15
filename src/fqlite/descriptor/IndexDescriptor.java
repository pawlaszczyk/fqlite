package fqlite.descriptor;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;
import fqlite.pattern.HeaderPattern;
import fqlite.util.Auxiliary;

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
public class IndexDescriptor extends AbstractDescriptor{

	public List<String> columntypes;
	public List<String> columnnames;
	int size = 0;
	public String idxname = "";
    public String tablename = "";
	public int root = -1;
    public HeaderPattern hpattern = null;	
    private String sql = "";

    public boolean checkMatch(String match) {
		
        try
        {
			/* hex-String representation to byte array */
			byte[] bcol = Auxiliary.decode(match);
	
			/* interpret all byte values as a list of varints */
			/* each varint represents a columntype */
			int[] values = Auxiliary.readVarInt(bcol);
	
			/*
			 * normally, the first byte of the match holds total length of header bytes
			 * including this byte
			 */
			int headerlength = values[0];
			System.out.println(headerlength);
			
			boolean valid = true;
	
			// check serialtypes, only if all serialtypes are valid the match is valid too
			for (int i = 1; i < values.length; i++) {
				/* get next column */
				String type = columntypes.get(i - 1);
	
				switch (type) {
				case "INT":
					/* an INT column has always a value between 0..6 */
					valid = values[i] >= 0 && values[i] <= 6;
					break;
				case "REAL":
					/* a FLOATING POINT COLUMN is always mapped with value 7 */
					valid = values[i] == 7;
					break;
				case "TEXT":
					/* a TEXT COLUMN has always an odd value bigger 13 or is zero */
					if (values[i] == 0)
						valid = true;
					else if (values[i] % 2 != 0)
						valid = values[i] > 13;
					else
						valid = false;
					break;
				case "BLOB":
					/* a BLOB COLUMN has always an even value bigger 13 or is zero */
					if (values[i] == 0)
						valid = true;
					else if (values[i] % 2 == 0)
						valid = values[i] > 12;
					else
						valid = false;
					break;
	
				case "NUMERIC":
					valid = values[i] >= 0 && values[i] <= 9;
					break;
				}
	
				/* as soon as one of the serialtypes is not valid -> discard the match */
				if (!valid)
					return false;
			}
        }
        catch(Exception err)
        {
        	/* Note: if something went wrong during validation - the match was not valid */
        	return false;
        }

			
		return true;
	}


	public IndexDescriptor(String idxname, String tablename, String stmt, List<String> names) {

		super.ROWID = false;
		columnnames = names;
		this.idxname = idxname;
		this.tablename = tablename;
		this.columntypes = new LinkedList<String>();
		this.setSql(stmt);
	}
	

	/**
	 * Returns a regex for only some serialtypes of the indices.
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

	
	/**
	 * Returns the indices header length.
	 * The header begins with a single varint which determines 
	 * the total number of bytes in the header. 
	 * The varint value is the size of the header in bytes including 
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
	 * consist of a single byte. The serial type varints for large 
	 * strings and BLOBs might extend to two or three byte varints,
	 * but that is the exception rather than the rule.
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
		System.out.println("Index" + idxname);
		System.out.println("COLUMNS: " + columnnames);
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

	
}
