package fqlite.descriptor;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import fqlite.pattern.HeaderPattern;
import fqlite.util.Auxiliary;

/**
 * Objects of this class are used to represent a component. 
 * Besides component names and column names, regular expressions 
 * are also managed by this class. 
 * 
 * The latter are used to assign a record to a component. 
 * 
 * @author pawlaszc
 *
 */
public class TableDescriptor extends AbstractDescriptor implements Comparable<TableDescriptor>  {

	String regex = "";
	String delregex = "";
	public List<String> serialtypes;
	public List<String> columnnames;
	public List<String> sqltypes;
	public List<String> constraints; // constraint for each column
	public List<String> tableconstraints; // constraints on table level
	public List<String> primarykeycolumns;
	
	int size = 0;
	int numberofmultibytecolumns = 0;
	String fingerprint = ""; // the regex expression
	String signature = ""; // the type signature, i.e., INTSTRINGSTINGINT
	public String tblname = "";
	public int root = -1;
	public boolean ROWID = true; 
    private HeaderPattern hpattern = null;	
	public boolean virtual = false;
	public String modulname = null;
	public String sql = "";
	
	public String rowidcolumn = null;

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
	
			// check serialtypes, only if all serialtypes are valid the match is also valid
			for (int i = 1; i < values.length; i++) {
				/* get next column */
				String type = getColumntypes().get(i - 1);
	
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

	
	@Override
	public String getName()
	{
		return this.tblname;
	}
	
	public void setModulname(String name)
	{
		modulname = name;
	}
	
	public String getModulename()
	{
		return modulname;
	}
	
	public void signature(List<String> col) {
		signature = "";

		Iterator<String> iter = getColumntypes().iterator();

		while (iter.hasNext()) {
			signature += iter.next();
		}

	//	System.out.println("Signature::" + signature + " Table:: " + tblname );
	
	}

	public String getSignature() {
		return signature;
	}

	/**
	 * Constructor.
	 * 
	 * @param tblname			name of the table
	 * @param stmt				sql-statement
	 * @param sqltypes			List with all SQL types from the statement
	 * @param col				List with serial types matching the SQL types
	 * @param names				List with column names
	 * @param constraints		List with column constraints
	 * @param tableconstraints  List with table constraints
	 * @param pattern			pattern for matching the table header
	 * @param withoutROWID		true, if the table has no ROWID
	 */
	public TableDescriptor(String tblname, String stmt, List<String> sqltypes, List<String> col, List<String> names, List<String> constraints, List<String> tableconstraints, HeaderPattern pattern, boolean withoutROWID) {

		setColumntypes(col);
		signature(col);
		
		this.sqltypes = sqltypes;
		this.constraints = constraints;
		this.tableconstraints = tableconstraints;
		columnnames = names;
		ROWID = !withoutROWID;
		sql = stmt;
		
		this.tblname = tblname;
		
		primarykeycolumns = new LinkedList<String>();
		
		
		/* find the primary key by checking the column constraint */ 
		for(int i=0; i < names.size(); i++)
		{
			
			if (null != constraints)
				System.out.println("tblname: " + tblname);
			
			if (tblname.equals("__UNASSIGNED"))
				break;
			/* we look for the keyword PRIMARYKEY */
			if (constraints.get(i).contains("PRIMARYKEY"))
			{
				
				this.primarykeycolumns.add(names.get(i));
			}
			
		}
		
		
		
		/* check, if there is a PRIMARYKEY definition in the table constraints */
		if (null != tableconstraints)
			for (int i=0; i<tableconstraints.size();i++)
			{
				 
				String constraint = tableconstraints.get(i);
				
				Matcher m = Pattern.compile("PRIMARYKEY\\((.*?)\\)").matcher(constraint);
				while (m.find()) {
					String key = m.group(1);
				    System.out.println("Table Constraint Key "  + key);
				    if (!key.contains(","))
				    {
				    	// simple key like 'id'
				    	this.primarykeycolumns.add(key);
				    }
				    else
				    {
				    	// composite key like 'name,birthdate'
				    	String[] parts = key.split(",");
				        for(String c: parts)
				        {
					    	this.primarykeycolumns.add(c);
				        }
				    }
				}
			}
		
		
		/*
		 *  With one exception noted below, if a rowid table has 
		 *  a primary key that consists of a <single column> and the 
		 *  declared type of that column is "INTEGER" in any mixture 
		 *  of upper and lower case, then the column becomes an alias 
		 *  for the rowid. 
		 *  Such a column is usually referred to as an "integer primary key". 
		 *  A PRIMARY KEY column only becomes an integer primary key if the
		 *  declared type name is exactly "INTEGER". 
		 *  
		 *  [source: https://www.sqlite.org/lang_createtable.html#rowid]	
		 */
		if (primarykeycolumns.size()==1)
		{	
			int i = names.indexOf(primarykeycolumns.get(0));
			System.out.println("Primary key column :: " + i);
			if(i >= 0 && sqltypes.get(i).toUpperCase().equals("INTEGER"))
			{
				if(!constraints.get(i).toUpperCase().contains("DESC"))
				{
					System.out.println("Attention! integer primary key: " + names.get(i));
					/* Note: this column has the columntype "00" */
					rowidcolumn = names.get(i);
					pattern.change2RowID(i);
				}
			}	
		}
		
		
		
		setHpattern(pattern);
		
	
		/* create a table fingerprint for later search */
		Iterator<String> iter = getColumntypes().iterator();
		while (iter.hasNext()) {
			size++;
			regex += getColumn(iter.next(), false);
			fingerprint += fingerprint;
		
		}
	}
	
	public void setVirtual(boolean val)
	{
		virtual = val;
	}
	
	public boolean isVirtual()
	{
		return virtual;
	}
	

	/**
	 * case 1: Returns the regex for a regular component header including the Header
	 * length byte. This pattern should match all records with a complete header.
	 * 
	 * [headerlength] col(1) col(2) col(3) ... col(4)
	 * 
	 * @return regex
	 */
	public Pattern getStandardPattern() {
		return Pattern.compile((Auxiliary.byteToHex((byte) (size + 1)) + regex).trim());
	}

	/**
	 * case 2: Returns the regex header without header length byte but with all
	 * serialtypes. This pattern should match all records startRegion a component, where parts
	 * of the header information has been overwritten.
	 * 
	 * @return regex
	 */
	public Pattern getPatternWithoutHeaderLength() {
		return Pattern.compile(regex.trim());
	}

	/**
	 * case 3: Returns the regex header without header length byte and first column.
	 * This pattern should match all records startRegion a component, where parts of the header
	 * has been overwritten, because it has been deleted.
	 * 
	 * col(2) col(3) col(4) ... col(n)
	 * 
	 * @return regex
	 */
	public Pattern getPatternWithoutFirstCol() {
		return Pattern.compile(getPattern(1, size, false));
	}

	/**
	 * Returns a regex for only some serialtypes of the component.
	 * 
	 * @param startcolumn
	 * @param endcolumn
	 * @return regex
	 */
	public String getPattern(int startcolumn, int endcolumn, boolean multicol) {
		String pat = "";

		for (int i = startcolumn; i < endcolumn; i++) {
			pat += getColumn(getColumntypes().get(i), multicol);

		}
		return pat;
	}

	/**
	 * Returns the number of the root data page. 
	 * @return
	 */
	public int getRootOffset() {
		return this.root;
	}

	/**
	 * Return the Fingerprint, i.e., a String of colummn types.
	 * @return
	 */
	public String getFingerprint() {
		return fingerprint;
	}

	/**
	 * Compares a given signature with the signature of the current component.
     *
	 * @param signature
	 * @return true, if the signature matches
	 */
	public boolean matches(String signature) {
		return fingerprint.equals(signature);
	}

	/**
	 * Returns the component header length.
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
		return getColumntypes().size();
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
	 * Returns a field of regex-pattern to match the current component.
	 * @return field with Pattern objects
	 */
	public Pattern[] getRegex() {

		Pattern[] headers = new Pattern[4];

		headers[0] = getStandardPattern();

		headers[1] = getPatternWithoutHeaderLength();

		headers[2] = getPatternMultiCol();

		headers[3] = getPatternWithoutFirstCol();

	
		return headers;
	}

	/**
	 * Outputs component name and column names to the console. 
	 * 
	 **/
	public void printTableDefinition() {
		System.out.println("TABLE" + tblname);
		System.out.println("COLUMNS: " + columnnames);
	}

	@Override
	public String toString() {
		Pattern[] elements = getRegex();
		String output = "";
		for (Pattern s : elements) {
			output += s + "\n";
		}

		return output;
	}

	@Override
	public boolean equals(Object o) {
		return this.regex.equals(((TableDescriptor) o).regex);
	}


	public List<String> getColumntypes() {
		return serialtypes;
	}


	public void setColumntypes(List<String> columntypes) {
		this.serialtypes = columntypes;
	}


	public HeaderPattern getHpattern() {
		return hpattern;
	}


	public void setHpattern(HeaderPattern hpattern) {
		this.hpattern = hpattern;
	}


	@Override
	public int compareTo(TableDescriptor o) {
		return tblname.compareTo(o.tblname);	
	}
}
