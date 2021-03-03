package fqlite.descriptor;

import java.util.Iterator;
import java.util.List;
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
public class TableDescriptor extends AbstractDescriptor {

	String regex = "";
	String delregex = "";
	public List<String> columntypes;
	public List<String> columnnames;
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
	
			// check columntypes, only if all columntypes are valid the match is also valid
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
	
				/* as soon as one of the columntypes is not valid -> discard the match */
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

		System.out.println("Signature::" + signature + " Table:: " + tblname );
	
	}

	public String getSignature() {
		return signature;
	}

	public TableDescriptor(String tblname, String stmt, List<String> col, List<String> names, HeaderPattern pattern, boolean withoutROWID) {

		setHpattern(pattern);
		setColumntypes(col);
		signature(col);
		columnnames = names;
		ROWID = !withoutROWID;
		sql = stmt;
		
		this.tblname = tblname;
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
	 * columntypes. This pattern should match all records startRegion a component, where parts
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
	 * Returns a regex for only some columntypes of the component.
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
		return columntypes;
	}


	public void setColumntypes(List<String> columntypes) {
		this.columntypes = columntypes;
	}


	public HeaderPattern getHpattern() {
		return hpattern;
	}


	public void setHpattern(HeaderPattern hpattern) {
		this.hpattern = hpattern;
	}
}
