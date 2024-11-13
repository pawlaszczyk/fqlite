package fqlite.base;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

import fqlite.log.AppLog;
import fqlite.types.SerialTypes;
import fqlite.types.StorageClass;
import fqlite.util.Auxiliary;
import fqlite.util.CarvingResult;
/**
 * This class provides different access methods to read the different cells records from a SQLite database.
 * 
 * We distinguish in between regular records, overflow and deleted records.
 * 
 * A separate access method is provided for each type.
 * 
 * @author pawlaszc
 *
 */
public class PageReader{

	public AtomicInteger found = new AtomicInteger();
	public AtomicInteger inrecover = new AtomicInteger();

	public static final String TABLELEAFPAGE = "0d";
	public static final String TABLEINTERIORPAGE = "05";
	public static final String INDEXLEAFPAGE = "0a";
	public static final String INDEXINTERIORPAGE = "02";
	public static final String OVERFLOWPAGE = "00";

	public Job job;

	/**
	 * Get the type of page. There are 4 different basic types in SQLite:  
	 * (1) component-leaf (2) component-interior  (3) indices-leaf and 
	 * (4) indices-interior. 
	 * 
	 * Beside this, we can further find overflow pages and removed pages.
	 * Both start with the 2-byte value 0x00.
	 * 
	 * @param content  the page content as a String
	 * @return type of page
	 */
	public static int getPageType(String content) {

		boolean skip = false;
		String type = content.substring(0, 2);
		switch (type) {

		case TABLELEAFPAGE:
			return 8;

		case TABLEINTERIORPAGE:
			return 12;

		case INDEXLEAFPAGE:
			skip = true;
			return 10;

		case INDEXINTERIORPAGE:
			skip = true;
			return 2;

		case OVERFLOWPAGE:  // or dropped page
			return 0;

		default:
			skip = true;
		}
		if (skip) {
			return -1;
		}

		return -99;
	}

	/**
	 * Constructor. To return values to the calling job environment, an object reference of
	 * job object is required.
	 * 
	 * @param job
	 */
	public PageReader(Job job) {
		this.job = job;
	}



	/**
	 * This method is used to extract a previously deleted record startRegion a page. 
	 * 
	 * @param start  the exact position (offset relative to the page start).
	 * @param buffer a ByteBuffer with the data page to analyze.
	 * @param header the record header bytes including header length and serial types.
	 * @param bs	a data structure that is used to record which areas have already been searched 
	 * @param pagenumber  the number of the page we going to analyze
	 * @return
	 * @throws IOException  if something went wrong during read-up. 
	 */
	public CarvingResult readDeletedRecord(Job job, int start, ByteBuffer buffer, String header, BitSet bs,
			int pagenumber) throws IOException {

		LinkedList<String> record = new LinkedList<String>();
		
		SqliteElement[] columns;

		buffer.position(start);

		int recordstart = start - (header.length() / 2) - 2;

		columns = toColumns(header);

		if (null == columns)
			return null;

		String fp = null;
		try {
			fp = Auxiliary.getTableFingerPrint(columns);

		} catch (NullPointerException err) {
			// System.err.println(err);
		}
		if (null == fp)
			fp = "unkown";

		boolean error = false;

		record.add(((pagenumber - 1) * job.ps + buffer.position())+"");
		
		/* use the header information to reconstruct */
		int pll = Auxiliary.computePayloadLengthS(header);

		int so = Auxiliary.computePayloadS(pll,job.ps);

		int overflow = -1;

		if (so < pll) {
			int phl = header.length() / 2;

			int last = buffer.position();
			AppLog.debug(" deleted spilled payload ::" + so);
			AppLog.debug(" deleted pll payload ::" + pll);
			buffer.position(buffer.position() + so - phl - 1);

			overflow = buffer.getInt();
			buffer.position(last);

			ByteBuffer bf;

			/* is the overflow page value correct ? */
			if (overflow < job.numberofpages) {

				/*
				 * we need to increment page number by one since we start counting with zero for
				 * page 1
				 */
				byte[] extended = readOverflow(overflow - 1);

				byte[] c = new byte[pll + job.ps];

				buffer.position(0);
				byte [] originalbuffer = new byte[job.ps];
				for (int bb = 0; bb < job.ps; bb++)
				{
				   originalbuffer[bb] = buffer.get(bb);	
				}	
				
				buffer.position(last);
				
				/* copy spilled overflow of current page into extended buffer */
				System.arraycopy(originalbuffer, buffer.position(), c, 0, so - phl);
				/* append the rest startRegion the overflow pages to the buffer */
				System.arraycopy(extended, 0, c, so - phl, extended.length - so);
				bf = ByteBuffer.wrap(c);

			} else {
				pll = so;
				bf = buffer;
			}
			bf.position(0);

			/* start reading the content */
			for (SqliteElement en : columns) {
				if (en == null) {
					continue;
				}

				byte[] value = new byte[en.getlength()];

				bf.get(value);

				record.add(en.toString(value,false,true));
				
			}

			// set original buffer pointer to the end of the spilled payload
			// just before the next possible record
			buffer.position(last + so - phl - 1);

		} else {

			for (SqliteElement en : columns) {

				if (en == null) {
					continue;
				}

				byte[] value = new byte[en.getlength()];
				if ((buffer.position() + en.getlength()) > buffer.limit()) {
					error = true;
					return null;
				}
				buffer.get(value);

                record.add(en.toString(value,false,true));

			}

			if (error)
				return null;
		}

		/* mark bytes as visited */
		bs.set(recordstart, buffer.position()-1, true);
		AppLog.debug("Besucht :: " + recordstart + " bis " + buffer.position());
		int cursor = ((pagenumber - 1) * job.ps) + buffer.position();
		AppLog.debug("Besucht :: " + (((pagenumber - 1) * job.ps) + recordstart) + " bis " + cursor);
        		
		return new CarvingResult(buffer.position(),cursor, new StringBuffer(), record);
	}

	public static String convertToUTF8(String s) {
		String out = null;
		try {
			out = new String(s.getBytes("UTF-8"), "ISO-8859-1");
		} catch (java.io.UnsupportedEncodingException e) {
			return null;
		}
		return out;
	}

	/**
	 * Converts a byte array into StringBuffer.
	 * @param col   column number
	 * @param en  the element to convert
	 * @param value the actual value
	 * @return  the converted StringBuffer value
	 */
	public StringBuffer write(int col, SqliteElement en, byte[] value) {

		StringBuffer val = new StringBuffer();

		if (col > 0)
			val.append(";");

		val.append(en.toString(value,false,true));

		return val;
	}



	/**
	 * Reads the specified page as overflow. 
	 * 
	 * Background:
	 * When the payload of a record is to large, it is spilled onto overflow pages.
	 * Overflow pages form a linked list. The first four bytes of each overflow page are a 
	 * big-endian integer which is the page number of the next page in the chain, or zero for 
	 * the final page in the chain. The fifth byte through the last usable byte are used to 
	 * hold overflow content.
     *
     *
	 * @param pagenumber
	 * @return all bytes that belong to the payload 
	 */
	public byte[] readOverflow(int pagenumber) {
		byte[] part = null;

		/* read the next overflow page startRegion file */
		ByteBuffer overflowpage = job.readPageWithNumber(pagenumber, job.ps);

		overflowpage.position(0);
		int overflow = overflowpage.getInt();
		//AppLog.debug(" overflow:: " + overflow);

		if (overflow == 0) {
			// termination condition for the recursive callup's
			AppLog.debug("No further overflow pages");
			/* startRegion the last overflow page - do not copy the zero bytes. */
		} else {
			/* recursively call next overflow page in the chain */
			part = readOverflow(overflow);
		}

		/*
		 * we always crab the complete overflow-page minus the first four bytes - they
		 * are reserved for the (possible) next overflow page offset
		 **/
		byte[] current = new byte[job.ps - 4];
		//System.out.println("current ::" + current.length);
		//System.out.println("bytes:: " + (job.ps -4));
		//System.out.println("overflowpage :: " + overflowpage.limit());
		
		overflowpage.position(4);
		overflowpage.get(current, 0, job.ps - 4);
		// overflowpage.get(current, 0, job.ps-4);

		/* Do we have a predecessor page? */
		if (null != part) {
			/* merge the overflow pages together to one byte-array */
			byte[] of = new byte[current.length + part.length];
			System.arraycopy(current, 0, of, 0, current.length);
			System.arraycopy(part, 0, of, current.length, part.length);
			return of;
		}

		/* we have the last overflow page - no predecessor pages */
		return current;
	}

	/**
	 * Convert a base16 string into a byte array.
	 */
	public static byte[] decode(String s) {
		int len = s.length();
		byte[] r = new byte[len / 2];
		for (int i = 0; i < r.length; i++) {
			int digit1 = s.charAt(i * 2), digit2 = s.charAt(i * 2 + 1);
			if (digit1 >= '0' && digit1 <= '9')
				digit1 -= '0';
			else if (digit1 >= 'a' && digit1 <= 'f')
				digit1 -= 'a' - 10;
			if (digit2 >= '0' && digit2 <= '9')
				digit2 -= '0';
			else if (digit2 >= 'a' && digit2 <= 'f')
				digit2 -= 'a' - 10;

			r[i] = (byte) ((digit1 << 4) + digit2);
		}
		return r;
	}

	

	/**
	 * 
	 * @param header
	 * @return
	 */
	public SqliteElement[] toColumns(String header) {
		/* hex-String representation to byte array */
		byte[] bcol = Auxiliary.decode(header);
		return get(bcol);
	}

	/**
	 * A passed ByteBuffer is converted into a byte array. Afterwards it is used
	 * to extract the column types. Exactly one element is created per column type. 
	 * 
	 * @param headerlength total length of the header in bytes
	 * @param buffer the headerbytes
	 * @return the column field
	 * @throws IOException
	 */
	public SqliteElement[] getColumns(int headerlength, ByteBuffer buffer) throws IOException {

		byte[] header = new byte[headerlength];

		try
		{
			// get header information
			buffer.get(header);
		}
		catch(Exception err)
		{
			System.out.println("ERROR " + err.toString());
		}
		
		AppLog.debug("Header: " + Auxiliary.bytesToHex3(header));
		
		return get(header);
	}

	/**
	 * Converts the header bytes of a record into a field of SQLite elements.
	 * Exactly one element is created per column type. 
	 * @param header
	 * @return
	 */
	private SqliteElement[] get(byte[] header) {
		// there are several varint values in the serialtypes header
		int[] columns = Auxiliary.readVarInt(header);
		if (null == columns)
			return null;

		SqliteElement[] column = new SqliteElement[columns.length];

		for (int i = 0; i < columns.length; i++) {

			switch (columns[i]) {
			case 0: // primary key or null value <empty> cell
				column[i] = new SqliteElement(SerialTypes.PRIMARY_KEY,StorageClass.INT, 0);
				break;
			case 1: // 8bit complement integer
				column[i] = new SqliteElement(SerialTypes.INT8,StorageClass.INT, 1);
				break;
			case 2: // 16bit integer
				column[i] = new SqliteElement(SerialTypes.INT16,StorageClass.INT, 2);
				break;
			case 3: // 24bit integer
				column[i] = new SqliteElement(SerialTypes.INT24,StorageClass.INT, 3);
				break;
			case 4: // 32bit integer
				column[i] = new SqliteElement(SerialTypes.INT32,StorageClass.INT, 4);
				break;
			case 5: // 48bit integer
				column[i] = new SqliteElement(SerialTypes.INT48,StorageClass.INT, 6);
				break;
			case 6: // 64bit integer
				column[i] = new SqliteElement(SerialTypes.INT64,StorageClass.INT, 8);
				break;
			case 7: // Big-endian floating point number
				column[i] = new SqliteElement(SerialTypes.FLOAT64,StorageClass.FLOAT, 8);
				break;
			case 8: // Integer constant 0
				column[i] = new SqliteElement(SerialTypes.INT0,StorageClass.INT, 0);
				break;
			case 9: // Integer constant 1
				column[i] = new SqliteElement(SerialTypes.INT1,StorageClass.INT, 0);
				break;
			case 10: // not used ;

			case 11:
				columns[i] = 0;
				break;
			default:
				if (columns[i] % 2 == 0) // even
				{
					// BLOB with the length (N-12)/2
					column[i] = new SqliteElement(SerialTypes.BLOB,StorageClass.BLOB, (columns[i] - 12) / 2);
				} 
				else // odd
				{
					// String in database encoding (N-13)/2
					column[i] = new SqliteElement(SerialTypes.STRING,StorageClass.TEXT, (columns[i] - 13) / 2);					
				}

			}

		}

		return column;
	}

	

	/**
	 * This method reads a Varint value startRegion the transferred buffer.
	 * A varint has a length between 1 and 9 bytes. The MSB displays
	 * whether further bytes follow. If it is set to 1, then  
	 * at least one more byte can be read. 
	 * 
	 * @param buffer with varint value
	 * @return a normal integer value extracted startRegion the buffer
	 * @throws IOException
	 */
	public int readUnsignedVarInt(ByteBuffer buffer) throws IOException {
//		int value = 0;
//		int b;
//		int shift=0;
//
//		// as long as we have a byte with most significant bit value 1
//		// there are more byte to read
//		while (((b = buffer.get()) & 0x80) != 0) {
//
//			shift=7;
//			value |= (b & 0x7F) << shift;
//		}
//
//		return value | b;
//		
		return Auxiliary.readUnsignedVarInt(buffer);
		
	}

	/**
	 * Auxiliary method for reading one of a two-byte number in 
	 * a data field of type short.
	 * @param b
	 * @return
	 */
	public static int TwoByteBuffertoInt(ByteBuffer b) {
		short v = b.getShort();
		// value
		int iv = v >= 0 ? v : 0x10000 + v; // we have to convert
		// an unsigned short
		// value to an
		// integer

		return iv;
	}

	
	
//	private void checkROWID(int co, SqliteElement en,int rowid, StringBuffer lineUTF)
//	{
//		/* There is a ROWID column ?*/
//	    if (co == 0 && en.length == 0)
//	    {
//	    	/*
//	    	 *  PRIMARY KEY COLUMN ALIASED ROWID
//	    	 *  From the SQLite documentation:
//	    	 *  rowid component has a primary key that consists of a single column and 
//	    	 *  the declared type of that column is "INTEGER" in any mixture of upper 
//	    	 *  and lower case, then the column becomes an alias for the rowid. 
//	    	 *	CREATE TABLE t(x INTEGER PRIMARY KEY ASC, y, z);
//	    	 *	CREATE TABLE t(x INTEGER, y, z, PRIMARY KEY(x ASC));
//	    	 *	CREATE TABLE t(x INTEGER, y, z, PRIMARY KEY(x DESC)); 
//	    	 *  
//	    	 *  Accordingly the first component column has length 0x00 and must be an INTEGER -> take the rowid instead;
//	    	 */
//	    	lineUTF.append(rowid);
//	    	//System.out.println("PRIMARY KEY COLUMN ALIASED ROWID");
//	    }	
//	}

}
