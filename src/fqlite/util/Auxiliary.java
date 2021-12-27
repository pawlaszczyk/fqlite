package fqlite.util;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicInteger;

import fqlite.base.Base;
import fqlite.base.Global;
import fqlite.base.Job;
import fqlite.base.SqliteElement;
import fqlite.descriptor.IndexDescriptor;
import fqlite.descriptor.TableDescriptor;
import fqlite.parser.SQLiteSchemaParser;
import fqlite.pattern.HeaderPattern;
import fqlite.pattern.IntegerConstraint;
import fqlite.types.SerialTypes;
import fqlite.types.StorageClasses;

/**
 * This class offers a number of useful methods that are needed from time to
 * time for the acquisition process.
 * 
 * @author pawlaszc
 *
 */
public class Auxiliary extends Base {

	public AtomicInteger found = new AtomicInteger();
	public AtomicInteger inrecover = new AtomicInteger();

	public static final String TABLELEAFPAGE = "0d";
	public static final String TABLEINTERIORPAGE = "05";
	public static final String INDEXLEAFPAGE = "0a";
	public static final String INDEXINTERIORPAGE = "02";
	public static final String OVERFLOWPAGE = "00";
	
	final protected static char[] hexArray = "0123456789abcdef".toCharArray();


	public Job job;

	/**
	 * Get the type of page. There are 4 different basic types in SQLite: (1)
	 * component-leaf (2) component-interior (3) indices-leaf and (4)
	 * indices-interior.
	 * 
	 * Beside this, we can further find overflow pages and removed pages. Both start
	 * with the 2-byte value 0x00.
	 * 
	 * @param content the page content as a String
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

		case OVERFLOWPAGE: // or dropped page
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
	 * Constructor. To return values to the calling job environment, an object
	 * reference of job object is required.
	 * 
	 * @param job
	 */
	public Auxiliary(Job job) {
		this.job = job;
	}

	/**
	 * An important step in data recovery is the analysis of the database schema.
	 * This method allows to read in the schema description into a ByteBuffer.
	 * 
	 * @param job
	 * @param start
	 * @param buffer
	 * @param header
	 * @throws IOException
	 */
	public boolean readMasterTableRecord(Job job, int start, ByteBuffer buffer, String header) throws IOException {

		SqliteElement[] columns;

		buffer.position(start);

		columns = toColumns(header);

		if (null == columns)
			return false;

		// use the header information to reconstruct
		int pll = computePayloadLength(header);

		int so;
		so = computePayload(pll);

//		int overflow = -1;
//		if (so < pll) {
//			int phl = header.length() / 2;
//
//			int last = buffer.position();
//			debug(" spilled payload ::" + so);
//			debug(" pll payload ::" + pll);
//			
//			if ((buffer.position() + so - phl - 1) > buffer.limit())
//				return false;
//			
//			buffer.position(buffer.position() + so - phl - 1);
//
//			overflow = buffer.getInt();
//			debug(" overflow::::::::: " + overflow + " " + Integer.toHexString(overflow));
//			/* regular overflow or rubbish value? */
//			if (overflow >job.file.size())
//				return false;
//			buffer.position(last);
//
//			/*
//			 * we need to increment page number by one since we start counting with zero for
//			 * page 1
//			 */
//			byte[] extended = readOverflow(overflow - 1);
//
//			byte[] c = new byte[pll + job.ps];
//
//			buffer.position(0);
//
//			/* method array() cannot be called, since we backed an array */
//			byte[] originalbuffer = new byte[job.ps];
//			for (int bb = 0; bb < job.ps; bb++) {
//				originalbuffer[bb] = buffer.get(bb);
//			}
//
//			buffer.position(last);
//
//			/* copy spilled overflow of current page into extended buffer */
//			System.arraycopy(originalbuffer, buffer.position(), c, 0, so - phl);
//			
//			/* append the rest startRegion the overflow pages to the buffer */
//			if (null != extended)
//				System.arraycopy(extended, 0, c, so - phl - 1, extended.length); // - so);
//			ByteBuffer bf = ByteBuffer.wrap(c);
//
//			buffer = bf;
//
//			// set original buffer pointer to the end of the spilled payload
//			// just before the next possible record
//			buffer.position(0);
//		}

		int con = 0;

		String tablename = null;
		int rootpage = -1;
		String statement = null;

		/* start reading the content */
		for (SqliteElement en : columns) {

			if (en == null) {
				continue;
			}

			byte[] value = null;

			if (con == 5)
				value = new byte[en.length];
			else
				value = new byte[en.length];

			try
			{
				buffer.get(value);
			}
			catch(BufferUnderflowException bue)
			{
				return false;
			}
			
			/* column 3 ? -> tbl_name TEXT */
			if (con == 3) {
				tablename = en.toString(value);
			}

			/* column 4 ? -> root page Integer */
			if (con == 4) {
				if (value.length == 0)
					debug("Seems to be a virtual component -> no root page ;)");
				else {
					rootpage = SqliteElement.decodeInt8(value[0]);
				}
			}

			/* read sql statement */

			if (con == 5) {
				statement = en.toString(value);
				break; // do not go further - we have everything we need
			}

			con++;

		}

		// finally, we have all information in place to parse the CREATE statement
		SQLiteSchemaParser.parse(job, tablename, rootpage, statement);

		return true;
	}

	/**
	 * This method is used to extract a previously deleted record in unused space a
	 * page.
	 * 
	 * @param start      the exact position (offset relative to the page start).
	 * @param buffer     a ByteBuffer with the data page to analyze.
	 * @param header     the record header bytes including header length and serial
	 *                   types.
	 * @param bs         a data structure that is used to record which areas have
	 *                   already been searched
	 * @param pagenumber the number of the page we going to analyze
	 * @return
	 * @throws IOException if something went wrong during read-up.
	 */
	public CarvingResult readDeletedRecord(Job job, int start, ByteBuffer buffer, String header, BitSet bs,
			int pagenumber) throws IOException {

		SqliteElement[] columns;

		buffer.position(start);

		int recordstart = start - (header.length() / 2);

		/* skip the header length byte */
		header = header.substring(2);

		columns = toColumns(header);

		if (null == columns)
			return null;

		StringBuffer lineUTF = new StringBuffer();
		int co = 0;
		String fp = null;
		try {
			fp = getTableFingerPrint(columns);

		} catch (NullPointerException err) {
			// System.err.println(err);
		}
		if (null == fp)
			fp = "unkown";

		boolean error = false;

		lineUTF.append(((pagenumber - 1) * job.ps + buffer.position()) + ";");

		/* use the header information to reconstruct */
		int pll = computePayloadLength(header);

		int so = computePayload(pll);

		int overflow = -1;

		if (so < pll) {
			int phl = header.length() / 2;

			int last = buffer.position();
			debug(" deleted spilled payload ::" + so);
			debug(" deleted pll payload ::" + pll);
			buffer.position(buffer.position() + so - phl - 1);

			overflow = buffer.getInt();
			debug(" deleted overflow::::::::: " + overflow + " " + Integer.toHexString(overflow));
			buffer.position(last);

			ByteBuffer bf;

			/* is the overflow page value correct ? */
			if (overflow > 0 && overflow  < job.numberofpages) {

				/*
				 * we need to increment page number by one since we start counting with zero for
				 * page 1
				 */
				byte[] extended = readOverflow(overflow - 1);

				byte[] c = new byte[pll + job.ps];

				buffer.position(0);
				byte[] originalbuffer = new byte[job.ps];
				for (int bb = 0; bb < job.ps; bb++) {
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

				byte[] value = new byte[en.length];

				bf.get(value);

				lineUTF.append(write(co, en, value));

				co++;
			}

			// set original buffer pointer to the end of the spilled payload
			// just before the next possible record
			buffer.position(last + so - phl - 1);

		} else {

			for (SqliteElement en : columns) {

				if (en == null) {
					continue;
				}

				byte[] value = new byte[en.length];
				if ((buffer.position() + en.length) > buffer.limit()) {
					error = true;
					return null;
				}
				buffer.get(value);

				lineUTF.append(write(co, en, value));

				co++;

			}

			if (error)
				return null;
		}

		/* mark bytes as visited */
		bs.set(recordstart, buffer.position() - 1, true);
		debug("visited :: " + recordstart + " bis " + buffer.position());
		int cursor = ((pagenumber - 1) * job.ps) + buffer.position();
		debug("visited :: " + (((pagenumber - 1) * job.ps) + recordstart) + " bis " + cursor);

		/* append header match string at the end */
		lineUTF.append("##header##" + header);

		lineUTF.append("\n");

		// if (!tables.containsKey(idxname))
		// tables.put(idxname, new ArrayList<String[]>());
		debug(lineUTF.toString());
		return new CarvingResult(buffer.position(), cursor, lineUTF);
	}

	/**
	 * Convert a UTF16-String into an UTF8-String.
	 * 
	 * @param s
	 * @return
	 */
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
	 * 
	 * @param col   column number
	 * @param en    the element to convert
	 * @param value the actual value
	 * @return the converted StringBuffer value
	 */
	private StringBuffer write(int col, SqliteElement en, byte[] value) {

		StringBuffer val = new StringBuffer();

		if (col > 0)
			val.append(";");

		val.append(en.toString(value));

		return val;
	}

	/**
	 * This method can be used to read an active data record.
	 * 
	 * A regular cell has the following structure
	 * 
	 * [Cell Size / Payload][ROW ID] [Header Size][Header Columns] [Data] varint
	 * varint varint varint ...
	 * 
	 * We only need to parse the headerbytes including the serial types of each
	 * column. Afterwards we can read each data cell of the tablerow and convert
	 * into an UTF8 string.
	 * 
	 * 
	 **/
	public String readRecord(int cellstart, ByteBuffer buffer, int pagenumber_db, BitSet bs, int pagetype,
			int maxlength, StringBuffer firstcol, boolean withoutROWID, int filepointer) throws IOException {

		boolean unkown = false;

		// first byte of the buffer
		buffer.position(0);

		// prepare the string for the return value
		StringBuffer lineUTF = new StringBuffer();

		/* For WAL and ROL files the values is always greater than 0 */
		if (filepointer > 0) {
			if (job.pages.length > pagenumber_db) {
				if (null != job.pages[pagenumber_db]) {

					lineUTF.append(job.pages[pagenumber_db].getName() + ";");
				}
			} else {
				lineUTF.append("__UNASSIGNED;");
				// unkown=true;
			}

			lineUTF.append(Global.REGULAR_RECORD + ";");
			lineUTF.append(filepointer + cellstart + ";");

		} else
		/* first, add component name if known */
		if (null != job.pages[pagenumber_db]) {
			lineUTF.append(job.pages[pagenumber_db].getName() + ";");
			lineUTF.append(Global.REGULAR_RECORD + ";");

			/*
			 * for a regular db-file the offset is derived from the page number, since all
			 * pages are a multiple of the page size (ps).
			 */
			lineUTF.append((pagenumber_db - 1) * job.ps + cellstart + ";");

		} else {
			/*
			 * component is not part of btree - page on free list -> need to determine name
			 * of component
			 */
			unkown = true;
		}

		// length of payload as varint
		buffer.position(cellstart);
		int pll = readUnsignedVarInt(buffer);
		debug("Length of payload int : " + pll + " as hex : " + Integer.toHexString(pll));

		if (pll < 4)
			return null;

		int rowid = 0;

		/* Do we have a ROWID component or not? 95% of SQLite Tables have a ROWID */
		if (!withoutROWID) {

			if (unkown) {
				rowid = readUnsignedVarInt(buffer);
				debug("rowid: " + Integer.toHexString(rowid));
			} else {
				if (pagenumber_db >= job.pages.length) {

				} else if (null == job.pages[pagenumber_db] || job.pages[pagenumber_db].ROWID) {
					// read rowid as varint
					rowid = readUnsignedVarInt(buffer);
					debug("rowid: " + Integer.toHexString(rowid));
					// We do not use this key in the moment.
					// However, we have to read the value.
				}

			}
		}

		// now read the header length as varint
		int phl = readUnsignedVarInt(buffer);

		/* error handling - if header length is 0 */
		if (phl == 0) {
			return null;
		}

		debug("Header Length int: " + phl + " as hex : " + Integer.toHexString(phl));

		phl = phl - 1;

		/*
		 * maxlength field says something about the maximum bytes we can read before in
		 * unallocated space, before we reach the cell content area (ppl + rowid header
		 * + data). Note: Sometimes the data record is already partly overwritten by a
		 * regular data record. We have only an artifact and not a complete data record.
		 * 
		 * For a regular data field startRegion the content area the value of maxlength
		 * should be INTEGER.max_value 2^32
		 */
		// System.out.println(" bufferposition :: " + buffer.position() + " headerlength
		// " + phl );
		maxlength = maxlength - phl; // - buffer.position();

		// read header bytes with serial types for each column
		// Attention: this takes most of the time during a run
		SqliteElement[] columns;

		if (phl == 0)
			return null;

		int pp = buffer.position();
		String hh = getHeaderString(phl, buffer);
		buffer.position(pp);

		columns = getColumns(phl, buffer, firstcol);

		if (null == columns) {
			debug(" No valid header. Skip recovery.");
			return null;
		}

		int co = 0;
		try {
			if (unkown) {
				
				TableDescriptor td = matchTable(columns);

				/* this is only neccessesary, when component name is unkown */
				if (null == td)
					lineUTF.append("__UNASSIGNED" + ";");
				else {
					lineUTF.append(td.tblname + ";");					
					job.pages[pagenumber_db] = td;
				}

				lineUTF.append(Global.REGULAR_RECORD + ";");
				lineUTF.append((pagenumber_db - 1) * job.ps + cellstart + ";");

			}
		} catch (NullPointerException err) {
			System.err.println(err);
		}

		boolean error = false;

		int so = computePayload(pll);

		int overflow = -1;

		if (so < pll) {
			int last = buffer.position();
			debug("regular spilled payload ::" + so);
			if ((buffer.position() + so - phl - 1) > (buffer.limit() - 4)) {
				return null;
			}
			try {
				buffer.position(buffer.position() + so - phl - 1);
				overflow = buffer.getInt();
			} catch (Exception err) {
				return null;
			}

			if (overflow < 0)
				return null;
			debug("regular overflow::::::::: " + overflow + " " + Integer.toHexString(overflow));
			buffer.position(last);

			/*
			 * we need to increment page number by one since we start counting with zero for
			 * page 1
			 */
			byte[] extended = readOverflow(overflow - 1);

			byte[] c = new byte[pll + job.ps];

			buffer.position(0);
			byte[] originalbuffer = new byte[job.ps];
			for (int bb = 0; bb < job.ps; bb++) {
				originalbuffer[bb] = buffer.get(bb);
			}

			buffer.position(last);
			/* copy spilled overflow of current page into extended buffer */
			System.arraycopy(originalbuffer, buffer.position(), c, 0, so - phl);
			/* append the rest startRegion the overflow pages to the buffer */
			try {
				if (null != extended)
					System.arraycopy(extended, 0, c, so - phl, pll - so);
			} catch (ArrayIndexOutOfBoundsException err) {
				System.out.println("Error IndexOutOfBounds");
			} catch (NullPointerException err2) {
				System.out.println("Error NullPointer in ");
			}

			ByteBuffer bf = ByteBuffer.wrap(c);
			bf.position(0);

			co = 0;
			/* start reading the content */
			for (SqliteElement en : columns) {
				if (en == null) {
					lineUTF.append(";NULL");
					continue;
				}

				if (!withoutROWID)
					checkROWID(co, en, rowid, lineUTF);

				byte[] value = new byte[en.length];

				if ((bf.limit() - bf.position()) < value.length) {
					System.out.println(
							" Bufferunderflow " + (bf.limit() - bf.position()) + " is lower than" + value.length);
				}

				try {
					bf.get(value);
					lineUTF.append(write(co, en, value));

				} catch (java.nio.BufferUnderflowException bue) {
					err("readRecord():: overflow" + "java.nio.BufferUnderflowException");
				}

				co++;
			}

			// set original buffer pointer to the end of the spilled payload
			// just before the next possible record
			buffer.position(last + so - phl - 1);

		} else {
			/*
			 * record is not spilled over different pages - no overflow, just a regular
			 * record
			 *
			 * start reading the content
			 */
			co = 0;

			/*
			 * there is a max length set - because we are in the unallocated space and may
			 * not read beyond the content area start
			 */
			for (SqliteElement en : columns) {
				if (en == null) {
					lineUTF.append(";");
					continue;
				}

				if (!withoutROWID)
					checkROWID(co, en, rowid, lineUTF);

				byte[] value = null;
				if (maxlength >= en.length)
					value = new byte[en.length];
				else if (maxlength > 0)
					value = new byte[maxlength];
				maxlength -= en.length;

				if (null == value)
					break;

				try {
					buffer.get(value);
				} catch (BufferUnderflowException err) {
					System.out.println("readRecord():: no overflow ERROR " + err);
					// err.printStackTrace();
					return null;
				}

				lineUTF.append(write(co, en, value));

				co++;

				if (maxlength <= 0)
					break;
			}

		}

		/* append header match string at the end */
		lineUTF.append("##header##" + hh);

		lineUTF.append("\n");

		/* mark as visited */
		debug("visted " + cellstart + " bis " + buffer.position());
		bs.set(cellstart, buffer.position());

		if (error) {
			err("spilles overflow page error ...");
			return "";
		}
		debug(lineUTF.toString());
		return lineUTF.toString();
	}

	private TableDescriptor matchTable(SqliteElement[] header) {

		Iterator<TableDescriptor> itds = job.headers.iterator();
		while (itds.hasNext()) {
			TableDescriptor table = itds.next();

			if (table.getColumntypes().size() == header.length) {
				int idx = 0;
				boolean eq = true;

				// System.out.println("TDS component " + table.tblname + " :: " +
				// table.columntypes.toString());

				// List<String> tblcolumns = table.columntypes;

				for (SqliteElement s : header) {
					String type = table.getColumntypes().get(idx);

					// System.out.println(s.serial.name() + " <?>" + type);
					if (!s.serial.name().equals(type)) {
						eq = false;
						break;
					}
					idx++;
				}

				if (eq) {

					// System.out.println(" Match found " + table.tblname);
					return table;
				}
			}
		}

		return null;
	}

	/**
	 * Reads the specified page as overflow.
	 * 
	 * Background: When the payload of a record is to large, it is spilled onto
	 * overflow pages. Overflow pages form a linked list. The first four bytes of
	 * each overflow page are a big-endian integer which is the page number of the
	 * next page in the chain, or zero for the final page in the chain. The fifth
	 * byte through the last usable byte are used to hold overflow content.
	 *
	 *
	 * @param pagenumber
	 * @return all bytes that belong to the payload
	 */
	private byte[] readOverflow(int pagenumber) {
		byte[] part = null;

		/* read the next overflow page startRegion file */
		if (pagenumber > job.ps)
		   return null;
		ByteBuffer overflowpage = job.readPageWithNumber(pagenumber, job.ps);

		int overflow = 0;
		if (overflowpage != null) {
			overflowpage.position(0);
			overflow = overflowpage.getInt();
			debug(" overflow:: " + overflow);
		} else {
			return null;
		}

		if (overflow == 0) {
			// termination condition for the recursive callup's
			debug("No further overflow pages");
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
		// System.out.println("current ::" + current.length);
		// System.out.println("bytes:: " + (job.ps -4));
		// System.out.println("overflowpage :: " + overflowpage.limit());

		overflowpage.position(4);
		overflowpage.get(current, 0, job.ps - 4);

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
	 * This method can be used to compute the payload length for a record, which pll
	 * field is deleted or overwritten.
	 * 
	 * @param header should be the complete header without headerlength byte
	 * @return the number of bytes including header and payload
	 */
	public int computePayloadLength(String header) {
		// System.out.println("HEADER" + header);
		byte[] bcol = Auxiliary.decode(header);

		int[] columns = readVarInt(bcol);
		int pll = 0;

		pll += header.length() / 2 + 1;

		for (int i = 0; i < columns.length; i++) {
			switch (columns[i]) {
			case 0: // zero length - primary key is saved in indices component
				break;
			case 1:
				pll += 1; // 8bit complement integer
				break;
			case 2: // 16bit integer
				pll += 2;
				break;
			case 3: // 24bit integer
				pll += 3;
				break;
			case 4: // 32bit integer
				pll += 4;
				break;
			case 5: // 48bit integer
				pll += 6;
				break;
			case 6: // 64bit integer
				pll += 8;
				break;
			case 7: // Big-endian floating point number
				pll += 8;
				break;
			case 8: // Integer constant 0
				break;
			case 9: // Integer constant 1
				break;
			case 10: // not used ;
				break;
			case 11:
				break;

			default:
				if (columns[i] % 2 == 0) // even
				{
					// BLOB with the length (N-12)/2
					pll += (columns[i] - 12) / 2;
				} else // odd
				{
					// String in database encoding (N-13)/2
					pll += (columns[i] - 13) / 2;
				}

			}

		}

		return pll;
	}

	/**
	 * 
	 * @param header
	 * @return
	 */
	public static SqliteElement[] toColumns(String header) {
		/* hex-String representation to byte array */
		byte[] bcol = Auxiliary.decode(header);
		return get(bcol);
	}

	public String getHeaderString(int headerlength, ByteBuffer buffer) {
		byte[] header = new byte[headerlength];

		try {
			// get header information
			buffer.get(header);

		} catch (Exception err) {
			System.out.println("ERROR " + err.toString());
		}

		String sheader = Auxiliary.bytesToHex(header);

		return sheader;
	}

	/**
	 * A passed ByteBuffer is converted into a byte array. Afterwards it is used to
	 * extract the column types. Exactly one element is created per column type.
	 * 
	 * @param headerlength total length of the header in bytes
	 * @param buffer       the headerbytes
	 * @return the column field
	 * @throws IOException
	 */
	public SqliteElement[] getColumns(int headerlength, ByteBuffer buffer, StringBuffer firstcol) throws IOException {

		byte[] header = new byte[headerlength];

		try {
			// get header information
			buffer.get(header);

		} catch (Exception err) {
			System.out.println("Auxiliary::ERROR " + err.toString());
			return null;
		}

		String sheader = Auxiliary.bytesToHex(header);

		if (sheader.length() > 1) {
			firstcol.insert(0, sheader.substring(0, 2));
		}
		// System.out.println("getColumns():: + Header: " + sheader);

		return get(header);
	}

	/**
	 * Converts the header bytes of a record into a field of SQLite elements.
	 * Exactly one element is created per column type.
	 * 
	 * @param header
	 * @return
	 */
	private static SqliteElement[] get(byte[] header) {
		// there are several varint values in the serialtypes header
		int[] columns = readVarInt(header);
		if (null == columns)
			return null;

		SqliteElement[] column = new SqliteElement[columns.length];

		for (int i = 0; i < columns.length; i++) {

			switch (columns[i]) {
			case 0: // primary key or null value <empty> cell
				column[i] = new SqliteElement(SerialTypes.PRIMARY_KEY, StorageClasses.INT, 0);
				break;
			case 1: // 8bit complement integer
				column[i] = new SqliteElement(SerialTypes.INT8, StorageClasses.INT, 1);
				break;
			case 2: // 16bit integer
				column[i] = new SqliteElement(SerialTypes.INT16, StorageClasses.INT, 2);
				break;
			case 3: // 24bit integer
				column[i] = new SqliteElement(SerialTypes.INT24, StorageClasses.INT, 3);
				break;
			case 4: // 32bit integer
				column[i] = new SqliteElement(SerialTypes.INT32, StorageClasses.INT, 4);
				break;
			case 5: // 48bit integer
				column[i] = new SqliteElement(SerialTypes.INT48, StorageClasses.INT, 6);
				break;
			case 6: // 64bit integer
				column[i] = new SqliteElement(SerialTypes.INT64, StorageClasses.INT, 8);
				break;
			case 7: // Big-endian floating point number
				column[i] = new SqliteElement(SerialTypes.FLOAT64, StorageClasses.FLOAT, 8);
				break;
			case 8: // Integer constant 0
				column[i] = new SqliteElement(SerialTypes.INT0, StorageClasses.INT, 0);
				break;
			case 9: // Integer constant 1
				column[i] = new SqliteElement(SerialTypes.INT1, StorageClasses.INT, 0);
				break;
			case 10: // not used ;
				columns[i] = 0;
				break;
			case 11:
				// columns[i] = 0;
				break;
			default:
				if (columns[i] % 2 == 0) // even
				{
					// BLOB with the length (N-12)/2
					column[i] = new SqliteElement(SerialTypes.BLOB, StorageClasses.BLOB, (columns[i] - 12) / 2);
				} 
				else // odd
				{
					// String in database encoding (N-13)/2
					column[i] = new SqliteElement(SerialTypes.STRING, StorageClasses.TEXT, (columns[i] - 13) / 2);
				}

			}

		}

		return column;
	}

	/**
	 * Computes the amount of payload that spills onto overflow pages.
	 * 
	 * @param p Payload size
	 * @return
	 */
	private int computePayload(int p) {

		// let U is the usable size of a database page,
		// the total page size less the reserved space at the end of each page.
		int u = job.ps;
		// x represents the maximum amount of payload that can be stored directly
		int x = u - 35;
		// m represents the minimum amount of payload that must be stored on the btree
		// page
		// before spilling is allowed
		int m = ((u - 12) * 32 / 255) - 23;

		int k = m + ((p - m) % (u - 4));

		// case 1: all P bytes of payload are stored directly on the btree page without
		// overflow.
		if (p <= x)
			return p;
		// case 2: first K bytes of P are stored on the btree page and the remaining P-K
		// bytes are stored on overflow pages.
		if ((p > x) && (k <= x))
			return k;
		// case 3: then the first M bytes of P are stored on the btree page and the
		// remaining P-M bytes are stored on overflow pages.
		if ((p > x) && (k > x))
			return m;

		return p;
	}

	/**
	 * This method reads a Varint value startRegion the transferred buffer. A varint
	 * has a length between 1 and 9 bytes. The MSB displays whether further bytes
	 * follow. If it is set to 1, then at least one more byte can be read.
	 * 
	 * @param buffer with varint value
	 * @return a normal integer value extracted startRegion the buffer
	 * @throws IOException
	 */
	public static int readUnsignedVarInt(ByteBuffer buffer) throws IOException {
		int value = 0;
		int b;
		int shift = 0;

		// as long as we have a byte with most significant bit value 1
		// there are more byte to read
		while (((b = buffer.get()) & 0x80) != 0) {

			shift = 7;
			value |= (b & 0x7F) << shift;

		}

		return value | b;
	}

	/**
	 * Auxiliary method for reading one of a two-byte number in a data field of type
	 * short.
	 * 
	 * @param b
	 * @return
	 */
	public static int TwoByteBuffertoInt(ByteBuffer b) {
	
		
		byte [] ret = new byte[4];
		ret[0] = 0;
		ret[1] = 0;
		ret[2] = b.get(0);
		ret[3] = b.get(1);
		
		
		return ByteBuffer.wrap(ret).getInt();
	}

	private void checkROWID(int co, SqliteElement en, int rowid, StringBuffer lineUTF) {
		/* There is a ROWID column ? */
		if (co == 0 && en.length == 0) {
			/*
			 * PRIMARY KEY COLUMN ALIASED ROWID From the SQLite documentation: rowid
			 * component has a primary key that consists of a single column and the declared
			 * type of that column is "INTEGER" in any mixture of upper and lower case, then
			 * the column becomes an alias for the rowid. CREATE TABLE t(x INTEGER PRIMARY
			 * KEY ASC, y, z); CREATE TABLE t(x INTEGER, y, z, PRIMARY KEY(x ASC)); CREATE
			 * TABLE t(x INTEGER, y, z, PRIMARY KEY(x DESC));
			 * 
			 * Accordingly the first component column has length 0x00 and must be an INTEGER
			 * -> take the rowid instead;
			 */
			lineUTF.append(rowid);
			// System.out.println("PRIMARY KEY COLUMN ALIASED ROWID");
		}
	}

	// Function to find the
	// Nth occurrence of a character
	public static int findNthOccur(String str, char ch, int N) {
		int occur = 0;

		// Loop to find the Nth
		// occurence of the character
		for (int i = 0; i < str.length(); i++) {
			if (str.charAt(i) == ch) {
				occur += 1;
			}
			if (occur == N)
				return i;
		}
		return -1;
	}

	/**
	 * This method will create and assign a pattern object for matching header
	 * information from a given index entry on binary level.
	 * 
	 * @param id
	 */
	public static void addHeadPattern2Idx(IndexDescriptor id) {
		List<String> colnames = id.columnnames;
		List<String> coltypes = id.columntypes;

		/* create a pattern object for constrain matching of records */
		HeaderPattern pattern = new HeaderPattern();

		/* the pattern always starts with a header constraint */
		pattern.addHeaderConstraint(colnames.size() + 1, colnames.size() + 1);

		/*
		 * map the correct constraint object to a column type for all columns within the
		 * index component
		 */
		ListIterator<String> list = coltypes.listIterator();
		while (list.hasNext()) {

			switch (list.next()) {
			case "INT":
				pattern.add(new IntegerConstraint(false));
				break;

			case "TEXT":
				pattern.addStringConstraint();
				break;

			case "BLOB":
				pattern.addBLOBConstraint();
				break;

			case "REAL":

				pattern.addFloatingConstraint();
				break;

			case "NUMERIC":
				pattern.addNumericConstraint();
				break;
			}
		}

		/* assign the new matching pattern with the index descriptor object */
		id.hpattern = pattern;

		System.out.println("addHeaderPattern2Idx() :: PATTTERN: " + pattern);

	}

	public static String bytesToHex(byte[] bytes) {

		StringBuilder sb = new StringBuilder();
		for (byte hashByte : bytes) {

			int intVal = 0xff & hashByte;
			if (intVal < 0x10) {
				sb.append('0');
			}
			sb.append(Integer.toHexString(intVal));
		}
		return sb.toString();

	}

	public static String byteToHex(byte b) {
		byte[] ch = new byte[1];
		ch[0] = b;
		return bytesToHex(ch);
	}
	
	public static String bytesToHex(ByteBuffer bb)
	{
		int limit = bb.limit();
		char[] hexChars = new char[limit * 2];
		
		bb.position(0);
		int counter = 0;
		
		while (bb.position() < limit)
		{
		
			int v = bb.get() & 0xFF;
			hexChars[counter * 2] = hexArray[v >>> 4];
			hexChars[counter * 2 + 1] = hexArray[v & 0x0F];
			counter++;
		}
		
		return new String(hexChars);
	}
	
	public static String bytesToHex(byte[] bytes, int fromidx, int toidx) {
		char[] hexChars = new char[(toidx - fromidx + 2) * 2];

		for (int j = 0; j < toidx; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
	}

	
	public static int[] readVarInt(byte[] values) {
		
		int value = 0;
		int counter = 0;
		byte b = 0;
		int shift = 0;
		
		LinkedList<Integer> res = new LinkedList<Integer>();

		while (counter < values.length) {

			b = values[counter];
			value = 0;
			while (((b = values[counter]) & 0x80) != 0) {

				shift=7;
				value |= (b & 0x7F) << shift;
				counter++;
				if (counter >= values.length) {
					return null;
				}
			}
			res.add(value | b);
			counter++;
		}
		int[] result = new int[res.size()];
		int n = 0;
		Iterator<Integer> it = res.iterator();
		while (it.hasNext()) {
			result[n] = it.next();
			n++;
		}
		return result;
	}
	
	public static String getSerial(SqliteElement[] columns) {
		String serial = "";

		for (SqliteElement e : columns)
			serial += e.serial;
		return serial;
	}

	public static String getTableFingerPrint(SqliteElement[] columns) {
		String fp = "";

		for (SqliteElement e : columns)
			fp += e.type;
		return fp;
	}

	static boolean contains(ByteBuffer bb, String searchText) {
		String text = new String(bb.array());
		if (text.indexOf(searchText) > -1)
			return true;
		else
			return false;
	}

	public static void printStackTrace() {
		System.out.println("Printing stack trace:");

		StackTraceElement[] elements = Thread.currentThread().getStackTrace();
		for (int i = 1; i < elements.length; i++) {
			StackTraceElement s = elements[i];
			System.out.println("\tat " + s.getClassName() + "." + s.getMethodName() + "(" + s.getFileName() + ":"
					+ s.getLineNumber() + ")");
		}
	}
	
	/**
	 * This method can be used to compute the payload length for a record, which pll
	 * field is deleted or overwritten.
	 * 
	 * @param header should be the complete header without headerlength byte
	 * @return the number of bytes including header and payload
	 */
	public static int computePayloadLengthS(String header) {
		System.out.println("HEADER" + header);
		byte[] bcol = Auxiliary.decode(header);
		int[] columns = Auxiliary.readVarInt(bcol);
		int pll = 0;

	
		pll += header.length() / 2 + 1;

		for (int i = 0; i < columns.length; i++) {
			switch (columns[i]) {
			case 0: // zero length - primary key is saved in indices component
				break;
			case 1:
				pll += 1; // 8bit complement integer
				break;
			case 2: // 16bit integer
				pll += 2;
				break;
			case 3: // 24bit integer
				pll += 3;
				break;
			case 4: // 32bit integer
				pll += 4;
				break;
			case 5: // 48bit integer
				pll += 6;
				break;
			case 6: // 64bit integer
				pll += 8;
				break;
			case 7: // Big-endian floating point number
				pll += 8;
				break;
			case 8: // Integer constant 0
				break;
			case 9: // Integer constant 1
				break;
			case 10: // not used ;
				break;
			case 11:
				break;

			default:
				if (columns[i] % 2 == 0) // even
				{
					// BLOB with the length (N-12)/2
					pll += (columns[i] - 12) / 2;
				} else // odd
				{
					// String in database encoding (N-13)/2
					pll += (columns[i] - 13) / 2;
				}

			}

		}

		return pll;
	}
	
	/**
	 * Computes the amount of payload that spills onto overflow pages.
	 * 
	 * @param p Payload size
	 * @return
	 */
	public static int computePayloadS(int p, int ps) {

		// let U is the usable size of a database page,
		// the total page size less the reserved space at the end of each page.
		int u = ps;
		// x represents the maximum amount of payload that can be stored directly
		int x = u - 35;
		// m represents the minimum amount of payload that must be stored on the btree
		// page
		// before spilling is allowed
		int m = ((u - 12) * 32 / 255) - 23;

		int k = m + ((p - m) % (u - 4));

		// case 1: all P bytes of payload are stored directly on the btree page without
		// overflow.
		if (p <= x)
			return p;
		// case 2: first K bytes of P are stored on the btree page and the remaining P-K
		// bytes are stored on overflow pages.
		if ((p > x) && (k <= x))
			return k;
		// case 3: then the first M bytes of P are stored on the btree page and the
		// remaining P-M bytes are stored on overflow pages.
		if ((p > x) && (k > x))
			return m;

		return p;
	}
	
	/**
	 * Conversion routine from a byte array to hex-String representation. 
	 * @param b
	 * @return
	 */
	public static String I2H(byte[] b) {
    
        char buf[] = new char[b.length * 3 - 1];
        int k = b[0] & 0xff;
        buf[0] = hexArray[k >>> 4];
        buf[1] = hexArray[k & 0xf];
        for (int i = 1; i < b.length; ) {
            k = b[i] & 0xff;
            //buf[i++] = ':';
            buf[i++] = hexArray[k >>> 4];
            buf[i++] = hexArray[k & 0xf];
        }
        
        return new String(buf);
    }
	
	/**
     * Converts and 4-Byte integer value into a hex-String. 
     * 
     */
	public static String Int2Hex(int i)
    {
    	 return Auxiliary.bytesToHex(new byte[] {(byte)(i >>> 24),(byte)(i >>> 16),(byte)(i >>> 8), (byte)i});
    }
    
    
}
