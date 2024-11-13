package fqlite.util;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.imageio.ImageIO;
import fqlite.base.BigByteBuffer;
import fqlite.base.GUI;
import fqlite.base.Global;
import fqlite.base.Job;
import fqlite.base.SqliteElement;
import fqlite.descriptor.AbstractDescriptor;
import fqlite.descriptor.IndexDescriptor;
import fqlite.descriptor.TableDescriptor;
import fqlite.log.AppLog;
import fqlite.parser.SQLiteSchemaParser;
import fqlite.pattern.HeaderPattern;
import fqlite.pattern.IntegerConstraint;
import fqlite.types.BLOBElement;
import fqlite.types.BLOBTYPE;
import fqlite.types.SerialTypes;
import fqlite.types.StorageClass;
import fqlite.types.TimeStamp;
import fqlite.ui.FQTableView;
import javafx.embed.swing.SwingFXUtils;
import javafx.scene.image.Image;

/**
 * This class offers a number of useful methods that are needed from time to
 * time for the acquisition process.
 * 
 * @author pawlaszc
 *
 */
public class Auxiliary{

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
			return 5;

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
	public boolean readMasterTableRecord(Job job, long start, BigByteBuffer buffer, String header) throws IOException {

		int mt = (int)start/job.ps;
		job.mastertable.add(mt+1);
		
		SqliteElement[] columns;

		columns = MasterRecordToColumns(header);

		if (null == columns)
			return false;

		String cl = header.substring(2);

		// use the header information to reconstruct pll info
		int pll = this.computePayloadLength(cl.substring(0, 12));

		int so;

		buffer.position(start + 8);
	
		// determine overflow that is not fitting
		so = computePayload(pll);

		int overflow = -1;

		/* Do we have overflow-page(s) ? */
		if (so < pll) {
			//int phl = header.length() / 2;

			long last = buffer.position();
	
			// get the last 4 byte of the first page -> this should contain the
			// page number of the first overflow page
			overflow = buffer.getInt(job.ps - 4);

			if (overflow > job.numberofpages)
				return false;

			/* remember all pages with schema information */
			job.mastertable.add(overflow);
			//System.out.println("Mastertables pages " + job.mastertable);
			
			/*
			 * we need to increment page number by one since we start counting with zero for
			 * page 1
			 */
			byte[] extended = readOverflow(job, overflow - 1);

			/* method array() cannot be called, since we backed an array */
			byte[] originalbuffer = new byte[job.ps];
			for (int bb = 0; bb < job.ps; bb++) {
				originalbuffer[bb] = buffer.get(bb);
			}

			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			outputStream.write(originalbuffer);
			outputStream.write(extended);

			byte c[] = outputStream.toByteArray();
			BigByteBuffer bf = BigByteBuffer.wrap(c);

			buffer = bf;

			// set original buffer pointer just before the record start of
			// table row
			buffer.position(last);
		}

		int con = 0;

		String objecttype = null;
		String namespace = null;
		String tablename = null;
		int rootpage = -1;
		String statement = null;

		boolean autoindex = false;
		
		/* start reading the content */
		for (SqliteElement en : columns) {

			if (en == null) {
				continue;
			}

			byte[] value = null;

			value = new byte[en.getlength()];

			try {
				// System.out.println("current pos" + buffer.position());
				buffer.get(value);
			} catch (BufferUnderflowException bue) {
				return false;
			}
			if (con == 0) {
				objecttype = en.toString(value, true, true);
			}
		    
			if (con == 1) {
				namespace = en.toString(value, true, true);
				/* look for autoindex */
				if(namespace.startsWith("sqlite_autoindex_")) {
 					/* 
 					 * UNIQUE and PRIMARY KEY constraints on tables cause SQLite to create internal indexes with names 
 					 * of the form "sqlite_autoindex_TABLE_N" where TABLE is replaced by the name of the table that 
 					 * contains the constraint and N is an integer beginning with 1 and increasing by one with each 
 					 * constraint seen in the table definition.
 					 */
					autoindex = true;
					//System.out.println("Found sqlite_autoindex_ - table" + namespace);
				}
			}
				

			/* column 3 ? -> tbl_name TEXT */
			if (con == 2) {
				tablename = en.toString(value, true, true);
				//System.out.println(" Tablename " + tablename);

			}

			/* column 4 ? -> root page Integer */
			if (con == 3) {
				if (value.length == 0)
					AppLog.debug("Seems to be a virtual component -> no root page ;)");
				else {
                    try {
						/* root page of table is decoded a BE integer value */
	
						if (en.type == SerialTypes.INT8)
							rootpage = SqliteElement.decodeInt8(value[0]);
						else if (en.type == SerialTypes.INT16)
							rootpage = SqliteElement.decodeInt16(new byte[] { value[0], value[1] });
						else if (en.type == SerialTypes.INT24)
							rootpage = SqliteElement.decodeInt24(new byte[] { value[0], value[1], value[2] });
						else if (en.type == SerialTypes.INT32)
							rootpage = SqliteElement.decodeInt32(new byte[] { value[0], value[1], value[2], value[3] });
						else 
							return false;
                    }catch(Exception err){
                    	return false;
                    }
                }
				
				if(autoindex) {
					
					createAutoIndexRecord(objecttype,namespace,tablename,rootpage);
				    
					break;
				}
			}

			/* read sql statement */

			if (con == 4) {
				statement = en.toString(value, true, true);
				//System.out.println(" SQL statement::" + statement);
				break; // do not go further - we have everything we need
			}

			con++;

		}
		
		if (!autoindex)
		{	// finally, we have all information in place to parse the CREATE statement
			SQLiteSchemaParser.parse(job, tablename, rootpage, statement);
		}
		else {
			autoindex=false;
		}
		
		job.mastertable.add(mt);
		return true;
	}
	

	
	private boolean createAutoIndexRecord(String objecttype,String namespace,String tablename, int rootpage){
		
		ArrayList<String> colnames = new ArrayList<String>();
		colnames.add("col1");
		
		IndexDescriptor ids = new IndexDescriptor(job,namespace,tablename,"",colnames);
		ids.root = rootpage;
 
		if (!job.indices.contains(ids))
		{	
			job.indices.add(ids);         
			ids.root = rootpage;
		}	
			
		return true;
	}
	



	/**
	 * This method is used to extract a previously deleted record in unused space a
	 * page.
	 * 
	 * @param buffer     a ByteBuffer with the data page to analyze.
	 * @param header     the record header bytes including header length and serial
	 *                   types.
	 * @param bs         a data structure that is used to record which areas have
	 *                   already been searched
	 * @param pagenumber the number of the page we going to analyze
	 * @return
	 * @throws IOException if something went wrong during read-up.
	 */
	public CarvingResult readDeletedRecordNew(Job job, ByteBuffer buffer, BitSet bs, Match m, Match next,
			int pagenumber, String fallback) throws IOException {

		//System.out.println("readDeletedRecordNew() - Entry");
		
		LinkedList<String> record = new LinkedList<String>();
		List<SqliteElement> columns;
		int rowid = -1;

		/* set the pointer directly after the header */
		buffer.position(m.end);

		//int co = 0;
		
		/**
		 * CASE 1: We have a found a complete but deleted record let us try to read the
		 * ROWID of this record if possible
		 **/
		if (m.match.startsWith("RI")) {
			//System.out.println(" need to determine rowid " + m.end);

			// need to check plausibility
			String withoutRI = m.match.substring(2);
			String headerlengthbyte = withoutRI.substring(0, 2);

			int headerlength = varintHexString2Integer(headerlengthbyte); // Integer.valueOf(headerlengthbyte);
			// System.out.println(" >>" + headerlength);

			/* check if header length byte is correct - otherwise wrong match */
			if (headerlength != (withoutRI.length() / 2))
				return null;
			else {
				// go back - before the offset of the header and
				// try to read the rowid byte
				buffer.position(m.end - headerlength - 4);

				byte[] beforeheader = new byte[4];
				buffer.get(beforeheader);

				int[] values = readVarInt(beforeheader);

				int rd = -1;
				if (values != null && values.length > 0) {
					rd = values[values.length - 1];
					//System.out.println("rowid ??? " + rd);
					rowid = rd;

				}

			}
			// remove rowid - flag
			m.match = m.match.replace("RI", "");
			// set position to the correct begin again
			buffer.position(m.end);
		}

		/* skip the header length byte */
		String header = m.match.substring(2);

		/**
		 * CASE 2: Partial header only with first (int) column overwritten?
		 * 
		 */
		if (header.startsWith("XX")) {

			String match = header.substring(2);

			int headerlength = match.length() / 2;

			// go back - before the offset of the header and
			// try to read the 2-Byte value for free block length
			buffer.position(m.end - headerlength - 2);

			byte[] freeblockinfo = new byte[2];
			buffer.get(freeblockinfo);
			buffer.position(buffer.position() - 2);

			short lg = buffer.getShort(); // 1

			if (null != next) {

				int nextmatch = next.begin;

				if (nextmatch < m.begin + lg) {

					lg = (short) (lg - (short) (m.begin + lg - (next.begin)));
				}

			}

			/* | 4 bytes | header with serial types | payload(data)| */
			/* compute the length of the missing first column */

			int first = lg - 4 - headerlength - getPayloadLength(match);
			//System.out.println("length of 1st column " + first);
			//System.out.println(" lg " + lg + " minus 4 " + "minus " + headerlength + " minus " + getPayloadLength(match));
			//System.out.println(" first ::" + first);

			if (first < 0) {
				if (header.startsWith("XX")) {
					header = "02" + header.substring(4);
				}
			} else if (first >= 0 && first <= 6) {
				String repl = Integer.toHexString(first);
				if (repl.length() % 2 != 0)
					repl = "0" + repl;
				header = header.replace("XX", repl);
			} else { // fallback strategy
				header = header.replace("XX", "02");// fallback);
				//System.out.println("fallback strategy" + header);

			}
			buffer.position(m.end);
		}

		/* get column types and length for each single column */
		columns = toColumns(header);

		/* in case of an error -> skip */
		if (null == columns) {
			AppLog.debug(" no valid header-string: " + header);
			return null;
		}
	
		/*
		 * use the header information to reconstruct the payload length, since this
		 * information is normally lost during delete
		 */

		int pll = computePayloadLength(header);

		int so = computePayload(pll);

		boolean error = false;

		/* in the output string we first start with the offset */
		record.add((pagenumber - 1) * job.ps + m.begin + "");

		int overflow = -1;

		/* Do we have overflow-page(s) ? */
		if (so < pll) {
			int phl = header.length() / 2;

			int last = buffer.position();
			AppLog.debug(" deleted spilled payload ::" + so);
			AppLog.debug(" deleted pll payload ::" + pll);
			buffer.position(buffer.position() + so - phl - 1);

			overflow = buffer.getInt();
			AppLog.debug(" deleted overflow::::::::: " + overflow + " " + Integer.toHexString(overflow));
			buffer.position(last);

			ByteBuffer bf;

			/* is the overflow page value correct ? */
			if (overflow > 0 && overflow < job.numberofpages) {

				/*
				 * we need to increment page number by one since we start counting with zero for
				 * page 1
				 */
				byte[] extended = readOverflowIterativ(overflow - 1, false);

				byte[] c = new byte[pll + job.ps];

				buffer.position(0);
				byte[] originalbuffer = new byte[job.ps];
				for (int bb = 0; bb < job.ps; bb++) {
					originalbuffer[bb] = buffer.get(bb);
				}

				buffer.position(last);
				bf = null;

				/* copy spilled overflow of current page into extended buffer */
				System.arraycopy(originalbuffer, buffer.position(), c, 0, so + 7); // - phl
				/* append the rest startRegion the overflow pages to the buffer */
				try {
					if (null != extended)
						// copy every byte from extended (beginning with index 0) into byte-array c, at
						// position so-phl
						System.arraycopy(extended, 0, c, so - phl - 1, pll - so);
					bf = ByteBuffer.wrap(c);
				} catch (ArrayIndexOutOfBoundsException err) {
					System.out.println("Error IndexOutOfBounds");
				} catch (NullPointerException err2) {
					System.out.println("Error NullPointer in ");
				}

			} else {
				pll = so;
				bf = buffer;
			}
			bf.position(0);
			int blobcolidx = 0;

			/* start reading the content */
			for (SqliteElement en : columns) {
				if (en == null) {
					continue;
				}

				byte[] value = new byte[en.getlength()];

				bf.get(value);
				if (en.serial == StorageClass.BLOB) {
					
					//if(isVT);
					
					String tablecelltext = en.getBLOB(value,true);
					
					if(tablecelltext.length() > 0) 
				    {
						record.add("[BLOB-" + blobcolidx + "] " + tablecelltext + "..");	
						storeBLOB(record, blobcolidx, tablecelltext,value,2);
						blobcolidx++;
				    }
				    else{
						record.add("");	
				    }

				} else {
					record.add(en.toString(value, false, true));
				}
				//co++;
			}

			// set original buffer pointer to the end of the spilled payload
			// just before the next possible record
			buffer.position(last + so - phl - 1);

		} else {

			if (pll < 42) {
				int blobcolidx = 0;
				int number = -1;
				for (SqliteElement en : columns) {

					number++;

					if (en == null) {
						continue;
					}

					byte[] value = new byte[en.getlength()];
					if ((buffer.position() + en.getlength()) > buffer.limit()) {
						error = true;
						return null;
					}

					if (rowid >= 0 && en.getlength() == 0 && m.rowidcolum >= 0) {
						if (m.rowidcolum == number) {
							record.add(rowid + "");
							//co++;
							continue;
						}
					}

					buffer.get(value);
					if (en.serial == StorageClass.BLOB) {
				
						//if(isVT);

						String tablecelltext = en.getBLOB(value,true);
					    if(tablecelltext.length() > 0) 
					    {
					    	String vv = "[BLOB-" + blobcolidx + "] " + tablecelltext;
					        if(tablecelltext.length() > 32)
					        	vv +=  "..";
							record.add(vv);	
							storeBLOB(record, blobcolidx, tablecelltext,value,0);
							blobcolidx++;
					    }
					    else{
							record.add("");	
					    }   
					    
					} 
					else
						record.add(en.toString(value, false, true));

					
					
					
					//co++;

				}

			} else {
				/* partial data record? -> check length */
				int nextrecord = bs.nextSetBit(buffer.position());
				boolean partial = false;
				int blobcolidx = 0;

				// no overflow - try to recover at least a partial record

				int cc = 0;
				for (SqliteElement v : columns) {

					if (partial) {
						record.add("");
						continue;
					}

					SqliteElement en = v;

					if (en == null) {
						continue;
					}

					if (rowid >= 0 && en.getlength() == 0 && m.rowidcolum >= 0) {
						if (m.rowidcolum == cc) {
							record.add(rowid + "");
							//co++;
							
							continue;
						}
					}

					byte[] value = new byte[en.getlength()];
					if ((buffer.position() + en.getlength()) > buffer.limit()) {
						error = true;
						return null;
					}

					if (nextrecord == -1)
						nextrecord = job.ps;

					/* partial data record? -> check length */
					if (((buffer.position() + en.getlength()) > nextrecord)) {
						/* we can only recover some bytes but not all of the given column */
						
						int oldlength = en.getlength();
						int newlength = en.getlength() - (buffer.position() + en.getlength() - nextrecord);
						
						if (newlength < oldlength) {
							en = en.clone(en, newlength);
							  
						}
						
						// System.out.println("Rest-Bytes:: " + en.length + "");
						if (en.getlength() > 0) {
							/* a partial restore is only possible for columns like STRING and BLOB */
							if (en.type == SerialTypes.BLOB || en.type == SerialTypes.STRING) {
								value = new byte[en.getlength()];
								buffer.get(value);
								// record.add(en.toString(value,false));

								if (en.serial == StorageClass.BLOB) {
								
									//if(isVT);
									
									String tablecelltext = en.getBLOB(value,true);
								    if(tablecelltext.length() > 0) 
								    {
										record.add("[BLOB-" + blobcolidx + "] " + tablecelltext + "..");	
										storeBLOB(record, blobcolidx, tablecelltext,value,0);
										blobcolidx++;
								    }
								    else{
										record.add("");	
								    }

								} else {
									record.add(en.toString(value, false, true));
								}

							}
						}
						partial = true;
						continue;
					} else {

						buffer.get(value);

						if (en.serial == StorageClass.BLOB) {
							
							//if(isVT);
							
							String tablecelltext = en.getBLOB(value,true);
						    if(tablecelltext.length() > 0) 
						    {
								record.add("[BLOB-" + blobcolidx + "] " + tablecelltext + "..");	
								storeBLOB(record, blobcolidx, tablecelltext,value,0);
								blobcolidx++;
						    }
						    else{
								record.add("");	
						    }

						    
						    
						    
						} else {
							
							
							record.add(en.toString(value, false, true));
						}

					}

					//co++;
					cc++;
				}
			}

			if (error)
				return null;
		}

		/* mark bytes as visited */
		bs.set(m.end, buffer.position(), true);
		AppLog.debug("visited :: " + m.end + " bis " + buffer.position());
		long cursor = ((pagenumber - 1L) * job.ps) + buffer.position();
		AppLog.debug("visited :: " + (((pagenumber - 1L) * job.ps) + m.end) + " bis " + cursor);

		record.add(0, "[" + pll + "|" + header.length() / 2 + "]");

		//record.add(0, "" + pll);
		
		//record.add(1, "" + header.length() / 2);
		record.add(1, "" + rowid);

		//int hl = (header.length() / 2) - 1;
		
		//if (columns.size() > (hl -1)) {
		//	System.out.println("columns " + columns.size() + " header -> " + (header.length() / 2));
			//return null;
		//}
		
		return new CarvingResult(buffer.position(), cursor, new StringBuffer(), record);
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
	public StringBuffer write(int col, SqliteElement en, byte[] value) {

		StringBuffer val = new StringBuffer();

		if (col > 0)
			val.append(";");

		val.append(en.toString(value, false, true));

		return val;
	}

	/**
	 * Check for weather the table name belongs to a virtual table 
	 * or not.
	 * 
	 * @param tblname
	 * @return
	 */
	private boolean isVirtualTable(String tblname){
		int p;
		if ((p = tblname.indexOf("_node")) > 0){
			String tbln = tblname.substring(0, p);
			return job.virtualTables.containsKey(tbln);
		}
		return false;
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
	public LinkedList<String> readRecord(int cellstart, ByteBuffer buffer, int pagenumber_db, BitSet bs, int pagetype,
			int maxlength, StringBuffer firstcol, boolean withoutROWID, int filetype, long offset) throws IOException {

		
		/* we use a list to hold the fields of the data record to read */
		LinkedList<String> record = new LinkedList<String>();

		/* true, if table a therefore record structure of page is unkown */
		boolean unkown = false;
		boolean isVT = false;		
		int rowid_col = -1;

		// first byte of the buffer
		buffer.position(0);

		// the concrete table information
		TableDescriptor td = null;
		AbstractDescriptor ad = null;

		/* For WAL and ROL files the values is always greater than 0 */
		if (filetype == Global.ROLLBACK_JOURNAL_FILE || filetype == Global.WAL_ARCHIVE_FILE) {

			if (pagenumber_db < job.pages.length && null != job.pages[pagenumber_db]) {

				ad = job.pages[pagenumber_db];
				if (ad instanceof TableDescriptor) {
					td = (TableDescriptor) ad;
				}			
				isVT = isVirtualTable(ad.getName());
				record.add(job.pages[pagenumber_db].getName());
			} 
			else {
				record.add(Global.DELETED_RECORD_IN_PAGE);
			}

			record.add(Global.REGULAR_RECORD);
			
			if (offset > -1)
				record.add(offset + "");
			else
				record.add(cellstart + "");
			
			if (pagenumber_db < job.pages.length && null != job.pages[pagenumber_db]) {
				rowid_col = job.pages[pagenumber_db].rowid_col;
			}
				
		} else
		/* first, add component name if known */
		if (null != job.pages[pagenumber_db]) {

			ad = job.pages[pagenumber_db];
			if (ad instanceof TableDescriptor) {
				td = (TableDescriptor) ad;
				isVT = isVirtualTable(ad.getName());
			}
			record.add(ad.getName());
			record.add(Global.REGULAR_RECORD);

			/*
			 * for a regular db-file the offset is derived from the page number, since all
			 * pages are a multiple of the page size (ps).
			 */
			record.add((((pagenumber_db - 1L) * job.ps) + cellstart) + "");
			
			rowid_col = job.pages[pagenumber_db].rowid_col;

		} else {
			/*
			 * component is not part of btree - page on free list -> need to determine name
			 * of component
			 */
			unkown = true;
		}

		AppLog.debug("cellstart for pll: " + (((pagenumber_db - 1L) * job.ps) + cellstart));
		
		// length of payload as varint
		try {
			buffer.position(cellstart);
		} catch (Exception err) {
			System.err.println("ERROR: cellstart not in buffer" + cellstart + " pagenumber_db " + (pagenumber_db - 1L) + " page size " + job.ps);
			return null;
		}
		int pll = readUnsignedVarInt(buffer);
		AppLog.debug("Length of payload int : " + pll + " as hex : " + Integer.toHexString(pll));

		if (pll < 4)
			return null;

		int rowid = 0;

		/* Do we have a ROWID component or not? 95% of SQLite Tables have a ROWID */
		if (!withoutROWID) {

			if (unkown) {
				rowid = readUnsignedVarInt(buffer);
				AppLog.debug("rowid: " + Integer.toHexString(rowid));
			} else {
				if (pagenumber_db >= job.pages.length) {

				} 
				else if (null == job.pages[pagenumber_db] || job.pages[pagenumber_db].ROWID) {
					// read rowid as varint
					rowid = readUnsignedVarInt(buffer);
					AppLog.debug("rowid: " + Integer.toHexString(rowid));
					
				}

			}
		}

		// now read the header length as varint
		int phl = readUnsignedVarInt(buffer);

		/* error handling - if header length is 0 */
		if (phl == 0) {
			return null;
		}

		AppLog.debug("Header Length int: " + phl + " as hex : " + Integer.toHexString(phl));

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
		maxlength = maxlength - phl; // - buffer.position();

		// read header bytes with serial types for each column
		// Attention: this takes most of the time during a run
		List<SqliteElement> columns;

		if (phl == 0)
			return null;

		int pp = buffer.position();
		String hh = getHeaderString(phl, buffer);
		buffer.position(pp);

		columns = getColumns(phl, buffer);

		if (null == columns) {
			AppLog.debug(" No valid header. Skip recovery.");
			return null;
		}

		int co = 0;
		try {
		
			// What about WAL-Archive ????
			if (unkown) {

				td = matchTable(columns);

				/* this is only necessary, when component name is unknown */
				if (null == td) {
					record.add(Global.DELETED_RECORD_IN_PAGE);
				} else {
					record.add(td.tblname);
					job.pages[pagenumber_db] = td;
					isVT = isVirtualTable(ad.getName());
					rowid_col = td.rowid_col;
				}

				record.add(Global.REGULAR_RECORD);
				record.add((((pagenumber_db - 1L) * job.ps) + cellstart) + "");
			}
		} catch (NullPointerException err) {
			System.err.println(err);
		}

		boolean error = false;

		int so = computePayload(pll);

		int overflow = -1;

		if (so < pll) {
			int last = buffer.position();
			AppLog.debug("regular spilled payload ::" + so);
			if ((buffer.position() + so - phl - 1) > (buffer.limit() - 4)) {
				return null;
			}
			try {
				/* read overflow */
				buffer.position(buffer.position() + so - phl - 1);
				overflow = buffer.getInt();
				//System.out.println("Overflow page for db page " + pagenumber_db + " overflow " + overflow  + " 1. overflow page to read");
			} catch (Exception err) {
				return null;
			}

			if (overflow < 0)
				return null;
			AppLog.debug("regular overflow::::::::: " + overflow + " " + Integer.toHexString(overflow));
			/*
			 * This is a regular overflow page - remember this decision & don't analyze this
			 * page
			 */
			buffer.position(last);

			byte[] extended;
			if (filetype == Global.WAL_ARCHIVE_FILE) {
				// write ahead log  overflow
				System.out.println("pll: " + pll);
				extended = readOverflowIterativ(overflow , true);
			}
			else {
				/*
				 * we need to decrement page number by one since we start counting with zero for
				 * page 1
				 */
				
				// regular overflow
				extended = readOverflowIterativ(overflow - 1, false);
			}
			
			byte[] c = new byte[pll + job.ps];

			buffer.position(0);
			byte[] originalbuffer = new byte[job.ps];
			for (int bb = 0; bb < job.ps; bb++) {
				originalbuffer[bb] = buffer.get(bb);
			}

			buffer.position(last);
			/* copy spilled overflow of current page into extended buffer */
			if(so-phl > 0)
				System.arraycopy(originalbuffer, buffer.position(), c, 0, so - phl); // - phl
			/* append the rest startRegion the overflow pages to the buffer */
			try {
				if (null != extended)
					// copy every byte from extended (beginning with index 0) into byte-array c, at
					// position so-phl
					System.arraycopy(extended, 0, c, so - phl - 1, pll - so);
			} catch (ArrayIndexOutOfBoundsException err) {
				System.out.println("Error IndexOutOfBounds");
			} catch (NullPointerException err2) {
				System.out.println("Error NullPointer in ");
			}

			/* now we have the complete overflow in one byte-array */
			ByteBuffer bf = ByteBuffer.wrap(c);
			bf.position(0);

			co = 0;
			int blobcolidx = 0;

			/* start reading the content of each column */
			for (SqliteElement en : columns) {
				
			
				if (en == null) {
					record.add("NULL");
					continue;
				}

				
				if (rowid_col == co) {
					if (!withoutROWID) {
						record.add(rowid + "");
						co++;
						continue;
					}
				}
				
				
				byte[] value = new byte[en.getlength()];

				if ((bf.limit() - bf.position()) < value.length) {
					System.out.println(
							" Bufferunderflow " + (bf.limit() - bf.position()) + " is lower than" + value.length);
				}

				
				try {
					
					bf.get(value);
					
				} catch (BufferUnderflowException err) {
					System.out.println("readRecord():: buffer underflow ERROR " + err);
					return null;
				}	
			
				if (en.serial == StorageClass.BLOB) {
					
					String tablecelltext;
					
					if(isVT)
						/* do not trunc the hex-string to 32 if the record belongs to a
						 * virtual table
						 */
						tablecelltext = en.getBLOB(value,false);
					else
						/*
						 * standard case: trunc blob string to only the first 32 bytes
						 */
					    tablecelltext = en.getBLOB(value,true);
				
					if(tablecelltext.length() > 0) 
				    {
						if (isVT)
							System.out.println(" Table cell text length " + tablecelltext.length());
						record.add("[BLOB-" + blobcolidx + "] " + tablecelltext + "..");	
						storeBLOB(record, blobcolidx, tablecelltext,value,2);
						blobcolidx++;
				    }
				    else{
						record.add("");	
				    }
						

				} else {

						String vv = null;
						boolean istimestamp = false;

						if (td != null) {
							
							
							if (co < td.serialtypes.size()) {
								String coltype = td.sqltypes.get(co);
							
								if (coltype.equals("TIMESTAMP")) {

									TimeStamp ts = timestamp2String(en, value);
									if (null != ts) {
										vv = ts.text;									
										istimestamp = true;
										//System.out.println("Update Timestamps :: " + vv + " ->  " + found);
										job.timestamps.put(vv, found);

									}
								}


							}
							
						} else {
							/* no table description (td) avail. -> take the raw-value */
							record.add(en.toString(value, false, true));
							continue;
						}
						
					
						if (!istimestamp) {
							if (en.type == SerialTypes.PRIMARY_KEY)
							{	
								en.toString(value,false,true);
								vv = null;
							}
							else	
								vv = en.toString(value, false, true);
							
						}
					
					
						
						
						/* whether or not timestamp -> write value to table model */
						if (null != vv)
							record.add(vv);
						else
							record.add("null");
						
					}

					co++;

					if (maxlength <= 0)
						break;
				
			}
			
			//System.out.println("$$$$$" + record);

		} else {
			/*
			 * record is not spilled over different pages - no overflow, just a regular
			 * record
			 *
			 * start reading the content
			 */
			co = 0;
			int blobcolidx = 0;

			/*
			 * there is a max length set - because we are in the unallocated space and may
			 * not read beyond the content area start
			 */
			for (SqliteElement en : columns) {
				
				
				if (en == null) {
					record.add("NULL");
					continue;
				}

				if (rowid_col == co) {
					if (!withoutROWID) {
						record.add(rowid + "");
						co++;
						continue;
					}
				}

				byte[] value = null;
				if (maxlength >= en.getlength()) {
				    if (en.getlength() < 0)
				    	value = new byte[0];
				    else
				    	value = new byte[en.getlength()];
				}
				maxlength -= en.getlength();

				if (null == value)
					break;

				try {
					buffer.get(value);
				} catch (BufferUnderflowException err) {
					System.out.println("readRecord():: buffer underflow ERROR " + err);
					return null;
				}
				

				if (en.serial == StorageClass.BLOB) {

					String tablecelltext; //= en.getBLOB(value,true);
				    
					if(isVT)
						/* do not trunc the hex-string to 32 if the record belongs to a
						 * virtual table
						 */
						tablecelltext = en.getBLOB(value,false);
					else
						/*
						 * standard case: trunc blob string to only the first 32 bytes
						 */
					    tablecelltext = en.getBLOB(value,true);
				
					
					
					
					if(tablecelltext.length() > 0) 
				    {
						record.add("[BLOB-" + blobcolidx + "] " + tablecelltext + "..");	
						storeBLOB(record, blobcolidx, tablecelltext,value,2);
						blobcolidx++;
						
						
						
				    }
				    else{
						record.add("");	
				    }
											

				} else {

					String vv = null;
					boolean istimestamp = false;

					if (td != null) {
						
						if (co < td.serialtypes.size()) {
							String coltype = td.sqltypes.get(co);
							if (coltype.equals("TIMESTAMP")) {

								
								TimeStamp ts = timestamp2String(en, value);
								if (null != ts) {
									vv = ts.text;									
									istimestamp = true;
		
								}
							}

						}
						
					} else {
						/* no table description (td) avail. -> take the raw-value */
						record.add(en.toString(value, false, true));
						continue;
					}
					
				
					if (!istimestamp) {
						if (en.type == SerialTypes.PRIMARY_KEY)
						{	
							en.toString(value,false, true);
							vv = null;
						}
						else	
							vv = en.toString(value, false, true);
						
					}
				
				
					
					
					/* whether or not timestamp -> write value to table model */
					if (null != vv)
						record.add(vv);
					else
						record.add("null");
					
				}

				co++;

				if (maxlength <= 0)
					break;
			}

		}

		/* append header match string at the end */
		record.add(1, "[" + pll + "|" + hh.length() / 2 + "]");
		//record.add(2, "" + hh.length() / 2);	
		record.add(2, "" + rowid);
		
		/* mark as visited */
		if (so < pll) {
			bs.set(cellstart, cellstart + so + 4);
		} else {
			bs.set(cellstart, buffer.position());
		}
		if (error) {
			AppLog.error("spilles overflow page error ...");
			return null;
			
		}
	

		/* the offset is derived from the name of the table plus primary key / rowid */
//		String archivekey = getPrimaryKey(td,record);
 	    //System.out.println("Archiv Key::" + archivekey);
		
		
		if (ad == null)
    	{	
			// every record that has no table descriptor assigned should be shown in the __FREELIST table. 
    		record.set(0,"__FREELIST");
    	}		
		
        /*  Is a Rollback journal file present ? */
//		if (filetype == Global.ROLLBACK_JOURNAL_FILE && null != ad && !unkown && !withoutROWID) {
//		
//			/* first occurrence of this records or already existing ? */
//			if(job.LineHashes.containsKey(archivekey)){
//		    	
//		    	Integer originalhash = job.LineHashes.get(archivekey);
//		    	Integer journalhash = computeHash(record);
//		    	
//		    	/* hash of line in database differs from hash in journal file */
//		    	if(!originalhash.equals(journalhash))
//		    	{
//		    		record.set(3,"");
//		    	}
//		    }
//		    else {
//		     	System.out.println("removed record at offset " + archivekey);
//		     	record.set(3,"");
//		    }
//		
//		}
//		
		
		
		/* Is a WAL archive file present ? */
//		if (filetype == Global.WAL_ARCHIVE_FILE && !unkown && !withoutROWID && null != ad) { 
//			
//			    
//		    if(job.TimeLineHashes.containsKey(archivekey)){
//		    	    	
//			    	LinkedList<Version> versions = job.TimeLineHashes.get(archivekey);
//			      
//			    	Integer originalhash = computeHash(record);
//			    	Integer journalhash = versions.getFirst().hash;
//				    	
//		    	if(!originalhash.equals(journalhash))
//		    	{
//		    	
//		    	    record.set(3,"");
//
//		    	}
//		    	else
//		    	{
//		    	    record.set(3,"");
//		    	    //record.set(3,String.format("%03d", lvs)  + ". version" + " (no change)");
//		    	  	//System.out.println("new change record for primary key " + archivekey);
//			     	//System.out.println(record);
//			   
//		    	}
//		    	
//		    	versions.addFirst(new Version(record));
//		    }
//		    else {
//	    	    record.set(3,"");
//		     	LinkedList<Version> recordlist = new LinkedList<Version>();
//		     	recordlist.add(new Version(record));
//		     	job.TimeLineHashes.put(archivekey,recordlist);
//		    }
//			
//			
//			
//		}
//
//		System.out.println("$$$$ " + record);
//		
		return record;
	}
	
	private void storeBLOB(LinkedList<String> record, int blobcolidx, String tablecelltext, byte[] value, int offsetidx) {
	
	Long hash;
	/* by default the record offset is used as key for a BLOB */
	if (record.get(offsetidx).length() > 2)
		hash = Long.parseLong(record.get(offsetidx));
	else
		hash = (long) blobcolidx;

	
	// example shash: /Users/pawlaszc/.fqlite/wickrLocal.sqlite_17633-0.bin

	String shash = GUI.baseDir + Global.separator + job.filename + "_" + hash + "-" + blobcolidx;
		
		if (tablecelltext.contains("png")){
			shash += ".png";
			job.bincache.put(shash, new BLOBElement(value, BLOBTYPE.PNG));
		}
		else if (tablecelltext.contains("gif"))
		{
			shash += ".gif";
			job.bincache.put(shash, new BLOBElement(value, BLOBTYPE.GIF));
		}
		else if (tablecelltext.contains("bmp"))
		{
			shash += ".bmp";
			job.bincache.put(shash, new BLOBElement(value, BLOBTYPE.BMP));
		}
		else if (tablecelltext.contains("jpg"))
		{
			shash += ".jpg";
			job.bincache.put(shash, new BLOBElement(value, BLOBTYPE.JPG));
		}
		else if (tablecelltext.contains("tiff"))
		{
			shash += ".tiff";
			job.bincache.put(shash, new BLOBElement(value, BLOBTYPE.TIFF));
		}
		else if (tablecelltext.contains("heic"))
		{
			shash += ".heic";
			job.bincache.put(shash, new BLOBElement(value, BLOBTYPE.HEIC));
		}
		else if (tablecelltext.contains("pdf"))
		{
			shash += ".pdf";
			job.bincache.put(shash, new BLOBElement(value, BLOBTYPE.PDF));
		}
		else if (tablecelltext.contains("plist"))
		{
			shash += ".plist";
			job.bincache.put(shash, new BLOBElement(value, BLOBTYPE.PLIST));
		}
		else if (tablecelltext.contains("gzip"))
		{
			shash += ".gzip";
			job.bincache.put(shash, new BLOBElement(value, BLOBTYPE.GZIP));							
		}
		else if (tablecelltext.contains("avro"))
		{
			shash += ".avro";
			job.bincache.put(shash, new BLOBElement(value, BLOBTYPE.AVRO));							
		}
		else
		{
			shash += ".bin";
			job.bincache.put(shash, new BLOBElement(value, BLOBTYPE.UNKOWN));
		}
		if (tablecelltext.contains("jpg") || tablecelltext.contains("png") || tablecelltext.contains("gif")
				|| tablecelltext.contains("bmp")) {
			/* for the tooltip preview we need to shrink the pictures */
			Image img = scaledown(value);
			
			if (null != img)
			{	
				job.Thumbnails.put(shash, img);
			}
		}
			  
	}

	
//	private String getPrimaryKey(TableDescriptor td, LinkedList<String> record){
//		
//		String key = null;
//		
//		if(null == td)
//			return null;
//		
//		List<Integer> pk  = td.primarykeycolumnnumbers;
//				
//		/* there exists one or even more explicitly defined primary key column */
//		if(null != pk && pk.size() > 0)
//		{
//			Iterator<Integer> pcol = pk.iterator();
//			StringBuffer sb = new StringBuffer();
//			while (pcol.hasNext()) {
//			    // create composite primary key tablename + 5 walframe columns 
//				int nxt = pcol.next();
//				if(6+nxt < record.size())
//					sb.append( record.get(6 + nxt));
//			}					
//		
//		    key = sb.toString();
//		       
//		}
//		
//		return key;
//	}
//	
	
	public static int computeHash(LinkedList<String> record){
		
		// we currently work on the original data base file within the readRecord() method		
		LinkedList<String> ll = new LinkedList<String>();
		// enumerate all line items starting with the first column just behind "offset" column (index 5)
		for (int zz = 5; zz < record.size(); zz++){
			ll.add(record.get(zz));
		}
	
		return ll.hashCode();
	}
	

	final static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy - HH:mm:ss Z");
	
	public static String int2Timestamp(String time){
			
		if ((time == null) || time.equals("") || time.equals("null"))  {
			return "";
		}
		
		long l;
		try {
			 l = Long.parseLong(time);
		}
		catch(Exception err){
			return "";
		}
		String timestamp = "";
	
		if(l > UNIX_MIN_SECONDS && l < UNIX_MAX_SECONDS)
		{
			ZonedDateTime utc = ZonedDateTime.ofInstant(Instant.ofEpochSecond(l), ZoneOffset.UTC);
		    timestamp = utc.format(formatter);
			//System.out.println(" Unix Epoch: " + l);
			//System.out.println(" UTC Epoch: " + timestamp);
			return timestamp;
		}
		
		if (l > MAC_MIN_DATE && l  < MAC_MAX_DATE)
		{
			long ut = (978307200 + (long) l);
			//System.out.println("isMacAbsoluteTime(): " + l + " unix " + ut);
			ZonedDateTime utc = ZonedDateTime.ofInstant(Instant.ofEpochSecond(ut), ZoneOffset.UTC);
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy - HH:mm:ss Z");
			timestamp = utc.format(formatter);
			//System.out.println("time: " + timestamp);
			return timestamp;
		}
		
		if (l > MAC_NANO_MIN_DATE && l  < MAC_NANO_MAX_DATE)
		{
			l = l / 1000000000;
			long ut = (978307200 + (long) l);
			ZonedDateTime utc = ZonedDateTime.ofInstant(Instant.ofEpochSecond(ut), ZoneOffset.UTC);
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy - HH:mm:ss Z");
			timestamp = utc.format(formatter);
			return timestamp;
		}
		
		if(l > UNIX_MIN_DATE && l < UNIX_MAX_DATE)
		{
			ZonedDateTime utc = ZonedDateTime.ofInstant(Instant.ofEpochMilli(l), ZoneOffset.UTC);
		    timestamp = utc.format(formatter);
			return timestamp;
		}
		
		if (l > UNIX_MIN_DATE_NANO && l  < UNIX_MAX_DATE_NANO)
		{
			l = l/1000; 
			ZonedDateTime utc = ZonedDateTime.ofInstant(Instant.ofEpochMilli(l), ZoneOffset.UTC);
			timestamp = utc.format(formatter);	
			return timestamp;
		}
		
		if (l > UNIX_MIN_DATE_PICO && l  < UNIX_MAX_DATE_PICO)
		{
			l = l/1000000; 
			ZonedDateTime utc = ZonedDateTime.ofInstant(Instant.ofEpochMilli(l), ZoneOffset.UTC);
			timestamp = utc.format(formatter);	
			return timestamp;
		}
		
		return timestamp;
	}
	
	
	/**
	 * Convert a SQL-TIMESTAMP value into a human readable format.
	 * 
	 * @param original
	 * @return
	 */
	private TimeStamp timestamp2String(SqliteElement en, byte[] value) {

		
		
		if (en.type == SerialTypes.INT48) {
			//System.out.println("INT48 zeit " + en.type);

			long l = SqliteElement.decodeInt48ToLong(value) / 100;
			long time = (978307200 + (long) l);
			ZonedDateTime utc = ZonedDateTime.ofInstant(Instant.ofEpochSecond(time), ZoneOffset.UTC);
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy - HH:mm:ss Z");
			String s = utc.format(formatter);
			return new TimeStamp(s, l);

		} else if (en.type == SerialTypes.INT64) {

			ByteBuffer bf = ByteBuffer.wrap(value);
			long l = bf.getLong();

			ZonedDateTime utc = ZonedDateTime.ofInstant(Instant.ofEpochSecond(l), ZoneOffset.UTC);
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy - HH:mm:ss Z");
			String s = utc.format(formatter);
			return new TimeStamp(s, l);
		} else if (en.type == SerialTypes.FLOAT64) {
			String s = "";
			double l = 0;
			try {
				ByteBuffer bf = ByteBuffer.wrap(value);
			
				l = bf.getDouble();
				
				long time = (978307200 + (long) l);
			   
		    	ZonedDateTime utc = ZonedDateTime.ofInstant(Instant.ofEpochSecond(time), ZoneOffset.UTC);
		    	DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy - HH:mm:ss Z");
		    	s = utc.format(formatter);
			
		    }catch(Exception err){
		    	AppLog.debug("DateTimeException: Instant exceeds minimum or maximum instant");
		        s = "";
		    }
			return new TimeStamp(s, l);

		} else if (en.type == SerialTypes.INT32) {

			ByteBuffer bf = ByteBuffer.wrap(value);
			try {
				int l = bf.getInt();
				long time = (978307200 + (long) l);
				ZonedDateTime utc = ZonedDateTime.ofInstant(Instant.ofEpochSecond(time), ZoneOffset.UTC);
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy - HH:mm:ss Z");
				String s = utc.format(formatter);
				return new TimeStamp(s, l);
			}catch(Exception err){
				
			}
		}
		
		
		return new TimeStamp("null", 0);
	}

	// UNIX Epoch in milliseconds since 1st January 1970 */
	final static long UNIX_MIN_SECONDS = 1262304000L; // 01.01.2010
	final static long UNIX_MAX_SECONDS = 2524608000L; // 01.01.2050
                                         
	/* UNIX Epoch in milliseconds since 1st January 1970 */
	final static long UNIX_MIN_DATE = 1262304000000L; // 01.01.2010
	final static long UNIX_MAX_DATE = 2524608000000L; // 01.01.2050

	/* UNIX Epoch in nanoseconds since 1st January 1970 */
	final static long UNIX_MIN_DATE_NANO = 1262304000000000L; // 01.01.2010
	final static long UNIX_MAX_DATE_NANO = 2524608000000000L; // 01.01.2050
	                                       
	final static long UNIX_MIN_DATE_PICO = 1262304000000000000L; // 01.01.2010
	final static long UNIX_MAX_DATE_PICO = 2524608000000000000L; // 01.01.2050	                                   	                                  	                                   	final static long UNIX_MIN_DATE_NANO = 1262304000000000L; // 01.01.2010
	                                       
	                              
	/* CFMACTime Stamps Seconds since 1st January 2001 */
	final static double MAC_MIN_DATE = 300000000; // 05. Jul. 2010
	final static double MAC_MAX_DATE = 800000000; // 10. Sep. 2064
                        
	/* CFMACTime Stamp in Nano Seconds (10^9) */
    final static long MAC_NANO_MIN_DATE = 300000000000000000L; // 05. Jul. 2010
    final static long MAC_NANO_MAX_DATE = 800000000000000000L; // 10. Sep. 2064
                                   	
                                       
	/* Google Chrome timestamps Microseconds since 1st January 1601 */
	final static long CHROME_MIN_DATE = 1L;

	/**
	 * Converts a CFAbsolute time stamp to String. This timestamp is expressed as a
	 * decimal number which is the number of seconds since midnight on 1st January
	 * 2001. However, this timestamp is shown with a decimal number that appears to
	 * do nothing. I'm sure there is a valid reason for it and it may well be an
	 * important level of accuracy or uniqueness in some circumstances. But for
	 * real-world purposes, simply ignoring everything after the decimal seems
	 * perfectly fine.
	 */

	protected TimeStamp Real2String(SqliteElement en, byte[] value) {

		if (value.length == 0)
			return null;

		ByteBuffer bf = ByteBuffer.wrap(value);
		System.out.println("value length " + value.length);
		double d = bf.getDouble();
		ZonedDateTime utc = null;
		System.out.print("CFMACTime " + d);

		if (d > MAC_MIN_DATE && d < MAC_MAX_DATE) {

			// seconds since midnight on 1st January 2001
			long l = Double.valueOf(d).longValue();

			System.out.print("CFMACTime as long" + l);

			// to Unix Epoch
			// l = l + 2082844800L;
			l += 978307200L;
			l = l * 1000;
			System.out.println(" summe " + l);
			utc = ZonedDateTime.ofInstant(Instant.ofEpochMilli(l), ZoneOffset.UTC);
			System.out.println("UTC " + utc);

			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy - HH:mm:ss Z");
			String s = utc.format(formatter);

			return new TimeStamp(s, d);

		}

		return null;
	}

	public TimeStamp Integer2String(SqliteElement en, byte[] value) {

		long l = -1;

		if (en.type == SerialTypes.INT32) {

			System.out.println("INT32 zeit " + en.type);
			ByteBuffer bf = ByteBuffer.wrap(value);
			l = getUnsignedInt(bf.getInt());

		}
		if (en.type == SerialTypes.INT48) {

			System.out.println("INT48 zeit " + en.type);
			l = SqliteElement.decodeInt48ToLong(value);

		}
		if (en.type == SerialTypes.INT64) {

			System.out.println("INT64 zeit " + en.type);

			ByteBuffer bf = ByteBuffer.wrap(value);
			l = bf.getLong();

		}

		int utc_type = 0;

		if (l > UNIX_MIN_DATE && l < UNIX_MAX_DATE) {
			utc_type = 1;
		} else if (l > UNIX_MIN_DATE_NANO && l < UNIX_MAX_DATE_NANO) {
			utc_type = 2;
		}

		if (utc_type > 0) {

			ZonedDateTime utc = null;
			if (utc_type == 1)
				utc = ZonedDateTime.ofInstant(Instant.ofEpochMilli(l), ZoneOffset.UTC);
			else if (utc_type == 2)
				utc = ZonedDateTime.ofInstant(Instant.ofEpochMilli(l / 1000), ZoneOffset.UTC);

			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy - HH:mm:ss Z");
			String s = utc.format(formatter);
			System.out.println(" Unix Epoch: " + l);
			System.out.println(" UTC Epoch: " + s);
			System.out.println("time: " + s);
			return new TimeStamp(s, l);
		}

		return new TimeStamp(String.valueOf(l), l);

	}

	public static long getUnsignedInt(int x) {
		return x & 0x00000000ffffffffL;
	}

	private TableDescriptor matchTable(List<SqliteElement> header) {

		Iterator<TableDescriptor> itds = job.headers.iterator();
		while (itds.hasNext()) {
			TableDescriptor table = itds.next();

			if (table.getColumntypes().size() == header.size()) {
				int idx = 0;
				boolean eq = true;

	
				for (SqliteElement s : header) {
					String type = table.getColumntypes().get(idx);

					if (!s.serial.name().equals(type)) {
						eq = false;
						break;
					}
					idx++;
				}

				if (eq) {

					return table;
				}
			}
		}

		return null;
	}

	private static Image scaledown(byte[] bf) {

		byte[] nbf = null;
		
		/* funny THREEMA messenger 1st byte different thing */
		if(bf[0] == 1)
		{
			nbf = Arrays.copyOfRange(bf, 1, bf.length);
		}	
		else 
			nbf = bf;
		
		InputStream is = new ByteArrayInputStream(nbf);
		BufferedImage image = null;
		try {
			// class ImageIO is able to read png, tiff, bmp, gif, jpeg formats
			try {
				image = ImageIO.read(is);
			}
			catch(EOFException eof){
				
				System.out.println("ERROR EOF reached");
			    image = null;
			}
			catch(javax.imageio.IIOException err){
				System.out.println("ERROR No image data present to read");
			    image = null;
			}
			
			if (image == null)
				return null;
			// scaling down to thumbnail
			java.awt.Image scaled = image.getScaledInstance(100, 100, java.awt.Image.SCALE_FAST);

			// we need a BufferdImage to convert the awt.Image to fx-graphics
			BufferedImage bimage = new BufferedImage(100, 100, image.getType());
			// Draw the image on to the buffered image
			Graphics2D bGr = bimage.createGraphics();
			// convert from awt.Image to BufferedImage
			bGr.drawImage(scaled, 0, 0, null);
			bGr.dispose();
			// convert from AWT to FX-Image
			Image ii = SwingFXUtils.toFXImage(bimage, null);
			return ii;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	 * @param pagenumber
	 * @return
	 *
	 */
	private byte[] readOverflowIterativ(int pagenumber, boolean fromWAL) {
	
		List<ByteBuffer> parts = new LinkedList<ByteBuffer>();
		boolean more = true;
		ByteBuffer overflowpage = null;
		int next = pagenumber;
		int frame = -1;
		int saved = -1;
        
		if (fromWAL){
			

			// WAL in memory file
			ByteBuffer db = job.wal.wal;
	
			// overflow pages until here 
			@SuppressWarnings("unused")
			HashMap<Integer,ByteBuffer> overflow = job.wal.overflow;
			
			// save current file pointer position
			saved = db.position();

			frame = saved / (job.ps +24);
			frame++; // go to the begin of the next frame/wal page
		
			
		}
		
		Set<Integer> visited = new HashSet<Integer>(); 
		
		while (more) {
			/* read next overflow page into buffer */
			if (fromWAL) {
				if(visited.contains(next))
				{
					System.out.println(" >>>> Cycle page already visited " + next);			
					more = false;
					break;
				}
				overflowpage = job.readWALOverflowPage(frame, next, job.ps, pagenumber);	
				visited.add(next);
			} 
			else {
				/* read from database */
				overflowpage = job.readPageWithNumber(next, job.ps);
			}
			if (overflowpage != null) {
				overflowpage.position(0);
				
				if (fromWAL) {
					next = overflowpage.getInt();
					
					if(next > 0)
						more = true;
					else
						more = false;
				}
				else {
					next = overflowpage.getInt() - 1;	
					AppLog.debug(" next overflow:: " + next);
				
					if(next >= 0)
						more = true;
					else 
						more = false;
				}
				
				
			} else {
				more = false;
				break;
			}

			/*
			 * we always crab the complete overflow-page minus the first four bytes - they
			 * are reserved for the (possible) next overflow page offset
			 **/
			byte[] current = new byte[job.ps - 4];
			overflowpage.position(4);
			overflowpage.get(current, 0, job.ps - 4);
			// Wrap a byte array into a buffer
			ByteBuffer part = ByteBuffer.wrap(current);
			parts.add(part);

			if (next < 0 ) { //|| next > job.numberofpages) {
				// termination condition for the recursive callup's
				AppLog.debug("No further overflow pages");
				//System.out.println("No further overflow pages");
				/* startRegion the last overflow page - do not copy the zero bytes. */
				more = false;
			}

		}
		
		if(fromWAL)
		{
			if(saved > 0)
				job.wal.wal.position(saved);
		}

		/* try to merge all the ByteBuffers into one array */
		if (parts == null || parts.size() == 0) {
		
			return ByteBuffer.allocate(0).array();
		
		
		} else if (parts.size() == 1) {
			
			return parts.get(0).array();
		
		} else {
			ByteBuffer fullContent = ByteBuffer.allocate(parts.stream().mapToInt(Buffer::capacity).sum());
			parts.forEach(fullContent::put);
			fullContent.flip();
			
			return fullContent.array();
		}

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

	public int computePLL(SqliteElement[] columns, int numberofcolumns, int headerlength) {
		int pll = 0;

		pll += headerlength;

		for (SqliteElement e : columns) {
			if (e.type == SerialTypes.BLOB || e.type == SerialTypes.STRING)
				pll += e.getlength() * 2; // since Java uses 2 Bytes to save one char (UTF16 with fixed length)
			else
				pll += e.getlength();
		}

		return pll;
	}

	/**
	 * This method can be used to compute the payload length for a record, which pll
	 * field is deleted or overwritten.
	 * 
	 * @param header should be the complete header without headerlength byte
	 * @return the number of bytes including header and payload
	 */
	public int computePayloadLength(String header) {
		//System.out.println("HEADER" + header);
		byte[] bcol = Auxiliary.decode(header);

		//int[] columns = readVarInt(bcol);
		
		VIntIter columns = VIntIter.wrap(bcol,1);
		int pll = 0;

		pll += header.length() / 2 + 1;

		while(columns.hasNext()) {
			int column = columns.next(); 
			switch (column) {
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
				if (column % 2 == 0) // even
				{
					// BLOB with the length (N-12)/2
					pll += (column - 12) / 2;
				} else // odd
				{
					// String in database encoding (N-13)/2
					pll += (column - 13) / 2;
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
	public static List<SqliteElement> toColumns(String header) {
		/* hex-String representation to byte array */
		byte[] bcol = Auxiliary.decode(header);
		return get(bcol);
	}

	private static SqliteElement[] MasterRecordToColumns(String header) {
		// skip header length byte information - the Master Table has always 6 columns
		if (header.startsWith("07") || header.startsWith("06"))
			header = header.substring(2);

		byte[] bcol = Auxiliary.decode(header);

		return getMaster(bcol);

	}

	public static int getPayloadLength(String header) {
		int sum = 0;
		List<SqliteElement> cols = toColumns(header);
		for (SqliteElement e : cols) {
			if (e != null)
				sum += e.getlength();
		}
		return sum;
	}

	public String getHeaderString(int headerlength, ByteBuffer buffer) {
		if(headerlength <= 0)
			return "";
		
		byte[] header = new byte[headerlength];

		try {
			// get header information
			buffer.get(header);

		} catch (Exception err) {
			System.out.println("ERROR " + err.toString());
			return "";
		}

		String sheader = bytesToHex3(header);
				
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
	public List<SqliteElement> getColumns(int headerlength, ByteBuffer buffer) throws IOException {

		if(headerlength < 0 || headerlength > 1024)
			return null;
		
		byte[] header = new byte[headerlength];

		try {
			// get header information
			buffer.get(header);

		} catch (Exception err) {
			System.out.println("Auxiliary::ERROR " + err.toString() + " headerlength " + headerlength + " buffer capacity " + buffer.capacity());
			return null;
		}

		return get(header);
	}

	private static SqliteElement[] getMaster(byte[] header) {

		int[] columns = readMasterHeaderVarInts(header);
		if (null == columns)
			return null;
		return getElements(columns);
	}

	
	static int getCounter = 0;
	
	/**
	 * Converts the header bytes of a record into a field of SQLite elements.
	 * Exactly one element is created per column type.
	 * 
	 * @param header
	 * @return
	 */
	private static List<SqliteElement> get(byte[] header) {
			
		VIntIter columns = VIntIter.wrap(header,3);	
		List<SqliteElement> result = getElements(columns);
		return result;
		
	}

	
	static final SqliteElement primkey = new SqliteElement(SerialTypes.PRIMARY_KEY, StorageClass.INT, 0);
	static final SqliteElement int8 = new SqliteElement(SerialTypes.INT8, StorageClass.INT, 1);
	static final SqliteElement int16 = new SqliteElement(SerialTypes.INT16, StorageClass.INT, 2);
	static final SqliteElement int24 = new SqliteElement(SerialTypes.INT24, StorageClass.INT, 3);
	static final SqliteElement int32 = new SqliteElement(SerialTypes.INT32, StorageClass.INT, 4);
    static final SqliteElement int48 = new SqliteElement(SerialTypes.INT48, StorageClass.INT, 6);
    static final SqliteElement int64 = new SqliteElement(SerialTypes.INT64, StorageClass.INT, 8);
    static final SqliteElement float64 =  new SqliteElement(SerialTypes.FLOAT64, StorageClass.FLOAT, 8);
    static final SqliteElement const0  =  new SqliteElement(SerialTypes.INT0, StorageClass.INT, 0);
    static final SqliteElement const1  = new SqliteElement(SerialTypes.INT1, StorageClass.INT, 0);
	
	private static SqliteElement[] getElements(int[] columns) {

		SqliteElement[] column = new SqliteElement[columns.length];

		for (int i = 0; i < columns.length; i++) {

			switch (columns[i]) {
			case 0: // primary key or null value <empty> cell
				column[i] = primkey;
				break;
			case 1: // 8bit complement integer
				column[i] = int8;
				break;
			case 2: // 16bit integer
				column[i] = int16;
				break;
			case 3: // 24bit integer
				column[i] = int24;
				break;
			case 4: // 32bit integer
				column[i] = int32;
				break;
			case 5: // 48bit integer
				column[i] = int48;
				break;
			case 6: // 64bit integer
				column[i] = int64;
				break;
			case 7: // Big-endian floating point number
				column[i] = float64;
				break;
			case 8: // Integer constant 0
				column[i] = const0;
				break;
			case 9: // Integer constant 1
				column[i] = const1;
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
					column[i] = new SqliteElement(SerialTypes.BLOB, StorageClass.BLOB, (columns[i] - 12) / 2);
				} else // odd
				{
					// String in database encoding (N-13)/2
					column[i] = new SqliteElement(SerialTypes.STRING, StorageClass.TEXT, (columns[i] - 13) / 2);
				}

			}

		}

		return column;
	}
	
	private static List<SqliteElement> getElements(VIntIter columns) {


		LinkedList <SqliteElement> column = new LinkedList<SqliteElement>();
		
		while (columns.hasNext()) {
              
			int value = columns.next();
			
			switch (value) {
			case 0: // primary key or null value <empty> cell
				column.add(primkey);
				break;
			case 1: // 8bit complement integer
				column.add(int8);
				break;
			case 2: // 16bit integer
				column.add(int16);
				break;
			case 3: // 24bit integer
				column.add(int24);
				break;
			case 4: // 32bit integer
				column.add(int32);
				break;
			case 5: // 48bit integer
				column.add(int48);
				break;
			case 6: // 64bit integer
				column.add(int64);
				break;
			case 7: // Big-endian floating point number
				column.add(float64);
				break;
			case 8: // Integer constant 0
				column.add(const0);
				break;
			case 9: // Integer constant 1
				column.add(const1);
				break;
			case 10: // not used ;
				
				break;
			case 11:
				
				break;
			default:
				if (value % 2 == 0) // even
				{
					// BLOB with the length (N-12)/2
					column.add(new SqliteElement(SerialTypes.BLOB, StorageClass.BLOB, (value - 12) / 2));
				} else // odd
				{
					// String in database encoding (N-13)/2
					column.add(new SqliteElement(SerialTypes.STRING, StorageClass.TEXT, (value - 13) / 2));
				}

			}

		}

		columns.setBack();
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
	public static int readUnsignedVarInt(ByteBuffer buffer) {

		byte b = buffer.get();
		int value = b & 0x7F;
		while ((b & 0x80) != 0) {
			b = buffer.get();
			value <<= 7;
			value |= (b & 0x7F);
		}

		return value;
	}

	/**
	 * Auxiliary method for reading one of a two-byte number in a data field of type
	 * short.
	 * 
	 * @param b
	 * @return
	 */
	public static int TwoByteBuffertoInt(ByteBuffer b) {

		byte[] ret = new byte[4];
		ret[0] = 0;
		ret[1] = 0;
		ret[2] = b.get(0);
		ret[3] = b.get(1);

		return ByteBuffer.wrap(ret).getInt();
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

	}

	/**
	 * Converts a byte array to Hex-String.
	 * 
	 * @param bytes
	 * @return
	 */
	public static String bytesToHex3(byte[] bytes) {

		return bytesToHex2(ByteBuffer.wrap(bytes));
		
	}

	/**
	 * Converts a single byte to a Hex-String.
	 * 
	 * @param b
	 * @return
	 */
	public static String byteToHex(byte b) {
		byte[] ch = new byte[1];
		ch[0] = b;
		return bytesToHex3(ch);
	}

	/**
	 * Converts the content of a ByteBuffer object into a Hex-String.
	 * 
	 * @param bb
	 * @return
	 */
	public static String bytesToHex2(ByteBuffer bb) {
		
		int limit = bb.limit();
		
		if (limit <= 0)
			return null;
		
		char[] hexChars = new char[limit * 2];

		bb.position(0);
		int counter = 0;

        
		while (bb.position() < limit) {

			int v = bb.get() & 0xFF;
			hexChars[counter * 2] = hexArray[v >>> 4];
			hexChars[counter * 2 + 1] = hexArray[v & 0x0F];
			counter++;
			
		}

		return new String(hexChars);
	}

	/**
	 * Converts specified range of the specified array into a Hex-String. The
	 * initial index of the range (from) must lie between zero and original.length,
	 * inclusive.
	 * 
	 * @param bytes
	 * @param fromidx
	 * @param toidx
	 * @return
	 */
	public static String bytesToHex1(byte[] bytes, int fromidx, int toidx) {
		char[] hexChars = new char[(toidx - fromidx + 2) * 2];

		for (int j = 0; j < toidx; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
	}

	static int[] resultset = new int[1000];
	static List<Integer> resultlist = new ArrayList<Integer>();
	
	 /**
     * Read an integer stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param array to read from
     * @return a array of all integers read
     *
     * If variable-length value does not terminate after 5 bytes have been read the complete result is
     * discarded.
     */
    public synchronized static int[] readVarInt_test(byte[] values) {
    	 	  
    	    int number = 0;
     
    		/* iterate over the complete value array byte wise*/
	    	for(int c = 0; c < values.length; c++) {
		        int value = 0;
		        int i = 0;
		        int b;
		        while (((b = values[c]) & 0x80) != 0) {
		            value |= (b & 0x7f) << i;
		            i += 7;
		            if (i > 28){
		               return Arrays.copyOfRange(resultset,0,number); // something went wrong while decoding a next varint
		            }
		        }
		        value |= b << i;
		        resultset[number] = (value >>> 1) ^ -(value & 1);
		        number++;
	    	}
	    	
	    	// return the sub-array
	    	return Arrays.copyOfRange(resultset,0,number);
    	
    }
    
    /**
     * Decodes a value using the variable-length encoding of SQLite.
     * @param bb
     * @return
     * @throws IllegalArgumentException
     */
     public static long readUnsigned(ByteBuffer bb) throws IllegalArgumentException {
        int a0 = bb.get() & 0xFF;
        if (a0 <= 240)
          return a0; 
        if (a0 <= 248) {
          int a1 = bb.get() & 0xFF;
          return (240 + 256 * (a0 - 241) + a1);
        } 
        if (a0 == 249) {
          int a1 = bb.get() & 0xFF;
          int a2 = bb.get() & 0xFF;
          return (2288 + 256 * a1 + a2);
        } 
        int bytes = a0 - 250 + 3;
        return readSignificantBits(bb, bytes);
      }
    
     private static long readSignificantBits(ByteBuffer bb, int bytes) {
        bytes--;
        long value = (bb.get() & 0xFF) << bytes * 8;
        while (bytes-- > 0)
          value |= (bb.get() & 0xFF) << bytes * 8; 
        return value;
     }
    
    /**
 	 * Read a variable length integer from the supplied ByteBuffer
 	 * 
 	 * @param in buffer to read from
 	 * @return the int value
 	 */
 	public static int[] readVarInt_alt(byte[] values) {

 		resultlist = new ArrayList<Integer>();
 		//int number = 0;
 		ByteBuffer bb = ByteBuffer.wrap(values);
 		do {
 			long value = readUnsigned(bb);
 			resultlist.add((int)value);
 		    //number++;
 		}
 		while(bb.hasRemaining());
 	    // return the sub-array
 		int [] result = new int[resultlist.size()];
 		for(int i = 0; i < resultlist.size(); i++)
 		{
 			result[i] = resultlist.get(i);
 		}
 			
 		return result;
	
 	}
   	
	static ArrayList<Integer> res = new ArrayList<Integer>();

	static int  calls = 0;
    static int  position = 0;
    static int  howmuch = 0;
    static int  limit = 0;
    static int  value = 0;
    static byte b = 0;

	/**
	 * Read a variable length integer from the supplied ByteBuffer
	 * 
	 * @param in buffer to read from
	 * @return the int value
	 */
	public static int[] readVarInt(byte[] values) {

		calls++;
		if ((calls % 100000) == 0)
			System.out.println(" calls :: " + calls);
        position = 0;
        howmuch =  0;
        //value = 0;
        limit = values.length;
        
		do {
			if (position < limit) {
				byte b = values[position++];
				value = b & 0x7F;
				
				test:
				while ((b & 0x80) != 0) {
					if (position < limit) {
						b = values[position++];
						value <<= 7;
						value |= (b & 0x7F);
					} else {
						//break;
						break test;
					}

				}
				resultset[howmuch++] = value;
			}
			
		} while (position < limit);

		int [] back = new int[howmuch];
		System.arraycopy(resultset, 0, back, 0, howmuch);

		return back;    
	}

	
	public static int[] readMasterHeaderVarInts(byte[] values) {
		return Arrays.copyOfRange(readVarInt(values), 0, 5);
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
	 * Converts and 4-Byte integer value into a hex-String.
	 * 
	 */
	public static String Int2Hex(int i) {
		return Auxiliary.bytesToHex3(new byte[] { (byte) (i >>> 24), (byte) (i >>> 16), (byte) (i >>> 8), (byte) i });
	}

	public static int varintHexString2Integer(String s) {
		byte[] value = hexStringToByteArray(s);
		ByteBuffer bb = ByteBuffer.wrap(value);
		return readUnsignedVarInt(bb);
	}

	/* s must be an even-length string. */
	public static byte[] hexStringToByteArray(String s) {
		int len = s.length();
		byte[] data = new byte[len / 2];
		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
		}
		return data;
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
	public static byte[] readOverflow(Job job, int pagenumber) {
		byte[] part = null;

		/* read the next overflow page startRegion file */
		ByteBuffer overflowpage = job.readPageWithNumber(pagenumber, job.ps);

		overflowpage.position(0);
		int overflow = overflowpage.getInt();


		if (overflow == 0) {
			// termination condition for the recursive callup's
			; //System.out.println("No further overflow pages");
			/* startRegion the last overflow page - do not copy the zero bytes. */
		} else {
			/* recursively call next overflow page in the chain */
			part = readOverflow(job, overflow);
		}

		/*
		 * we always crab the complete overflow-page minus the first four bytes - they
		 * are reserved for the (possible) next overflow page offset
		 **/
		byte[] current = new byte[job.ps - 4];
	

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

	public static int computeWALPageForOffset(int offset, int ps) {

		int temp = offset - 32;
		int pagenumber = temp / (24 + ps);
		return pagenumber;
	}
	
	public static String hex2ASCII(String hex){
		
		// remove .. at the end of the hex - string if present
		hex = hex.replace("..","");
		
		int idx = hex.indexOf("] ");
		//String begin = hex.substring(0,idx);
		String tail = hex.substring(idx+2);
		
		idx  = hex.indexOf(">");
		if (idx > 0) {
			//begin = hex.substring(0,idx);
			tail = hex.substring(idx+1);
		}
		
		StringBuilder output = new StringBuilder();
		for (int i = 0; i < tail.length(); i+=2) {
		    String str = tail.substring(i, i+2);    
		    //output.append((char)Integer.parseInt(str, 16));
		    char next = (char)Integer.parseInt(str, 16);
		    if(next > 31 && next < 127)
		    	output.append(next);
		    else
		    	output.append('.');
		}
		
		return output.toString();
	}
	
	public static String hex2ASCII_v2(String hex){
		
		
		StringBuilder output = new StringBuilder();
		for (int i = 0; i < hex.length(); i+=2) {
		    String str = hex.substring(i, i+2);    
		    //output.append((char)Integer.parseInt(str, 16));
		    char next = (char)Integer.parseInt(str, 16);
		    if(next > 31 && next < 127)
		    	output.append(next);
		    else
		    	output.append('.');
		}
		
		return output.toString();
	}
	
	public static boolean isWindowsSystem() {
	    String os = System.getProperty("os.name");
	    System.out.println("Using System Property: " + os);
	    return os.contains("Windows");
	}

	public static boolean isMacOS() {
	    String os = System.getProperty("os.name");
	    System.out.println("Using System Property: " + os);
	    return os.contains("Mac");
	}
	
	public static boolean writeBLOB2Disk(Job job, String path) {
		try {
			BLOBElement e = job.bincache.get(path);
			BufferedOutputStream buffer = new BufferedOutputStream(new FileOutputStream(path));
			buffer.write(e.binary);
			buffer.close();
		}catch(Exception err){
			return false;
		}
		return true;
	}	
	
    /**
     * Calculates the entropy per character/byte of a byte array.
     *
     * @param input array to calculate entropy of
     *
     * @return entropy bits per byte
     */
    public static double entropy(byte[] input) {
        if (input.length == 0) {
            return 0.0;
        }

        /* Total up the occurrences of each byte */
        int[] charCounts = new int[256];
        for (byte b : input) {
            charCounts[b & 0xFF]++;
        }

        double entropy = 0.0;
        for (int i = 0; i < 256; ++i) {
            if (charCounts[i] == 0.0) {
                continue;
            }

            double freq = (double) charCounts[i] / input.length;
            entropy -= freq * (Math.log(freq) / Math.log(2));
        }

        return entropy;
    }
    
	@SuppressWarnings("rawtypes")
    public static String composeOutputLine(FQTableView table, Iterator<String> s) {
    	
    	int current = 0;
    	String offset = null;
  	
    	
    	StringBuffer sb = new StringBuffer();
    	
    	// BLOB handling
    	while (s.hasNext()){ 
    	
    		/* column for offset found ? */
    		if(current == 5)
    		{
    			offset = s.next();
    			sb.append(";");
    			sb.append(offset);
    			current++;
    			continue;
    		}	
    		
    		String cellvalue = s.next();

		    /* BLOB-value found ? */     		
    		if(cellvalue.length()>7) {
    			
        		
    			int from = cellvalue.indexOf("BLOB-");
        	    int to = cellvalue.indexOf("]");
        	    
        	    if(from > 0 && to > 0)
        	    {
        	    
	        	    String number = cellvalue.substring(from+5, to);			
	        		int start = cellvalue.indexOf("<");
	        		int end   = cellvalue.indexOf(">");
	        				
	        		String type;
					if (end > 0) {
						type = cellvalue.substring(start+1,end);
					}
					else 
	        			type = "bin";
	        				
	        		if(type.equals("java"))
	        			type = "bin";
	        			
	        		String path = GUI.baseDir + Global.separator + table.dbname + "_" + offset + "-" + number + "." + type;
	        		String data = table.job.bincache.getHexString(path);
	        		System.out.println(" Data" + data);				
	        		cellvalue = data.toUpperCase();
        	    }
        	}
    		
    		if(current > 0)
    			sb.append(";");
    		sb.append(cellvalue);	
            current++;
    	}
    	// put a end of line to the line to export
    	sb.append("\n");
    	return sb.toString();
    
	}
   
}
