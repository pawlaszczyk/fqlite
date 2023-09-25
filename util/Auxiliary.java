package fqlite.util;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.Buffer;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicInteger;

import javax.imageio.ImageIO;

import org.apache.commons.codec.digest.DigestUtils;

import fqlite.base.Base;
import fqlite.base.Global;
import fqlite.base.Job;
import fqlite.base.SqliteElement;
import fqlite.descriptor.AbstractDescriptor;
import fqlite.descriptor.IndexDescriptor;
import fqlite.descriptor.TableDescriptor;
import fqlite.parser.SQLiteSchemaParser;
import fqlite.pattern.HeaderPattern;
import fqlite.pattern.IntegerConstraint;
import fqlite.types.BLOBElement;
import fqlite.types.BLOBTYPE;
import fqlite.types.SerialTypes;
import fqlite.types.StorageClass;
import fqlite.types.TimeStamp;
import javafx.collections.ObservableList;
import javafx.embed.swing.SwingFXUtils;
import javafx.scene.Node;
import javafx.scene.control.TableView;
import javafx.scene.image.Image;
import javafx.scene.layout.VBox;

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
	public boolean readMasterTableRecord(Job job, int start, ByteBuffer buffer, String header) throws IOException {

		System.out.println("in readMasterTableRecord() " + start + " " + buffer);

		SqliteElement[] columns;

		columns = MasterRecordToColumns(header);

		if (null == columns)
			return false;

		String cl = header.substring(2);

		// use the header information to reconstruct pll info
		int pll = this.computePayloadLength(cl.substring(0, 12));

		int so;

		buffer.position(start + 8);
		System.out.println(buffer.toString());

		// determine overflow that is not fitting
		so = computePayload(pll);

		int overflow = -1;

		/* Do we have overflow-page(s) ? */
		if (so < pll) {
			int phl = header.length() / 2;

			int last = buffer.position();
			System.out.println(" readMasterTableRecord() spilled payload ::" + so);
			System.out.println(" pll payload ::" + pll);

			// get the last 4 byte of the first page -> this should contain the
			// page number of the first overflow page
			overflow = buffer.getInt(job.ps - 4);

			if (overflow > job.numberofpages)
				return false;

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
			ByteBuffer bf = ByteBuffer.wrap(c);

			buffer = bf;

			// set original buffer pointer just before the record start of
			// table row
			buffer.position(last);
		}

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

			value = new byte[en.length];

			try {
				// System.out.println("current pos" + buffer.position());
				buffer.get(value);
			} catch (BufferUnderflowException bue) {
				return false;
			}

			/* column 3 ? -> tbl_name TEXT */
			if (con == 2) {
				tablename = en.toString(value, true);
				System.out.println(" Tablename " + tablename);

			}

			/* column 4 ? -> root page Integer */
			if (con == 3) {
				if (value.length == 0)
					debug("Seems to be a virtual component -> no root page ;)");
				else {

					/* root page of table is decoded a BE integer value */

					if (en.type == SerialTypes.INT8)
						rootpage = SqliteElement.decodeInt8(value[0]);
					else if (en.type == SerialTypes.INT16)
						rootpage = SqliteElement.decodeInt16(new byte[] { value[0], value[1] });
					else if (en.type == SerialTypes.INT24)
						rootpage = SqliteElement.decodeInt24(new byte[] { value[0], value[1], value[2] });
					else if (en.type == SerialTypes.INT32)
						rootpage = SqliteElement.decodeInt32(new byte[] { value[0], value[1], value[2], value[3] });

				}
			}

			/* read sql statement */

			if (con == 4) {
				statement = en.toString(value, true);
				System.out.println(" SQL statement::" + statement);
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

		LinkedList<String> record = new LinkedList<String>();
		SqliteElement[] columns;
		int rowid = -1;

		/* set the pointer directly after the header */
		buffer.position(m.end);

		/**
		 * CASE 1: We have a found a complete but deleted record let us try to read the
		 * ROWID of this record if possible
		 **/
		if (m.match.startsWith("RI")) {
			System.out.println(" need to determine rowid " + m.end);

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

				System.out.println(beforeheader[2] + " " + beforeheader[3]);

				int[] values = readVarInt(beforeheader);

				int rd = -1;
				if (values != null && values.length > 0) {
					rd = values[values.length - 1];
					System.out.println("rowid ??? " + rd);
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

			System.out.println(freeblockinfo[0] + " " + freeblockinfo[1]);

			buffer.position(buffer.position() - 2);

			short lg = buffer.getShort(); // 1

			if (null != next) {

				int nextmatch = next.begin;

				if (nextmatch < m.begin + lg) {

					System.out.println("  lg vorher " + lg);
					lg = (short) (lg - (short) (m.begin + lg - (next.begin)));
					System.out.println(" korrigierter Wert für lg" + lg);
				}

			}

			/* | 4 bytes | header with serial types | payload(data)| */
			/* compute the length of the missing first column */

			int first = lg - 4 - headerlength - getPayloadLength(match);
			System.out.println("length of 1st column " + first);
			System.out
					.println(" lg " + lg + " minus 4 " + "minus " + headerlength + " minus " + getPayloadLength(match));
			System.out.println(" first ::" + first);

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
				System.out.println("fallback strategy" + header);

			}
			buffer.position(m.end);
		}

		/* get column types and length for each single column */
		columns = toColumns(header);

		/* in case of an error -> skip */
		if (null == columns) {
			debug(" no valid header-string: " + header);
			return null;
		}

		/*
		 * use the header information to reconstruct the payload length, since this
		 * information is normally lost during delete
		 */

		int pll = computePayloadLength(header);

		int so = computePayload(pll);

		int co = 0;
		boolean error = false;

		/* in the output string we first start with the offset */
		record.add((pagenumber - 1) * job.ps + m.begin + "");

		int overflow = -1;

		/* Do we have overflow-page(s) ? */
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

				byte[] value = new byte[en.length];

				bf.get(value);
				if (en.serial == StorageClass.BLOB) {
					//System.out.println("BLOB column detected");
					// record.add("[BLOB] " + en.getBLOB(value) + "..");

					blobcolidx++;
					String tablecelltext = en.getBLOB(value);
					record.add("[BLOB-" + blobcolidx + "] " + tablecelltext + "..");

					/* by default the record offset is used as key for a BLOB */
					Long hash = Long.parseLong(record.get(2)) + blobcolidx;

					if (tablecelltext.contains("jpg"))
						job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.JPG));
					else if (tablecelltext.contains("png"))
						job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.PNG));
					else if (tablecelltext.contains("gif"))
						job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.GIF));
					else if (tablecelltext.contains("bmp"))
						job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.BMP));
					else if (tablecelltext.contains("tiff"))
						job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.TIFF));
					else if (tablecelltext.contains("heic"))
						job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.HEIC));
					else if (tablecelltext.contains("pdf"))
						job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.PDF));
					else if (tablecelltext.contains("pdf"))
						job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.PLIST));					
					else
						job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.UNKOWN));

					if (tablecelltext.contains("jpg") || tablecelltext.contains("png") || tablecelltext.contains("gif")
							|| tablecelltext.contains("bmp")) {
						/* for the tooltip preview we need to shrink the pictures */
						Image img = scaledown(value);
						if (null != img)
							job.Thumbnails.put(hash, img);
					}

				} else {
					record.add(en.toString(value, false));
				}
				co++;
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

					byte[] value = new byte[en.length];
					if ((buffer.position() + en.length) > buffer.limit()) {
						error = true;
						return null;
					}

					if (rowid >= 0 && en.length == 0 && m.rowidcolum >= 0) {
						if (m.rowidcolum == number) {
							record.add(rowid + "");
							co++;
							continue;
						}
					}

					buffer.get(value);
					if (en.serial == StorageClass.BLOB) {
						//System.out.println("BLOB column detected");

						// record.add("[BLOB] " + en.getBLOB(value) + "..");

						blobcolidx++;
						String tablecelltext = en.getBLOB(value);
						record.add("[BLOB-" + blobcolidx + "] " + tablecelltext + "..");

						/* by default the record offset is used as key for a BLOB */
						Long hash = Long.parseLong(record.get(2)) + blobcolidx;

						if (tablecelltext.contains("jpg"))
							job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.JPG));
						else if (tablecelltext.contains("png"))
							job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.PNG));
						else if (tablecelltext.contains("gif"))
							job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.GIF));
						else if (tablecelltext.contains("bmp"))
							job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.BMP));
						else if (tablecelltext.contains("tiff"))
							job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.TIFF));
						else if (tablecelltext.contains("heic"))
							job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.HEIC));
						else if (tablecelltext.contains("pdf"))
							job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.PDF));
						else if (tablecelltext.contains("plist"))
							job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.PLIST));
						else
							job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.UNKOWN));

						if (tablecelltext.contains("jpg") || tablecelltext.contains("png")
								|| tablecelltext.contains("gif") || tablecelltext.contains("bmp")) {
							/* for the tooltip preview we need to shrink the pictures */
							Image img = scaledown(value);
							if (null != img)
								job.Thumbnails.put(hash, img);
						}

					} else
						record.add(en.toString(value, false));

					co++;

				}

			} else {
				/* partial data record? -> check length */
				int nextrecord = bs.nextSetBit(buffer.position());
				boolean partial = false;
				int blobcolidx = 0;

				// no overflow - try to recover at least a partial record

				for (int cc = 0; cc < columns.length; cc++) {

					if (partial) {
						record.add("");
						continue;
					}

					SqliteElement en = columns[cc];

					if (en == null) {
						continue;
					}

					if (rowid >= 0 && en.length == 0 && m.rowidcolum >= 0) {
						if (m.rowidcolum == cc) {
							record.add(rowid + "");
							co++;
							continue;
						}
					}

					byte[] value = new byte[en.length];
					if ((buffer.position() + en.length) > buffer.limit()) {
						error = true;
						return null;
					}

					if (nextrecord == -1)
						nextrecord = job.ps;

					/* partial data record? -> check length */
					if (((buffer.position() + en.length) > nextrecord)) {
						/* we can only recover some bytes but not all of the given column */
						en.length = en.length - (buffer.position() + en.length - nextrecord);
						// System.out.println("Rest-Bytes:: " + en.length + "");
						if (en.length > 0) {
							/* a partial restore is only possible for columns like STRING and BLOB */
							if (en.type == SerialTypes.BLOB || en.type == SerialTypes.STRING) {
								value = new byte[en.length];
								buffer.get(value);
								// record.add(en.toString(value,false));

								if (en.serial == StorageClass.BLOB) {
									//System.out.println("BLOB column detected");

									// record.add("[BLOB] " + en.getBLOB(value) + "..");

									blobcolidx++;
									String tablecelltext = en.getBLOB(value);
									record.add("[BLOB-" + blobcolidx + "] " + tablecelltext + "..");

									/* by default the record offset is used as key for a BLOB */
									Long hash = Long.parseLong(record.get(2)) + blobcolidx;

									if (tablecelltext.contains("jpg"))
										job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.JPG));
									else if (tablecelltext.contains("png"))
										job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.PNG));
									else if (tablecelltext.contains("gif"))
										job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.GIF));
									else if (tablecelltext.contains("bmp"))
										job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.BMP));
									else if (tablecelltext.contains("tiff"))
										job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.TIFF));
									else if (tablecelltext.contains("heic"))
										job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.HEIC));
									else if (tablecelltext.contains("pdf"))
										job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.PDF));
									else if (tablecelltext.contains("plist"))
										job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.PLIST));								
									else
										job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.UNKOWN));

									if (tablecelltext.contains("jpg") || tablecelltext.contains("png")
											|| tablecelltext.contains("gif") || tablecelltext.contains("bmp")) {
										/* for the tooltip preview we need to shrink the pictures */
										Image img = scaledown(value);
										if (null != img)
											job.Thumbnails.put(hash, img);
									}

								} else {
									record.add(en.toString(value, false));
								}

							}
						}
						partial = true;
						continue;
					} else {

						buffer.get(value);

						if (en.serial == StorageClass.BLOB) {
							//System.out.println("BLOB column detected");
							// record.add("[BLOB] " + en.getBLOB(value) + "..");

							blobcolidx++;
							String tablecelltext = en.getBLOB(value);
							record.add("[BLOB-" + blobcolidx + "] " + tablecelltext + "..");

							/* by default the record offset is used as key for a BLOB */
							Long hash = Long.parseLong(record.get(2)) + blobcolidx;

							if (tablecelltext.contains("jpg"))
								job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.JPG));
							else if (tablecelltext.contains("png"))
								job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.PNG));
							else if (tablecelltext.contains("gif"))
								job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.GIF));
							else if (tablecelltext.contains("bmp"))
								job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.BMP));
							else if (tablecelltext.contains("tiff"))
								job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.TIFF));
							else if (tablecelltext.contains("heic"))
								job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.HEIC));
							else if (tablecelltext.contains("pdf"))
								job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.PDF));
							else if (tablecelltext.contains("plist"))
								job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.PLIST));
							else
								job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.UNKOWN));

							if (tablecelltext.contains("jpg") || tablecelltext.contains("png")
									|| tablecelltext.contains("gif") || tablecelltext.contains("bmp")) {
								/* for the tooltip preview we need to shrink the pictures */
								Image img = scaledown(value);
								if (null != img)
									job.Thumbnails.put(hash, img);
							}

						} else {
							record.add(en.toString(value, false));
						}

					}

					co++;

				}
			}

			if (error)
				return null;
		}

		/* mark bytes as visited */
		bs.set(m.end, buffer.position(), true);
		debug("visited :: " + m.end + " bis " + buffer.position());
		int cursor = ((pagenumber - 1) * job.ps) + buffer.position();
		debug("visited :: " + (((pagenumber - 1) * job.ps) + m.end) + " bis " + cursor);

		record.add(0, "" + pll);
		record.add(1, "" + header.length() / 2);

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
	private StringBuffer write(int col, SqliteElement en, byte[] value) {

		StringBuffer val = new StringBuffer();

		if (col > 0)
			val.append(";");

		val.append(en.toString(value, false));

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
	public LinkedList<String> readRecord(int cellstart, ByteBuffer buffer, int pagenumber_db, BitSet bs, int pagetype,
			int maxlength, StringBuffer firstcol, boolean withoutROWID, int filetype, long offset) throws IOException {

		/* we use a list to hold the fields of the data record to read */
		LinkedList<String> record = new LinkedList<String>();

		/* true, if table a therefore record structure of page is unkown */
		boolean unkown = false;

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
				record.add(job.pages[pagenumber_db].getName());
			} 
			else {
				record.add(Global.DELETED_RECORD_IN_PAGE);
				record.add("__UNASSIGNED");
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
			}
			record.add(ad.getName());
			record.add(Global.REGULAR_RECORD);

			/*
			 * for a regular db-file the offset is derived from the page number, since all
			 * pages are a multiple of the page size (ps).
			 */
			record.add((((pagenumber_db - 1) * job.ps) + cellstart) + "");
			rowid_col = job.pages[pagenumber_db].rowid_col;

		} else {
			/*
			 * component is not part of btree - page on free list -> need to determine name
			 * of component
			 */
			unkown = true;
		}



		debug("cellstart for pll: " + (((pagenumber_db - 1) * job.ps) + cellstart));
		// length of payload as varint
		try {
			buffer.position(cellstart);
		} catch (Exception err) {
			System.err.println("ERROR: cellstart not in buffer" + cellstart);
			return null;
		}
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

				} 
				else if (null == job.pages[pagenumber_db] || job.pages[pagenumber_db].ROWID) {
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

		int[] values = readVarInt(decode(hh));

		columns = getColumns(phl, buffer, firstcol);

		if (null == columns) {
			debug(" No valid header. Skip recovery.");
			return null;
		}

		debug("Number of columns: " + columns.length);

		int co = 0;
		try {
			if (unkown) {

				td = matchTable(columns);

				/* this is only neccessesary, when component name is unkown */
				if (null == td) {
					record.add(Global.DELETED_RECORD_IN_PAGE);
					record.add("__UNASSIGNED");
				} else {
					record.add(td.tblname);
					job.pages[pagenumber_db] = td;
					rowid_col = td.rowid_col;
				}

				record.add(Global.REGULAR_RECORD);
				record.add((((pagenumber_db - 1) * job.ps) + cellstart) + "");
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
				/* read overflow */
				buffer.position(buffer.position() + so - phl - 1);
				overflow = buffer.getInt();
			} catch (Exception err) {
				return null;
			}

			if (overflow < 0)
				return null;
			debug("regular overflow::::::::: " + overflow + " " + Integer.toHexString(overflow));
			/*
			 * This is a regular overflow page - remember this decision & don't analyze this
			 * page
			 */
			job.overflowpages.add(overflow);
			buffer.position(last);

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
			/* copy spilled overflow of current page into extended buffer */
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
				
				
				byte[] value = new byte[en.length];

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
						//System.out.println("BLOB column detected");

						// record.add("[BLOB] " + en.getBLOB(value) + "..");
						blobcolidx++;
						String tablecelltext = en.getBLOB(value);
						record.add("[BLOB-" + blobcolidx + "] " + tablecelltext + "..");

						Long hash;
						/* by default the record offset is used as key for a BLOB */
						if (record.get(2).length() > 2)
							hash = Long.parseLong(record.get(2)) + blobcolidx;
						else
							hash = (long) blobcolidx;

						if (tablecelltext.contains("jpg"))
							job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.JPG));
						else if (tablecelltext.contains("png"))
							job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.PNG));
						else if (tablecelltext.contains("gif"))
							job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.GIF));
						else if (tablecelltext.contains("bmp"))
							job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.BMP));
						else if (tablecelltext.contains("tiff"))
							job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.TIFF));
						else if (tablecelltext.contains("heic"))
							job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.HEIC));
						else if (tablecelltext.contains("pdf"))
							job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.PDF));
						else if (tablecelltext.contains("plist"))
							job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.PLIST));
						else
							job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.UNKOWN));

						if (tablecelltext.contains("jpg") || tablecelltext.contains("png") || tablecelltext.contains("gif")
								|| tablecelltext.contains("bmp")) {
							/* for the tooltip preview we need to shrink the pictures */
							Image img = scaledown(value);
							if (null != img)
								job.Thumbnails.put(hash, img);
						}

					} else {

						String vv = null;
						boolean istimestamp = false;

						if (td != null) {
							
							
							if (co < td.serialtypes.size()) {
								String coltype = td.sqltypes.get(co);
								//System.out.println(" Column " + coltype);
								if (coltype.equals("TIMESTAMP")) {

									TimeStamp ts = timestamp2String(en, value);
									if (null != ts) {
										vv = ts.text;									
										istimestamp = true;
										System.out.println("<<<< write timestamp key:: " + vv + " ");
										job.timestamps.put(vv, found);

									}
								}
//								if (coltype.equals("INTEGER")) {
	//
//									TimeStamp ts = Integer2String(en, value);
//									if (null != ts) {
//										vv = ts.text;
//										istimestamp = true;
//										System.out.println("<<<< write timestamp key:: " + vv + " ");
//										job.timestamps.put(vv, found);
	//
//									}
//								} else if (coltype.equals("REAL")) {
	//
//									TimeStamp ts = Real2String(en, value);
//									if (null != ts) {
//										vv = ts.text;
//										istimestamp = true;
//										System.out.println("<<<< write timestamp key:: " + vv + " ");
//										job.timestamps.put(vv, found);
	//
//									}
//								}

							}
							
						} else {
							/* no table description (td) avail. -> take the raw-value */
							record.add(en.toString(value, false));
							continue;
						}
						
					
						if (!istimestamp) {
							if (en.type == SerialTypes.PRIMARY_KEY)
							{	
								en.toString(value,false);
								vv = null;
							}
							else	
								vv = en.toString(value, false);
							
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
					System.out.println("readRecord():: buffer underflow ERROR " + err);
					return null;
				}

				if (en.serial == StorageClass.BLOB) {
					//System.out.println("BLOB column detected");

					// record.add("[BLOB] " + en.getBLOB(value) + "..");
					blobcolidx++;
					String tablecelltext = en.getBLOB(value);
					record.add("[BLOB-" + blobcolidx + "] " + tablecelltext + "..");

					Long hash;
					/* by default the record offset is used as key for a BLOB */
					if (record.get(2).length() > 2)
						hash = Long.parseLong(record.get(2)) + blobcolidx;
					else
						hash = (long) blobcolidx;

					if (tablecelltext.contains("jpg"))
						job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.JPG));
					else if (tablecelltext.contains("png"))
						job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.PNG));
					else if (tablecelltext.contains("gif"))
						job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.GIF));
					else if (tablecelltext.contains("bmp"))
						job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.BMP));
					else if (tablecelltext.contains("tiff"))
						job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.TIFF));
					else if (tablecelltext.contains("heic"))
						job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.HEIC));
					else if (tablecelltext.contains("pdf"))
						job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.PDF));
					else if (tablecelltext.contains("plist"))
						job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.PLIST));
					else
						job.BLOBs.put(hash, new BLOBElement(value, BLOBTYPE.UNKOWN));

					if (tablecelltext.contains("jpg") || tablecelltext.contains("png") || tablecelltext.contains("gif")
							|| tablecelltext.contains("bmp")) {
						/* for the tooltip preview we need to shrink the pictures */
						Image img = scaledown(value);
						if (null != img)
							job.Thumbnails.put(hash, img);
					}

				} else {

					String vv = null;
					boolean istimestamp = false;

					if (td != null) {
						
						
						if (co < td.serialtypes.size()) {
							String coltype = td.sqltypes.get(co);
							//System.out.println(" Column " + coltype);
							if (coltype.equals("TIMESTAMP")) {

								TimeStamp ts = timestamp2String(en, value);
								if (null != ts) {
									vv = ts.text;									
									istimestamp = true;
									System.out.println("<<<< write timestamp key:: " + vv + " ");
									job.timestamps.put(vv, found);

								}
							}
//							if (coltype.equals("INTEGER")) {
//
//								TimeStamp ts = Integer2String(en, value);
//								if (null != ts) {
//									vv = ts.text;
//									istimestamp = true;
//									System.out.println("<<<< write timestamp key:: " + vv + " ");
//									job.timestamps.put(vv, found);
//
//								}
//							} else if (coltype.equals("REAL")) {
//
//								TimeStamp ts = Real2String(en, value);
//								if (null != ts) {
//									vv = ts.text;
//									istimestamp = true;
//									System.out.println("<<<< write timestamp key:: " + vv + " ");
//									job.timestamps.put(vv, found);
//
//								}
//							}

						}
						
					} else {
						/* no table description (td) avail. -> take the raw-value */
						record.add(en.toString(value, false));
						continue;
					}
					
				
					if (!istimestamp) {
						if (en.type == SerialTypes.PRIMARY_KEY)
						{	
							en.toString(value,false);
							vv = null;
						}
						else	
							vv = en.toString(value, false);
						
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
		record.add(1, "" + pll);
		record.add(2, "" + hh.length() / 2);

		/* mark as visited */
		if (so < pll) {
			debug("visted " + cellstart + " bis " + (cellstart + so + 7));
			bs.set(cellstart, cellstart + so + 4);
		} else {
			debug("visted " + cellstart + " bis " + buffer.position());
			bs.set(cellstart, buffer.position());
		}
		if (error) {
			err("spilles overflow page error ...");
			// return "";
			return null;
		}
		
	    	
	    if (filetype == Global.ROLLBACK_JOURNAL_FILE && null != ad && !unkown && !withoutROWID) {
			
			/* the offset is derived from the name of the table plus the internal rowid */
			String offs = ad.getName() + "_" + rowid;
			
		    if(job.LineHashes.containsKey(offs)){
		    	//System.out.println("line is still in the database " + offs);
		    	
		    	Integer originalhash = job.LineHashes.get(offs);
		    	Integer journalhash = computeHash(record);
		    	
		    	/* hash of line in database differs from hash in journal file */
		    	if(!originalhash.equals(journalhash))
		    	{
		    		//System.out.println(originalhash + " <> " + journalhash);
		    		
		    		record.set(3,"updated");
		    	}
		    }
		    else {
		     	System.out.println("removed record at offset " + offs);
		     	//System.out.println(record);
		     	record.set(3,"deleted");
		    }
		
		}
		else if (filetype == Global.WAL_ARCHIVE_FILE && !unkown && null != ad && !withoutROWID) {
			
			/* the offset is derived from the name of the table plus the internal rowid */
			String offs = ad.getName() + "_" + rowid;
			
		    if(job.TimeLineHashes.containsKey(offs)){
		    	
		    //	System.out.println("line is in the database " + offs);
		    	
		    	Integer originalhash = job.LineHashes.get(offs);
		    	Integer journalhash = computeHash(record);
		    
		    //	System.out.println("line is still in the database " + offs + " " + originalhash + "<>" + journalhash);
			    
		    	//updateDatarecord(ad.getName(),"test");
		    	
		    	/* hash of line in database differs from hash in journal file */
		    	if(!originalhash.equals(journalhash))
		    	{
		    //		System.out.println(originalhash + " <> " + journalhash);
		    		
		    		record.set(3,"updated");
		    	}
		    }
		    else {
		     	//System.out.println("new record at offset " + offs);
		     	//System.out.println(record);
		     	record.set(3,"new");
		    }
			
			
			
		}
		else if (!unkown && null != ad && !withoutROWID) {
			
		
		
				// we need this information to detect differences between current line in database and
				// value in Journal file
			
			    // case 1: Journal File
			
				// Note: a missing hash means that a line as added/removed recently, an exsiting offset with
				// a different hash value compared to the hash of the WAL,Journal means that this line has been 
				// updated.
				job.LineHashes.put(ad.getName() + "_" + rowid,computeHash(record));
				
				// case 2: WAL File	
				LinkedList<Integer> ll = new LinkedList<Integer>();
				ll.add(computeHash(record));
				job.TimeLineHashes.put(ad.getName() + "_" + rowid, (new LinkedList<Integer>()));
				
		}
	
		
		return record;
	}
	
	private void updateDatarecord(int page, String tablename,String RowID, String action){
		
		if(job.gui == null)
			return;
		
		
		
		Node n = job.gui.tables.get("/"+tablename);
		
		if(n != null){
			
			
				if (n instanceof VBox) {

					VBox panel = (VBox)n;
					TableView<Object> table = (TableView<Object>)panel.getChildren().get(1);  // get the table back
			
				    ObservableList<Object> list = table.getItems();
				    
				    Object first = list.get(1);
				}
		}
	}

	private int computeHash(LinkedList<String> record){
		
		// we currently work on the original data base file within the readRecord() method		
		LinkedList<String> ll = new LinkedList<String>();
		// enumerate all line items starting with the first column just behind "offset" column (index 5)
		for (int zz = 5; zz < record.size(); zz++){
			ll.add(record.get(zz));
		}
	
		return ll.hashCode();
	}
	
	
	/**
	 * Convert a SQL-TIMESTAMP value into a human readable format.
	 * 
	 * @param original
	 * @return
	 */
	private TimeStamp timestamp2String(SqliteElement en, byte[] value) {

		if (en.type == SerialTypes.INT48) {
			System.out.println("INT48 zeit " + en.type);

			long l = SqliteElement.decodeInt48ToLong(value) / 100;
			long time = (978307200 + (long) l);
			System.out.println("isMacAbsoluteTime(): " + l + " unix " + time);
			ZonedDateTime utc = ZonedDateTime.ofInstant(Instant.ofEpochSecond(time), ZoneOffset.UTC);
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy - HH:mm:ss Z");
			String s = utc.format(formatter);
			return new TimeStamp(s, l);

		} else if (en.type == SerialTypes.INT64) {
			System.out.println("INT64 zeit " + en.type);

			ByteBuffer bf = ByteBuffer.wrap(value);
			long l = bf.getLong();
			// DatetimeConverter.isUnixEpoch(l);

			ZonedDateTime utc = ZonedDateTime.ofInstant(Instant.ofEpochSecond(l), ZoneOffset.UTC);
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy - HH:mm:ss Z");
			String s = utc.format(formatter);
			System.out.println(" Unix Epoch: " + l);
			System.out.println(" UTC Epoch: " + s);
			System.out.println("time: " + s);
			return new TimeStamp(s, l);
		} else if (en.type == SerialTypes.FLOAT64) {
			System.out.println("FLOAT64 zeit " + en.type);
			ByteBuffer bf = ByteBuffer.wrap(value);
			double l = bf.getDouble();
			//System.out.println(">>" + l);

			long time = (978307200 + (long) l);
			System.out.println("isMacAbsoluteTime(): " + l + " unix " + time);
			ZonedDateTime utc = ZonedDateTime.ofInstant(Instant.ofEpochSecond(time), ZoneOffset.UTC);
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy - HH:mm:ss Z");
			String s = utc.format(formatter);
			System.out.println("time: " + s);
			return new TimeStamp(s, l);

		} else if (en.type == SerialTypes.INT32) {

			System.out.println("INT32 zeit " + en.type);
			ByteBuffer bf = ByteBuffer.wrap(value);
			int l = bf.getInt();

			long time = (978307200 + (long) l);
			System.out.println("isMacAbsoluteTime(): " + l + " unix " + time);
			ZonedDateTime utc = ZonedDateTime.ofInstant(Instant.ofEpochSecond(time), ZoneOffset.UTC);
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy - HH:mm:ss Z");
			String s = utc.format(formatter);
			System.out.println("time: " + s);
			return new TimeStamp(s, l);
		}

		return new TimeStamp("null", 0);
	}

	// microseconds (one millionth of a second)

	/* UNIX Epoch in milliseconds since 1st January 1970 */
	final static long UNIX_MIN_DATE = 1262304000000L; // 01.01.2010
	final static long UNIX_MAX_DATE = 2524608000000L; // 01.01.2050

	/* UNIX Epoch in nanoseconds since 1st January 1970 */
	final static long UNIX_MIN_DATE_NANO = 1262304000000000L; // 01.01.2010
	final static long UNIX_MAX_DATE_NANO = 2524608000000000L; // 01.01.2050

	/* CFMACTime Stamps Seconds since 1st January 2001 */
	final static double MAC_MIN_DATE = 300000000; // 05. Jul. 2010
	final static double MAC_MAX_DATE = 2010000000; // 10. Sep. 2064

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

	private TimeStamp Real2String(SqliteElement en, byte[] value) {

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

	private TimeStamp Integer2String(SqliteElement en, byte[] value) {

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

	private static Image scaledown(byte[] bf) {

		InputStream is = new ByteArrayInputStream(bf);
		BufferedImage image = null;
		try {
			// class ImageIO is able to read png, tiff, bmp, gif, jpeg formats
			image = ImageIO.read(is);
			if (image == null)
				return null;
			// scaling down to thumbnail
			java.awt.Image scaled = image.getScaledInstance(100, 100, java.awt.Image.SCALE_FAST);

			//System.out.println(">>> Type:::: " + image.getType());
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

		while (more) {
			/* read next overflow page into buffer */
			if (fromWAL) {
				System.out.println(" >>>> Read Overflow Iterativ from WAL Archive");
				more = false;
				break;
			} else {
				overflowpage = job.readPageWithNumber(next, job.ps);
			}

			if (overflowpage != null) {
				overflowpage.position(0);
				next = overflowpage.getInt() - 1;
				info(" next overflow:: " + next);
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

			if (next < 0 || next > job.numberofpages) {
				// termination condition for the recursive callup's
				debug("No further overflow pages");
				/* startRegion the last overflow page - do not copy the zero bytes. */
				more = false;
			}

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
				pll += e.length * 2; // since Java uses 2 Bytes to save one char (UTF16 with fixed length)
			else
				pll += e.length;
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

	private static SqliteElement[] MasterRecordToColumns(String header) {
		// skip header length byte information - the Master Table has always 6 columns
		if (header.startsWith("07") || header.startsWith("06"))
			header = header.substring(2);

		byte[] bcol = Auxiliary.decode(header);

		return getMaster(bcol);

	}

	public static int getPayloadLength(String header) {
		int sum = 0;
		SqliteElement[] cols = toColumns(header);
		for (SqliteElement e : cols) {
			sum += e.length;
		}
		return sum;
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

	private static SqliteElement[] getMaster(byte[] header) {

		int[] columns = readMasterHeaderVarInts(header);
		if (null == columns)
			return null;
		return getElements(columns);

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

		// SqliteElement[] column = new SqliteElement[columns.length];
		return getElements(columns);
	}

	private static SqliteElement[] getElements(int[] columns) {

		SqliteElement[] column = new SqliteElement[columns.length];

		for (int i = 0; i < columns.length; i++) {

			switch (columns[i]) {
			case 0: // primary key or null value <empty> cell
				column[i] = new SqliteElement(SerialTypes.PRIMARY_KEY, StorageClass.INT, 0);
				break;
			case 1: // 8bit complement integer
				column[i] = new SqliteElement(SerialTypes.INT8, StorageClass.INT, 1);
				break;
			case 2: // 16bit integer
				column[i] = new SqliteElement(SerialTypes.INT16, StorageClass.INT, 2);
				break;
			case 3: // 24bit integer
				column[i] = new SqliteElement(SerialTypes.INT24, StorageClass.INT, 3);
				break;
			case 4: // 32bit integer
				column[i] = new SqliteElement(SerialTypes.INT32, StorageClass.INT, 4);
				break;
			case 5: // 48bit integer
				column[i] = new SqliteElement(SerialTypes.INT48, StorageClass.INT, 6);
				break;
			case 6: // 64bit integer
				column[i] = new SqliteElement(SerialTypes.INT64, StorageClass.INT, 8);
				break;
			case 7: // Big-endian floating point number
				column[i] = new SqliteElement(SerialTypes.FLOAT64, StorageClass.FLOAT, 8);
				break;
			case 8: // Integer constant 0
				column[i] = new SqliteElement(SerialTypes.INT0, StorageClass.INT, 0);
				break;
			case 9: // Integer constant 1
				column[i] = new SqliteElement(SerialTypes.INT1, StorageClass.INT, 0);
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

		// System.out.println("addHeaderPattern2Idx() :: PATTTERN: " + pattern);

	}

	/**
	 * Converts a byte array to Hex-String.
	 * 
	 * @param bytes
	 * @return
	 */
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

	/**
	 * Converts a single byte to a Hex-String.
	 * 
	 * @param b
	 * @return
	 */
	public static String byteToHex(byte b) {
		byte[] ch = new byte[1];
		ch[0] = b;
		return bytesToHex(ch);
	}

	/**
	 * Converts the content of a ByteBuffer object into a Hex-String.
	 * 
	 * @param bb
	 * @return
	 */
	public static String bytesToHex(ByteBuffer bb) {
		int limit = bb.limit();
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
	public static String bytesToHex(byte[] bytes, int fromidx, int toidx) {
		char[] hexChars = new char[(toidx - fromidx + 2) * 2];

		for (int j = 0; j < toidx; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
	}

	/**
	 * Read a variable length integer from the supplied ByteBuffer
	 * 
	 * @param in buffer to read from
	 * @return the int value
	 */
	public static int[] readVarInt(byte[] values) {

		ByteBuffer in = ByteBuffer.wrap(values);
		LinkedList<Integer> res = new LinkedList<Integer>();

		do {
			if (in.hasRemaining()) {
				byte b = in.get();
				int value = b & 0x7F;
				while ((b & 0x80) != 0) {
					if (in.hasRemaining()) {
						b = in.get();
						value <<= 7;
						value |= (b & 0x7F);
					} else {
						break;
					}

				}
				res.add(value);

			}

		} while (in.position() < in.limit());

		int[] result = new int[res.size()];
		int n = 0;
		Iterator<Integer> it = res.iterator();
		while (it.hasNext()) {
			result[n] = it.next();
			n++;
		}
		return result;
	}

	public static int[] readMasterHeaderVarInts(byte[] values) {
		return Arrays.copyOfRange(readVarInt(values), 0, 5);
	}

	/**
	 * Don't use. Old implementation.
	 * 
	 * @param values
	 * @return
	 * @deprecated
	 */
	public static int[] readVarIntOld(byte[] values) {

		int value = 0;
		int counter = 0;
		byte b = 0;
		int shift = 0;

		LinkedList<Integer> res = new LinkedList<Integer>();

		while (counter < values.length) {

			b = values[counter];
			value = 0;
			while (((b = values[counter]) & 0x80) != 0) {

				shift = 7;
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
		return Auxiliary.bytesToHex(new byte[] { (byte) (i >>> 24), (byte) (i >>> 16), (byte) (i >>> 8), (byte) i });
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

	public static long String2long(String s) {
		long longInput = -1L;
		int var5 = -1;
		int var6 = 0;
		int length;
		if ((length = s.length()) != 0 && !s.equals("-") && !s.equals("+")) {
			BigDecimal bigdecimal = null;
			BigInteger biginteger = null;
			boolean var9 = false;
			boolean ishex = s.startsWith("0x") || s.startsWith("Ox") || s.startsWith("ox");
			String var11 = "yzafpnµm kMGTPEZY";
			String var12 = "KMGTPE";

			s.replaceAll(" ", "");
			if (1 < s.length() && !ishex) {
				if (var9 = 105 == s.charAt(s.length() - 1)) {
					var5 = var12.indexOf(s.charAt(s.length() - 2));
				} else {
					int var18 = s.endsWith("c") ? -2
							: (s.endsWith("d") ? -1
									: (s.endsWith("da") ? 1 : (s.endsWith("h") ? 2 : (s.endsWith("%") ? -2 : 0))));
					int var19 = var11.indexOf(s.charAt(s.length() - 1));
					var6 = var18 != 0 ? var18 : (-1 < var19 ? var19 * 3 - 24 : 0);
				}
			}

			if (!var9 || s.length() >= 3 && var5 >= 0) {
				if (ishex) {
					if (s.length() < 3) {
						return -1;
					}

					try {
						biginteger = new BigInteger(s.substring(2, length), 16);
					} catch (Exception var16) {
						return -1;
					}
				} else {
					while (0 < length) {
						try {
							bigdecimal = new BigDecimal(s.substring(0, length));
							break;
						} catch (Exception var17) {
							--length;
						}
					}

					if (length == 0 || bigdecimal == null) {
						return -1;
					}

					bigdecimal = bigdecimal.scaleByPowerOfTen(var6).multiply(BigDecimal.valueOf(1L << 10 * (var5 + 1)));
					biginteger = bigdecimal.toBigInteger();
				}

				long var14 = biginteger.longValue();
				if (biginteger.signum() < 0) {
					longInput = -1L;
				} else if (BigInteger.valueOf(Long.MAX_VALUE).compareTo(biginteger) < 0) {
					longInput = Long.MAX_VALUE;
				} else {
					longInput = var14;
				}

			}
		}
		return longInput;
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
		System.out.println(" overflow:: " + overflow);

		if (overflow == 0) {
			// termination condition for the recursive callup's
			System.out.println("No further overflow pages");
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
		// System.out.println("current ::" + current.length);
		// System.out.println("bytes:: " + (job.ps -4));
		// System.out.println("overflowpage :: " + overflowpage.limit());

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

	public static int computeWALPageForOffset(int offset, int ps) {

		int temp = offset - 32;
		int pagenumber = temp / (24 + ps);
		return pagenumber;
	}
	
	
	public static String hex2ASCII(String hex){
		
		// remove .. at the end of the hex - string if present
		hex = hex.replace("..","");
		
		int idx = hex.indexOf("] ");
		String begin = hex.substring(0,idx);
		String tail = hex.substring(idx+2);
		
		idx  = hex.indexOf(">");
		if (idx > 0) {
			begin = hex.substring(0,idx);
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

}
