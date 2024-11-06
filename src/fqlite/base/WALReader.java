package fqlite.base;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import fqlite.descriptor.AbstractDescriptor;
import fqlite.descriptor.TableDescriptor;
import fqlite.log.AppLog;
import fqlite.pattern.SerialTypeMatcher;
import fqlite.types.CarverTypes;
import fqlite.util.Auxiliary;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;

/**
 * The class analyzes a WAL-file and writes the found records into a file.
 * 
 * From the SQLite documentation:
 * 
 * "The original content is preserved in the database file and the changes are appended into a separate WAL file. 
 *  A COMMIT occurs when a special record indicating a commit is appended to the WAL. Thus a COMMIT can happen 
 *  without ever writing to the original database, which allows readers to continue operating from the original 
 *  unaltered database while changes are simultaneously being committed into the WAL. Multiple transactions can be 
 *  appended to the end of a single WAL file."
 * 
 * @author pawlaszc
 *
 */
public class WALReader{
	
	
	/* checkpoints is a data structure 
	 * 
	 * The salt1-value stays the same for all operations within the same transaction. It is incremented by one, 
	 * if a new checkpoint or transaction is prepared.
	 *
	 * All frames that belong to the same transaction also have an identical "salt1" value
	 * Accordingly, we use salt1 as key for the data structure. 
	 * 
	 * A list of all page numbers belonging to the same transaction and their status (committed or not). 
	 * 
	 * 
	 */
	TreeMap<Long,LinkedList<WALFrame>> checkpoints = new TreeMap<Long,LinkedList<WALFrame>>();
	
	
	
	/* this is a multi-threaded program -> all data are saved to the list first*/
	public ConcurrentHashMap<String,ObservableList<ObservableList<String>>> resultlist = new ConcurrentHashMap<>();

	
	/* An asynchronous channel for reading, writing, and manipulating a file. */
	public AsynchronousFileChannel file;

	/* This buffer holds WAL-file in RAM */
	public ByteBuffer wal;

	/* total size of WAL-file in bytes */
	long size;

	/* File format version. Currently 3007000. */
	
	int ffversion;
	
	/* pagesize */
	int ps;

	/* header field: checkpoint sequence number */
	int csn;
	
	/* Salt-1: random integer incremented with each checkpoint */
	long hsalt1;
	
	/* Salt-2: a different random number for each checkpoint */
	long hsalt2;
	
	/* Checksum-1: First part of a checksum on the first 24 bytes of header */
	long hchecksum1;
	
	/* Checksum-2: Second part of the checksum on the first 24 bytes of header  */
	long hchecksum2;
	
	/* path to WAL-file */
	public String path;

	/* flag for already visited Bytes of the page */
	BitSet visit = null;

	boolean withoutROWID = false;

	/* reference to the MAIN class */
	Job job;

	/* number of page that is currently analyzed */
	int pagenumber_maindb;
    int pagenumber_wal;
	int framestart = 0;
    
	public String headerstring = "";
	
	/* offers a lot of useful utility functions */
	private Auxiliary ct;

	/* knowledge store */
	private StringBuffer firstcol = new StringBuffer();

	private static final String MAGIC_HEADER_STRING1 = "377f0682";
	private static final String MAGIC_HEADER_STRING2 = "377f0683";

	/* buffer that holds the current page */
	ByteBuffer buffer;

	public static List<TableDescriptor> tables = new LinkedList<TableDescriptor>();
	/* this is a multi-threaded program -> all data are saved to the list first */

	/* outputlist */
	ConcurrentLinkedQueue<LinkedList<String>> output = new ConcurrentLinkedQueue<LinkedList<String>>();
	
	public HashMap<Integer,ByteBuffer> overflow = new HashMap<Integer,ByteBuffer>();
	

	/**
	 * Constructor.
	 * 
	 * @param path    full qualified file name to the WAL archive
	 * @param job reference to the Job class
	 */
	public WALReader(String path, Job job) {
		this.path = path;
		this.job = job;
		this.ct = new Auxiliary(job);
	}

	
	
	
	/**
	 * This method is the main processing loop. First the header is analyzed.
	 * Afterwards all write ahead frames are recovered.
	 * 
	 * @return
	 */
	public void parse() {
		int framenumber = 0;
		
		Path p = Paths.get(path);

		System.out.println("parse WAL-File");
		/*
		 * we have to do this before we open the database because of the concurrent
		 * access
		 */

		/* try to open the db-file in read-only mode */
		try {
			file = AsynchronousFileChannel.open(p, StandardOpenOption.READ);
		} catch (Exception e) {
            AppLog.error("Cannot open WAL-file" + p.getFileName());
			return;
		}

		/** Caution!!! we read the complete file into RAM **/
		try {
			readFileIntoBuffer();
		} catch (IOException e) {

			e.printStackTrace();
		}

		/* read header of the WAL file - the first 32 bytes */
		buffer = ByteBuffer.allocate(32);

		Future<Integer> result = file.read(buffer, 0); // position = 0

		while (!result.isDone()) {

			// we can do something in between or just wait ;-).
		}

		// set filepointer to begin of the file
		buffer.flip();

		try {
			if(file.size() <= 32)
			{	
				    AppLog.info("WAL-File is empty. Skip analyzing.");
					return;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}	
		
		/* 
		 * WAL Header Format:
		 * 
		 *	0	4 	Magic number. 0x377f0682 or 0x377f0683
		 *  4	4 	File format version. Currently 3007000.
		 *  8	4 	Database page size. Example: 1024
		 *  12	4 	Checkpoint sequence number
		 *  16	4 	Salt-1: random integer incremented with each checkpoint
		 *  20	4 	Salt-2: a different random number for each checkpoint
		 *  24	4 	Checksum-1: First part of a checksum on the first 24 bytes of header
		 *  28	4 	Checksum-2: Second part of the checksum on the first 24 bytes of header 
		 * 
		 * 
		 *  Source: https://www.sqlite.org/fileformat2.html#walformat
		 */
		
		/********************************************************************/
		/* Check the MAGIC NUMBERS 0x377f0682 or 0x377f0683 */

		byte header[] = new byte[4];
		buffer.get(header);
		if (Auxiliary.bytesToHex(header).equals(MAGIC_HEADER_STRING1))
		{		
				headerstring = MAGIC_HEADER_STRING1;
				AppLog.info("header is okay. seems to be an write ahead log file.");
		}
		else
		if (Auxiliary.bytesToHex(header).equals(MAGIC_HEADER_STRING2))
		{
			headerstring = MAGIC_HEADER_STRING2;
			AppLog.info("header is okay. seems to be an write ahead log file.");
			
		}
		else {
			AppLog.info("sorry. doesn't seem to be an WAL file. Wrong header.");
			AppLog.error("Doesn't seem to be an valid WAL file. Wrong header");
		}

		/*******************************************************************/

		/* at offset 4 */
		buffer.position(4);
		
		ffversion = buffer.getInt();
		AppLog.info(" file format version " + ffversion);
		
		ps = buffer.getInt();

		/*
		 * Must be a power of two between 512 and 32768 inclusive, or the value 1
		 * representing a page size of 65536.
		 */

		/*
		 * Beginning with SQLite version 3.7.1 (2010-08-23), a page size of 65536 bytes
		 * is supported.
		 */
		if (ps == 0 || ps == 1)
			ps = 65536;

		AppLog.info("page size " + ps + " Bytes ");

		
	
		/*
		 * Offset 12 Size 4 Checkpoint sequence number 
		 */
		csn = buffer.getInt();
		AppLog.info(" checkpoint sequence number " + csn);

	
		/*
		 * Offset 16 Size 4 Salt-1: random integer incremented with each checkpoint 
		 */
		hsalt1 = Integer.toUnsignedLong(buffer.getInt());
		AppLog.info(" salt1 " + hsalt1);

		
		/*
		 * Offset 20 Size 4 Salt-2: Salt-2: a different random number for each checkpoint 
		 */
		hsalt2 = Integer.toUnsignedLong(buffer.getInt());
		AppLog.info(" salt2 " + hsalt2);

		
		/* Offset 24 Checksum-1: First part of a checksum on the first 24 bytes of header */
		hchecksum1 = Integer.toUnsignedLong(buffer.getInt());
		AppLog.info(" checksum-1 of first frame header " + hchecksum1);

		
		
		/* Offset 28 Checksum-2: Second part of the checksum on the first 24 bytes of header  */
		hchecksum2 = Integer.toUnsignedLong(buffer.getInt());
		AppLog.info(" checksum-2 second part ot the checksum on the first frame header " + hchecksum2);

		
		/* initialize the BitSet for already visited location within a wal-page */

		visit = new BitSet(ps);
		
		/* end of WAL-header has been reached at offset 31 */
		/* now we can go on with the frames */

		/*******************************************************************/

		/*
		 * WAL Frame Header Format
		 * 
		 * Let us now read the WAL Frame Header. Immediately following the wal-header
		 * are zero or more frames. Each frame consists of a 24-byte frame-header
		 * followed by a page-size bytes of page data. The frame-header is six
		 * big-endian 32-bit unsigned integer values, as follows:
		 * 
		 * 
		 * 
		 * 0	4 	Page number 
		 * 4	4 	For commit records, the size of the database file in pages after the commit. 
		 *          For all other records, zero. 
		 * 8	4 	Salt-1 copied from the WAL header 
		 * 12	4 	Salt-2 copied from the WAL header 
		 * 16	4 	Checksum-1: Cumulative checksum up through and including this page 
		 * 20	4 	Checksum-2: Second half of the cumulative checksum. 
		 * 
		 * 
		 * Source: https://www.sqlite.org/fileformat2.html#walformat
         *
		 */
		
		
		framestart = 32; // this is the position, where the first frame should be

		boolean next = false;
		int numberofpages = 0;
		long start = System.currentTimeMillis();

		do
		{
			/* 24 Byte - with six 4-Byte big endian values */
			byte frameheader[] = new byte[24];
			wal.position(framestart);
			wal.get(frameheader);
			ByteBuffer fheader = ByteBuffer.wrap(frameheader);
	
			/* get the page number of this frame */
			pagenumber_maindb = fheader.getInt();
			
			/* number or size of pages for a commit header, otherwise zero. */
			int commit = fheader.getInt();
			if (commit > 0)
				AppLog.info(" Information of the WAL-archive has been commited successful. ");
			else
				AppLog.info(" No commit so far. this frame holds the latest! version of the page ");
			
			long fsalt1 = Integer.toUnsignedLong(fheader.getInt());
			AppLog.info("fsalt1 " + fsalt1);
			
			long fsalt2 = Integer.toUnsignedLong(fheader.getInt());
			AppLog.info("fsalt2" + fsalt2);
			
			/* A frame is considered valid if and only if the following conditions are true:
			 * 
			 * 1) The salt-1 and salt-2 values in the frame-header match salt values in the wal-header
			 * 
			 * 2) The checksum values in the final 8 bytes of the frame-header exactly match the checksum
			 *    computed consecutively on the first 24 bytes of the WAL header and the first 8 bytes and 
			 *    the content of all frames up to and including the current frame.
			 */
			
			if (hsalt1 == fsalt1 && hsalt2 == fsalt2)
			{
				 AppLog.info("seems to be an valid frame. Condition 1 is true at least. ");
			}	
				
	
			AppLog.debug("pagenumber of frame in main db " + pagenumber_maindb);
	
			/* now we can read the page - it follows immediately after the frame header */
	
			/* read the db page into buffer */
			//System.out.println(" page number of frame in main db " + pagenumber_maindb);
			buffer = readPage();
	
			numberofpages++;
			pagenumber_wal = numberofpages;

			
			WALFrame frame = updateCheckpoint(pagenumber_maindb, framenumber,fsalt1, fsalt2,(commit==0)? false: true);
			analyzePage(frame);

			
			framestart += ps +  24;
			
		
			/*  More pages to analyze ? */
			if(framestart+24+ps < size)
			{
				
				next = true;
			}
			else
				next = false;
			
			framenumber++;
			
		}while(next);


		
		AppLog.info("Lines after WAL-file recovery: " + output.size());
		AppLog.info("Number of pages in WAL-file" + numberofpages);
		long end = System.currentTimeMillis();
		System.out.println("WAL Import duration in ms: " + (end-start));
		
		AppLog.info("Checkpoints " + checkpoints.toString());
	
	}
	
	
	
	private WALFrame updateCheckpoint(int pagenumber, int framenumber,long salt1, long salt2, boolean committed){
		
		WALFrame f = new WALFrame(pagenumber, framenumber , salt1, salt2,  committed);
		//checkpointlist.add(f);

		
		/* new checkpoint/transaction id? */
		if (!checkpoints.containsKey(salt1))
		{
			LinkedList<WALFrame> trx = new LinkedList<WALFrame>();
			trx.add(f);
			checkpoints.put(salt1, trx);
		}
		else
		{
			LinkedList<WALFrame> trx = checkpoints.get(salt1);
			trx.add(f);
		}
		
		return f;
	}

	/**
	 * Analyze the actual database page and try to recover regular and deleted content.
	 * 
	 * @return int success
	 */
	public int analyzePage(WALFrame frame) {
		
		withoutROWID = false;

		/* convert byte array into a string representation */
		String content = Auxiliary.bytesToHex(buffer);

		// offset 0
		buffer.position(0);

		/* check type of the page by reading the first two bytes */
		int type = Auxiliary.getPageType(content);

		/* mark bytes as visited */
		visit.set(0, 2);

		/*
		 * Tricky thing, since a zero page type has normally two possible reasons:
		 * 
		 * reason 1:
		 * 
		 * It is a dropped page. We have to carve for deleted cells but without cell
		 * pointers, cause this list is dropped too or is damaged.
		 * 
		 * reason 2:
		 * 
		 * It is an overflow page -> skip it!
		 */
		if (type == 0) {

			/*
			 * if page was dropped - because of a DROP TABLE command - first 8 Bytes are
			 * zero-bytes
			 */
			buffer.position(0);
			Integer checksum = buffer.getInt();
			/* was page dropped ? */
			if (checksum == 0) {
				// System.out.println(" DROPPED PAGE !!!");
				/* no overflow page -> carve for data records - we do our best! ;-) */
				//if(Global.intenseScan)
				//	carve(content, null);
			}
			/*
			 * otherwise it seems to be a overflow page - however, that is not 100% save !!!
			 */

			/* we have to leave in any case */
			return 0;
		}

		if (pagenumber_wal == 12)
		{
			System.out.println(" page 12 type ::" + type);
		}
		
			
		/************** skip unkown page types ******************/

		// no leaf page -> skip this page
		if (type < 0) {
			AppLog.info("No Data page. " + pagenumber_wal);
			//System.out.println(" write page to overflow. skip page for now " + pagenumber_maindb);
		    overflow.put(pagenumber_maindb,buffer);
			return -1;
		}
		else if (type == 2){
			AppLog.debug("WAL Internal Index page " + pagenumber_wal);
			//System.out.println("WAL Internal Index page " + pagenumber_wal);
			//System.out.println("WALReader:analyzePage() -> Internal index page found" + pagenumber_wal);
			return -1;
		}	
		else if (type == 5) {
			AppLog.info("WAL Internal Table page " + pagenumber_wal);
			//System.out.println("WAL Internal Table page " + pagenumber_wal);
			//System.out.println("WALReader:analyzePage() -> Internal page found" + pagenumber_wal);
			return -1;
		} else if (type == 10) {
			AppLog.debug("WAL Index leaf page " + pagenumber_wal);
			//System.out.println("WAL Index leaf page " + pagenumber_wal);		
			// note: WITHOUT ROWID tables are saved here.
		 	withoutROWID = true;
		} else {
			// 0x0D -> 13 value is 8 (see Auxiliary)
			AppLog.debug("WAL Data page " + pagenumber_wal + " Offset: " + (wal.position()) + " Type " + type);
			//System.out.println("WAL Data page " + pagenumber_wal + " Offset: " + (wal.position()) + " Type " + type);
		}

		/************** regular leaf page with data ******************/

		// boolean freeblocks = false;
		if (type == 8 || type == 13) {
			// offset 1-2 let us find the first free block offset for carving
			byte fboffset[] = new byte[2];
			buffer.position(1);
			buffer.get(fboffset);

		}
		
		int ccrstart = job.ps;

		// found Data-Page - determine number of cell pointers at offset 3-4 of page
		byte cpn[] = new byte[2];
		buffer.position(3);
		buffer.get(cpn);

		// get start pointer for the cell content region
		byte ccr[] = new byte[2];
		buffer.position(5);

		ByteBuffer contentregionstart = ByteBuffer.wrap(ccr);
		ccrstart = Auxiliary.TwoByteBuffertoInt(contentregionstart);


		/* mark as visited */
		visit.set(2, 8);

		ByteBuffer size = ByteBuffer.wrap(cpn);
		int cp = Auxiliary.TwoByteBuffertoInt(size);

		AppLog.debug(" number of cells: " + cp + " type of page " + type);
		job.numberofcells.addAndGet(cp);
		if (0 == cp)
			AppLog.debug(" Page seems to be dropped. No cell entries.");

		int headerend = 8 + (cp * 2);
		visit.set(0, headerend);
		
		/***************************************************************
		 * STEP 2:
		 * 
		 * Cell pointer array scan (if possible)
		 * 
		 ***************************************************************/
		int last = 0;

		/* go on with the cell pointer array */
		for (int i = 0; i < cp; i++) {

			// address of the next cell pointer
			byte pointer[] = new byte[2];
			if (type == 5)
				buffer.position(12 + 2 * i);
			else {
				if (buffer.position() + 8 + 2 * i < buffer.limit())
					buffer.position(8 + 2 * i);
				else
					continue;
			}
			
			if (buffer.position() + 2 < buffer.limit())
			   buffer.get(pointer);
			else
			   continue;
			
			ByteBuffer celladdr = ByteBuffer.wrap(pointer);
			int celloff = Auxiliary.TwoByteBuffertoInt(celladdr);

			if (last > 0) {
				if (celloff == last) {
					continue;
				}
			}
			last = celloff;
			String hls = Auxiliary.Int2Hex(celloff); 
			hls.trim();
			
			LinkedList<String> rc = null;
			

			try { 
				rc = ct.readRecord(celloff, buffer, pagenumber_maindb, visit, type, Integer.MAX_VALUE, firstcol, withoutROWID,Global.WAL_ARCHIVE_FILE,framestart+24 + celloff);
			    if (null == rc)
			    	continue;
			} catch (IOException e) {
				e.printStackTrace();
			}

			if(rc.size()==0)
				System.out.println("rc ist leer");
	
			
			/* adding WAL-Frame fields to output line */
		
			//String info = frame.committed + "," + frame.pagenumber + "," + frame.framenumber + "," + frame.salt1 + "," +  frame.salt2;
			rc.add(5,""+frame.salt2);
			rc.add(5,""+frame.salt1);
			rc.add(5,""+frame.framenumber);
			rc.add(5,""+pagenumber_maindb);
			rc.add(5,""+frame.committed);
								
			// add new line to output
			if (null != rc && rc.size() > 0) {
		
				output.add(rc);
				updateResultSet(rc);
			}

		} // end of for - cell pointer

		
		AppLog.debug("finished STEP2 -> cellpoint array completed");
		
	try 
	{	

		/***************************************************************
		 * STEP 3:
		 * 
		 * Scan unallocated space between header and  the cell
		 * content region 
		 * 
		 ***************************************************************/
		
		/* before we go to the free blocks an gaps let us first check the area between the header and 
		   the start byte of the cell content region */
		if (headerend < buffer.limit())
			buffer.position(headerend);
		else
			return -1;
			
		/* 	Although we have already reached the official end of the cell pointer array, 
		 *  there may be more pointers startRegion deleted records. They do not belong to the
		 *  official content region. We have to skip them, before we can search for more 
		 *  artifacts in the unallocated space. 
		 */
		
		byte garbage[] = new byte[2];
		
		int garbageoffset = -1;
		do
		{
			
			buffer.get(garbage);
			ByteBuffer ignore = ByteBuffer.wrap(garbage);
			garbageoffset = Auxiliary.TwoByteBuffertoInt(ignore);
			//System.out.println("garbage bytes " + buffer.position());
		} while (buffer.position() < ps && garbageoffset > 0);
		
		
		/*  Now, skip all zeros - no information to recover just empty space */
		byte zerob = 0;
		while(buffer.position() < ps && zerob == 0)
		{
			zerob = buffer.get();
		}
		
		/* mark the region startRegion the end of page header till end of zero space as visited */
		visit.set(headerend,buffer.position());
		
		/* go back one byte */
		buffer.position(buffer.position()-1);
		
		/* only if there is a significant number of bytes in the unallocated area, evaluate it more closely. */
		if (ccrstart - buffer.position() > 3)
		{
			/* try to read record as usual */
			LinkedList<String> rc;
			
			/* Tricky thing : data record could be partly overwritten with a new data record!!!  */
			/* We should read until the end of the unallocated area and not above! */
			rc = ct.readRecord(buffer.position(), buffer, ps, visit, type, ccrstart - buffer.position(),firstcol,withoutROWID,Global.WAL_ARCHIVE_FILE,framestart+24 + buffer.position());
			
			// add new line to output
			if (null != rc) { 
				

				String secondcol = rc.get(1);
				secondcol = Global.DELETED_RECORD_IN_PAGE  + secondcol;
				rc.set(1,secondcol);
				updateResultSet(rc);
			}
			
		}
		
		
		/***************************************************************
		 * STEP 4:
		 * 
		 * if there are still gaps, go for it and carve it  
		 * 
		 ***************************************************************/
		
		/* now we are ready to carve the rest of the page */
		//carve(content,null);
		
	} catch (Exception err) {
		err.printStackTrace();
		return -1;
	}

	return 0;
		
	}
	
	
	private void updateResultSet(LinkedList<String> line) 
	{
		// entry for table name already exists  
		if (resultlist.containsKey(line.getFirst()))
		{
			     ObservableList<ObservableList<String>> tablelist = resultlist.get(line.getFirst());
			     tablelist.add(FXCollections.observableList(line));  // add row 
		}
		
		// create a new data set since table name occurs for the first time
		else {
		          ObservableList<ObservableList<String>> tablelist = FXCollections.observableArrayList();
				  tablelist.add(FXCollections.observableList(line)); // add row 
				  resultlist.put(line.getFirst(),tablelist);  	
		}
	}

	/**
	 * Quick lookup. Does a given hex-String starts with Zeros?
	 * 
	 * @param s the String to check
	 * @return true, if zero bytes could be found
	 */
	static boolean allCharactersZero(String s) {
		if (!s.startsWith("0000"))
			return false;

		int n = s.length();
		for (int i = 1; i < n; i++)
			if (s.charAt(i) != s.charAt(0))
				return false;

		return true;
	}

	/**
	 * This method is used to read the database file into RAM.
	 * 
	 * @return a read-only ByteBuffer representing the WAL file content
	 * @throws IOException
	 */
	private void readFileIntoBuffer() throws IOException {

		/* read the complete file into a ByteBuffer */
		size = file.size();

		wal = ByteBuffer.allocateDirect((int) size);

		Future<Integer> result = file.read(wal, 0); // position = 0

		while (!result.isDone()) {

			// we can do something in between or we can do nothing ;-).
		}

		// set filepointer to begin of the file
		wal.position(0);

	}
	
	

	/**
	 * Starting with the current position of the wal-ByteBuffer read the next
	 * db-page.
	 * 
	 * @return
	 */
	protected ByteBuffer readPage() {

		ByteBuffer page = wal.slice();
		page.limit(ps);
		return page;
	}

	/**
	 * This method is called to carve a data page for records.
	 * 
	 * @param content page content as hex-string
	 */
	public void carve(ByteBuffer buffer, String content, Carver crv) {

		Carver c = crv;

		if (null == c)
			/* no type could be found in the first two bytes */
			/* Maybe the whole page was drop because of a drop component command ? */
			/* start carving on the complete page */
			c = new Carver(job, buffer, content, visit, ps);

		// Matcher mat = null;
		// boolean match = false;

		/* try to get component schema for the current page, if possible */
		TableDescriptor tdesc = null;
		if (job.pages.length > ps) {
			AbstractDescriptor ad = job.pages[ps];
			if (ad instanceof TableDescriptor)
				tdesc = (TableDescriptor) ad;
		}

		List<TableDescriptor> tab = tables;
		AppLog.debug(" tables :: " + tables.size());

		if (null != tdesc) {
			/* there is a schema for this page */
			tab = new LinkedList<TableDescriptor>();
			tab.add(tdesc);
			AppLog.debug(" added tdsec ");
		} else {
			AppLog.warning(" No component description!" + content);
			tab = tables;
		}

		LinkedList<Gap> gaps = findGaps();
		if (gaps.size() == 0) {
			AppLog.debug("no gaps anymore. Stopp search");
			return;
		}

		/* try out all component schema(s) */
		for (int n = 0; n < tab.size(); n++) {
			tdesc = tab.get(n);
			AppLog.debug("pagenumber :: " + pagenumber_maindb + " component size :: " + tab.size());
			AppLog.debug("n " + n);
			// TableDescriptor tdb = tab.get(n);

			/* access pattern for a particular component */
			String tablename = tab.get(n).tblname;
			AppLog.debug("WALReader Check component : " + tablename);
			if (tablename.startsWith("__FREELIST"))
				continue;
			/* create matcher object for constrain check */
			SerialTypeMatcher stm = new SerialTypeMatcher(buffer);

			gaps = findGaps();

			for (int a = 0; a < gaps.size(); a++) {

				Gap next = gaps.get(a);

				if (next.to - next.from > 10)
					/* do we have at least one match ? */
					if (c.carve(next.from + 4, next.to, stm, CarverTypes.NORMAL, tab.get(n))!= Global.CARVING_ERROR) {
						AppLog.debug("*****************************  STEP NORMAL finished with matches");

					}
			}

			gaps = findGaps();

			for (int a = 0; a < gaps.size(); a++) {

				Gap next = gaps.get(a);

				if (c.carve(next.from + 4, next.to, stm, CarverTypes.COLUMNSONLY, tab.get(n)) != Global.CARVING_ERROR) {
					AppLog.debug("*****************************  STEP COLUMNSONLY finished with matches");

				}
			}
 
			gaps = findGaps();

			for (int a = 0; a < gaps.size(); a++) {

				Gap next = gaps.get(a);

				if (c.carve(next.from + 4, next.to, stm, CarverTypes.FIRSTCOLUMNMISSING, tab.get(n)) != Global.CARVING_ERROR) {
					AppLog.debug("*****************************  STEP FIRSTCOLUMNMISSING finished with matches");

				}

			}

			/**
			 * When a record deletion occurs, the first 2 bytes of the cell are set to the
			 * offset value of next free block and latter 2 bytes covers the length of the
			 * current free block. Because of this, the first 4 bytes of a deleted cell
			 * differ startRegion the normal data. Accordingly, we need a different approach
			 * to recover the data records.
			 * 
			 * In most cases, at least the header length information is overwritten. Boyond
			 * this, sometimes, also the first column type field is overwritten too.
			 * 
			 * We have to cases:
			 * 
			 * (1) only the first column of the header is missing, but the rest of the
			 * header is intact.
			 * 
			 * (2) both header length field plus first column are overwritten.
			 * 
			 * [cell size | rowid | header size | header bytes | payload ]
			 * 
			 * for a deleted cell is looks maybe like this
			 * 
			 * [offset of next free block | length of the current free block | ]
			 */

			/* There are still gaps? */
			gaps = findGaps();

			for (int a = 0; a < gaps.size(); a++) {

				Gap next = gaps.get(a);

				/* one last try with 4+1 instead of 4 Bytes */
				c.carve(next.from + 4 + 1, next.to, stm, CarverTypes.FIRSTCOLUMNMISSING, tab.get(n));

			}

		} // end of tables ( component fingerprint )

		AppLog.debug("End of WALReader:parse()");

		
	}
	
	
	
	/**
	 *  This method can be used to write the result to a file or
	 *  to update tables in the user interface (in gui-mode). 
	 */
	public void output()
	{
		// Are we in GUI mode or in cli mode?
		if (job.gui != null) {
			
			
			System.out.println("Number of WAL records recovered: " + output.size());
	
			Iterator<LinkedList<String>> lines = output.iterator();
			
			// holds all data sets for all tables, whereas the tablename represents the key
			Hashtable<String,ObservableList<ObservableList<String>>> dataSets = new Hashtable<>();
			
			/* scan line by line */
			while(lines.hasNext()) {

			
				LinkedList<String> line = lines.next();

			   	
			   	/* if table name is empty -> assign data record to table __FREELIST */
			   	if (line.getFirst().trim().length()==0)
			   	{
			 
			        // Add the new element add the begin 
			        //line.add(0,"__FREELIST"); 
			 
			   	}
	
			   	
			   	// is there already a data set for this particular table
			   	if (dataSets.containsKey(line.get(0)))
				{
					 
					     ObservableList<ObservableList<String>> tablelist = dataSets.get(line.getFirst());
					     tablelist.add(FXCollections.observableList(line));  // add row 
				}
				
				// create a new data set before inserting the first row
				else {
				  ObservableList<ObservableList<String>> tablelist = FXCollections.observableArrayList();
				  tablelist.add(FXCollections.observableList(line));  // add row 
				  System.out.println("");
			   	  dataSets.put(line.getFirst(),tablelist); 
				}	
			
				
			} // end of for (String line : lines){...} 	
	
			
			Enumeration<String> tables = dataSets.keys();
			
			/* finally we can update the TableView for each table */
			while(tables.hasMoreElements())
			{	
					String tablename = tables.nextElement();
			        
					/* there are some times false-positive matches -> just skip them */
					if(tablename.startsWith("null#"))
						continue;
		
					String walpath = job.guiwaltab.get(tablename);
					if (walpath == null)
						continue;
					System.out.println("WALReader:: called update_table for " + tablename + " path  :: " + walpath);			
					job.gui.update_table(walpath,dataSets.get(tablename),true);
					
			}	
				
			
		} 
		else 
		{

			Path dbfilename = Paths.get(path);
			String name = dbfilename.getFileName().toString();

			LocalDateTime now = LocalDateTime.now();
			DateTimeFormatter df;
			df = DateTimeFormatter.ISO_DATE_TIME; // 2020-01-31T20:07:07.095
			String date = df.format(now);

			String filename = "results" + name + date + ".csv";
			
			String[] lines = output.toArray(new String[0]);
			job.writeResultsToFile(filename,lines);

		}
		
	}
	
	

	/**
	 * Check the BitSet for gaps, i.e. regions we still have to carve.
	 * 
	 * @return
	 */
	public LinkedList<Gap> findGaps() {
		LinkedList<Gap> gaps = new LinkedList<Gap>();

		int from = 0;

		/* are there any regions left in the page ? */
		for (int i = 0; i < ps; i++) {

			if (!visit.get(i)) {
				from = i;

				int to = i;

				while (!visit.get(++i) && i < (ps - 1)) {
					to++;
				}

				if (to - from > 10) {

					/* check for zero bytes */
					boolean isNull = false;
					if (buffer.get(from) == 0) {
						isNull = true;
						for (int index = from; index < to; index++) {
							if (0 != buffer.get(index))
								isNull = false;
						}
					}
					// skip NULL-Byte areas - mark as visited
					if (isNull)
						visit.set(from, to);
					else {

						gaps.add(new Gap(from, to));
					}
				}
				from = i;

			}

		} // end of finding gaps in BitSet

		return gaps;
	}

	/**
	 * This method is called to carve a data page for records.
	 * 
	 * @param content page content as hex-string
	 */
	public void carve(String content, Carver crv) {

		Carver c = crv;

		if (null == c)
			/* no type could be found in the first two bytes */
			/* Maybe the whole page was drop because of a drop component command ? */
			/* start carving on the complete page */
			c = new Carver(job, buffer, content, visit, pagenumber_maindb);

		// Matcher mat = null;
		// boolean match = false;

		/* try to get component schema for the current page, if possible */
		TableDescriptor tdesc = null;
		if (job.pages.length > pagenumber_maindb) {
			AbstractDescriptor ad = job.pages[pagenumber_maindb];
			if (ad instanceof TableDescriptor)
				tdesc = (TableDescriptor) ad;
		}

		List<TableDescriptor> tab = tables;
		AppLog.debug(" tables :: " + tables.size());

		if (null != tdesc) {
			/* there is a schema for this page */
			tab = new LinkedList<TableDescriptor>();
			tab.add(tdesc);
			AppLog.debug(" added tdsec ");
		} else {
			AppLog.warning(" No component description!");
			tab = tables;
		}

		LinkedList<Gap> gaps = findGaps();
		if (gaps.size() == 0) {
			AppLog.debug("no gaps anymore. Stopp search");
			return;
		}

		/* try out all component schema(s) */
		for (int n = 0; n < tab.size(); n++) {
			tdesc = tab.get(n);
			AppLog.debug("pagenumber :: " + pagenumber_maindb + " component size :: " + tab.size());
			AppLog.debug("n " + n);
			// TableDescriptor tdb = tab.get(n);

			/* access pattern for a particular component */
			String tablename = tab.get(n).tblname;
			AppLog.debug("Check component : " + tablename);
			if (tablename.startsWith("__FREELIST"))
				continue;
			/* create matcher object for constrain check */
			SerialTypeMatcher stm = new SerialTypeMatcher(buffer);

			gaps = findGaps();

			for (int a = 0; a < gaps.size(); a++) {

				Gap next = gaps.get(a);

				if (next.to - next.from > 10)
					/* do we have at least one match ? */
					if (c.carve(next.from + 4, next.to, stm, CarverTypes.NORMAL, tab.get(n)) != Global.CARVING_ERROR) {
						AppLog.debug("*****************************  STEP NORMAL finished with matches");

					}
			}

			gaps = findGaps();

			for (int a = 0; a < gaps.size(); a++) {

				Gap next = gaps.get(a);

				if (c.carve(next.from + 4, next.to, stm, CarverTypes.COLUMNSONLY, tab.get(n)) != Global.CARVING_ERROR) {
					AppLog.debug("*****************************  STEP COLUMNSONLY finished with matches");

				}
			}

			gaps = findGaps();

			for (int a = 0; a < gaps.size(); a++) {

				Gap next = gaps.get(a);

				if (c.carve(next.from + 4, next.to, stm, CarverTypes.FIRSTCOLUMNMISSING, tab.get(n)) != Global.CARVING_ERROR) {
					AppLog.debug("*****************************  STEP FIRSTCOLUMNMISSING finished with matches");

				}

			}

		}
	}
	
	
}
