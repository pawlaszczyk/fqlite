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
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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
 * The class analyses a Rollback Journal file and writes the found records into a file.
 * 
 * From the SLite documentation:
 * 
 * "The rollback journal is usually created when a transaction is first started and 
 *  is usually deleted when a transaction commits or rolls back. The rollback journal file 
 *  is essential for implementing the atomic commit and rollback capabilities of SQLite."
 * 
 * 
 * 
 * @author pawlaszc
 *
 */
public class RollbackJournalReader{

	public static final String MAGIC_HEADER_STRING = "d9d505f920a163d7";
	
	/* An asynchronous channel for reading, writing, and manipulating a file. */
	public AsynchronousFileChannel file;

	/* this is a multi-threaded program -> all data are saved to the list first*/
	ConcurrentHashMap<String,ObservableList<LinkedList<String>>> resultlist = new ConcurrentHashMap<>();
	
	/* This buffer holds RollbackJournal-file in RAM */
	ByteBuffer rollbackjournal;

	/* total size of RollbackJournal-file in bytes */
	long size;

	/* pagesize */
	public int ps;

	/* path to RollbackJournal-file */
	public String path;

	/* flag for already visited Bytes of the page */
	BitSet visit = null;

	
	/* reference to the MAIN class */
	Job job;

	/* number of page that is currently analyzed */
	int pagenumber_rol;
	int pagenumber_maindb;
	
	long pagecount;
	long nounce;
	long pages;
	long sectorsize;
	long journalpagesize;

	boolean withoutROWID = false;


	/* offers a lot of useful utility functions */
	private Auxiliary ct;

	/* knowlegde store */
	private StringBuffer firstcol = new StringBuffer();

	/* buffer that holds the current page */
	ByteBuffer buffer;

	public static List<TableDescriptor> tables = new LinkedList<TableDescriptor>();
	/* this is a multi-threaded program -> all data are saved to the list first */

	/* outputlist */
	ConcurrentLinkedQueue<LinkedList<String>> output = new ConcurrentLinkedQueue<LinkedList<String>>();
	

	/* file pointer */
	int journalpointer = 0;
	

	/**
	 * Constructor.
	 * 
	 * @param path    full qualified file name to the RollbackJournal archive
	 * @param job reference to the Job class
	 */
	public RollbackJournalReader(String path, Job job) {
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
		Path p = Paths.get(path);

		
		/*
		 * we have to do this before we open the database because of the concurrent
		 * access
		 */

		/* try to open the db-file in read-only mode */
		try {
			file = AsynchronousFileChannel.open(p, StandardOpenOption.READ);
		} catch (Exception e) {
            AppLog.error("Cannot open RollbackJournal-file" + p.getFileName());
			return;
		}

		/** Caution!!! we read the complete file into RAM **/
		try {
			readFileIntoBuffer();
		} catch (IOException e) {

			e.printStackTrace();
		}
		
		try {
			if(file.size() <= 512)
			{	
				System.out.println("RollbackJournal-File is empty. Skip analyzing.");
					return;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}	
		
		
		
		/*
		 * In practice when a transaction is committed it seems that the journal 
		 * header is normally zeroed and the data in the journal remains. 
		 * This is not a problem when it comes to reading each page from 
		 * the journal as we can obtain the page size from the database itself.
		 */

		/*******************************************************************/

		
		/*
		 * A valid rollback journal begins with a header in the following format:
		 * 
		 * Offset	Size	Description
		 * 0 		8 		Header string: 0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7
		 * 8	 	4 		The "Page Count" - The number of pages in the next segment of the journal, or -1 to mean all content to the end of the file
		 * 12	 	4 		A random nonce for the checksum
		 * 16	 	4 		Initial size of the database in pages
		 * 20 		4 		Size of a disk sector assumed by the process that wrote this journal.
		 * 24 		4 		Size of pages in this journal. 
		 */
		
		/* read header of the WAL file - the first 28 bytes */
		ByteBuffer header = ByteBuffer.allocate(28);

		
		Future<Integer> result = file.read(header, 0); // position = 0

		while (!result.isDone()) {

			// we can do something in between or just wait ;-).
		}

		header.flip();
		
		
		byte head[] = new byte[8];
		header.get(head);
		

		if (Auxiliary.bytesToHex(head).equals(MAGIC_HEADER_STRING))
		{
			AppLog.info("header is okay. seems to be an rollback journal file.");
		}
		else 
		{
				AppLog.info("This file does not contain a valid header.");
				
		}
		
		pagecount = Integer.toUnsignedLong(header.getInt());
		AppLog.info(" pagecount " + pagecount);

		nounce = Integer.toUnsignedLong(header.getInt());
		AppLog.info(" nounce " + nounce);

		pages = Integer.toUnsignedLong(header.getInt());
		AppLog.info(" pages " + pages);
		
		sectorsize = Integer.toUnsignedLong(header.getInt());
		AppLog.info(" sector size  " + sectorsize);
		
		journalpagesize = Integer.toUnsignedLong(header.getInt());
		AppLog.info(" journal page size  " + journalpagesize);

		
	    journalpointer = 512; // this is the position, where the first frame should be


		/* initialize the BitSet for already visited location within */

		visit = new BitSet(ps);

		
		
		boolean next = false;
		int numberofpages = 0;
		do
		{
			rollbackjournal.position(journalpointer);
			/* get the page number of the journal page in main db */
			
		    pagenumber_maindb = rollbackjournal.getInt();
			AppLog.debug("pagenumber of journal-entry " + pagenumber_maindb);
			
			//* offset of the journal page -> save it */
			int pageoffset = rollbackjournal.position();
			
			/* now we can read the page - it follows immediately after the frame header */
	
			/* read the db page into buffer */
			buffer = readPage();
	
			numberofpages++;
			pagenumber_rol = numberofpages;
			
			analyzePage(pagenumber_maindb,pageoffset); // we need the original page number to compute the offset for the diff between journal and database
			
			/* set pointer to next journal record  -> currentpos + 4 Byte for the page number in mainDB + pagesize + 4 Byte for Checksum */ 
			journalpointer += (4 + ps + 4);
			
			//System.out.println(" Position in RollbackJournal-file " + journalpointer + " " );
			
			/*  More pages to analyze ? */
			if(journalpointer + ps  <= size)
			{
				next = true;
			}
			else
				next = false;
			
		}while(next);

		AppLog.info("Lines after RollbackJournal-file recovery: " + output.size());
		AppLog.info("Number of pages in RollbackJournal-file" + numberofpages);
	
	}
	
	private void updateResultSet(LinkedList<String> line) 
	{
		// entry for table name already exists  
		if (resultlist.containsKey(line.getFirst()))
		{
			     ObservableList<LinkedList<String>> tablelist = resultlist.get(line.getFirst());
			     tablelist.add(line);  // add row 
		}
		
		// create a new data set since table name occurs for the first time
		else {
		          ObservableList<LinkedList<String>> tablelist = FXCollections.observableArrayList();
				  tablelist.add(line); // add row 
				  resultlist.put(line.getFirst(),tablelist);  	
		}
	}

	/**
	 * Analyze the actual database page and try to recover regular and deleted content.
	 * 
	 * @return int success
	 */
	public int analyzePage(int originalpagenumber,int pageoffset) {

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
				System.out.println(" DROPPED PAGE !!!");
				/* no overflow page -> carve for data records - we do our best! ;-) */
				carve(content, null);
			}
			/*
			 * otherwise it seems to be a overflow page - however, that is not 100% save !!!
			 */

			/* we have to leave in any case */
			return 0;
		}

		/************** skip unkown page types ******************/

		// no leaf page -> skip this page
		if (type < 0) {
			AppLog.info("No Data page. " + pagenumber_rol);
			return -1;
		} else if (type == 5) {
			AppLog.info("Internal Table page " + pagenumber_rol);
			return -1;
		} else if (type == 10) {
			AppLog.info("Index leaf page " + pagenumber_rol);
			withoutROWID = true;

		} else {
			AppLog.info("Data page " + pagenumber_rol+ " Offset: " + (rollbackjournal.position() - ps));
		}

		/************** regular leaf page with data ******************/

		// boolean freeblocks = false;
		if (type == 8) {
			// offset 1-2 let us find the first free block offset for carving
			byte fboffset[] = new byte[2];
			buffer.position(1);
			buffer.get(fboffset);

		}

		

		// found Data-Page - determine number of cell pointers at offset 3-4 of this
		// page
		byte cpn[] = new byte[2];
		buffer.position(3);
		buffer.get(cpn);

		// get start pointer for the cell content region
		byte ccr[] = new byte[2];
		buffer.position(5);
		buffer.get(ccr);

		ByteBuffer contentregionstart = ByteBuffer.wrap(ccr);
		Auxiliary.TwoByteBuffertoInt(contentregionstart);

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
			else
				buffer.position(8 + 2 * i);
			buffer.get(pointer);
			ByteBuffer celladdr = ByteBuffer.wrap(pointer);
			int celloff = Auxiliary.TwoByteBuffertoInt(celladdr);

			if (last > 0) {
				if (celloff == last) {
					continue;
				}
			}
			last = celloff;
				String hls = Auxiliary.Int2Hex(celloff); 
				AppLog.debug(pagenumber_rol + " -> " + celloff + " " + "0" + hls);
			hls.trim();
			
            LinkedList<String> record = null;
			
			try {
				record = ct.readRecord(celloff, buffer, pagenumber_maindb, visit, type, Integer.MAX_VALUE, firstcol, withoutROWID, Global.ROLLBACK_JOURNAL_FILE, pageoffset + celloff);
			} catch (IOException e) {
				e.printStackTrace();
			}

			// add new line to output
			if (null != record && record.size() > 0) {

		
				output.add(record);
				updateResultSet(record);
			}

		} // end of for - cell pointer

		
		AppLog.debug("finished STEP2 -> cellpoint array completed");

		
		return 0;

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
	 * @return a read-only ByteBuffer representing the RollbackJournal file content
	 * @throws IOException
	 */
	private void readFileIntoBuffer() throws IOException {

		/* read the complete file into a ByteBuffer */
		size = file.size();

		rollbackjournal = ByteBuffer.allocateDirect((int) size);

		Future<Integer> result = file.read(rollbackjournal, 0); // position = 0

		while (!result.isDone()) {

			// we can do something in between or we can do nothing ;-).
		}

		// set filepointer to begin of the file
		rollbackjournal.position(0);

	}

	/**
	 * Starting with the current position of the RollbackJournal-ByteBuffer 
	 * 
	 * read the next db-page.
	 * 
	 * @return
	 */
	protected ByteBuffer readPage() {

		byte [] page = new byte[ps];
		
		rollbackjournal.get(page);
		
		ByteBuffer content = ByteBuffer.wrap(page);
		
		return content;
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

				if (c.carve(next.from + 4, next.to, stm, CarverTypes.FIRSTCOLUMNMISSING, tab.get(n))!= Global.CARVING_ERROR) {
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

		AppLog.debug("End of journal parse");
		
	}
	
	/**
	 *  This method can be used to write the result to a file or
	 *  to update tables in the user interface (in gui-mode). 
	 */
	public void output()
	{
		if (job.gui != null) {
			
			AppLog.info("Number of records recovered: " + output.size());
			//String[] lines = output.toArray(new String[0]);
			//Arrays.sort(lines);

			Iterator<LinkedList<String>> lines = output.iterator();

			// holds all data sets for all tables, whereas the tablename represents the key
			Hashtable<String,ObservableList<LinkedList<String>>> dataSets = new Hashtable<>();
			
			while(lines.hasNext())
			{
				LinkedList<String> line = lines.next();
		
			 	/* if table name is empty -> assign data record to table __FREELIST */
			   	if (line.getFirst().trim().length()==0)
			   	{
			 
			        // Add the new element add the begin 
			       // line.set(0,"__FREELIST"); 			   	
			   	}
				
			   	// is there already a data set for this particular table
							
				if (dataSets.containsKey(line.get(0)))
				{
					 
					     ObservableList<LinkedList<String>> tablelist = dataSets.get(line.getFirst());
					     tablelist.add(line);  // add row 
				}
				
				// create a new data set before inserting the first row
				else {
					  	  ObservableList<LinkedList<String>> tablelist = FXCollections.observableArrayList();
					  	  tablelist.add(line); // add row 
					  	  dataSets.put(line.getFirst(),tablelist); 
				}	
			
	            // TODO: Remove empty tables from rollback journal		
	
		
			}
		
			//Platform.runLater(() -> {
			//	fqlite.ui.HexViewFX hex = new fqlite.ui.HexViewFX(GUI.topContainer,this.path,this.rollbackjournal);
 			//   hexview = hex; 
			//});
			
			Enumeration<String> tables = dataSets.keys();
			
			/* finally we can update the TableView for each table */
			while(tables.hasMoreElements())
			{	
				String tablename = tables.nextElement();
		        /* get tree path i.e. /data bases/02-05.db/users */
				String rpath = job.guiroltab.get(tablename);
				job.gui.update_table(rpath,dataSets.get(tablename),false);
				
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
