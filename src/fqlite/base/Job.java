package fqlite.base;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import fqlite.base.WALReader.WALFrame;
import fqlite.descriptor.AbstractDescriptor;
import fqlite.descriptor.IndexDescriptor;
import fqlite.descriptor.TableDescriptor;
import fqlite.types.BLOBElement;
import fqlite.ui.DBPropertyPanel;
import fqlite.ui.NodeObject;
import fqlite.ui.RollbackPropertyPanel;
import fqlite.ui.WALPropertyPanel;
import fqlite.util.Auxiliary;
import fqlite.util.BLOB2Disk;
import fqlite.util.ByteSeqSearcher;
import javafx.application.Platform;
import javafx.collections.ObservableList;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.TreeItem;
import javafx.scene.image.Image;


/*
---------------
Job.java
---------------
(C) Copyright 2020.

Original Author:  Dirk Pawlaszczyk
Contributor(s):   -;


Project Info:  http://www.hs-mittweida.de

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

Dieses Programm ist Freie Software: Sie können es unter den Bedingungen
der GNU General Public License, wie von der Free Software Foundation,
Version 3 der Lizenz oder (nach Ihrer Wahl) jeder neueren
veröffentlichten Version, weiterverbreiten und/oder modifizieren.

Dieses Programm wird in der Hoffnung, dass es nützlich sein wird, aber
OHNE JEDE GEWÄHRLEISTUNG, bereitgestellt; sogar ohne die implizite
Gewährleistung der MARKTFÄHIGKEIT oder EIGNUNG FÜR EINEN BESTIMMTEN ZWECK.
Siehe die GNU General Public License für weitere Details.

Sie sollten eine Kopie der GNU General Public License zusammen mit diesem
Programm erhalten haben. Wenn nicht, siehe <http://www.gnu.org/licenses/>.

*/
/**
 * Core application class. It is used to recover lost SQLite records from a
 * sqlite database file. As a carving utility, it is binary based and can recover
 * deleted entries from sqlite 3.x database files.
 * 
 * Note: For each database file exactly one Job object is created. This class has
 * a reference to the file on disk. 
 * 
 * From the sqlite web-page: "A database file might contain one or more pages
 * that are not in active use. Unused pages can come about, for example, when
 * information is deleted startRegion the database. Unused pages are stored on the
 * freelist and are reused when additional pages are required."
 * 
 * This class makes use of this behavior.
 * 
 * 
 * 
 *     __________    __    _ __     
 *    / ____/ __ \  / /   (_) /____ 
 *   / /_  / / / / / /   / / __/ _ \
 *  / __/ / /_/ / / /___/ / /_/  __/
 * /_/    \___\_\/_____/_/\__/\___/ 
 * 
 * 
 * 
 * @author Dirk Pawlaszczyk
 * @version 2.0
 */
public class Job extends Base {

	/* the byte buffer representing the database file in RAM */
	public ByteBuffer db;
	
	
	/* since version 1.2 - support for write ahead logs WAL */
	public boolean readWAL = false;
	String walpath = null;
	public WALReader wal = null;
	Hashtable<String, String> guiwaltab = new Hashtable<String, String>();

	/* since version 1.2 - support for write Rollback Journal files */
	public boolean readRollbackJournal = false;
	String rollbackjournalpath = null;
	public RollbackJournalReader rol = null;
	Hashtable<String, String> guiroltab = new Hashtable<String, String>();
	
	
	
    /* some constants */
	final static String MAGIC_HEADER_STRING = "53514c69746520666f726d6174203300";
	final static String NO_AUTO_VACUUM = "00000000";
	final static String NO_MORE_ENTRIES = "00000000";
	final static String LEAF_PAGE = "0d";
	final static String INTERIOR_PAGE = "05";
	final static String ROW_ID = "00";
	
	
	/* the next fields hold informations for the GUI */
	public GUI gui = null;
	public TreeItem<NodeObject> dbNode = null;
	public TreeItem<NodeObject> walNode = null;
	public TreeItem<NodeObject> rjNode = null;
	
	/* if a page is handled as a regular overflow page -> don't analyze those pages */
	public ConcurrentLinkedQueue<Integer> overflowpages = new ConcurrentLinkedQueue<>();
	
	Hashtable<String, String> guitab = new Hashtable<String, String>();
	public HashMap<String,Number> timestamps = new HashMap<String,Number>();
	
	/* property panel for the user interface - only in gui-mode */
	DBPropertyPanel panel = null;
	WALPropertyPanel walpanel = null;
	RollbackPropertyPanel rolpanel = null;
	
	/* size of file */
	long size = 0;
	
	/* path - used to locate a file in a file system */
	public String path;
	
	public String filename;
	
	/* An asynchronous channel for reading, writing, and manipulating the database file. */
	public AsynchronousFileChannel file;
	
	/* this field represent the database encoding */
	public static Charset db_encoding = StandardCharsets.UTF_8;
	
	/* the list is used to export the recovered data records */
	List<String> lines = new LinkedList<String>();
	
	/* this is a multi-threaded program -> all data are saved to the list first*/
	ConcurrentHashMap<String,ObservableList<LinkedList<String>>> resultlist = new ConcurrentHashMap<>();
	
	
	/* header fields */
	
	String headerstring;
	String PRAGMA_journal_mode = "OFF";
	byte ffwversion;
	byte ffrversion;
	byte reservedspace;
	byte maxpayloadfrac;
	byte minpayloadfrac;
	byte leafpayloadfrac;
	long filechangecounter;
	long inheaderdbsize;
	long sizeinpages;
	long schemacookie;
	long schemaformatnumber;
	long defaultpagecachesize;
	long userversion;
	long vacuummode;
	long versionvalidfornumber;
	long avacc;
	
	/* Virtual Tables */
	public Map<String,TableDescriptor> virtualTables = new HashMap<String,TableDescriptor>();
	
	int scanned_entries = 0;
	
	Hashtable<String, String> tblSig;
	boolean is_default = false;
	public List<TableDescriptor> headers = new ArrayList<TableDescriptor>();
	public List<IndexDescriptor> indices = new ArrayList<IndexDescriptor>();
	public AtomicInteger runningTasks = new AtomicInteger();
	int tablematch = 0;
	int indexmatch = 0; 
	public Map<Long,BLOBElement> BLOBs = new HashMap<Long,BLOBElement>();
	public Map<Long,Image> Thumbnails = new HashMap<Long,Image>();
	public Map<Long,String> FileCache = new HashMap<Long,String>();
	
	// Offset (table name plus rowid) + object hash value to detect updates in Journal Mode
	public Map<String,Integer> LineHashes = new HashMap<String,Integer>();
		
	// Offset (table name plus rowid) + checkpoint hashes
	public Map<String,List<Integer>> TimeLineHashes = new HashMap<String,List<Integer>>();
	
	
	AtomicInteger numberofcells = new AtomicInteger();
	
	Set<Integer> allreadyvisit;
	
	/* all unfinished tasks are hold in this list*/
	List<RecoveryTask> tasklist = new LinkedList<RecoveryTask>();
	
	/* this array holds a description of the associated component for each data page, if known */
	public AbstractDescriptor[] pages;
	
	/* each db-page has only one type */
	int[] pagetype;
	
	/* page size */
	public int ps = 0;
	public int numberofpages = 0;
	/* free page pragma fields in db header */
	int fpnumber = 0;
	int fphead = 0;
	String sqliteversion = "";
	boolean autovacuum = false;
	long totalbytes = 0;
	
	/* if a page has been scanned it is marked as checked */
	AtomicReferenceArray<Boolean> checked;
	
	public AtomicInteger hits = new AtomicInteger();
 
	/* sometimes, the db-file is empty (no schema exists in databas and data + schema information only reside in the
	 * WAL-Archive) -> emptydb is set to true in this case 
	 */
	boolean emptydb = false;
	  
	/******************************************************************************************************/
	
	/**
	 * This method is used to read the database file into RAM.
	 * @return a read-only ByteBuffer representing the db content
	 * @throws IOException
	 */
	private void readFileIntoBuffer() throws IOException {
		/* read the complete file into a ByteBuffer */
		size = file.size();
		
		db = ByteBuffer.allocateDirect((int) size);
		
		Future<Integer> result = file.read(db, 0); // position = 0

		while (!result.isDone()) {

			// we can do something in between or we can do nothing ;-).
		}

		// set file pointer to begin of the file
		db.position(0);

	}
	
	
	private ByteBuffer readWALIntoBuffer(String walpath) throws IOException {
		
		Path p = Paths.get(walpath);

		/* First - try to analyze the db-schema */
		/*
		 * we have to do this before we open the database because of the concurrent
		 * access
		 */

		/* try to open the wal-file in read-only mode */
		file = AsynchronousFileChannel.open(p, StandardOpenOption.READ);
		
		if(null == file)
			return null;
		
		/* read the complete file into a ByteBuffer */
		size = file.size();
		
		ByteBuffer bb = ByteBuffer.allocateDirect((int) size);
		
		Future<Integer> result = file.read(bb, 0); // position = 0

		while (!result.isDone()) {

			// we can do something in between or we can do nothing ;-).
		}

		// set file pointer to begin of the file
		bb.position(0);

		return bb;
	}
	
	public void setPropertyPanel(DBPropertyPanel p)
	{
		this.panel = p;
	}
	
	public void setWALPropertyPanel(WALPropertyPanel p)
	{
		this.walpanel = p;
	}
	

	public void setRollbackPropertyPanel(RollbackPropertyPanel p)
	{
		this.rolpanel = p;
	}
	
	public void setTreeItem(TreeItem<NodeObject> dbNode)
	{
		this.dbNode = dbNode;
	}
	
	public TreeItem<NodeObject> getTreeItem()
	{
		return dbNode;
	}
	
	
	public String[][] getHeaderProperties()
	{
		if(ffwversion == ffrversion)
		{
			/*
			 * Official Database File Format (sqlite.org):
			 * 
			 * The file format write version and file format read version at 
			 * offsets 18 and 19 are intended to allow for enhancements of the 
			 * file format in future versions of SQLite. In current versions of 
			 * SQLite, both of these values are 1 for rollback journalling modes
			 * and 2 for WAL journalling mode. If a version of SQLite coded to 
			 * the current file format specification encounters a database file 
			 * where the read version is 1 or 2 but the write version is greater 
			 * than 2, then the database file must be treated as read-only. 
			 * If a database file with a read version greater than 2 is encountered,
			 *  then that database cannot be read or written.

			 */
			
		    switch(ffwversion)
		    {
		    	case 0: PRAGMA_journal_mode = "OFF";
		    	  		break;
		    	case 1: if(readRollbackJournal)
		    				PRAGMA_journal_mode = "Rollback Journal PERSIST";
		    			else
		    				PRAGMA_journal_mode = "Rollback Journal OFF (no file)";		    	    	
		    			break;
		    	case 2: PRAGMA_journal_mode = "WAL";
    					break;
    			default: 
    					if (ffwversion > 2 && (ffrversion == 1 || ffrversion == 2))
    					{
    						PRAGMA_journal_mode = "READ ONLY";
    					}
    					else
    					{
    						PRAGMA_journal_mode = "NO READ OR WRITE";
    					}
		    }
		}
		
		
		String [][] prop = {{"0","The header string",headerstring},
				{"16","The database page size in bytes",String.valueOf(ps)},
				{"18","File format write version",String.valueOf(ffwversion) + " Journal Mode ->" + PRAGMA_journal_mode},
				{"19","File format read version",String.valueOf(ffrversion)},
				{"20","Unused reserved space at the end of each page ",String.valueOf(reservedspace)},
				{"21","Maximum embedded payload fraction. Must be 64.",String.valueOf(maxpayloadfrac)},
				{"22","Minimum embedded payload fraction. Must be 32.",String.valueOf(minpayloadfrac)},
				{"23","Leaf payload fraction. Must be 32.",String.valueOf(leafpayloadfrac)},
				{"24","File change counter.",String.valueOf(filechangecounter)},
				{"28","Size of the database file in pages. ",String.valueOf(sizeinpages)},
				{"32","Page number of the first freelist trunk page.",String.valueOf(fphead)},
				{"36","Total number of freelist pages.",String.valueOf(fpnumber)},
				{"40","The schema cookie.",String.valueOf(schemacookie)},
				{"44","The schema format number. Supported schema formats are 1, 2, 3, and 4.",String.valueOf(schemaformatnumber)},
				{"48","Default page cache size.",String.valueOf(defaultpagecachesize)},
				{"52","The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.",String.valueOf(avacc)+" (" + (avacc > 0 ? true : false) + ")"},
				{"56","The database text encoding.",String.valueOf(db_encoding.displayName())},
				{"60","The \"user version\"",String.valueOf(userversion)},
				{"64","True (non-zero) for incremental-vacuum mode. False (zero) otherwise.",String.valueOf(vacuummode)+" (" + (vacuummode > 0 ? true : false) + ")"},
				{"92","The version-valid-for number.",String.valueOf(versionvalidfornumber)},
				{"96","SQLITE_VERSION_NUMBER",String.valueOf(sqliteversion)}};
		
		
		
		
		
	
		return prop;
	}
	
	public String[][] getWALHeaderProperties()
	{
		String [][] prop = {{"0","HeaderString",wal.headerstring},
				{"4","File format version",String.valueOf(wal.ffversion)},
				{"8","Database page size",String.valueOf(wal.ps)},
				{"12","Checkpoint sequence number",String.valueOf(wal.csn)},
				{"16","Salt-1",String.valueOf(wal.hsalt1)},
				{"20","Salt-2",String.valueOf(wal.hsalt2)},
				{"24","Checksum1",String.valueOf(wal.hchecksum1)},
				{"28","Checksum2",String.valueOf(wal.hchecksum2)}};
		
		return prop;
	}
	
	public String[][] getRollbackHeaderProperties()
	{
		String [][] prop = {{"0","HeaderString",RollbackJournalReader.MAGIC_HEADER_STRING},
				{"8","number of pages",String.valueOf(rol.pagecount)},
				{"12","nounce for checksum",String.valueOf(rol.nounce)},
				{"16","pages",String.valueOf(rol.pages)},
				{"20","sector size ",String.valueOf(rol.sectorsize)},
				{"24","journal page size",String.valueOf(rol.journalpagesize)}};
		
		return prop;
	}
	
	public String[][] getCheckpointProperties()
	{
		ArrayList<String []> prop = new ArrayList<String []>();
		
		Set<Long> data = wal.checkpoints.descendingKeySet();
		
		Iterator<Long> it = data.iterator();
		
		while (it.hasNext())
		{
			Long salt1 = it.next();
			
			LinkedList<WALFrame> list = wal.checkpoints.get(salt1);
			
			Iterator<WALFrame> frames = list.iterator();
			
			while (frames.hasNext())
			{
				WALFrame current = frames.next();
				
			    String[] line = new String[5];
			    line[0] = String.valueOf(current.salt1);
			    line[1] = String.valueOf(current.salt2);
			    line[2] = String.valueOf(current.framenumber);
			    line[3] = String.valueOf(current.pagenumber);
			    line[4] = String.valueOf(current.committed);
			    
			    prop.add(line);
			    
			}
		}
		
		String[][] result = new String[prop.size()][5];
		prop.toArray(result);
		
		return result;
	}
	
	public LinkedHashMap<String,String[][]> getTableColumnTypes() 
	{
		
		Iterator<TableDescriptor> it1 = headers.iterator();
		LinkedHashMap<String,String[][]> ht = new LinkedHashMap<String,String[][]>();
		
		
		while (it1.hasNext())
		{
			TableDescriptor td = it1.next();
		    String[] names = (String[])td.columnnames.toArray(new String[0]);
		    String[] types = (String[])td.serialtypes.toArray(new String[0]);
		    String[] sqltypes = (String[])td.sqltypes.toArray(new String[0]);
		    String[] tableconstraints = null;
		    String[] constraints = null;
		    
			/* check, if there exists global constraints to the table */
		    if (null != td.tableconstraints)
	    	{	
	    		tableconstraints = (String[])td.tableconstraints.toArray(new String[0]);	    		
	    	}
		    /* check, if there are constraints on one of the columns */
		    if (null != td.constraints)
		    {
		    	constraints = (String[])td.constraints.toArray(new String[0]);    			    	
		    }
		    
		    String[][] row = null;
		    if(null != tableconstraints && null != constraints)
		    {
			    row = new String[][]{names,types,sqltypes,constraints,tableconstraints};
		    		    	
		    }
		    else if (null != tableconstraints)
		    {
		       	row = new String[][]{names,types,sqltypes,tableconstraints};
				 
		    }
		    else if (null != constraints)
		    {
		       	row = new String[][]{names,types,sqltypes,constraints};
				    	
		    }
		    else
		    {
		    	row = new String[][]{names,types,sqltypes};
		    }
		    
			ht.put(td.tblname,row);
		}
		
		Iterator<IndexDescriptor> it2 = indices.iterator();
		
		while (it2.hasNext())
		{
			IndexDescriptor id = it2.next();
			String[] names = (String[])id.columnnames.toArray(new String[0]);
			String[] types = (String[])id.columntypes.toArray(new String[0]);
		    
		    String[][] row = null;
	    	row = new String[][]{names,types};

			ht.put("idx:" + id.idxname,row);
		}	

		return ht;
	}
	

	
	public String[][] getSchemaProperties()
	{
		String [][] prop = new String[headers.size() + indices.size()][6];//{{"","",""},{"","",""}};
		int counter = 0;
		
		Iterator<TableDescriptor> it1 = headers.iterator();
		
		while (it1.hasNext())
		{
			TableDescriptor td = it1.next();
			
		    if (!td.tblname.startsWith("__"))
		    	prop[counter] = new String[]{"Table",td.tblname,String.valueOf(td.root),td.sql,String.valueOf(td.isVirtual()),String.valueOf(td.ROWID)};
			counter++;			
		}
		
		Iterator<IndexDescriptor> it2 = indices.iterator();
		
		while (it2.hasNext())
		{
			IndexDescriptor td = it2.next();
	
			prop[counter] = new String[]{"Index",td.idxname,String.valueOf(td.root),td.getSql(),"",""};
			counter++;			
		}
		
		return prop;
	}

	
	public void updateRollbackPanel()
	{
		rolpanel.initHeaderTable(this.getRollbackHeaderProperties());	
	}
	
	public void updatePropertyPanel()
	{
		panel.initHeaderTable(getHeaderProperties());
		panel.initSchemaTable(getSchemaProperties());
		panel.initColumnTypesTable(getTableColumnTypes());
	}

	public void updateWALPanel()
	{
		walpanel.initHeaderTable(getWALHeaderProperties());
		walpanel.initCheckpointTable(getCheckpointProperties());
	}
	
	/**
	 * This is the main processing loop of the program.
	 * 
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	protected int processDB() throws InterruptedException, ExecutionException {

		allreadyvisit = ConcurrentHashMap.newKeySet();

		try {

			Path p = Paths.get(path);
			this.filename = p.getFileName().toString();
			this.path = p.toString();

			/* First - try to analyze the db-schema 	
			 * we have to do this before we open the database because of the concurrent
			 * access in multi threading mode
			 */

			/* try to open the db-file in read-only mode */
			file = AsynchronousFileChannel.open(p, StandardOpenOption.READ);

			/** Caution!!! we read the complete file into RAM **/
			readFileIntoBuffer();

			/* read header of the sqlite db - the first 100 bytes */
			ByteBuffer buffer = ByteBuffer.allocate(100);

			Future<Integer> result = file.read(buffer, 0); // position = 0

			while (!result.isDone()) {

				// we can do something in between or we just wait ;-).
			}

			// set filepointer to begin of the file
			buffer.flip();

			/* The first 100 bytes of the database file comprise the database file header. 
			 * The database file header is divided into fields as shown by the table below. 
			 * All multibyte fields in the database file header are stored with the must 
			 * significant byte first (big-endian).
			 * 
			 * 0	16	The header string: "SQLite format 3\000"
			 * 16	 2	The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
			 * 18    1	File format write version. 1 for legacy; 2 for WAL.
		     * 19  	 1	File format read version. 1 for legacy; 2 for WAL.
			 * 20    1	Bytes of unused "reserved" space at the end of each page. Usually 0.
			 * 21	 1	Maximum embedded payload fraction. Must be 64.
			 * 22 	 1	Minimum embedded payload fraction. Must be 32.
			 * 23 	 1	Leaf payload fraction. Must be 32.
			 * 24  	 4	File change counter.
			 * 28 	 4	Size of the database file in pages. The "in-header database size".
			 * 32  	 4	Page number of the first freelist trunk page.
			 * 36	 4	Total number of freelist pages.
			 * 40 	 4	The schema cookie.
			 * 44 	 4	The schema format number. Supported schema formats are 1, 2, 3, and 4.
			 * 48	 4	Default page cache size.
			 * 52	 4	The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
			 * 56	 4	The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
			 * 60	 4	The "user version" as read and set by the user_version pragma.
			 * 64	 4	True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
			 * 68	24	Reserved for expansion. Must be zero.
			 * 92	 4	The version-valid-for number.
			 * 96	 4	SQLITE_VERSION_NUMBER
			 */
			
			
			/********************************************************************/

			byte header[] = new byte[16];
			buffer.get(header);
			headerstring = Auxiliary.bytesToHex(header);
			char charArray[] = new char[16];
			
			int cn = 0;
			for (byte b: header)
			{
				charArray[cn] = (char)b;
				cn++;
			}
			String txt = new String(charArray);
			
			headerstring = txt + " (" + "0x" + headerstring + ")";
			
			if (Auxiliary.bytesToHex(header).equals(MAGIC_HEADER_STRING)) // we currently
			{
				// support
				// sqlite 3 data
				// bases
				info("header is okay. seems to be an sqlite database file.");
		    }
			else {
				info("sorry. doesn't seem to be an sqlite file. Wrong header.");
				err("Doesn't seem to be an valid sqlite file. Wrong header");
				return -1;
			}

			
			buffer.position(18);
			ffwversion = buffer.get();
			info("File format write version. 1 for legacy; 2 for WAL. " + ffwversion);

			buffer.position(19);
			ffrversion = buffer.get();
			info("File format read version. 1 for legacy; 2 for WAL. " + ffrversion);

			buffer.position(20);
		    reservedspace = buffer.get();
			info("Bytes of unused \"reserved\" space at the end of each page. Usually 0. " + reservedspace);

			maxpayloadfrac = buffer.get();
			info("Maximum embedded payload fraction. Must be 64." + maxpayloadfrac);

			minpayloadfrac = buffer.get();
			info("Minimum embedded payload fraction. Must be 32." + maxpayloadfrac);

			leafpayloadfrac = buffer.get();
			info("Leaf payload fraction. Must be 32.  " + leafpayloadfrac);

			buffer.position(16);
			inheaderdbsize = Integer.toUnsignedLong(buffer.getInt());
			if (inheaderdbsize == 1)
				inheaderdbsize = 65536;
			
			buffer.position(24);
			filechangecounter = Integer.toUnsignedLong(buffer.getInt());
			info("File change counter " + filechangecounter);

			buffer.position(28);
			sizeinpages = Integer.toUnsignedLong(buffer.getInt());
			info("Size of the database file in pages " + sizeinpages);

			buffer.position(40);
			schemacookie = Integer.toUnsignedLong(buffer.getInt());
			info("The schema cookie. (offset 40) " + schemacookie);

		    schemaformatnumber = Integer.toUnsignedLong(buffer.getInt());
			info("The schema format number. (offset 44) " + schemaformatnumber);
 
			defaultpagecachesize = Integer.toUnsignedLong(buffer.getInt());
			info("Default page cache size. (offset 48) " + defaultpagecachesize);
 
			buffer.position(60);
			
			userversion = Integer.toUnsignedLong(buffer.getInt());
			info("User version (offset 60) " + userversion);
 
			 
			vacuummode = Integer.toUnsignedLong(buffer.getInt());
			info("Incremential vacuum-mode (offset 64) " + vacuummode);
 
			buffer.position(92);

			versionvalidfornumber = Integer.toUnsignedLong(buffer.getInt());
			info("The version-valid-for number.  " + versionvalidfornumber);
 
			
					
			/********************************************************************/

			is_default = true;
			tblSig = new Hashtable<String, String>();
			tblSig.put("", "default");
			info("found unkown sqlite-database.");

			/********************************************************************/
			
			buffer.position(52);
			avacc = Integer.toUnsignedLong(buffer.getInt());
			if (avacc == 0) {
				info("Seems to be no AutoVacuum db. Nice :-).");
				autovacuum = false;
			} else
				autovacuum = true;

			/********************************************************************/
			// Determine database text encoding.
			// A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of
			// 3 means UTF-16be.
			byte[] encoding = new byte[4];
			buffer.position(56);
			buffer.get(encoding);
			int codepage = ByteBuffer.wrap(encoding).getInt();

			switch (codepage) {
			case 0:
			case 1:
				db_encoding = StandardCharsets.UTF_8;
				info("Database encoding: " + "UTF_8");
				break;
			case 3:
				db_encoding = StandardCharsets.UTF_16BE;
				info("Database encoding: " + "UTF_16BE");
				break;

			case 2:
				db_encoding = StandardCharsets.UTF_16LE;
				info("Database encoding: " + "UTF_16LE");
				break;

			}

			/*******************************************************************/

			/* 2 Byte big endian value */
			byte pagesize[] = new byte[2];
			/* at offset 16 */
			buffer.position(16);
			buffer.get(pagesize);

			ByteBuffer psize = ByteBuffer.wrap(pagesize);
			/*
			 * Must be a power of two between 512 and 32768 inclusive, or the value 1
			 * representing a page size of 65536.
			 */
			ps = Auxiliary.TwoByteBuffertoInt(psize);

			/*
			 * Beginning with SQLite version 3.7.1 (2010-08-23), a page size of 65536 bytes
			 * is supported.
			 */
			if (ps == 0 || ps == 1)
				ps = 65536;

			info("page size " + ps + " Bytes ");

			/*******************************************************************/

			/*
			 * get file size: Attention! We use the real file size information startRegion
			 * the file object not the header information!
			 */
			totalbytes = file.size();

			/*
			 * dividing the number of bytes by pagesize we can compute the number of pages.
			 */
			numberofpages = (int) (totalbytes / ps);

			info("Number of pages:" + numberofpages);

			/* extract schema startRegion binary file */

			ByteBuffer schema = ByteBuffer.allocate(ps);

			/* we use a parallel read up */
			Future<Integer> rs = file.read(schema, 0);

			while (!rs.isDone()) {

				// we can drink a cup of coffee in between or just wait ;-).
			}

			/*******************************************************************/

			/* determine the SQL-version of db on offset 96 */

			byte version[] = new byte[4];
			buffer.position(96);
			buffer.get(version);

			Integer v = ByteBuffer.wrap(version).getInt();
			sqliteversion = "" + v;

			/*******************************************************************/

			/* initialize some data structures */
			pages = new AbstractDescriptor[numberofpages + 1];
			pagetype = new int[numberofpages];
			checked = new AtomicReferenceArray<Boolean>(new Boolean[numberofpages]);

			/*******************************************************************/

			// byte[] pattern = null;
			byte[] tpattern = null;
			byte[] ipattern = null;
			int goback = 0;

			/* there are 3 possible encodings */
			if (db_encoding == StandardCharsets.UTF_8) {
				// pattern = new byte[]{7, 23};
				/* we are looking for the word 'table' */
				tpattern = new byte[] { 116, 97, 98, 108, 101 };
				/* we are looking for the word 'index' */
				ipattern = new byte[] { 105, 110, 100, 101, 120 };
				goback = 11;

			} else if (db_encoding == StandardCharsets.UTF_16LE) {
				/* we are looking for the word 'table' coding with UTF16LE */
				tpattern = new byte[] { 116, 00, 97, 00, 98, 00, 108, 00, 101 };
				/* we are looking for the word 'index' */
				ipattern = new byte[] { 105, 00, 110, 00, 100, 00, 101, 00, 120 };

				goback = 15;

			} else if (db_encoding == StandardCharsets.UTF_16BE) {
				/* we are looking for the word 'table' coding with UTF16BE */
				tpattern = new byte[] { 00, 116, 00, 97, 00, 98, 00, 108, 00, 101 };
				/* we are looking for the word 'index' */
				ipattern = new byte[] { 00, 105, 00, 110, 00, 100, 00, 101, 00, 120 };
				goback = 16;

			}

			
	
			
			/* we looking for the key word <table> of the type column */
			/* the mark the start of an entry for the sqlmaster_table */
			ByteSeqSearcher bsearch = new ByteSeqSearcher(tpattern);
			
			boolean again = false;
			int round = 0;
			ByteBuffer bb = db;
			
			/**
			 * Step into loop
			 * 
			 * 1. iteration:  Try to read table and index information from normal database
			 * 
			 * 2. iteration:  No information found but a WAL archive in place - try to get the
			 *                missing information in a second look from the WAL archive
			 * 
			 */
			do
			{
				round++;
				
				
				if (round == 2 && (!readWAL && !readRollbackJournal))
					break;
			
				if (round == 2 && readWAL) {
					emptydb = true;	
					bb = readCheckpoint();
					
				}
				
				/* Search for keyword CREATE... in hex-stream*/
				int index = bsearch.indexOf(bb, 0);
	
				/* search as long as we can find further table keywords */
				while (index != -1) {
					/* get Header */
					byte mheader[] = new byte[40];
					bb.position(index - goback);
					bb.get(mheader);
					String headerStr = Auxiliary.bytesToHex(mheader);
	
					
					/**
					 *  Attention:
					 *  
					 *  A master table record has always 5 columns: 
					 *  [type text, name text, tbl_name text, rootpage integer, sql texte]
					 * 
					 *  Accordingly the header can have a length of at least 6 Bytes (5+1 for the header length byte).
					 *  In most cases the header has a length of 7, because the length of SQL-statement is typically 
					 *  higher than 57 characters ((127-13)/2 -> 57) and lower than 8192. 
					 *  
					 */
					
					
					/**
					 * case 1: seems to be a table data set with at least 7 or possible more header length
					 *         or it was overwritten (dropped table)
					 **/
					if (headerStr.startsWith("17") || headerStr.startsWith("21")) {
	
						System.out.println(" 17 ");
						
						headerStr = "07" + headerStr; // put the header length byte in front
						// note: for a dropped component this information is lost!!!
	
						tablematch++;
						readSchema(bb,index,headerStr);
						
					} 
					/**
					 *  case 2: Normal (possible) intact table with intact but short header
					 *  header has a length of 6 bytes including header length (06)
					 **/
					else if (headerStr.startsWith("0617")) {
						
						System.out.println(" 0617 ");
						tablematch++;
						readSchema(bb,index,headerStr);
					} 
					else {
						// false-positive match for <component> since no column type 17 (UTF-8) resp. 21
						// (UTF-16) was found
					}
					/* search for more component entries */
					index = bsearch.indexOf(bb, index + 2);
				}
	
				
				/**
				 *  Last but not least: Start Index-Search.
				 */
				
				/* we looking for the key word <index> */
				/* to mark the start of an indices entry for the sqlmaster_table */
				ByteSeqSearcher bisearch = new ByteSeqSearcher(ipattern);
	
				int index2 = bisearch.indexOf(bb, 0);
	
				/* search as long as we can find further index keywords */
				while (index2 != -1) {
	
					/* get Header */
					byte mheader[] = new byte[40];
					bb.position(index2 - goback);
					bb.get(mheader);
					String headerStr = Auxiliary.bytesToHex(mheader);
	
					System.out.println("INDEX - Search " + headerStr);
	
					/**
					 * case 3: seems to be a dropped index data set
					 *         add the missing header length byte in front 
					 **/					
					 if (headerStr.startsWith("17") || headerStr.startsWith("21")) {
	
						// a typical master table record has a length of 7 bytes
					    // for smaller tables it is sometime 6 bytes
						headerStr = "07" + headerStr; // put the header length byte in front
						// note: for a dropped indices this information is lost!!!
	
						indexmatch++;
						readSchema(bb,index2,headerStr);
			
					} 
				    /**
				     * case 4: seems to be a regular index data set
				     *         with a header length of 6 bytes 
				     **/
					 
					 else if ((headerStr.startsWith("0617"))) {
						Auxiliary c = new Auxiliary(this);
						indexmatch++;						
						readSchema(bb,index2,headerStr);

						
					} else {
						// false-positive match for <component> since no column type 17 (UTF-8) resp. 21
						// (UTF-16) was found
					}
					/* search for more indices entries */
					index2 = bisearch.indexOf(bb, index2 + 2);
	
				}
			
				
				/* Is there a table schema definition inside the main database file ???? */
				System.out.println("headers:::: " + headers.size());
				
				if(headers.size()==0 && this.readWAL ==true)
				{
					// second try - maybe we could find a schema definition inside of the WAL-archive file instead?!
					
				    info("Could not find a schema definition inside the main db-file. Try to find something inside the WAL archive");
				
				    bb = readWALIntoBuffer(this.path+"-wal");
				    
				    if (null != bb)
				    	again = true;
					
				}
			
			}
			while(again && round < 2 && (readWAL));

			/*******************************************************************/

			/*
			 * Since we now have all component descriptors - let us update the 
			 * indices descriptors
			 */

			Iterator<IndexDescriptor> itidx = indices.iterator();
			while (itidx.hasNext()) {
				IndexDescriptor id = itidx.next();
				String tbn = id.tablename;

				Iterator<TableDescriptor> hdi = headers.iterator();
				
				while(hdi.hasNext()) {

					TableDescriptor desc = hdi.next();
					if (tbn != null && tbn.equals(desc.tblname)) {

						/* component for index could be found */
						TableDescriptor td = desc;

						List<String> idname = id.columnnames;
						List<String> tdnames = td.columnnames;

						for (int z = 0; z < idname.size(); z++) {

							if (tdnames.contains(idname.get(z))) {
								/* we need to find the missing column types */
								try
								{
									int match = tdnames.indexOf(idname.get(z));
									String type = td.getColumntypes().get(match);
									id.columntypes.add(type);
									System.out.println("ADDING IDX TYPE::: " + type + " FOR TABLE " + td.tblname + " INDEx "
											+ id.idxname);
								}
								catch(Exception err)
								{
									
									id.columntypes.add("");
								}
								

							}

						}
						/**
						 * The index contains data from the columns that you specify in the index and
						 * the corresponding rowid value. This helps SQLite quickly locate the row based
						 * on the values of the indexed columns.
						 */

						id.columnnames.add("rowid");
						id.columntypes.add("INT");

						/* HeadPattern !!! */
						Auxiliary.addHeadPattern2Idx(id);

						exploreBTree(id.getRootOffset(), id, bb);

						break;
					}

				}
			}
			
			

			/* prepare pre-compiled pattern objects */
			Collections.sort(headers);
			Iterator<TableDescriptor> iter = headers.iterator();

		
			/* the auto vacuum flag is > 0 -> this is a AUTOVACUUM database */
			if(autovacuum){
				
				/*  
				 * Auto-vacuum capable SQLite databases make use of Pointer Map pages. 
				 * In this case, there is no freepage list. Pointer Maps only exist if 
				 * the according flag is set.
				 * They only exist within auto-vacuum capable databases, which require 
				 * the 32 bit integer value, read big endian, stored at byte offset 52 
				 * of the database header to be non-zero.
				 */
				
				/* reserve space for the first/next page of the free list */
				ByteBuffer pointermap = ByteBuffer.allocate(ps);

				/* read next db page of the list - sometimes there is only one */
				Future<Integer> operation = file.read(pointermap, ps);
				while (!operation.isDone()) {
					try {
						TimeUnit.MILLISECONDS.sleep(1);
					} catch (InterruptedException err) {
						err.printStackTrace();
					}
				}
				pointermap.flip();

				
				// now read the first byte (page type)
				byte pagetype[] = new byte[1];

				try {
					pointermap.get(pagetype);
					System.out.println("Pagetype of Pointer Map (should be 1): " + pagetype);
				    pointermap.position(5);	
					
				}
				catch(Exception err)
				{
					System.out.println("Warning" + " Error parsing pointer map");
				}

				
				//for (AbstractDescriptor td: pages) {
					
					//System.out.println("td:" + td);
				//}
				
				//do {
					
					byte type = pointermap.get();

					switch(type) {
					
					case 1:    /* 
								0x01 0x00 0x00 0x00 0x00
								This record relates to a B-tree root page,
								hence the page number being indicated as zero. 
								*/
					
					case 2:	   /*
					 			0x02 0x00 0x00 0x00 0x00
								This record relates to a free page, which also does not have a parent page. 
								*/
						      
						 
							 break;
					
					case 3:  	/*
					 			0x03 0xVV 0xVV 0xVV 0xVV (where VV indicates a variable)
								This record relates to the first page in an overflow chain. 
								The parent page number is the number of the B-Tree page containing the B-Tree 
								cell to which the overflow chain belongs.
								*/
						
							 
							 break;
					
					case 4:  break;
					
					case 5:  break;
					
					
					}
					
					
				//}while(true);
				
			}
			
			
			
			HashSet<String> doubles = new HashSet<String>();
 			
			/*
			 * Now, since we have all component definitions, we can start
			 * exploring the b-tree for each component
			 */
			while (iter.hasNext()) {

				TableDescriptor td = iter.next();
				/* remove enclosing double quotes */
                if (td.tblname.length()>2 && td.tblname.startsWith("\""))
                		td.tblname = td.tblname.substring(1,td.tblname.length()-1);
				
				if (!doubles.add(td.tblname))
				  continue;
				
				td.printTableDefinition();

				int r = td.getRootOffset();
				info(" root offset for component " + r);

				String signature = td.getSignature();
				info(" signature " + signature);

				/* save component fingerprint for compare */
				if (signature != null && signature.length() > 0)
					tblSig.put(td.getSignature(), td.tblname);

							
				/* update treeview - skip this step in console modus */
				if (null != gui) {
                     
					Platform.runLater(() -> {
							
						
						/* since table nodes will also be needed in journals and wal-files we have to add those nodes too */
						
						String path = gui.add_table(this, td.tblname, td.columnnames, td.getColumntypes(), td.primarykeycolumns, td.boolcolumns, false, false,0);
						guitab.put(td.tblname, path);
							//lastpath = path;
							
						if (readWAL) {
							
							List<String> cnames = td.columnnames;
							cnames.add(0,"commit");
							cnames.add(1,"dbpage");
							cnames.add(2,"walframe");
							cnames.add(3,"salt1");
							cnames.add(4,"salt2");
							
							List<String> ctypes = td.serialtypes;
							ctypes.add(0,"INT");
							ctypes.add(1,"INT");
							ctypes.add(2,"INT");
							ctypes.add(3,"INT");
							ctypes.add(4,"INT");
							
							
							String walpath = gui.add_table(this, td.tblname, cnames, ctypes, td.primarykeycolumns, td.boolcolumns, true, false,0);
							guiwaltab.put(td.tblname, walpath);
							setWALPath(walpath.toString());
	
						}
	
						else if (readRollbackJournal) {
							String rjpath = gui.add_table(this, td.tblname, td.columnnames, td.getColumntypes(),td.primarykeycolumns, td.boolcolumns, false, true,0);
							guiroltab.put(td.tblname, rjpath);
							setRollbackJournalPath(rjpath.toString());
						}
					});
				}

				if (td.isVirtual())
					continue;

				/* transfer component information for later recovery */
				RecoveryTask.tables.add(td);

				/* explore a component trees and build up page info */
				exploreBTree(r, td, bb);

			}

			/*******************************************************************/

			
            Collections.sort(indices);
			Iterator<IndexDescriptor> it = indices.iterator();
            
			while (it.hasNext()) {

				IndexDescriptor id = it.next();
				int r = id.getRootOffset();
				info(" root offset for index " + r);

				/* update treeview in HexViewFactory - skip this step in console modus */
				if (null != gui) {
					String path = gui.add_table(this, id.idxname, id.columnnames, id.columntypes, id.boolcolumns, null, false, false,1);
					System.out.println("id.idxname " + id.idxname);
					guitab.put(id.idxname, path);
					//lastpath = path;

					if (readWAL) {
						
						
						List<String> cnames = id.columnnames;
						cnames.add(0,"commit");
						cnames.add(1,"dbpage");
						cnames.add(2,"walframe");
						cnames.add(3,"salt1");
						cnames.add(4,"salt2");
						
						List<String> ctypes = id.columntypes;
						ctypes.add(0,"INT");
						ctypes.add(1,"INT");
						ctypes.add(2,"INT");
						ctypes.add(3,"INT");
						ctypes.add(4,"INT");
						
						
						
						String walpath = gui.add_table(this, id.idxname, cnames, ctypes, id.boolcolumns, null, true, false,1);
						guiwaltab.put(id.idxname, walpath);
						setWALPath(walpath.toString());

					}
					else if (readRollbackJournal) {
						String rjpath = gui.add_table(this, id.idxname, id.columnnames, id.columntypes, id.boolcolumns, null, false, true,1);
						guiroltab.put(id.idxname, rjpath);
						setRollbackJournalPath(rjpath.toString());
					}

					
				}

			}
		
			

			/*******************************************************************/

			/**
			 * Sometimes, a record cannot assigned to a component or index -> these records
			 * are assigned to the __UNASSIGNED component.
			 */

			if (null != gui) {
				List<String> col = new ArrayList<String>();
				List<String> names = new ArrayList<String>();

				/* create dummy component for unassigned records */
				for (int i = 0; i < 20; i++) {
					col.add("TEXT");
					names.add("col" + (i + 1));
				}
				TableDescriptor tdefault = new TableDescriptor("__UNASSIGNED", "",col, col, names, null, null, null, false);
				headers.add(0,tdefault);

				/* update treeview - skip this step in console modus */
				if (null != gui) {
					
					String path = gui.add_table(this, tdefault.tblname, tdefault.columnnames,
							tdefault.getColumntypes(), tdefault.boolcolumns, null, false, false,0);
					guitab.put(tdefault.tblname, path);
					//lastpath = path;

					if (readWAL) {
						String walpath = gui.add_table(this, tdefault.tblname, tdefault.columnnames,
								tdefault.getColumntypes(), tdefault.boolcolumns ,null, true, false,0);
						guiwaltab.put(tdefault.tblname, walpath);
						setWALPath(walpath.toString());
					}
					else if (readRollbackJournal) {
						String rjpath = gui.add_table(this, tdefault.tblname, tdefault.columnnames,
								tdefault.getColumntypes(),tdefault.boolcolumns ,null, false, true,0);
						guiroltab.put(tdefault.tblname, rjpath);
						setRollbackJournalPath(rjpath.toString());
					}

					
				}
			}

			
			if(emptydb){
				
				System.out.println("Omit analysis of the database since database file is empty ");
				return 0;
			}
			
			
			
			/*******************************************************************/

			byte freepageno[] = new byte[4];
			buffer.position(36);
			buffer.get(freepageno);
			info("Total number of free list (trunk) pages " + Auxiliary.bytesToHex(freepageno));
			ByteBuffer no = ByteBuffer.wrap(freepageno);
			fpnumber = no.getInt();
			System.out.println(" no " + fpnumber);

			/*******************************************************************/

			byte freelistpage[] = new byte[4];
			buffer.position(32);
			buffer.get(freelistpage);
			info("FreeListPage starts at offset " + Auxiliary.bytesToHex(freelistpage));
			ByteBuffer freelistoffset = ByteBuffer.wrap(freelistpage);
			int head = freelistoffset.getInt();
			info("head:: " + head);
			fphead = head;
			int start = (head - 1) * ps;

			/*******************************************************************/

			if (head == 0) {
				info("INFO: Couldn't locate any free pages to recover. ");
			}

			
			/*******************************************************************
			 *
			 * STEP 1: we start recovery process with scanning the free list first
			 **/

			if (head > 0) {
				info("first:: " + start + " 0hx " + Integer.toHexString(start));

				long startfp = System.currentTimeMillis();
				System.out.println("Start free page recovery .....");

				// seeking file pointer to the first free page entry

				/* create a new threadpool to analyze the freepages */
				ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Global.numberofThreads);

				/* a list can extend over several memory pages. */
				boolean morelistpages = false;

				int freepagesum = 0;

				do {
					/* reserve space for the first/next page of the free list */
					ByteBuffer fplist = ByteBuffer.allocate(ps);

					/* read next db page of the list - sometimes there is only one */
					Future<Integer> operation = file.read(fplist, start);
					while (!operation.isDone()) {
						try {
							TimeUnit.MILLISECONDS.sleep(1);
						} catch (InterruptedException err) {
							err.printStackTrace();
						}
					}
					fplist.flip();

					// next (possible) freepage list offset or 0xh00000000 + number of entries
					// example : 00 00 15 3C | 00 00 02 2B

					// now read the first 4 Byte to get the offset for the next free page list
					byte nextlistoffset[] = new byte[4];

					try {
						fplist.get(nextlistoffset);
					}
					catch(Exception err)
					{
						System.out.println("Warning" + " Error while parsing free list.");
					}
						
					/*
					 * is there a further page - <code>nextlistoffset</code> has a value > 0 in this
					 * case
					 */
					if (!Auxiliary.bytesToHex(nextlistoffset).equals(NO_MORE_ENTRIES)) {
						
						ByteBuffer of = ByteBuffer.wrap(nextlistoffset);
						int nfp = of.getInt();
						start = (nfp - 1) * ps;
						if (!allreadyvisit.contains(nfp))
							allreadyvisit.add(nfp);
						else {
							info("Antiforensiscs found: cyclic freepage list entry");
							morelistpages = true;
						}
					} else
						morelistpages = false;

					// now read the number of entries for this particular page
					byte numberOfEntries[] = new byte[4];
					try
					{
						fplist.get(numberOfEntries);
					}
					catch(Exception err)
					{
						
					}
					
					int entries = 0;
					
				    ByteBuffer e = ByteBuffer.wrap(numberOfEntries);
					entries = e.getInt();
					
					info(" Number of Entries in freepage list " + fpnumber);

					runningTasks.set(0);
					
					/* iterate through free page list and read free page offsets */
					for (int zz = 1; zz <= entries; zz++) {
						byte next[] = new byte[4];
						fplist.position(4 + 4 * zz);
						fplist.get(next); 

						ByteBuffer bf = ByteBuffer.wrap(next);
						int n = bf.getInt();

						if (n == 0) {
							continue;
						}
						// determine offset for free page
						int offset = (n - 1) * ps;

						System.out.println("free page " + n + " at offset " + offset);

						// if (offset Job.size)
						// continue;

						RecoveryTask task1 = new RecoveryTask(new Auxiliary(this), this, offset, n, ps, true);
						/* add new task to executor queue */
						runningTasks.incrementAndGet();
						tasklist.add(task1);
						executor.execute(task1);
						// task.run();
					}
					freepagesum += entries;

				} while (morelistpages); // while

				executor.shutdown();

				info("ImportDBTask total: " + runningTasks.intValue());

				// wait for Threads to finish the tasks
				while (runningTasks.intValue() != 0) {
					try {
						TimeUnit.MILLISECONDS.sleep(10);
						// System.out.println("wait...");
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				info("Number of cells " + numberofcells.intValue());

				info(" Finished. No further free pages. Scanned " + freepagesum);

				long endfp = System.currentTimeMillis();
				info("Duration of free page recovery in ms: " + (endfp - startfp));

			} // end of free page list recovery

			info("Lines after free page recovery: " + resultlist.size());

			/*******************************************************************/
			// start carving

			
			// full db-scan (including all database pages)
			scan(numberofpages, ps);

			/*******************************************************************/

			if (gui != null) {
				
				info("Number of tables recovered: " + resultlist.size());
				System.out.println("Number of tables recovered: " + resultlist.size());
				
				Enumeration<String> tables = resultlist.keys();
				
				/* finally we can update the TableView for each table */
				while(tables.hasMoreElements())
				{	
					String tablename = tables.nextElement();
			        /* get tree path i.e. /data bases/02-05.db/users */
					String path = guitab.get(tablename);
					System.out.println("update_table() in Job.java : " + path);
					gui.update_table(path,resultlist.get(tablename),false);
				}
					
				//System.out.println(" Hashes \n" + LineHashes);

				
			} 
			else {
				String[] lines = null; //resultlist.toArray(new String[0]);
				writeResultsToFile(null, lines);

				if (readRollbackJournal) {
					/* the readWAL option is enabled -> check the WAL-file too */
					System.out.println(" RollbackJournal-File " + this.rollbackjournalpath);
					rol = new RollbackJournalReader(rollbackjournalpath, this);
					rol.ps = this.ps;
					/* start parsing Rollbackjournal-file */
					rol.parse();
					rol.output();
				}
				else if (readWAL) {
					/* the readWAL option is enabled -> check the WAL-file too */
					System.out.println(" WAL-File " + walpath);
					WALReader wal = new WALReader(walpath, this);
					/* start parsing WAL-file */
					wal.parse();
					wal.output();
				}

				if (null != lines)
					return lines.toString().hashCode();
				
			}
			
			// start with import
			Platform.runLater(() -> {
				//fqlite.ui.HexViewFX hex = new fqlite.ui.HexViewFX(GUI.topContainer,path,db);
 			   
				//hexview = hex; 
 				/* prepare garbage collection - */
 				db.clear(); //db = null;
 				//resultlist.clear(); resultlist = null;
 				lines.clear(); lines = null;
 				try {
					file.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
 				tasklist.clear(); 
 				tasklist = null;
 				overflowpages.clear(); //overflowpages = null;
 				System.gc();
 				System.out.println("Garbage Collector started...");
			    
 				Platform.runLater(new Runnable() {
				       public void run() {             
				           
						  if(BLOBs.size() > 0){
							  
							  BLOB2Disk out = new BLOB2Disk(Job.this);
							  out.export();
						  }
							  
						
				       }
 				});
			
			});

		} catch (IOException e) {

			info("Error: Could not open file.");
			System.exit(-1);
		}
		
	
		return 0;
	}
	
	/**
	 * If the database is still empty, all changes are still in the WAL file. 
	 * In this case, a snapshot with all changed database pages is stored there.
	 * This happens quite often, because by default a commit to the actual database 
	 * only takes place after the WAL archive has reached 1000 pages. 
	 */
	private ByteBuffer readCheckpoint(){

		TreeMap<Integer,ByteBuffer> cptable = new TreeMap<Integer,ByteBuffer>();
		
		ByteBuffer wal = null;
        ByteBuffer checkpoint = ByteBuffer.allocate(0);
			
		try {
			wal = readWALIntoBuffer(this.path+"-wal");
		} catch (IOException e) {
			System.err.println("Could not access wal-archive");
			return null;
		}
		
		
		
		boolean next = false;
		int numberofpages = 0;
		int framestart = 32; // this is the position, where the first frame should be
	    
	    do {

	    	/* 24 Byte - with six 4-Byte big endian values */
			byte frameheader[] = new byte[24];
			wal.position(framestart);
			wal.get(frameheader);
			ByteBuffer fheader = ByteBuffer.wrap(frameheader);
	
			/* get the page number of this frame */
			int pagenumber_maindb = fheader.getInt();
			
			/* number or size of pages for a commit header, otherwise zero. */
			int commit = fheader.getInt();
			if (commit > 0)
				info(" Information of the WAL-archive has been commited successful. ");
			else
				info(" No commit so far. this frame holds the latest! version of the page ");
			
			/* read the db page into buffer */
			ByteBuffer page = wal.slice();
			page.limit(ps);
			
			framestart += ps +  24;
		
			System.out.println("create Checkpoint() for page::" + pagenumber_maindb);
			cptable.put(pagenumber_maindb,page);
		
			
			/*  More pages to analyze ? */
			if(framestart+24+ps < size)
			{
				
				next = true;
			}
			else
				next = false;
			
			
	    	
	    }
	    while(next);
	    
	    Set<Integer> cppages = cptable.keySet();
	    Iterator<Integer> it = cppages.iterator();
	    
	    while(it.hasNext())
	    {
	    	ByteBuffer pp = cptable.get(it.next());
	    	
	    	checkpoint = ByteBuffer.allocate(checkpoint.limit() + pp.limit())
	        .put(checkpoint)
	        .put(pp)
	        .rewind();
	    }
	    
	    /**
	     * important:: Whenever the database file is empty and the WAL archive 
	     * has not been committed yet, all information is in the WAL file. This 
	     * also applies to the number of pages in the database. 
	     */
		pages = new AbstractDescriptor[cptable.size() + 1];

		
		
	    
	    return checkpoint;
	}
	
	

	
	/**
	 * Write all data records - all tables to csv-file.
	 * @param f
	 */
	public boolean exportResults2File(File f,String tablename){

		
		try (final BufferedWriter writer = Files.newBufferedWriter(f.toPath(),Charset.forName("UTF-8"), StandardOpenOption.CREATE)) 
		{
			System.out.println("tablename " + tablename);
			// no tablename? -> export complete database
			if(this.resultlist.get(tablename)== null)
			{	  	
				Enumeration<String> keyset = this.resultlist.keys();
				while (keyset.hasMoreElements()) {
					
					ObservableList<LinkedList<String>> table = resultlist.get(keyset.nextElement());
					Iterator<LinkedList<String>> iter = table.iterator();
					
					while(iter.hasNext()){
						LinkedList<String> list = iter.next();
						String[] line = list.toArray(new String[0]);
						String output = "";
						int counter = 0;
						for (String c: line){
							
							/* we need to replace the cell value with the BLOB filename */
							if(c.startsWith("[BLOB"))
							{
								//System.out.println(" 2 " + line[3]);  // offset
								
								Iterator<TableDescriptor> td = headers.iterator();
								/* iterate over all table descriptor objects */
								while(td.hasNext()) {
								
									TableDescriptor tbl = td.next();
									
									/* find the matching table - line[0] holds the actual tablename */
									if(tbl.tblname.equals(line[0])) {
									    // skip first 5 columns [0..4] tblname,offset,status,pll,hl
										if (counter<5)
									    	continue;
										// the first data column starts with index 5 - get column name
										//String column = tbl.columnnames.get(counter-5);
										//System.out.println("&&& col " + column);
										// hash = offset value + hashcode("columnname")
										//Long hash = Long.parseLong(line[counter-3])+ column.hashCode();
										int from = c.indexOf("BLOB-");
							        	int to = c.indexOf("] ");
							        	String number = c.substring(from+5, to);
							        	System.out.println("BLOB >>>>" + c);
							        	System.out.println(Arrays.toString(line));    
    
							        	int id = Integer.parseInt(number);
							        	Long hash = Long.parseLong(line[4]) + id;
										System.out.println("filecache hash"  + hash);
										
										// getPath
										String path = FileCache.get(hash);
										System.out.println("pfad : " + path);
										Path pp = Paths.get(path);
									    c = pp.getFileName().toString();
									    
									    break;
									}
								}
							}
                            counter++;
							
							output += c + ",";
						}
						writer.write(output + "\n");
						counter = 0;

					}
					
				}
			}
			// only one particular table to export
			else{
				 //this.resultlist.get(tablename);
  				 ObservableList<LinkedList<String>> table = resultlist.get(tablename);
  				 if (null == table){
  					 return false;
  				 }
				 Iterator<LinkedList<String>> iter = table.iterator();
					
					while(iter.hasNext()){
						LinkedList<String> list = iter.next();
						String[] line = list.toArray(new String[0]);
						String output ="";
						int counter = 0;
						for (String c: line){
							
							/* we need to replace the cell value with the BLOB filename */
							if(c.startsWith("[BLOB-"))
							{
								Iterator<TableDescriptor> td = headers.iterator();
								/* iterate over all table descriptor objects */
								while(td.hasNext()) {
								
									TableDescriptor tbl = td.next();
									
									/* find the matching table - line[0] holds the actual tablename */
									if(tbl.tblname.equals(line[0])) {
									    // skip first 5 columns [0..4] tblname,offset,status,pll,hl
										if (counter<5)
									    	continue;
										// the first data column starts with index 5 - get column name
										//String column = tbl.columnnames.get(counter-5);
										//System.out.println("&&& col " + column);
										// hash = offset value + hashcode("columnname")
										//Long hash = Long.parseLong(line[counter-3])+ column.hashCode();
								
										int from = c.indexOf("BLOB-");
							        	int to = c.indexOf("]");
							        	String number = c.substring(from+5, to);
							        	System.out.println("BLOB >>>>" + c);
							        	System.out.println(Arrays.toString(line));    
							        	
							        	int id = Integer.parseInt(number);
							        	Long hash = Long.parseLong(line[4]) + id;
										System.out.println("HASH :: " + hash);
										// getPath
										String path = FileCache.get(hash);
										Path pp = Paths.get(path);
									    c = pp.getFileName().toString();
									    break;
									}
								}
								
								
							
							
							}
                            counter++;
							output += c + ",";
						
						
						
						}
						
						counter = 0;
						writer.write(output + "\n");
				 }
				 
			}
				
			
				
		}
	    catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		
		/* look out for BLOB files in file cache and export them too */
		if(FileCache != null && FileCache.size()>0) {
			System.out.println("§§§§ " + f.getParentFile().getAbsolutePath());
			exportBLOB2Disk(f.getParentFile().getAbsolutePath());
		}
		
		
		return true;

	}
	
	
	private void exportBLOB2Disk(String target){
		Set<Long> files = FileCache.keySet();
		files.forEach(
				
				f -> {
		            try {
		                //get next element from set 
		            	String path = FileCache.get(f);
		           
		            	//create a PATH object since copy method needs such an object
		            	Path source = Paths.get(path);
		            	
		            	//we need the filename only
		            	String fname = source.getFileName().toString();
		            	
		            	//create target path -> all BLOBs get copied to this
		            	Path ftarget = Paths.get(target + File.separator + fname);
		            	
		            	//let's do it!
		            	Files.copy(source, ftarget, StandardCopyOption.REPLACE_EXISTING);
		            	
		            } catch (Exception e) {
		                e.printStackTrace();
		            }
				
				}
		);
	}
	
	/**
	 * Save findings into a comma separated file.
	 * @param filename
	 * @param lines
	 */
	public void writeResultsToFile(String filename, String [] lines) {
		System.out.println("Write results to file...");
		System.out.println("Number of records recovered: " + resultlist.size());

		if (null == filename) {
			Path dbfilename = Paths.get(path);
			String name = dbfilename.getFileName().toString();

			LocalDateTime now = LocalDateTime.now();
			DateTimeFormatter df;
			df = DateTimeFormatter.ISO_DATE_TIME; // 2020-01-31T20:07:07.095
			String date = df.format(now);
			date = date.replace(":","_");

			filename = "results" + name + date + ".csv";
		}
		

		/** convert line to UTF-8 **/
		try {
			
			final File file = new File(filename);
            
			
		    try (final BufferedWriter writer = Files.newBufferedWriter(file.toPath(),Charset.forName("UTF-8"), StandardOpenOption.CREATE)) 
		    {
		      for (String line: lines)
		      {	
		    	  writer.write(line + "\n");
		      }
		    }
			
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	
	private void readSchema(ByteBuffer bb, int index, String headerStr) throws IOException
	{
		
		System.out.println("Entering readSchema()");
		
		/* we need a utility method for parsing the Mastertable */
		Auxiliary c = new Auxiliary(this);
		
		// compute offset
		int starthere = index;
		//int starthere = index % ps;
		//int pagenumber = index / ps;
				
		/* start reading the schema string from the correct position */
		if (db_encoding == StandardCharsets.UTF_8) 
		{
			c.readMasterTableRecord(this, starthere - 13, bb, headerStr);
		}
		else if (db_encoding == StandardCharsets.UTF_16LE)
		{
			c.readMasterTableRecord(this, starthere - 17, bb, headerStr);
		}
		else if (db_encoding == StandardCharsets.UTF_16BE)
		{
			c.readMasterTableRecord(this, starthere - 18, bb, headerStr);
		}
		System.out.println("Leave readSchema()");

	}

	/**
	 *  Translate a given ByteBuffer to a String. 
	 *  
	 */
	public static String decode(ByteBuffer tblb) {

		/* Attention: we need to use the correct encoding!!! */
		try {
			byte[] m = tblb.array();

			/* try to read bytes an create a UTF-8 string first */
			String val = new String(m, StandardCharsets.UTF_8);

			/* get byte array with UTF8 representation */
			byte[] n = val.getBytes(StandardCharsets.UTF_8); //org.apache.commons.codec.binary.StringUtils.getBytesUtf8(val);

			/* there are Null-Byte values left after conversion */
			/* I don't know why ? */
		
			byte[] cs = new byte[n.length];

			int xx = 0;
			for (byte e : n) {
				//String ee = String.format("%02X", e);
				//System.out.print(ee + " ");

				if (e != 0) {
					cs[xx] = e;
					xx++;

				}

			}

			String schemastr = new String(cs);

			return schemastr;

		} catch (Exception err) {
			System.out.println(err);
		}

		return "";
	}

	/**
	 * The method creates a ImportDBTask object for each page to be scanned.
	 * Each task is then scheduled into a worker thread's ToDo list. 
	 * 
	 * After all tasks are assigned, the worker threads are started 
	 * and begin processing.  
	 * 
	 * @param number
	 * @param ps
	 */
	public void scan(int number, int ps) {
		info("Start with scan...");
		

		/* create a new threadpool to analyze the freepages */
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Global.numberofThreads);

		long begin = System.currentTimeMillis();
		
		/* first start scan of regular pages */
	
		Worker[] worker = new Worker[Global.numberofThreads]; 
        for (int i = 0; i < Global.numberofThreads; i++)
        {
        	worker[i] = new Worker(this);
        }
		
		for (int cc = 1; cc < pages.length; cc++) {
			
			if (null == pages[cc]) 
			{
				debug("page " + cc + " is no regular leaf page component. Maybe a indices or overflow or dropped component page.");
				//System.out.println("page " + cc + " offset " + ((cc-1)*ps) + " is no regular leaf page component. Maybe a indices or overflow or dropped component page.");
			} 
			else 
			{
				debug("page" + cc + " is a regular leaf page. ");

				// determine offset for free page
				long offset = (cc - 1) * ps;

				//System.out.println("************************ pagenumber " + cc + " " + offset);

				RecoveryTask task = new RecoveryTask(worker[cc % Global.numberofThreads].util,this, offset, cc, ps, false);
				worker[cc % Global.numberofThreads].addTask(task);
				runningTasks.incrementAndGet();
			}
		}
		debug("ImportDBTask total: " + runningTasks.intValue() + " worker threads " + Global.numberofThreads);
		
		int c = 1;
		/* start executing the work threads */
		for (Worker w : worker)
		{	
			System.out.println(" Start worker thread" + c++);
			/* add new task to executor queue */
			//executor.execute(w);			
            w.run();
		}

		try {
		    // System.out.println("attempt to shutdown executor");
		    executor.shutdown();
		
		    int remaining = 0;
		    int i = 0;
		    do
		    {
		    	remaining = runningTasks.intValue();
		    	i++;
		        //System.out.println(" Still running tasks " + remaining);
		        Thread.currentThread();
				Thread.sleep(100);
		    }while(i < 50 && remaining > 0);
		   
		    executor.awaitTermination(2, TimeUnit.SECONDS);
		
		
		}
		catch (InterruptedException e) {
		    System.err.println("tasks interrupted");
		}
		finally {
		    if (!executor.isTerminated()) {
		        System.err.println("cancel non-finished tasks");
		    }
		    executor.shutdownNow();
		    System.out.println("shutdown finished");
		}
		
		// wait for Threads to finish the tasks
		while (runningTasks.intValue() != 0) {
			try {
				TimeUnit.MILLISECONDS.sleep(2000);
				System.out.println("warte....");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		long ende = System.currentTimeMillis();
		info("Duration of scanning all pages in ms : " + (ende-begin));
		info("End of Scan...");
		
		executor = null;
		
	}


	protected void setGUI(GUI gui) {
		this.gui = gui;
	}

	public void setPath(String path) {
		this.path = path;
	}
	
	public void setWALPath(String path) {
		walpath = path;
	}
	
	public void setRollbackJournalPath(String path)
	{
		rollbackjournalpath = path;
	}
	

	/**
	 *  This method is used to start the job. 
	 */
	public void start() {
		if (path != null)
			try {
				run(path);
			} catch (Exception e) {
				e.printStackTrace();
			}
		else
			return;
	}

	public void info(String message) {
		if (gui != null)
			gui.doLog(message);
		else
			System.out.println(message);
	}

	public void err(String message) {
		if (gui != null)
		{	//JOptionPane.showMessageDialog(gui, message);
			Alert alert = new Alert(AlertType.ERROR);
			alert.setTitle("ERROR");
			alert.setContentText(message);
			alert.showAndWait();
		}
		else
			System.err.println("ERROR: " + message);
	}


	/**
	 *	 
	 */
	public Job() {
		Base.LOGLEVEL = Global.LOGLEVEL;
	}


	/**
	 * Start processing a new Sqlite file.
	 * 
	 * @param p
	 * @return
	 */
	public int run(String p) {
		
		int hashcode = -1;
		path = p;
		long start = System.currentTimeMillis();
		try {
			hashcode = processDB();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		System.out.println("Duration in ms: " + (end - start));

		return hashcode;
	}

	

	/**
	 * Read a database page from a given offset with a fixed pagesize.
	 * 
	 * @param offset
	 * @param pagesize
	 * @return  A <code>ByteBuffer</code> object containing the page content. 
	 */
	public ByteBuffer readPageWithOffset(long offset, int pagesize) {

		if ((offset > db.limit()) || (offset < 0))
		{
			
			System.out.println(" offset greater than file size ?!" + offset + " > " + db.limit());
						
			return null;
		}
		db.position((int)offset);
		ByteBuffer page = db.slice();
		if (page.capacity() == 0) {
			System.out.println(" Achtung!!!! leere Seite");
			return null;
		}
		else
		{		
			page.limit(pagesize);
		}
		return page;
	}

	/**
	 *  Since all pages are assigned to a unique number in SQLite, we can read a 
	 *  page by using this value together with the pagesize. 
	 *  
	 * @param pagenumber (>=1)
	 * @param pagesize
	 * @return  A <code>ByteBuffer</code> object containing the page content. 
	 */
	public ByteBuffer readPageWithNumber(int pagenumber, int pagesize) {
		
		if (pagenumber < 0)
		{
		   return null;
		}
		return readPageWithOffset(pagenumber*pagesize,pagesize);
	}
	
	
	/**
	 *  Since all pages are assigned to a unique number in SQLite, we can read a 
	 *  page by using this value together with the pagesize. 
	 *  
	 * @param pagenumber (>=1)
	 * @param pagesize
	 * @return  A <code>ByteBuffer</code> object containing the page content. 
	 */
	public ByteBuffer readPageWithNumberFromBuffer(int pagenumber, int pagesize, ByteBuffer bb) {
		
		if (pagenumber < 0)
		{
		   return null;
		}
		
		int offset = pagenumber*pagesize;
		
		if ((offset > db.limit()) || (offset < 0))
		{
			
			System.out.println(" offset greater than file size ?!" + offset + " > " + db.limit());
			//Auxiliary.printStackTrace();
			
			
			return null;
		}
		db.position((int)offset);
		ByteBuffer page = db.slice();
		page.limit(pagesize);
		return page;
	}


	/**
	 * The B-tree , or, more specifically, the B+-tree, 
	 * is the most widely used physical database structure 
	 * for primary and secondary indexes on database relations. 
	 * 
	 * This method can be called to traverse all nodes of 
	 * a table-tree. 
	 * 
	 * Attention! This is a recursive function. 
	 * 
	 * 
	 * @param root  the root node.
	 * @param td   this reference holds a Descriptor for the page type. 
	 * @throws IOException
	 */
	private void exploreBTree(int root, AbstractDescriptor td, ByteBuffer filebuffer) throws IOException {
		
		/* add page to list */
		//System.out.println("Adding page " + root + " to page list.");
		
		
		/* none synchronized WAL <> DB-File ? */
		//if (pages.length-1 < root)
		//{
		//	Logger.out.warning(" WAL file is not synchronized ");
		//	return;
		//} 
			
		if (root < pages.length)
			pages[root] = td;
		else
			return;
		
		// pagesize * (rootindex - 1) -> go to the start of this page
		int offset = ps * (root - 1);

		if(offset <= 0)
			return;
		
		if (root <  0)
		{
			return;
		}
		
		
		//ByteBuffer nextpage = ByteBuffer.allocate(1);
		// first two bytes of page
		//Future<Integer> type = file.read(nextpage, offset); 		
		//while (!type.isDone()) {
		//}
		
		// read type byte
		filebuffer.position(offset);	
        byte typ = filebuffer.get();
		
		/* check type of the page by reading the first two bytes */
		//int typ = Auxiliary.getPageType(Auxiliary.bytesToHex(nextpage));

		/* not supported yet */
		if (typ == 2) 
		{
			debug(" page number" + root + " is a  INDEXINTERIORPAGE.");
			
		}
		/* type is either a data interior page (12) */
		else if (typ == 5) {
			debug("page number " + root + " is a interior data page ");

			
			//ByteBuffer rightChildptr = ByteBuffer.allocate(4);
			
			//Future<Integer> child = file.read(rightChildptr, offset + 8); // offset 8-11 of an internal page are used
																		  // for rightmost child page number
			//while (!child.isDone()) {
			//}
			
			//filebuffer.position(offset + 8);
			int rightChildptr = filebuffer.getInt(offset + 8);
			
			/* recursive */
			//rightChildptr.position(0);
			//exploreBTree(rightChildptr.getInt(), td, filebuffer);
			exploreBTree(rightChildptr, td, filebuffer);

			
			/* now we have to read the cell pointer list with offsets for the other pages */

			/* read the complete internal page into buffer */
			//System.out.println(" read internal page at offset " + ((root-1)*ps));
			ByteBuffer buffer = readPageWithNumber(root - 1, ps);

			byte[] numberofcells = new byte[2];
			buffer.position(3);

			buffer.get(numberofcells);
			ByteBuffer noc = ByteBuffer.wrap(numberofcells);
			int e = Auxiliary.TwoByteBuffertoInt(noc);	
			
			
			byte cpn[] = new byte[2];
			buffer.position(5);

			buffer.get(cpn);

			ByteBuffer size = ByteBuffer.wrap(cpn);
			int cp = Auxiliary.TwoByteBuffertoInt(size);

			//System.out.println(" cell offset start: " + cp);
			//System.out.println(" root is " + root + " number of elements: " + e);
			

			/* go on with the cell pointer array */
			for (int i = 0; i < e; i++) {

				// address of the next cell pointer
				byte pointer[] = new byte[2];
				buffer.position(12 + 2 * i);
				if (buffer.capacity() <= buffer.position()+2)
					continue;
				buffer.get(pointer);
				ByteBuffer celladdr = ByteBuffer.wrap(pointer);
				int celloff = Auxiliary.TwoByteBuffertoInt(celladdr);

				debug(" celloff " + celloff);
				// read page number of next node in the btree
				byte pnext[] = new byte[4];
				if (celloff >= buffer.capacity() || celloff < 0)
					continue;
				if (celloff > ps)
					continue;
				buffer.position(celloff);
				buffer.get(pnext);
				int p = ByteBuffer.wrap(pnext).getInt();
				// unfolding the next level of the tree
				debug(" child page " + p);
				exploreBTree(p, td,filebuffer);
			}

		} 
		else if (typ == 8 || typ == 10) {
			debug("page number " + root + " is a leaf page " + " set component/index to " + td.getName());
			if (root >  numberofpages)
				return;
			if (null == pages[root])
				pages[root] = td;
			else
				debug("WARNING page is member in two B+Trees! Possible Antiforensics.");
		} 
		else {
			debug("Page" + root + " is neither a leaf page nor a internal page. Try to set component to " + td.getName());
			
			if (root >  numberofpages)
				return;
			if (null == pages[root])
				pages[root] = td;

		}

	}



	/**
	 * Return the columnnames as a String array for a given table or indextable name.
	 * @param tablename
	 * @return
	 */
	public String[] getHeaderString(String tablename)
	{
		/* check tables first */
		Iterator<TableDescriptor> iter = this.headers.iterator();
		while (iter.hasNext())
		{
			TableDescriptor td = iter.next();
			if (td.tblname.equals(tablename))
			{
				return td.columnnames.toArray(new String[0]);
			}	
			
		}
		
		/* check indicies next */
		Iterator<IndexDescriptor> itI = this.indices.iterator();
		while (itI.hasNext())
		{
			IndexDescriptor id = itI.next();
			if (id.idxname.equals(tablename))
			{
				return id.columnnames.toArray(new String[0]);
			}	
			
		}

		/* no luck */
		return null;
	}

}




class Signatures {

	static String getTable(String signature) {

		signature = signature.trim();

		signature = signature.replaceAll("[0-9]", "");

		signature = signature.replaceAll("PRIMARY_KEY", "");

		return signature;
	}
}
