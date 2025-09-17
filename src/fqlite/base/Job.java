package fqlite.base;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
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
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import fqlite.analyzer.BLOBCache;
import fqlite.descriptor.ADComparator;
import fqlite.descriptor.AbstractDescriptor;
import fqlite.descriptor.IndexDescriptor;
import fqlite.descriptor.TableDescriptor;
import fqlite.export.SQLiteDatabaseCreator;
import fqlite.log.AppLog;
import fqlite.pattern.HeaderPattern;
import fqlite.types.ExportType;
import fqlite.types.FileTypes;
import fqlite.ui.DBPropertyPanel;
import fqlite.ui.NodeObject;
import fqlite.ui.RollbackPropertyPanel;
import fqlite.ui.WALPropertyPanel;
import fqlite.util.Auxiliary;
import fqlite.util.ByteSeqSearcher;
import fqlite.util.Version;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.TreeItem;
import javafx.scene.image.Image;

import static java.nio.file.Files.newBufferedWriter;


/**
 * Core application class. It is used to recover lost SQLite records from a
 * sqlite database file. As a carving utility, it is binary based and can
 * recover deleted entries from sqlite 3.x database files.
 * 
 * Note: For each database file exactly one Job object is created. This class
 * has a reference to the file on disk.
 * 
 * From the sqlite web-page: "A database file might contain one or more pages
 * that are not in active use. Unused pages can come about, for example, when
 * information is deleted startRegion the database. Unused pages are stored on
 * the freelist and are reused when additional pages are required."
 * 
 * This class makes use of this behavior.
 * 
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
 * 
 * 
 * @author Dirk Pawlaszczyk
 * @version 2.4
 */
public class Job {

	/* the byte buffer representing the database file in RAM */
	public BigByteBuffer db;
	
	
	/* since version 1.2 - support for write-ahead logs WAL */
	public boolean readWAL = false;
	String walpath = null;
	public WALReader wal = null;
	public Hashtable<String, String> guiwaltab = new Hashtable<String, String>();

	/* since version 1.2 - support for write Rollback Journal files */
	public boolean readRollbackJournal = false;
	String rollbackjournalpath = null;
	public RollbackJournalReader rol = null;
	public Hashtable<String, String> guiroltab = new Hashtable<String, String>();
	
	/* this is a multi-threaded program -> all data are saved to the list first*/
	ConcurrentHashMap<String,ObservableList<ObservableList<String>>> resultlist = new ConcurrentHashMap<>();
	
    /* some constants */
	final static String MAGIC_HEADER_STRING = "53514c69746520666f726d6174203300";
	final static String NO_AUTO_VACUUM = "00000000";
	final static String NO_MORE_ENTRIES = "00000000";
	final static String LEAF_PAGE = "0d";
	final static String INTERIOR_PAGE = "05";
	final static String ROW_ID = "00";
	
	
	/* the next fields hold information for the GUI */
	public GUI gui = null;
	public TreeItem<NodeObject> dbNode = null;
	public TreeItem<NodeObject> walNode = null;
	public TreeItem<NodeObject> rjNode = null;
	
	/* if a page is handled as a regular overflow page -> don't analyse those pages */
	//public ConcurrentLinkedQueue<Integer> overflowpages = new ConcurrentLinkedQueue<>();
	
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
		
	/* this field represents the database encoding */
	public static Charset db_encoding = StandardCharsets.UTF_8;
	
	/* the list is used to export the recovered data records */
	public List<String> lines = new LinkedList<String>();
	
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
	long appid;
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
	public Map<String,Image> Thumbnails = new HashMap<String,Image>();
	public Map<String,String> FileCache = new HashMap<String,String>();
	public BLOBCache bincache;
	
	
	// Offset (table name plus rowid) + object hash value to detect updates in Journal Mode
	public Map<String,Integer> LineHashes = new HashMap<String,Integer>();
		
	// Offset (table name plus rowid) + checkpoint hashes
	public Map<String,LinkedList<Version>> TimeLineHashes = new HashMap<String,LinkedList<Version>>();
	
	public HashMap<String,String> convertto = new HashMap<String,String>(); 
	
	public Set<String> inspectBASE64 = new HashSet<String>();
	
    public List<String> fts4tables = new ArrayList<String>();
  

	public AtomicInteger numberofcells = new AtomicInteger();
	
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
	long fphead = 0;
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
	
	public SortedSet<Integer> mastertable = new ConcurrentSkipListSet<Integer>();
	
	public TableDescriptor tdefault = null;
	
	//public Hashtable<String,List<String>> autoindex = new Hashtable<String,List<String>>();
	
	public List<Integer> freelistpages = new ArrayList<Integer>();
	
    public LinkedList<WALFrame> checkpointlist = new LinkedList<WALFrame>();

	
	/******************************************************************************************************/
	
	/**
	 * This method is used to read the database file into RAM.
	 * @return a read-only ByteBuffer representing the db content
	 * @throws IOException
	 */
	private void readFileIntoBuffer() throws IOException {
		/* read the complete file into a ByteBuffer */
		db = new BigByteBuffer(path);
		
		size = db.limit();
		
		// set file pointer to begin of the file
		db.position(0);

	}
	
	
	private BigByteBuffer readWALIntoBuffer(String walpath) throws IOException {
	
		/* read the complete file into a ByteBuffer */
		BigByteBuffer bbb = new BigByteBuffer(walpath);
		
		size = bbb.capacity();
		
		return bbb;
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
				{"68","The “Application ID” set by PRAGMA application_id." + "",String.valueOf(appid)},
				{"74","Reserved for expansion. Must be zero." + "",String.valueOf(0)},
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
	
	
	public void updateResultSet(LinkedList<String> line) 
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
	
	
	public String[][] getPagesOverview(){
		
		String[][] pagelist = new String[pages.length-1][5]; 
	
		LinkedList<String> ll = new LinkedList<String>();
		
		if (pages == null)
			return null;
		
		for(int i = 1; i < (pages.length); i++)
		{	
			long offset = ps * (i - 1L);
			
			if(offset >= db.capacity())
				continue;
			
			db.position(offset);	
	      
			byte typ = db.get();
			
	        String type = "";
	        switch(typ){
	        	case 1: type = "pointer map";
	        			break;
	        	case 2: type = "interior index b-tree page";
	        			break;   
	        	case 5: type = "interior table b-tree page";
    					break;   
	        	case 10: type = "leaf index b-tree page";
    					break;   
	        	case 13: type = "leaf table b-tree page";
    					break;   
	        	case 0: type =  "overflow or unassigned";
    					break;   
    
	        }
			
			if (null != pages[i]) {
			    ll.add(offset+"");
			    if (i == 1)
			    	ll.add("database header");
			    else
			    	ll.add(type);
				ll.add(pages[i].getName());
				ll.add(pages[i].serialtypes.toString());
				String[] line  = new String[4];
				ll.toArray(line);
				pagelist[i-1]= line; 
				ll.clear();
			}
			else {
			    ll.add(offset+"");
			    if (i == 1)
			    	ll.add("database header");
			    else
			    	ll.add(type);
				ll.add("null");
				ll.add("null");
				ll.add("null");
				pagelist[i-1]= ll.toArray(new String[0]);
				ll.clear();
			}
		}
		return pagelist;
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
		/* check for empty database */
		if (headers.size()-1 + indices.size() == -1)
			return null;
		
		String [][] prop = new String[headers.size()-1 + indices.size() + 2][6];//{{"","",""},{"","",""}};
		int counter = 0;
		
		Iterator<TableDescriptor> it1 = headers.iterator();
		
		while (it1.hasNext())
		{
			TableDescriptor td = it1.next();
			
		    if (!td.tblname.startsWith("__"))
		    {	
		    	prop[counter] = new String[]{"Table",td.tblname,String.valueOf(td.root),td.sql,String.valueOf(td.isVirtual()),String.valueOf(td.ROWID)};
		    	counter++;
		    }
							
		}
		
		Iterator<IndexDescriptor> it2 = indices.iterator();
		
		while (it2.hasNext())
		{
			IndexDescriptor td = it2.next();
		    try {
			prop[counter] = new String[]{"Index",td.idxname,String.valueOf(td.root),td.getSql(),"",""};
		    }catch(Exception err) {
		    	System.out.println("Exception wurde abgefangen");
		    }
			
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
		panel.initPagesTable(getPagesOverview());
	}

	public void updateWALPanel()
	{
		walpanel.initHeaderTable(getWALHeaderProperties());
		walpanel.initCheckpointTable(getCheckpointProperties());
	}
	
	private void createFREELISTTable(int numberofcolumns){
		
		
		List<String> col = new ArrayList<String>();
		List<String> names = new ArrayList<String>();

		/* create dummy component for unassigned records */
		for (int i = 0; i < numberofcolumns + 10; i++) {
			col.add("TEXT");
			names.add("col" + (i + 1));
		}
		
		tdefault = new TableDescriptor("__FREELIST", "",col, col, names, null, null, null, false);	

	}
	
	
	/**
	 * This is the main processing loop of the program.
	 * 
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	@SuppressWarnings("unchecked")
	protected int processDB() throws InterruptedException, ExecutionException {

		bincache = new BLOBCache(this);
		
		allreadyvisit = ConcurrentHashMap.newKeySet();

		try {

			Path p = Paths.get(path);
			this.filename = p.getFileName().toString();
			this.path = p.toString();

			/* First - try to analyse the db-schema
			 * We have to do this before we open the database because of the concurrent
			 * access in multi-threading mode
			 */

			/** Caution!!! We read the complete file into RAM **/
			readFileIntoBuffer();

			/* read header of the SQLite DB - the first 100 bytes */
			ByteBuffer buffer = ByteBuffer.allocate(100);

			db.read(buffer, 0);
			
			
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
			 * 52	 4	The page number of the largest root B-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
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
			headerstring = Auxiliary.bytesToHex3(header);
			char charArray[] = new char[16];
			
			int cn = 0;
			for (byte b: header)
			{
				charArray[cn] = (char)b;
				cn++;
			}
			String txt = new String(charArray);
			
			headerstring = txt + " (" + "0x" + headerstring + ")";
			
			if (Auxiliary.bytesToHex3(header).equals(MAGIC_HEADER_STRING)) // we currently
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
 
			appid = Integer.toUnsignedLong(buffer.getInt());
			info("appid (offset 68) " + appid);
			
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
			//totalbytes = file.size();
			totalbytes = db.capacity();
			
			/*
			 * dividing the number of bytes by pagesize we can compute the number of pages.
			 */
			numberofpages = (int) (totalbytes / ps);

			info("Number of pages:" + numberofpages);

			/* extract schema startRegion binary file */

			ByteBuffer schema = ByteBuffer.allocate(ps);

			/* we use a parallel read up */
			//Future<Integer> rs = file.read(schema, 0);
			db.read(schema, 0);

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
			BigByteBuffer bb = db;
			
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
				long index = bsearch.indexOf(bb, 0);
	
				/* search as long as we can find further table keywords */
				while (index != -1) {
					/* get Header */
					byte mheader[] = new byte[40];
					bb.position(index - goback);
					bb.get(mheader);
					String headerStr = Auxiliary.bytesToHex3(mheader);
	
					
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
	
				long index2 = bisearch.indexOf(bb, 0);
	
				/* search as long as we can find further index keywords */
				while (index2 != -1) {
	
					/* get Header */
					byte mheader[] = new byte[40];
					bb.position(index2 - goback);
					try {
						bb.get(mheader);
					}
					catch(Exception err) {
						index2 = -1;
						continue;
					}
					String headerStr = Auxiliary.bytesToHex3(mheader);
		
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
									id.addColumtype(type);
								}
								catch(Exception err)
								{
									
									id.addColumtype("");
								}
								

							}

						}
						/**
						 * The index contains data from the columns that you specify in the index and
						 * the corresponding rowid value. This helps SQLite quickly locate the row based
						 * on the values of the indexed columns.
						 */

						id.columnnames.add("rowid");
						id.addColumtype("INT");

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
			if(autovacuum && db.capacity() >= ps*2){
				
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
				//Future<Integer> operation = file.read(pointermap, ps);
				
				db.read(pointermap, ps);
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

				
					byte type = -1;
					boolean cont = true;
					try {
						type = pointermap.get();
					}catch(Exception err) {
						cont = false;
					}
					
					if (cont) {
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
						
						case 4:  	break;
						
						case 5:  	break;
						
						
						}
					}
					
				
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
				//info(" root offset for component " + r);

				String signature = td.getSignature();
				//info(" signature " + signature);

				/* save component fingerprint for compare */
				if (signature != null && signature.length() > 0)
					tblSig.put(td.getSignature(), td.tblname);

							
				/* update treeview - skip this step in console modus */
				if (null != gui) {
                     
					//Platform.runLater(() -> {
						
						
						/* since table nodes will also be needed in journals and wal-files we have to add those nodes too */
						
						String path = gui.add_table(this, td.tblname, td.columnnames, td.getColumntypes(), td.primarykeycolumns, td.boolcolumns, false, false,0);
						guitab.put(td.tblname, path);
							//lastpath = path;
												
						if (readWAL) {
													
							
							List<String> cnames = 	new ArrayList<>(td.columnnames);							
							cnames.add(0,"commit");
							cnames.add(1,"dbpage");
							cnames.add(2,"walframe");
							cnames.add(3,"salt1");
							cnames.add(4,"salt2");
							
							List<String> ctypes = new ArrayList<>(td.serialtypes);
							
							/* add missing SQL types for the first 5 columns */
							for(int cc =0; cc < 5; cc++)
							{
							//	td.sqltypes.add(0,"INT");
							}
							
							String walpath = gui.add_table(this, td.tblname, cnames, ctypes, td.primarykeycolumns, td.boolcolumns, true, false,0);
							guiwaltab.put(td.tblname, walpath);
							setWALPath(walpath.toString());
	
						}
	
						else if (readRollbackJournal) {
							String rjpath = gui.add_table(this, td.tblname, td.columnnames, td.getColumntypes(),td.primarykeycolumns, td.boolcolumns, false, true,0);
							guiroltab.put(td.tblname, rjpath);
							setRollbackJournalPath(rjpath.toString());
						}
					//});
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
				AppLog.debug(" root offset for index " + r);

				/* update treeview in HexViewFactory - skip this step in console modus */
				if (null != gui) {
					
					if(id.columnnames.size() > id.columntypes.size())
					{
						id.columntypes.add("String");
					}
					
					String path = gui.add_table(this, id.idxname, id.columnnames, id.columntypes, id.boolcolumns, null, false, false,1);
					
					//System.out.println("id.idxname " + id.idxname);
					guitab.put(id.idxname, path);

					if (readWAL) {
						
						
						List<String> cnames = new ArrayList<>(id.columnnames);	
						cnames.add(0,"commit");
						cnames.add(1,"dbpage");
						cnames.add(2,"walframe");
						cnames.add(3,"salt1");
						cnames.add(4,"salt2");
						
						List<String> ctypes = new ArrayList<>(id.columntypes);

						String walpath = gui.add_table(this, id.idxname, cnames, ctypes, id.boolcolumns, null, true, false,1);
						guiwaltab.put(id.idxname, walpath);
						setWALPath(walpath);
					}
					else if (readRollbackJournal) {
						
							String rjpath = gui.add_table(this, id.idxname, id.columnnames, id.columntypes, id.boolcolumns, null, false, true,1);
							guiroltab.put(id.idxname, rjpath);
							setRollbackJournalPath(rjpath);
					}

					
				}
				
				/* transfer component information for later recovery */
				RecoveryTask.tables.add(id);


			}
		
			try{
				RecoveryTask.tables.sort(new ADComparator());
			}
			catch(Exception err)
			{
                AppLog.error(err.getMessage());
				System.out.println(err);
			}


            int maxcol = 0;
            for(TableDescriptor t : headers){
                maxcol = Math.max(t.columnnames.size(),maxcol);
            }

            // We need to create a table for all "free list" content that could not be assigned directly.
            createFREELISTTable(maxcol);



			/*
			 * Sometimes, a record cannot be assigned to a component or index -> these records
			 * are assigned to the __FREELIST component.
			 */

			if (null != gui) {
				/* create header for the SQLiteMaster table */
				List<String> mcol = new ArrayList<String>();
				mcol.add("TEXT"); mcol.add("TEXT"); mcol.add("TEXT"); mcol.add("INT"); mcol.add("TEXT");
			
				HeaderPattern hp = new HeaderPattern();
				hp.addStringConstraint();
				hp.addStringConstraint();
				hp.addStringConstraint();
				hp.addIntegerConstraint();
				hp.addStringConstraint();
				
				
				List<String> mnames = new ArrayList<String>();
				mnames.add("object"); mnames.add("obj name"); mnames.add("namespace");  mnames.add("root page");  mnames.add("Statement");
				TableDescriptor tdmaster = new TableDescriptor("__SQLiteMaster", "",mcol, mcol, mnames, null, null, hp, false);
				for (int pg: mastertable) {
				    if (pg > 0)
						if(pg < pages.length && pages[pg]== null)
					    {	
							try {
								headers.add(pg,tdmaster);
							}
							catch(Exception err){
								AppLog.debug(err.toString());
							}
							pages[pg] = tdmaster;
					    }
				
					
				}

                String path = gui.add_table(this, tdefault.tblname, tdefault.columnnames,
                        tdefault.getColumntypes(), tdefault.boolcolumns, null, false, false,0);
                guitab.put(tdefault.tblname, path);

                String pathmaster = gui.add_table(this, tdmaster.tblname, tdmaster.columnnames,
							tdmaster.getColumntypes(), tdmaster.boolcolumns, null, false, false,0);
					guitab.put(tdmaster.tblname, pathmaster);
				
					
					//lastpath = path;

					if (readWAL) {
						
						tdefault.columnnames.add(0, "salt2");
						tdefault.columnnames.add(0, "salt1");
						tdefault.columnnames.add(0, "walframe");
						tdefault.columnnames.add(0, "dbpage");
						tdefault.columnnames.add(0, "commit");
						
						
						tdefault.getColumntypes().add(0, "INTEGER");
						tdefault.getColumntypes().add(0, "INTEGER");
						tdefault.getColumntypes().add(0, "INTEGER");
						tdefault.getColumntypes().add(0, "INTEGER");
						tdefault.getColumntypes().add(0, "BOOLEAN");
						
						
						String walpath = gui.add_table(this, tdefault.tblname, tdefault.columnnames,
								tdefault.getColumntypes(), tdefault.boolcolumns ,null, true, false,0);
						guiwaltab.put(tdefault.tblname, walpath);
						setWALPath(walpath);
		
						tdmaster.columnnames.add(0, "salt2");
						tdmaster.columnnames.add(0, "salt1");
						tdmaster.columnnames.add(0, "walframe");
						tdmaster.columnnames.add(0, "dbpage");
						tdmaster.columnnames.add(0, "commit");
						
						
						tdmaster.getColumntypes().add(0, "INTEGER");
						tdmaster.getColumntypes().add(0, "INTEGER");
						tdmaster.getColumntypes().add(0, "INTEGER");
						tdmaster.getColumntypes().add(0, "INTEGER");
						tdmaster.getColumntypes().add(0, "BOOLEAN");
							
						
						String walpathmaster = gui.add_table(this, tdmaster.tblname, tdmaster.columnnames,
								tdmaster.getColumntypes(), tdmaster.boolcolumns ,null, true, false,0);
						guiwaltab.put(tdmaster.tblname, walpathmaster);
						
						
					}
					else if (readRollbackJournal) {
						String rjpath = gui.add_table(this, tdefault.tblname, tdefault.columnnames,
								tdefault.getColumntypes(),tdefault.boolcolumns ,null, false, true,0);
						guiroltab.put(tdefault.tblname, rjpath);
						setRollbackJournalPath(rjpath);
					
						String rjpathmaster = gui.add_table(this, tdmaster.tblname, tdmaster.columnnames,
								tdmaster.getColumntypes(), tdmaster.boolcolumns ,null,false, true,0);
						guiroltab.put(tdmaster.tblname, rjpathmaster);
					
					}

					
			}

			
			if(emptydb){
				
				System.out.println("Omit analysis of the database since database file is empty ");
				return 0;
			}


            ///*******************************************************************/

			byte[] freepageno = new byte[4];
			buffer.position(36);
			buffer.get(freepageno);
			info("Total number of free list (trunk) pages " + Auxiliary.bytesToHex3(freepageno));
			ByteBuffer no = ByteBuffer.wrap(freepageno);
			fpnumber = no.getInt();
			System.out.println(" no " + fpnumber);

			///******************************************************************/

			byte[] freelistpage = new byte[4];
			buffer.position(32);
			buffer.get(freelistpage);
			info("FreeListPage starts at offset " + Auxiliary.bytesToHex3(freelistpage));
			ByteBuffer freelistoffset = ByteBuffer.wrap(freelistpage);
			long head = freelistoffset.getInt();
			info("head:: " + head);
			fphead = head;
			long start = (head - 1L) * ps;

			///*****************************************************************/

			if (head == 0) {
				info("INFO: Couldn't locate any free pages to recover. ");
			}

			
			/*
			 * STEP 1: We start the recovery process by scanning the free list first
			 */

			 if (head > 0) {
				AppLog.debug("first:: " + start + " 0hx " + Long.toHexString(start));

				long startfp = System.currentTimeMillis();
				System.out.println("Start free page recovery .....");

				// seeking file pointer to the first free page entry

				/* create a new threadpool to analyse the freepages */
				ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Global.numberofThreads);

				/* A list can extend over several memory pages. */
				boolean morelistpages = false;

				int freepagesum = 0;

				do {
					/* reserve space for the first/next page of the free list */
					ByteBuffer fplist = ByteBuffer.allocate(ps);

					db.read(fplist, start);
					
					// next (possible) freepage list offset or 0xh00000000 + number of entries
					// example : 00 00 15 3C | 00 00 02 2B

					// now read the first 4 bytes to get the offset for the next free page list
					byte[] nextlistoffset = new byte[4];

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
					if (!Auxiliary.bytesToHex3(nextlistoffset).equals(NO_MORE_ENTRIES)) {
						
						ByteBuffer of = ByteBuffer.wrap(nextlistoffset);
						int nfp = of.getInt();
						start = (nfp - 1L) * ps;
						if (!allreadyvisit.contains(nfp))
							allreadyvisit.add(nfp);
						else {
							info("Antiforensiscs found: cyclic freepage list entry");
						}
						morelistpages = true;

					} 
					else
						morelistpages = false;

					// now read the number of entries for this particular page
					byte[] numberOfEntries = new byte[4];
					try
					{
						fplist.get(numberOfEntries);
					}
					catch(Exception err)
					{
						AppLog.error(err.getMessage());
					}
					
					int entries = 0;
					
				    ByteBuffer e = ByteBuffer.wrap(numberOfEntries);
					entries = e.getInt();
					
					info(" Number of Entries in freepage list " + fpnumber);  //2704

					runningTasks.set(0);
					
					/* iterate through free page list and read free page offsets */
					for (int zz = 1; zz <= entries; zz++) {
				
						byte[] next = new byte[4];
						fplist.position(4 + 4 * zz);
						fplist.get(next); 

						ByteBuffer bf = ByteBuffer.wrap(next);
						int n = bf.getInt();

						if (n == 0) {
							continue;
						}
						// determine offset for free page
						long offset = (n - 1L) * ps;

						pages[n] = this.tdefault;

						/* remember already scanned pages in a list */ 
						freelistpages.add(n);
						RecoveryTask task1 = new RecoveryTask(new Auxiliary(this), this, offset, n, ps, true);
						/* add new task to executor queue */
						runningTasks.incrementAndGet();
						tasklist.add(task1);
						//executor.execute(task1);
					    task1.run();
					}
					freepagesum += entries;

				} while (morelistpages); // while
	
				System.out.println("Number of pages in the free list : " + freelistpages.size());
				
				
				executor.shutdown();

				info("ImportDBTask total: " + runningTasks.intValue());

				// wait for Threads to finish the tasks
				while (runningTasks.intValue() != 0) {
					try {
						TimeUnit.MILLISECONDS.sleep(10);
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


			// full db-scan (including all database pages)
			scan(numberofpages, ps);

			if (gui != null) {
				
				info("Number of tables recovered: " + resultlist.size());
				System.out.println("Number of tables recovered: " + resultlist.size());
				
				Enumeration<String> tables = resultlist.keys();
				
				/* finally, we can update the TableView for each table */
				while(tables.hasMoreElements())
				{	
					String tablename = tables.nextElement();
			        /* get tree path, i.e. /databases/02-05.db/users */
					String path = guitab.get(tablename);
					if (null != path)
						gui.update_table(path,resultlist.get(tablename),false);
				}
					

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

				
			}
						
			// start with import
			Platform.runLater(() -> {
				
				/* prepare garbage collection - */
 				db.clear(); //db = null;
 				lines.clear(); lines = null;
 				tasklist.clear(); 
 				tasklist = null;
 				System.gc();
 				System.out.println("Garbage Collector started...");
			    	
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
	 * This happens quite often, because by default, a commit to the actual database
	 * only takes place after the WAL archive has reached 1000 pages. 
	 */
	private BigByteBuffer readCheckpoint(){

		TreeMap<Integer,ByteBuffer> cptable = new TreeMap<Integer,ByteBuffer>();
		
		BigByteBuffer wal = null;
        ByteBuffer checkpoint = ByteBuffer.allocate(0);
			
		try {
			wal = readWALIntoBuffer(this.path+"-wal");
		} catch (IOException e) {
			System.err.println("Could not access wal-archive");
			return null;
		}
		
		
		
		boolean next = false;
		int framestart = 32; // this is the position, where the first frame should be
	    
		int frame = 1;
		
	    do {

	    	/* 24 Byte - with six 4-Byte big endian values */
			byte[] frameheader = new byte[24];
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
			
			long salt1 = fheader.getInt();
			long salt2 = fheader.getInt();
			
			/* read the db page into buffer */
			ByteBuffer page = wal.slice();
			page.limit(ps);
			
			framestart += ps +  24;
		    cptable.put(pagenumber_maindb,page);
		
		    
			WALFrame f = new WALFrame(pagenumber_maindb, frame , salt1, salt2, commit != 0);
			checkpointlist.add(f);

		 	
			/*  More pages to analyze? */
			if(framestart+24+ps < size)
			{
				
				next = true;
			}
			else
				next = false;
			
			
	    	frame++;
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
	    
	    /*
	     * Important: Whenever the database file is empty and the WAL archive
	     * has not been committed yet; all information is in the WAL file. This
	     * also applies to the number of pages in the database. 
	     */
		pages = new AbstractDescriptor[cptable.size() + 1];

		
		
	    
	    return new BigByteBuffer(checkpoint);
	}

    /**
     * This method can be used to export all recovered database records into
     * a new database.
     *
     * @param objectname name of the database file
     * @param exp the type of source file (db, rb-journal or wal)
     * @return successful or not
     */
	public boolean exportDB(String filename, String objectname, ExportType exp){

        String dbname = objectname;

        if(exp == ExportType.SQLITEDB || exp == ExportType.ROLLBACKJOURNAL || exp == ExportType.WALARCHIVE)
        {

            ConcurrentHashMap<String,ObservableList<ObservableList<String>>> exportlist = switch (exp) {
                case ROLLBACKJOURNAL -> this.rol.resultlist;
                case SQLITEDB -> this.resultlist;
                case WALARCHIVE -> this.wal.resultlist;
                default -> null;
            };

            /* 1st step: get a database manager instance and create the database schema */
            SQLiteDatabaseCreator exporter = SQLiteDatabaseCreator.getInstance();
            try {
                exporter.createDatabaseAndSchema(headers, tdefault, objectname,  exp == ExportType.WALARCHIVE);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            /* get all values of the result list*/
            Enumeration<String> keyset = exportlist.keys();

            /* iterate over all tables inside the file */
            while (keyset.hasMoreElements()) {

                /* get next table */
               String tblname = keyset.nextElement();
               ObservableList<ObservableList<String>> rows = exportlist.get(tblname);

                try {
                    // skip the internal tables
                    if(tblname.equals("__SQLiteMaster") || tblname.startsWith("sqlite_"))
                        continue;
                    exporter.insertRows(bincache,filename,tblname,headers,tdefault,rows,exp == ExportType.WALARCHIVE);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

            }
        }

        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle("Information Dialog");
        alert.setHeaderText("Export was successful"); // oder null
        alert.setContentText("All tables were successfully exported into a new database.");
        alert.showAndWait();

        return true;
    }


	
	/// This method could be used to export the content of a single table,
	/// a complete database, a WAL archive or even a rollback-journal
	/// to disk.
	///
	/// @param file  		destination
	/// @param objectname	the name of the table or database to export
	/// @param parenttype	is it a normal database, WAL or journal?
	/// @param exp			type of export (single table, complete database, WAL, journal)
	@SuppressWarnings("incomplete-switch")
	public boolean exportResults2File(File file,String objectname, FileTypes parenttype, ExportType exp){

		String dbname = null;
				
		try (final BufferedWriter writer = newBufferedWriter(file.toPath(),StandardCharsets.UTF_8, StandardOpenOption.CREATE))
		{
				
			/*
			 * CASE 1: Export a complete database file with all table information inside
			 */
			
			if(exp == ExportType.SQLITEDB || exp == ExportType.ROLLBACKJOURNAL || exp == ExportType.WALARCHIVE)
			{
				dbname = objectname;

				ConcurrentHashMap<String,ObservableList<ObservableList<String>>> exportlist = switch (exp) {
                    case ROLLBACKJOURNAL -> this.rol.resultlist;
                    case SQLITEDB -> this.resultlist;
                    case WALARCHIVE -> this.wal.resultlist;
                    default -> null;
                };


                /* get all values of the result list*/
				Enumeration<String> keyset = exportlist.keys();
				
				/* iterate over all tables inside the file */
				while (keyset.hasMoreElements()) {
					
					/* get next table */
					String tblname = keyset.nextElement();
					ObservableList<ObservableList<String>> table = exportlist.get(tblname);

					if(Global.EXPORTTABLEHEADER)
	  				 	writer.write(prepareHeader(tblname) + "\n");
		  			
					/* get table lines */

                    /* as long as there are lines inside, go on */
                    for (ObservableList<String> strings : table) {
                        String out = prepareOutput(strings, dbname, file.getParent()) + "\n";
                        writer.write(out);
                    }
					
					
				}
			}

			/*
			 * CASE 2: Export a particular table
			 */
			else{
				
				ObservableList<ObservableList<String>> table = null;
  				
				/* get table rows from result list */
				switch(parenttype){
					case RollbackJournalLog:
						table = this.rol.resultlist.get(objectname);
						dbname = this.filename + "-journal";
					break;
					case SQLiteDB:
						table = this.resultlist.get(objectname);
						dbname = this.filename;
					break;
					case WriteAheadLog:
						table = this.wal.resultlist.get(objectname);
						dbname = this.filename + "-wal";
					break;
				}
			 
  				 if (null == table){
  					 return false;
  				 }
			
  				 Iterator<ObservableList<String>> iter = table.iterator();
				
  				 if(Global.EXPORTTABLEHEADER)
  				 	writer.write(prepareHeader(objectname) + "\n");
	  				
  				 /* as long as there are lines inside, go on */
				 while(iter.hasNext()){
						String out = prepareOutput(iter.next(),dbname,file.getParent()) + "\n";
						writer.write(out);
				 }
					
				 
			}
				
			
				
		}
	    catch (IOException e) {
	    	// in case something went wrong 
			e.printStackTrace();
			return false;
		}
	
		
		return true;

	}
	
	
	private String prepareHeader(String tablename) {
		
		String token = Global.CSV_SEPARATOR;
		
		if(token.equals("[TAB]"))
			token = "\t";
		
		
		String[] head = getColumnNamesForTable(tablename);
		if (head != null) {
			String line = "";
			line += "PLL" + token + "HL" + token + "STATE" + token + "OFFSET" + token;
			for(String c : head){
				line += c + token;
			}
			return line;
		}
		
		return null;
	}
	
	private String[] getColumnNamesForTable (String tablename){

        for (TableDescriptor td: headers) {
            if (td.tblname.equals(tablename)) {

                return td.columnnames.toArray(new String[0]);

            }
        }
		
		return null;
	}
	
	private String prepareOutput(ObservableList<String> list, String dbname, String exportfolder) throws IOException{
		
		
			String token = Global.CSV_SEPARATOR;
			
			if(token.equals("[TAB]"))
				token = "\t";
		
			/* get String token (cell values) */
			String[] line = list.toArray(new String[0]);
			
			String output = "";
			String offset = null;
			
			int counter = 0;
				
			/* skip schema definition lines of table __SQLiteMaster */
			if(line[0].startsWith("__SQLiteMaster"))
					return null;
			
			// iterate over all cells in one particular table row
			for (String c: line){
			
				/* skip first column - it normally holds the table name */
				if(counter == 0)
				{
					counter++;
					continue;
				}
				
				/* put a separator between the two values */
				if(counter > 1)
					output += token;
				
				
				/* column for offset found? */
	    		if(counter == 4)
	    		{
	    			offset = c;
	    			output += offset;
	    			counter++;
	    			continue;
	    		}
				
				
				/* If the cell value is a BLOB, it depends on the
				 * export mode (from the properties dialogue) how to
				 * continue:
				 * 
				 */
				if(c!=null && c.startsWith("[BLOB"))
				{
					
					/* mode 1: do not export any BLOB values to .csv -> instead set a placeholder */
					if(Global.EXPORT_MODE == Global.EXPORT_MODES.DONTEXPORT)
					{	
						
						output += "<BLOB>"; 
						counter++;
						continue;
					}
					
					else if(Global.EXPORT_MODE == Global.EXPORT_MODES.TOCSV || Global.EXPORT_MODE == Global.EXPORT_MODES.TOSEPARATEFILES)
					{	
						
						/* BLOB-value found? */
			    		if(c.length()>7) {
			    			
			    			int from = c.indexOf("BLOB-");
			        	    int to = c.indexOf("]");
			        	    
			        	    if(from > 0 && to > 0)
			        	    {
			        	    
				        	    String number = c.substring(from+5, to);			
				        		int start = c.indexOf("<");
				        		int end   = c.indexOf(">");
				        				
				        		String type;
								if (end > 0) {
									type = c.substring(start+1,end);
								}
								else 
				        			type = "bin";
				        				
				        		if(type.equals("java"))
				        			type = "bin";
				        			
				        		String path = GUI.baseDir + Global.separator + dbname + "_" + offset + "-" + number + "." + type;
				        		String data = bincache.getHexString(path);
				    
				        		/* CASE: export BLOB data directly to CSV */
				        		if (Global.EXPORT_MODE == Global.EXPORT_MODES.TOCSV)
				        		{
				        			c = data.toUpperCase();
				        		}
				        		/* CASE: export BLOB to separate file & write filename to csv-file */
				        		else
				        		{
				        			// file name of the BLOB 
				        			c = dbname + "_" + offset + "-" + number + "." + type;
				        			String saveas = exportfolder + Global.separator + c;
				        			Path p = Path.of(saveas);
				        			
				        			// write binary file to export folder
				        			Files.write(p,bincache.get(path).binary,StandardOpenOption.CREATE);

				        		}
				        	   
				        }
			        	    
							
			        }
							
						output += c;
						counter++;
						continue;
					}
				
				}
				
				/* No BLOB? -> just copy table cell content to output string */
                counter++;	
				output += c; 
			}
	
			return output;
	}
	
	
	
	/**
	 * Save findings into a comma-separated file.
	 * @param filename name of file to export
	 * @param lines lines to write into the file
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
		

		/* convert line to UTF-8 */
		try {
			
			final File file = new File(filename);
            
			
		    try (final BufferedWriter writer = newBufferedWriter(file.toPath(),Charset.forName("UTF-8"), StandardOpenOption.CREATE))
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
	
	
	private void readSchema(BigByteBuffer bb, long starthere, String headerStr) throws IOException
	{
		
		
		/* we need a utility method for parsing the Mastertable */
		Auxiliary c = new Auxiliary(this);
		
		// compute offset

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
		
	}

	

	/**
	 * The method creates an ImportDBTask object for each page to be scanned.
	 * Each task is then scheduled into a worker thread's To-Do list.
	 * After all tasks are assigned, the worker threads are started 
	 * and begin processing.
	 * @param number number of threads
	 * @param ps page size
	 */
	public void scan(int number, int ps) {
		info("Start with scan...");
		
		/* create a new thread pool to analyse the freepages */
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Global.numberofThreads);

		long begin = System.currentTimeMillis();
		
		/* first start scan of regular pages */
	
		Worker[] worker = new Worker[Global.numberofThreads]; 
        for (int i = 0; i < Global.numberofThreads; i++)
        {
        	worker[i] = new Worker(this);
        }
		
		for (int cc = 1; cc < pages.length; cc++) {
			
			if (cc == 1) {
				RecoveryTask task = new RecoveryTask(worker[cc % Global.numberofThreads].util,this, 100, cc, ps, false);
				worker[cc % Global.numberofThreads].addTask(task);
				runningTasks.incrementAndGet();
			}	
			else if (null == pages[cc]) 
			{
				AppLog.debug("page " + cc + " is no regular leaf page component. Maybe a indices or overflow or dropped component page.");			
			} 
			else 
			{
				AppLog.debug("page " + cc + " is a regular leaf page. ");
				AppLog.debug("table: " + pages[cc].getName());
						
				
				if (pages[cc].doNotScan) {
					AppLog.debug("Skip page " + cc + " since it was already scanned while free list lookup");
					continue;
				}
				
				// determine offset for free page
				long offset = (cc - 1L) * ps;
                
				/* skip formally scanned pages */
				if(freelistpages.contains(cc))
					continue;
				
				RecoveryTask task = new RecoveryTask(worker[cc % Global.numberofThreads].util,this, offset, cc, ps, false);
				worker[cc % Global.numberofThreads].addTask(task);
				runningTasks.incrementAndGet();
			}
		}
		AppLog.debug("ImportDBTask total: " + runningTasks.intValue() + " worker threads " + Global.numberofThreads);
		
		//int c = 1;
		/* start executing the work threads */
		for (Worker w: worker)
		{	
			//System.out.println(" Start worker thread" + c++);
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
		{	
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
		AppLog.setLevel(Global.LOGLEVEL);
	}


	/**
	 * Start processing a new SQLite file.
	 * 
	 * @param p path
	 * @return success or not
	 */
	public int run(String p) {
		
		int hashcode = -1;
		path = p;
		long start = System.currentTimeMillis();
		AppLog.info("Start import of file " + path);
		try {
			hashcode = processDB();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		AppLog.info("Duration in ms: " + (end - start));
		System.out.println("Duration in ms: " + (end - start));

		return hashcode;
	}


	/**
	 * Read a database page from a given offset with a fixed pagesize.
	 * 
	 * @param offset byte position in database
	 * @param pagesize number in bytes for a single database page
	 * @return  A <code>ByteBuffer</code> object containing the page content. 
	 */
	public ByteBuffer readDBPageWithOffset(long offset, int pagesize) {

		if ((offset > db.limit()) || (offset < 0))
		{
			
			System.out.println("readDBPageWithOffset()-> offset greater than file size ?!" + offset + " > " + db.limit());
						
			return null;
		}
		db.position(offset);
		ByteBuffer page = db.slice();
		if (page.capacity() == 0) {
			return null;
		}
		else
		{		
			page.limit(pagesize);
		}
		return page;
	}
	

	/**
	 * Read a WAL page from a given offset with a fixed pagesize.
	 * 
	 * @param pagenumber in the database
	 * @param pagesize number of bytes for one database page
	 * @return  A <code>ByteBuffer</code> object containing the page content. 
	 */
	 public ByteBuffer readWALOverflowPage(int frame, int pagenumber, int pagesize, int firstpage) {

		// WAL in memory file
		ByteBuffer db = wal.wal;
		
		// overflow pages until here 
		HashMap<Integer,ByteBuffer> overflow = wal.overflow;
		
		int pnumber = -1;
		
		// determine the RVA for the next frame 
		int offset =  32 + frame*(pagesize + 24) + pagesize + 24; 

			
		do 
		{
			// Check if frame is committed?
			boolean isCommited = isCommitted(frame);
			
			if (offset >= db.capacity()){
				return null;
			}
			
			db.position(offset);
			
			// get the page number of the WAL frame
			pnumber = db.getInt();
			
			//Did we find the overflow page?
			if(pnumber == pagenumber){
				
				if ((offset >  db.limit()) || (offset < 0)){
					
					System.out.println("readDBPageWithOffset()-> offset greater than file size ?!" + offset + " > " + db.limit());							
					return null;
				}
				
			
				/* Note: the maximum size for a WAL-Archive at the moment is 2,1 GB */
				db.position(offset+24);
			
				
				ByteBuffer page = db.slice();
				if (page.capacity() == 0) {
					return null;
				}
				else
				{		
					page.limit(pagesize);
				}
				
				overflow.put(pnumber,page);
				return page;
	
			}			
			else{
				
				if (isCommited && overflow.containsKey(pagenumber)){
							
						 return overflow.get(pagenumber);
				}
				
				
			}
			frame++;

			offset =  32 + frame*(pagesize + 24) + pagesize + 24; 
			
		}
		while(pnumber != pagenumber);
		
		// we could not find any page with this number inside the wal archive -> let's have look at the database
		
		// this part will hopefully never be reached ;-)
		return null;
	}
	 
	public boolean isCommitted(int frame) {
	
			// read up until next commit! frame 
			for (WALFrame f: checkpointlist){
			    	  
				  if (f.framenumber == frame){
			    	   if (f.committed) {		    		   
			    		   
			    		   return true;
			    	   }
			    	   else
			    	   {
			    		   return false;
			    	   }
			    		   
			      }
			}
			
			return false;
			
	}
	 

	
	/**
	 *  Since all pages are assigned to a unique number in SQLite, we can read a 
	 *  page by using this value together with the pagesize. 
	 *  
	 * @param pagenumber (>=1)
	 * @param pagesize - number of bytes for one single database page
	 * @return  A <code>ByteBuffer</code> object containing the page content. 
	 */
	public ByteBuffer readPageWithNumber(int pagenumber, int pagesize) {
		
		if (pagenumber < 0)
		{
		   return null;
		}
		return readDBPageWithOffset((long)pagenumber*pagesize,pagesize);
	}
	
	
	/**
	 *  Since all pages are assigned to a unique number in SQLite, we can read a 
	 *  page by using this value together with the pagesize. 
	 *  
	 * @param pagenumber (>=1)
	 * @param pagesize - number of bytes for one single database page
	 * @return  A <code>ByteBuffer</code> object containing the page content. 
	 */
	public ByteBuffer readPageWithNumberFromBuffer(int pagenumber, int pagesize, BigByteBuffer bb) {
		
		if (pagenumber < 0)
		{
		   return null;
		}
		
		long offset = (long) pagenumber * pagesize;
		
		if ((offset > bb.limit()) || (offset < 0))
		{
			
			System.out.println(" offset greater than file size ?!" + offset + " > " + bb.limit());
						
			return null;
		}
		bb.position(offset);
		ByteBuffer page = bb.slice();
		page.limit(pagesize);
		return page;
	}


	/**
	 * The B-tree, or, more specifically, the B+-tree,
	 * is the most widely used physical database structure 
	 * for primary and secondary indexes on database relations.
	 * This method can be called to traverse all nodes of 
	 * a table-tree.
	 * Attention! This is a recursive function. 
	 * 
	 * 
	 * @param root the root node.
	 * @param td this reference holds a Descriptor for the page type.
	 * @throws IOException
	 */
	private void exploreBTree(int root, AbstractDescriptor td, BigByteBuffer filebuffer) throws IOException {
		
			
		if (root < pages.length && root >= 0)
			pages[root] = td;
		else
			return;
		
		// pagesize * (rootindex - 1) -> go to the start of this page
		long offset = ps * (root - 1);
		
		if(offset <= 0)
			return;
		
		if (root <  0)
		{
			return;
		}
		
		
		// read type byte
		filebuffer.position(offset);	
        byte typ = filebuffer.get();
		
	
		/* not supported yet */
		if (typ == 2) 
		{
			AppLog.debug(" page number" + root + " is a  INDEXINTERIORPAGE.");			
			int rightChildptr = filebuffer.getInt(offset + 8);

			exploreBTree(rightChildptr, td, filebuffer);

			if (root <= 0)
				return;
			ByteBuffer buffer = readPageWithNumberFromBuffer(root - 1, ps, filebuffer);
			
			if (buffer == null)
				return;
			
			byte[] numberofcells = new byte[2];
			buffer.position(3);

			buffer.get(numberofcells);
			ByteBuffer noc = ByteBuffer.wrap(numberofcells);
			int e = Auxiliary.TwoByteBuffertoInt(noc);	
			
			
			byte[] cpn = new byte[2];
			buffer.position(5);

			buffer.get(cpn);

			/* go on with the cell pointer array */
			for (int i = 0; i < e; i++) {

				// address of the next cell pointer
				byte[] pointer = new byte[2];
				buffer.position(12 + 2 * i);
				if (buffer.capacity() <= buffer.position()+2)
					continue;
				buffer.get(pointer);
				ByteBuffer celladdr = ByteBuffer.wrap(pointer);
				int celloff = Auxiliary.TwoByteBuffertoInt(celladdr);

				AppLog.debug(" celloff " + celloff);
				// read page number of next node in the b*tree
				byte[] pnext = new byte[4];
				if (celloff >= buffer.capacity() || celloff < 0)
					continue;
				if (celloff > ps)
					continue;
				buffer.position(celloff);
				buffer.get(pnext);
				int p = ByteBuffer.wrap(pnext).getInt();
				// unfolding the next level of the tree
				AppLog.debug(" child page " + p);
				exploreBTree(p, td,filebuffer);
			}
			
			
		}
		/* type is either a data interior page (12) */
		else if (typ == 5) {

			AppLog.debug("page number " + root + " is a interior data page ");

			int rightChildptr = filebuffer.getInt(offset + 8);
			
			/* recursive */
			exploreBTree(rightChildptr, td, filebuffer);

			
			/* now we have to read the cell pointer list with offsets for the other pages */
			/* read the complete internal page into buffer */
			if (root <= 0)
				return;
			ByteBuffer buffer = readPageWithNumberFromBuffer(root - 1, ps, filebuffer);
				
			if (buffer == null)
				return;
			
			byte[] numberofcells = new byte[2];
			buffer.position(3);

			buffer.get(numberofcells);
			ByteBuffer noc = ByteBuffer.wrap(numberofcells);
			int e = Auxiliary.TwoByteBuffertoInt(noc);	
			
			
			byte[] cpn = new byte[2];
			buffer.position(5);

			buffer.get(cpn);

			/* go on with the cell pointer array */
			for (int i = 0; i < e; i++) {

				// address of the next cell pointer
				byte[] pointer = new byte[2];
				buffer.position(12 + 2 * i);
				if (buffer.capacity() <= buffer.position()+2)
					continue;
				buffer.get(pointer);
				ByteBuffer celladdr = ByteBuffer.wrap(pointer);
				int celloff = Auxiliary.TwoByteBuffertoInt(celladdr);

				AppLog.debug(" celloff " + celloff);
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
				AppLog.debug(" child page " + p);
				exploreBTree(p, td,filebuffer);
			}

		} 
		else if (typ == 8 || typ == 10 || typ == 13) {
			AppLog.debug("page number " + root + " is a leaf page " + " set component/index to " + td.getName());
			//System.out.println("page number " + root + " is a leaf page " + " set component/index to " + td.getName());
			if (root >  numberofpages)
				return;
			if (null == pages[root])
				pages[root] = td;
			else {
				if (!td.getName().equals(td.getName()))
					AppLog.debug("Page has already a B+Tree assignment with table " + td.getName());
			}
				
			//	debug("WARNING page is member in two B+Trees! Possible Antiforensics.");
		} 
		else {
			AppLog.debug("Page" + root + " is neither a leaf page nor a internal page. Try to set component to " + td.getName());
			
			if (root >  numberofpages)
				return;
			if (null == pages[root])
				pages[root] = td;

		}

	}


}
