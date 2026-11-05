package fqlite.base;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
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
import fqlite.export.CASEExport;
import fqlite.export.SQLiteDatabaseCreator;
import fqlite.log.AppLog;
import fqlite.pattern.HeaderPattern;
import fqlite.sql.DBManager;
import fqlite.sql.InMemoryDatabase;
import fqlite.types.BLOBElement;
import fqlite.types.BLOBTYPE;
import fqlite.types.ExportType;
import fqlite.types.FileType;
import fqlite.ui.DBPropertyPanel;
import fqlite.ui.NodeObject;
import fqlite.ui.RollbackPropertyPanel;
import fqlite.ui.WALPropertyPanel;
import fqlite.util.Auxiliary;
import fqlite.util.ByteSeqSearcher;
import javafx.application.Platform;
import javafx.collections.ObservableList;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.TreeItem;
import javafx.scene.image.Image;
import org.apache.commons.codec.digest.DigestUtils;

import javax.swing.*;

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
	public String walpath = null;
	public WALReader wal = null;
	public ConcurrentHashMap<String, String> guiwaltab = new ConcurrentHashMap<String, String>();

	/* since version 1.2 - support for write Rollback Journal files */
	public boolean readRollbackJournal = false;
	public String rollbackjournalpath = null;
	public RollbackJournalReader rol = null;
	public ConcurrentHashMap<String, String> guiroltab = new ConcurrentHashMap<String, String>();

	/* this is a multi-threaded program -> all data are saved to the list first*/
	public ConcurrentHashMap<String,ObservableList<ObservableList<String>>> resultlist = new ConcurrentHashMap<>();
	public ConcurrentHashMap<String,ObservableList<ObservableList<byte[]>>> hexdumplist = new ConcurrentHashMap<>();


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

	ConcurrentHashMap<String, String> guitab = new ConcurrentHashMap<String, String>();
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
	public Charset db_encoding = StandardCharsets.UTF_8;

	/* the list is used to export the recovered data records */
	//public List<String> lines = new LinkedList<String>();

	/* header fields */
	String headerstring;
	String PRAGMA_journal_mode = "OFF";

	/** value layout of recovered records ("offset:length ..."), key = filetype + ":" + record offset (NIST SFT-AO-06) */
	public final ConcurrentHashMap<String, String> cellLayout = new ConcurrentHashMap<>();
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

	ConcurrentHashMap<String, String> tblSig;
	boolean is_default = false;
	public List<TableDescriptor> headers = new ArrayList<TableDescriptor>();
	public List<IndexDescriptor> indices = new ArrayList<IndexDescriptor>();
	public AtomicInteger runningTasks = new AtomicInteger();
	int tablematch = 0;
	int indexmatch = 0;
	public BLOBCache bincache;
	public AtomicInteger numberofcells = new AtomicInteger();

	Set<Integer> allreadyvisit;

	/* all unfinished tasks are hold in this list*/
	List<RecoveryTask> tasklist = new ArrayList<RecoveryTask>();

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
	public List<Integer> freelistpages = new ArrayList<Integer>();

	/** free list trunk pages (carved like dropped pages, see RecoveryTask) */
	public final Set<Integer> freelistTrunks = java.util.concurrent.ConcurrentHashMap.newKeySet();

	/** table leaf pages that no b-tree references (manipulated pointers), see scan() */
	public final Set<Integer> orphanPages = java.util.concurrent.ConcurrentHashMap.newKeySet();

	/** rowid ranges (lo, hi] of child slots whose pointer was rejected in exploreBTree(): [table, lo, hi] */
	private final List<Object[]> danglingSlots = java.util.Collections.synchronizedList(new ArrayList<>());

	public List<WALFrame> checkpointlist = new ArrayList<WALFrame>();

	// for analyzing
	public InMemoryDatabase mdb;

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
				/* bytes 18/19 only distinguish legacy (rollback journal) from WAL; DELETE, TRUNCATE,
				 * PERSIST, MEMORY and OFF are not recorded in the database header */
				case 0: PRAGMA_journal_mode = "invalid value 0 (header manipulated?)";
					break;
				case 1: PRAGMA_journal_mode = readRollbackJournal
						? "legacy (rollback journal, -journal file present; DELETE/TRUNCATE/PERSIST/MEMORY not distinguishable from the header)"
						: "legacy (rollback journal; DELETE/TRUNCATE/PERSIST/MEMORY/OFF not distinguishable from the header)";
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


	public String[][] getPagesOverview(){

		if (pages == null)
			return null;

		// db may already be closed (e.g. updatePropertyPanel called after processDB finishes)
		if (db == null) {
			AppLog.debug("getPagesOverview: db already closed — returning cached page list without type bytes.");
			String[][] pagelist = new String[pages.length - 1][5];
			for (int i = 1; i < pages.length; i++) {
				if (pages[i] != null)
					pagelist[i-1] = new String[]{ String.valueOf(ps * (i - 1L)), "—", pages[i].getName(), pages[i].serialtypes.toString(), "" };
				else
					pagelist[i-1] = new String[]{ String.valueOf(ps * (i - 1L)), "—", "null", "null", "null" };
			}
			return pagelist;
		}

		String[][] pagelist = new String[pages.length-1][5];

		LinkedList<String> ll = new LinkedList<String>();

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
				AppLog.debug("Exception caught in getSchemaProperties");
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
		walpanel.initTransactionTable(getWALTransactions());
	}


	/**
	 * This is the main processing loop of the program.
	 *
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	@SuppressWarnings("unchecked")
	protected int processDB() throws InterruptedException, ExecutionException, IOException {

		bincache = new BLOBCache(this);

		allreadyvisit = ConcurrentHashMap.newKeySet();

		// Clear the shared table-descriptor list for this import run.
		// Since the GUI guard in open_db() ensures only one import runs at a time,
		// this is safe — no other Job is reading or writing this list concurrently.
		synchronized (RecoveryTask.tables) {
			RecoveryTask.tables.clear();
		}

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
		tblSig = new ConcurrentHashMap<String, String>();
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
		// Option B: keep SqliteElement in sync so decodeString() uses the correct charset
		SqliteElement.db_encoding = db_encoding;

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

		// feature request
		if (size > 0 && ps > 0 && size/ps != numberofpages)
		{
			info(" number of pages in header is not matching with real database size");

		}
		else
			info(" number of pages in header matches with real page number");


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


		/* we're looking for the key word <table> of the type column */
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
				 *  Accordingly, the header can have a length of at least 6 Bytes (5+1 for the header length byte).
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
				 *  has a length of 6 bytes including header length (06)
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

			/* we're looking for the key word <index> */
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

		HashSet<String> doubles = new HashSet<String>();

		boolean celltower_analysis = false;

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

			if (td.tblname.equals("response_records")){
				td.celltower_analysis = true;
			}

			td.printTableDefinition();

			int r = td.getRootOffset();

			String signature = td.getSignature();

			/* save component fingerprint for compare */
			if (signature != null && signature.length() > 0)
				tblSig.put(td.getSignature(), td.tblname);

			/* update treeview - skip this step in console modus */
			if (null != gui) {

				/* since table nodes will also be needed in journals and wal-files we have to add those nodes too */
				gui.add_table(this, td.tblname, td.columnnames, td.getColumntypes(), td.primarykeycolumns, td.boolcolumns, td.sqltypes, false, false,0).thenAccept(result -> {
					String path = result;
					guitab.put(td.tblname, path);
				});

				if (readWAL) {

					List<String> cnames = 	new ArrayList<>(td.columnnames);
					cnames.add(0,Global.col_commit);
					cnames.add(1,Global.col_dbpage);
					cnames.add(2,Global.col_walframe);
					cnames.add(3,Global.col_salt1);
					cnames.add(4,Global.col_salt2);

					List<String> ctypes = new ArrayList<>(td.serialtypes);

					gui.add_table(this, td.tblname, cnames, ctypes, td.primarykeycolumns, td.boolcolumns,  td.sqltypes,true, false,0).thenAccept(result -> {
						String walpath = result;
						guiwaltab.put(td.tblname, walpath);
						setWALPath(walpath.toString());
					});

				}

				else if (readRollbackJournal) {
					gui.add_table(this, td.tblname, td.columnnames, td.getColumntypes(),td.primarykeycolumns, td.boolcolumns, td.sqltypes, false, true,0).thenAccept(result -> {
						String rjpath = result;
						guiroltab.put(td.tblname, rjpath);
						setRollbackJournalPath(rjpath.toString());
					});

				}

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

				gui.add_table(this, id.idxname, id.columnnames, id.columntypes, id.boolcolumns, null, id.sqltypes, false, false,1).thenAccept(result -> {
					String path =  result;
					guitab.put(id.idxname, path);
				});

				if (readWAL) {


					List<String> cnames = new ArrayList<>(id.columnnames);
					cnames.add(0,"commit");
					cnames.add(1,"dbpage");
					cnames.add(2,"walframe");
					cnames.add(3,"salt1");
					cnames.add(4,"salt2");

					List<String> ctypes = new ArrayList<>(id.columntypes);

					gui.add_table(this, id.idxname, cnames, ctypes, id.boolcolumns, null, id.sqltypes, true, false,1).thenAccept(result -> {
						String walpath = result;
						guiwaltab.put(id.idxname, walpath);
						setWALPath(walpath);
					});

				}
				else if (readRollbackJournal) {

					gui.add_table(this, id.idxname, id.columnnames, id.columntypes, id.boolcolumns, id.sqltypes,null, false, true,1).thenAccept(result -> {
						String rjpath =result;
						guiroltab.put(id.idxname, rjpath);
						setRollbackJournalPath(rjpath);
					});

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
			//System.out.println(err);
		}


		int maxcol = 0;
		for(TableDescriptor t : headers){
			maxcol = Math.max(t.columnnames.size(),maxcol);
		}


		/*
		 * Sometimes, a record cannot be assigned to a component or index -> these records
		 * are assigned to the fqlite_freelist component.
		 */

		/*
		 * Register the sqlite_master descriptor for all schema pages - independent of the GUI.
		 * Previously this happened only when a GUI was attached; in headless runs page 1 stayed
		 * unassigned, so the live schema records ended up in the unassigned bucket "D" and were
		 * mistaken for deleted records by headless tools (e.g. benchmark harnesses).
		 */
		{
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
			List<String> sqltypes = new ArrayList<>();
			mnames.add("object"); mnames.add("obj name"); mnames.add("namespace");  mnames.add("root page");  mnames.add("Statement");
			sqltypes.add("STRING");sqltypes.add("STRING");sqltypes.add("STRING");sqltypes.add("INT");sqltypes.add("STRING");
			TableDescriptor tdmaster = new TableDescriptor("sqlite_master", "",mcol, mcol, mnames, null, null, hp, false);
			tdmaster.sqltypes = sqltypes;
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

			if (null != gui)
			gui.add_table(this, tdmaster.tblname, tdmaster.columnnames,
					tdmaster.getColumntypes(), tdmaster.boolcolumns, null, null, false, false,0).thenAccept(result -> {

				String pathmaster = result;
				guitab.put(tdmaster.tblname, pathmaster);
			});


			if (readWAL) {


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


				if (null != gui)
				gui.add_table(this, tdmaster.tblname, tdmaster.columnnames,
						tdmaster.getColumntypes(), tdmaster.boolcolumns ,null, tdmaster.sqltypes, true, false,0).thenAccept(result -> {
					String walpathmaster = result;
					guiwaltab.put(tdmaster.tblname, walpathmaster);
				});


			}
			else if (readRollbackJournal) {


				if (null != gui)
				gui.add_table(this, tdmaster.tblname, tdmaster.columnnames,
						tdmaster.getColumntypes(), tdmaster.boolcolumns ,null, tdmaster.sqltypes, false, true,0).thenAccept(result -> {
					String rjpathmaster = result;
					guiroltab.put(tdmaster.tblname, rjpathmaster);
				});

			}


		}


		if(emptydb){

			info("Omit analysis: database file is empty.");
			return 0;
		}


		///*******************************************************************/

		byte[] freepageno = new byte[4];
		buffer.position(36);
		buffer.get(freepageno);
		info("Total number of free list (trunk) pages " + Auxiliary.bytesToHex3(freepageno));
		ByteBuffer no = ByteBuffer.wrap(freepageno);
		fpnumber = no.getInt();
		//	System.out.println(" no " + fpnumber);

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

		if (head > 0 && (head == 1 || head > numberofpages)) {
			info("Possible manipulation: first freelist trunk page " + head + " is outside the database ("
					+ numberofpages + " pages) - free list ignored");
			head = 0;
		}

		if (head > 0) {
			AppLog.debug("first:: " + start + " 0hx " + Long.toHexString(start));

			long startfp = System.currentTimeMillis();
			info("Start free page recovery...");

			// seeking file pointer to the first free page entry

			/* create a new threadpool to analyse the freepages */
			ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Global.numberofThreads);

			/* A list can extend over several memory pages. */
			boolean morelistpages = false;

			runningTasks.set(0);

			int freepagesum = 0;
			allreadyvisit.add((int) head);

			do {
				/* reserve space for the first/next page of the free list */
				ByteBuffer fplist = ByteBuffer.allocate(ps);

				db.read(fplist, start);

				/*
				 * The trunk page itself is a freed page as well: only its first 8 + 4*n bytes are
				 * overwritten by the trunk header, the rest still holds the old records (e.g. after
				 * B-tree rebalancing or DROP TABLE). Previously only the leaf pages were carved.
				 */
				int trunkPage = (int) (start / ps) + 1;

				/*
				 * Sanity check of the trunk header: a real trunk page holds at most (ps-8)/4 leaf
				 * numbers. A larger count means the pointer leads to a page that is no trunk at all
				 * (e.g. a live table page) - do not treat its content as free.
				 */
				int trunkEntries = fplist.getInt(4);
				if (trunkEntries < 0 || trunkEntries > (ps - 8) / 4) {
					info("Possible manipulation: page " + trunkPage + " is referenced as freelist trunk but its header is invalid ("
							+ trunkEntries + " leaf pages) - free list traversal stopped");
					break;
				}

				if (trunkPage > 1 && !freelistpages.contains(trunkPage)) {
					freelistpages.add(trunkPage);
					freelistTrunks.add(trunkPage);
					RecoveryTask trunkTask = new RecoveryTask(new Auxiliary(this), this, start, trunkPage, ps, true);
					runningTasks.incrementAndGet();
					tasklist.add(trunkTask);
					executor.execute(trunkTask);
				}

				// next (possible) freepage list offset or 0xh00000000 + number of entries
				// example : 00 00 15 3C | 00 00 02 2B

				// now read the first 4 bytes to get the offset for the next free page list
				byte[] nextlistoffset = new byte[4];

				try {
					fplist.get(nextlistoffset);
				}
				catch(Exception err)
				{
					AppLog.error("Error while parsing free list.");
				}

				/*
				 * is there a further page - <code>nextlistoffset</code> has a value > 0 in this
				 * case
				 */
				if (!Auxiliary.bytesToHex3(nextlistoffset).equals(NO_MORE_ENTRIES)) {

					ByteBuffer of = ByteBuffer.wrap(nextlistoffset);
					int nfp = of.getInt();
					start = (nfp - 1L) * ps;
					morelistpages = true;
					if (nfp <= 1 || nfp > numberofpages) {
						// a trunk pointer outside the file can only result from corruption/manipulation
						info("Possible manipulation: freelist trunk page " + trunkPage + " points to invalid page " + nfp
								+ " (database has " + numberofpages + " pages) - free list traversal stopped");
						morelistpages = false;
					} else if (!allreadyvisit.add(nfp)) {
						// without this stop a cyclic trunk chain (e.g. trunk -> itself) loops forever
						info("Possible manipulation: cyclic freelist trunk chain (page " + trunkPage + " -> " + nfp
								+ ") - free list traversal stopped");
						morelistpages = false;
					}

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

				// (counter is reset once before the trunk loop - resetting it here while tasks of a
				//  previous trunk are still running drove it negative and the wait loop never ended)

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
					if (n == 1 || n > numberofpages || n < 0) {
						info("Possible manipulation: freelist trunk page " + trunkPage + " lists invalid leaf page " + n);
						continue;
					}
					if (freelistpages.contains(n)) {
						continue;		// already scheduled (e.g. listed twice by a manipulated trunk)
					}
					// determine offset for free page
					long offset = (n - 1L) * ps;

					//pages[n] = this.tdefault;
					/* remember already scanned pages in a list */
					freelistpages.add(n);
					RecoveryTask task1 = new RecoveryTask(new Auxiliary(this), this, offset, n, ps, true);
					/* add new task to executor queue */
					runningTasks.incrementAndGet();
					tasklist.add(task1);
					// Dispatch to the thread pool
					executor.execute(task1);
				}
				freepagesum += entries;

			} while (morelistpages); // while

			info("Number of pages in the free list: " + freelistpages.size());


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

		// attribute records of orphaned pages to the right table (structurally identical tables)
		reassignOrphanPages();

		// drop stale copies of live records found on freed pages (b-tree rebalancing)
		removeStaleCopiesOfLiveRecords();
		removeTruncatedDuplicates();
		markUpdatedRecords();

		if (gui != null) {

			info("Number of tables recovered: " + resultlist.size());

			// Post the update loop to the JavaFX thread.
			// All guitab.put() calls happen via thenAccept() on the JavaFX thread;
			// because the JavaFX event queue is FIFO, this runLater block is guaranteed
			// to execute AFTER all guitab.put() callbacks have completed.
			Platform.runLater(() -> {
				Enumeration<String> tableKeys = resultlist.keys();
				while (tableKeys.hasMoreElements()) {
					String tablename = tableKeys.nextElement();
					String path = guitab.get(tablename);
					if (null != path) {
						gui.update_table(path, resultlist.get(tablename), hexdumplist.get(tablename), false);
					} else {
						AppLog.debug("update_table: no guitab entry for '" + tablename + "' — table view will remain empty");
					}
				}
			});

		}


		// Cleanup: post a fence runLater so the JavaFX thread processes all
		// pending GUI updates *before* we close the db buffer on this thread.
		java.util.concurrent.CountDownLatch guiFence = new java.util.concurrent.CountDownLatch(1);
		Platform.runLater(() -> {
			tasklist.clear();
			tasklist = null;
			guiFence.countDown();
		});
		try {
			guiFence.await();
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
		}
		// Now safe to close: all JavaFX callbacks that touch db have run.
		try {
			db.close();
			db = null;   // signal to any late callers (e.g. updatePropertyPanel) that db is gone
		} catch (IOException e) {
			AppLog.error("Could not close database buffer: " + e.getMessage());
		}
		System.gc();
		AppLog.info("Database buffer closed, GC requested.");

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
			AppLog.error("Could not access wal-archive");
			return null;
		}



		boolean next = false;
		int framestart = 32; // this is the position, where the first frame should be

		int frame = 1;

		do {

			/* 24 Byte - with six 4-Byte big endian values */
			byte[] frameheader = new byte[24];
			if (framestart + 24 > wal.limit()) {
				AppLog.debug("readCheckpoint: WAL truncated at framestart=" + framestart + " — stopping.");
				break;
			}
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
			// Copy instead of slice(): page is stored in cptable and read
			// back much later, long after this window of the (LRU-evicting,
			// demand-paged) wal BigByteBuffer may have been evicted and its
			// native mapping unmapped — see readDBPageWithOffset() for the
			// full explanation of why a live slice() is unsafe here.
			byte[] pagebytes = new byte[ps];
			wal.position(framestart + 24);
			wal.get(pagebytes);
			ByteBuffer page = ByteBuffer.wrap(pagebytes);
			page.order(ByteOrder.BIG_ENDIAN);

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

	public static String db_info;

	/**
	 *  This method is used to create a InMemory DB for SQL Analyzes
	 */
	public InMemoryDatabase createInMemoryDB(String filename, String objectname, ExportType exp) {

		String dbname = objectname;

		if (exp == ExportType.SQLITEDB || exp == ExportType.ROLLBACKJOURNAL || exp == ExportType.WALARCHIVE) {

			ConcurrentHashMap<String, ObservableList<ObservableList<String>>> exportlist = switch (exp) {
				case ROLLBACKJOURNAL -> this.rol.resultlist;
				case SQLITEDB -> this.resultlist;
				case WALARCHIVE -> this.wal.resultlist;
				default -> null;
			};

			if (exp == ExportType.WALARCHIVE || exp == ExportType.ROLLBACKJOURNAL)
			{
				/* 1st step: get a database manager instance and create the database schema */
				mdb = DBManager.get(objectname);
				try {
					mdb.createDatabaseAndSchema(headers, objectname, exp == ExportType.WALARCHIVE);
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}
			else{
				/* 1st step: get a database manager instance and create the database schema */
				mdb = DBManager.get(filename);
				try {
					mdb.createDatabaseAndSchema(headers, objectname, exp == ExportType.WALARCHIVE);
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
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
					if (tblname.equals("sqlite_master") || tblname.startsWith("sqlite_") || tblname.startsWith("fqlite_"))
						continue;

					mdb.insertRows(bincache, objectname, tblname, headers, rows, exp == ExportType.WALARCHIVE);

				} catch (SQLException e) {
					AppLog.error("Error while inserting rows for " + tblname);
					e.printStackTrace();
					throw new RuntimeException(e);
				}

			}

			return mdb;

		}

		return null;
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
				exporter.createDatabaseAndSchema(headers, objectname,  exp == ExportType.WALARCHIVE);
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
					// skip the internal tables except the freelist

					if(tblname.equals("sqlite_master") || tblname.startsWith("sqlite_"))
						continue;
					exporter.insertRows(bincache,filename,tblname,headers,rows,exp == ExportType.WALARCHIVE);
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
	public boolean exportResults2File(File file, String objectname, FileType parenttype, ExportType exp){

		String dbname = null;

		try (final BufferedWriter writer = newBufferedWriter(file.toPath(),StandardCharsets.UTF_8, StandardOpenOption.CREATE))
		{

			/*
			 * CASE 1: Export a complete database file with all table information inside
			 */

			if(exp == ExportType.SQLITEDB || exp == ExportType.ROLLBACKJOURNAL || exp == ExportType.WALARCHIVE)
			{
				dbname = objectname;
				final int filetag = fileTag(exp);

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

					/* filter internal tables */
					if(tblname.equals("sqlite_master") || tblname.startsWith("sqlite_"))
						continue;

					ObservableList<ObservableList<String>> table = exportlist.get(tblname);

					if(Global.EXPORTTABLEHEADER) {
						String hd = prepareHeader(tblname, exp == ExportType.WALARCHIVE);
						if (null != hd) {
							writer.write(hd +"\n");
						}
					}

					/* only export regular tables no index or autoindex tables */
					boolean istable = false;
					for (TableDescriptor td: headers) {
						if (td.tblname.equals(tblname)) {
							istable = true;
						}
					}
					if (!istable) { continue; }

					/* as long as there are lines inside, go on */
					for (ObservableList<String> strings : table) {
						String out = prepareOutput(strings, dbname, file.getParent(), filetag) + "\n";
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
				final int filetag = parenttype == FileType.WriteAheadLog ? Global.WAL_ARCHIVE_FILE
						: parenttype == FileType.RollbackJournalLog ? Global.ROLLBACK_JOURNAL_FILE : Global.REGULAR_DB_FILE;

				if(Global.EXPORTTABLEHEADER)
					writer.write(prepareHeader(objectname,exp == ExportType.WALARCHIVE) + "\n");

				/* as long as there are lines inside, go on */
				while(iter.hasNext()){
					String out = prepareOutput(iter.next(),dbname,file.getParent(), filetag) + "\n";
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


	public String prepareHeader(String tablename, boolean isWAL) {

		String token = Global.CSV_SEPARATOR;

		if(token.equals("[TAB]"))
			token = "\t";

		String[] head = getColumnNamesForTable(tablename);
		if ((head != null && head.length > 0)) {
			String line = Global.col_source + token + Global.col_cells + token;
			if (isWAL) {
				line += Global.col_tblname + token + Global.col_pll + token + Global.col_rowid + token + Global.col_status + token + Global.col_offset + token + Global.col_commit + token + Global.col_dbpage + token + Global.col_walframe + token + Global.col_salt1 + token + Global.col_salt2 + token;
			}
			else{
				line += Global.col_tblname + token + Global.col_pll + token + Global.col_rowid + token + Global.col_status + token + Global.col_offset + token;
			}
			if (tablename.equals("fqlite_freelist")){
				// add actual column names
				for (int i = 0; i < 42; i++) {
					line += "col" + i + token;
				}
			}
			else {
				// add actual column names
				for (String c : head) {
					line += c + token;
				}
			}
			return line;
		}

		return null;
	}

	public String[] getExentedColumnNamesForTable (String tablename, ExportType exp){


		for (TableDescriptor td: headers) {

			if (td.tblname.equals(tablename)) {

				List<String> names = new ArrayList<>();

				if(exp == ExportType.WALARCHIVE){
					names.addFirst(Global.col_salt2);
					names.addFirst(Global.col_salt1);
					names.addFirst(Global.col_walframe);
					names.addFirst(Global.col_dbpage);
					names.addFirst(Global.col_commit);
				}

				names.addFirst(Global.col_offset);
				names.addFirst(Global.col_status);
				names.addFirst(Global.col_rowid);
				names.addFirst(Global.col_pll);
				//names.addFirst(Global.col_tblname);
				names.addFirst(Global.col_no);



				names.addAll(td.columnnames);
				return names.toArray(new String[0]);

			}
		}

		return null;
	}

	public String[] getColumnNamesForTable (String tablename){

		for (TableDescriptor td: headers) {
			if (td.tblname.equals(tablename)) {

				return td.columnnames.toArray(new String[0]);

			}
		}

		return null;
	}

	private String createMetadataSection(Map<String, String> metaData) {
		StringBuilder sb = new StringBuilder();

		sb.append("<div class='metadata-section'>\n");
		sb.append("<h2>File Information</h2>\n");
		sb.append("<div class='metadata-grid'>\n");

		// Erste Spalte
		sb.append("<div class='metadata-column'>\n");
		sb.append(createMetadataField("File size", metaData.get("size")));
		sb.append(createMetadataField("Path", metaData.get("path")));
		sb.append(createMetadataField("Creation Date", metaData.get("creationTime")));
		sb.append(createMetadataField("Last Access", metaData.get("lastAccess")));
		sb.append(createMetadataField("Last Modified", metaData.get("lastModified")));
		sb.append("</div>\n");

		// Zweite Spalte
		sb.append("<div class='metadata-column'>\n");
		sb.append(createMetadataField("SHA-1", metaData.get("sha1"), true));
		sb.append(createMetadataField("SHA-256", metaData.get("sha256"), true));
		sb.append(createMetadataField("MD5", metaData.get("md5"), true));
		sb.append(createMetadataField("Investigator", metaData.get("investigator")));
		sb.append(createMetadataField("Tool-Version", metaData.get("toolVersion")));
		sb.append("</div>\n");

		sb.append("</div>\n");
		sb.append("</div>\n");

		return sb.toString();
	}

	private String createMetadataField(String label, String value) {
		return createMetadataField(label, value, false);
	}

	private String createMetadataField(String label, String value, boolean monospace) {
		StringBuilder sb = new StringBuilder();
		String displayValue = (value != null && !value.trim().isEmpty()) ? escapeHtml(value) : "<span class='no-data'>Nicht verfügbar</span>";
		String cssClass = monospace ? " class='monospace'" : "";

		sb.append("<div class='metadata-row'>\n");
		sb.append("<div class='metadata-label'>").append(escapeHtml(label)).append(":</div>\n");
		sb.append("<div class='metadata-value'").append(cssClass).append(">").append(displayValue).append("</div>\n");
		sb.append("</div>\n");

		return sb.toString();
	}

	/**
	 * This method actually creates a CASE ontology file (JSON) with the database content
	 * @param dbname
	 * @param outputPath
	 * @param exp
	 * @throws IOException
	 */
	public void exportToCASE(String dbname, String outputPath, String xpath, ExportType exp, boolean isTable, String tname) throws IOException {

		CASEExport.exportToCase(this, dbname, outputPath, xpath, exp, isTable, tname);

	}


	/**
	 * This method actually creates a html file with the database content
	 * @param dbname
	 * @param outputPath
	 * @param exp
	 * @throws IOException
	 */
	public void exportToHtml(String dbname, String outputPath, String xpath, ExportType exp, boolean isTable, String tname) throws IOException {

		try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath, StandardCharsets.UTF_8))) {

			ConcurrentHashMap<String, ObservableList<ObservableList<String>>> exportlist = switch (exp) {
				case ROLLBACKJOURNAL -> this.rol.resultlist;
				case SQLITEDB -> this.resultlist;
				case WALARCHIVE -> this.wal.resultlist;
				default -> null;
			};

			Path p = Paths.get(path);
			Map<String,Object> attributes = Files.readAttributes(p,"*", LinkOption.NOFOLLOW_LINKS);

			// collect metadata
			Map<String, String> metaData = new HashMap<>();
			metaData.put("size", attributes.get("size").toString() + " bytes");
			if (exp == ExportType.SQLITEDB)
				metaData.put("path", path + " (database file)");
			if (exp == ExportType.WALARCHIVE) {
				int id = walpath.lastIndexOf("/");
				String wpath = walpath.substring(0, id);
				metaData.put("path", wpath + " (write-ahead log)");
			}
			if (exp == ExportType.ROLLBACKJOURNAL) {
				int id = rollbackjournalpath.lastIndexOf("/");
				String rpath = rollbackjournalpath.substring(0, id);
				metaData.put("path", rpath + " (journal file)");
			}
			metaData.put("creationTime", attributes.get("creationTime").toString());
			metaData.put("lastAccess", attributes.get("lastAccessTime").toString());
			metaData.put("lastModified", attributes.get("lastModifiedTime").toString());
			metaData.put("sha1", new DigestUtils(org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_1).digestAsHex(new File(path)));
			metaData.put("sha256", new DigestUtils(org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_256).digestAsHex(new File(path)));
			metaData.put("md5", new DigestUtils(org.apache.commons.codec.digest.MessageDigestAlgorithms.MD5).digestAsHex(new File(path)));
			metaData.put("investigator", System.getProperty("user.name"));
			metaData.put("toolVersion", "FQlite v" + Global.FQLITE_VERSION);

			Enumeration<String> tbls = exportlist.keys();
			List<String> rendered = new ArrayList<>();
			final int filetag = fileTag(exp);
			final String sourceName = sourceFileName(filetag);

			writer.write(createHtmlHeader());

			writer.write("<body>\n");
			writer.write("<div class='container'>\n");
			writer.write("<h1>database export: " + escapeHtml(dbname) + "</h1>\n");

			// meta data division
			writer.write(createMetadataSection(metaData));

			while (tbls.hasMoreElements()) {
				String tblname = tbls.nextElement();
				if(isTable && !tname.equals(tblname))
					continue;

				ObservableList<ObservableList<String>> table = exportlist.get(tblname);

				if (tblname.startsWith("sqlite_")) {continue;}
				if (table == null || table.isEmpty()) {
					AppLog.debug("HTML export: skip '" + tblname + "' (no rows)");
					continue;
				}

				// skip index tables
				String[] h = getExentedColumnNamesForTable(tblname,exp);
				if (h == null && !tblname.equals("fqlite_freelist")) {
					AppLog.info("HTML export: skip '" + tblname + "' (no TableDescriptor in headers, known: "
							+ headers.stream().map(t -> "'" + t.tblname + "'").toList() + ")");
					continue;
				}
				rendered.add(tblname);

				writer.write("<div class='table-section'>\n");
				writer.write("<h2>" + escapeHtml(tblname) + "</h2>\n");

				writer.write("<div class='filter-row'>\n");
				writer.write("<input type='text' class='global-filter-input' ");
				writer.write("placeholder='Search in complete table...' ");
				writer.write("onkeyup=\"applyFilters('" + escapeHtml(jsEscape(tblname)) + "')\">\n");
				writer.write("</div>\n");
				writer.write("<div class='table-wrapper'>\n");
				writer.write("<table id='table-" + escapeHtml(tblname) + "' class='data-table'>\n");

				if (!table.isEmpty() && h!=null && h.length>0) {
					//ObservableList<String> headerRow = table.get(0);

					// Spaltenüberschriften mit Sortierung
					writer.write("<thead>\n");
					writer.write("<tr class='header-row'>\n");
					String[] extra = {Global.col_source, Global.col_cells};
					for (int colIndex = 0; colIndex < extra.length; colIndex++) {
						writer.write("<th onclick=\"sortTable('" + escapeHtml(jsEscape(tblname)) + "', " + colIndex + ")\">");
						writer.write(escapeHtml(extra[colIndex]));
						writer.write(" <span class='sort-indicator'></span>");
						writer.write("</th>\n");
					}
					for (int colIndex = 0; colIndex < h.length; colIndex++) {
						String header = h[colIndex];
						writer.write("<th onclick=\"sortTable('" + escapeHtml(jsEscape(tblname)) + "', " + (colIndex + 2) + ")\">");
						writer.write(escapeHtml(header));
						writer.write(" <span class='sort-indicator'></span>");
						writer.write("</th>\n");
					}
					writer.write("</tr>\n");

					writer.write("<tr class='filter-row'>\n");
					for (int colIndex = 0; colIndex < h.length + 2; colIndex++) {
						writer.write("<th>");
						writer.write("<input type='text' class='column-filter' ");
						writer.write("data-column='" + colIndex + "' ");
						writer.write("placeholder='Filter...' ");
						writer.write("onkeyup=\"applyFilters('" + escapeHtml(jsEscape(tblname)) + "')\">");
						writer.write("</th>\n");
					}
					writer.write("</tr>\n");
					writer.write("</thead>\n");
				}

				writer.write("<tbody>\n");
				//int startIndex = Global.EXPORTTABLEHEADER ? 1 : 0;
				int startIndex = 0;


				for (int i = startIndex; i < table.size(); i++) {
					ObservableList<String> row = table.get(i);
					String offset = row.get(5);
					writer.write("<tr>\n");
					writer.write("<td>" + escapeHtml(sourceName) + "</td>\n");
					writer.write("<td>" + escapeHtml(cellsOf(filetag, row)) + "</td>\n");
					int cl = 0;
					for (String cell : row) {
						if (cell == null) cell = "";
						if(cl==1) {
							cl++;
							continue;
						}
						/* export binary to seperate files - if enabled in the configuration */
						if(cell.contains("[BLOB")){

							if(Global.EXPORT_MODE == Global.EXPORT_MODES.TOSEPARATEFILES)
							{
								cell = exportBLOB(dbname,offset,cell,xpath);

								if(cell.endsWith(".png") || cell.endsWith(".jpg") || cell.endsWith(".gif") || cell.endsWith(".jpeg") || cell.endsWith(".bmp")) {
									cell = "<a href=" + cell + ">"
										   + "<img src=" + cell + " width=\"40\" height=\"40\" alt=\"preview\"></a>";
								}
								else {
									cell = "<a href=" + cell + ">" + cell + "</a>";
								}
								writer.write("<td>" + cell + "</td>\n");
							}
							else{
								writer.write("<td> [BLOB] </td>\n");
							}
						}
						else
							writer.write("<td>" + escapeHtml(cell) + "</td>\n");
						cl++;
					}
					writer.write("</tr>\n");
				}

				writer.write("</tbody>\n");
				writer.write("</table>\n");
				writer.write("</div>\n");
				writer.write("<div class='pagination-container'>\n");
				writer.write("<div class='pagination-info'>\n");
				writer.write("<span class='showing-info' id='showing-" + escapeHtml(tblname) + "'></span>\n");
				writer.write("</div>\n");
				writer.write("<div class='pagination-controls'>\n");
				writer.write("<label>lines per page: ");
				writer.write("<select class='rows-per-page' onchange=\"changeRowsPerPage('" + escapeHtml(jsEscape(tblname)) + "', this.value)\">\n");
				writer.write("<option value='10'>10</option>\n");
				writer.write("<option value='25' selected>25</option>\n");
				writer.write("<option value='50'>50</option>\n");
				writer.write("<option value='100'>100</option>\n");
				writer.write("<option value='all'>all</option>\n");
				writer.write("</select>\n");
				writer.write("</label>\n");
				writer.write("<div class='pagination-buttons' id='pagination-" + escapeHtml(tblname) + "'></div>\n");
				writer.write("</div>\n");
				writer.write("</div>\n");

				writer.write("</div>\n\n");
			}

			writer.write("</div>\n");
			writer.write("<script>\n");
			writer.write("document.addEventListener('DOMContentLoaded', function() {\n");

			// only initialise tables that were actually written – otherwise
			// getElementById() returns null and the TypeError aborts the init of all following tables
			for (String tblname : rendered) {
				writer.write("    initTable('" + jsEscape(tblname) + "');\n");
			}

			writer.write("});\n");
			writer.write("</script>\n");

			writer.write("</body>\n</html>");

		} // end try-with-resources (writer auto-closed)
	}


	private String createHtmlHeader() {
		return "<!DOCTYPE html>\n" +
			   "<html lang='de'>\n" +
			   "<head>\n" +
			   "<meta charset='UTF-8'>\n" +
			   "<meta name='viewport' content='width=device-width, initial-scale=1.0'>\n" +
			   "<title>Datenbank Export</title>\n" +
			   "<style>\n" +
			   "* {\n" +
			   "    box-sizing: border-box;\n" +
			   "}\n" +
			   "body {\n" +
			   "    font-family: Arial, sans-serif;\n" +
			   "    margin: 0;\n" +
			   "    padding: 20px;\n" +
			   "    background-color: #f5f5f5;\n" +
			   "}\n" +
			   ".container {\n" +
			   "    max-width: 1400px;\n" +
			   "    margin: 0 auto;\n" +
			   "    background: white;\n" +
			   "    padding: 20px;\n" +
			   "    border-radius: 8px;\n" +
			   "    box-shadow: 0 2px 4px rgba(0,0,0,0.1);\n" +
			   "}\n" +
			   "h1 {\n" +
			   "    color: #333;\n" +
			   "    border-bottom: 3px solid #4CAF50;\n" +
			   "    padding-bottom: 10px;\n" +
			   "    margin-top: 0;\n" +
			   "}\n" +
			   "h2 {\n" +
			   "    color: #555;\n" +
			   "    margin-top: 30px;\n" +
			   "    margin-bottom: 15px;\n" +
			   "}\n" +
			   ".metadata-section {\n" +
			   "    background-color: #f9f9f9;\n" +
			   "    border: 1px solid #e0e0e0;\n" +
			   "    border-radius: 6px;\n" +
			   "    padding: 20px;\n" +
			   "    margin-bottom: 30px;\n" +
			   "}\n" +
			   ".metadata-section h2 {\n" +
			   "    margin-top: 0;\n" +
			   "    color: #333;\n" +
			   "    font-size: 1.3em;\n" +
			   "    border-bottom: 2px solid #4CAF50;\n" +
			   "    padding-bottom: 8px;\n" +
			   "}\n" +
			   ".metadata-grid {\n" +
			   "    display: grid;\n" +
			   "    grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));\n" +
			   "    gap: 20px;\n" +
			   "}\n" +
			   ".metadata-column {\n" +
			   "    display: flex;\n" +
			   "    flex-direction: column;\n" +
			   "    gap: 12px;\n" +
			   "}\n" +
			   ".metadata-row {\n" +
			   "    display: grid;\n" +
			   "    grid-template-columns: 150px 1fr;\n" +
			   "    gap: 10px;\n" +
			   "    padding: 8px 0;\n" +
			   "    border-bottom: 1px solid #e8e8e8;\n" +
			   "}\n" +
			   ".metadata-row:last-child {\n" +
			   "    border-bottom: none;\n" +
			   "}\n" +
			   ".metadata-label {\n" +
			   "    font-weight: bold;\n" +
			   "    color: #555;\n" +
			   "    align-self: start;\n" +
			   "}\n" +
			   ".metadata-value {\n" +
			   "    color: #333;\n" +
			   "    word-break: break-all;\n" +
			   "}\n" +
			   ".metadata-value.monospace {\n" +
			   "    font-family: 'Courier New', monospace;\n" +
			   "    font-size: 0.9em;\n" +
			   "    background-color: #f5f5f5;\n" +
			   "    padding: 4px 8px;\n" +
			   "    border-radius: 3px;\n" +
			   "}\n" +
			   ".no-data {\n" +
			   "    color: #999;\n" +
			   "    font-style: italic;\n" +
			   "}\n" +
			   ".table-section {\n" +
			   "    margin-bottom: 40px;\n" +
			   "}\n" +
			   ".filter-row {\n" +
			   "    margin-bottom: 15px;\n" +
			   "}\n" +
			   ".global-filter-input {\n" +
			   "    width: 100%;\n" +
			   "    padding: 10px;\n" +
			   "    border: 2px solid #ddd;\n" +
			   "    border-radius: 4px;\n" +
			   "    font-size: 14px;\n" +
			   "}\n" +
			   ".global-filter-input:focus {\n" +
			   "    outline: none;\n" +
			   "    border-color: #4CAF50;\n" +
			   "}\n" +
			   ".table-wrapper {\n" +
			   "    overflow-x: auto;\n" +
			   "    margin-bottom: 15px;\n" +
			   "}\n" +
			   ".data-table {\n" +
			   "    width: 100%;\n" +
			   "    border-collapse: collapse;\n" +
			   "    box-shadow: 0 1px 3px rgba(0,0,0,0.1);\n" +
			   "}\n" +
			   ".data-table thead {\n" +
			   "    background-color: #4CAF50;\n" +
			   "    color: white;\n" +
			   "    position: sticky;\n" +
			   "    top: 0;\n" +
			   "    z-index: 10;\n" +
			   "}\n" +
			   ".data-table thead .header-row th {\n" +
			   "    cursor: pointer;\n" +
			   "    user-select: none;\n" +
			   "    position: relative;\n" +
			   "    padding-right: 25px;\n" +
			   "}\n" +
			   ".data-table thead .header-row th:hover {\n" +
			   "    background-color: #45a049;\n" +
			   "}\n" +
			   ".sort-indicator {\n" +
			   "    position: absolute;\n" +
			   "    right: 8px;\n" +
			   "    top: 50%;\n" +
			   "    transform: translateY(-50%);\n" +
			   "}\n" +
			   ".sort-indicator::after {\n" +
			   "    content: '⇅';\n" +
			   "    opacity: 0.5;\n" +
			   "}\n" +
			   ".sort-indicator.asc::after {\n" +
			   "    content: '▲';\n" +
			   "    opacity: 1;\n" +
			   "}\n" +
			   ".sort-indicator.desc::after {\n" +
			   "    content: '▼';\n" +
			   "    opacity: 1;\n" +
			   "}\n" +
			   ".data-table th,\n" +
			   ".data-table td {\n" +
			   "    padding: 12px;\n" +
			   "    text-align: left;\n" +
			   "    border-bottom: 1px solid #ddd;\n" +
			   "}\n" +
			   ".data-table .filter-row {\n" +
			   "    background-color: #f8f8f8;\n" +
			   "}\n" +
			   ".column-filter {\n" +
			   "    width: 100%;\n" +
			   "    padding: 6px;\n" +
			   "    border: 1px solid #ccc;\n" +
			   "    border-radius: 3px;\n" +
			   "    font-size: 12px;\n" +
			   "}\n" +
			   ".column-filter:focus {\n" +
			   "    outline: none;\n" +
			   "    border-color: #4CAF50;\n" +
			   "}\n" +
			   ".data-table tbody tr:hover {\n" +
			   "    background-color: #f5f5f5;\n" +
			   "}\n" +
			   ".data-table tbody tr.hidden {\n" +
			   "    display: none;\n" +
			   "}\n" +
			   ".pagination-container {\n" +
			   "    display: flex;\n" +
			   "    justify-content: space-between;\n" +
			   "    align-items: center;\n" +
			   "    padding: 15px 0;\n" +
			   "    flex-wrap: wrap;\n" +
			   "    gap: 15px;\n" +
			   "}\n" +
			   ".pagination-info {\n" +
			   "    color: #666;\n" +
			   "    font-size: 14px;\n" +
			   "}\n" +
			   ".pagination-controls {\n" +
			   "    display: flex;\n" +
			   "    align-items: center;\n" +
			   "    gap: 15px;\n" +
			   "}\n" +
			   ".rows-per-page {\n" +
			   "    padding: 6px 10px;\n" +
			   "    border: 1px solid #ddd;\n" +
			   "    border-radius: 4px;\n" +
			   "    font-size: 14px;\n" +
			   "}\n" +
			   ".pagination-buttons {\n" +
			   "    display: flex;\n" +
			   "    gap: 5px;\n" +
			   "}\n" +
			   ".page-btn {\n" +
			   "    padding: 6px 12px;\n" +
			   "    border: 1px solid #ddd;\n" +
			   "    background-color: white;\n" +
			   "    cursor: pointer;\n" +
			   "    border-radius: 4px;\n" +
			   "    font-size: 14px;\n" +
			   "    transition: all 0.2s;\n" +
			   "}\n" +
			   ".page-btn:hover:not(:disabled) {\n" +
			   "    background-color: #4CAF50;\n" +
			   "    color: white;\n" +
			   "    border-color: #4CAF50;\n" +
			   "}\n" +
			   ".page-btn.active {\n" +
			   "    background-color: #4CAF50;\n" +
			   "    color: white;\n" +
			   "    border-color: #4CAF50;\n" +
			   "}\n" +
			   ".page-btn:disabled {\n" +
			   "    opacity: 0.5;\n" +
			   "    cursor: not-allowed;\n" +
			   "}\n" +
			   "@media (max-width: 768px) {\n" +
			   "    .metadata-grid {\n" +
			   "        grid-template-columns: 1fr;\n" +
			   "    }\n" +
			   "    .metadata-row {\n" +
			   "        grid-template-columns: 120px 1fr;\n" +
			   "    }\n" +
			   "    .pagination-container {\n" +
			   "        flex-direction: column;\n" +
			   "    }\n" +
			   "}\n" +
			   "</style>\n" +
			   "<script>\n" +
			   "// [JavaScript-Code bleibt unverändert wie vorher]\n" +
			   "const tableStates = {};\n" +
			   "\n" +
			   "function initTable(tableName) {\n" +
			   "    const table = document.getElementById('table-' + tableName);\n" +
			   "    const tbody = table.querySelector('tbody');\n" +
			   "    const rows = Array.from(tbody.querySelectorAll('tr'));\n" +
			   "    \n" +
			   "    tableStates[tableName] = {\n" +
			   "        allRows: rows,\n" +
			   "        filteredRows: rows,\n" +
			   "        currentPage: 1,\n" +
			   "        rowsPerPage: 25,\n" +
			   "        sortColumn: -1,\n" +
			   "        sortDirection: 'asc'\n" +
			   "    };\n" +
			   "    \n" +
			   "    updatePagination(tableName);\n" +
			   "}\n" +
			   "\n" +
			   "function applyFilters(tableName) {\n" +
			   "    const table = document.getElementById('table-' + tableName);\n" +
			   "    const state = tableStates[tableName];\n" +
			   "    \n" +
			   "    const globalFilter = table.closest('.table-section').querySelector('.global-filter-input').value.toLowerCase();\n" +
			   "    const columnFilters = Array.from(table.querySelectorAll('.column-filter')).map(input => ({\n" +
			   "        column: parseInt(input.dataset.column),\n" +
			   "        value: input.value.toLowerCase()\n" +
			   "    }));\n" +
			   "    \n" +
			   "    state.filteredRows = state.allRows.filter(row => {\n" +
			   "        const cells = Array.from(row.querySelectorAll('td'));\n" +
			   "        \n" +
			   "        if (globalFilter) {\n" +
			   "            const rowText = cells.map(cell => cell.textContent).join(' ').toLowerCase();\n" +
			   "            if (rowText.indexOf(globalFilter) === -1) return false;\n" +
			   "        }\n" +
			   "        \n" +
			   "        for (const filter of columnFilters) {\n" +
			   "            if (filter.value && cells[filter.column]) {\n" +
			   "                const cellText = cells[filter.column].textContent.toLowerCase();\n" +
			   "                if (cellText.indexOf(filter.value) === -1) return false;\n" +
			   "            }\n" +
			   "        }\n" +
			   "        \n" +
			   "        return true;\n" +
			   "    });\n" +
			   "    \n" +
			   "    state.currentPage = 1;\n" +
			   "    updatePagination(tableName);\n" +
			   "}\n" +
			   "\n" +
			   "function sortTable(tableName, columnIndex) {\n" +
			   "    const state = tableStates[tableName];\n" +
			   "    const table = document.getElementById('table-' + tableName);\n" +
			   "    \n" +
			   "    if (state.sortColumn === columnIndex) {\n" +
			   "        state.sortDirection = state.sortDirection === 'asc' ? 'desc' : 'asc';\n" +
			   "    } else {\n" +
			   "        state.sortColumn = columnIndex;\n" +
			   "        state.sortDirection = 'asc';\n" +
			   "    }\n" +
			   "    \n" +
			   "    table.querySelectorAll('.sort-indicator').forEach(indicator => {\n" +
			   "        indicator.className = 'sort-indicator';\n" +
			   "    });\n" +
			   "    \n" +
			   "    const indicators = table.querySelectorAll('.sort-indicator');\n" +
			   "    if (indicators[columnIndex]) {\n" +
			   "        indicators[columnIndex].classList.add(state.sortDirection);\n" +
			   "    }\n" +
			   "    \n" +
			   "    state.filteredRows.sort((a, b) => {\n" +
			   "        const cellA = a.querySelectorAll('td')[columnIndex].textContent.trim();\n" +
			   "        const cellB = b.querySelectorAll('td')[columnIndex].textContent.trim();\n" +
			   "        \n" +
			   "        const numA = parseFloat(cellA);\n" +
			   "        const numB = parseFloat(cellB);\n" +
			   "        \n" +
			   "        if (!isNaN(numA) && !isNaN(numB)) {\n" +
			   "            return state.sortDirection === 'asc' ? numA - numB : numB - numA;\n" +
			   "        }\n" +
			   "        \n" +
			   "        const comparison = cellA.localeCompare(cellB, 'de');\n" +
			   "        return state.sortDirection === 'asc' ? comparison : -comparison;\n" +
			   "    });\n" +
			   "    \n" +
			   "    updatePagination(tableName);\n" +
			   "}\n" +
			   "\n" +
			   "function changeRowsPerPage(tableName, value) {\n" +
			   "    const state = tableStates[tableName];\n" +
			   "    state.rowsPerPage = value === 'all' ? state.filteredRows.length : parseInt(value);\n" +
			   "    state.currentPage = 1;\n" +
			   "    updatePagination(tableName);\n" +
			   "}\n" +
			   "\n" +
			   "function changePage(tableName, page) {\n" +
			   "    const state = tableStates[tableName];\n" +
			   "    state.currentPage = page;\n" +
			   "    updatePagination(tableName);\n" +
			   "}\n" +
			   "\n" +
			   "function updatePagination(tableName) {\n" +
			   "    const state = tableStates[tableName];\n" +
			   "    const totalRows = state.filteredRows.length;\n" +
			   "    const totalPages = Math.ceil(totalRows / state.rowsPerPage);\n" +
			   "    \n" +
			   "    state.allRows.forEach(row => row.classList.add('hidden'));\n" +
			   "    \n" +
			   "    const startIndex = (state.currentPage - 1) * state.rowsPerPage;\n" +
			   "    const endIndex = Math.min(startIndex + state.rowsPerPage, totalRows);\n" +
			   "    \n" +
			   "    for (let i = startIndex; i < endIndex; i++) {\n" +
			   "        state.filteredRows[i].classList.remove('hidden');\n" +
			   "    }\n" +
			   "    \n" +
			   "    const showingInfo = document.getElementById('showing-' + tableName);\n" +
			   "    if (totalRows === 0) {\n" +
			   "        showingInfo.textContent = 'Keine Ergebnisse';\n" +
			   "    } else {\n" +
			   "        showingInfo.textContent = `Zeige ${startIndex + 1}-${endIndex} von ${totalRows} Zeilen`;\n" +
			   "    }\n" +
			   "    \n" +
			   "    const paginationDiv = document.getElementById('pagination-' + tableName);\n" +
			   "    paginationDiv.innerHTML = '';\n" +
			   "    \n" +
			   "    if (totalPages <= 1) return;\n" +
			   "    \n" +
			   "    const prevBtn = document.createElement('button');\n" +
			   "    prevBtn.className = 'page-btn';\n" +
			   "    prevBtn.textContent = '«';\n" +
			   "    prevBtn.disabled = state.currentPage === 1;\n" +
			   "    prevBtn.onclick = () => changePage(tableName, state.currentPage - 1);\n" +
			   "    paginationDiv.appendChild(prevBtn);\n" +
			   "    \n" +
			   "    const maxButtons = 5;\n" +
			   "    let startPage = Math.max(1, state.currentPage - Math.floor(maxButtons / 2));\n" +
			   "    let endPage = Math.min(totalPages, startPage + maxButtons - 1);\n" +
			   "    \n" +
			   "    if (endPage - startPage < maxButtons - 1) {\n" +
			   "        startPage = Math.max(1, endPage - maxButtons + 1);\n" +
			   "    }\n" +
			   "    \n" +
			   "    if (startPage > 1) {\n" +
			   "        const firstBtn = document.createElement('button');\n" +
			   "        firstBtn.className = 'page-btn';\n" +
			   "        firstBtn.textContent = '1';\n" +
			   "        firstBtn.onclick = () => changePage(tableName, 1);\n" +
			   "        paginationDiv.appendChild(firstBtn);\n" +
			   "        \n" +
			   "        if (startPage > 2) {\n" +
			   "            const dots = document.createElement('span');\n" +
			   "            dots.textContent = '...';\n" +
			   "            dots.style.padding = '6px';\n" +
			   "            paginationDiv.appendChild(dots);\n" +
			   "        }\n" +
			   "    }\n" +
			   "    \n" +
			   "    for (let i = startPage; i <= endPage; i++) {\n" +
			   "        const pageBtn = document.createElement('button');\n" +
			   "        pageBtn.className = 'page-btn' + (i === state.currentPage ? ' active' : '');\n" +
			   "        pageBtn.textContent = i;\n" +
			   "        pageBtn.onclick = () => changePage(tableName, i);\n" +
			   "        paginationDiv.appendChild(pageBtn);\n" +
			   "    }\n" +
			   "    \n" +
			   "    if (endPage < totalPages) {\n" +
			   "        if (endPage < totalPages - 1) {\n" +
			   "            const dots = document.createElement('span');\n" +
			   "            dots.textContent = '...';\n" +
			   "            dots.style.padding = '6px';\n" +
			   "            paginationDiv.appendChild(dots);\n" +
			   "        }\n" +
			   "        \n" +
			   "        const lastBtn = document.createElement('button');\n" +
			   "        lastBtn.className = 'page-btn';\n" +
			   "        lastBtn.textContent = totalPages;\n" +
			   "        lastBtn.onclick = () => changePage(tableName, totalPages);\n" +
			   "        paginationDiv.appendChild(lastBtn);\n" +
			   "    }\n" +
			   "    \n" +
			   "    const nextBtn = document.createElement('button');\n" +
			   "    nextBtn.className = 'page-btn';\n" +
			   "    nextBtn.textContent = '»';\n" +
			   "    nextBtn.disabled = state.currentPage === totalPages;\n" +
			   "    nextBtn.onclick = () => changePage(tableName, state.currentPage + 1);\n" +
			   "    paginationDiv.appendChild(nextBtn);\n" +
			   "}\n" +
			   "</script>\n" +
			   "</head>\n";
	}

	/** Escapes a value for use inside a single-quoted JS string (wrap with escapeHtml() inside attributes). */
	private String jsEscape(String text) {
		if (text == null) return "";
		return text.replace("\\", "\\\\").replace("'", "\\'").replace("</", "<\\/");
	}

	private String escapeHtml(String text) {
		if (text == null) return "";
		return text.replace("&", "&amp;")
				.replace("<", "&lt;")
				.replace(">", "&gt;")
				.replace("\"", "&quot;")
				.replace("'", "&#39;");
	}

	private String prepareOutput(ObservableList<String> list, String dbname, String exportfolder, int filetag) throws IOException{


		String token = Global.CSV_SEPARATOR;

		if(token.equals("[TAB]"))
			token = "\t";

		/* get String token (cell values) */
		String[] line = list.toArray(new String[0]);

		String output = "";
		String offset = null;

		int counter = 0;

		/* skip schema definition lines of table sqlite_master */
		if(line[0].startsWith("sqlite_master"))
			return null;

		/* source file and value layout first - independent of the number of columns of the row */
		output += sourceFileName(filetag) + token + cellsOf(filetag, list) + token;

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
			if(counter == 5)
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
					output += "[BLOB]";
					counter++;
					continue;
				}
				else if(Global.EXPORT_MODE == Global.EXPORT_MODES.TOSEPARATEFILES)
				{
					String fname = exportBLOB(dbname,offset,c,exportfolder);
					output += fname;
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

	/// This method is used to export a BLOB-field as spearate file
	/// to the file system.
	public String exportBLOB(String dbname, String offset, String c, String exportfolder){

		/* BLOB-value found? */
		if(c.length()>7) {

			int from = c.indexOf("-");
			int to = c.indexOf("]");

			int number = Integer.parseInt(c.substring(from+1, to));

			long hash = -1;
			if (offset.length() > 2) {
				try {
					hash = Long.parseLong(offset);
				} catch (NumberFormatException e) {
					AppLog.debug("> Number format exception: compute hash failed");
				}
			} else {
				hash = 0;
			}


			if (from > 0 && to > 0) {

				String base  = hash + "-" + number;
				String data = bincache.getHexString(path);

				BLOBElement e = bincache.get(base);

				if(e == null)
					return "<no value>";

				BLOBTYPE type = e.type;
				String extension = ".bin";

				switch (type) {
					case AVRO ->  extension = ".avro";
					case TIFF ->  extension = ".tiff";
					case BMP  ->  extension = ".bmp";
					case GIF  ->  extension = ".gif";
					case PNG  ->  extension = ".png";
					case JPG  ->  extension = ".jpg";
					case PDF  ->  extension = ".pdf";
					case GZIP ->  extension = ".gz";
					case HEIC ->  extension = ".heic";
					case PLIST  -> extension = ".plist";
					case UNKOWN -> 	extension = ".bin";
				}

				/* CASE: export BLOB data directly to CSV */
				if (Global.EXPORT_MODE == Global.EXPORT_MODES.TOCSV) {
					c = data.toUpperCase();
				}
				/* CASE: export BLOB to separate file & write filename to csv-file */
				else {

					// Build a safe filename: strip any path separators from components
					String safeDbname  = Path.of(dbname).getFileName().toString()
							.replaceAll("[/\\\\]", "_");
					String safeOffset  = offset.replaceAll("[^a-zA-Z0-9_\\-]", "_");
					String safeNumber  = String.valueOf(number);
					c = safeDbname + "_" + safeOffset + "-" + safeNumber + extension;

					// Prevent path traversal: resolve and verify the result stays inside exportfolder
					Path exportDir = Path.of(exportfolder).toAbsolutePath().normalize();
					Path saveas    = exportDir.resolve(c).normalize();
					if (!saveas.startsWith(exportDir)) {
						AppLog.error("exportBLOB: path traversal attempt blocked — " + saveas);
						return "<export blocked>";
					}

					if (e != null) {
						try {
							Files.write(saveas, e.binary, StandardOpenOption.CREATE);
						} catch (IOException ex) {
							AppLog.error("exportBLOB: could not write file " + saveas + ": " + ex.getMessage());
							return "<write error>";
						}
					}
					c = saveas.getFileName().toString();
				}

			}

		}
		return c;
	}



	/**
	 * Save findings into a comma-separated file.
	 * @param filename name of file to export
	 * @param lines lines to write into the file
	 */
	public void writeResultsToFile(String filename, String [] lines) {
		AppLog.info("Write results to file...");
		AppLog.info("Number of records recovered: " + resultlist.size());

		if (lines == null) {
			AppLog.debug("writeResultsToFile: lines is null — nothing to write.");
			return;
		}

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
				/*
				 * Anti-forensics: a table leaf page that no b-tree references (manipulated root or
				 * child pointers) used to be skipped completely, so its live records vanished.
				 * Scan such orphaned table leaf pages as well; the records are attributed to a table
				 * by schema matching (Auxiliary.matchTable).
				 */
				if (!freelistpages.contains(cc)) {
					ByteBuffer pg = readDBPageWithOffset((cc - 1L) * ps, 1);
					if (pg != null && pg.limit() > 0 && pg.get(0) == 13) {
						AppLog.info("Possible manipulation: table leaf page " + cc + " is not referenced by any b-tree - scanning it as orphaned page.");
						RecoveryTask task = new RecoveryTask(worker[cc % Global.numberofThreads].util,this, (cc - 1L) * ps, cc, ps, false);
						worker[cc % Global.numberofThreads].addTask(task);
						runningTasks.incrementAndGet();
						continue;
					}
				}
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
			// Dispatch each worker to the thread pool instead of calling
			// run() directly on the calling thread.
			// executor.shutdown()/awaitTermination() and the runningTasks
			// wait loop below already assume tasks complete asynchronously.
			executor.execute(w);
		}

		try {
			executor.shutdown();
			// Wait up to 5 minutes for all recovery tasks to complete
			boolean finished = executor.awaitTermination(5, TimeUnit.MINUTES);
			if (!finished) {
				AppLog.error("scan(): executor did not finish within timeout — some pages may be incomplete.");
			}
		}
		catch (InterruptedException e) {
			AppLog.error("scan(): interrupted while waiting for tasks");
			Thread.currentThread().interrupt();
		}
		finally {
			if (!executor.isTerminated()) {
				AppLog.error("scan(): cancelling non-finished tasks");
				executor.shutdownNow();
			}
		}

		// wait for runningTasks counter (decremented inside RecoveryTask.run())
		while (runningTasks.intValue() != 0) {
			try {
				TimeUnit.MILLISECONDS.sleep(100);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
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
			AppLog.error("Processing interrupted for " + path + ": " + e.getMessage());
		} catch (IOException e) {
			AppLog.error("Could not open file " + path + ": " + e.getMessage());
			info("Error: Could not open file.");
		}
		long end = System.currentTimeMillis();
		AppLog.info("Duration in ms: " + (end - start));

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

			AppLog.debug("readDBPageWithOffset: offset " + offset + " > db limit " + db.limit());

			return null;
		}

		long available = db.limit() - offset;
		if (available <= 0) {
			return null;
		}
		int toRead = (int) Math.min(pagesize, available);

		// Copy the page bytes into an independent heap buffer instead of
		// returning db.slice() — a slice() is only a *view* backed directly
		// by the mmap window currently resident in BigByteBuffer's LRU page
		// cache (see BigByteBuffer.slice() javadoc). Copying the bytes out up
		// front makes the returned page's lifetime
		// fully independent of the cache.
		byte[] copy = new byte[toRead];
		db.position(offset);
		db.get(copy);

		ByteBuffer page = ByteBuffer.wrap(copy);
		page.order(ByteOrder.BIG_ENDIAN);
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

					AppLog.debug("readWALOverflowPage: offset " + offset + " > db limit " + db.limit());
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

			AppLog.debug("readPageWithNumberFromBuffer: offset " + offset + " > buffer limit " + bb.limit());

			return null;
		}

		long available = bb.limit() - offset;
		if (available <= 0) {
			return null;
		}
		int toRead = (int) Math.min(pagesize, available);

		// See readDBPageWithOffset() above: copy instead of slice() so the
		// returned page's lifetime doesn't depend on this window still
		// being resident in bb's LRU cache later on.
		byte[] copy = new byte[toRead];
		bb.position(offset);
		bb.get(copy);

		ByteBuffer page = ByteBuffer.wrap(copy);
		page.order(ByteOrder.BIG_ENDIAN);
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
	/**
	 * Records of an orphaned table leaf page are attributed by schema matching (first table
	 * with the same column types). If several tables share the same structure, that guess can
	 * be wrong. Use the rowids as tie-breaker: an orphaned page belongs to the table that does
	 * NOT already contain its rowids on regularly referenced pages.
	 */
	private void reassignOrphanPages() {
		if (orphanPages.isEmpty())
			return;
		for (int pg : orphanPages) {
			if (pg <= 0 || pg >= pages.length || !(pages[pg] instanceof TableDescriptor))
				continue;
			TableDescriptor cur = (TableDescriptor) pages[pg];
			ObservableList<ObservableList<String>> curRows = resultlist.get(cur.tblname);
			if (curRows == null)
				continue;
			Set<String> pageRowids = new HashSet<>();
			for (ObservableList<String> r : curRows)
				if (pageOf(r) == pg && r.size() > 3 && Global.REGULAR_RECORD.equals(r.get(3)))
					pageRowids.add(r.get(2));
			if (pageRowids.isEmpty())
				continue;

			// (1) strongest evidence: an interior page of table T has a rejected child slot whose
			//     key range (lo, hi] fits this page's rowids (exact upper key preferred)
			TableDescriptor bySlot = tableBySlot(cur, pageRowids);
			if (bySlot != null) {
				if (bySlot != cur) {
					moveRowsOfPage(pg, cur.tblname, bySlot.tblname);
					pages[pg] = bySlot;
					info("Possible manipulation: orphaned page " + pg + " re-attributed from table "
							+ cur.tblname + " to " + bySlot.tblname + " (dangling child slot of its b-tree).");
				}
				continue;
			}

			// (2) fallback: rowids already present in the current table on regular pages
			if (rowidOverlap(cur, pg, pageRowids) == 0)
				continue;
			for (TableDescriptor cand : headers) {
				if (cand == cur || !cand.getColumntypes().equals(cur.getColumntypes()))
					continue;
				if (resultlist.get(cand.tblname) == null || rowidOverlap(cand, pg, pageRowids) != 0)
					continue;
				moveRowsOfPage(pg, cur.tblname, cand.tblname);
				pages[pg] = cand;
				info("Possible manipulation: orphaned page " + pg + " re-attributed from table "
						+ cur.tblname + " to " + cand.tblname + " (rowid ranges).");
				break;
			}
		}
	}

	/** table whose rejected child slot (lo, hi] contains all rowids of the page; null if none or ambiguous */
	private TableDescriptor tableBySlot(TableDescriptor cur, Set<String> rowids) {
		long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
		for (String r : rowids) {
			try {
				long v = Long.parseLong(r.trim());
				min = Math.min(min, v);
				max = Math.max(max, v);
			} catch (RuntimeException e) {
				return null;
			}
		}
		Set<String> exact = new HashSet<>(), contained = new HashSet<>();
		synchronized (danglingSlots) {
			for (Object[] slot : danglingSlots) {
				long lo = (Long) slot[1], hi = (Long) slot[2];
				if (min > lo && max <= hi) {
					contained.add((String) slot[0]);
					if (max == hi)
						exact.add((String) slot[0]);
				}
			}
		}
		Set<String> pick = exact.size() == 1 ? exact : (exact.isEmpty() && contained.size() == 1 ? contained : null);
		if (pick == null)
			return null;
		String name = pick.iterator().next();
		for (TableDescriptor t : headers)
			if (t.tblname.equals(name) && t.getColumntypes().equals(cur.getColumntypes()))
				return t;
		return null;
	}

	/**
	 * B-tree rebalancing (e.g. after a range DELETE) moves live cells to other pages; the
	 * freed pages still contain the old, byte-identical copies. Reporting them as deleted or
	 * freelist records is a false positive (Lee et al. 2025, "Type **"). A deleted/freelist
	 * record is dropped if a live record of the same table has identical column values (and,
	 * if both are known, the same rowid). Copies with different content (older versions of an
	 * updated row) are kept - they are evidence.
	 */
	private void removeStaleCopiesOfLiveRecords() {
		int removed = 0;
		for (String table : resultlist.keySet()) {
			ObservableList<ObservableList<String>> rows = resultlist.get(table);
			ObservableList<ObservableList<byte[]>> hex = hexdumplist.get(table);
			if (rows == null || rows.size() < 2)
				continue;
			Map<List<String>, Set<String>> live = new HashMap<>();
			// same, with the INTEGER PRIMARY KEY column (value = rowid) blanked: a carved copy loses
			// its rowid and the id column with it (the freeblock header overwrites the cell start)
			Map<List<String>, Set<String>> liveNoId = new HashMap<>();
			for (ObservableList<String> r : rows)
				if (r.size() > 5 && Global.REGULAR_RECORD.equals(r.get(3))) {
					live.computeIfAbsent(new ArrayList<>(r.subList(5, r.size())), k -> new HashSet<>()).add(r.get(2));
					String rid = r.get(2) == null ? "" : r.get(2).trim();
					if (!positiveRowid(rid))
						continue;
					List<String> v = blankValues(r.subList(5, r.size()));
					boolean ipk = false;
					for (int k = 0; k < v.size(); k++)
						if (rid.equals(v.get(k).trim())) {
							v.set(k, "");
							ipk = true;
						}
					if (ipk)
						liveNoId.computeIfAbsent(v, k -> new HashSet<>()).add(rid);
				}
			if (live.isEmpty())
				continue;
			for (int i = rows.size() - 1; i >= 0; i--) {
				ObservableList<String> r = rows.get(i);
				if (r.size() <= 5)
					continue;
				String st = r.get(3);
				if (!Global.FREELIST_ENTRY.equals(st) && !Global.DELETED_RECORD_IN_PAGE.equals(st))
					continue;
				Set<String> liveRowids = live.get(r.subList(5, r.size()));
				if (liveRowids == null && !liveNoId.isEmpty()) {
					List<String> v = blankValues(r.subList(5, r.size()));
					if (v.stream().filter(x -> !x.isEmpty()).count() >= 2)
						liveRowids = liveNoId.get(v);
				}
				if (liveRowids == null)
					continue;
				String rowid = r.get(2);
				boolean rowidUnknown = rowid == null || rowid.isEmpty() || "-1".equals(rowid);
				if (rowidUnknown || liveRowids.contains(rowid)) {
					rows.remove(i);
					if (hex != null && i < hex.size())
						hex.remove(i);
					removed++;
				}
			}
		}
		if (removed > 0)
			info(removed + " deleted/freelist record(s) are identical copies of live records (b-tree rebalancing) - not reported.");
	}

	/**
	 * A deleted record is often found twice: completely (e.g. with its overflow chain) and as a
	 * truncated copy (only the in-page part, cut by the next cell). The truncated copy adds no
	 * information - it is dropped if another record of the same table contains it, i.e. every
	 * non-empty value of the copy equals or is a prefix of the corresponding value. Truncated
	 * records without any recovered value are dropped as well.
	 */
	private void removeTruncatedDuplicates() {
		int removed = 0;
		for (String table : resultlist.keySet()) {
			ObservableList<ObservableList<String>> rows = resultlist.get(table);
			ObservableList<ObservableList<byte[]>> hex = hexdumplist.get(table);
			if (rows == null || rows.size() < 2)
				continue;
			for (int i = rows.size() - 1; i >= 0; i--) {
				ObservableList<String> r = rows.get(i);
				if (r.size() <= 5 || r.get(1) == null || !r.get(1).contains(Auxiliary.FLAG_TRUNCATED))
					continue;
				// a truncated record without any recovered value carries no information at all
				boolean covered = r.subList(5, r.size()).stream().allMatch(v -> v == null || v.isEmpty());
				for (int j = 0; j < rows.size() && !covered; j++) {
					if (j == i)
						continue;
					ObservableList<String> o = rows.get(j);
					if (o.size() != r.size() || (o.get(1) != null && o.get(1).contains(Auxiliary.FLAG_TRUNCATED)))
						continue;
					int same = 0;
					boolean ok = true;
					for (int k = 5; k < r.size() && ok; k++) {
						String a = r.get(k), b = o.get(k);
						if (a == null || a.isEmpty())
							continue;
						if (b != null && b.startsWith(a))
							same++;
						else
							ok = false;
					}
					covered = ok && same >= 2;
				}
				if (covered) {
					rows.remove(i);
					if (hex != null && i < hex.size())
						hex.remove(i);
					removed++;
				}
			}
		}
		if (removed > 0)
			info(removed + " truncated record(s) are empty or partial copies of completely recovered records - not reported.");
	}

	/**
	 * NIST SFT-AO-04: a deleted record whose rowid belongs to a live record of the same table,
	 * that shares at least one value with it and differs in at least one other value, is an
	 * older version left behind by an UPDATE. The finding is appended to the "[pll|hl]" column.
	 * (Rows of a dropped and re-created table share the rowid but no values - not flagged.)
	 */
	private void markUpdatedRecords() {
		int marked = 0;
		for (String table : resultlist.keySet()) {
			ObservableList<ObservableList<String>> rows = resultlist.get(table);
			if (rows == null || rows.size() < 2)
				continue;
			Map<String, List<ObservableList<String>>> live = new HashMap<>();
			for (ObservableList<String> r : rows)
				if (r.size() > 5 && Global.REGULAR_RECORD.equals(r.get(3)) && positiveRowid(r.get(2)))
					live.computeIfAbsent(r.get(2).trim(), k -> new ArrayList<>()).add(r);
			if (live.isEmpty())
				continue;
			for (ObservableList<String> r : rows) {
				if (r.size() <= 5)
					continue;
				String st = r.get(3), pll = r.get(1);
				if (!Global.DELETED_RECORD_IN_PAGE.equals(st) && !Global.FREELIST_ENTRY.equals(st))
					continue;
				if (pll == null || pll.contains(Auxiliary.FLAG_UPDATED) || r.get(2) == null)
					continue;
				List<ObservableList<String>> candidates = live.get(r.get(2).trim());
				if (candidates == null)
					continue;
				for (ObservableList<String> l : candidates) {
					if (isOlderVersion(r, l)) {
						r.set(1, pll + " " + Auxiliary.FLAG_UPDATED);
						marked++;
						break;
					}
				}
			}
		}
		marked += markUpdatedWithoutRowid();
		if (marked > 0)
			info(marked + " deleted record(s) are older versions of live records (UPDATE) - flagged.");
	}

	/**
	 * Carved records often lose their rowid (the freeblock header overwrites the cell start).
	 * Such a record is an older version of a live record if exactly one live record of the
	 * table shares a distinctive value with it and has the same value in all but one of its
	 * (at least three) non-empty columns.
	 */
	private int markUpdatedWithoutRowid() {
		int marked = 0;
		for (String table : resultlist.keySet()) {
			ObservableList<ObservableList<String>> rows = resultlist.get(table);
			if (rows == null || rows.size() < 2)
				continue;
			// index: column -> value -> live rows (only distinctive values are used)
			Map<Integer, Map<String, List<ObservableList<String>>>> index = new HashMap<>();
			for (ObservableList<String> r : rows) {
				if (r.size() <= 5 || !Global.REGULAR_RECORD.equals(r.get(3)))
					continue;
				for (int k = 5; k < r.size(); k++) {
					String v = r.get(k);
					if (v == null || v.isEmpty() || v.equals("NULL"))
						continue;
					index.computeIfAbsent(k, x -> new HashMap<>()).computeIfAbsent(v, x -> new ArrayList<>()).add(r);
				}
			}
			if (index.isEmpty())
				continue;
			for (ObservableList<String> r : rows) {
				if (r.size() <= 5 || positiveRowid(r.get(2)))
					continue;                         // rows with a rowid are handled by markUpdatedRecords()
				String st = r.get(3), pll = r.get(1);
				if (!Global.DELETED_RECORD_IN_PAGE.equals(st) && !Global.FREELIST_ENTRY.equals(st))
					continue;
				if (pll == null || pll.contains(Auxiliary.FLAG_UPDATED) || pll.contains(Auxiliary.FLAG_TRUNCATED)
						|| pll.contains("header\u2260payload"))
					continue;                         // incomplete or misparsed records are not compared
				int nonEmpty = 0;
				Map<ObservableList<String>, Integer> hits = new java.util.IdentityHashMap<>();
				for (int k = 5; k < r.size(); k++) {
					String v = r.get(k);
					if (v == null || v.isEmpty() || v.equals("NULL"))
						continue;
					nonEmpty++;
					Map<String, List<ObservableList<String>>> col = index.get(k);
					List<ObservableList<String>> same = col == null ? null : col.get(v);
					if (same == null || same.size() > 50)
						continue;                     // value not distinctive
					for (ObservableList<String> l : same)
						hits.merge(l, 1, Integer::sum);
				}
				if (nonEmpty < 3)
					continue;
				ObservableList<String> match = null;
				int matches = 0;
				// candidates share at least one distinctive value; all but one of the values must be equal
				for (Map.Entry<ObservableList<String>, Integer> e : hits.entrySet()) {
					if (differsInOneColumn(r, e.getKey())) {
						match = e.getKey();
						matches++;
					}
				}
				if (matches == 1 && match != null) {
					r.set(1, pll + " " + Auxiliary.FLAG_UPDATED + " of rowid " + match.get(2));
					marked++;
				}
			}
		}
		return marked;
	}

	private static boolean differsInOneColumn(List<String> deleted, List<String> live) {
		int diff = 0, col = -1;
		for (int k = 5; k < Math.min(deleted.size(), live.size()); k++) {
			String a = deleted.get(k), b = live.get(k);
			if (a == null || a.isEmpty() || a.equals("NULL"))
				continue;
			if (!a.equals(b)) {
				diff++;
				col = k;
			}
		}
		if (diff != 1)
			return false;
		// only the INTEGER PRIMARY KEY (= rowid) differs: a copy of the same row with a lost id, not an UPDATE
		String rowid = live.get(2) == null ? "" : live.get(2).trim();
		return !rowid.equals(live.get(col) == null ? null : live.get(col).trim());
	}

	/** column values with SQL NULL / "NULL" / null mapped to "" (for comparisons only) */
	private static List<String> blankValues(List<String> vals) {
		List<String> v = new ArrayList<>(vals.size());
		for (String x : vals)
			v.add(x == null || x.equals("NULL") ? "" : x);
		return v;
	}

	private static boolean positiveRowid(String s) {
		try { return s != null && Long.parseLong(s.trim()) > 0; } catch (NumberFormatException e) { return false; }
	}

	private static boolean isOlderVersion(List<String> deleted, List<String> live) {
		String rowid = deleted.get(2).trim();
		int same = 0, diff = 0;
		int n = Math.min(deleted.size(), live.size());
		for (int k = 5; k < n; k++) {
			String a = deleted.get(k), b = live.get(k);
			if (a == null || a.isEmpty() || b == null)
				continue;
			if (a.equals(rowid) && b.equals(rowid))
				continue;            // INTEGER PRIMARY KEY column = alias of the rowid
			if (a.equals(b)) same++; else diff++;
		}
		return same >= 1 && diff >= 1;
	}

	/** NIST SFT-AO-08: WAL frames grouped into transactions, in chronological order. Row 0 = column names. */
	public String[][] getWALTransactions() {
		List<String[]> out = new ArrayList<>();
		out.add(new String[]{"Tx", "Generation (salt-1)", "Salt-2", "Frames", "Pages (table)", "DB size after commit", "State"});
		if (wal == null)
			return out.toArray(new String[0][]);
		int tx = 0;
		for (Long salt1 : wal.checkpoints.keySet()) {     // ascending: salt-1 is incremented on every WAL reset
			List<WALFrame> frames = new ArrayList<>(wal.checkpoints.get(salt1));
			frames.sort((a, b) -> Integer.compare(a.framenumber, b.framenumber));
			boolean current = salt1 == wal.hsalt1;
			List<WALFrame> open = new ArrayList<>();
			for (int i = 0; i < frames.size(); i++) {
				WALFrame f = frames.get(i);
				open.add(f);
				boolean lastFrame = i == frames.size() - 1;
				if (f.commitSize <= 0 && !lastFrame)
					continue;
				String state;
				if (!current)
					state = "earlier WAL generation (stale frames)";
				else
					state = f.commitSize > 0 ? "committed" : "not committed (no commit frame)";
				StringBuilder pg = new StringBuilder();
				java.util.LinkedHashSet<Integer> seen = new java.util.LinkedHashSet<>();
				for (WALFrame o : open)
					seen.add(o.pagenumber);
				for (Integer n : seen) {
					if (pg.length() > 0)
						pg.append(", ");
					pg.append(n);
					if (pages != null && n >= 0 && n < pages.length && pages[n] != null)
						pg.append(" (").append(pages[n].getName()).append(")");
				}
				out.add(new String[]{String.valueOf(++tx), String.valueOf(salt1), String.valueOf(f.salt2),
						open.get(0).framenumber + "-" + f.framenumber, pg.toString(),
						f.commitSize > 0 ? String.valueOf(f.commitSize) : "", state});
				open.clear();
			}
		}
		return out.toArray(new String[0][]);
	}

	private int fileTag(ExportType exp) {
		if (exp == ExportType.WALARCHIVE) return Global.WAL_ARCHIVE_FILE;
		if (exp == ExportType.ROLLBACKJOURNAL) return Global.ROLLBACK_JOURNAL_FILE;
		return Global.REGULAR_DB_FILE;
	}

	/** name of the file a row was read from (NIST SFT-CA-13) */
	private String sourceFileName(int filetag) {
		String p = filetag == Global.WAL_ARCHIVE_FILE ? walpath
				 : filetag == Global.ROLLBACK_JOURNAL_FILE ? rollbackjournalpath : path;
		if (p == null)
			return "";
		Path fn = Paths.get(p).getFileName();
		return fn == null ? p : fn.toString();
	}

	/** "offset:length" of every value of a recovered row, as recorded during the recovery (NIST SFT-AO-06) */
	private String cellsOf(int filetag, List<String> row) {
		if (row == null || row.size() <= 5 || row.get(5) == null)
			return "";
		return cellLayout.getOrDefault(filetag + ":" + row.get(5).trim(), "");
	}

	private long pageOf(List<String> row) {
		try {
			return Long.parseLong(row.get(4).trim()) / ps + 1;
		} catch (RuntimeException e) {
			return -1;
		}
	}

	private int rowidOverlap(TableDescriptor td, int pg, Set<String> rowids) {
		ObservableList<ObservableList<String>> rows = resultlist.get(td.tblname);
		if (rows == null)
			return 0;
		int n = 0;
		for (ObservableList<String> r : rows) {
			long p = pageOf(r);
			if (p == pg || orphanPages.contains((int) p) || r.size() < 4
					|| !Global.REGULAR_RECORD.equals(r.get(3)))
				continue;
			if (rowids.contains(r.get(2)))
				n++;
		}
		return n;
	}

	private void moveRowsOfPage(int pg, String from, String to) {
		ObservableList<ObservableList<String>> src = resultlist.get(from);
		ObservableList<ObservableList<byte[]>> srcHex = hexdumplist.get(from);
		ObservableList<ObservableList<String>> dst = resultlist.get(to);
		ObservableList<ObservableList<byte[]>> dstHex = hexdumplist.get(to);
		for (int i = src.size() - 1; i >= 0; i--) {
			ObservableList<String> r = src.get(i);
			if (pageOf(r) != pg || r.size() < 4 || !Global.REGULAR_RECORD.equals(r.get(3)))
				continue;
			src.remove(i);
			r.set(0, to);
			dst.add(r);
			if (srcHex != null && dstHex != null && i < srcHex.size())
				dstHex.add(srcHex.remove(i));
		}
	}

	/** reads a SQLite varint at an absolute position of the buffer */
	private static long varintAt(ByteBuffer b, int pos) {
		long v = 0;
		for (int k = 0; k < 9 && pos < b.limit(); k++) {
			int c = b.get(pos++) & 0xFF;
			if (k == 8) return (v << 8) | c;
			v = (v << 7) | (c & 0x7F);
			if (c < 0x80) return v;
		}
		return v;
	}

	/** rowid of a table leaf cell [payload length varint][rowid varint] */
	private static long leafRowidAt(ByteBuffer b, int celloff) {
		int pos = celloff;
		for (int k = 0; k < 9 && pos < b.limit(); k++)        // skip payload length
			if ((b.get(pos++) & 0x80) == 0) break;
		return varintAt(b, pos);
	}

	/**
	 * true if the smallest and largest key of table b-tree page {@code child} lie in (lo, hi].
	 * Invalid page numbers are rejected; pages that cannot be checked (other page types,
	 * unreadable) are accepted.
	 */
	private boolean childWithinKeyRange(int child, long lo, long hi, BigByteBuffer filebuffer) {
		try {
			if (child <= 1 || child > numberofpages)
				return false;                  // invalid page number - certainly not a valid child
			ByteBuffer c = readPageWithNumberFromBuffer(child - 1, ps, filebuffer);
			if (c == null)
				return true;
			int t = c.get(0);
			if (t != 13 && t != 5)
				return true;
			int n = Short.toUnsignedInt(c.getShort(3));
			if (n == 0)
				return true;
			int hdr = (t == 13) ? 8 : 12;
			int first = Short.toUnsignedInt(c.getShort(hdr));
			int last  = Short.toUnsignedInt(c.getShort(hdr + 2 * (n - 1)));
			if (first <= 0 || last <= 0 || first >= c.limit() || last >= c.limit())
				return true;
			long kmin = (t == 13) ? leafRowidAt(c, first) : varintAt(c, first + 4);
			long kmax = (t == 13) ? leafRowidAt(c, last)  : varintAt(c, last + 4);
			return kmin > lo && kmax <= hi;
		} catch (RuntimeException ex) {
			return true;
		}
	}

	private void exploreBTree(int root, AbstractDescriptor td, BigByteBuffer filebuffer) throws IOException {
		exploreBTree(root, td, filebuffer, new HashSet<>());
	}

	private void exploreBTree(int root, AbstractDescriptor td, BigByteBuffer filebuffer, Set<Integer> visited) throws IOException {

		if (!visited.add(root)) {
			AppLog.debug("exploreBTree: cycle detected at page " + root + " — stopping.");
			return;
		}

		if (root >= pages.length || root <= 0)
			return;

		// pagesize * (rootindex - 1) -> go to the start of this page
		long offset = (long) ps * (root - 1);

		if (offset <= 0) {                     // page 1 (sqlite_master)
			pages[root] = td;
			return;
		}

		// read type byte
		filebuffer.position(offset);
		byte typ = filebuffer.get();

		/*
		 * Anti-forensics: a (manipulated) root or child pointer may point to a page of the wrong
		 * kind, e.g. a table root pointing to an index page or to an overflow page. Assigning such
		 * a page to the table produced garbage rows (index entries shown as table rows). Only
		 * table b-tree pages (5, 13) belong to a table, only index b-tree pages (2, 10) to an index.
		 */
		boolean isTable = !(td instanceof IndexDescriptor);
		// typ 0: page of a dropped table / cleared page (freelist trunk, zeroed header) - keep the
		// assignment, the page is carved with this table's schema later (RecoveryTask, type 0)
		boolean typeOk  = typ == 0 || (isTable ? (typ == 5 || typ == 13) : (typ == 2 || typ == 10));
		if (!typeOk) {
			AppLog.info("Possible manipulation: page " + root + " (page type " + typ + ") is referenced by "
					+ (isTable ? "table " : "index ") + td.getName() + " but is no "
					+ (isTable ? "table" : "index") + " b-tree page - ignored.");
			return;
		}
		pages[root] = td;


		/* not supported yet */
		if (typ == 2)
		{
			AppLog.debug(" page number" + root + " is a  INDEXINTERIORPAGE.");
			int rightChildptr = filebuffer.getInt(offset + 8);

			exploreBTree(rightChildptr, td, filebuffer, visited);

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
				exploreBTree(p, td, filebuffer, visited);
			}


		}
		/* type is either a data interior page (12) */
		else if (typ == 5) {

			AppLog.debug("page number " + root + " is a interior data page ");

			if (root <= 0)
				return;
			ByteBuffer buffer = readPageWithNumberFromBuffer(root - 1, ps, filebuffer);
			if (buffer == null)
				return;

			int rightChildptr = buffer.getInt(8);
			int e = Short.toUnsignedInt(buffer.getShort(3));

			/*
			 * Anti-forensics: every interior cell is [left child (4)][key (varint)], keys ascending.
			 * The rowids of a child must lie in (previous key, key]; the right-most child holds the
			 * rowids > last key. A manipulated child pointer typically points to a page of another
			 * table (or another part of the tree) whose rowids violate these bounds. Such a child
			 * is not attached here - it is scanned later as orphaned page (see scan()) and
			 * attributed by schema and rowid ranges.
			 */
			long prevKey = Long.MIN_VALUE;
			for (int i = 0; i < e; i++) {
				int ptrpos = 12 + 2 * i;
				if (ptrpos + 2 > buffer.limit())
					break;
				int celloff = Short.toUnsignedInt(buffer.getShort(ptrpos));
				if (celloff <= 0 || celloff + 5 > buffer.limit() || celloff > ps)
					continue;
				int p = buffer.getInt(celloff);
				long key = varintAt(buffer, celloff + 4);
				AppLog.debug(" child page " + p);
				if (childWithinKeyRange(p, prevKey, key, filebuffer))
					exploreBTree(p, td, filebuffer, visited);
				else {
					danglingSlots.add(new Object[]{td.getName(), prevKey, key});
					AppLog.info("Possible manipulation: child page " + p + " of interior page " + root
							+ " (table " + td.getName() + ") holds rowids outside (" + prevKey + ", " + key + "] - not attached.");
				}
				prevKey = key;
			}

			if (childWithinKeyRange(rightChildptr, prevKey, Long.MAX_VALUE, filebuffer))
				exploreBTree(rightChildptr, td, filebuffer, visited);
			else {
				danglingSlots.add(new Object[]{td.getName(), prevKey, Long.MAX_VALUE});
				AppLog.info("Possible manipulation: right child page " + rightChildptr + " of interior page " + root
						+ " (table " + td.getName() + ") holds rowids <= " + prevKey + " - not attached.");
			}

		}
		else if (typ == 8 || typ == 10 || typ == 13) {
			AppLog.debug("page number " + root + " is a leaf page " + " set component/index to " + td.getName());
			if (root >  numberofpages)
				return;
			if (null == pages[root])
				pages[root] = td;
			else {
				// M-fix: was comparing td.getName() with itself — now compares with existing assignment
				if (!td.getName().equals(pages[root].getName()))
					AppLog.debug("Page " + root + " has already a B+Tree assignment with table " + pages[root].getName()
								 + " — attempted re-assignment to " + td.getName());
			}
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
