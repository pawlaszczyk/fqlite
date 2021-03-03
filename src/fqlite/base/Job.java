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
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import javax.swing.JOptionPane;
import javax.swing.SwingWorker;
import javax.swing.tree.TreePath;

import fqlite.descriptor.AbstractDescriptor;
import fqlite.descriptor.IndexDescriptor;
import fqlite.descriptor.TableDescriptor;
import fqlite.ui.DBPropertyPanel;
import fqlite.ui.HexView;
import fqlite.ui.RollbackPropertyPanel;
import fqlite.ui.WALPropertyPanel;
import fqlite.util.Auxiliary;
import fqlite.util.ByteSeqSearcher;


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
 * @version 1.2
 */
public class Job extends Base {

	/* the byte buffer representing the database file in RAM */
	public ByteBuffer db;
	
	/* hex editor object reference */
	HexView hexview;

	/* since version 1.2 - support for write ahead logs WAL */
	public boolean readWAL = false;
	String walpath = null;
	WALReader wal = null;
	Hashtable<String, TreePath> guiwaltab = new Hashtable<String, TreePath>();

	/* since version 1.2 - support for write Rollback Journal files */
	public boolean readRollbackJournal = false;
	String rollbackjournalpath = null;
	RollbackJournalReader rol = null;
	Hashtable<String, TreePath> guiroltab = new Hashtable<String, TreePath>();
	
	
    /* some constants */
	final static String MAGIC_HEADER_STRING = "53514c69746520666f726d6174203300";
	final static String NO_AUTO_VACUUM = "00000000";
	final static String NO_MORE_ENTRIES = "00000000";
	final static String LEAF_PAGE = "0d";
	final static String INTERIOR_PAGE = "05";
	final static String ROW_ID = "00";
	
	
	/* the next fields hold informations for the GUI */
	GUI gui = null;
	Hashtable<String, TreePath> guitab = new Hashtable<String, TreePath>();
	
	/* property panel for the user interface - only in gui-mode */
	DBPropertyPanel panel = null;
	WALPropertyPanel walpanel = null;
	RollbackPropertyPanel rolpanel = null;
	
	/* size of file */
	long size = 0;
	
	/* path - used to locate a file in a file system */
	String path;
	
	/* An asynchronous channel for reading, writing, and manipulating the database file. */
	public AsynchronousFileChannel file;
	
	/* this field represent the database encoding */
	public static Charset db_encoding = StandardCharsets.UTF_8;
	
	/* the list is used to export the recovered data records */
	List<String> lines = new LinkedList<String>();
	
	/* this is a multi-threaded program -> all data are saved to the list first*/
	ConcurrentLinkedQueue<String> ll = new ConcurrentLinkedQueue<String>();
	
	/* Virtual Tables */
	public Map<String,TableDescriptor> virtualTables = new HashMap<String,TableDescriptor>();
	
	int scanned_entries = 0;
	
	Hashtable<String, String> tblSig;
	boolean is_default = false;
	public List<TableDescriptor> headers = new LinkedList<TableDescriptor>();
	public List<IndexDescriptor> indices = new LinkedList<IndexDescriptor>();
	public AtomicInteger runningTasks = new AtomicInteger();
	int numberofThreads = 1;
	int tablematch = 0;
	int indexmatch = 0; 
	
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
	
	
	public String[][] getHeaderProperties()
	{
		String [][] prop = {{"","Path",path},{"","File Size (in Bytes)",String.valueOf(totalbytes)},{"","Number of Pages",String.valueOf(numberofpages)},{"16","Page Size",String.valueOf(ps)},{"32","Page number of the first freelist trunk page.",String.valueOf(fphead)},{"36","Total number of freelist pages.\n"
				+ "",String.valueOf(fpnumber)},{"52","Auto-Vacuum",String.valueOf(autovacuum)},    {"56","Database Encoding",db_encoding.displayName()}, {"96","SQLITE_VERSION_NUMBER",sqliteversion}};
		
		return prop;
	}
	
	public String[][] getWALHeaderProperties()
	{
		String [][] prop = {{"","Path",wal.path}};
		
		return prop;
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

	public void updatePropertyPanel()
	{
		panel.initHeaderTable(getHeaderProperties());
		panel.initSchemaTable(getSchemaProperties());
	}

	public void updateWALPanel()
	{
		walpanel.initHeaderTable(getWALHeaderProperties());
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

			/* First - try to analyze the db-schema */
			/*
			 * we have to do this before we open the database because of the concurrent
			 * access
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

			/********************************************************************/

			byte header[] = new byte[16];
			buffer.get(header);
			if (Auxiliary.bytesToHex(header).equals(MAGIC_HEADER_STRING)) // we currently
				// support
				// sqlite 3 data
				// bases
				info("header is okay. seems to be an sqlite database file.");
			else {
				info("sorry. doesn't seem to be an sqlite file. Wrong header.");
				err("Doesn't seem to be an valid sqlite file. Wrong header");
				return -1;
			}

			/********************************************************************/

			is_default = true;
			tblSig = new Hashtable<String, String>();
			tblSig.put("", "default");
			info("found unkown sqlite-database.");

			/********************************************************************/

			byte avacuum[] = new byte[4];
			buffer.position(52);
			buffer.get(avacuum);

			if (Auxiliary.bytesToHex(avacuum).equals(NO_AUTO_VACUUM)) {
				info("Seems to be no AutoVacuum db. Nice :-).");
				autovacuum = true;
			} else
				autovacuum = false;

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

			/* determine the sqlversion ot db on offset 96 */

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

			int index = bsearch.indexOf(db, 0);

			/* search as long as we can find further table keywords */
			while (index != -1) {
				/* get Header */
				byte mheader[] = new byte[40];
				db.position(index - goback);
				db.get(mheader);
				String headerStr = Auxiliary.bytesToHex(mheader);

				/* normal component */
				if (headerStr.startsWith("17") || headerStr.startsWith("21")) {

					headerStr = "07" + headerStr; // put the header length byte in front
					// note: for a dropped component this information is lost!!!

					tablematch++;
					Auxiliary c = new Auxiliary(this);
					headerStr = headerStr.substring(0, 14);

					// compute offset
					int starthere = index % ps;
					int pagenumber = index / ps;

					ByteBuffer bbb = readPageWithNumber(pagenumber, ps);

					if (db_encoding == StandardCharsets.UTF_8)
						c.readMasterTableRecord(this, starthere - 13, bbb, headerStr);
					else if (db_encoding == StandardCharsets.UTF_16LE)
						c.readMasterTableRecord(this, starthere - 17, bbb, headerStr);
					else if (db_encoding == StandardCharsets.UTF_16BE)
						c.readMasterTableRecord(this, starthere - 18, bbb, headerStr);
				} else if (headerStr.startsWith("0617")) {
					tablematch++;
					Auxiliary c = new Auxiliary(this);
					headerStr = headerStr.substring(0, 14);

					// compute offset
					int starthere = index % ps;
					int pagenumber = index / ps;

					ByteBuffer bbb = readPageWithNumber(pagenumber, ps);

					if (db_encoding == StandardCharsets.UTF_8)
						c.readMasterTableRecord(this, starthere - 13, bbb, headerStr);
					else
						c.readMasterTableRecord(this, starthere - 17, bbb, headerStr);

				} else {
					// false-positive match for <component> since no column type 17 (UTF-8) resp. 21
					// (UTF-16) was found
				}
				/* search for more component entries */
				index = bsearch.indexOf(db, index + 2);
			}

			/* we looking for the key word <index> */
			/* to mark the start of an indices entry for the sqlmaster_table */
			ByteSeqSearcher bisearch = new ByteSeqSearcher(ipattern);

			int index2 = bisearch.indexOf(db, 0);

			/* search as long as we can find further index keywords */
			while (index2 != -1) {

				/* get Header */
				byte mheader[] = new byte[40];
				db.position(index2 - goback);
				db.get(mheader);
				String headerStr = Auxiliary.bytesToHex(mheader);

				System.out.println(headerStr);

				/* normal component */
				if (headerStr.startsWith("17") || headerStr.startsWith("21")) {

					headerStr = "07" + headerStr; // put the header length byte in front
					// note: for a dropped indices this information is lost!!!

					indexmatch++;
					Auxiliary c = new Auxiliary(this);
					headerStr = headerStr.substring(0, 14);

					// compute offset
					int starthere = index2 % ps;
					int pagenumber = index2 / ps;

					ByteBuffer bbb = readPageWithNumber(pagenumber, ps);

					if (db_encoding == StandardCharsets.UTF_8)
						c.readMasterTableRecord(this, starthere - 13, bbb, headerStr);
					else if (db_encoding == StandardCharsets.UTF_16LE)
						c.readMasterTableRecord(this, starthere - 17, bbb, headerStr);
					else if (db_encoding == StandardCharsets.UTF_16BE)
						c.readMasterTableRecord(this, starthere - 18, bbb, headerStr);
				} else if ((headerStr.startsWith("0617"))) {
					Auxiliary c = new Auxiliary(this);
					headerStr = headerStr.substring(0, 14);

					// compute offset
					int starthere = index2 % ps;
					int pagenumber = index2 / ps;

					ByteBuffer bbb = readPageWithNumber(pagenumber, ps);

					if (db_encoding == StandardCharsets.UTF_8)
						c.readMasterTableRecord(this, starthere - 13, bbb, headerStr);
					else
						c.readMasterTableRecord(this, starthere - 17, bbb, headerStr);

				} else {
					// false-positive match for <component> since no column type 17 (UTF-8) resp. 21
					// (UTF-16) was found
				}
				/* search for more indices entries */
				index2 = bisearch.indexOf(db, index2 + 2);

			}

			/*******************************************************************/

			/*
			 * Since we now have all component descriptors - let us update the 
			 * indices descriptors
			 */

			Iterator<IndexDescriptor> itidx = indices.iterator();
			while (itidx.hasNext()) {
				IndexDescriptor id = itidx.next();
				String tbn = id.tablename;

				for (int i = 0; i < headers.size(); i++) {

					if (tbn != null && tbn.equals(headers.get(i).tblname)) {

						/* component for index could be found */
						TableDescriptor td = headers.get(i);

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

						exploreBTree(id.getRootOffset(), id);

						break;
					}

				}
			}

			/* prepare pre-compiled pattern objects */
			Iterator<TableDescriptor> iter = headers.iterator();

			/* this one is needed for the UI */
			TreePath lastpath = null;

			HashSet<String> doubles = new HashSet<String>();
 			
			/*
			 * Now, since we have all component definitions, we can update the UI and start
			 * exploring the b-tree for each component
			 */
			while (iter.hasNext()) {

				TableDescriptor td = iter.next();

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

				/* update treeview in UI - skip this step in console modus */
				if (null != gui) {

					TreePath path = gui.add_table(this, td.tblname, td.columnnames, td.getColumntypes(), false, false,0);
					guitab.put(td.tblname, path);
					lastpath = path;

					if (readWAL) {

						TreePath walpath = gui.add_table(this, td.tblname, td.columnnames, td.getColumntypes(), true, false,0);
						guiwaltab.put(td.tblname, walpath);
						setWALPath(walpath.toString());

					}

					else if (readRollbackJournal) {
						TreePath rjpath = gui.add_table(this, td.tblname, td.columnnames, td.getColumntypes(), false, true,0);
						guiroltab.put(td.tblname, rjpath);
						setRollbackJournalPath(rjpath.toString());
					}

					if (null != lastpath) {
						System.out.println("Expend Path" + lastpath);
						GUI.tree.expandPath(lastpath);
					}
				}

				if (td.isVirtual())
					continue;

				/* transfer component information for later recovery */
				RecoveryTask.tables.add(td);

				/* explore a component trees and build up page info */
				exploreBTree(r, td);

			}

			/*******************************************************************/

			Iterator<IndexDescriptor> it = indices.iterator();

			while (it.hasNext()) {

				IndexDescriptor id = it.next();
				int r = id.getRootOffset();
				info(" root offset for index " + r);

				/* update treeview in UI - skip this step in console modus */
				if (null != gui) {
					TreePath path = gui.add_table(this, id.idxname, id.columnnames, id.columntypes, false, false,1);
					System.out.println("id.idxname " + id.idxname);
					guitab.put(id.idxname, path);
					lastpath = path;

					if (readWAL) {
						TreePath walpath = gui.add_table(this, id.idxname, id.columnnames, id.columntypes, true, false,1);
						guiwaltab.put(id.idxname, walpath);
						setWALPath(walpath.toString());

					}

					else if (readRollbackJournal) {
						TreePath rjpath = gui.add_table(this, id.idxname, id.columnnames, id.columntypes, false, true,1);
						guiroltab.put(id.idxname, rjpath);
						setRollbackJournalPath(rjpath.toString());
					}

					if (null != lastpath) {
						System.out.println("Expend Path" + lastpath);
						GUI.tree.expandPath(lastpath);
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
				TableDescriptor tdefault = new TableDescriptor("__UNASSIGNED", "", col, names, null, false);
				headers.add(tdefault);

				/* update treeview in UI - skip this step in console modus */
				if (null != gui) {
					TreePath path = gui.add_table(this, tdefault.tblname, tdefault.columnnames,
							tdefault.getColumntypes(), false, false,0);
					guitab.put(tdefault.tblname, path);
					lastpath = path;

					if (readWAL) {
						TreePath walpath = gui.add_table(this, tdefault.tblname, tdefault.columnnames,
								tdefault.getColumntypes(), true, false,0);
						guiwaltab.put(tdefault.tblname, walpath);
						setWALPath(walpath.toString());
					}

					else if (readRollbackJournal) {
						TreePath rjpath = gui.add_table(this, tdefault.tblname, tdefault.columnnames,
								tdefault.getColumntypes(), false, true,0);
						guiroltab.put(tdefault.tblname, rjpath);
						setRollbackJournalPath(rjpath.toString());
					}

					if (null != lastpath) {
						System.out.println("Expend Path" + lastpath);
						GUI.tree.expandPath(lastpath);
					}
				}
			}

			/*******************************************************************/

			byte freepageno[] = new byte[4];
			buffer.position(36);
			buffer.get(freepageno);
			info("Total number of free list pages " + Auxiliary.bytesToHex(freepageno));
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
				ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numberofThreads);

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

					fplist.get(nextlistoffset);

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
					fplist.get(numberOfEntries);
					ByteBuffer e = ByteBuffer.wrap(numberOfEntries);
					int entries = e.getInt();
					info(" Number of Entries in freepage list " + entries);

					runningTasks.set(0);
					
					/* iterate through free page list and read free page offsets */
					for (int zz = 1; zz <= entries; zz++) {
						byte next[] = new byte[4];
						fplist.position(4 * zz);
						fplist.get(next);

						ByteBuffer bf = ByteBuffer.wrap(next);
						int n = bf.getInt();

						if (n == 0) {
							continue;
						}
						// determine offset for free page
						int offset = (n - 1) * ps;

						//System.out.println("page " + n + " at offset " + offset);

						// if (offset Job.size)
						// continue;

						RecoveryTask task = new RecoveryTask(new Auxiliary(this), this, offset, n, ps, true);
						/* add new task to executor queue */
						runningTasks.incrementAndGet();
						tasklist.add(task);
						executor.execute(task);
						// task.run();
					}
					freepagesum += entries;

				} while (morelistpages); // while

				executor.shutdown();

				info("Task total: " + runningTasks.intValue());

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

			info("Lines after free page recovery: " + ll.size());

			/*******************************************************************/
			// start carving

			// full db-scan (including all database pages)
			scan(numberofpages, ps);

			/*******************************************************************/

			if (gui != null) {
				info("Number of records recovered: " + ll.size());

				String[] lines = ll.toArray(new String[0]);
				Arrays.sort(lines);

				TreePath path = null;
				for (String line : lines) {
					String[] data = line.split(";");

					path = guitab.get(data[0]);
					gui.update_table(path, data);
				}

				SwingWorker<Boolean, Void> backgroundProcess = new HexViewCreator(this, path, file, this.path, 0);

				
				backgroundProcess.execute();

				if (GUI.doesWALFileExist(this.path) > 0) {

					String walpath = this.path + "-wal";
					wal = new WALReader(walpath, this);
					wal.parse();
					wal.output();

				}

				if (GUI.doesRollbackJournalExist(this.path) > 0) {

					String rjpath = this.path + "-journal";
					rol = new RollbackJournalReader(rjpath, this);
					rol.ps = this.ps;
					rol.parse();
					rol.output();
				}
			} else {
				String[] lines = ll.toArray(new String[0]);
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

				return lines.toString().hashCode();
			}

		} catch (IOException e) {

			info("Error: Could not open file.");
			System.exit(-1);
		}

		return 0;
	}
	/**
	 * Save findings into a comma separated file.
	 * @param filename
	 * @param lines
	 */
	public void writeResultsToFile(String filename, String [] lines) {
		System.out.println("Write results to file...");
		System.out.println("Number of records recovered: " + ll.size());

		if (null == filename) {
			Path dbfilename = Paths.get(path);
			String name = dbfilename.getFileName().toString();

			LocalDateTime now = LocalDateTime.now();
			DateTimeFormatter df;
			df = DateTimeFormatter.ISO_DATE_TIME; // 2020-01-31T20:07:07.095
			String date = df.format(now);

			filename = "results" + name + date + ".csv";
		}
		
		Arrays.sort(lines);

		/** convert line to UTF-8 **/
		try {
			
			final File file = new File(filename);
            
			
		    try (final BufferedWriter writer = Files.newBufferedWriter(file.toPath(),Charset.forName("UTF-8"), StandardOpenOption.CREATE)) 
		    {
		      for (String line: lines)
		      {	
		    	  writer.write(line);
		      }
		    }
			
		} catch (IOException e) {
			e.printStackTrace();
		}

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
	 * The method creates a Task object for each page to be scanned.
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
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numberofThreads);

		long begin = System.currentTimeMillis();
		
		/* first start scan of regular pages */
	
		Worker[] worker = new Worker[numberofThreads]; 
        for (int i = 0; i < numberofThreads; i++)
        {
        	worker[i] = new Worker(this);
        }
		
		for (int cc = 1; cc < pages.length; cc++) {
			
			if (null == pages[cc]) 
			{
				debug("page " + cc + " is no regular leaf page component. Maybe a indices or overflow or dropped component page.");
				System.out.println("page " + cc + " offset " + ((cc-1)*ps) + " is no regular leaf page component. Maybe a indices or overflow or dropped component page.");
			} 
			else 
			{
				debug("page" + cc + " is a regular leaf page. ");

				// determine offset for free page
				long offset = (cc - 1) * ps;

				//System.out.println("************************ pagenumber " + cc + " " + offset);

				RecoveryTask task = new RecoveryTask(worker[cc % numberofThreads].util,this, offset, cc, ps, false);
				worker[cc % numberofThreads].addTask(task);
				runningTasks.incrementAndGet();
			}
		}
		debug("Task total: " + runningTasks.intValue() + " worker threads " + numberofThreads);
		
		int c = 1;
		/* start executing the work threads */
		for (Worker w : worker)
		{	
			System.out.println(" Start worker thread" + c++);
			/* add new task to executor queue */
			executor.execute(w);			

		}

		try {
		    System.out.println("attempt to shutdown executor");
		    executor.shutdown();
		
		    int remaining = 0;
		    int i = 0;
		    do
		    {
		    	remaining = runningTasks.intValue();
		    	i++;
		    	System.out.println(" Still running tasks " + remaining);
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
		//while (runningTasks.intValue() != 0) {
		//	try {
		//		TimeUnit.MILLISECONDS.sleep(1000);
		//		System.out.println("warte....");
		//	} catch (InterruptedException e) {
		//		e.printStackTrace();
		//	}
		//}

		long ende = System.currentTimeMillis();
		info("Duration of scanning all pages in ms : " + (ende-begin));
		info("End of Scan...");
		
		

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
			JOptionPane.showMessageDialog(gui, message);
		else
			System.err.println("ERROR: " + message);
	}


	/**
	 *	 
	 */
	public Job() {
		Base.LOGLEVEL = Base.INFO;
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

	protected String readPageAsString(int pagenumber, int pagesize) {

		return Auxiliary.bytesToHex(readPageWithNumber(pagenumber, pagesize).array());

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
			Auxiliary.printStackTrace();
			
			
			return null;
		}
		db.position((int)offset);
		ByteBuffer page = db.slice();
		page.limit(pagesize);
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
		if (pagenumber*pagesize < 0)
		   System.out.println("Interesting "  +  pagenumber + " " + pagesize);
		return readPageWithOffset(pagenumber*pagesize,pagesize);
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
	private void exploreBTree(int root, AbstractDescriptor td) throws IOException {
		
		// pagesize * (rootindex - 1) -> go to the start of this page
		int offset = ps * (root - 1);

		if(offset <= 0)
			return;
		
		if (root <  0)
		{
			return;
		}
		
		
		ByteBuffer pageType = ByteBuffer.allocate(1);
		// first two bytes of page
		Future<Integer> type = file.read(pageType, offset); 		
		while (!type.isDone()) {
		}

		/* check type of the page by reading the first two bytes */
		int typ = Auxiliary.getPageType(Auxiliary.bytesToHex(pageType.array()));

		/* not supported yet */
		if (typ == 2) 
		{
			debug(" page number" + root + " is a  INDEXINTERIORPAGE.");
			
		}
		/* type is either a data interior page (12) */
		else if (typ == 12) {
			debug("page number " + root + " is a interior data page ");

			ByteBuffer rightChildptr = ByteBuffer.allocate(4);
			
			Future<Integer> child = file.read(rightChildptr, offset + 8); // offset 8-11 of an internal page are used
																		  // for rightmost child page number
			while (!child.isDone()) {
			}
			/* recursive */
			rightChildptr.position(0);
			exploreBTree(rightChildptr.getInt(), td);

			/* now we have to read the cell pointer list with offsets for the other pages */

			/* read the complete internal page into buffer */
			System.out.println(" read internal page at offset " + ((root-1)*ps));
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

			System.out.println(" cell offset start: " + cp);
			System.out.println(" root is " + root + " number of elements: " + e);
			
			if (e > ps)
			{
				System.out.println("Intesting");
			}	
			

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
				exploreBTree(p, td);
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
