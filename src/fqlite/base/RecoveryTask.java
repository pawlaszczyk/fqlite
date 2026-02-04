package fqlite.base;

import java.nio.ByteBuffer;
import java.util.*;

import fqlite.descriptor.AbstractDescriptor;
import fqlite.descriptor.TableDescriptor;
import fqlite.log.AppLog;
import fqlite.pattern.SerialTypeMatcher;
import fqlite.types.CarverTypes;
import fqlite.util.Auxiliary;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;

/**
 * This class represents a recovery task. 
 * Since FQLite supports concurrent search 
 * in databases this class implements the interface Runnable. 
 * 
 * @author pawlaszc
 *
 */
public class RecoveryTask implements Runnable {

	public int pagesize;
	public long offset;
	public ByteBuffer buffer;
	public BitSet visit;
	public static List<AbstractDescriptor> tables = new LinkedList<>();
	public int pagenumber;
    private final Job job;
	private final Auxiliary ct;
    private final StringBuffer firstcol = new StringBuffer();
    private boolean freeList;
    
	/**
	 * Constructor method.
	 * 
	 * @param offset the position to start recovery from
	 * @param pagenumber number of the page in the database
	 * @param pagesize size of the page
	 */
	public RecoveryTask(Auxiliary ct, Job job, long offset, int pagenumber, int pagesize, boolean freeList) {
		
		
		if (job.size < offset)
			System.exit(-1);
		
		this.job = job;
		this.pagesize = pagesize;
		this.offset = offset;
		this.pagenumber = pagenumber;
		this.ct = ct;
		this.freeList = freeList;
		visit = new BitSet(pagesize);
	}
	
	/**
	 * This method is called to recover regular data records from the start of a database page.
	 * 
	 * @return 0 if successful
	 */
	public int recover() {

		boolean withoutROWID = false;
		int fbstart = -1;
		
		try {
			
			AppLog.debug("Offset in recover()::" + offset);
			
			/* read the db page into buffer */
			buffer = job.readDBPageWithOffset(offset, pagesize);
			/* convert byte array into a string representation */
			if (null == buffer)
				return -1;
			String content = Auxiliary.bytesToHex2(buffer);

			// offset 0
			buffer.position(0);

			/* check type of the page by reading the first two bytes */
			int type = Auxiliary.getPageType(Objects.requireNonNull(content));

			/* mark bytes as visited */
			visit.set(0, 2);

			/* first page -> we need to check from offset 100 */
			if(offset == 0 || offset == 100){
				// offset 0
				//buffer.position(100);

				/* check type of the page by reading the first two bytes */
				type = Auxiliary.getPageType(content);

				/* mark bytes as visited */
				visit.set(0,102);

			}
			
			
			/* component page was dropped */

			/*
			 * Tricky thing, since a zero page type has normally two possible reasons: 
			 * 
			 * reason 1:
			 * 
			 * It is a dropped page. 
			 * We have to carve for deleted cells, but without cell pointers, cause this list
			 * is dropped too or is damaged. 
			 * 
			 * reason 2: 
			 * 
			 * It is an overflow page -> skip it!
			 */
			if (type == 0) {
				
				/* if page was dropped - because of a DROP TABLE command - first 8 Bytes are zero-bytes */
				buffer.position(0);
				int checksum = buffer.getInt();
				/* was page dropped? */
				if (checksum == 0)
				{
					/* no overflow page -> carve for data records - we do our best! ;-)*/
					carve(content,null);
				}
				/* Otherwise, it seems to be an overflow page; however, that is not 100% safe. */
				
				/* we have to leave in any case */
				return 0;
			}

			/* skip unknown page types */

			// no leaf page -> skip this page
			if (type < 0) {
				AppLog.debug("No Data page. " + pagenumber);	
				return -1;
			} 
			else if(type == 5){
			    AppLog.debug("Inner Table page (only references no data). Page:" + pagenumber);
			  
			    
			    if(pagenumber == 1)
			    	return 0;
			}
		    else if (type == 10) {
				AppLog.debug("Index leaf page " + pagenumber);	
				// note: WITHOUT ROWID tables are saved here.
				withoutROWID=true;
			} else {
				AppLog.debug("Data page " + pagenumber + " Offset: " + offset);
                 	
			}

			/* regular leaf page with data */
            // hx8 or hxD
			if (type == 8 || type == 13)
			{
               	// offset 1-2 let us find the first free block offset for carving
				byte[] fboffset = new byte[2];
				buffer.position(1);
				buffer.get(fboffset);
			
				// Note: The two-byte integer at offset 1 of the page gives the start of the first free block
				// on the page, or is zero if there are no free blocks.
				// A free block marks an area between 2 normal cells (that was removed, for example)
				// every byte before the cell content region (on offset 5) is no part of the freeblock!!! 
			
				ByteBuffer firstfb = ByteBuffer.wrap(fboffset);
				fbstart = Auxiliary.TwoByteBuffertoInt(firstfb);
			
			}

            int columns = 1;
            boolean firstColumnIsROWID = false;
				
			// found Data-Page - determine number of cell pointers at offset 3-4 of this page
			byte[] cpn = new byte[2];
			buffer.position(3);
			buffer.get(cpn);
			
			// get start pointer for the cell content region
			byte[] ccr = new byte[2];
			buffer.position(5);
			buffer.get(ccr);
			
			/* mark as visited */
			visit.set(2, 8);

			ByteBuffer size = ByteBuffer.wrap(cpn);
			int cp = Auxiliary.TwoByteBuffertoInt(size);

			AppLog.debug(" number of cells: " + cp + " type of page " +  type);
			job.numberofcells.addAndGet(cp);
			if (0 == cp)
				AppLog.debug(" Page seems to be dropped. No cell entries.");

			int pageheaderend = 8 + (cp * 2);
			visit.set(0, pageheaderend);

            /* first, add component name if known */
            if (null != job.pages[pagenumber]) {

                AbstractDescriptor ad = job.pages[pagenumber];
                if (ad instanceof TableDescriptor) {
                    TableDescriptor td = (TableDescriptor) ad;
                    columns = td.numberOfColumns();
                    if (td.ROWID && td.rowidcolumn != null)
                        if (td.rowidcolumn.equals(td.columnnames.get(0))){
                            firstColumnIsROWID = true;
                        }
                }



            }

            //String rc;
            LinkedList<String> record;

            /*
			 * scan of Freeblock list 
			 */
			if (fbstart > pageheaderend  && fbstart <  job.ps) {
			
		    	boolean goon = false;

                do {
			    	// go to next free block
					buffer.position(fbstart);

                    // read the next 2 bytes -> this value represents the length of the
                    // freeblock. Remember: The maximum size of a database page is 64kb (2^16 bytes)
                    int next = Auxiliary.TwoByteBuffertoInt(buffer);

                    // Note: The third and fourth bytes of each freeblock form a big-endian integer, which is the size of the freeblock in bytes.
                    // including the 4-byte header
                    int freeblocksize = Auxiliary.TwoByteBuffertoInt(buffer);
                    freeblocksize-=2;
                    int remaining = freeblocksize;
                    int pos = 0;

					if(freeblocksize <= 0)
                    	break;

					byte[] freeblock = new byte[freeblocksize];

                    // [  PLL |  ROWID | HeadLength | serialtype col1  | serialtype col2 | serialtype col3 ... | Data
                    // all values are varint-values
                    // in some rare cases - when ROWID is 1 byte, PLL is one byte, headerlength is 1 byte -> the serialtype info for the first column is overwritten
                    // during the removal of the record

                    try {

                        buffer.get(freeblock);

                        boolean go = false;

                        // slice from index 0 to index number of columns - 1 - since 1st column is probably overwritten
                        byte[] slice = Arrays.copyOfRange(freeblock, 0, (columns - 1));
                        int data_length = Auxiliary.computePayloadLengthByte(slice, 0);

                        leave:
                        do {
                            boolean addfirstcolumn = true;

                            if (!firstColumnIsROWID)
                                break leave;

                            // Github issue number #19:
                            if (firstColumnIsROWID && freeblock[0] == 0){
                                addfirstcolumn = false;
                            }

                                remaining = remaining - 4 - data_length;

                                // reconstruct the original header with the missing 1st column
                                ByteBuffer recover = ByteBuffer.allocate(freeblock.length + 6);


                                recover.put(pos + 0, (byte) (data_length + 4)); // total length byte(s)
                                recover.put(pos + 1, (byte) 00); // rowid
                                recover.put(pos + 2, (byte) (columns + 1));  // header length
                                // only add the serial type 00 for the first column if the table is a ROWID table in the
                                // 1st column and the serial byte of colum 1 is wiped
                                if (addfirstcolumn)
                                    recover.put(pos + 3, (byte) 0);  // first column is 00
                                recover.put(pos + 4, freeblock); // the freeblock


                                record = ct.readRecord(pos, recover, pagenumber, null, Integer.MAX_VALUE, withoutROWID, Global.REGULAR_DB_FILE, -1);

                                // update status column -> this is a dropped record
                                record.set(3, Global.DELETED_RECORD_IN_PAGE);

                                // compute the correct offset for this match
                                long match_offset = +(pagenumber - 1) * pagesize + fbstart;
                                record.set(4, "" + match_offset);

                                /* add record to result set */
                                if (!record.isEmpty()) {
                                    visit.set(fbstart,fbstart + 4 + data_length);
                                    updateResultSet(record);
                                }

                                /* only if there are at least 6 bytes remain -> go for an additional round */
                                if (remaining > 5) {
                                    // Is there another removed record in the remaining bytes?
                                    go = true;
                                    byte[] rm = new byte[remaining];
                                    next = Auxiliary.TwoByteBuffertoInt(recover);
                                    int sz = Auxiliary.TwoByteBuffertoInt(recover);
                                    //recover.position(recover.position()+4);
                                    recover.get(rm, 0, remaining - 4);
                                    ByteBuffer rest = ByteBuffer.wrap(rm);
                                    data_length = Auxiliary.computePayloadLengthByte(slice, 0);
                                    freeblock = rm;
                                } else
                                    go = false;

                        }
                        while (go) ;

                    }catch(Exception error){
                        goon = false;
                        continue;
                    }

                    if (next > 0 && next < job.ps) {
                        fbstart = next;
                        goon = true;
                    } else
                        goon = false;

				}
                while(goon);


			}
			
				
			
			/*
			 * STEP 2:
			 * 
			 * Cell pointer array scan (if possible)
			 * 
			 */
			int last = 0;

			/* go on with the cell pointer array */
			for (int i = 0; i < cp; i++) {

				// address of the next cell pointer
				byte[] pointer = new byte[2];
				if (type == 5)
					buffer.position(12+2*i);
				else
					buffer.position(8+2*i);
				buffer.get(pointer);
				ByteBuffer celladdr = ByteBuffer.wrap(pointer);
				int celloff = Auxiliary.TwoByteBuffertoInt(celladdr);

				if (offset == 100){
					celloff-=100;
				}
				
				if (last > 0) {
					if (celloff == last) {
						continue;
					}
				}
				last = celloff;
				
				String hls = Auxiliary.Int2Hex(celloff); // Integer.toHexString(celloff);
				AppLog.debug(pagenumber + " -> " + celloff + " " + "0" + hls);
				
					

				record = ct.readRecord(celloff, buffer, pagenumber, visit, Integer.MAX_VALUE, withoutROWID,Global.REGULAR_DB_FILE,-1);
				
				
				// add new line to output
				if (null != record && record.size() > 0) {
					
					int p;
					
					// check for fts3/4 tables

					if ((p = record.getFirst().indexOf("_content")) > 0)
					{	
						String rc = record.getFirst();
						String tbln = rc.substring(0, p);
						if (job.virtualTables.containsKey(tbln))
						{
							TableDescriptor tds = job.virtualTables.get(tbln);
							if (tds.modulname.equals("fts4")|| tds.modulname.equals("fts3"))
							{
								// take the columns and create a record for the virtual table
                            	LinkedList<String> ftsrecord = new LinkedList<>();

                            	ftsrecord.add(tbln + "");  // start a new row for the virtual component 
                            	ftsrecord.add("");
                            	ftsrecord.add("");
                            	ftsrecord.add("");
                            	ftsrecord.add("");
                                
                            	/**
                            	 * The leftmost column of the "%_content" table is an INTEGER PRIMARY KEY 
                            	 * field named "docid". Following this is one column for each column of 
                            	 * the FTS virtual table as declared by the user, named by prepending the 
                            	 * column name supplied by the user with "cN", where N is the index of the 
                            	 * column within the table, numbered from left to right starting with 0. 
                            	 * Data types supplied as part of the virtual table declaration are not 
                            	 * used as part of the %_content table declaration.
                            	 */
								for(int ii = 6; ii < record.size(); ii++) {
									
									ftsrecord.add(record.get(ii));
								}
								
	                            updateResultSet(ftsrecord);

							}
						}
					}

					// rtree virtual table
					if ((p = record.getFirst().indexOf("_node")) > 0)
					{
						String rc = record.getFirst();
						String tbln = rc.substring(0, p);
						if (job.virtualTables.containsKey(tbln))
						{
							TableDescriptor tds = job.virtualTables.get(tbln);
							
							/* we use the xxx_node shadow component to construct the 
							 * virtual component
							 * skip the first information -> go directly to the 5th element
							 * of the data record line, i.e. go to the BLOB with the row data
							 */
							
				            String data = record.get(6);  //rc.substring(pp+1);
						    int endofprefix = data.indexOf("] ");
				            if (endofprefix > 0)
				            	data = data.substring(endofprefix+2);
				            System.out.println("data length " + data.length());
				            
							/* transform String data into a byte array */
							byte[] binary = Auxiliary.decode(data);
							ByteBuffer bf = ByteBuffer.wrap(binary);
							
							bf.rewind();
							
							/* skip the first two bytes */
							bf.getShort();
							/* first get the total number of entries for this rtree branch */
                            int entries = bf.getShort();
					
                            System.out.println("Virtual Table "+ tbln + " entries ::" + entries);  
                            
                            /* create a new line for every data row */ 
                            while(entries>0)
                            {
                            	LinkedList<String> rtreerecord = new LinkedList<>();
                    			
                            	rtreerecord.add(tbln + "");  // start a new row for the virtual component 
                            	rtreerecord.add("");
                            	rtreerecord.add("");
                            	rtreerecord.add("");
                            	rtreerecord.add("");
                            	
                            	// The first column is always a 64-bit signed integer primary key.
                            	try {
                            		long primarykey = bf.getLong();
                            		rtreerecord.add(String.valueOf(primarykey));
                            	}
                            	catch(Exception err){
                            		rtreerecord.add("null");
                            	}
                            	
                            	//Each R*Tree index is a virtual component with an odd number of columns between 3 and 11
                            	//The other columns are pairs, one pair per dimension, containing the minimum and maximum values for that dimension, respectively.
                            	int number = tds.columnnames.size() - 1;

                            	while (number > 0)
                            	{	
                            		try {
                            			if (bf.limit() - bf.position() >= 4)
                            			{
                            				float rv = bf.getFloat();
                            				rtreerecord.add(String.valueOf(rv));
                                			
                            			}
                            			number--;
                            		}catch(Exception err){
                            			System.out.println(" Fehler " + number);
                            		}
                            	}
                            	
	                            entries--;
	                            updateResultSet(rtreerecord);
	                           
	                            
                            }	
							
							
						}
						
					}
					
					/* if record resides inside a free page -> add a flag char to document this */
					if(freeList)
					{  
					   String secondcol = record.get(3);
					   secondcol = Global.FREELIST_ENTRY + secondcol;
					   record.set(3, secondcol);
					}
					updateResultSet(record);
				}

			} // end of for - cell pointer

			
			
	
			
			
			
			/*
			 * STEP 3:

			 * Scan unallocated space between the header and  the cell
			 * content region 
			 * 
			 */
			
			/* before we go to the free blocks and gaps, let us first check the area between the header and
			   the start byte of the cell content region */
			
			buffer.position(pageheaderend);
			

			/*
			 * STEP 4:
			 * If there are still gaps, go for it and carve it
			 * 
	         */
			
			
			if(job.pages[pagenumber]!=null && job.pages[pagenumber].getName().equals("sqlite_master"))
				return 0;
						
			if(offset != 100)
				carve(content,null);
			
		} catch (Exception err) {
			err.printStackTrace();
			return -1;
		}

		return 0;
	}
	
	
	
	
	private void updateResultSet(LinkedList<String> line) 
	{
		// entry for table name already exists  
		if (job.resultlist.containsKey(line.getFirst()))
		{
			     ObservableList<ObservableList<String>> tablelist = job.resultlist.get(line.getFirst());
			     tablelist.add(FXCollections.observableList(line));  // add row 
		}
		
		// create a new data set since the table name occurs for the first time
		else {
		          ObservableList<ObservableList<String>> tablelist = FXCollections.observableArrayList();
				  tablelist.add(FXCollections.observableList(line));  // add row 
				  job.resultlist.put(line.getFirst(),tablelist);  	
		}
	}

	/**
	 * Quick lookup. Does a given hex-string start with Zeros?
	 * @param s the String to check
	 * @return true if zero bytes could be found
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
	 * Check the BitSet for gaps, i.e. regions we still have to carve.
	 * 
	 * @return list with gaps inside.
	 */
	public LinkedList<Gap> findGaps() {
		LinkedList <Gap> gaps = new LinkedList<Gap>();

		int from = 0;

		/* Are there any regions left in the page? */
		for (int i = 0; i < pagesize; i++) {

			if (!visit.get(i)) {
				from = i;

				int to = i;

				while (!visit.get(++i) && i < (pagesize - 1)) {
					to++;
				}

				if (to - from >= 4) {

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
						Gap g = new Gap(from, to);
						if (!gaps.contains(g))
						AppLog.debug("ohne match : " + (job.ps * (pagenumber - 1) + from) + " - "
								+ (job.ps * (pagenumber - 1) + to) + " Bytes");
						gaps.add(g);
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
	 * @param content page content as a hex-string
	 */
	public void carve(String content, Carver crv) {

		/* if file is bigger than 5 MB skip intense scan */
		//if(job.size > 5242880)  //1024*1024)// 5242880)
		//	return;
		
		Carver c = crv;
		
		if (null == c)
			/* no type could be found in the first two bytes */
			/* Maybe the whole page was dropped because of a drop component command? */
			/* start carving on the complete page */
			c = new Carver(job, buffer, content, visit, pagenumber);


		/* try to get component schema for the current page, if possible */
		AbstractDescriptor tdesc = null;
		if (job.pages.length > pagenumber) {
            AbstractDescriptor ad = job.pages[pagenumber];
            if (ad instanceof TableDescriptor) {
                tdesc = (TableDescriptor) ad;
            }
        }
		List<AbstractDescriptor> tab = new ArrayList<AbstractDescriptor>();//tables;
		AppLog.debug(" tables :: " + tables.size());

		if (null != tdesc) {
			/* there is a schema for this page */
			tab = new LinkedList<>();
			tab.add(tdesc);
			AppLog.debug(" added tdsec ");													
		} else {
			AppLog.debug(" No component description!");
			tab = tables;
		}
		
			
		List<Gap> gaps = findGaps();

		if (gaps.size() == 0)
		{
			AppLog.debug("no gaps anymore. Stopp search");
			return;
		}	
		
		/*
		 * When a record deletion occurs, the first 2 bytes of the cell are set to the
		 * offset value of next free block and latter 2 bytes covers the length of the
		 * current free block. Because of this, the first 4 bytes of a deleted cell
		 * differ startRegion from the normal data. Accordingly, we need a different approach to
		 * Recover the data records.
		 * In most cases, at least the header length information is overwritten. Beyond
		 * This sometimes also overwrites the first column type field.
		 * We have two cases:
		 * (1) only the first column of the header is missing, but the rest of the
		 * header is intact.
		 * (2) Both the header length field plus the first column are overwritten.
		 * [cell size | rowid | header size | header bytes | payload ]
		 * for a deleted cell looks maybe like this
		 * [offset of next free block | length of the current free block | ]
		 */

			
		/* try out all component schema(s) */
		for(int hh = 0; hh<3; hh++) {
	 			
			for (int n = 0; n < tab.size(); n++) {
				tdesc = tab.get(n);
								
				if(tdesc != null && tdesc.serialtypes != null && tdesc.serialtypes.size()>0 && tdesc.serialtypes.get(0).equals("BLOB"))
					continue;
				
				if(tdesc != null && tdesc.serialtypes != null){
					if (tdesc.serialtypes.size()>1 && tdesc.serialtypes.size()<4){
						if (Objects.equals(tdesc.serialtypes.get(1), "BLOB") || Objects.equals(tdesc.serialtypes.get(1), "TEXT")){
							continue;
						}
					}
					
				}
				
				AppLog.debug("pagenumber :: " + pagenumber + " component size :: " + tab.size());
				AppLog.debug("n " + n);
				
				
				if( pagenumber == 18 && tab.size() == 78 && n == 52){
					System.out.println("Stop here.");
				}
							
				/* access pattern for a particular component */
				String tablename = tab.get(n).getName();
				if (tablename.startsWith("fqlite_freelist"))
					continue;
				/* create matcher object for constraint check */
				SerialTypeMatcher stm = new SerialTypeMatcher(buffer);
	
				if (hh==0) {
					gaps = findGaps();

                    for (Gap next: gaps) {

                        if (next.to - next.from > 5)
                            /* do we have at least one match? */
                            if (c.carve(next.from, next.to, stm, CarverTypes.NORMAL, tab.get(n)) != Global.CARVING_ERROR) {
                                AppLog.debug("*****************************  STEP NORMAL finished with matches");

                            }
                    }
		    	}	
				
				if (hh==1) {
					gaps = findGaps();

                    for (Gap next: gaps) {


                        if (c.carve(next.from, next.to, stm, CarverTypes.COLUMNSONLY, tab.get(n)) != Global.CARVING_ERROR) {
                            AppLog.debug("*****************************  STEP COLUMNSONLY finished with matches");

                        }
                    }
				}
				
				if (hh==2) {
					
					gaps = findGaps();


                    for (Gap next: gaps) {

                        if (c.carve(next.from, next.to, stm, CarverTypes.FIRSTCOLUMNMISSING, tab.get(n)) != Global.CARVING_ERROR) {
                            AppLog.debug("RecoveryTask *****************************  STEP FIRSTCOLUMNMISSING finished with matches");

                        }


                    }
                  

				}
				
	
			} // end of tables ( component fingerprint )
		}
	}
	
	

	@Override
	public void run() {
		
		try
		{
			recover();
		}
		catch(Exception err)
		{
			System.err.println(err);
		}
		finally
		{
			/* if task has finished, decrement this counter to inform the main-thread */
			job.runningTasks.decrementAndGet();
		}
		
		
	}

    

	


}




