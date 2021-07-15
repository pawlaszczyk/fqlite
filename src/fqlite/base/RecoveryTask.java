package fqlite.base;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

import fqlite.descriptor.AbstractDescriptor;
import fqlite.descriptor.TableDescriptor;
import fqlite.pattern.SerialTypeMatcher;
import fqlite.types.CarverTypes;
import fqlite.util.Auxiliary;
import fqlite.util.Logger;

/**
 * This class represents a recovery task. 
 * Since FQLite supports concurrent search 
 * in databases this class implements the interface Runnable. 
 * 
 * @author pawlaszc
 *
 */
public class RecoveryTask extends Base implements Runnable {

	public int pagesize;
	public long offset;
	public ByteBuffer buffer;
	public BitSet visit;
	public static List<TableDescriptor> tables = new LinkedList<TableDescriptor>();
	public int pagenumber;
    private Job job;
	private Auxiliary ct;
    private StringBuffer firstcol = new StringBuffer(); 
    private boolean freeList = false;
    
	/**
	 * Constructor method.
	 * 
	 * @param offset
	 * @param pagenumber
	 * @param pagesize
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
	 * This method called to recover regular data records startRegion a database page.
	 * 
	 * @return
	 */
	public int recover() {

		boolean withoutROWID = false;
		
		try {
			
			debug("Offset in recover()::" + offset);
			/* read the db page into buffer */
			buffer = job.readPageWithOffset(offset, pagesize);
			/* convert byte array into a string representation */
			String content = Auxiliary.bytesToHex(buffer);

			// offset 0
			buffer.position(0);

			/* check type of the page by reading the first two bytes */
			int type = Auxiliary.getPageType(content);

			/* mark bytes as visited */
			visit.set(0, 2);

			/************** component page was dropped ******************/

			/*
			 * Tricky thing, since a zero page type has normally two possible reasons: 
			 * 
			 * reason 1:
			 * 
			 * It is a dropped page. 
			 * We have to carve for deleted cells but without cell pointers, cause this list
			 * is dropped too or is damaged. 
			 * 
			 * reason 2: 
			 * 
			 * It is an overflow page -> skip it!
			 */
			if (type == 0) {
				
				/* if page was dropped - because of a DROP TABLE command - first 8 Bytes are zero-bytes */
				buffer.position(0);
				Integer checksum = buffer.getInt();
				/* was page dropped ? */
				if (checksum == 0)
				{
					System.out.println(" DROPPED PAGE !!!");
					/* no overflow page -> carve for data records - we do our best! ;-)*/
					carve(content,null);
				}
				/* otherwise it seems to be a overflow page - however, that is not 100% save !!! */
				
				/* we have to leave in any case */
				return 0;
			}

			/************** skip unkown page types ******************/

			// no leaf page -> skip this page
			if (type < 0) {
				info("No Data page. " + pagenumber);
				return -1;
			} else if (type == 12) {
				info("Internal Table page " + pagenumber);
				return -1;
			} else if (type == 10) {
				info("Index leaf page " + pagenumber);	
				// note: WITHOUT ROWID tables are saved here.
				withoutROWID=true;
			} else {
				info("Data page " + pagenumber + " Offset: " + offset);

			}

			/************** regular leaf page with data ******************/

			//boolean freeblocks = false;
			if (type == 8)
			{
				// offset 1-2 let us find the first free block offset for carving
				byte fboffset[] = new byte[2];
				buffer.position(1);
				buffer.get(fboffset);
				//ByteBuffer fboff = ByteBuffer.wrap(fboffset);
				//int ffb = Auxiliary.TwoByteBuffertoInt(fboff);

				// Note: The two-byte integer at offset 1 of the page gives the start of the first freeblock 
				// on the page, or is zero if there are no freeblocks.
				//if (ffb > 0)
				//	freeblocks = true;  
				// A freeblock marks an area between 2 normal cells (that was removed for example)
				// every byte before the cell content region (on offset 5) is no part of the freeblock!!! 
			}	
				
			int ccrstart = job.ps;

			// found Data-Page - determine number of cell pointers at offset 3-4 of this page
			byte cpn[] = new byte[2];
			buffer.position(3);
			buffer.get(cpn);
			
			// get start pointer for the cell content region
			byte ccr[] = new byte[2];
			buffer.position(5);
			buffer.get(ccr);
			
			ByteBuffer contentregionstart = ByteBuffer.wrap(ccr);
			ccrstart = Auxiliary.TwoByteBuffertoInt(contentregionstart);
			
			/* mark as visited */
			visit.set(2, 8);

			ByteBuffer size = ByteBuffer.wrap(cpn);
			int cp = Auxiliary.TwoByteBuffertoInt(size);

			debug(" number of cells: " + cp + " type of page " +  type);
			job.numberofcells.addAndGet(cp);
			if (0 == cp)
				debug(" Page seems to be dropped. No cell entries.");

			int headerend = 8 + (cp * 2);
			visit.set(0, headerend);
			//System.out.println("headerend:" + headerend);

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
					buffer.position(12+2*i);
				else
					buffer.position(8+2*i);
				buffer.get(pointer);
				ByteBuffer celladdr = ByteBuffer.wrap(pointer);
				int celloff = Auxiliary.TwoByteBuffertoInt(celladdr);

				if (last > 0) {
					if (celloff == last) {
						continue;
					}
				}
				last = celloff;
				//if (Logger.LOGLEVEL == Logger.DEBUG)
				//{	
					String hls = Auxiliary.Int2Hex(celloff); // Integer.toHexString(celloff);
					Logger.out.debug(pagenumber + " -> " + celloff + " " + "0" + hls);
				//}
					
				String rc;
				
				rc = ct.readRecord(celloff, buffer, pagenumber, visit, type, Integer.MAX_VALUE, firstcol,withoutROWID,-1);
								
				// add new line to output
				if (null != rc && rc.length() > 0) {
					
					int p;
					if ((p = rc.indexOf("_node;")) > 0)
					{
						String tbln = rc.substring(0, p);
						
						if (job.virtualTables.containsKey(tbln))
						{
							TableDescriptor tds = job.virtualTables.get(tbln);
							
							/* we use the xxx_node shadow component to construct the 
							 * virtual component
							 */
							String BLOB = rc.substring(p);
							//System.out.println(BLOB);
							
							/* skip the first information -> go directly to the 5th element
							 * of the data record line, i.e. go to the BLOB with the row data
							 */
							int pp = Auxiliary.findNthOccur(rc, ';', 4);
							String data = rc.substring(pp+1);
						
							/* transform String data into an byte array */
							byte[] binary = Auxiliary.decode(data);
							ByteBuffer bf = ByteBuffer.wrap(binary);
                            
							/* skip the first to bytes */
							bf.getShort();
							/* first get the total number of entries for this rtree branch */
                            int entries = bf.getShort();
					
                            /* create a new line for every data row */ 
                            while(entries>0)
                            {
                        		StringBuffer vrow = new StringBuffer();
    							vrow.append(tbln + ";VT;0;");  // start a new row for the virtual component 
    					
                            	// The first column is always a 64-bit signed integer primary key.
                            	long primarykey = bf.getLong();
                            	vrow.append(primarykey+";");
                            	
                            	//Each R*Tree indices is a virtual component with an odd number of columns between 3 and 11
                            	//The other columns are pairs, one pair per dimension, containing the minimum and maximum values for that dimension, respectively.
                            	int number = tds.columnnames.size() - 1;
                            	
                            	while (number > 0)
                            	{	
	                            	float rv = bf.getFloat();
	                            	vrow.append(rv + ";");
	                            	number--;
                            	}
                            	
	                            	
	                            vrow.append("\n");
                            	job.ll.add(vrow.toString());

    							System.out.println(vrow);
    							
    							entries--;
	                            
                            }	
							
							
						}
						
					}
					
					/* if record resides inside a free page -> add a flag char to document this */
					if(freeList)
					{    
					   int idx = rc.indexOf(";");
					   rc = rc.substring(0, idx) + ";"+ Global.FREELIST_ENTRY + rc.substring(idx+1);
					   
					}
					job.ll.add(rc);
				}

			} // end of for - cell pointer

			
			/***************************************************************
			 * STEP 3:
			 * 
			 * Scan unallocated space between header and  the cell
			 * content region 
			 * 
			 ***************************************************************/
			
			/* before we go to the free blocks an gaps let us first check the area between the header and 
			   the start byte of the cell content region */
			
			buffer.position(headerend);
			
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
			} while (buffer.position() < pagesize && garbageoffset > 0);
			
			
			/*  Now, skip all zeros - no information to recover just empty space */
			byte zerob = 0;
			while(buffer.position() < pagesize && zerob == 0)
			{
				zerob = buffer.get();
			}
			
			/* mark the region startRegion the end of page header till end of zero space as visited */
			visit.set(headerend,buffer.position());
			
			/* go back one byte */
			buffer.position(buffer.position()-1);
		
			//System.out.println("First none-zero Byte " + zerob);
			
			//System.out.println("Cell Content Region start offset : " + ccrstart);
			//System.out.println("First none zero byte in unallocated space : " + buffer.position());
			
			/* only if there is a significant number of bytes in the unallocated area, evaluate it more closely. */
			if (ccrstart - buffer.position() > 3)
			{
				/* try to read record as usual */
				String rc;
				
				/* Tricky thing : data record could be partly overwritten with a new data record!!!  */
				/* We should read until the end of the unallocated area and not above! */
				rc = ct.readRecord(buffer.position(), buffer, pagenumber, visit, type, ccrstart - buffer.position(),firstcol,withoutROWID,-1);
				
				// add new line to output
				if (null != rc) { // && rc.length() > 0) {
					
					int idx = rc.indexOf(";");
					rc = rc.substring(0, idx) + ";" + Global.DELETED_RECORD_IN_PAGE  + rc.substring(idx+1);
					   					
					
					//if (job.doublicates.add(rc.hashCode()))
					job.ll.add(rc);
				}
				
			}
			
			
			/***************************************************************
			 * STEP 4:
			 * 
			 * if there are still gaps, go for it and carve it  
			 * 
			 ***************************************************************/
			
			/* now we are ready to carve the rest of the page */
			carve(content,null);
			
		} catch (Exception err) {
			err.printStackTrace();
			return -1;
		}

		return 0;
	}

	/**
	 * Quick lookup. Does a given hex-String starts with Zeros?
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
	 * Check the BitSet for gaps, i.e. regions we still have to carve.
	 * 
	 * @return
	 */
	public LinkedList<Gap> findGaps() {
		LinkedList <Gap> gaps = new LinkedList<Gap>();

		int from = 0;

		/* are there any regions left in the page ? */
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
						debug("ohne match : " + (job.ps * (pagenumber - 1) + from) + " - "
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
	 * @param content page content as hex-string
	 */
	public void carve(String content, Carver crv) {

		Carver c = crv;
		
		if (null == c)
			/* no type could be found in the first two bytes */
			/* Maybe the whole page was drop because of a drop component command ? */
			/* start carving on the complete page */
			c = new Carver(job, buffer, content, visit, pagenumber);

		//Matcher mat = null;
		// boolean match = false;

		/* try to get component schema for the current page, if possible */
		TableDescriptor tdesc = null;
		if (job.pages.length > pagenumber)
		{
			AbstractDescriptor ad = job.pages[pagenumber]; 
			if (ad instanceof TableDescriptor)
					tdesc = (TableDescriptor)ad;
		}
			
		List<TableDescriptor> tab = tables;
		debug(" tables :: " + tables.size());

		if (null != tdesc) {
			/* there is a schema for this page */
			tab = new LinkedList<TableDescriptor>();
			tab.add(tdesc);
			debug(" added tdsec ");
		} else {
			warning(" No component description!");
			tab = tables;
		}
		
		List<Gap> gaps = findGaps();

		System.out.println("gaps.size()" + gaps.size());
		if (gaps.size() == 0)
		{
			debug("no gaps anymore. Stopp search");
			return;
		}	
		
		/* try out all component schema(s) */
		for (int n = 0; n < tab.size(); n++) {
			tdesc = tab.get(n);
			debug("pagenumber :: " + pagenumber + " component size :: " + tab.size());
			debug("n " + n);
			//TableDescriptor tdb = tab.get(n);
		
			/* access pattern for a particular component */
			String tablename = tab.get(n).tblname;
			if (tablename.startsWith("__UNASSIGNED"))
				continue;
			/* create matcher object for constrain check */
			SerialTypeMatcher stm = new SerialTypeMatcher(buffer);

			gaps = findGaps();
			
			for (int a = 0; a < gaps.size(); a++) {
			
				Gap next = gaps.get(a);

				
				if (next.to - next.from > 5)
					/* do we have at least one match ? */
					if (c.carve(next.from+4,next.to, stm, CarverTypes.NORMAL, tab.get(n),firstcol)) {
						debug("*****************************  STEP NORMAL finished with matches");
						
					}
			}
				
			gaps = findGaps();
			
			for (int a = 0; a < gaps.size(); a++) {
				
				Gap next = gaps.get(a);
				
 				if (c.carve(next.from+4,next.to, stm, CarverTypes.COLUMNSONLY, tab.get(n),firstcol)) {
					debug("*****************************  STEP COLUMNSONLY finished with matches");
					
				}
			}
			
			gaps = findGaps();
			
			
			for (int a = 0; a < gaps.size(); a++) {
				
				Gap next = gaps.get(a);
				
				
				if (c.carve(next.from+4,next.to, stm, CarverTypes.FIRSTCOLUMNMISSING, tab.get(n),firstcol)) {
					debug("*****************************  STEP FIRSTCOLUMNMISSING finished with matches");
					
				}
			
			}
			
			
		
			/**
			 * When a record deletion occurs, the first 2 bytes of the cell are set to the
			 * offset value of next free block and latter 2 bytes covers the length of the
			 * current free block. Because of this, the first 4 bytes of a deleted cell
			 * differ startRegion the normal data. Accordingly, we need a different approach to
			 * recover the data records.
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
				c.carve(next.from+4+1,next.to, stm, CarverTypes.FIRSTCOLUMNMISSING, tab.get(n),firstcol); 
				
			}
			

		} // end of tables ( component fingerprint )

	}

	@Override
	public void run() {
		
		try
		{
			recover();
			/* if task has finished, decrement this counter to inform the main-thread */
			//System.out.println("task finished" );
		}
		catch(Exception err)
		{
			System.err.println(err.toString());
		}
		finally
		{
			//System.out.println("task finally" );
			job.runningTasks.decrementAndGet();
		}
		
		
	}

    

	


}




