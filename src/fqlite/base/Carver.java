package fqlite.base;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.LinkedList;
import fqlite.types.CarverTypes;
import fqlite.util.Auxiliary;
import java.util.LinkedList;
import fqlite.descriptor.TableDescriptor;
import fqlite.pattern.MMode;
import fqlite.pattern.SerialTypeMatcher;
import fqlite.types.CarverTypes;
import fqlite.util.Auxiliary;
import fqlite.util.CarvingResult;
import fqlite.util.Match;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;

/**
 * Use this class to carve for deleted records inside a page.
 * 
 * We can search for header signatures of records either within 
 * the whole page or only in a specific area.
 * 
 * @author pawlaszc
 *
 */
public class Carver extends Base {

	ByteBuffer block;

	BitSet bs;

	String content;

	Job job;

	int pagenumber;

	/**
	 * Constructor.
	 * 
	 * @param job    reference to the calling job object
	 * @param bl         a ByteBuffer representing the binary page content
	 * @param content    the String representation of the page content
	 * @param bs         a BitSet to mark places
	 * @param pagenumber the number of the page within the database.
	 */
	public Carver(Job job, ByteBuffer bl, String content, BitSet bs, int pagenumber) {
		this.job = job;
		block = bl;
		this.bs = bs;
		this.content = content;
		this.pagenumber = pagenumber;
	}

	/**
	 * This method allows to carve for hidden records in the slack space (uncharted
	 * region: a region that does not belong to the header or to the cell content
	 * region).
	 *
	 */

	public int carve(int fromidx, int toidx, SerialTypeMatcher mat, int headertype, TableDescriptor tbd) 
	{
		Auxiliary c = new Auxiliary(job);
	
		switch (headertype) 
		{
			case CarverTypes.NORMAL:
				mat.setMatchingMode(MMode.NORMAL);
				break;
			case CarverTypes.COLUMNSONLY:
				mat.setMatchingMode(MMode.NOHEADER);
				break;
			case CarverTypes.FIRSTCOLUMNMISSING:
				mat.setMatchingMode(MMode.NO1stCOL);
				break;
		}
		

		/* gap is to small for a regular record? */
		if((toidx - fromidx) <= 5)
			return -1;
		
		/* set search region */
		mat.region(fromidx, toidx);

		/* set pattern to search for */
		mat.setPattern(tbd.getHpattern());
		
		//System.out.println("header pattern: " + tbd.getHpattern());

		LinkedList<Match> matches = new LinkedList<Match>();
		
		/* find every match within the given region */
		while (mat.find()){
			
			/* get the hex-presentation of the match */
			String m = mat.group2Hex();
			
			/* skip stupid matches - remember - it is just a heuristic */
			if ((m.length() < 2) || (m.startsWith("00000000")) || (m.startsWith("030000")))
				  continue;
			//System.out.println("Bereich: " + ((pagenumber - 1) * job.ps + fromidx) + " "
			//		+ ((pagenumber - 1) * job.ps + toidx));

			/* get the start indices of the match */
			int from = mat.start();

			/* region visited in a match before */
			if (bs.get(from)) {
				continue;
			}
			
			/* don't regard the first column byte value since it is probably not the first column length */
			//if (headertype == CarverTypes.FIRSTCOLUMNMISSING) {
			//	m = m.substring(2);
			//}

			/* match longer than gap ?-) */
			if ((m.length()/2 + Auxiliary.getPayloadLength(m)) > (toidx - fromidx))
			{
				
				System.out.println(" PayloadLength:: " + Auxiliary.getPayloadLength(m) + " > " + (toidx - fromidx));
				// sometimes a match is too long, i.e. 89|19190704, where the first byte belongs to the
				// length byte of the free block (first column overridden)
				if (m.startsWith("8")) {
					m = m.substring(2);
					mat.start+=1;  
					from+=1;
				}
				if (headertype == CarverTypes.NORMAL || headertype == CarverTypes.COLUMNSONLY)
				{
					// do nothing - maybe an overflow record?
				}
				//else   
				//	continue;
			}
			else{
				int abstand = from - 4;
				if (abstand > 0 && bs.get(from-4)) {
					System.out.println("Abstand kleiner vier zum letzten Matchh: " + m);
					
				}

			}
			
			/*
			 * get the indices of the 1st byte after the header -> this is, where the data
			 * begins
			 */
			int end = mat.end();
			

			if (Match.onlyZeros(m))
			{
				continue;
			}
			
			
			debug("Match (0..NORMAL, 1..NOLENGTH, 2..FIRSTCOLMISSING) : " + headertype);
			debug("Match: " + m + " on pos:" + ((pagenumber - 1) * job.ps + from));
			
	       	
			if (headertype == CarverTypes.NORMAL) {
				if (m.length()>=4)
					mat.fallbackFor1stColumn = m.substring(2,4);
				m = "RI" + m;	      
			}
			else if (headertype == CarverTypes.COLUMNSONLY) {
				if (m.length()>=2)
					mat.fallbackFor1stColumn = m.substring(0,2);
				m = addHeaderByte(m);
			}
			else if (headertype == CarverTypes.FIRSTCOLUMNMISSING) {
				
				/* check first column  - if it is set to type <00> we can leave the value as it is*/
			    // first column type is not <00> -> remove the header column type
				if (tbd.rowid_col == 0)
				{	
					m = "00" + m; 
				}
				else
				{
					if(tbd.serialtypes.get(0).equals("INT"))
					{
						m = "XX" + m;
						System.out.println("XX - column on first place");
					}
					else if(tbd.serialtypes.get(0).equals("REAL"))
					{
						m = "07" + m;
					}	
					else if (tbd.serialtypes.get(0).equals("TEXT"))
					{
						m = "21" + m;
					}
					else if (tbd.serialtypes.get(0).equals("BLOB"))
					{
						m = "20" + m;
					}
					else
						m = "02" + m;
				}
					
				/* note  [headerlength|type 1st col|type 2nd col|...] */
				m = addHeaderByte(m);
			}

			/* add new match to list */
			//System.out.println(" Adding match header " + ((pagenumber - 1) * job.ps + from) +  ".." + ((pagenumber - 1) * job.ps + end));
		
	        if (!Match.onlyZeros(m))
	        {  
				/* add match to list */
				//matches.addFirst(new Match(m,from,end));
				matches.add(new Match(m,from,end));
				
	        }

		} // end-search loop
		
		for (Match e : matches)
		{
			// mark as visited
			bs.set(e.begin,e.end);
		}
		
	   Match[] mm =  matches.toArray(new Match[0]);
	
		
		// take all matches in this region and try to recover those data records
		for (int i = 0; i < mm.length; i++)
		{
			Match e = mm[i];
			Match next = null;
			if (i+1 < mm.length)
			   next = mm[i+1];
				
			if (tbd.rowid_col >= 0)
				e.rowidcolum = tbd.rowid_col;
			try {		
				CarvingResult res = c.readDeletedRecordNew(job, block, bs, e, next, pagenumber, mat.fallbackFor1stColumn);
	
				if (null == res)
					/* something went wrong */
					continue;
				
				LinkedList<String> record = res.record;
	
				// add new line to output
				//if (null != rc) { 
				//	job.ll.add(tbd.tblname + ";" + Global.DELETED_RECORD_IN_PAGE + ";" + rc.toString());
				//}
				
				if (null != record) {
					
					record.add(2,Global.DELETED_RECORD_IN_PAGE);
					record.addFirst(tbd.tblname);
					updateResultSet(record);
				}
				
				
			} catch (Exception err) {
				warning("Could not read record" + err.toString());
			}
			
		}
		
		return 0;
	}
	
	private void updateResultSet(LinkedList<String> line) 
	{
		// entry for table name already exists  
		if (job.resultlist.containsKey(line.getFirst()))
		{
			     ObservableList<LinkedList<String>> tablelist = job.resultlist.get(line.getFirst());
			     if(!tablelist.contains(line))
			    	 tablelist.add(line);  // add row 
		}
		
		// create a new data set since table name occurs for the first time
		else {
		          ObservableList<LinkedList<String>> tablelist = FXCollections.observableArrayList();
				  tablelist.add(line); // add row 
				  job.resultlist.put(line.getFirst(),tablelist);  	
		}
	}
	

	private String addHeaderByte(String s) {
		int hl = (s.length() / 2) + 1;

		String hls = Integer.toHexString(hl); 

		if (hls.length() == 1)
			hls = "0" + hls;

		return hls + s;
	}


}
