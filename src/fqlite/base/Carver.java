package fqlite.base;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.LinkedList;

import fqlite.descriptor.AbstractDescriptor;
import fqlite.log.AppLog;
import fqlite.pattern.HeaderPattern;
import fqlite.pattern.MMode;
import fqlite.pattern.SerialTypeMatcher;
import fqlite.types.CarverTypes;
import fqlite.util.Auxiliary;
import fqlite.util.CarvingResult;
import fqlite.util.Match;
import fqlite.util.SQLiteRecovery;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import org.jspecify.annotations.NonNull;

/**
 * Use this class to carve for deleted records inside a page.
 * 
 * We can search for header signatures of records either within 
 * the whole page or only in a specific area.
 * 
 * @author pawlaszc
 *
 */
public class Carver{

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
	 * This method allows carving for hidden records in the slack space (uncharted
	 * region: a region that does not belong to the header or to the cell content
	 * region).
	 *
	 */

	public int carve(int fromidx, int toidx, SerialTypeMatcher mat, int headertype, AbstractDescriptor tbd) 
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
		
		/* gap is too small for a regular record? */
		if((toidx - fromidx) <= 5)
			return -1;

		/* search for a match within a two-column table (mostly index tables) where the first column is missing -> skip */
		if(tbd.serialtypes.size() <= 2 && headertype != CarverTypes.NORMAL)
			return -1;

		/* set search region */
		mat.region(fromidx, toidx);

		/* set pattern to search for */
		HeaderPattern pattern = tbd.getHpattern();

		mat.setPattern(tbd.getHpattern());
		
		LinkedList<Match> matches = new LinkedList<>();
		
		/* find every match within the given region */
		while (mat.find()){

			/* get the hex-presentation of the match */
			String m = mat.group2Hex();
			char[] cmatch = m.toCharArray();

			if (headertype == CarverTypes.COLUMNSONLY) {

				mat.start +=2;
				cmatch = m.toCharArray();

			}
			if (headertype == CarverTypes.FIRSTCOLUMNMISSING){

				if(tbd.serialtypes.get(0)=="INT") {

					String firstcol = m.substring(0,2);

					byte[] bmatch = Auxiliary.hexStringToByteArray(m);
					int[] first = Auxiliary.readVarInt(bmatch);

					int ff = first[0];
					if(ff > job.ps)
					{
						cmatch = m.substring(2).toCharArray();
						m = m.substring(2);
						mat.start += 2;
					}


				}
			}

			/* skip stupid matches - remember - it is just a heuristic */
			if ((m.length() < 2) || (m.startsWith("00")))	
			{	
				continue;
			}
			/* skip match xxxxxx0000 */
			if ((m.length() >= 6) && m.endsWith("0000"))
				  continue;
		
			/* if there are three zero bytes or even more inside the match -> skip */
			if (cmatch.length >= 10){
				int nullbytes = 0;
				for (int t = 0; t < cmatch.length; t+=2){
					if(t > 0)
					{
						if(cmatch[t]== '0' && cmatch[t-1] == '0') 
						{
							nullbytes++;
						}
					}	
				}
				if(nullbytes >=3){
					return 0;
				}								
			}

			/* get the start indices of the match */
			int from = mat.start();

			/* region visited in a match before */
			if (bs.get(from)) {
				continue;
			}
			
			/* match longer than gap */
			if ((m.length()/2 + Auxiliary.getPayloadLength(m)) > (toidx - fromidx))
			{
				
				// sometimes a match is too long, i.e. 89|19190704, where the first byte belongs to the
				// length byte of the free block (first column overridden)
				if (m.startsWith("8")) {
					m = m.substring(2);
					mat.start+=1;  
					from+=1;
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
			
			AppLog.debug("Match (0..NORMAL, 1..NOLENGTH, 2..FIRSTCOLMISSING) : " + headertype);
			AppLog.debug("Match: " + m + " on pos:" + ((pagenumber - 1) * job.ps + from));

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

	        if (!Match.onlyZeros(m))
	        {  
				/* add match to list */
				matches.add(new Match(m,from,end));
				
	        }

		} // end-search loop
		
		for (Match e: matches)
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
				CarvingResult res = c.readDeletedRecord(tbd, job, block, bs, e, next, pagenumber, mat.fallbackFor1stColumn);
				if (null == res)
					/* something went wrong */
					continue;

					// test if recovered data record has a appropriate length
                    // accept table even if 1 column is missing
				if (res.record.size() < tbd.columnnames.size() + 2)
					continue;

				LinkedList<String> record = res.record;
				LinkedList<byte[]> raw = res.hexdump;

				if (null != record) {

					int empty_columns = 0;
					for (String key : record) {
						if (key == null || key.length() == 0)
							empty_columns++;
					}
					if (record.size() - empty_columns < 2)
						continue;

					record.add(2,Global.DELETED_RECORD_IN_PAGE);
					record.addFirst(tbd.getName());
					raw.add(2,null);
					raw.addFirst(null);
					updateResultSet(record,raw);
				}
				
				
			} catch (IllegalArgumentException | BufferUnderflowException ex) {
				AppLog.debug("Could not read record (buffer bounds): " + ex.getClass().getSimpleName() + " – " + ex.getMessage());
			} catch (Exception err) {
				AppLog.debug("Could not read record: " + err.getClass().getName() + " – " + err.getMessage());
			}
			
		}
		return 0;
	}
	
	@SuppressWarnings("unlikely-arg-type")
	private void updateResultSet(LinkedList<String> line, LinkedList<byte[]> hex)
	{
		// entry for table name already exists
		if (job.resultlist.containsKey(line.getFirst())) {
			ObservableList<ObservableList<String>> tablelist = job.resultlist.get(line.getFirst());

				tablelist.add(FXCollections.observableList(line)); // add row

				// save the original hex-values of a table row separately to a different list
				ObservableList<ObservableList<byte[]>> hexlist = job.hexdumplist.get(line.getFirst());
				hexlist.add(FXCollections.observableList(hex));

		}
		// create a new data set since the table name occurs for the first time
		else {
		          ObservableList<ObservableList<String>> tablelist = FXCollections.observableArrayList();
				  tablelist.add(FXCollections.observableList(line)); // add row 
				  job.resultlist.put(line.getFirst(),tablelist);

				  ObservableList<ObservableList<byte[]>> hexlist = FXCollections.observableArrayList();
				  hexlist.add(FXCollections.observableList(hex));
				  job.hexdumplist.put(line.getFirst(),hexlist);

		}
	}



	private @NonNull String addHeaderByte(String s) {
		int hl = (s.length() / 2) + 1;

		String hls = Integer.toHexString(hl); 

		if (hls.length() == 1)
			hls = "0" + hls;

		return hls + s;
	}


}
