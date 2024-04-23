package fqlite.util;

import java.util.LinkedList;

/**
 * Container class. It is used to return some result from 
 * carving back to the calling thread. 
 * 
 * @author pawlaszc
 *
 */
public class CarvingResult {

	public StringBuffer bf;
	public int rcursor;
	public long offset; 
	public LinkedList<String> record;
	
	public CarvingResult(int rcursor,long offset, StringBuffer result, LinkedList<String> record)
	{
		bf = result;
		this.rcursor = rcursor;
		this.offset  = offset;
		this.record  = record;
	}
}
