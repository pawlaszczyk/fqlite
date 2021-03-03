package fqlite.util;

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
	public int offset; 
	
	public CarvingResult(int rcursor,int offset, StringBuffer result)
	{
		bf = result;
		this.rcursor = rcursor;
		this.offset  = offset;
	}
}
