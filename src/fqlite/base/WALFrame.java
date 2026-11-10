package fqlite.base;

/**
 *  This class represents a WAL-Frame structure from an WAL archive.
 *  It holds information like pagenumber of the original db the frame belongs to.
 *  Furthermore, an information about the current state (committed or not) is managed.
 */
public class WALFrame
{
	public int pagenumber;
	public int framenumber;
	public long salt1;
    public long salt2;
	public boolean committed = false;


	/** "DB size after commit" from the frame header (0 = no commit frame) */
	public int commitSize = 0;
	
	@Override
	public String toString()
	{
		return "{pagenumber=" + pagenumber + " framenumber=" + framenumber + " committed=" + committed +"}";
		
	}
	
	
	public WALFrame(int pagenumber, int framenumber, long salt1, long salt2, boolean committed)
	{
		this.salt1 = salt1;
		this.salt2 = salt2;
		this.pagenumber = pagenumber;
		this.framenumber  = framenumber;
		this.committed = committed;
	}
	
	
}
