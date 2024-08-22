package fqlite.base;

public class WALFrame
{
	public int pagenumber;
	public int framenumber;
	public long salt1;
    public long salt2;
	public boolean committed = false;
	
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
