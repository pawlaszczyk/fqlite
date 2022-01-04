package fqlite.base;

public class Global {

	public static final String REGULAR_RECORD = " "; // U2713 - regular
	public static final String DELETED_RECORD_IN_PAGE = "D"; // U2718 - deleted
	public static final String FREELIST_ENTRY = "F"; // U267D - freelist
	public static final String STATUS_CLOMUN = "S"; // U291D U2691
	public static final String UNALLOCATED_SPACE = "U"; // U2318 - unallocated space
	public static final String FQLITE_VERSION = "1.57";
	public static final String FQLITE_RELEASEDATE = "04/01/2022";
	public static final int CARVING_ERROR = -1;
	public static int LOGLEVEL = Base.ERROR;
	public static int numberofThreads = 1;
}
