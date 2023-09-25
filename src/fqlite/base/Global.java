package fqlite.base;

public class Global {

	public static final String REGULAR_RECORD = " "; // U2713 - regular
	public static final String DELETED_RECORD_IN_PAGE = "D"; // U2718 - deleted
	public static final String FREELIST_ENTRY = "F"; // U267D - freelist
	public static final String STATUS_CLOMUN = "S"; // U291D U2691
	public static final String UNALLOCATED_SPACE = "U"; // U2318 - unallocated space
	public static final String FQLITE_VERSION = "2.1";
	public static final String FQLITE_RELEASEDATE = "21/09/2023";
	public static final int CARVING_ERROR = -1;
	public static int LOGLEVEL = Base.ERROR;
	public static int numberofThreads = 1;

	
	/* internal constants of page types */
	public static final int TTBLLEAFPAGE = 8;
	public static final int TTBLINTERIORPAGE = 12;
	public static final int TIDXLEAFPAGE = 10;
	public static final int TIDXINTERIORPAGE = 2;
	public static final int TOVERFLOWPAGE = 0;

	
	/* for MAC OSX only */
	public static final String APPLICATION_NAME = "FQLite";
	public static final String APPLICATION_ICON = "/logo.png";
	
	/* file types */
	public static final int REGULAR_DB_FILE = 0;
	public static final int ROLLBACK_JOURNAL_FILE = 1;
	public static final int WAL_ARCHIVE_FILE = 2;
	
}
