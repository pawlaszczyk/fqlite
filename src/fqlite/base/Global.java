package fqlite.base;

import java.nio.file.FileSystems;
import java.util.logging.Level;

public class Global {

	public static final String REGULAR_RECORD = " "; // U2713 - regular
	public static final String DELETED_RECORD_IN_PAGE = "D"; // U2718 - deleted
	public static final String FREELIST_ENTRY = "F"; // U267D - freelist
	public static final String STATUS_CLOMUN = "S"; // U291D U2691
	public static final String UNALLOCATED_SPACE = "U"; // U2318 - unallocated space
	public static final String FQLITE_VERSION = "3.0";
	public static final String FQLITE_RELEASEDATE = "27/09/2024";
	public static final String YEAR = "2024";
	public static final int CARVING_ERROR = -1;
	public static Level LOGLEVEL = Level.INFO;
	public static int numberofThreads = 1;
	public static final String separator = FileSystems.getDefault().getSeparator();
	public static String WORKINGDIRECTORY;
	
	public static boolean intenseScan = false;
	
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

	/* .csv export settings */
	public static String CSV_SEPARATOR = ";";
	public static boolean EXPORTTABLEHEADER = true;
	public static enum EXPORT_MODES {DONTEXPORT,TOCSV,TOSEPARATEFILES};
	public static EXPORT_MODES EXPORT_MODE = EXPORT_MODES.DONTEXPORT;
	
	
	/* font settings */
	public static String font_name = "System";
	public static String font_style = "Regular";
	public static String font_size = "12";
}
