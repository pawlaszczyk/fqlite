package fqlite.base;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;

/**
 *  This class defines global variables and program constants.
 */
public class Global {

	public static final String REGULAR_RECORD = " "; // U2713 - regular
	public static final String DELETED_RECORD_IN_PAGE = "D"; // U2718 - deleted
	public static final String FREELIST_ENTRY = "F"; // U267D - freelist
	public static final String STATUS_CLOMUN = "S"; // U291D U2691
	public static final String FQLITE_VERSION = "4.01";
	public static final String FQLITE_RELEASEDATE = "19/02/2026";
	public static final String YEAR = "2026";
	public static final int CARVING_ERROR = -1;
	public static Level LOGLEVEL = Level.INFO;
	public static int numberofThreads = 1;
	public static final String separator = FileSystems.getDefault().getSeparator();
	public static String WORKINGDIRECTORY;
	public static final String CONFIG_FILE = "fqlitellm-config.properties";
	public static final File baseDir = new File(System.getProperty("user.home"), ".fqlite");
	public static final Path llmconfig = Paths.get(baseDir.getAbsolutePath(), CONFIG_FILE);

	/* file types */
	public static final int REGULAR_DB_FILE = 0;
	public static final int ROLLBACK_JOURNAL_FILE = 1;
	public static final int WAL_ARCHIVE_FILE = 2;

	/* .csv export settings */
	public static String CSV_SEPARATOR = ";";
	public static boolean EXPORTTABLEHEADER = true;
	public static enum EXPORT_MODES {DONTEXPORT,TOCSV,TOSEPARATEFILES};
	public static EXPORT_MODES EXPORT_MODE = EXPORT_MODES.DONTEXPORT;
	public static String SQL_LEX_MODE = "JAVA";
	
	/* font settings */
	public static String font_name = "System";
	public static String font_style = "Regular";
	public static String font_size = "12";
	
	public static boolean SQLWARNING_SEEN = false;

	public final static String col_offset = "_OFFSET";
	public final static String col_rowid = "_ROWID";
	public final static String col_no = "_NO";
	public final static String col_pll = "_PLL";
	public final static String col_status = "_STATUS";
	public final static String col_tblname = "_TBLNAME";
	public final static String col_commit = "_COMMIT";
	public final static String col_dbpage = "_DBPAGE";
	public final static String col_walframe = "_WALFRAME";
	public final static String col_salt1 = "_SALT1";
	public final static String col_salt2 = "_SALT2";

}
