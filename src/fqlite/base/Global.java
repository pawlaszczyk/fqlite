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
	public static final String FQLITE_VERSION = "5.0";
	public static final String FQLITE_RELEASEDATE = "16/06/2026";
	public static final String YEAR = "2026";
	public static final int CARVING_ERROR = -1;
	public static Level LOGLEVEL = Level.FINER;
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
	public static String OpenCellID = null;


	/**
	 * Output pattern used by {@code Auxiliary.int2Timestamp()} to render
	 * numeric timestamps as human-readable strings.
	 * Follows the Unicode CLDR Date Field Symbol Table
	 * (https://www.unicode.org/reports/tr35/tr35-dates.html#Date_Field_Symbol_Table)
	 * as interpreted by {@link java.time.format.DateTimeFormatter}.
	 * Examples:
	 *   "MM/dd/yyyy - HH:mm:ss Z"   → 04/28/2023 - 17:30:47 +0000
	 *   "yyyy-MM-dd HH:mm:ss Z"     → 2023-04-28 17:30:47 +0000  (default)
	 */
	public static String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss Z";

	/**
	 * When {@code true}, timestamps are displayed in UTC.
	 * When {@code false}, the system's local time zone is used instead.
	 */
	public static boolean TIMESTAMP_USE_UTC = true;

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
