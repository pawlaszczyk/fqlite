package fqlite.log;

import java.nio.file.FileSystems;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/// This is the central Logger class for FQLite.
/// We are using <code>java.util.logging.Logger</code> functionality
/// to enable rotating logging.
/// A total of 3 log files are kept in the <user directory>/.fqlite folder.
/// The maximum size of each log file is 4 MB.
/// A text area is also updated with the log messages so that
/// the last entries in the log file can be viewed via the interface
/// if required (see {@link AppLogWindow}; not created in the headless mode).
///
/// @author pawlaszc
public class AppLog {

    public static final Logger LOGGER = Logger.getLogger(AppLog.class.getName());
    
    static {
    	
        LOGGER.setUseParentHandlers(false);
        if (isHeadless())
            initHeadless();
        else
            AppLogWindow.setup();  // log window + log file (GUI mode)
    }

    /**
     * true if FQLite runs without a JavaFX user interface, i.e. as a library
     * ({@code fqlite.headless.FQLiteHeadless}) - set by the system property
     * {@code fqlite.headless=true}. In this mode no JavaFX control is created,
     * so only the module javafx.base is needed and no display/toolkit.
     */
    public static boolean isHeadless() {
        return Boolean.getBoolean("fqlite.headless");
    }

    /**
     * Logger set-up of the headless mode: no log window. A log file is only written
     * if the system property {@code fqlite.log.dir} names a directory.
     */
    private static void initHeadless() {
        String dir = System.getProperty("fqlite.log.dir");
        if (dir == null || dir.isBlank())
            return;
        try {
            java.io.File d = new java.io.File(dir);
            d.mkdirs();
            FileHandler fh = new FileHandler(d.getAbsolutePath() + FileSystems.getDefault().getSeparator()
                    + "fqlite.log", 4096 * 1000, 3, true);
            fh.setFormatter(new SimpleFormatter());
            LOGGER.addHandler(fh);
        } catch (Exception e) {
            System.err.println("FQLite: log file could not be created in " + dir + ": " + e.getMessage());
        }
    }

    public static void debug(String log){
    	LOGGER.finest(log);
    }
    
    public static void info(String log){
    	LOGGER.info(log);
    }
   
    public static void warning(String log){
    	LOGGER.warning(log);
    }
    
    public static void error(String log){
    	LOGGER.severe(log);
    }
    
    public static void setLevel(Level newLevel){
    	LOGGER.setLevel(newLevel);
    }
}
