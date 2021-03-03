package fqlite.base;

/**
 * Defines an abstracts Base class with some basic
 * logging functionality.
 * 
 * @author pawlaszc
 *
 */
public abstract class Base {

	static final int ALL = 0;
	static final int DEBUG = 1;
	static final int INFO = 2;
	static final int WARNING = 3;
	static final int ERROR = 4;
	static int LOGLEVEL = ALL; 
	
			
	public void debug(String message) {
	
		if (LOGLEVEL <= DEBUG)
			System.out.println("[DEBUG] " +message);
	}
	
    public void info(String message) {
	
		if (LOGLEVEL <= INFO)
			System.out.println("[INFO] " + message);
	}

	public void warning(String message) {
		
		if (LOGLEVEL <= WARNING)
			System.out.println("[WARNING] " + message);
	}
	
	public void err(String message) {
		
		/* always print error messages */
		System.err.println("ERROR: " + message);
	}
}
