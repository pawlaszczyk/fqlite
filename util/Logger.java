package fqlite.util;

import fqlite.base.Base;

/**
 * A basic Logger class.
 * 
 * @author pawlaszc
 *
 */
public class Logger extends Base {

	public static Base out;
	
	
	static
	{
		out = new Logger();
	}
	
	
	public Logger()
	{}
	
	
	
}
