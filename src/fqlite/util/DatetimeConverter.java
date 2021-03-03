package fqlite.util;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * This class offers some basic date conversion routines. 
 *
 * @author pawlaszc
 *
 */


public class DatetimeConverter{

	final static long UNIX_MIN_DATE = 1262304000000L; // 01.01.2010 
	final static long UNIX_MAX_DATE = 2524608000000L; // 01.01.2050  
		
	/**
	 * Core Data is a data storage framework to manage objects in iOS and OS X applications.
	 * Core Data is part of the Cocoa API. These timestamps are sometimes labeled 'Mac absolute time'.
	 * A Core Data timestamp is the number of seconds (or nanoseconds) since midnight, January 1, 2001, GMT (see CFAbsoluteTime).
	 * The difference between a Core Data timestamp and a Unix timestamp (seconds since 1/1/1970) is 978307200 seconds.
	 * 
	 * 
	 * @param timestamp
	 * @return
	 */
	
	public static String isMacAbsoluteTime(double timestamp)
	{		
		long time = (978307200 + (long)timestamp)*1000;
		System.out.println("isMacAbsoluteTime(): " + timestamp + " unix " + time);
	
		
		if (time > UNIX_MIN_DATE && time < UNIX_MAX_DATE)
		{
			Date d = new Date(time); 
			return d.toString();
		}
		return null;
	}
	
	/**
	 *  Current Unix epoch time in  So the Epoch is Unix time 0 (1-1-1970) but it is also used as Unix Time or Unix Timestamp.
	 *  
	 * @param time
	 * @return
	 */
	public static String isUnixEpoch(long timestamp)
	{
		if (timestamp > UNIX_MIN_DATE && timestamp < UNIX_MAX_DATE)
		{
			Date d = new Date(timestamp);
			return d.toString();
		}
		return null;
	}
	
	/**
	 * 
	 * @param timestamp
	 * @return
	 */
	public static String isJulianDate(double timestamp)
	{
		return calculateGregorianDate(timestamp).toString();
	}
	
	
  	/**     
  	 * Convert the given Julian Day to Gregorian Date (in UT time zone).
     * Based on the formula given in the Explanitory Supplement to the
     * Astronomical Almanac, pg 604.
     */
    private static Date calculateGregorianDate(double jd) {
        int l = (int) jd + 68569;
        int n = (4 * l) / 146097;
        l = l - (146097 * n + 3) / 4;
        int i = (4000 * (l + 1)) / 1461001;
        l = l - (1461 * i) / 4 + 31;
        int j = (80 * l) / 2447;
        int d = l - (2447 * j) / 80;
        l = j / 11;
        int m = j + 2 - 12 * l;
        int y = 100 * (n - 49) + i + l;

        double fraction = jd - Math.floor(jd);
        double dHours = fraction * 24.0;
        int hours = (int) dHours;
        double dMinutes = (dHours - hours) * 60.0;
        int minutes = (int) dMinutes;
        int seconds = (int) ((dMinutes - minutes) * 60.0);

        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UT"));
        cal.set(y, m - 1, d, hours + 12, minutes, seconds);
        return cal.getTime();
    }
}
