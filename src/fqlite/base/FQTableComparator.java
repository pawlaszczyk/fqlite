package fqlite.base;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class FQTableComparator<T> implements java.util.Comparator<T>
{

    /**
     * Custom compare to sort numbers as numbers.
     * Strings as strings, with numbers ordered before strings.
     * 
     * @param o1
     * @param o2
     * @return
     */
    @Override
    public int compare(Object oo1, Object oo2) {
        
    	boolean isFirstNumeric, isSecondNumeric;
        String o1 = oo1.toString(), o2 = oo2.toString();

        /* are there some dates to compare like dd/MM/yyyy hh:mm:ss */
        if (o1.length() == 19 && o2.length() == 19)
        	if (o1.charAt(2)=='/' && o1.charAt(5)=='/'  && o2.charAt(2)=='/' && o2.charAt(5)=='/' )
        	{
        		return compareDates(o1,o2);
        	}
        
        /* check both string with a REGEX */
        /* Do they contain one or even more numbers ? */
        isFirstNumeric = o1.matches("\\d+");
        isSecondNumeric = o2.matches("\\d+");

        
 
        if (isFirstNumeric) {
            if (isSecondNumeric) {
            	// do the compare with integer values
                return Integer.valueOf(o1).compareTo(Integer.valueOf(o2));
            } else {
                return -1; // numbers always smaller than letters
            }
        } 
        else {
           
        	if (isSecondNumeric) {
                return 1; // numbers always smaller than letters
            } 
        	else 
        	{
        		// Wait a moment. There could be characters between like . <space> _ 
        	
        		
                // Trying to parse String to Integer.
                // If there is no Exception then Object is numeric, else it's not.
                try{
                    Integer.parseInt(o1);
                    isFirstNumeric = true;
                }catch(NumberFormatException e){
                    isFirstNumeric = false;
                }
                try{
                    Integer.parseInt(o2);
                    isSecondNumeric = true;
                }catch(NumberFormatException e){
                    isSecondNumeric = false;
                }

                if (isFirstNumeric) {
                    if (isSecondNumeric) {
                    	
                    	/* Okay - There are some characters which are not a digit*/
                    	/* i.e.  '123.456' or -123 ... */
                        int intCompare = Integer.valueOf(o1.split("[^0-9]")[0]).compareTo(Integer.valueOf(o2.split("[^0-9]")[0]));
                        if (intCompare == 0) {
                            return o1.compareToIgnoreCase(o2);
                        }
                        return intCompare;
                    } else {
                        return -1; // numbers always smaller than letters
                    }
                } else {
                    if (isSecondNumeric) {
                        return 1; // numbers always smaller than letters
                    } else {
                        return o1.compareToIgnoreCase(o2);
                    }
                }
            }
        }
    }
    
    public int compareDates(String o1, String o2)
    {
    	SimpleDateFormat f = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
    	try {
    		return f.parse(o1).compareTo(f.parse(o2));
    	} 
    	catch (ParseException e) {
         throw new IllegalArgumentException(e);
    	}
    }
    
    
}
