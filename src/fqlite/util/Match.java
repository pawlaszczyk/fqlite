package fqlite.util;

/**
 * This class represents a match of the header signature.
 * It is used during data recovery process.
 */
public class Match
{
	 public Match(String match,int begin, int end)
	 {
		 this.match = match;
		 this.begin = begin;
		 this.end = end;
	 }
	 
	 public String match;
	 public int begin;  // start offset of the match
	 public int end; // end offset of the match
	 public int rowidcolum = -1;  // number of the rowid column 
	 
	 
	 public static boolean onlyZeros(String s)
	 {
		 for (int i = 0; i < s.length(); i++)
		 {
			 if (s.charAt(i)!='0') return false;
		 }
		 return true;
	 }
}

