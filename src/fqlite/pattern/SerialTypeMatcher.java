package fqlite.pattern;

import java.nio.ByteBuffer;

import fqlite.util.Auxiliary;

/**
 * An engine that performs match operations on a byte buffer sequence by
 * interpreting a pattern.
 * 
 * @author pawlaszc
 *
 */
public class SerialTypeMatcher {

	HeaderPattern pattern = null;
	ByteBuffer buffer = null;
	int pos = 0;
	int startRegion;
	int endRegion;
	public int start;
	int end;
	MMode mode = MMode.NORMAL;
    public String fallbackFor1stColumn = "02";
	
	
	/**
	 * Constructor.
	 * 
	 * @param buffer ByteBuffer to analyze
	 */
	public SerialTypeMatcher(ByteBuffer buffer) {
		this.buffer = buffer;
		startRegion = 0;
		endRegion = buffer.capacity();
		buffer.position(0);
	}

	/**
	 * Change the matching behavior. You can choose between:
	 * NORMAL,NOHEADER,NO1STCOL.
	 * 
	 * @param newMode
	 */
	public void setMatchingMode(MMode newMode) {
		mode = newMode;
	}

	public MMode getMachtingMode() {
		return mode;
	}

	/**
	 * Set a list of matching constrains.
	 * 
	 * @param pattern
	 */
	public void setPattern(HeaderPattern pattern) {
		this.pattern = pattern;
	}

	/**
	 * Sets the limits of this matcher's region.
	 * 
	 * @param from
	 * @param to
	 */
	public void region(int from, int to) {
		this.startRegion = from;
		this.endRegion = to;
		// renew position for search
		buffer.position(from);
	}

	/**
	 * Returns the offset after the last character matched.
	 * 
	 * @return The offset after the last character matched
	 */
	public int end() {
		return end;
	}

	/**
	 * Returns the start indices of the previous match.
	 *
	 * @return The indices of the first byte matched
	 */
	public int start() {
		return start;
	}

	/**
	 * This method starts at the beginning of this matcher's region, or, if a
	 * previous invocation of the method was successful and the matcher has not
	 * since been reset, at the first character not matched by the previous match.
	 * If the match succeeds then more information can be obtained via the start,
	 * end, and group methods.
	 *
	 * @return true if, and only if, a subsequence of the input sequence matches
	 *         this matcher's pattern
	 */
	public boolean find() {
				
		int idx = 0;
		switch (mode) 
		{
			case NORMAL:
				idx = 0;
				break;
	
			case NOHEADER:
				idx = 1;
				break;
	
			case NO1stCOL:
				idx = 2;
				break;
		}
		int i = idx;

		/* check pattern constrain by constrain */
  
		if (pattern == null) {
			return false;
		}
		else {
			while (i < pattern.size()) 
			{
			
				/* do not read out of bounds - stop before the end */
				if (buffer.position() > (endRegion - 4))
				  return false;
				
								
				/* remember the begin of a possible match */
				int current = buffer.position();
			
					
				if (i == idx)
					pos = current;
				
				/* read next value */
				int value = readUnsignedVarInt();
				
				
				//System.out.println(";" + value + ";");
					
					// no varint OR costrain does not match -> skip this an go on with the next
					// bytes
					if (value == -1 || !pattern.get(i).match(value)) 
					{
					    	buffer.position(pos+1);
					    	/* and again, startRegion the beginning but with the next byte */
					    	i = idx;
					    	/* skip pattern matching step and try again */
					    	continue;
					    
					}
					//System.out.println(";" + value + ";");
					
					/* go ahead with next constrain */
					
					i++;
	
					
			}
		
	
			start = pos;
			end = buffer.position();
		}
		
		if (end <= start)
			return false;
		
		return true; // byte number of the match
	}

	/**
	 * Returns the input subsequence matched by the previous match. For a matcher m
	 * with input sequence s, the expressions m.group() and s.substring(m.start(),
	 * m.end()) are equivalent.
	 * 
	 * @return
	 */
	public ByteBuffer group() {
		byte[] match = new byte[(end) - start];
		buffer.position(start);
		buffer.get(match, 0, (end - start));
		return ByteBuffer.wrap(match);
	}

	/**
	 * Returns the input subsequence matched by the previous match. The return value
	 * contains a hex-representation of the match byte values.
	 * 
	 * @return
	 */
	public String substring(int start, int end) {
		if (start > end)
			return "";
		byte[] match = new byte[(end) - start];
		buffer.position(start);
		buffer.get(match, 0, (end - start));
		return Auxiliary.bytesToHex(match);
	}

	/**
	 * Returns the input subsequence matched by the previous match. The return value
	 * contains a hex-representation of the match byte values.
	 * 
	 * @return
	 */
	public String group2Hex() {
		return substring(start, end);
	}

	/**
	 * Try to read the next varint value from the buffer.
	 * 
	 * @return the corresponding int value or -1 if no varint could be found.
	 */
	public int readUnsignedVarInt() {
		int value = 0;
		int b = 0;
		int counter = 0;
		int shift = 0;

		// as long as we have a byte with most significant bit value 1
		// there are more byte to read
		// we only read a maximum of 3 bytes (8 bytes would be possible to)
		while ((((b = buffer.get()) & 0x80) != 0) && counter < 3) {
			counter++;
			shift += 7;
			value |= (b & 0x7F) << shift;
		}
		// last rightmost byte has always need to have a 0 at the MSB
		// hence, if there is a 1 -> no varint value
		if ((b & 0x80) != 0)
			return -1;

		// return a normalized integer value
		return value | b;
		
		
	}

	public static void main(String[] args) 
	{
		String hexstring = "00000000 210A0603 151D0407 00C35A4C 75697348 6572726D 616E6E47 A0081441 EA353168 BAA2DB22 09";
	    hexstring = hexstring.replaceAll(" ","");
		byte[] barray = Auxiliary.hexStringToByteArray(hexstring);
	    ByteBuffer buffer = ByteBuffer.wrap(barray);
	    SerialTypeMatcher stm = new SerialTypeMatcher(buffer);
	    HeaderPattern pattern = new HeaderPattern();
	   
	   // [06..10|01..06|>=13|>=13|00..06|07|]
	    
	    
	    pattern.addHeaderConstraint(6,10);
	    pattern.addIntegerConstraint(); //00..06
	    pattern.addStringConstraint(); // >= 13 or Null
	    pattern.addStringConstraint(); // >= 13 or Null
	    pattern.addIntegerConstraint(); //00..06
	    pattern.addFloatingConstraint(); // 07
	    
	    
	    stm.setPattern(pattern);
	    
		//System.out.println("header pattern: " + pattern);

		/* find every match within the given region */
		while (stm.find()){
			
			/* get the hex-presentation of the match */
			String m = stm.group2Hex();
			
			System.out.println("Got it! :: " + m);
		}
	    
	}
	

}
