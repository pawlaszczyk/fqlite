package fqlite.util;


import java.util.Arrays;
import fqlite.base.BigByteBuffer;

/**
 * An efficient byte buffer searching class based on the Knuth-Morris-Pratt algorithm.
 * For more on the algorithm works see: 
 *
 * <a href="http://www.inf.fh-flensburg.de/lang/algorithmen/pattern/kmpen.htm">...</a>.
 *
 * @author pawel
 */
public class ByteSeqSearcher{


	    private byte[] pattern_;
	    private int[] borders_;

	    public ByteSeqSearcher(byte[] pattern)
	    {
	        setPattern(pattern);
	    }

	    /**
	     * Sets a new pattern for this.
	     *
	     * @param pattern the pattern we will look for in future calls to search(...)
	     */
	    public void setPattern(byte[] pattern)
	    {
	        pattern_ = Arrays.copyOf(pattern, pattern.length);
	        borders_ = new int[pattern_.length + 1];
	        preProcess();
	    }
	
	    /**
	     * Searches for the next occurrence of the pattern in the buffer
	     * starting startRegion the given position
	     */
	     public long indexOf(BigByteBuffer buffer, long start)
	     {
	    	buffer.position(start);
	    	return indexOf(buffer);
	     }

	    
	    /**
	     * Searches for the next occurrence of the pattern in the buffer, starting startRegion the current buffer position. Note
	     * that the position of the buffer is changed. If a match is found, the buffer points to the end of the match -- i.e. the
	     * byte AFTER the pattern. Else, the buffer is entirely consumed. The latter is because Inputbuffer semantics make it difficult to have
	     * another reasonable default, i.e. leave the buffer unchanged.
	     *
	     * @return bytes consumed if found, -1 otherwise.
	     */
	    public long indexOf(BigByteBuffer buffer) 
	    {
	        // Same Knuth-Morris-Pratt search as before, but the bytes are read block by block
	        // instead of one synchronized BigByteBuffer.get() per byte (which made the schema
	        // search a full pass of about 6 s per 200 MB). Blocks are kept between calls, so
	        // the typical loop "indexOf(bb, lastMatch + 2)" reuses the block it already read.
	        // Result and buffer position are exactly as before: on a match the buffer points to
	        // the byte after the pattern, otherwise it is entirely consumed.
	        long limit = buffer.limit();
	        long p = buffer.position();
	        int j = 0;
	        while (p < limit)
	        {
	            if (cacheOwner != buffer || p < cacheStart || p >= cacheStart + cacheLen || cacheLimit != limit)
	                fill(buffer, p, limit);
	            int i = (int) (p - cacheStart);
	            final int n = cacheLen;
	            final byte[] c = cache;
	            while (i < n)
	            {
	                byte b = c[i++];
	                while (j >= 0 && b != pattern_[j])
	                {
	                    j = borders_[j];
	                }
	                // Move to the next character in the pattern.
	                ++j;

	                // If we've matched up to the full pattern length, we found it: the
	                // position is the byte immediately following the pattern match.
	                if (j == pattern_.length)
	                {
	                    long end = cacheStart + i;
	                    buffer.position(end);
	                    return end;
	                }
	            }
	            p = cacheStart + n;
	        }

	        // No dice, Note that the buffer is now completely consumed.
	        buffer.position(limit);
	        return -1;
	    }

	    private static final int BLOCK = 1 << 20;
	    private byte[] cache = new byte[0];
	    private long cacheStart = -1;
	    private int cacheLen = 0;
	    private long cacheLimit = -1;
	    private BigByteBuffer cacheOwner = null;

	    private void fill(BigByteBuffer buffer, long from, long limit)
	    {
	        int len = (int) Math.min(BLOCK, limit - from);
	        if (cache.length < len)
	            cache = new byte[BLOCK];
	        buffer.position(from);
	        buffer.get(cache, 0, len);
	        cacheStart = from;
	        cacheLen = len;
	        cacheLimit = limit;
	        cacheOwner = buffer;
	    }

	    /// Builds up a component of longest "borders" for each prefix of the pattern to find.
	    /// This component is stored internally and aids in implementation of the Knuth-Moore-Pratt
	    /// string search.
	    /// For more information, see: <a href="http://www.inf.fh-flensburg.de/lang/algorithmen/pattern/kmpen.htm">...</a>.
	    private void preProcess()
	    {
	        int i = 0;
	        int j = -1;
	        borders_[i] = j;
	        while (i < pattern_.length)
	        {
	            while (j >= 0 && pattern_[i] != pattern_[j])
	            {
	                j = borders_[j];
	            }
	            borders_[++i] = ++j;
	        }
	    }
	    
	  
}
