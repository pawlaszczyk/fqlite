package fqlite.base;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 
 * BigByteBuffer allows mmaping files larger than Integer.MAX_VALUE bytes.
 * 
 */
public class BigByteBuffer {

	
	private static int OVERLAP = Integer.MAX_VALUE / 2;
	private final List<ByteBuffer> buffers;
	private long pos = 0;
	private long limit = 0;
	private long cap = 0;
	private long mark = -1;

	/**
	 * Retrieves the capacity of the buffer.
	 *
	 * @return the capacity of the buffer
	 */
	public final long capacity() {
		return cap;
	}

	/**
	 * Clears the buffer.
	 *
	 * @return this buffer
	 */
	public final BigByteBuffer clear() {
		limit = cap;
		pos = 0;
		mark = -1;
		return this;
	}

	/**
	 * Flips the buffer.
	 *
	 * @return this buffer
	 */
	public final BigByteBuffer flip() {
		limit = pos;
		pos = 0;
		mark = -1;
		return this;
	}

	/**
	 * Retrieves the current limit of the buffer.
	 *
	 * @return the limit of the buffer
	 */
	public final long limit() {
		return limit;
	}

	/**
	 * Sets this buffer's limit.
	 * 
	 * @param newLimit The new limit value; must be non-negative and no larger than
	 *                 this buffer's capacity.
	 *
	 * @return this buffer
	 *
	 * @exception IllegalArgumentException If the preconditions on newLimit do not
	 *                                     hold.
	 */
	public final BigByteBuffer limit(long newLimit) {
		if ((newLimit < 0) || (newLimit > cap))
			throw new IllegalArgumentException();
		if (newLimit < mark)
			mark = -1;
		if (pos > newLimit)
			pos = newLimit;
		limit = newLimit;
		return this;

	}

	/**
	 * Sets this buffer's mark at its position.
	 *
	 * @return this buffer
	 */
	public final BigByteBuffer mark() {
		mark = pos;
		return this;
	}

	/**
	 * Retrieves the current position of this buffer.
	 *
	 * @return the current position of this buffer
	 */
	public final long position() {
		return pos;
	}

	/**
	 * Sets this buffer's position. If the mark is defined and larger than the new
	 * position then it is discarded.
	 * 
	 * @param newPosition The new position value; must be non-negative and no larger
	 *                    than the current limit.
	 *
	 * @return this buffer
	 *
	 * @exception IllegalArgumentException If the preconditions on newPosition do
	 *                                     not hold
	 */
	public final BigByteBuffer position(long newPosition) {
		if ((newPosition < 0) || (newPosition > limit))
		{	
			System.err.println(" new Position is " + newPosition);
			throw new IllegalArgumentException();
		}
		if (newPosition <= mark)
			mark = -1;

		pos = newPosition;
		return this;
	}

	/**
	 * mmap the file and return a BigByteBuffer for it.
	 * 
	 * @param file the file to mmap
	 * @throws java.io.IOException if there is an error reading the file
	 */
	public BigByteBuffer(File file) throws IOException {
		this(new RandomAccessFile(file, "r"));
	}
	
	/**
	 * mmap the file and return a BigByteBuffer for it.
	 * 
	 * @param file the file to mmap
	 * @throws java.io.IOException if there is an error reading the file
	 */
	public BigByteBuffer(String filename) throws IOException {
		this(new RandomAccessFile(new File(filename), "r"));
	}
	

	/**
	 * mmap the file and return a BigByteBuffer for it.
	 * 
	 * @param file the file to mmap
	 * @throws java.io.IOException if there is an error reading the file
	 */
	public BigByteBuffer(RandomAccessFile file) throws IOException {
		long length = file.length();

		if (length == 0) {
			throw new IllegalArgumentException("File is empty");
		}

		buffers = new ArrayList<ByteBuffer>();
		for (long i = 0; i < length; i += OVERLAP) {
			buffers.add(file.getChannel().map(FileChannel.MapMode.READ_ONLY, i, Math.min(length - i, Integer.MAX_VALUE)));
		}
		
		limit = cap = length;
		pos = 0;
		mark = -1;
	}
	
	private BigByteBuffer(byte[] array, int offset, int length){

		pos=0;
		limit = cap = length;
		mark = -1;
		ByteBuffer bb = ByteBuffer.wrap(array, offset, length);
		buffers = new ArrayList<ByteBuffer>();
		buffers.add(bb);
	}

	private BigByteBuffer(byte[] array){

		pos=0;
		limit = cap = array.length;
		mark = -1;
		ByteBuffer bb = ByteBuffer.wrap(array);
		buffers = new ArrayList<ByteBuffer>();
		buffers.add(bb);
	}
	

	/**
	 * Wrap a ByteBuffer. Used for testing.
	 * 
	 * @param buffer the buffer to wrap
	 */
	public BigByteBuffer(ByteBuffer buffer) {
		limit = cap = buffer.limit();
		pos = 0;
		mark = -1;
		buffers = Arrays.asList(buffer);
	}

    /**
     * Returns the byte at the current position and increases the position by 1.
     *
     * @return the byte at the current position.
     * @exception BufferUnderflowException
     *                if the position is equal or greater than limit.
     */
	public byte get(){
		if (pos >= limit){
			throw new BufferUnderflowException();
		}	
		ByteBuffer buffer = buffers.get((int) (pos / OVERLAP));
		buffer.position((int) (pos % OVERLAP));
		byte b = buffer.get();
		pos++; 
		return b;
	}
	
	/**
     * Returns the byte at the given position and increases the position by 1.
     *
     * @return the byte at the current position.
     * @exception BufferUnderflowException
     *                if the position is equal or greater than limit.
     */
    
	public byte get(long position){
		position(position);
		return get();
	}
	
	
	/**
     * Returns a sliced buffer that shares its content with this buffer.
     * <p>
     * The sliced buffer's capacity will be this buffer's
     * {@code remaining()}, and it's zero position will correspond to
     * this buffer's current position. The new buffer's position will be 0,
     * limit will be its capacity, and its mark is cleared. The new buffer's
     * read-only property and byte order are the same as this buffer's.
     * <p>
     * The new buffer shares its content with this buffer, which means either
     * buffer's change of content will be visible to the other. The two buffer's
     * position, limit and mark are independent.
     *
     * @return a sliced buffer that shares its content with this buffer.
     */
    public ByteBuffer slice(){
    	// TODO: Overlapping buffers
    	ByteBuffer buffer = buffers.get((int) (pos / OVERLAP));
    	buffer.position((int) (pos % OVERLAP));
    	return buffer.slice();
    }
    
    /**
     * Reads bytes from the current position into the specified byte array and
     * increases the position by the number of bytes read.
     * <p>
     * Calling this method has the same effect as
     * {@code get(dst, 0, dst.length)}.
     *
     * @param dst
     *            the destination byte array.
     * @return {@code this}
     * @exception BufferUnderflowException
     *                if {@code dst.length} is greater than {@code remaining()}.
     */
    public ByteBuffer get(byte[] dst) {
    	// TODO: Overlapping buffers
    	ByteBuffer buffer = buffers.get((int) (pos / OVERLAP));
    	buffer.position((int) (pos % OVERLAP));
    	pos+= dst.length;
    	return buffer.get(dst, 0, dst.length);
    }
    
    
    /**
      * Wraps a <code>byte</code> array into a <code>ByteBuffer</code>
      * object.
      *
      * @exception IndexOutOfBoundsException If the preconditions on the offset
      * and length parameters do not hold
      */
    public static final BigByteBuffer wrap(byte[] array, int offset, int length)
    {
    	return new BigByteBuffer(array,offset,length);
    }
    
    public static final BigByteBuffer wrap(byte[] array)
    {
    	return new BigByteBuffer(array);
    }
    
    
    /**
     * Returns the int at the current position and increases the position by 4.
     * <p>
     * The 4 bytes starting at the current position are composed into a int
     * according to the current byte order and returned.
     *
     * @return the int at the current position.
     * @exception BufferUnderflowException
     *                if the position is greater than {@code limit - 4}.
     */
    public int getInt(long position){
    	// TODO: Overlapping buffers
    	this.pos = position;
    	ByteBuffer buffer = buffers.get((int) (position / OVERLAP));
    	long pp = position%OVERLAP;
        buffer.position((int)pp);
    	pos = position + 4;
    	return buffer.getInt();
    }
    
    
    public ByteBuffer read(ByteBuffer b,long position){
    	
    	//System.out.println(">>> position read()" + position);
    	this.pos = position;
    	ByteBuffer buffer = buffers.get((int) (pos / OVERLAP));
        int howmany = b.capacity();
        byte[] dst = new byte[howmany];
        long pp = pos%OVERLAP;
        buffer.position((int)pp);
        buffer.get(dst);
        b.position(0);
        b.put(dst);
        b.limit(b.capacity());
        b.position(0);
        return b;
        
    }

}