package fqlite.util;


/**
 * This class is a wrapper class.
 * It implements a simple iterator for variable integer values.
 *
 * @author pawel
 */
public class VIntIter {

	private byte[] values;
	private int position;
	private int limit;
	private int value;
	
	public VIntIter(byte[] values){
		this.values = values;
		this.position = 0;
		limit = values.length;
	}
	
	public VIntIter(){
		this.values = null;
		this.position = 0;
		limit = 0;
	}
	
	static 	VIntIter[] channels = {new VIntIter(),new VIntIter(),new VIntIter(),new VIntIter(),new VIntIter(),new VIntIter(),new VIntIter()};
	
	public void setBack(){
		this.position = 0;
		limit = values.length;
	}
	
	public static VIntIter wrap(byte[] values, int channel){
		channels[channel].put(values);
		return channels[channel];
	}
	
	public void put(byte[] values){
		this.values = values;
		this.position = 0;
		limit = values.length;	
	}
	
	public boolean hasNext() {
		return (position < limit);
	}

	/**
	 * Next varint as a 64-bit value with SQLite semantics (up to 9 bytes, the ninth byte
	 * contributes all 8 bits). Serial types of BLOBs/texts larger than about 1 GiB
	 * (serial type = 2 * length + 12) do not fit into an int; use this method for them.
	 * Returns -1 if no value is left.
	 */
	public long nextLong() {
		if (position >= limit)
			return -1;
		long v = 0;
		for (int i = 0; i < 9 && position < limit; i++) {
			int b = values[position++] & 0xFF;
			if (i == 8)
				return (v << 8) | b;
			v = (v << 7) | (b & 0x7F);
			if ((b & 0x80) == 0)
				return v;
		}
		return v;
	}

	public int next() {

		value = -1;
		
		if (position < limit) {
			byte b = values[position++];
			value = b & 0x7F;
			
			test:
			while ((b & 0x80) != 0) {
				if (position < limit) {
					b = values[position++];
					value <<= 7;
					value |= (b & 0x7F);
				} else {
					//break;
					break test;
				}

			}

		}
		
		return value;
	}
	
}
