package fqlite.util;


/**
 * This class is a wrapper class. 
 * 
 * 
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
	    //return new VIntIter(values);
	}
	
	public void put(byte[] values){
		this.values = values;
		this.position = 0;
		limit = values.length;	
	}
	
	public boolean hasNext() {
		return (position < limit);
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
