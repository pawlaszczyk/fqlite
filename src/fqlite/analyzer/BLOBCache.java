package fqlite.analyzer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import fqlite.base.Job;
import fqlite.types.BLOBElement;
import fqlite.util.Auxiliary;

/**
 * This is a wrapper class for managing BLOB content.
 * 
 * Note: All binary cell values are completely managed in RAM be default.
 * 
 */
public class BLOBCache {

	// The cache is backed by a Map.
	public Map<String,BLOBElement> cache = new ConcurrentHashMap<String,BLOBElement>();

	// we define a 1-to-1 relation between a job object and a cache object
	public Job job;
	
	/**
	 * assign BLOB list to this cache
	 * @param job
	 */
	public BLOBCache(Job job){
		this.job = job;
	}
	
	/**
	 * Returns all keys currently that are currently in the cache.
	 * @return
	 */
	public Set<String> keySet(){
		return cache.keySet();
	}
	
	/**
	 * Returns the number of binary elements in the cache.
	 * @return
	 */
	public int size(){			
		return cache.size();
	}
	
	/**
	 * Get the BLOBElement for a given key.
	 * @param offset
	 * @return
	 */
	public BLOBElement get(String key){
		return cache.get(key);
	}
	
	/**
	 * Insert a new binary object to the cache.
	 * @param key
	 * @param value
	 */
	public void put(String key, BLOBElement value){
		cache.put(key,value);
	}
	
	/**
	 * Get the BLOB content as hex-string.
	 * @param path
	 * @return
	 */
	public String getHexString(String path){
	    
		String result;
		ByteBuffer bf = ByteBuffer.wrap(get(path).binary);
		bf.position(0);
		result = Auxiliary.bytesToHex2(bf); 
		return result;
	}
	
	public String getASCII(String path){
	
		
		ByteBuffer bf = ByteBuffer.wrap(get(path).binary);
		bf.position(0);
		char[] result = new char[bf.limit()];
		int i = 0;
		while (bf.position() < bf.limit()){
			byte b = bf.get();
			if (b > 32 && b < 127){
				result[i] = (char)b;
			}
			else
				result[i] = '.';
			i++;
		}
		return new String(result);
	}
	
	
	/**
	 * Get the underlying byte array.
	 *
	 * @param path
	 * @return
	 */
	public byte[] read(String path){
		return get(path).binary;
	}
	
}
