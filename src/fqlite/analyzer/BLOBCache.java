package fqlite.analyzer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import fqlite.base.Job;
import fqlite.types.BLOBElement;
import fqlite.util.Auxiliary;

/// This is a wrapper class for managing BLOB content.
/// Note: All binary cell values are completely managed in RAM by default.
public class BLOBCache {

	// The cache is backed by a Map.
	public Map<String,BLOBElement> cache;

	//We define a 1-to-1 relation between a job object and a cache object
	public Job job;
	
	/**
	 * assign BLOB list to this cache
	 * @param job the parent object
	 */
	public BLOBCache(Job job){
		this.job = job;
        cache = new ConcurrentHashMap<String,BLOBElement>();
    }
	
	/**
	 * Returns all keys that are in the cache.
	 * @return keyset
	 */
	public Set<String> keySet(){
		return cache.keySet();
	}
	
	/**
	 * Returns the number of binary elements in the cache.
	 * @return number of elements
	 */
	public int size(){			
		return cache.size();
	}
	
	/**
	 * Get the BLOBElement for a given key.
	 * @param key access key to blob object
	 * @return a BLOBElement
	 */
	public BLOBElement get(String key){
		return cache.get(key);
	}
	
	/**
	 * Insert a new binary object into the cache.
	 * @param key the key
	 * @param value the value to store
	 */
	public void put(String key, BLOBElement value){
		cache.put(key,value);
	}
	
	/**
	 * Get the BLOB content as a hex string.
	 * @param path location on the filesystem.
	 * @return hex string
	 */
	public String getHexString(String path){

		System.out.println("path:"+path);
		String result;
		BLOBElement e = get(path);
		if (e ==null)
			return "NULL";
		ByteBuffer bf = ByteBuffer.wrap(e.binary);
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
	 * @param path path to the blob element on the filesystem
	 * @return a byte array
	 */
	public byte[] read(String path){
		return get(path).binary;
	}
	
}
