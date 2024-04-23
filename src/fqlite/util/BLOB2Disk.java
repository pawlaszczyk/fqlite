package fqlite.util;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.util.Iterator;
import java.util.Set;

import fqlite.base.GUI;
import fqlite.base.Job;
import fqlite.types.BLOBElement;

public class BLOB2Disk {

	Job job;
	
	
	public BLOB2Disk(Job job){
		this.job = job;
	}
	
	/**
	 * @deprecated
	 */
	public void export(){
		
		if(job.bincache == null)
			return;
		
		Set<String> keys = job.bincache.keySet();
		Iterator<String> iter = keys.iterator();
	    while(iter.hasNext()){
	    	 String key = iter.next();
	         BLOBElement e = job.bincache.get(key);
	         if (e != null){
	            String extension = ".bin";
	            
	            switch(e.type){
	            
	            	case BMP:  extension = ".bmp";
	            							break;
	            	case JPG:  extension = ".jpg";
	            							break;
	              	case GIF:  extension = ".gif";
	              							break;
	             	case PNG:  extension = ".png";
											break;
	             	case TIFF: extension = ".tiff";
	             							break;
	             	case PDF: extension = ".pdf";
	             							break;
	             	case HEIC: extension = ".heic";
											break;
	             	case PLIST: extension = ".plist";
	             							break;
	             	case GZIP: extension = ".gzip";
	             							break;
	                default: extension = ".bin";
	            }
	            
	           
	        	try { 
	        		job.FileCache.put(key,"" + GUI.baseDir + "/"+ job.filename + "_" + key + extension);
	        		
					BufferedOutputStream buffer = new BufferedOutputStream(new FileOutputStream("" + GUI.baseDir + "/"+ job.filename + "_" + key + extension));
					//System.out.println(" Write BLOB to file ::"+ "" + GUI.baseDir + "/" + job.filename + "_" + key + extension);
					buffer.write(e.binary);
					buffer.close();
					
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
	         
	         }
	    }
	    
	    //job.BLOBs.clear();
	    //System.gc();
	
	}
	
}
