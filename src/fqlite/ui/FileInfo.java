package fqlite.ui;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;

import fqlite.base.Global;

public class FileInfo {

	StringBuilder sb;
	String sha256hash = "";
	String md5hash = "";
	String sha1 = "";
	String filename = "";
	
	
	/**
	 * Constructs a StringBuilder object informations about 
	 * a file on the file system. 
	 *  
	 * @param path
	 */
	public FileInfo(String path) 
	{
		sb = new StringBuilder();
		
		//if (true)
		//return;
		
		Path p = Paths.get(path);
		try {
			computeHashes(path);
			
			Map<String,Object> attributes = Files.readAttributes(p,"*", LinkOption.NOFOLLOW_LINKS);
		   	int i = 1;	 
			
		   	filename = p.getFileName().toString();
		   	
		    sb.append("--------------------------------------------------------------------------------\n");

		   	
		    sb.append(String.format("%80s%n",  "FQlite Forensic Report"));
		    sb.append(String.format("%80s%n",  "created with version " + Global.FQLITE_VERSION));
		    sb.append(String.format("%80s%n",  "created by " + "[" + System.getProperty("user.name") + "]" ));
		    sb.append(String.format("%80s%n",  "Operating System " + System.getProperty("os.name")));
		    
		    
		    sb.append("--------------------------------------------------------------------------------\n");
		    sb.append("\n");
		    sb.append(String.format(" Current Local Time: " + (new Date()).toString()));
		    sb.append("\n");

		    
		    sb.append(" File: " + p.getFileName());
		    sb.append("\n");
		    sb.append(" Path: " + path);
		    sb.append("\n");
		    sb.append(" Size: " + attributes.get("size") + " Bytes" );
		    sb.append("\n");

		    sb.append("\n\n");
            
		   		    
		    //printRow(" " , "Key", "Value", "Remarks");
		    sb.append("  Key                         Value                                         Remarks" + "\n");
		    printRow(" " , "--------------", " ----------------------", "-----------------");
		    printRow("" + i++, "creationTime    ", attributes.get("creationTime"), " of file on disk ");
		    printRow("" + i++, "lastAccess        ", attributes.get("lastAccessTime"), " of file on disk ");
		    printRow("" + i++, "lastModified     ", attributes.get("lastModifiedTime"), " of file on disk ");
		    sb.append("\n");
	           
			try {
			sb.append("Hashes \n");	
				
			  	 //MessageDigest md = MessageDigest.getInstance("MD5");
				 String hexMD5 = md5hash; //checksum(path, md);
				 sb.append(" md5      " + hexMD5 + "\n");

				 sb.append(" sha1     " + sha1 + "\n");

				 
				 //md = MessageDigest.getInstance("SHA-256");
				 String hex = sha256hash; //checksum(path, md);
				 sb.append(" sha256 " + hex + "\n");

				 
			} catch (Exception e) {
				e.printStackTrace();
			} //SHA, MD2, MD5, SHA-256, SHA-384...
	  
		
		
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
    public void computeHashes(String path)
    {
    	// MessageDigest md;
		try {
			//md = MessageDigest.getInstance("SHA-256");
			sha256hash = new DigestUtils(org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_256).digestAsHex(new File(path));
			md5hash = new DigestUtils(org.apache.commons.codec.digest.MessageDigestAlgorithms.MD5).digestAsHex(new File(path));
			sha1 = new DigestUtils(org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_1).digestAsHex(new File(path));

		} catch (IOException e) {
			e.printStackTrace();
		}
		 

    	
    }
	


	private void printRow(String c0, String c1, Object c2, Object c3 ) {
		sb.append(String.format("%s %s %-30s %-15s%n", c0, c1, String.valueOf(c2), c3 instanceof Integer ? "$" + c3 : c3));
	}
		
	public StringBuilder getReport()
	{
		return sb;
	}
	
	public static String center(String text, int len){
        if (len <= text.length())
            return text.substring(0, len);
        int before = (len - text.length())/2;
        if (before == 0)
            return String.format("%-" + len + "s", text);
        int rest = len - before;
        return String.format("%" + before + "s%-" + rest + "s", "", text);  
    }
	
		
	public void print()
	{
		System.out.println(sb);
	}
	
	@Override
	public String toString() 
	{
		return sb.toString();
	}
	
}
