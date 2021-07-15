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
		 
		    sb.append("--------------------------------------------------------------------------------\n");
		    sb.append("\n");
		 
		    sb.append(center(" File: " + p.getFileName(),80));
		    sb.append("\n");
		    sb.append(center(" Path:" + path,80));
		    sb.append("\n");
		    sb.append("                     creation time " + new Date());
		    sb.append("\n\n");
            
		    
		    printRow(" " , "Key", "Value", "Remarks");
		    printRow(" " , "--------------", "--------------", "--------------");
		    printRow("" + i++, "creationTime", attributes.get("creationTime"), "of file on disk ");
		    printRow("" + i++, "lastAccessTime", attributes.get("lastAccessTime"), "of file on disk ");
		    printRow("" + i++, "lastModifiedTime", attributes.get("lastModifiedTime"), "of file on disk ");
		    printRow("" + i++, "size",attributes.get("size"), "in bytes" );
		
		    sb.append("\n");
	           
			try {
			  	 //MessageDigest md = MessageDigest.getInstance("MD5");
				 String hexMD5 = md5hash; //checksum(path, md);
				 printRowShort("" + i++, "md5 ",hexMD5);

				 
				 //md = MessageDigest.getInstance("SHA-1");
				 //String hexSHA1 = ""; //checksum(path, md);
				 //printRowShort("" + i++, "sha1 ",hexSHA1);

				 //md = MessageDigest.getInstance("SHA-256");
				 String hex = sha256hash; //checksum(path, md);
				 printRowShort("" + i++, "sha256 ",hex);

				 
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} //SHA, MD2, MD5, SHA-256, SHA-384...
	  
		
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
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

			
			//checksum(path, md);
			 //printRowShort("" + i++, "sha256 ",hex);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 

    	
    }
	
	private void printRowShort(String c0, String c1, Object c2) {
	    sb.append(String.format("%s %-10s %-25s%n", c0, c1, String.valueOf(c2)));
	}

	private void printRow(String c0, String c1, Object c2, Object c3 ) {
	    sb.append(String.format("%s %-20s %-25s %-20s%n", c0, c1, String.valueOf(c2), c3 instanceof Integer ? "$" + c3 : c3));
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
	
	public static void main(String [] args)
	{
		FileInfo f = new FileInfo("/Users/pawlaszc/Desktop/FQLite/cookies.sqlite");
		
		System.out.println(f.sb);
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
