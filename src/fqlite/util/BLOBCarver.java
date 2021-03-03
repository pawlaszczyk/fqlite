package fqlite.util;

public class BLOBCarver {

	static final String jpeg_header  = "ffd8ff";
	static final String gif87a_header  = "474946383761";  
	static final String gif89a_header  = "474946383961";
	static final String png_header  = "89504e470d0a1a0a";
	static final String ico_header  = "00000100";
	static final String pdf_header   = "25504446";
	static final String bmp_header   = "424d";
	static final String tiff_header  = "492049";
    
	
	public static boolean isGraphic(String s)
	{
		return isJPEG(s) || isICO(s) || isGIF(s) || isPNG(s); 
	}
	
	public static boolean isPNG(String s)
	{
	  return s.startsWith(png_header);
	}
	
	public static boolean isJPEG(String s)
	{
	  return s.startsWith(jpeg_header);
	}

	public static boolean isPDF(String s)
	{
	  return s.startsWith(pdf_header);
	}

	public static boolean isGIF(String s)
	{
	  return s.startsWith(gif87a_header) || s.startsWith(gif89a_header);
	}
	
	public static boolean isICO(String s)
	{		
	  return s.startsWith(ico_header);
	}
	
}
