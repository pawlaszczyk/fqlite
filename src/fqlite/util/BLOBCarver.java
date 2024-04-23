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
	static final String svg_header   = "3c737667";
	static final String plist_header = "62706c697374";
	static final String gzip_header = "1f8b";
	
	static final String ftypheic_header ="6674797068656963"; //the usual HEIF images
	static final String ftypheix_header ="6674797068656978"; //10bit images, or anything that uses h265 with range extension
	static final String ftyphevc_header ="6674797068657663"; //brands for image sequences
	static final String ftyphevx_header ="6674797068657678"; //brands for image sequences
	static final String ftypheim_header ="667479706865696d"; //multi view
	static final String ftypheis_header ="6674797068656973"; //scalable
	static final String ftyphevm_header ="667479706865766d"; //multiview sequence
	static final String ftyphevs_header ="6674797068657673"; //scalable sequence 
	
	public static boolean isHEIC(String s)
	{
		if (s.contains(ftypheic_header))
			return true;
		if (s.contains(ftypheix_header))
			return true;
		if (s.contains(ftyphevc_header))
			return true;
		if (s.contains(ftyphevx_header))
			return true;
		if (s.contains(ftypheim_header))
			return true;
		if (s.contains(ftypheis_header))
			return true;
		if (s.contains(ftyphevm_header))
			return true;
		if (s.contains(ftyphevs_header))
			return true;
		return false;
	}
	
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
	
	public static boolean isSVG(String s)
	{
	   return s.startsWith(svg_header);
	}
	
	public static boolean isTIFF(String s)
	{
	   return s.startsWith(tiff_header);
	}
	
	public static boolean isBMP(String s)
	{
	   return s.startsWith(bmp_header);
	}
	
	public static boolean isPLIST(String s)
	{
		return s.startsWith(plist_header);
	}
	
	public static boolean isGZIP(String s)
	{
		return s.startsWith(gzip_header);
	}
	
	
	public static boolean isICO(String s)
	{		
	  return s.startsWith(ico_header);
	}
	
}
