package fqlite.ui;

/**
 * 
 * @author       D. Pawlaszczyk
 * @version      1.1
 */
import java.io.IOException;
import java.nio.ByteBuffer;

import fqlite.base.Job;
import fqlite.util.Auxiliary;

public class HexDumpFactory
{

	
	
 /**
  * returns the "HexDump" of the specified file, i.e, 
  * per line, 16 bytes of the file are dumped in the form of two hex digits 
  *, next to it 8 spaces and then the printable characters 
  * in plain text and the non-printable characters replaced by periods. 
  * 
  * @return String with HexDump for the current file loaded
  * @throws IOException
  */
public static String dump(Job job) throws IOException
  {
    StringBuffer bf = new StringBuffer(job.db.capacity()*2);
	
    final String tabulator = "    ";

    String dump = "";
    String hexline = "";
    String txtline = "";
    ByteBuffer in = job.db;
    in.position(0);
     
    int of = 0;
    
    long lines = in.limit() / 16;
    int width = 0;
    
    if (lines < 65535)
    	width = 4;
    else if (lines < 16777215)
        width = 6;
    else if (lines < 4294967295L)
    	width = 8;
    else 
    	width = 10;
    do
    {
	    byte[ ] line = new byte[16];
	    in.get(line);
	    
	    hexline += Auxiliary.bytesToHex(line);
	    	
	    for(int x = 0; x < line.length; x++)
	    {
	        int byt = line[x];
	        
	        if ((byt >= 32) && (byt < 127))
	        { // printable charaters to 
	          txtline = txtline + ((char)byt);
	        }
	        else
	        { // for non-printable chars write a '.' 
	          txtline = txtline + ".";
	        }
	      
	    }
	    
	    
	    String offset = String.format("%0" + width + "X|",of);
	    
	    of += 16;
	    	    
	    bf.append(offset);
	    bf.append(" ");
	    bf.append(hexline);
	    bf.append(tabulator);
	    bf.append(txtline);
	    bf.append("\n");
        
        
	    // start with a new line
        hexline = "";
        txtline = "";
      
	    
    } while((in.limit() - in.position()) >= 16 );
    
    return dump;
  }

  

}
