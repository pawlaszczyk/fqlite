package fqlite.ui.hexviewer;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Hashtable;
import java.util.concurrent.locks.ReentrantLock;

import fqlite.log.AppLog;
import goryachev.common.util.text.IBreakIterator;
import goryachev.fxtexteditor.Edit;
import goryachev.fxtexteditor.FxTextEditorModel;
import goryachev.fxtexteditor.ITextLine;
import goryachev.fxtexteditor.PlainTextLine;


/**
 * File-cached Plain Text FxTextEditorModel.
 * 
 * 
 */
public class FileCachePlainTextEditorModel extends FxTextEditorModel
{
	protected Hashtable<Integer,String> lines = null;
	private int lineCount; 
	private RandomAccessFile raf;
	private ReentrantLock lock;
	private long length = 0;
	
	
	public FileCachePlainTextEditorModel(File f)
	{
		lineCount = (int)(f.length()/16+1);
		lines =  new Hashtable<Integer,String>(); 
		lock = new ReentrantLock();
		this.length = f.length();
		
		try {
			raf = new RandomAccessFile(f,"r");
			loadLines(0,50);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public IBreakIterator getBreakIterator()
	{
		return null;
	}
	

	public int getLineCount()
	{
		return lineCount;
	}
	
	public long length(){
		return length;
	}
	

	
	protected String plainText(int line)
	{
		if(line < 0)
		{
			throw new IllegalArgumentException("line=" + line);
		}
		
		if(line < getLineCount())
		{
			int ix = line % lines.size();
			String s = lines.get(ix);
			if(s.length() > 0)
			{
				switch(s.charAt(s.length() - 1))
				{
				case '\r':
				case '\n':
					return s.substring(0, s.length() - 1);
				}
			}
			return s;
		}
		return null;
	}
	
	
	public ITextLine getTextLine(int line)
	{
		
		if(line >= lineCount)
			return null;
		
        lock.lock();
        try {
		
			String text =  lines.get(line);    
			if(text == null)
			{
				loadLines(line,50);
			}
		
			text = lines.get(line); 
			
			if(text != null)
			{
				return new PlainTextLine(line, text);					
			}
			else{
				return null;
			}
			
        }finally{
        	lock.unlock();
        }
			

    }
	
	public synchronized void loadLines(int start, int numberoflines){
		try
		{	
	      	readFromFile(start, numberoflines);
	      	return;
		}
		catch(Exception err){
            AppLog.error(err.getMessage());
		}
		finally
		{
			
		}
		
	}
	

	private static final char[] HEX = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'A', 'B', 'C', 'D', 'E', 'F' };
	  
	
	public synchronized void readFromFile(int linetostart, int numberoflines) throws Exception
	{
	        
			long total = numberoflines * 16L;
			char [] tarray = new char[16]; 
            int linenumber = linetostart;    
		
            lock.lock();
    	      
            StringBuilder sb = new StringBuilder(53);
		    if (linetostart > 0)
		    	raf.seek((linetostart) * 16L);
		    else
		    	raf.seek((linetostart) * 16L);
			
	        try {
		    
				int inhexline = 0;
				int k = 0;
				
			    for(int i = 0; i < total; i++) {
			    	
			    	if(inhexline == 0){
			    		sb.append(" \u2503 ");
			    	}
			    	
			    	int nextbyte = raf.read(); 
			    	if (nextbyte == -1)
			    	{
			    		return;
			    	}
			    	
			    	sb.append(HEX[(0xF0 & nextbyte) >>> 4]);
			    	sb.append(HEX[(0x0F & nextbyte)]);
			    	
			    	if ((nextbyte >= 32) && (nextbyte < 127))
				    {
				    	tarray[k] = (char)nextbyte;
				    }   
				    else
				    {
				    	tarray[k] = '.';
				    }
			    	k++;
			    	inhexline+=2;
			    
			    	if(inhexline == 32){
			    		inhexline = 0;
			    		sb.append(" \u2503 ");
			    		sb.append(tarray);
			    		sb.append("\n"); 
			    		lines.put(linenumber, new String(sb.toString()));
			    		sb = new StringBuilder(50);
			    		linenumber++;
			    		k=0;
			    		if (linenumber >= lineCount || lines.get(linenumber)!=null) 
			    			return; 
			    	}
			   }
	        }
			finally{
				  lock.unlock(); 
			}
		    
		    	
		    
		
		   
	}

	@Override
	public Edit edit(Edit arg0) throws Exception {
		return null;
	}
	

	
	

}


