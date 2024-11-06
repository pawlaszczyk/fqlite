package fqlite.ui;

import fqlite.analyzer.ConverterFactory;
import fqlite.analyzer.Names;
import fqlite.analyzer.avro.Avro;
import fqlite.analyzer.javaserial.Deserializer;
import fqlite.analyzer.pblist.BPListParser;
import fqlite.analyzer.telegram.TelegramDecoder;
import fqlite.base.GUI;
import fqlite.base.Global;
import fqlite.base.Job;
import fqlite.util.Auxiliary;
import javafx.scene.control.Cell;
import javafx.scene.control.TableCell;
import javafx.scene.control.Tooltip;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.util.StringConverter;

/**
 * Custom tooltip implementation.
 */
@SuppressWarnings("rawtypes")
class CustomTooltip extends Tooltip {
 
	private String coltype;
	private TableCell tc;
	private Cell cell;
	private String text;
	private Job job;
	private String tablename;
	private long off;
	
	public CustomTooltip() {
        super();
    }

    public CustomTooltip(String txt) {
        super(txt);
    }
    
    public CustomTooltip(String tablename, long off, Job job, String tttype, String txt, TableCell tc, final Cell cell, final StringConverter converter) {
        super(txt);
        this.text = txt;
        this.tc = tc;
        this.cell = cell;
        this.coltype = tttype;
        this.job = job;
        this.tablename = tablename;
        this.off = off;
    }
    
	public void addCellText(String tablename,long off, Job job, String tttype, String txt, TableCell tc, Cell cell,StringConverter converter){
    	 this.text = txt;
    	 this.tc = tc;
         this.cell = cell;
         this.coltype = tttype;
         this.job = job;
         this.tablename = tablename;
         this.off = off;
    }

    
    @Override
    protected void show() {
         setGraphic(null);
         setText(null);
    	 setToolTipText();
         super.show();
        
    }

    /**
     * This is the central method for preparation of
     * the ToolTip content.
     *  
     * Depending on the column type we have a slightly
     * different output.  
     */
    private void setToolTipText(){
    	  	
    	String colname = tc.getTableColumn().getText();
    	
    	if(colname.equals("")){		
            String s = "state (D: deleted, F: freelist)"; //getItemText(cell, converter);
        	setText(s);     
            return;
    	}
    	
    	if(colname.equals("Offset")){
            setText("byte position");     
            return;
    	}
    	
    	if(colname.equals("PLL")){
    		 setText("payload length");     
             return;
    	}
    	
    	if(colname.equals("HL")){
    		 setText("header length");     
             return;
    	}	
    	
    	
    	if(coltype == null){
    		coltype = "";
    	}
    	
    	/* get the cell text */
    	String s = text; //(String)cell.getItem();
    	
    	if(coltype.equals("REAL") || coltype.equals("DOUBLE") || coltype.equals("FLOAT") || coltype.equals("TIMESTAMP") ) {

        	int point = s.indexOf(",");
            String firstpart;
        	if (point > 0)
            	firstpart = s.substring(0, point);
            else
            	firstpart = s;
           
        	String value = Auxiliary.int2Timestamp(firstpart);
        	setText("[" + coltype + "] " +  s + "\n" + value );
    		return;
    	}
    	
    	if(coltype.equals("INTEGER") || coltype.equals("INT") || coltype.equals("BIGINT") || coltype.equals("LONG") || coltype.equals("TINYINT") || coltype.equals("INTUNSIGNED") || coltype.equals("INTSIGNED") || coltype.equals("MEDIUMINT") || coltype.equals("TIMESTAMP")) {
    		        	
    		String value = Auxiliary.int2Timestamp(s);
    		setText("[" + coltype + "] " +  s + "\n" + value );
    		return;
    	}
    	
    	if(s.contains("[BLOB")){
    		
         	int from = s.indexOf("BLOB-");
    	    int to = s.indexOf("]");
    	    String number = s.substring(from+5, to);
    	    String shash = off + "-" + number;
    	    
    	    if(s.contains("jpg"))
    	    	shash += ".jpg";
    	    else if(s.contains("png"))
    	    	shash += ".png";
    	    else if(s.contains("gif"))
    	    	shash += ".gif";
    	    else if(s.contains("bmp"))
    	    	shash += ".bmp";
    	    
    	    String key = GUI.baseDir + Global.separator + job.filename + "_" +  shash;
    	    /* Is there a thumbnail picture in the cache ?*/     	  
    	    Image ii = job.Thumbnails.get(key);
    	    
    	    
    	    boolean bson 	 = false;
    	    boolean fleece 	 = false;
    	    boolean msgpack  = false;
    	    boolean thriftb  = false;
    	    boolean thriftc  = false;
    	    boolean protobuf = false;
    	    
    	    /* check, wether the user has activated a converter for binary 
    	     * columns for this table 
    	     */
    	    if (job.convertto.containsKey(tablename)){
    	    	
    	    	String con = job.convertto.get(tablename);
    	    	
    	    	switch(con){
    	    	
	    	    	case Names.BSON   : 	 	bson = true;
	    	    					    	 	break;
	    	    	case Names.Fleece : 	 	fleece = true;
					  						 	break;
	    	    	case Names.MessagePack  : 	msgpack = true;
					  						 	break;
	    	    	case Names.ProtoBuffer	: 	protobuf = true;
	    	    							 	break;
	    	    	case Names.ThriftBinary : 	thriftb = true;
						 					 	break;
	    	    	case Names.ThriftCompact : 	thriftc = true;
					 						 	break;
	    	    	
    	    	}
    	    }
    	    
    	    /* image file -> show picture in tool tip and leave method */        	    
    	    if(null != ii)
    	    {
   	
  	    		ImageView iv = new ImageView(ii);
	            setGraphic(iv);
	            return;
    	    }
    	    else
    	    {	
    	    	/**
    	    	 * Telegram-BLOB?
    	    	 */
    	    	if(job.filename.contains("cache4.db")) {
    	    	
    	    		try {
	    	    		switch(tablename){
	    	    		
	    	    			case "user_settings" : 	
													String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".bin";
													String text = job.bincache.getHexString(path);
													String decoded = TelegramDecoder.decodeUserFull(text);
						    	    				setText(decoded);
						    	    				return;
							
	    	    		
	    	    			case "messages_v2" : 	
	    	    									path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".bin";
	    	    									text = job.bincache.getHexString(path);
	    	    									decoded = TelegramDecoder.decodeMessage(text);
						    	    				setText(decoded);
						    	    				return;
	    	    								
	    	    			case "users" : 
	    	    									path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".bin";		
	    	    									text = job.bincache.getHexString(path);
	    	    									decoded = TelegramDecoder.decodeUser(text);
	    	    									setText(decoded);
	    	    									return;
	    	    									
	    	    			case "app_config" :     
						    	    				path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".bin";		
													text = job.bincache.getHexString(path);
													decoded = TelegramDecoder.decodeAppConfig(text);
													setText(decoded);
													return;
													
	    	    		}
    	    		}catch(Exception err){
    	    			setText("This version of telegram is not supported.");
    	    			return;
    	    		}
    	    		
    	    	}
    	    	
    	    	
    	    	if(s.contains("java"))
    	    	{
    	    		String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".bin";
    	    		
    	    		if (Auxiliary.writeBLOB2Disk(job, path)) {
    	    			
    	    			String javaclass = Deserializer.decode(path); 
    	           	
	    	           	if(javaclass.length() > 2000){
	    	           		javaclass = javaclass.substring(0,2000);
	    	           	}
    	           	
	    	           	setText(javaclass);
	    	           	setWrapText(true);
	    	           	prefWidthProperty().bind(cell.widthProperty());
    	    			return;
    	    		}
	    	            
    	    	}    	
    	    	else if(s.contains("plist"))
    	    	{
    	            String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".plist";
    	           	
    	            	String plist = BPListParser.parse(job,path); 
    	           	
    	            	if(plist.length() > 1000){
    	            		plist = plist.substring(0,1000);
    	            	}
    	           	
    	            	setText(plist);
    	            	return;
    	          
    	    	}
    	    	else if(s.contains("avro")) {
    	    		
    	    		  try {
    	    			  
    	    			  String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".avro";
    	    		     
    	    			  if (Auxiliary.writeBLOB2Disk(job, path)) {
    	    	    			
	    	      	    		
	    	    			  String buffer = Avro.decode(path);
	     	    	          setWrapText(true);
	     	    	          prefWidthProperty().bind(cell.widthProperty());
	     	    	          setText(buffer);
	     	    	          return;
    	    			  }
	     	    	          
    	    		  } catch (Exception e) {
    	    		    throw new RuntimeException(e);
    	    		  }
    	    		
    	    		
    	    	}
    	    	
    	    	else if(fleece)
    	    	{
    	        	String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".bin";

    	            if (Auxiliary.writeBLOB2Disk(job, path)) {
	    	        	/* inspection is enabled */
	    	        	String result = ConverterFactory.build(Names.Fleece).decode(job,path);
	    	         	if (null != result) {
	    	                setText(result);
			    	        setWrapText(true);
			    	        prefWidthProperty().bind(cell.widthProperty());
			    	        return;
	    	        	}
			    	    else {
			    	        setText("invalid value");   
			    	        return;
			    	    }
    	            }
    	    	}
    	    	
    	    	else if(protobuf)
    	    	{
    	        	String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".bin";
    	
    	        	if (Auxiliary.writeBLOB2Disk(job, path)) {    
	    	        	/* inspection is enabled */
	    	        	String buffer = ConverterFactory.build(Names.ProtoBuffer).decode(job,path);
	    	        	setText(buffer);
	 	    	        setWrapText(true);
	 		    	    prefWidthProperty().bind(cell.widthProperty());
	 		    	    return;
    	        	}
    	    	}
    	    	
    	    	else if(thriftb)
    	    	{
    	        	String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".bin";
    	        	
    	        	if (Auxiliary.writeBLOB2Disk(job, path)) {    
	    	        	/* inspection is enabled */
	    	        	String buffer = ConverterFactory.build(Names.ThriftBinary).decode(job,path);
	    	        	setText(buffer);
	 	    	        setWrapText(true);
	 		    	    prefWidthProperty().bind(cell.widthProperty());
	 		    	    return;
    	        	}
    	    	}
    	    	
    	    	else if(thriftc)
    	    	{        	    	
    	        	String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".bin";
    	        	
    	        	if (Auxiliary.writeBLOB2Disk(job, path)) {        
	    	        	/* inspection is enabled */
	    	        	String buffer = ConverterFactory.build(Names.ThriftCompact).decode(job,path);      
		    	        setText(buffer);
		    	        setWrapText(true);
			    	    prefWidthProperty().bind(cell.widthProperty());
			    	    return;
    	        	}
    	    	}
    	    	
    	    	else if(msgpack)
    	    	{
    	        	String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".bin";
    	        	
    	        	if (Auxiliary.writeBLOB2Disk(job, path)) {        
	    	        	/* inspection is enabled */
	    	        	String buffer = ConverterFactory.build(Names.MessagePack).decode(job,path);
	    	            setText(buffer);
	    	           	setWrapText(true);
		    	        prefWidthProperty().bind(cell.widthProperty());
		    	        return;
    	        	}
    	        }
    	    	
    	    	else if(bson)
    	    	{
    	        	String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + ".bin";
    	        	
    	        	if (Auxiliary.writeBLOB2Disk(job, path)) {        
	    	        	/* inspection is enabled */
	    	        	String buffer = ConverterFactory.build(Names.BSON).decode(job,path);
	    	        	setText(buffer);
	    	           	setWrapText(true);
		    	        prefWidthProperty().bind(cell.widthProperty());
		    	        return;
    	        	}
		    	  
    	       }
    	    	/* this binary formats cannot be viewed inside a ToolTip. We need to call the viewer from
    	    	 * the operating system.
    	    	 */
    	        else if(s.contains("pdf") || s.contains("heic") || s.contains("tiff")) {
    	    	       
	    	        setText("double-click to preview.");
	    	        setWrapText(true);
		    	    prefWidthProperty().bind(cell.widthProperty());
		    	    return;
		    	  
    	    	}
    	     	else
    	    	{
    	            
    	            String fext = ".bin";
    	         
    	            if(s.contains("<tiff>"))
    	            		fext = ".tiff";
    	            else if(s.contains("<pdf>"))
    	            		fext = ".pdf";
    	            else if(s.contains("<heic>"))
	            		fext = ".heic";
    	            else if(s.contains("<gzip>"))
	            		fext = ".gzip";
    	            else if(s.contains("<avro>"))
    	            	fext = ".avro";
    	            else if(s.contains("<jpg>"))
    	            	fext = ".jpg";
    	            else if(s.contains("<bmp>"))
    	            	fext = ".bmp";
    	            else if(s.contains("<png>"))
    	            	fext = ".png";
    	            else if(s.contains("<gif>"))
    	            	fext = ".gif"; 
    	            else if(s.contains("<gzip>"))
    	            	fext = ".gzip"; 
    	            else if(s.contains("<plist>"))
    	            	fext = ".plist"; 
    	       
             
    	            String path = GUI.baseDir + Global.separator + job.filename + "_" + off + "-" + number + fext;
    	            String text = job.bincache.getASCII(path);
    	    	    
    	            // only show the first 2000 characters - it's just a tooltip ;-)
    	    		if(text.length() > 2000)
    	    			text = text.substring(0,2000);
    	    	
    	    		
    	    		//Add text as "tooltip" so that user can read text without editing it.
    	    		setText(text);
     	           	setWrapText(true);
 	    	        prefWidthProperty().bind(cell.widthProperty());
    	     	            
    	            s = GUI.class.getResource("/hex-32.png").toExternalForm();
    	    		ImageView iv = new ImageView(s);
    	    		setGraphic(iv);   
    	    		return;
    	    	}
    	    
    	    }

    	}
    	if (coltype != null && coltype.length()>0)
    		setText("[" +coltype + "] " + tc.getText());
    	else
    		setText(tc.getText());
    }
    

}
