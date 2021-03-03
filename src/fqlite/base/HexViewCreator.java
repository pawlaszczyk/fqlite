package fqlite.base;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.concurrent.Future;

import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.tree.TreePath;

import fqlite.ui.HexView;
import fqlite.util.Auxiliary;


/**
 * This class is used to create a new HexView window for a particular
 * database. Creating a new window is done in background (SwingWorker). 
 * 
 * @author pawlaszc
 *
 */
public class HexViewCreator extends SwingWorker<Boolean, Void>{

	String hexline = "";
	String txtline = "";
	String soffset = "";
	String filename = "";
	int width = 0;
    AsynchronousFileChannel file;
    int type = 0;
	
	Job job;
	TreePath path;
	
	/**
	 * 
	 * @param job
	 * @param path
	 * @param file
	 * @param filename
	 * @param type
	 */
	public HexViewCreator(Job job, TreePath path, AsynchronousFileChannel file, String filename, int type) {
		super();
		this.job = job;
		this.path = path;
		this.file = file;  // reference to file object of the database, wal or rollback
		this.filename = filename;
		this.type = type;
	}

	
	
	/**
     * Computes a result, or throws an exception if unable to do so.
     *
     * <p>
     * Note that this method is executed only once.
     *
     * <p>
     * Note: this method is executed in a background thread.
     *
     *
     * @return the computed result
     * @throws Exception if unable to compute a result
     *
     */
	@Override
	protected Boolean doInBackground() throws Exception {
		create(job);
		return true;
	}
	

	/**
     * Executed on the <i>Event Dispatch Thread</i> after the {@code doInBackground}
     * method is finished. The default
     * implementation does nothing. Subclasses may override this method to
     * perform completion actions on the <i>Event Dispatch Thread</i>. Note
     * that you can query status inside the implementation of this method to
     * determine the result of this task or whether this task has been cancelled.
     *
     * @see #doInBackground
     * @see #isCancelled()
     * @see #get
     */
	@Override
	protected void done() {
		
		
	}
	
	
	
	/**
	 * returns the "HexDump" of the specified file, i.e, per line, 16 bytes of the
	 * file are dumped in the form of two hex digits , next to it 8 spaces and then
	 * the printable characters in plain text and the non-printable characters
	 * replaced by periods.
	 * 
	 * @return String with HexDump for the current file loaded
	 * @throws IOException
	 */
	public void create(Job job) throws IOException {

		
		ByteBuffer in = readFileIntoBuffer(job);
		in.position(0);

		int length = 0;
		length = in.capacity();
			
		byte [] line = new byte[length];
		
		in.get(line);
		
		
				hexline = Auxiliary.bytesToHex(line);
		
	
		for (int x = 0; x < line.length; x++)
		{
			byte b = line[x];
			if((b < 32) || (b>=127))
			{
				line[x]= 46;
			}		
		}
		
		txtline = new String(line, "ASCII");
			
		int of = 0;
		
		
		int lines = txtline.length()/16;
		
		StringBuffer off = new StringBuffer();

		for (int i = 0; i < lines; i++)
	    {	
			//	String offset = String.valueOf(of); //String.format("%0" + width + "X", of);
	    	off.append(of);
	    	off.append("\n");
			of += 16;
	    }	
		soffset = off.toString();
		
		width = String.valueOf(of).length();
		

		try {
			createDialog();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * This method is used to read the database file into RAM.
	 * @return a read-only ByteBuffer representing the db content
	 * @throws IOException
	 */
	private ByteBuffer readFileIntoBuffer(Job job) throws IOException {
		/* read the complete file into a ByteBuffer */
		long size = file.size();
		
		ByteBuffer db = ByteBuffer.allocateDirect((int) size);
		
		Future<Integer> result = file.read(db, 0); // position = 0

		while (!result.isDone()) {

			// we can do something in between or we can do nothing ;-).
		}

		// set filepointer to begin of the file
		db.position(0);

		return db;
	}
	
	/**
	 * Creates a new HexView-UI object. It is just a JDialog.
	 * 
	 * @throws Exception
	 */
	void createDialog() throws Exception {
	    Runnable showDialog = new Runnable() {
	        public void run() {
	        	HexView dialog = new HexView(filename, job.gui, soffset, hexline, txtline, width);

	        	if (type == 0)
	        		job.hexview = dialog;
	        	else if (type == 1)
	        		job.wal.hexview = dialog;
	        	else if (type == 2)
	        		job.rol.hexview = dialog;
	        	
                dialog.setVisible(false);
	        }
	    };
	    SwingUtilities.invokeLater(showDialog);
	}
	
	
	
	
}
