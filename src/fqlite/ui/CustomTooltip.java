package fqlite.ui;

import fqlite.base.GUI;
import fqlite.base.Job;
import fqlite.types.BLOBElement;
import fqlite.types.BLOBTYPE;
import fqlite.util.Auxiliary;
import javafx.scene.control.Cell;
import javafx.scene.control.TableCell;
import javafx.scene.control.Tooltip;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.util.StringConverter;
import java.io.ByteArrayInputStream;
import java.util.Objects;

/**
 * Custom tooltip implementation.
 *
 * @author pawel
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
    private void setToolTipText() {

		String colname = tc.getTableColumn().getText();

		if (colname.isEmpty()) {
			String s = "state (D: deleted, F: freelist)"; //getItemText(cell, converter);
			setText(s);
			return;
		}

		if (colname.equals("_OFFSET")) {
			setText("byte position");
			return;
		}

		if (colname.equals("_PLL")) {
			setText("payload length + header length");
			return;
		}

		if (colname.equals("_ROWID")) {
			setText("unique internal data record number");
			return;
		}


		if (coltype == null) {
			coltype = "";
		}

		/* get the cell text */
		String s = text; //(String)cell.getItem();

		if (coltype.equals("REAL") || coltype.equals("DOUBLE") || coltype.equals("FLOAT") || coltype.equals("TIMESTAMP")) {

			int point = s.indexOf(",");
			if (point < 0) {
				point = s.indexOf(".");
			}

			String firstpart;
			if (point > 0)
				firstpart = s.substring(0, point);
			else
				firstpart = s;

			String value = Auxiliary.int2Timestamp(firstpart);
			setText("[" + coltype + "] " + s + "\n" + value);
			return;
		}

		coltype = coltype.toUpperCase();

		if (coltype.equals("INTEGER") || coltype.equals("INT") || coltype.equals("BIGINT") || coltype.equals("LONG") || coltype.equals("TINYINT") || coltype.equals("INTUNSIGNED") || coltype.equals("INTSIGNED") || coltype.equals("MEDIUMINT") || coltype.equals("TIMESTAMP") || coltype.equals("DATE"))
		{

			String value = "";
			if (s.contains("."))
				value =	Auxiliary.convertTimestamp(s);
			else
				value = Auxiliary.int2Timestamp(s);
			setText("[" + coltype + "] " + s + "\n" + value);
			return;
		}

		if (s.contains("[BLOB")) {

			int from = s.indexOf("BLOB-");
			int to = s.indexOf("]");
			String number = s.substring(from + 5, to);
			String shash = off + "-" + number;

			/* image file -> show picture in tool tip and leave method */
			BLOBElement blob = job.bincache.get(shash);
			if (blob != null && (blob.type == BLOBTYPE.GIF || blob.type == BLOBTYPE.JPG || blob.type == BLOBTYPE.PNG || blob.type == BLOBTYPE.TIFF  || blob.type == BLOBTYPE.BMP)) {

				Image image = new Image(new ByteArrayInputStream(blob.binary));
				ImageView iv = new ImageView();
				iv.setImage(image);
				iv.setFitHeight(64);
				iv.setFitWidth(64);
				setGraphic(iv);
				return;
			}
			else if (blob.type == BLOBTYPE.TIFF || blob.type == BLOBTYPE.PDF || blob.type == BLOBTYPE.HEIC) {
				setText("Type: " +  blob.type + "\n Double-Click to view.");
				setWrapText(true);
				return;
			}




			//Add text as "tooltip" so that the user can read text without editing it.
			setText("BLOB value");
			setWrapText(true);
			prefWidthProperty().bind(cell.widthProperty());

			s = Objects.requireNonNull(GUI.class.getResource("/hex-32.png")).toExternalForm();
			ImageView iv = new ImageView(s);
			iv.setFitWidth(32);
			iv.setFitHeight(32);
			setGraphic(iv);


		}

		// for all remaining cell values just print the SQL-type
		setText("[" + coltype + "]");
	}

}
