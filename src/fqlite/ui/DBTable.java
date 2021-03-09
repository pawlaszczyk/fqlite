package fqlite.ui;

import java.awt.event.MouseEvent;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.table.*;

import fqlite.base.GUI;
import fqlite.base.Global;

/**
 * Just a Wrapper class for a JTable. It is used to return a custom cell
 * renderer.
 * 
 * @author pawlaszc
 *
 */
public class DBTable extends JTable {

	boolean isWALTable = false;
	private static final long serialVersionUID = 1L;
	final CustomCellRenderer renderer = new CustomCellRenderer();

	public DBTable(TableModel dm, boolean isWALTable, GUI gui) {
		this(dm);  
		this.isWALTable = isWALTable;
		if (isWALTable)
		{
			renderer.walnode = true;
			renderer.gui = gui;
		}
	}	
	
	public DBTable(TableModel dm) {
	   super(dm);
	   setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
       setColumnSelectionAllowed(false);
	   setRowSelectionAllowed(true);
	}

	@Override
	public TableCellRenderer getCellRenderer(int row, int column) {

		return renderer;
	}

	@Override
	public boolean getScrollableTracksViewportWidth() {
		return getPreferredSize().width < getParent().getWidth();
	}

	@Override
	public void doLayout() {
		TableColumn resizingColumn = null;

		if (tableHeader != null)
			resizingColumn = tableHeader.getResizingColumn();

		// Viewport size changed. May need to increase columns widths

		if (resizingColumn == null) {
			setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
			super.doLayout();
		}

		// Specific column resized. Reset preferred widths

		else {
			TableColumnModel tcm = getColumnModel();

			for (int i = 0; i < tcm.getColumnCount(); i++) {
				TableColumn tc = tcm.getColumn(i);
				tc.setPreferredWidth(tc.getWidth());
			}

			// Columns don't fill the viewport, invoke default layout

			if (tcm.getTotalColumnWidth() < getParent().getWidth())
				setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
			super.doLayout();
		}

		setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
	}

	@Override
	public String getToolTipText(MouseEvent e) {
		String tip = null;
		java.awt.Point p = e.getPoint();

		int rowIndex = rowAtPoint(p);
		int colIndex = columnAtPoint(p);
		
		if (colIndex == 0)
		{
			String status = (String)this.getModel().getValueAt(rowIndex,0);
			
			tip = "<html>";
			
			if(status.contains(Global.UNALLOCATED_SPACE))
			{ // place of interest - unallocated space
			  tip += "\u2318 - This record is located in the unallocated area of a database page. <br>";
			}	
			else if(status.contains(Global.FREELIST_ENTRY))
			{ 
				//\u267D // particaly recycled - free list
				
				tip += "\u267D - This record is located in a free list page of the database <br>";
			}	
			else if(status.contains(Global.REGULAR_RECORD))
			{ 
				tip += "This is a complete cell record. <br>";
			}		
			else if(status.contains(Global.DELETED_RECORD_IN_PAGE))
			{ 
				tip += "\u2718 - This is a deleted cell record. It could be partly overriden. <br>";
			}
			if(status.contains("VT"))
			{
				tip += "This is a VIRTUAL TABLE entry.";
			}
			
			
			tip += "</html>";
			
		}

		
		
		return tip;
	}
	
	 
}
