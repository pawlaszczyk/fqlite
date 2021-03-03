package fqlite.ui;

import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.Transferable;
import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;
import javax.swing.JTable;

public class CopyAction extends AbstractAction {

    /**
	 * 
	 */
	private static final long serialVersionUID = -5863604468463177615L;
	private JTable table;

    public CopyAction(JTable table) {
        this.table = table;
        putValue(NAME, "Copy");
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        //int row = table.getSelectedRow();
        //int col = table.getSelectedColumn();

        Clipboard cb = Toolkit.getDefaultToolkit().getSystemClipboard();
        //cb.setContents(new CellTransferable(component.getValueAt(row, col)), null);
        //Transferable transferable = new StringSelection((String)table.getValueAt(row, col));
        //cb.setContents(transferable,null);
        
        
        int [] rows = table.getSelectedRows();
        int [] columns = table.getSelectedColumns();
    
       
        StringBuffer selection = new StringBuffer();
        for (int r: rows)
        {
        	for (int c: columns)
        	{
        			selection.append((String)table.getValueAt(r, c)); 
        			selection.append(";");
        	}
			selection.append("\n");

        }
        Transferable transferable = new StringSelection(selection.toString());
        cb.setContents(transferable,null);
        
    }

}