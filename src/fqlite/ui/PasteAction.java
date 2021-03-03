package fqlite.ui;

import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.FlavorEvent;
import java.awt.datatransfer.FlavorListener;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.awt.event.ActionEvent;
import java.io.IOException;
import javax.swing.AbstractAction;
import javax.swing.JTable;

public class PasteAction extends AbstractAction {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private JTable table;

    public PasteAction(JTable table) {
        this.table = table;
        putValue(NAME, "Paste");
        final Clipboard cb = Toolkit.getDefaultToolkit().getSystemClipboard();
        cb.addFlavorListener(new FlavorListener() {
			
            @Override
            public void flavorsChanged(FlavorEvent e) {
                setEnabled(cb.isDataFlavorAvailable(CellTransferable.CELL_DATA_FLAVOR));
            }
        });
        setEnabled(cb.isDataFlavorAvailable(CellTransferable.CELL_DATA_FLAVOR));
    }
    
    @Override
    public void actionPerformed(ActionEvent e) {
        int row = table.getSelectedRow();
        int col = table.getSelectedColumn();

        Clipboard cb = Toolkit.getDefaultToolkit().getSystemClipboard();
        if (cb.isDataFlavorAvailable(CellTransferable.CELL_DATA_FLAVOR)) {
            try {
                Object value = cb.getData(CellTransferable.CELL_DATA_FLAVOR);
                table.setValueAt(value, row, col);
            } catch (UnsupportedFlavorException | IOException ex) {
                ex.printStackTrace();
            }
        }
    }

}    