package fqlite.ui;

import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.Transferable;
import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;
import javax.swing.JTextArea;

public class CopyActionTA extends AbstractAction {

    /**
	 * 
	 */
	private static final long serialVersionUID = -5863604468463177615L;
	private JTextArea ta;

    public CopyActionTA(JTextArea ta) {
        this.ta = ta;
        putValue(NAME, "Copy");
    }

    @Override
    public void actionPerformed(ActionEvent e) {
     
        Clipboard cb = Toolkit.getDefaultToolkit().getSystemClipboard();
      
       
        StringBuffer selection = new StringBuffer();
      
        selection.append(ta.getSelectedText());
        
        Transferable transferable = new StringSelection(selection.toString());
        cb.setContents(transferable,null);
        
    }

}