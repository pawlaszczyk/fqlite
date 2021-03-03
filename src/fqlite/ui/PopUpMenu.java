package fqlite.ui;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.net.URL;

import javax.swing.ImageIcon;
import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;
import javax.swing.KeyStroke;

import fqlite.base.GUI;



/**
 * Constructs a context-menu for the UI.
 * 
 * @author pawlaszc
 *
 */
public class PopUpMenu extends JPopupMenu{


	private static final long serialVersionUID = 8313149301533565783L;

	JMenuItem close;
	JMenuItem export;
	GUI gui;
	
    public PopUpMenu(GUI g, NodeObject no) {
    	this.gui = g;
    	URL url = GUI.class.getResource("/closeDB_gray.png");
        close = new JMenuItem("Close Database",new ImageIcon(url));
       
        
        JMenuItem mntopen = new JMenuItem("Open Database...");
		mntopen.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_O, ActionEvent.META_MASK));
		mntopen.getAccessibleContext().setAccessibleDescription("Open a sqlite database file to analyse...");
		mntopen.setMnemonic(KeyEvent.VK_O);
		mntopen.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				gui.open_db();
			}
		});
		add(mntopen);

        String type; 		
		switch(no.tabletype)
		{
			case 0   :	type = "Table";
						break;
			case 1   : 	type = "Index Table";
						break;
			case 99  :  type = "Database";
						break;
			case 100 :  type = "Journal-File";
						break; 			
			case 101 :  type = "WAL-File";
						break;
			default  :  type = "root";
		}
		
		
		JMenuItem mntmExport = new JMenuItem("Export " + type + "...");
		mntmExport.setMnemonic(KeyEvent.VK_X);
		mntmExport.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_X, ActionEvent.META_MASK));
		mntmExport.getAccessibleContext().setAccessibleDescription("Start export of a " + type + " to csv...");
		mntmExport.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				gui.doExport();
			}
		});
		add(mntmExport);
        
		addSeparator();

		JMenuItem mntclose = new JMenuItem("Close All");
		mntclose.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_D, ActionEvent.CTRL_MASK));
		mntclose.getAccessibleContext().setAccessibleDescription("Close All currently open databases");
		mntclose.setMnemonic(KeyEvent.VK_D);
		mntclose.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				gui.closeAll();
			}
		});
		add(mntclose);

		addSeparator();

		JMenuItem mntmExit = new JMenuItem("Exit");
		mntmExit.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_F4, ActionEvent.ALT_MASK));
		mntmExit.setMnemonic(KeyEvent.VK_F4);
		mntmExit.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				System.exit(0);
			}
		});
		add(mntmExit);

    }
	
	
}
