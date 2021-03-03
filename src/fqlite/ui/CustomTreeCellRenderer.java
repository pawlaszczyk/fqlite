package fqlite.ui;

import java.awt.Component;
import java.awt.Toolkit;
import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;

import fqlite.base.GUI;

/**
 * A specialized tree node renderer with colored table nodes.
 * 
 * @author pawlaszc
 *
 */
public class CustomTreeCellRenderer extends DefaultTreeCellRenderer {

	private static final long serialVersionUID = 967420122770627719L;
	private static final String SPAN_FORMAT = "<span style='color:%s;'>%s</span>";
	
	private Icon icon_table;
	private Icon icon_database;
	private Icon icon_index;
	private Icon icon_wal;
	private Icon icon_journal;
	private Icon icon_root;
	
	public CustomTreeCellRenderer()
	{
		super();
		
		icon_table = new ImageIcon(Toolkit.getDefaultToolkit().getImage(GUI.class.getResource("/table-icon.png")));
		icon_database = new ImageIcon(Toolkit.getDefaultToolkit().getImage(GUI.class.getResource("/database-small-icon.png")));
        icon_index = new ImageIcon(Toolkit.getDefaultToolkit().getImage(GUI.class.getResource("/table-key-icon.png")));
        icon_wal = new ImageIcon(Toolkit.getDefaultToolkit().getImage(GUI.class.getResource("/wal-icon.png")));
        icon_journal = new ImageIcon(Toolkit.getDefaultToolkit().getImage(GUI.class.getResource("/journal-icon.png")));
        icon_root = new ImageIcon(Toolkit.getDefaultToolkit().getImage(GUI.class.getResource("/leaf.jpg")));
	}
	

	

	@Override
	public Component getTreeCellRendererComponent(JTree tree, Object value, boolean sel, boolean expanded, boolean leaf,
			int row, boolean hasFocus) {
		
		super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row, hasFocus);

		DefaultMutableTreeNode node = (DefaultMutableTreeNode) value;

		Object userObject = node.getUserObject();

		if (userObject != null)
			if (userObject instanceof NodeObject) {
				NodeObject no = (NodeObject) userObject;
				
				if (null != no.table) {
					DBTable t = no.table;
					if (t.getModel().getRowCount() > 1) {
						String text = String.format(SPAN_FORMAT, "blue", no.name);
						setText("<html>" + text + "</html>");
						
						
					
						// this.setIcon(NodeIcon);
					} else {
						String text = String.format(SPAN_FORMAT, "gray", no.name);
						setText("<html>" + text + "</html>");

					}
					
					
				}
				
				switch(no.tabletype)
				{
					case 0   :	this.setIcon(icon_table);
								break;
					case 1   : 	this.setIcon(icon_index);
								break;
					case 99  :  this.setIcon(icon_database);
								break;
					case 100 :  this.setIcon(icon_journal);
								break; 			
					case 101 :  this.setIcon(icon_wal);
								break;
				}
			}
			else 
				this.setIcon(icon_root);
		
		return this;
       
	}
}
