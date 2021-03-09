package fqlite.ui;

import java.awt.Color;
import java.awt.Component;
import java.awt.Font;
import java.awt.Toolkit;
import javax.swing.ImageIcon;
import javax.swing.JTable;
import javax.swing.table.DefaultTableCellRenderer;
import fqlite.base.GUI;
import fqlite.base.Global;

import fqlite.util.BLOBCarver;

public class CustomCellRenderer extends DefaultTableCellRenderer {

	private static final long serialVersionUID = 1L;
	private static final Font offsetFont = new Font("Courier New", Font.BOLD, 13);
	private static final Color rowcol = new Color(246, 246, 246); // 204,229,255;
	private static final Font defaultFont = new Font("OpenSansEmoji", Font.PLAIN, 13);
	private static ImageIcon icon_deleted = new ImageIcon(Toolkit.getDefaultToolkit().getImage(GUI.class.getResource("/icon_deleted.png")));
	private static ImageIcon icon_trash = new ImageIcon(Toolkit.getDefaultToolkit().getImage(GUI.class.getResource("/icon_trash.png")));
 // private static ImageIcon icon_status =  new ImageIcon(Toolkit.getDefaultToolkit().getImage(GUI.class.getResource("/icon_status.png")));
 //	private static ImageIcon icon_photo = new ImageIcon(Toolkit.getDefaultToolkit().getImage(GUI.class.getResource("/picture.png")));

	public boolean walnode = false;
    public GUI gui = null;
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * javax.swing.table.DefaultTableCellRenderer#getTableCellRendererComponent(
	 * javax.swing.JTable, java.lang.Object, boolean, boolean, int, int)
	 */
	public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus,
			int row, int column) {

		
		Component rendererComp = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
		if (column < 2)
			rendererComp.setFont(offsetFont);
		else {
			if (GUI.ttfFont != null)
				rendererComp.setFont(GUI.ttfFont);
			else
				rendererComp.setFont(defaultFont);
		}
		
		if (column > 1)
		{ 
			if (value instanceof String)
			{	
				String v = (String)value;
				if (BLOBCarver.isGraphic(v))
				{
					try {
			
						this.setValue("[PICTURE]");

					} catch (Exception e) {
						
						e.printStackTrace();
					}
					
				}	
		
			}
		}
		if (column == 0)
		{
			String v = (String)value;
			
			if (v.contains(Global.FREELIST_ENTRY))
			{	this.setIcon(icon_trash);
			    this.setValue("");
			}
			else if (v.contains(Global.DELETED_RECORD_IN_PAGE))
			{
				this.setIcon(icon_deleted);
			    this.setValue("");
			}
			else if (v.contains(Global.UNALLOCATED_SPACE))
			{	
				this.setIcon(icon_deleted);
				this.setValue("");
			}
		}	
		else
		{
			this.setIcon(null);
		}

		
		
		if(walnode)
		{
			Object salt = table.getValueAt(row,5);
			//int vrow = table.convertRowIndexToView(row);
			
			Component cell = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);

			if (null != gui && gui.getRowcolors().containsKey(salt)) {
				cell.setBackground(gui.getRowcolors().get(salt));
			}
		}
		else
		{
			if (! isSelected)
			{	
				if ((row % 2) == 0) {
					rendererComp.setBackground(rowcol);
				} else
					rendererComp.setBackground(Color.WHITE);
		
				if (column > 1)
					rendererComp.setForeground(Color.BLUE);
				else
					rendererComp.setForeground(Color.BLACK);
			}
			
		}
	
		return rendererComp;
	}

	
	public static byte[] hexStringToByteArray(String s) {
	    int len = s.length();
	    byte[] data = new byte[len / 2];
	    for (int i = 0; i < len; i += 2) {
	        data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
	                             + Character.digit(s.charAt(i+1), 16));
	    }
	    return data;
	}
}