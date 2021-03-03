package fqlite.ui;

import java.awt.Component;

import javax.swing.JLabel;
import javax.swing.JTable;
import javax.swing.UIManager;
import javax.swing.table.DefaultTableCellRenderer;

public class IconRenderer extends DefaultTableCellRenderer {
	
	private static final long serialVersionUID = -8152050467650427337L;

	public Component getTableCellRendererComponent(JTable table, Object obj, boolean isSelected, boolean hasFocus,
			int row, int column) {
		txtIcon i = (txtIcon) obj;
		if (obj == i) {
			setIcon(i.imageIcon);
			setText(i.txt);
		}
		setBorder(UIManager.getBorder("TableHeader.cellBorder"));
		setHorizontalAlignment(JLabel.CENTER);
		return this;
	}
}