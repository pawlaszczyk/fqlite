package fqlite.ui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Font;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;

import fqlite.base.GUI;

public class WALPropertyPanel extends JPanel {

	private static final long serialVersionUID = 3116420145072139413L;
	public JLabel ldbpath;
	public JLabel lpagesize;
	public JLabel lencoding;
	public JLabel ltotalsize;
	public JLabel lpagesizeout;
	public JLabel lencodingout;
	public JLabel ltotalsizeout;

	private FileInfo info;
	GUI gui;

	JTabbedPane tabpane = new JTabbedPane(JTabbedPane.TOP, JTabbedPane.SCROLL_TAB_LAYOUT);

	public WALPropertyPanel(FileInfo info, GUI gui) {
		this.info = info;
		this.gui = gui;
	}

	public void initHeaderTable(String[][] data) {

		setLayout(new BorderLayout());

		JTextArea headerinfo = new JTextArea();
		PopupFactory.createPopup(headerinfo);
		headerinfo.setColumns(85);
		headerinfo.setAlignmentX(CENTER_ALIGNMENT);
		Font font = new Font("Courier", Font.BOLD, 12);
		headerinfo.setFont(font);
		headerinfo.setText(info.toString());
		tabpane.addTab("File info", headerinfo);

		String column[] = { "Offset", "Property", "Value" };
		JTable jt = new JTable(data, column);
		PopupFactory.createPopup(jt);


		JTableHeader th = jt.getTableHeader();
		th.setFont(new Font("Serif", Font.BOLD, 15));

		TableColumnModel tcm = jt.getColumnModel();

		// Columns don't fill the viewport, invoke default layout
		JScrollPane sp = new JScrollPane(jt);

		tabpane.addTab("Header Fields", sp);

		add(tabpane, BorderLayout.CENTER);

		if (tcm.getTotalColumnWidth() < jt.getParent().getWidth())
			jt.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);

	}

	Color[] colors = new Color[] { Color.green, Color.yellow, Color.orange, Color.cyan, Color.white, Color.lightGray };

	public void initCheckpointTable(String[][] data) {

		String column[] = { "salt1", "salt2", "framenumber", "pagenumber", "commit" };
		JTable jt = new JTable(data, column);
		PopupFactory.createPopup(jt);
		
		TableColumnModel tcm = jt.getColumnModel();
		int cc = tcm.getColumnCount();
		
		int m = 0;
		for (int j = 0; j < data.length; j++)
		{
			String salt = data[j][0];
			if (!gui.getRowcolors().containsKey(salt))
			{
				gui.getRowcolors().put(salt, colors[m % 6]);
				m++;
			}			
		}
	
		
		
		for (int i = 0; i < cc; i++) {
			TableColumn tm = tcm.getColumn(i);
			tm.setCellRenderer(new ColoredTableCellRenderer());
		}

		JTableHeader th = jt.getTableHeader();
		th.setFont(new Font("Serif", Font.BOLD, 15));

		JScrollPane sp = new JScrollPane(jt);
		// add(sp,BorderLayout.SOUTH);
		tabpane.addTab("Checkpoints", sp);

		if (tcm.getTotalColumnWidth() < jt.getParent().getWidth())
			jt.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);

	}

	class ColoredTableCellRenderer extends DefaultTableCellRenderer {

		int i = 0;

		private static final long serialVersionUID = -1013353404857478956L;

		public Component getTableCellRendererComponent(JTable table, Object value, boolean selected, boolean focused,
				int row, int column) {

			Component cell = super.getTableCellRendererComponent(table, value, selected, focused, row, column);

			Object salt = table.getValueAt(row, 0);

			if (gui.getRowcolors().containsKey(salt)) {
				cell.setBackground(gui.getRowcolors().get(salt));
			}

			return this;
		}
	}

}
