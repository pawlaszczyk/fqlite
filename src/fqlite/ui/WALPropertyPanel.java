package fqlite.ui;

import java.awt.BorderLayout;
import java.awt.Font;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableColumnModel;

public class WALPropertyPanel extends JPanel {

	
	private static final long serialVersionUID = 3116420145072139413L;
	public JLabel ldbpath;
    public JLabel lpagesize;
    public JLabel lencoding;
    public JLabel ltotalsize;
 
    
    public JLabel lpagesizeout;
    public JLabel lencodingout;
    public JLabel ltotalsizeout;

	public WALPropertyPanel()
	{
	}
	
	
	public void initHeaderTable(String[][] data)
	{
		JLabel heading = new JLabel(" WAL Pragmas ");
		add(heading,BorderLayout.NORTH);
		
		setLayout(new BorderLayout());
		
		
		String column[]={"Offset","Property","Value"};         
		JTable jt=new JTable(data,column);    
		
		
		JTableHeader th = jt.getTableHeader();
		th.setFont(new Font("Serif", Font.BOLD, 15));
		
				
		TableColumnModel tcm = jt.getColumnModel();

		// Columns don't fill the viewport, invoke default layout

		JScrollPane sp = new JScrollPane(jt);    
		add(sp,BorderLayout.CENTER);
		
		if (tcm.getTotalColumnWidth() < jt.getParent().getWidth())
			jt.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
		
		
		
	}
	
	
	
}
