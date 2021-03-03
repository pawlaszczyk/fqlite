package fqlite.ui;

import java.util.List;
import java.util.ArrayList;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;

/**
 * Implementation of a customized table model. It is used to enable type safe
 * sorting.
 * 
 * @author pawlaszc
 *
 */
public class CustomTableModel extends DefaultTableModel {

	private JTable parent;
	private List<String> headers = new ArrayList<String>();
	private List<ct> types = new ArrayList<ct>();

	/**
	 * 
	 */

	private static final long serialVersionUID = 1L;

	public CustomTableModel() {
		super();
		// for the first two columns (status and offset)
		types.add(ct.STRING);
		types.add(ct.STRING);
	}

	public void setTable(JTable parent) {
		this.parent = parent;
	}

	public JTable getTable() {
		return parent;
	}

	public String getHeader(int idx) {
		if (idx < headers.size())
			return headers.get(idx);
		else
			return "";
	}

	public void addHeader(String head) {
		headers.add(head);
	}

	@Override
	public boolean isCellEditable(int row, int column) {
		return false;
	}

	/**
	 * 
	 * @param type
	 */
	public void addColumnType(String type) {
		if (type.equals("INT") || type.equals("INTEGER") || type.equals("LONG") || type.equals("NUMERIC"))
			types.add(ct.INTEGER);
		else if (type.equals("REAL"))
			types.add(ct.DOUBLE);
		else
			types.add(ct.STRING);
	}

	@SuppressWarnings("rawtypes")
	Class[] coltypes = { String.class, Integer.class, Double.class, Boolean.class };

	enum ct {
		STRING, INTEGER, DOUBLE
	};

	@Override
	public Class<?> getColumnClass(int columnIndex) {

		if (types.isEmpty()) {

			return Object.class;
		} else if (columnIndex >= types.size()) {
			return Object.class;
		} else {
			ct selected = types.get(columnIndex);
			switch (selected) {
			case STRING:
				//System.out.println(columnIndex + ">>>>>>>> STRING");
				return coltypes[0];

			case INTEGER:
				//System.out.println(columnIndex + ">>>>>>>> INTEGER!");
				return coltypes[1];
			case DOUBLE:
				//System.out.println(columnIndex + ">>>>>>>> DOUBLE!");
				return coltypes[2];
			}
			return Object.class;

		}
	}

}
