package fqlite.ui;

import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JPanel;
import java.net.URL;

import javax.swing.ImageIcon;
import javax.swing.JLabel;
import javax.swing.JTextField;

import fqlite.base.GUI;

import javax.swing.JSeparator;

public class PropertyPanelFrame extends JFrame {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7125380052686271671L;
	public JTextField TFdbpath;
	//private JTextField textField_1;
	//private JTextField textField_2;
	//private JTextField textField_3;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					PropertyPanelFrame frame = new PropertyPanelFrame();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	public void setDBPath(String path)
	{
		this.TFdbpath.setText(path);
	}
	
	public JPanel createPanel()
	{
		JPanel outerpanel = new JPanel();
		outerpanel.setLayout(null);
		
		JPanel panel = new JPanel();
		panel.setBounds(17, 141, 615, 157);
		panel.setLayout(null);
		outerpanel.add(panel);
			
		JLabel lbDBName_1 = new JLabel("database path");
		lbDBName_1.setBounds(46, 10, 89, 16);
		panel.add(lbDBName_1);
		
	    TFdbpath = new JTextField();
	    TFdbpath.setBounds(217, 5, 190, 26);
		panel.add(TFdbpath);
		TFdbpath.setColumns(20);
		
		
		JLabel Encoding = new JLabel("total size in bytes");
		Encoding.setBounds(48, 75, 114, 16);
		panel.add(Encoding);
		
		JTextField textField_1 = new JTextField();
		textField_1.setColumns(20);
		textField_1.setBounds(217, 70, 190, 26);
		panel.add(textField_1);
		
		
		JLabel lpagesize = new JLabel("page size in bytes ");
		lpagesize.setBounds(47, 106, 117, 16);
		panel.add(lpagesize);
		
		JTextField textField_2 = new JTextField();
		textField_2.setColumns(20);
		textField_2.setBounds(217, 38, 190, 26);
		panel.add(textField_2);
		
		JLabel Encoding_1 = new JLabel("encoding");
		Encoding_1.setBounds(46, 43, 58, 16);
		panel.add(Encoding_1);
		
		JTextField textField_3 = new JTextField();
		textField_3.setColumns(20);
		textField_3.setBounds(217, 101, 190, 26);
		panel.add(textField_3);
		
		URL url = GUI.class.getResource("/find.png");
		
		JLabel lblNewLabel = new JLabel(new ImageIcon(url));
		lblNewLabel.setBounds(17, 41, 24, 24);
		outerpanel.add(lblNewLabel);
		
		JSeparator separator = new JSeparator();
		separator.setBounds(20, 97, 612, 12);
		outerpanel.add(separator);
		
		JSeparator separator_1 = new JSeparator();
		separator_1.setBounds(20, 327, 612, 12);
		outerpanel.add(separator_1);
		
		JLabel lblNewLabel_1 = new JLabel("Database - Header Info");
		lblNewLabel_1.setBounds(78, 49, 232, 16);
		outerpanel.add(lblNewLabel_1);
		
		//The database page size in bytes.
		
		//Bytes of unused "reserved" space at the end of each page. Usually 0.
		
		//Page number of the first freelist trunk page.

		//Page number of the first freelist trunk page.

		
		
		return outerpanel;
		
	}

	/**
	 * Create the frame.
	 */
	public PropertyPanelFrame() {
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 650, 800);
		JPanel p = createPanel();
		getContentPane().add(p);
	}
}
