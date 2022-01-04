package fqlite.ui;

import static javax.swing.GroupLayout.Alignment.CENTER;

import java.awt.Color;
import java.awt.Container;
import java.awt.Font;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.net.URL;

import javax.swing.Box;
import javax.swing.GroupLayout;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextPane;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyledDocument;

import fqlite.base.Global;

/*
 ---------------
 AboutDialog.java
 ---------------
 (C) Copyright 2020.

 Original Author:  Dirk Pawlaszczyk
 Contributor(s):   -;


 Project Info:  http://www.hs-mittweida.de

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.

 Dieses Programm ist Freie Software: Sie können es unter den Bedingungen
 der GNU General Public License, wie von der Free Software Foundation,
 Version 3 der Lizenz oder (nach Ihrer Wahl) jeder neueren
 veröffentlichten Version, weiterverbreiten und/oder modifizieren.

 Dieses Programm wird in der Hoffnung, dass es nützlich sein wird, aber
 OHNE JEDE GEWÄHRLEISTUNG, bereitgestellt; sogar ohne die implizite
 Gewährleistung der MARKTFÄHIGKEIT oder EIGNUNG FÜR EINEN BESTIMMTEN ZWECK.
 Siehe die GNU General Public License für weitere Details.

 Sie sollten eine Kopie der GNU General Public License zusammen mit diesem
 Programm erhalten haben. Wenn nicht, siehe <http://www.gnu.org/licenses/>.

 */

/**
 * Brings up a simple About-Dialog with application information.
 * 
 * @author Dirk Pawlaszczyk
 *
 */
@SuppressWarnings("serial")
public class AboutDialog extends JDialog {

	public AboutDialog(Frame parent) {
		super(parent);

		URL url = AboutDialog.class.getResource("/fqlite_logo_small.png");

		ImageIcon icon = new ImageIcon(url);

		JLabel label = new JLabel(icon);

		JLabel name = new JLabel("FQLite Retrieval Tool, Version " + Global.FQLITE_VERSION);
		JLabel author = new JLabel("Author: Dirk Pawlaszczyk");
		JLabel company = new JLabel("Mittweida University of Applied Sciences");
//		JTextArea greetings = new JTextArea(""+
//"                     *                   \n"+
//"                    /|\\                 \n"+
//"                   /*|O\\                \n"+
//"                  /*/|\\*\\              \n"+
//"                 /X/O|*\\X\\             \n"+
//"                /*/X/|\\X\\*\\           \n"+
//"               /O/*/X|*\\O\\X\\          \n"+
//"              /*/O/X/|\\X\\O\\*\\        \n"+
//"             /X/O/*/X|O\\X\\*\\O\\       \n"+
//"            /O/X/*/O/|\\X\\*\\O\\X\\     \n"+
//"                    |X|                  \n"+
//"                    |X|     <XMAS Edition> \n");
		JTextArea greetings = new JTextArea("\n"
				+ "*°*”˜˜”*°•.¸☆ ★ ☆¸.•°*”˜˜”*°•.¸☆\n"
				+ "╔╗╔╦══╦═╦═╦╗╔╗ ★ ★ ★\n"
				+ "║╚╝║══║═║═║╚╝║ ☆¸.•°*”˜˜”*°•.¸☆\n"
				+ "║╔╗║╔╗║╔╣╔╩╗╔╝ ★ NEW YEAR ☆\n"
				+ "╚╝╚╩╝╚╩╝╚╝═╚╝ ♥￥☆★☆★☆￥♥ ★☆❤♫❤♫❤\n"
				+ ".•*¨`*•..¸☼ ¸.•*¨`*•.♫❤♫❤♫❤");
	    greetings.setEnabled(false);
	    greetings.setFont(new Font("Courier", Font.PLAIN, 14));
		
		
		JButton btn = new JButton("   OK   ");
		btn.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent event) {
				dispose();
			}
		});

		createLayout(name, label, author, company, greetings, btn);

		setModalityType(ModalityType.APPLICATION_MODAL);

		setTitle("About this Program");
		setDefaultCloseOperation(DISPOSE_ON_CLOSE);
		setLocationRelativeTo(getParent());
	}

	private void createLayout(JComponent... arg) {

		Container pane = getContentPane();
		GroupLayout gl = new GroupLayout(pane);
		pane.setLayout(gl);
	    pane.setBackground(Color.WHITE);
		
		JScrollPane jp = new JScrollPane();
		jp.setAutoscrolls(true);

		// Define the attribute you want for the line of text

		SimpleAttributeSet center = new SimpleAttributeSet();
		StyleConstants.setAlignment(center, StyleConstants.ALIGN_CENTER);

		// Add some text to the end of the Document

		JTextPane licence = new JTextPane();
		StyledDocument doc = null;
		try {
			doc = licence.getStyledDocument();
			int length = doc.getLength();
			doc.insertString(doc.getLength(), "\n", null);
			doc.setParagraphAttributes(length + 1, 1, center, false);
			doc.insertString(doc.getLength(), "GNU GENERAL PUBLIC LICENSE \n Version 3, 29 June 2007 \n", null);
			doc.setParagraphAttributes(length + 1, 1, center, false);
			doc.insertString(doc.getLength(), "\n", null);

			doc.insertString(doc.getLength(), "This program is free software:  you  can  redistribute \n ", null);
			doc.insertString(doc.getLength(), "it and/or modify it under the terms of the GNU General \n", null);
			doc.insertString(doc.getLength(), "Public License as published by the Free Software \n", null);
			doc.insertString(doc.getLength(), "Foundation, either version 3 of the License, or (at your \n ", null);
			doc.insertString(doc.getLength(), "option) any later version. This program is distributed in \n", null);
			doc.insertString(doc.getLength(), "without even the implied warranty of MERCHANTABILITY or \n", null);
			doc.insertString(doc.getLength(), "FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General \n", null);
			doc.insertString(doc.getLength(), "Public License for more details. You should have received \n", null);
			doc.insertString(doc.getLength(), "a copy of the GNU General Public License along with this \n", null);
			doc.insertString(doc.getLength(), "program.  If not, see <http://www.gnu.org/licenses/>.", null);

		} catch (Exception e) {
			return;
		}

		licence.setEditable(false);

		licence.setAlignmentY(JTextArea.CENTER_ALIGNMENT);
		jp.setViewportView(licence);

		gl.setAutoCreateContainerGaps(true);
		gl.setAutoCreateGaps(true);

		gl.setHorizontalGroup(gl.createParallelGroup(CENTER).addComponent(arg[0]).addComponent(arg[1])
				.addComponent(arg[2]).addComponent(arg[3]).addComponent(jp).addGap(100).addComponent(arg[4]));

		gl.setVerticalGroup(gl.createSequentialGroup().addGap(30).addComponent(arg[0]).addGap(20).addComponent(arg[1])
				.addGap(20).addComponent(arg[2]).addGap(30).addComponent(arg[3]).addGap(30).addComponent(jp).addGap(30)
				.addComponent(arg[4]));

		pack();
	}
}

@SuppressWarnings("serial")
class JDialogEx extends JFrame implements ActionListener {

	public JDialogEx() {

		initUI();
	}

	private void initUI() {

		createMenuBar();

		setTitle("About this Program");
		setSize(350, 300);
		setLocationRelativeTo(null);
		setDefaultCloseOperation(EXIT_ON_CLOSE);
	}

	private void createMenuBar() {

		JMenuBar menubar = new JMenuBar();

		JMenu fileMenu = new JMenu("File");
		fileMenu.setMnemonic(KeyEvent.VK_F);

		JMenu helpMenu = new JMenu("Help");
		helpMenu.setMnemonic(KeyEvent.VK_H);

		JMenuItem aboutMi = new JMenuItem("About");
		aboutMi.setMnemonic(KeyEvent.VK_A);
		helpMenu.add(aboutMi);

		aboutMi.addActionListener(this);

		menubar.add(fileMenu);
		menubar.add(Box.createGlue());
		menubar.add(helpMenu);
		setJMenuBar(menubar);
	}

	@Override
	public void actionPerformed(ActionEvent e) {

		showAboutDialog();
	}

	private void showAboutDialog() {

		AboutDialog ad = new AboutDialog(this);
		ad.setVisible(true);
	}
	
	

}