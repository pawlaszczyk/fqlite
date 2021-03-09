package fqlite.ui;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Rectangle;
import java.awt.event.ActionListener;

import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JToggleButton;
import javax.swing.text.BadLocationException;
import javax.swing.text.Document;

public class HexView extends JDialog {

	private static final long serialVersionUID = 1L;

	String soffset;
	String stext;
	String shex;
	int width;
	String path;
	public JTextArea thex;
	public JTextArea ttext;
	public JTextArea taddress;
    int currentsearch = 1;
	
	int pos = 0;

	public HexView() {
		initComponents();
	}

	public HexView(String path, JFrame parent, String soffset, String shex, String stext, int width) {
		this.soffset = soffset;
		this.stext = stext;
		this.shex = shex;
		this.width = width;
		this.path = path;
		initComponents();
	}

	private void initComponents() {

		
		
		taddress = new JTextArea();
		taddress.setColumns(width);
		// taddress.setLineWrap(true); don't use this!!! - application freezes when
		// opening larger files
		taddress.append(soffset);
		taddress.setFont(new java.awt.Font("Courier New", java.awt.Font.BOLD, 18));
		taddress.setEditable(false);
		PopupFactory.createPopup(taddress);
		
		
		LimitlessPlainDocument contentp = new LimitlessPlainDocument(Integer.MAX_VALUE);

		thex = new JTextArea(contentp);
		thex.setColumns(32);
		// thex.setLineWrap(true);
		thex.setFont(new java.awt.Font("Courier New", java.awt.Font.PLAIN, 18));
		thex.setEditable(false);
		PopupFactory.createPopup(thex);


		/**
		 * Attetion: We do linewrapping manualy
		 */
		try {
			StringBuffer shexnl = new StringBuffer();
			for (int i = 0; i < shex.length(); i += 32) {
				if (i + 32 > shex.length()) {
					shexnl.append(shex.substring(i, shex.length()));
				} else
					shexnl.append(shex.substring(i, i + 32));
				shexnl.append("\n");
			}
			contentp.insertString(0, shexnl.toString(), null);
		} catch (BadLocationException e) {
			e.printStackTrace();
		}

		JSeparator sleft = new JSeparator(JSeparator.VERTICAL);

		LimitlessPlainDocument contenttext = new LimitlessPlainDocument(Integer.MAX_VALUE);

		ttext = new JTextArea(contenttext);
		PopupFactory.createPopup(ttext);

		ttext.setColumns(16);
		// ttext.setLineWrap(true); don't use this!!! - application freezes when opening
		// larger files
		try {

			StringBuffer stextnl = new StringBuffer();
			for (int i = 0; i < stext.length(); i += 16) {
				if (i + 16 > stext.length()) {
					stextnl.append(stext.substring(i, stext.length()));
				} else
					stextnl.append(stext.substring(i, i + 16));
				stextnl.append("\n");
			}
			ttext.setFont(new java.awt.Font("Courier New", java.awt.Font.PLAIN, 18));
			// ttext.append(stext);
			ttext.setEditable(false);

			contenttext.insertString(0, stextnl.toString(), null);
		} catch (BadLocationException e) {
			e.printStackTrace();
		}

		JSeparator sright = new JSeparator(JSeparator.VERTICAL);

		JPanel content = new JPanel();
		content.add(taddress);
		content.add(sleft);
		content.add(thex);
		content.add(sright);
		content.add(ttext);

		content.setLayout(new BoxLayout(content, BoxLayout.X_AXIS));
		JScrollPane scrollpane = new JScrollPane(content);
		scrollpane.setPreferredSize(new Dimension(650, 600));

		JPanel searchpanel = new JPanel();
		searchpanel.setLayout(new FlowLayout(FlowLayout.CENTER));

		ButtonGroup buttonGroup = new ButtonGroup();

		ActionListener listenerH = actionEvent -> {

			
			currentsearch = 0;
			
		};

		ActionListener listenerT = actionEvent -> {
		
			
			currentsearch = 1;

		};
		
		JToggleButton bhex = new JToggleButton("Hex");
		bhex.addActionListener(listenerH);
		buttonGroup.add(bhex);

		JToggleButton btext = new JToggleButton("Text");
		btext.addActionListener(listenerT);
		buttonGroup.add(btext);
		buttonGroup.setSelected(btext.getModel(), true);

		searchpanel.add(bhex);
		searchpanel.add(btext);

		searchpanel.add(new JLabel("Find:"));
		JTextField searchfield = new JTextField(20);
		searchpanel.add(searchfield);

		// JButton prev = new JButton("Prev");
		JButton bnext = new JButton("Next");
		
		

		ActionListener listenerNext = actionEvent -> {

			// Get the text to find...convert it to lower case for eaiser comparision
			String find = searchfield.getText().toLowerCase();
			// Focus the text area, otherwise the highlighting won't show up
			if (currentsearch == 1)
				ttext.requestFocusInWindow();
			else
				thex.requestFocusInWindow();
			// Make sure we have a valid search term
			if (find != null && find.length() > 0) {
				
				
				Document document = null;
				if (currentsearch == 1)
					document = ttext.getDocument();
				else
					document = thex.getDocument();
				
				int findLength = find.length();

				try {
					boolean found = false;
					// Rest the search position if we're at the end of the document
					if (pos + findLength > document.getLength()) {
						pos = 0;
					}
					// While we haven't reached the end...
					// "<=" Correction
					while (pos + findLength <= document.getLength()) {
						// Extract the text from the document
						String match = document.getText(pos, findLength).toLowerCase();
						// Check to see if it matches or request
						if (match.equals(find)) {
							found = true;
							break;
						}
						pos++;
					}

					// Did we find something...
					if (found) {
						
						if (currentsearch == 1)
						{
							// Get the rectangle of the where the text would be visible...
							Rectangle viewRect = ttext.modelToView(pos);
							// Scroll to make the rectangle visible
							ttext.scrollRectToVisible(viewRect);
							// Highlight the text
							ttext.setCaretPosition(pos + findLength);
							ttext.moveCaretPosition(pos);
							// Move the search position beyond the current match
							pos += findLength;
						}
						else
						{
							// Get the rectangle of the where the text would be visible...
							Rectangle viewRect = thex.modelToView(pos);
							// Scroll to make the rectangle visible
							thex.scrollRectToVisible(viewRect);
							// Highlight the text
							thex.setCaretPosition(pos + findLength);
							thex.moveCaretPosition(pos);
							// Move the search position beyond the current match
							pos += findLength;
							
							
						}
						
					}

				} catch (Exception exp) {
					exp.printStackTrace();
				}

			}
		};

		bnext.addActionListener(listenerNext);

		// searchpanel.add(prev);
		searchpanel.add(bnext);

		JPanel mainpanel = new JPanel();
		mainpanel.setLayout(new BorderLayout());
		mainpanel.add(searchpanel, BorderLayout.NORTH);
		mainpanel.add(scrollpane, BorderLayout.CENTER);

		this.add(mainpanel);
		this.pack();
		this.setResizable(false);
		setLocationRelativeTo(null);

		setTitle("HexView " + path);

		thex.setCaretPosition(0);

		setDefaultCloseOperation(JDialog.HIDE_ON_CLOSE);
	}

}
