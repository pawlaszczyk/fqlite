package fqlite.ui;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.io.InputStream;

import javax.swing.*;
import javax.swing.event.*;
import fqlite.base.GUI;
import javax.swing.border.EmptyBorder;
import java.util.Vector;

public class FontChooser extends JDialog {

	private static final long serialVersionUID = -1460158313220812964L;
	private JPanel main = new JPanel();
	private JComponent ui = null;
	private String text = "Can you see the bears ? ";
	GUI parent = null;
	Font currentlySelectedFont;

	public FontChooser(GUI parent) {

		super(parent);
		this.parent = parent;

		GraphicsEnvironment ge = GraphicsEnvironment.getLocalGraphicsEnvironment();
		try {
			//ge.registerFont(Font.createFont(Font.TRUETYPE_FONT,
			//		new File("/Users/pawlaszc/Downloads/OpenSansEmoji.ttf")));
			 InputStream is = this.getClass().getResourceAsStream("/OpenSansEmoji.ttf");
	    	 Font uniFont= Font.createFont(Font.TRUETYPE_FONT,is);
			 ge.registerFont(uniFont);
			
		} catch (FontFormatException | IOException e) {
			e.printStackTrace();
		}

		text = text + "\ud83d\udc3b" + "\n What is bear + 1 ? " + "\ud83d\udc3c";

		initUI();

		setTitle("FontChooser");
		setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		setModalityType(ModalityType.APPLICATION_MODAL);
		setContentPane(main);
		setLocationRelativeTo(null);
		pack();
		setMinimumSize(getSize());

	}

	public void initUI() {
		if (ui != null)
			return;

		ui = new JPanel(new BorderLayout(4, 4));
		ui.setBorder(new EmptyBorder(4, 4, 4, 4));

		String[] fontFamilies = GraphicsEnvironment.getLocalGraphicsEnvironment().getAvailableFontFamilyNames();
		Vector<String> EmojiFriendlyFonts = new Vector<String>();

		for (String name : fontFamilies) {
			// Font font = new Font(name, Font.PLAIN, 20);
			// if (font.canDisplayUpTo(text)<0) {
			EmojiFriendlyFonts.add(name);
			// }
		}
		EmojiFriendlyFonts.add(0, "OpenSansEmoji");

		JList<String> list = new JList<String>(EmojiFriendlyFonts);
		list.setVisibleRowCount(20);
		list.getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		ui.add(new JScrollPane(list), BorderLayout.LINE_START);

		final JTextArea output = new JTextArea(text, 2, 20);
		output.setLineWrap(true);
		output.setWrapStyleWord(true);
		ui.add(new JScrollPane(output));

		ListSelectionListener showFontListener = new ListSelectionListener() {

			@Override
			public void valueChanged(ListSelectionEvent e) {
				Font f = new Font(list.getSelectedValue().toString(), Font.PLAIN, 30);
				currentlySelectedFont = new Font(list.getSelectedValue().toString(), Font.PLAIN, 13);
				output.setFont(f);
			}
		};
		list.addListSelectionListener(showFontListener);
		list.setSelectedIndex(0);

		main.setLayout(new BorderLayout());
		main.add(ui, BorderLayout.CENTER);

		JPanel applypanel = new JPanel();
		JButton applybtn = new JButton("Apply");
		applybtn.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
			
				EventQueue.invokeLater(new Runnable() {
					public void run() {
						try {
							if (null != parent)
								GUI.ttfFont = currentlySelectedFont;	
								dispose();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
				
			}
		});

		applypanel.add(applybtn);
		main.add(applybtn, BorderLayout.SOUTH);

	}

	public JComponent getUI() {
		return ui;
	}

}
