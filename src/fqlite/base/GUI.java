package fqlite.base;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Desktop;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.Font;
import java.awt.FontFormatException;
import java.awt.GraphicsEnvironment;
import java.awt.Point;
import java.awt.SystemColor;
import java.awt.Toolkit;
import java.awt.datatransfer.DataFlavor;
import java.awt.dnd.DnDConstants;
import java.awt.dnd.DropTarget;
import java.awt.dnd.DropTargetDropEvent;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.DefaultCellEditor;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.JToolBar;
import javax.swing.JTree;
import javax.swing.JWindow;
import javax.swing.KeyStroke;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.UIManager;
import javax.swing.border.EmptyBorder;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;
import javax.swing.table.TableRowSorter;
import javax.swing.text.DefaultHighlighter;
import javax.swing.text.Highlighter;
import javax.swing.text.Highlighter.HighlightPainter;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreePath;
import fqlite.types.FileTypes;
import fqlite.ui.AboutDialog;
import fqlite.ui.CopyAction;
import fqlite.ui.CustomCellRenderer;
import fqlite.ui.CustomTableModel;
import fqlite.ui.CustomTreeCellRenderer;
import fqlite.ui.DBPropertyPanel;
import fqlite.ui.DBTable;
import fqlite.ui.FTreeNode;
import fqlite.ui.FileInfo;
import fqlite.ui.FontChooser;
import fqlite.ui.HexView;
import fqlite.ui.NodeObject;
import fqlite.ui.PasteAction;
import fqlite.ui.PopUpListener;
import fqlite.ui.ProgressBar;
import fqlite.ui.RollbackPropertyPanel;
import fqlite.ui.RowFilterUtil;
import fqlite.ui.Statusbar;
import fqlite.ui.WALPropertyPanel;
import fqlite.ui.IconRenderer;
import fqlite.util.Auxiliary;
import fqlite.util.BLOBCarver;
import fqlite.util.Logger;

/*
    ---------------
    GUI.java
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
 * This class offers a basic graphical user interface. It is based on swing 
 * and extends the class <code>JFrame</code>.
 * 
 * Most of the decoration code was generated or build with WindowBuilder.
 * 
 * If you want to start FQLite in graphic-mode you have to call this class. 
 * 
 * It contains a main()-function.
 * 
 *     __________    __    _ __     
 *    / ____/ __ \  / /   (_) /____ 
 *   / /_  / / / / / /   / / __/ _ \
 *  / __/ / /_/ / / /___/ / /_/  __/
 * /_/    \___\_\/_____/_/\__/\___/ 
 * 
 * 
 * 
 * @author Dirk Pawlaszczyk
 * @version 1.5.7
 *
 */

public class GUI extends JFrame {

	
	//ImageViewer vi = new ImageViewer(null,null);
	private static final long serialVersionUID = -4356985691362465255L;
	private JPanel contentPane;
	private GUI mainwindow;
	ConcurrentHashMap<TreePath, JComponent> tables = new ConcurrentHashMap<TreePath, JComponent>();
	ConcurrentHashMap<TreePath, JTextPane> hexviews = new ConcurrentHashMap<TreePath, JTextPane>();
	private Hashtable<Object, Color> rowcolors = new Hashtable<Object, Color>();


	JTextArea logwindow;
	JMenuBar menuBar;
	JScrollPane scrollpane_tables;
	JPanel table_panel_with_filter;
	Statusbar statusbar = new Statusbar();
	JScrollPane hexScrollPane;
	JTextField currentFilter = null;
	JPanel head;

	static GUI app;
	static JTree tree;
	DefaultMutableTreeNode dbNode;
	DefaultMutableTreeNode walNode;
	DefaultMutableTreeNode rjNode;

	public static Font ttfFont = null;

	DefaultMutableTreeNode root = new DefaultMutableTreeNode("data bases");
	

	String lasthit = "";
	int lasthitrow = 0;
	int lasthitcol = 0;

	File lastDir = null;

	ImageIcon facewink;
	ImageIcon findIcon;
	ImageIcon errorIcon;
	ImageIcon infoIcon;
	ImageIcon questionIcon;
	ImageIcon warningIcon;

	
	
	
	/**
	 * Launch the graphic front-end with this method.
	 */
	public static void main(String[] args) {

	
		/**
		  * This is needed because only one main class can be called in an
		  * executable jar archive. 
		  * 
		  **/
		if (args.length > 0)
		{	
		   // There is a least one parameter -> check, if nogui-option is set
            String option = args[0];
			if (args[0].contains(option))
			try {
				// switch to CLI mode instead
				System.out.println("[nogui] option is set => starting in CLI-mode...\n");
				MAIN.main(args);
			} catch (Exception e) {
				System.out.println("ERROR while running MANI.main(). Leave program now.");
			}
		    // do not call the UI and leave right now.
		    return;
		}
		
		
		JWindow window = new JWindow();
		window.setLayout(new BorderLayout());
        window.getContentPane().add(
                new JLabel("", new ImageIcon(GUI.class.getResource("/fqlite_logo_small.png"),BorderLayout.CENTER), SwingConstants.CENTER)).setBackground(Color.WHITE);
        window.getContentPane().setBackground(Color.WHITE);
        window.getContentPane().add(new JLabel("<html><h3>V."+ Global.FQLITE_VERSION + "</h3></html>"),BorderLayout.SOUTH);
        //Set the window's bounds, centering the window
        int width = 400;
        int height = 300;
        Dimension screen = Toolkit.getDefaultToolkit().getScreenSize();
        int x = (screen.width - width) / 2;
        int y = (screen.height - height) / 2;
        window.setBounds(x, y, width, height);

        window.setVisible(true);
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
  

		
		
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					GUI frame = new GUI();
					GUI.app = frame;

					String plaf = UIManager.getSystemLookAndFeelClassName();
					UIManager.setLookAndFeel(plaf);
					SwingUtilities.updateComponentTreeUI(frame);

					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Set the TTF-font for the user interface. 
	 * @param f the font we want to set. 
	 */
	public static void setUIFont(javax.swing.plaf.FontUIResource f) {
		java.util.Enumeration<Object> keys = UIManager.getDefaults().keys();
		while (keys.hasMoreElements()) {
			Object key = keys.nextElement();
			Object value = UIManager.get(key);
			if (value instanceof javax.swing.plaf.FontUIResource)
				UIManager.put(key, f);
		}
	}

	/**
	 * Constructor of the user interface class. 
	 * @throws IOException
	 */
	public GUI() throws IOException {
	    	
			GraphicsEnvironment ge = GraphicsEnvironment.getLocalGraphicsEnvironment();
   	    
   	    	 // let us first have a look on the currently installed Fonts
   	    	 // if Segoe UI Emoji (the standard font for emojis on win10
   	    	 // go for it
   	    	
   	    	 String[] fontFamilies = GraphicsEnvironment.getLocalGraphicsEnvironment().getAvailableFontFamilyNames();
   	    	 //Vector<String> EmojiFriendlyFonts = new Vector<String>();
             boolean msfontinstalled = false;
   	    	 
   			for (String name : fontFamilies) {
   			
   	    	    if( name.contains("Segoe UI Emoji"))
   	    	    {	
   	    	    
   	    	    	System.out.println(" Found Microsoft Emoji font. Let use this for the Cell Rendering.");
   	    	    	GUI.ttfFont = new Font(name, Font.PLAIN, 13);
   	    	    	msfontinstalled = true;
   	    	    }	
   			}
   				
   	    	 // if the Segoe Emoji font is not installed -> use an opensource TTF
   	    	 // that is part of the Archive file
   			if (!msfontinstalled)
   			{	
   				
   			    try
   			    {
   			 	 System.out.println(" Didn't find the Microsoft font. Use OpenSansEmoji instead.");
   	    	     InputStream is = this.getClass().getResourceAsStream("/OpenSansEmoji.ttf");
		    	 Font uniFont=Font.createFont(Font.TRUETYPE_FONT,is);
		    	 ge.registerFont(uniFont);
				} 
   			    catch (FontFormatException | IOException e) 
   			    {
					e.printStackTrace();
				}
			}
   
		mainwindow = this;
		
		
		setTitle("FQLite Carving Tool");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		URL url = GUI.class.getResource("/find.png");
		findIcon = new ImageIcon(url);
		// available size of the screen
		Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
		this.setSize((int) (screenSize.width * 0.8), (int) (screenSize.height * 0.8));
		setLocation((screenSize.width - getWidth()) / 2, (screenSize.height - getHeight()) / 2);

		menuBar = new JMenuBar();
		setJMenuBar(menuBar);
		JMenu mnFiles = new JMenu("File");
		mnFiles.setMnemonic(KeyEvent.VK_F);
		menuBar.add(mnFiles);

		JMenuItem mntopen = new JMenuItem("Open Database...");
		mntopen.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_O, ActionEvent.META_MASK));
		mntopen.getAccessibleContext().setAccessibleDescription("Open a sqlite database file to analyse...");
		mntopen.setMnemonic(KeyEvent.VK_O);
		mntopen.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				open_db(null);
			}
		});
		mnFiles.add(mntopen);

		JMenuItem mntmExport = new JMenuItem("Export Database...");
		mntmExport.setMnemonic(KeyEvent.VK_X);
		mntmExport.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_X, ActionEvent.META_MASK));
		mntmExport.getAccessibleContext().setAccessibleDescription("Start export a database to csv...");
		mntmExport.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {

				doExport();
			}
		});
		mnFiles.add(mntmExport);

		mnFiles.addSeparator();

		JMenuItem mntclose = new JMenuItem("Close All");
		mntclose.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_D, ActionEvent.CTRL_MASK));
		mntclose.getAccessibleContext().setAccessibleDescription("Close All currently open databases");
		mntclose.setMnemonic(KeyEvent.VK_D);
		mntclose.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				closeAll();
			}
		});
		mnFiles.add(mntclose);

		mnFiles.addSeparator();

		JMenuItem mntmExit = new JMenuItem("Exit");
		mntmExit.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_F4, ActionEvent.ALT_MASK));
		mntmExit.setMnemonic(KeyEvent.VK_F4);
		mntmExit.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				System.exit(0);
			}
		});
		mnFiles.add(mntmExit);

		JMenu mnInfo = new JMenu("Info");
		menuBar.add(mnInfo);
		mnInfo.setMnemonic(KeyEvent.VK_I);

		JMenuItem mntAbout = new JMenuItem("About...");
		mntAbout.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				JDialog about = new AboutDialog(mainwindow);
				about.setVisible(true);
			}
		});
		
		
		JMenuItem mntFont= new JMenuItem("Fonts...");
		mntFont.addActionListener(new ActionListener() 
		{
			public void actionPerformed(ActionEvent e) {
				FontChooser fc = new FontChooser(mainwindow);
				fc.setVisible(true);
			}
		});

		JMenuItem mntmHelp = new JMenuItem("Help");
		mntmHelp.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {

				// Create Desktop object
			    Desktop d = Desktop.getDesktop();

				// Browse a URL, say google.com
				try {
					d.browse(new URI("https://www.staff.hs-mittweida.de/~pawlaszc/fqlite/"));
				 } catch (IOException e1) {
					 e1.printStackTrace();
				 } catch (URISyntaxException e1) {
					 e1.printStackTrace();
				 }

			}
		});
		mnInfo.add(mntmHelp);
		mnInfo.add(mntFont);
		mnInfo.add(mntAbout);

		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		contentPane.setLayout(new BorderLayout(0, 0));
		setContentPane(contentPane);

		JSplitPane splitPane = new JSplitPane();
		splitPane.setDividerLocation(0.2);
		contentPane.add(splitPane, BorderLayout.CENTER);

		JScrollPane scrollPane = new JScrollPane();
		splitPane.setLeftComponent(scrollPane);

		scrollpane_tables = new JScrollPane(null, JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED,
				JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
		table_panel_with_filter = new JPanel();
		table_panel_with_filter.setLayout(new BorderLayout());
		table_panel_with_filter.add(scrollpane_tables, BorderLayout.CENTER);
		splitPane.setRightComponent(table_panel_with_filter);

		
	    head = new JPanel();
		head.add(new JLabel("Filter:"));
		table_panel_with_filter.add(head, BorderLayout.NORTH);
		
	
		
		prepare_table_default();

		tree.setMinimumSize(new Dimension(300,4000));
		tree.setAutoscrolls(true);

		scrollPane.setViewportView(tree);
		scrollPane.setMinimumSize(new Dimension(300,600));
		
		statusbar.showStatus("Ready");

		JPanel panel = new JPanel();
		panel.setPreferredSize(new Dimension(500, this.getHeight() / 5));
		contentPane.add(panel, BorderLayout.SOUTH);
		panel.setLayout(new BorderLayout(5, 5));

		JScrollPane scrollPane_3 = new JScrollPane();
		panel.add(scrollPane_3, BorderLayout.CENTER);

		logwindow = new JTextArea("Welcome");
		logwindow.setBackground(SystemColor.text);
		logwindow.setEditable(false);
		scrollPane_3.setViewportView(logwindow);

		panel.add(statusbar, BorderLayout.SOUTH);

		JToolBar toolBar = new JToolBar();
		contentPane.add(toolBar, BorderLayout.NORTH);

		JButton btnOeffne = new JButton("");
		btnOeffne.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				open_db(null);
			}
		});

		btnOeffne.setToolTipText("open database file");
		url = GUI.class.getResource("/openDB_gray.png");
		btnOeffne.setIcon(new ImageIcon(url));

		toolBar.add(btnOeffne);

		JButton btnExport = new JButton("");
		btnExport.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				doExport();
			}
		});
		btnExport.setToolTipText("export data base to file");
		url = GUI.class.getResource("/export_gray.png");
		btnExport.setIcon(new ImageIcon(url));
		toolBar.add(btnExport);

		JButton btnClose = new JButton("");
		btnClose.setToolTipText("Close All");
		url = GUI.class.getResource("/closeDB_gray.png");
		btnClose.setIcon(new ImageIcon(url));
		toolBar.add(btnClose);

		btnClose.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {

				closeAll();
			}
		});

		JButton about = new JButton("");
		about.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {
				JDialog about = new AboutDialog(mainwindow);
				about.setVisible(true);
			}
		});
		about.setToolTipText("about");
		url = GUI.class.getResource("/helpcontent_gray.png");
		about.setIcon(new ImageIcon(url));
		toolBar.add(about);

		JButton btnexit = new JButton("");
		btnexit.setToolTipText("exit");

		url = GUI.class.getResource("/exit3_gray.png");
		btnexit.setIcon(new ImageIcon(url));
		btnexit.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				System.exit(-1);
			}
		});
		toolBar.add(btnexit);

		
		JButton hexViewBtn = new JButton("");
		hexViewBtn.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				DefaultMutableTreeNode node = (DefaultMutableTreeNode) tree.getLastSelectedPathComponent();
				if (node == null || node == root) {

				} else if (null != node.getUserObject()) {
					NodeObject no = (NodeObject) node.getUserObject();
					
					// show Hex-View of DB-File
					if (no.type == FileTypes.SQLiteDB)
					{	
						
						
						if (null != no.job) {
							HexView hv = no.job.hexview;
							if (null != hv)
								hv.setVisible(true);
	
						}

					
					}
					// show Hex-View of WAL-File
					else if (no.type == FileTypes.WriteAheadLog)
					{
						
						if (null != no.job.wal) {
							HexView hv = no.job.wal.hexview;
							if (null != hv)
								hv.setVisible(true);
							
						}
					}	
					else if (no.type == FileTypes.RollbackJournalLog)
					{
						
						if (null != no.job.rol) {
							HexView hv = no.job.rol.hexview;
							if (null != hv)
								hv.setVisible(true);
							
						}
					}
						
				}
			}
		});
		url = GUI.class.getResource("/hex-32.png");
		hexViewBtn.setIcon(new ImageIcon(url));
		hexViewBtn.setToolTipText("open database in HexView");
		toolBar.add(hexViewBtn);

		tree.addMouseListener(new PopUpListener(this));

		url = GUI.class.getResource("/facewink.png");
		facewink = new ImageIcon(url);

		url = GUI.class.getResource("/error-48.png");
		errorIcon = new ImageIcon(url);

		url = GUI.class.getResource("/information-48.png");
		infoIcon = new ImageIcon(url);

		url = GUI.class.getResource("/question-48.png");
		questionIcon = new ImageIcon(url);

		url = GUI.class.getResource("/warning-48.png");
		warningIcon = new ImageIcon(url);

		
		this.getContentPane().setDropTarget(new DropTarget() {
		
			private static final long serialVersionUID = 1L;

			@SuppressWarnings("unchecked")
			public synchronized void drop(DropTargetDropEvent evt) {
		        try {
		            evt.acceptDrop(DnDConstants.ACTION_COPY);
		            List<File> droppedFiles = (List<File>)evt.getTransferable().getTransferData(DataFlavor.javaFileListFlavor);
		            for (File file : droppedFiles) {
		            		
		            	open_db(file);
		            }
		        } catch (Exception ex) {
		            ex.printStackTrace();
		        }
		    }
		});
	}

	/**
	 *  Close all databases that are currently open. 
	 */
	public void closeAll() {

		DefaultMutableTreeNode root = (DefaultMutableTreeNode) tree.getModel().getRoot();

		@SuppressWarnings("unchecked")
		Enumeration<DefaultMutableTreeNode> en = root.children();
		while (en.hasMoreElements()) {
			DefaultMutableTreeNode node = en.nextElement();

			if (null != node.getUserObject()) {
				NodeObject no = (NodeObject) node.getUserObject();
				if (null != no.job) {
					no.job.hexview = null;
				}
			}
		}
		root.removeAllChildren();
		tables.clear();
		updateTreeUI();
	}

	/**
	 * Start a data export.
	 */
	public void doExport() {
		NodeObject no = null;
		
		/* Do we really have a database node currently selected ? */
		DefaultMutableTreeNode node = (DefaultMutableTreeNode) tree.getLastSelectedPathComponent();
		if (node == null || node == root) {
		} 
		else if (null != node.getUserObject()) {
			no = (NodeObject) node.getUserObject();
			if (null == no.job) {}
		}

		/* it is indeed a valid database node -> begin with export */
		export_table(no);
	}


	
	 public void SetIcon(JTable table, int col_index, ImageIcon icon,String name){
		  table.getTableHeader().getColumnModel().getColumn(col_index).setHeaderRenderer
		(new IconRenderer());
		  table.getColumnModel().getColumn(col_index).setHeaderValue(new fqlite.ui.txtIcon(name, icon));
     }

		
	 
	/**
	 * Add a new table header to the database tree.
	 * 
	 * @param job
	 * @param tablename
	 * @param columns
	 * @return
	 */
	TreePath add_table(Job job, String tablename, List<String> columns, List<String> columntypes, List<String> PK, boolean walnode,
			boolean rjnode, int db_object) {

		NodeObject o = null;

		CustomTableModel model = new CustomTableModel();
		model.addColumn(" ");
		model.addColumn(Global.STATUS_CLOMUN);
		model.addColumn("Offset");
		for (String colname : columns) {
			model.addColumn(colname);
		}
		for (String coltype : columntypes) {
			model.addColumnType(coltype);
		}
		
		DBTable table = new DBTable(model, walnode, this);
		
			
		TableRowSorter<CustomTableModel> sorter = new TableRowSorter<CustomTableModel>();
		table.setRowSorter(sorter);
		sorter.setModel(model);
		
		table.setRowSelectionAllowed(true);
		table.setCellSelectionEnabled(true);
		table.setColumnSelectionAllowed(true);
		
		
		ImageIcon imageIcon = new ImageIcon(Toolkit.getDefaultToolkit().getImage(GUI.class.getResource("/icon_status.png")));
		SetIcon(table, 1, imageIcon,"");
		
		/* add icon to PRIMARYKEY columns */
		if (null != PK)
		{
			Iterator<String> it = PK.iterator();
			while(it.hasNext())
			{
				String pkcol = it.next();
				for (int c = 0; c < model.getColumnCount(); c++)
				{
					if (pkcol.equals(model.getColumnName(c)))
					{
						// there is a column with this name -> set PK icon
						ImageIcon pkIcon = new ImageIcon(Toolkit.getDefaultToolkit().getImage(GUI.class.getResource("/key-icon.png")));
						SetIcon(table, c, pkIcon,model.getColumnName(c));
					
					}	
				}
			}
		}
		
		
		if (walnode)
			o = new NodeObject(tablename, table, columns.size(), FileTypes.WriteAheadLog, db_object); // wal file
		if (rjnode)
			o = new NodeObject(tablename, table, columns.size(), FileTypes.RollbackJournalLog, db_object); // rollback
																										// journal file

		if (!walnode && !rjnode)
			o = new NodeObject(tablename, table, columns.size(), FileTypes.SQLiteDB, db_object); // normal db

		o.job = job;
		DefaultMutableTreeNode dmtn = new fqlite.ui.FTreeNode(o);

		if (walnode) {
			/* WAL-tree node - add child node of table */
			walNode.add(dmtn);
		}
		if (rjnode) {
			/* Rollback Journal */
			rjNode.add(dmtn);
		}

		if (!walnode && !rjnode) /* main db */
			dbNode.add(dmtn);

		TableColumnModel columnModel = table.getColumnModel();
		columnModel.getColumn(0).setPreferredWidth(20);
		columnModel.getColumn(0).setMinWidth(20);
		columnModel.getColumn(0).setMaxWidth(150);
		
		columnModel.getColumn(1).setPreferredWidth(20);
		columnModel.getColumn(1).setMinWidth(20);
		columnModel.getColumn(1).setMaxWidth(150);
		table.setName(tablename);
		table.setColumnSelectionAllowed(true);
		table.setRowSelectionAllowed(true);
		table.setCellSelectionEnabled(true);

		TreePath tp = getPath(dmtn);

		/* create popup menu */
		final JPopupMenu pm = new JPopupMenu();
		pm.add(new CopyAction(table));
		pm.add(new PasteAction(table));
		table.setComponentPopupMenu(pm);

		table.addMouseListener(new MouseAdapter() {

			//JDialog preview;
			
			
			@Override
			public void mouseEntered(MouseEvent e)
			{
				
				
			}
			
			
			public void mouseExited(MouseEvent event)
			{
				//if (null != preview)
					//preview.setVisible(false);
					//preview.dispose();
			}
			
			@Override
			public void mouseClicked(MouseEvent e) {
				
				ImageIcon ico = getImageIcon(e);
				if (null != ico)
				{
					//vi.toFront();
					//vi.updateImage(ico);
					//vi.show(ico);
					//vi.setVisible(true);
					//vi.repaint();
					// show a joptionpane dialog using showMessageDialog
				     //   JOptionPane.showMessageDialog(GUI.app,
				     //   "Found Image-BLOB.",
				     //   "Preview",
				     //   JOptionPane.INFORMATION_MESSAGE,ico);
				}
				
				if (e.isPopupTrigger()) {
					highlightRow(e);
					doPopup(e);
				}
			}
			
			@Override
			public void mousePressed(MouseEvent e) {
				ImageIcon ico = getImageIcon(e);
				if (null != ico)
				{
				    
					JOptionPane.showMessageDialog(GUI.app, "Found Image-BLOB.",  "Preview",  JOptionPane.INFORMATION_MESSAGE,ico);
				}
			}

			@Override
			public void mouseReleased(MouseEvent e) {
			    
				
				
				if (e.isPopupTrigger()) {
					highlightRow(e);
					doPopup(e);
				}
			}

			public void doPopup(MouseEvent e) {
				pm.show(e.getComponent(), e.getX(), e.getY());
			}

			public void highlightRow(MouseEvent e) {
				JTable table = (JTable) e.getSource();
				Point point = e.getPoint();
				int row = table.rowAtPoint(point);
				int col = table.columnAtPoint(point);

				table.setRowSelectionInterval(row, row);
				table.setColumnSelectionInterval(col, col);
			}

			
			
			public ImageIcon getImageIcon(MouseEvent event)
			{
				Point point = event.getPoint();
				int column = table.columnAtPoint(point);
				int row = table.rowAtPoint(point);
				ImageIcon out = null; 
				
				
				//if (column > 1) //&& lastclickrow != row)
				{
					JTable table = (JTable)event.getSource();
					Object o = table.getValueAt(row, column);
					
					
				    if (o instanceof String)
				    {
						String v = (String)o;
				    	if (BLOBCarver.isGraphic(v))
						{
				    		byte[] b = CustomCellRenderer.hexStringToByteArray(v.toUpperCase());
								
						}	
				    	
				    	
				    	
				    }	
				}
				return out;
			}
			
		});
		
		

		tables.put(tp, table);

	    TableColumn testColumn = table.getColumnModel().getColumn(2);
	    JComboBox<String> comboBox = new JComboBox<>();
	    comboBox.addItem("default");
	    comboBox.addItem("hex2UTF");
	    comboBox.addItem("int2time");
	    comboBox.addItem("float2time");
	    testColumn.setCellEditor(new DefaultCellEditor(comboBox));

		
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				// Here, we can safely update the GUI
				// because we'll be called from the
				// event dispatch thread
				scrollpane_tables.updateUI();
			}
		});

		table.addMouseListener(new TableMouseListener((JTable) table));

		return tp;
	}
	
	 


	/**
	 * Returns the Path object of for a given treeNode.
	 * @param treeNode
	 * @return TreePath 
	 */
	public static TreePath getPath(TreeNode treeNode) {
		List<Object> nodes = new ArrayList<Object>();
		if (treeNode != null) {
			nodes.add(treeNode);
			treeNode = treeNode.getParent();
			while (treeNode != null) {
				nodes.add(0, treeNode);
				treeNode = treeNode.getParent();
			}
		}

		return nodes.isEmpty() ? null : new TreePath(nodes.toArray());
	}

	/**
	 * Sometimes we need an empty table as placeholder.
	 * 
	 */
	protected void prepare_table_default() {

		if (tree == null) {
			tree = new JTree(root);
			tree.setCellRenderer(new CustomTreeCellRenderer());

		}

		tree.getSelectionModel().addTreeSelectionListener(new TreeSelectionListener() {

			@Override
			public void valueChanged(TreeSelectionEvent e) {

				TreePath path = e.getNewLeadSelectionPath();

				if (null == path)
					return;
				Logger.out.debug(path.toString());
				JComponent cp = tables.get(path);
				if (null == cp)
					return;

				if (cp instanceof DBTable) {
					DBTable tb = (DBTable) cp;
					tb.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);

					JTextField filterField = RowFilterUtil.createRowFilter(tb);
					if (null != currentFilter) {
						head.remove(currentFilter);
					}
					head.add(filterField);
					currentFilter = filterField;

					table_panel_with_filter.add(head, BorderLayout.NORTH);
					table_panel_with_filter.updateUI();
					scrollpane_tables.setViewportView(tb);
					updateTableUI();
				} else {

					scrollpane_tables.setViewportView(cp);

					updateTableUI();
				}
			}
		});

	}
	
	/**
	 *  Reload the TableView scroll pane. 
	 */
	public void updateTableUI() {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				// Here, we can safely update the GUI
				// because we'll be called from the
				// event dispatch thread
				scrollpane_tables.updateUI();
			}
		});
	}

	/**
	 *  Reload the TreeView component. 
	 */
	public void updateTreeUI() {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				// Here, we can safely update the GUI
				// because we'll be called from the
				// event dispatch thread
				tree.updateUI();
			}
		});
	}

	

	/**
	 * Show an open dialog and import <code>sqlite</code>-file.
	 * 
	 */
	public void open_db(File f) {
		File file = f;
		
		if (file == null)
		{	
			JFileChooser chooser = new JFileChooser();
			if (lastDir == null)
				chooser.setCurrentDirectory(new File(System.getProperty("user.home")));
			else
				chooser.setCurrentDirectory(lastDir);
			chooser.setDialogTitle("open database");
			chooser.setName("open database");
			FileNameExtensionFilter filter = new FileNameExtensionFilter("Sqlite & DB Files (*.sqlite,*.db)", "sqlite",	"db");
			chooser.setFileFilter(filter);
			int returnVal = chooser.showOpenDialog(this);
			if (returnVal == JFileChooser.APPROVE_OPTION) {
				file = chooser.getSelectedFile();
				lastDir = chooser.getCurrentDirectory();
			}
		}
			
		if (file == null)
			return;
			
		/* check file size - the size has to be at least 512 Byte */
		if (file.length() < 512)
		{
			JOptionPane.showMessageDialog(GUI.app, "File size is smaller than 512 bytes. Import stopped",  "Error",  JOptionPane.ERROR_MESSAGE,null);
			return;
		}
		
		RandomAccessFile raf = null;
		boolean abort = false;
		/* check header string for magic number to match */
		try 
		{
			raf = new RandomAccessFile(file,"r");
			byte h[] = new byte[16];
			raf.read(h);
			if (!Auxiliary.bytesToHex(h).equals(Job.MAGIC_HEADER_STRING)) // we currently
			{
				abort = true;
				JOptionPane.showMessageDialog(GUI.app, "Couldn't find a valid SQLite3 magic. Import stopped",  "Error",  JOptionPane.ERROR_MESSAGE,null);
			}
		}
		catch(Exception err)
		{
			JOptionPane.showMessageDialog(GUI.app, "IO-Exception. Cloud not open file.",  "Error",  JOptionPane.ERROR_MESSAGE,null);
			abort = true;
		}
		finally		
		{
			try { raf.close(); } catch(IOException err){}
		}
	    /* no valid file or no permissions -> cancel import */
		if (abort)
			return;
		
		FileInfo info = new FileInfo(file.getAbsolutePath());
		
		 
		
		DBPropertyPanel panel = new DBPropertyPanel(info);
			
		NodeObject o = new NodeObject(file.getName(), null, -1, FileTypes.SQLiteDB, 99);
		dbNode = new FTreeNode(o);
		root.add(dbNode);

		/* insert Panel with general header information for this database */
		TreePath tp = getPath(dbNode);
			
		Job job = new Job();
		tables.put(tp, panel);
		updateTableUI();
			
			
		/* Does a companion RollbackJournal exist ? */
		if (doesRollbackJournalExist(file.getAbsolutePath()) > 0) {
				NodeObject ro = new NodeObject(file.getName() + "-journal", null, -1,
						FileTypes.RollbackJournalLog, 100);
				rjNode = new FTreeNode(ro);
				root.add(rjNode);

				/* insert Panel with general header information for this database */
				TreePath tpr = getPath(rjNode);
				FileInfo rinfo = new FileInfo(file.getAbsolutePath()+"-journal");
				RollbackPropertyPanel rpanel = new RollbackPropertyPanel(rinfo);
				tables.put(tpr, rpanel);

				updateTableUI();
				job.setRollbackPropertyPanel(rpanel);
				ro.job = job;
				ro.job.readRollbackJournal = true;
				ro.job.readWAL = false;

		}

			/* Does a companion WAL-archive exist ? */
			else if (doesWALFileExist(file.getAbsolutePath()) > 0) {

			NodeObject wo = new NodeObject(file.getName() + "-wal", null, -1, FileTypes.WriteAheadLog, 101);
			walNode = new FTreeNode(wo);
			root.add(walNode);


			/* insert Panel with general header information for this database */
			TreePath tpw = getPath(walNode);
			FileInfo winfo = new FileInfo(file.getAbsolutePath()+"-wal");
			WALPropertyPanel wpanel = new WALPropertyPanel(winfo,this);
			tables.put(tpw, wpanel);

			updateTableUI();
			job.setWALPropertyPanel(wpanel);
			wo.job = job;
			wo.job.readWAL = true;
			wo.job.readRollbackJournal = false;

		}

		DefaultTreeModel model = (DefaultTreeModel) tree.getModel();
		model.reload(root);

		job.setPropertyPanel(panel);
		
		
		
		
		job.setGUI(this);
		job.setPath(file.getAbsolutePath());
		ProgressBar.createAndShowGUI(this, file.getAbsolutePath(), job);
		o.job = job;	

	}

	/**
	 * Try to find out, if there is a companion WAL-Archive. This file can be found
	 * in the same directory as the database file and also has the same name as the
	 * database, but with 4 character added to the end – “-wal”
	 * 
	 * 
	 * @param dbfile
	 * @return
	 */
	public static long doesWALFileExist(String dbfile) {
		
		
		String walpath = dbfile + "-wal";
		Path path = Paths.get(walpath);

		if (Files.exists(path, new LinkOption[] { LinkOption.NOFOLLOW_LINKS }))
			try {
				return Files.size(path);
			} catch (IOException e) {
				e.printStackTrace();
			}

		return -1L;

	}

	/**
	 * Try to find out, if there is a companion RollbackJournal-File. This file can
	 * be found in the same directory as the database file and also has the same
	 * name as the database, but with 8 character added to the end – “-journal”
	 * 
	 * 
	 * @param dbfile
	 * @return file size of the rollback-journal archive or -1 if no -journal files exists. 
	 */
	public static long doesRollbackJournalExist(String dbfile) {
		String rolpath = dbfile + "-journal";
		Path path = Paths.get(rolpath);

		if (Files.exists(path, new LinkOption[] { LinkOption.NOFOLLOW_LINKS }))
			try {
				return Files.size(path);
			} catch (IOException e) {
				e.printStackTrace();
			}

		return -1L;

	}

	/**
	 * Print a new message to the trace window on the bottom of the screen. 
	 * @param message
	 */
	protected void doLog(String message) {
		logwindow.append("\n" + message);
	}

	/**
	 * This method is called to write the contents of a database to a CSV file. 
	 * 
	 * @param no Database node for export
	 */
	private void export_table(NodeObject no) {

		if (null == no)
			return;
		JFileChooser chooser = new JFileChooser();
		chooser.setCurrentDirectory(new File(System.getProperty("user.home")));
		chooser.setDialogTitle("export records to file");
		chooser.setName("export records");
		chooser.setSelectedFile(new File(no.name + ".csv"));
		FileNameExtensionFilter filter = new FileNameExtensionFilter("CSV file ", "csv");
		chooser.setFileFilter(filter);

		int returnVal = chooser.showSaveDialog(this);
		if (returnVal != JFileChooser.APPROVE_OPTION)
			return;

		File f = new File(chooser.getSelectedFile().getAbsolutePath());

		String [] lines = null; 
		
		switch(no.tabletype)
		{
			case 0      :	// table 					   
			case 1      :   // index
				
						String headerline = getColumnLineForTable(no.table);
						lines = filterLines(no.job.ll.iterator(),no.name,headerline);
						
						
						break;
			case 99  :  // database
					
			case 100 :  // journal
					 			
			case 101 :  // wal 
					    
				        lines = no.job.ll.toArray(new String[0]);
				        	
						break;
			default  :  // root;
		}
		
	    /* filter and format export*/
		for (int i = 0; i < lines.length; i++)
        {
        	int idx = lines[i].indexOf("##header##");
        	if (idx > 0)
        	{
           		lines[i] = lines[i].substring(0, idx) +"\n"; 
        		idx = lines[i].indexOf("##header##", idx+1);
        	}
        }	
		
		
		no.job.writeResultsToFile(f.getAbsolutePath(), lines);

		JOptionPane.showMessageDialog(this,
				"Data base " + no.name + " exported successfully to \n" + f.getAbsolutePath());
		

	}
	
	private String getColumnLineForTable(JTable t)
	{
		String result = "_table;";
		
		int no = t.getColumnCount();
		for (int i = 0; i < no; i++)
		{
			result += t.getColumnName(i) + ";";
		}
		
		return result + "\n";
	}
	
	private String[] filterLines(Iterator<String> lines, String filterword, String headerline)
	{
		LinkedList<String> filtered = new LinkedList<String>();
		
		
		while(lines.hasNext())
		{
			String line = lines.next();
			if(line.startsWith(filterword))
			{
				filtered.add(line);
			}
		
		}
		
		filtered.addFirst(headerline);
		
		return filtered.toArray(new String[0]);
	}

	/**
	 * This method is used to insert new records into a output table. 
	 * 
	 * @param tp
	 * @param data
	 */
	protected void update_table(TreePath tp, String[] data, boolean isWALTable) {
		
		
		String tablename = data[0];

		JTable tb = null;
		try {
			
			tb = (JTable) tables.get(tp);
			
		} catch (Exception err) {
			return;
		}
		
		
		if (tb == null) {
			doLog(">>>> Unkown tablename" + tablename);
			return;
		}

		Vector<String> v = new Vector<String>();
		CustomTableModel ctm = ((CustomTableModel) tb.getModel());

		
		// add line number 
		v.add(String.valueOf(ctm.getRowCount()+1));
		
		
		String[] waldata = new String[5];
		
		int last = data.length-1;
		
		String walframe = "";
		
		if(isWALTable)
		{
			String lastcol = data[last];
			
			/* WALframe information ? */
			int ff = lastcol.indexOf("#walframe#");
			if (ff > 0)
			{
				walframe = lastcol.substring(ff+10);
				lastcol = lastcol.substring(0, ff);	
				waldata = walframe.split(",");		
				data[last] = data[last].substring(0, ff);
			}
			
		}
		
		/* Extract header information */
		String header = "";
		
		
		String lastcol = data[last];
			
		/* Header fingerprint ? */
		int hh = lastcol.indexOf("##header##");
		if (hh >= 0) {
			header = lastcol.substring(hh);	
			data[last] = data[last].substring(0, hh);
			/* add header string to table model */
			ctm.addHeader(header.substring(10));
		}
	
		
		for (int i = 1; i < data.length; i++) {
			
			if (i == 2) {
				try
				{
					v.add(String.format("%0" + 6 + "X", Long.parseLong(data[2])));
			
				}catch(NumberFormatException nfe)
				{
					v.add(data[2]);
				}
				
				if(isWALTable)
				{
					v.add(waldata[0]);
					v.add(waldata[1]);
					v.add(waldata[2]);
					v.add(waldata[3]);
					v.add(waldata[4]);
					
				}
			} 
			
			
			
			else
			{
			    v.add(data[i]);
			}
		}
		
			
		ctm.addRow(v);

	}

	public Hashtable<Object, Color> getRowcolors() {
		return rowcolors;
	}

	public void setRowcolors(Hashtable<Object, Color> rowcolors) {
		this.rowcolors = rowcolors;
	}

}



/**
 * A mouse listener class which is used to handle mouse clicking event on column
 * headers of a JTable.
 * 
 * @author www.codejava.net
 *
 */
class TableMouseListener extends MouseAdapter {

	private JTable table;
	private int lastclickrow = -1;

	HexView hv = null;

	public TableMouseListener(JTable table) {
		this.table = table;
	}

	
	
	
	
	public void mouseClicked(MouseEvent event) {

		Point point = event.getPoint();
		int column = table.columnAtPoint(point);
		int row = table.rowAtPoint(point);
//
//		if (column > 1 && lastclickrow != row)
//		{
//			JTable table = (JTable)event.getSource();
//			Object o = table.getValueAt(row, column);
//			
//		    if (o instanceof String)
//		    {
//				String v = (String)o;
//		    	if (BLOBCarver.isGraphic(v))
//				{
//					byte[] b = CustomCellRenderer.hexStringToByteArray(v.toUpperCase());
//					try {
//						ImageIcon out = new ImageIcon(b,"test");
//						ImageViewer vi = new ImageViewer(null,out);
//					} catch (Exception e) {
//						
//						e.printStackTrace();
//					}
//					
//				}	
//		    	
//		    	
//		    	
//		    }	
//		}
		
		/* When the address column is clicked, the HexView is automatically generated.*/
		if (column == 2 && lastclickrow != row) {

			SwingWorker<Boolean, Void> backgroundProcess = new SwingWorker<Boolean, Void>() {

				@Override
				protected Boolean doInBackground() throws Exception {

					String value = (String) table.getModel().getValueAt(row, column);
					long v = Long.parseLong(value, 16);
					int start = (int) v * 2;

					DefaultMutableTreeNode node = (DefaultMutableTreeNode) GUI.tree.getLastSelectedPathComponent();

					if (node != null) {

						if (null != node.getUserObject()) {
							NodeObject no = (NodeObject) node.getUserObject();
							if (null != no.job) {

								if (no.type == FileTypes.SQLiteDB)
									hv = no.job.hexview;
								else if (no.type == FileTypes.WriteAheadLog)
									hv = no.job.wal.hexview;
								else if (no.type == FileTypes.RollbackJournalLog)
									hv = no.job.rol.hexview;

								CustomTableModel ctm = (CustomTableModel) table.getModel();
								String hd = ctm.getHeader(row);

								String text = hv.thex.getText();

								String status = (String) table.getModel().getValueAt(row, 2);

								if (status.contains(Global.DELETED_RECORD_IN_PAGE)) {
									/* a removed entry - skip the first two bytes */
									hd = hd.substring(2);

								}

								int match = -1;
								Pattern exampleRegex = Pattern.compile(hd.trim(), Pattern.DOTALL);

								String test = text.substring(start + (start / 32 + 4 + hd.length()),
										start + (start / 32) + 4 * hd.length());
								test = test.replace(System.getProperty("line.separator"), "");

								Matcher m = exampleRegex.matcher(test);

								if (m.find()) {
									match = m.start();
								}

								int hdstart = -1;
								hdstart = match + start;
								if (status.contains(Global.DELETED_RECORD_IN_PAGE))
									hdstart = match + start - hd.length();

								Color[] colors = new Color[] { Color.yellow, Color.orange, Color.green, Color.cyan,
										Color.red, Color.blue, Color.magenta, Color.pink, Color.gray };
								int linebreaks = start / 32;
								hdstart += linebreaks;
								int currenthex = hdstart;
								int currenttxt = (hdstart / 2) + linebreaks / 2 + 1;

								if ((currenthex % 33) % 2 == 1) {
									currenthex++;
									currenttxt++;
								}

								if (status.contains(Global.DELETED_RECORD_IN_PAGE))
									currenttxt--;

								Highlighter highlighter = hv.thex.getHighlighter();
								Highlighter highlighterText = hv.ttext.getHighlighter();

								SqliteElement[] columns = Auxiliary.toColumns(hd);
								int delta = 2;
								for (int i = 0; i < columns.length; i++) {

									HighlightPainter painter = new DefaultHighlighter.DefaultHighlightPainter(
											colors[i % 9]);

									delta = 2;
									if (hv.thex.getText().substring(currenthex, currenthex + 2).indexOf("\n") >= 0)
										delta = 3;
									highlighter.addHighlight(currenthex, currenthex + delta, painter);
									currenthex += delta;

									delta = 1;
									if (hv.ttext.getText().substring(currenttxt, currenttxt + 1).indexOf("\n") >= 0) {
										delta = 2;

									}
									highlighterText.addHighlight(currenttxt, currenttxt + delta, painter);
									currenttxt += delta;
								}

								hv.thex.setCaretPosition(currenthex);

							}
						}
					}

					return true;
				}

				@Override
				protected void done() {
					// Process ended, mark some ended flag here
					// or show result dialog, messageBox, etc
					hv.setVisible(true);
				}

			};

			backgroundProcess.execute();
		}

	}

}
