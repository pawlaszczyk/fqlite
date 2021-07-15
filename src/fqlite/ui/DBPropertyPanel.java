package fqlite.ui;

import java.awt.BorderLayout;
import java.awt.Desktop;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.table.JTableHeader;

import fqlite.base.GUI;

public class DBPropertyPanel extends JPanel {

	
	private static final long serialVersionUID = 3116420145072139413L;
	public JLabel ldbpath;
    public JLabel lpagesize;
    public JLabel lencoding;
    public JLabel ltotalsize;   
    public JLabel lpagesizeout;
    public JLabel lencodingout;
    public JLabel ltotalsizeout;
    private FileInfo info;
    public JButton columnBtn;
    public String  columnStr;
    
    
    JTabbedPane tabpane = new JTabbedPane(JTabbedPane.TOP,JTabbedPane.SCROLL_TAB_LAYOUT );

    
	public DBPropertyPanel(FileInfo info)
	{
		this.info = info;
		URL url = GUI.class.getResource("/find.png");

		this.columnBtn = new JButton("View Schema Details");
		this.columnBtn.setIcon(new ImageIcon(url));
		this.columnBtn.setToolTipText("Show Schema Information with Standard Webbrowser");
		

	}
	
	
	
	
	public void initHeaderTable(String[][] data)
	{
	
		setLayout(new BorderLayout());	
		JTextArea headerinfo = new JTextArea();
		PopupFactory.createPopup(headerinfo);
		headerinfo.setColumns(85);
		headerinfo.setAlignmentX(CENTER_ALIGNMENT);
		Font font = new Font("Courier", Font.BOLD, 12);
	    headerinfo.setFont(font);
	    headerinfo.setText(info.toString());
		tabpane.addTab("File info",headerinfo);
			
		String column[]={"Offset","Property","Value"};         
		JTable jt=new JTable(data,column);    
		PopupFactory.createPopup(jt);

		
		JTableHeader th = jt.getTableHeader();
		th.setFont(new Font("Serif", Font.BOLD, 15));
		
		JScrollPane sp=new JScrollPane(jt);    
		add(sp,BorderLayout.CENTER);
	    tabpane.addTab("Header Fields",sp);
	    
	    add(tabpane, BorderLayout.CENTER);
	    
		showColumnInfo();

	}
	
	public void initColumnTypesTable(LinkedHashMap<String,String[][]> ht)
	{
        String str = "<!DOCTYPE html>" + "<html>"
        		+ " <head> "
        		+ "<title>" + info.filename +" - Schema Information</title>"
        		+ " <style type=\"text/css\">\n"
        		+ "  table, td, tr { border:1px solid black;}\n"
        		+ " </style>"
                + " </head>"
            	+ "<h1>Schema Information for database: " + info.filename + "</h1>";	
        
        str += "<a id=\"top\"></a>";
        str += "<br />";
        str += "<hr>";
        str += "<h2> TABLES </h2>";
        str += "<hr>";
        
        boolean firstindex = true;
        
        for (Map.Entry<String, String[][]> entry : ht.entrySet()) {
            String tname = entry.getKey();
         
            if (tname.startsWith("__"))
        		continue;
            if (firstindex && tname.startsWith("idx:"))
            {
                str += "<br />";
                str += "<hr>";
                str += "<h2> INDICES </h2>";
                str += "<hr>";
            	firstindex=false;
            }
        	str += "<a href=\"#"+tname+"\">"+tname+"</a><br />";
        }
        	
        
        str += "<br />";
        str += "<hr>";
        str += "<br />";

        
        for (Map.Entry<String, String[][]> entry : ht.entrySet()) {
            String tname = entry.getKey();
           
       
        	if (tname.startsWith("__"))
        		continue;
        	
        	str += "<a id=\""+tname+"\"></a>";
        	str += "<p>";
        	String bgcolor = "#00008b";
        	
        	if(tname.startsWith("idx:"))
        	{
        		bgcolor = "#DF0101";
        	}
      
        	
        	String [][] tab = entry.getValue();
    	    int cols =	tab[0].length;
         	
        	str	+= "<table rules=groups>";
        	str	+= "<thead bgcolor="+bgcolor+" style=\"color:white;white-space:nowrap;width:100%;\" bordercolor=#000099>";
        	str	+= "<tr>";
        	if(tname.startsWith("idx:"))
        	{
        		str	+= "<th>INDEX </th><th>"+ "\""+ tname + "\"</th>"; 
            }
        	else
        	{
        		str	+= "<th>TABLE	 </th><th>"+ "\""+ tname + "\"</th>";    
            }	
        	for (int i=0; i < cols-1;i++)
            	str	+= "<th></th>";
        	str += "</thead>";
        	str	+= "</tr>";
        	str += "<tbody>";
        	
	        
       
        	
        	for (int i = 0; i < tab.length; i++) {
	            
	        	 str += "<tr>";
	             
        	 	 switch(i)	
            	 { 
            		case 0 :  
            			str+="<td> <b> column name </b> </td>";
            			break;
            		case 1 :  
            			str+="<td> <b> serialtype </b> </td>";
                		break;
            		case 2 :  
            			str+="<td> <b> sqltype </b> </td>";
                		break;
            		case 3 :
            			str+="<td> <b> column constraints </b> </td>";
                		break;                		
            		case 4 : 	
            			str+="<td> <b> table constraint </b> </td>";
            			break;	
                	default :
                		str+="<td>  </td>";
                		
            	 }
        	 
	        	 
	             for(int j=0; j <tab[i].length; j++)
	             {
	            	 str+="<td>" + tab[i][j] + "</td>";
	             }
	             
	             str+="</tr>";
	            
	        }
	        str += "</tbody>";
	        str += "</table>";
	        str += "<a href=\"#top\">[TOP]</a><br />";
	        str += "</p>";
        
        }
        
        
        
         columnStr = str;
	}
	
	public void showColumnInfo()
	{
		    columnBtn.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
    
			    File file = new File("test.html");
			    try {
			        Files.write(file.toPath(), columnStr.getBytes());
			        Desktop.getDesktop().browse(file.toURI());
				 } catch (IOException e1) {
					 e1.printStackTrace();
				 } 

			}
		});
		
		
	}
	
	
	
	
	public void initSchemaTable(String[][] data)
	{
		
		
		String column[]={"Type","Tablename","Root","SQL-Statement","Virtual","ROWID"};         
		JTable jt = new JTable(data,column);    
		PopupFactory.createPopup(jt);
		
		//Layout the buttons from left to right.
		JPanel buttonPane = new JPanel();
		buttonPane.setLayout(new FlowLayout(FlowLayout.CENTER));//new BoxLayout(buttonPane, BoxLayout.LINE_AXIS));
		buttonPane.setBorder(BorderFactory.createEmptyBorder(0, 10, 10, 10));
		buttonPane.add(Box.createHorizontalGlue());
		buttonPane.add(columnBtn);
		buttonPane.add(Box.createRigidArea(new Dimension(10, 0)));
	
	
		JTableHeader th = jt.getTableHeader();
		th.setFont(new Font("Serif", Font.BOLD, 15));
		
		JPanel schema = new JPanel();
		schema.setLayout(new BorderLayout());
		schema.add(buttonPane, BorderLayout.PAGE_START);
		JScrollPane sp = new JScrollPane();
		sp.setViewportView(jt);
		schema.add(sp, BorderLayout.CENTER);
				
        tabpane.addTab("SQL-Schema",schema);
			

	}
}
