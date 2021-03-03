package fqlite.ui;

import java.awt.Font;
import java.io.IOException;
import java.net.URL;

import javax.swing.JDialog;
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.UIManager;
import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.StyleSheet;

import fqlite.base.GUI;

/**
 * Java class brings up some Dialog.
 * @author pawlaszc
 *
 */
public class HelpWindow
{
	  public static void show(JFrame parent) {
	        JDialog frame = new JDialog(parent);
	 
	        JEditorPane editorPane = new JEditorPane();
	 
	        editorPane.setEditable(false);

	        // add a HTMLEditorKit to the editor pane
	        HTMLEditorKit kit = new HTMLEditorKit();
	        editorPane.setEditorKit(kit);
	        Font font=UIManager.getFont("Label.font");
	        StyleSheet css = kit.getStyleSheet();
	        css.addRule("body { font-family: " + font.getFamily() + "; "+ "font-size: 14pt; margin: 10pt; margin-top: 0pt; "+ "}");
	        css.addRule(".step { margin-bottom: 5pt; }");
	        css.addRule("ol { padding-left: 14px; margin: 0px; }");
	        css.addRule("a { color: black; text-decoration:none; }");
	        css.addRule("p.note { font-style: italic; margin: 0pt; margin-bottom: 5pt; }");
	        
	        try {
	        	URL url = GUI.class.getResource("/help/index.html");
	    		
	            editorPane.setPage(url);
	        } 
	        catch (IOException ioe) {
	               editorPane.setText("<html> <center>"
	                    + "<h1>Page not found</h1>"
	                    + "</center> </html>.");
	        }
	 
	        editorPane.setEditable(false);
	 
	        JScrollPane scrollPane = new JScrollPane(editorPane);
	        frame.add(scrollPane);
	        frame.pack();
	        frame.setSize(1024, 730);
			frame.setLocationRelativeTo(null);
	        frame.setVisible(true);
	    }
}
