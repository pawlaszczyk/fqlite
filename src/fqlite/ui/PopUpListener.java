package fqlite.ui;

import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;

import fqlite.base.GUI;

/**
 * Listener class. Every time the user clicks on a 
 * Mouse Button (the right) this event is handled 
 * within this class. 
 * 
 * @author pawlaszc
 *
 */
public class PopUpListener extends MouseAdapter {
	    
		GUI gui;
	
	    public PopUpListener(GUI gui)
	    {
	    	this.gui = gui;
	    }
	    
	
		public void mousePressed(MouseEvent e) {
	        if (e.isPopupTrigger())
	            doPop(e);
	    }

	    public void mouseReleased(MouseEvent e) {
	        if (e.isPopupTrigger())
	            doPop(e);
	    }

	    private void doPop(MouseEvent e) {
	      
	        
	        if (e.getSource() instanceof JTree)
	        {
	        	JTree tree = (JTree)e.getComponent();
	        	DefaultMutableTreeNode dtn = (DefaultMutableTreeNode)tree.getLastSelectedPathComponent();
	        	
	        	Object userObject = dtn.getUserObject();

	    		if (userObject != null)
	    			if (userObject instanceof NodeObject) {
	    				NodeObject no = (NodeObject) userObject;
	    			
	    		
			    		PopUpMenu menu = new PopUpMenu(gui,no);
			 	        
			    		menu.show(e.getComponent(), e.getX(), e.getY());
			        	
	    		  }
	        } 
	        
	       

	    
	        
	        
	    }
	

	
	
}
