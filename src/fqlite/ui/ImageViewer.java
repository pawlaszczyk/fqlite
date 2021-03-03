package fqlite.ui;

import java.awt.Dimension;
import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JWindow;

public class ImageViewer extends JWindow
{
  private static final long serialVersionUID = -3899019766669490549L;
  ScrollablePicture s;
  JLabel label;
 
  public void updateImage(ImageIcon img)
  {
	  	label = new JLabel(img);
  }
  
  
  public ImageViewer(JFrame parent, ImageIcon img)
  {
	  super(parent);
	  if (null != img)
		  create(parent,img);
  }  
  
  public void show(ImageIcon img)
  {
	  s = new ScrollablePicture(img,1);
	  this.update(getGraphics());
  }
  
  public void create(JFrame parent, ImageIcon img)
  {
    //setTitle("Preview");
    setPreferredSize(new Dimension(400,400));

    // Create a Swing label and a panel for double buffering.

    label = new JLabel(img);
    JPanel panel = new JPanel();
    panel.add(label);

    // Create a scroll pane and add the panel to it.

    ScrollablePicture s = new ScrollablePicture(img,1);

    getContentPane().add(s);
	//pack();
	
    setVisible( true );
  }
  
}
