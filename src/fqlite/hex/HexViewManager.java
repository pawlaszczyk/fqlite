package fqlite.hex;

import javafx.stage.Stage;

/**
 *  This class manages the different hex viewer objects.
 *  
 *  @author pawel
 */
public class HexViewManager {

	static fqlite.hex.HexViewerApp app = null;
	private final static HexViewManager instance = new HexViewManager();
	static String current_file = null;
	
	public boolean isClosed(){
		return app.closed;
	}

	public static void setParent(Stage parent){
		HexViewerApp.parent = parent;
	}
	
	public static HexViewManager getInstance(){
		return instance;
	}
	
	public void load(String filename) 
	{
		if(null == app || isClosed()){
			start();
		}
		app.loadNewHexFile(filename);

	}
	
	public void start(){
		
		app = new HexViewerApp();
	    Stage hexstage = new Stage();
	    try {
			app.start(hexstage);
		} catch (Exception e) {
			e.printStackTrace();
		}
	  
	}
	
	public void close(){
		if (null == app || isClosed())
			return;
		app = null;
	}
	
	public void show() {
		if (null == app || isClosed())
			start();
		app.setVisible();
		app.toFront();
	}
	
	public void go2(String model, long position) {

		  if (null == app || isClosed()){
			 load(model);
		  }

		  // There is already a model file open?
		  if (current_file == null || (current_file != null && !current_file.equals(model)))
		  {
			  app.loadNewHexFile(model);
			  current_file = model;
		  }

		  app.go2Position(position);
		  app.setVisible();
		  app.toFront();
	}
	
	public void onClose(){
		try {
			current_file = null;
			app.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
