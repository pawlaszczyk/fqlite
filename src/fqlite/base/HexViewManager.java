package fqlite.base;

import fqlite.ui.hexviewer.HexViewerApp;
import javafx.stage.Stage;

public class HexViewManager {

	HexViewerApp app = null;
	private final static HexViewManager instance = new HexViewManager();
	
	
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
		app.clearAll();
		app = null;
	}
	
	public void show() {
		if (null == app || isClosed())
			start();
		HexViewerApp.setVisible();
	}
	
	public void go2(String model, long position) {
		
		  if (null == app || isClosed())
			start();
		  
		 
		  app.switchModel(model);
//	  app.loadLines(model, position);
		  app.goTo(position);
		 
		  HexViewerApp.setVisible();
		   
	}
	
	public void onClose(){
		try {
			app.stop();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
