package fqlite.ui.hexviewer;

import java.io.File;
import demo.fxtexteditor.Styles;
import fqlite.base.GUI;
import fqlite.base.HexFXWindow;
import goryachev.common.util.FileSettingsProvider;
import goryachev.common.util.GlobalSettings;
import goryachev.fx.CssLoader;
import goryachev.log.config.JsonLogConfig;
import javafx.application.Application;
import javafx.stage.Modality;
import javafx.stage.Stage;

public class HexViewerApp extends Application
{

public static Stage parent;
static HexFXWindow mw;
public Stage myStage;
public boolean closed = false;

public static void main(String[] args)
{
	JsonLogConfig.configure(new File("log-conf.json"), 1000);
	launch(args);
}


public void init() throws Exception
{
			
	File settingsFile = new File(GUI.baseDir, "settings.conf");
	System.out.println("settingsFile " + settingsFile.getAbsolutePath());	
	FileSettingsProvider p = new FileSettingsProvider(settingsFile);
	GlobalSettings.setProvider(p);
	p.loadQuiet();
	
}

public void goTo(long offset){
  
	mw.goTo(offset);	
}

public void loadLines(String path, long offset){
	mw.loadLines(path, offset);
}

public void switchModel(String path){
	mw.loadNewHexFile(path);
}


public void loadNewHexFile(String path){
	
	mw.loadNewHexFile(path);
}

public void clearAll(){
	mw.clearAll();
	mw = null;
}



public static void setVisible(){
	mw.setIconified(false);
	mw.toFront();
}

public void start(Stage stage) throws Exception
{	
	myStage = stage;
	// we need to explicitly do a call to init() - I don't know why :-)
	init();
	
	mw = new HexFXWindow();
	
	myStage.setAlwaysOnTop(true);
	myStage.initModality(Modality.APPLICATION_MODAL);

	
	try {

		mw.open();
		mw.setOnCloseRequest(e ->{ 
			System.out.println("HexView was closed");
			closed = true;		    
		});
		
	}catch(Exception err) {
		
		err.printStackTrace();
		
	}
	
	// init styles
	CssLoader.setStyles(() -> new Styles());		
}


}
