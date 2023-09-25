package fqlite.ui.hexviewer;

import java.io.File;

import demo.fxtexteditor.MainWindow;
import demo.fxtexteditor.Styles;
import fqlite.base.GUI;
import goryachev.common.util.FileSettingsProvider;
import goryachev.common.util.GlobalSettings;
import goryachev.fx.CssLoader;
import goryachev.log.config.JsonLogConfig;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.stage.Stage;

public class HexViewerApp
extends Application
{

	static MainWindow mw;
	
public static void main(String[] args)
{
	JsonLogConfig.configure(new File("log-conf.json"), 1000);
	launch(args);
}


public void init() throws Exception
{
	// TODO change to something visible in Documents? platform-specific?
	//File baseDir = new File(System.getProperty("user.home"), ".fqlite");
		
	File logFolder = new File(GUI.baseDir, "logs"); 
	//Log.init(logFolder);
	
	File settingsFile = new File(GUI.baseDir, "settings.conf");
	System.out.println("settingsFile " + settingsFile.getAbsolutePath());	
	FileSettingsProvider p = new FileSettingsProvider(settingsFile);
	GlobalSettings.setProvider(p);
	p.loadQuiet();
	
	
}


public void goTo(int offset){
  mw.goTo(offset);	
}

public void switchModel(String path){
	mw.loadNewHexFile(path);
}

public void loadNewHexFile(String path){
	
	mw.loadNewHexFile(path);
}

public void clearAll(){
	mw.clearAll();
}



public static void setVisible(){
	mw.setIconified(false);
	mw.toFront();
}

public void start(Stage stage) throws Exception
{
	
	// we need to explicitly do a call to init() - I don't know we :-)
	init();
	//new MainWindow().open();

	this.mw = new MainWindow();
	
	try {
		//mw.initStyle(StageStyle.UTILITY);
		mw.open();
		Platform.setImplicitExit(false);
		/* hide() normally call close() and would shoutdown the complete app.
		 * We have to consume the close-event. Instead of closing we just
		 * iconify the hexview. With setIconified(false) the hexview can be
		 * made visible again. 
		 */
		mw.setOnCloseRequest(e ->{ e.consume(); mw.setIconified(true);});
		mw.setIconified(true);
		
		
	}catch(Exception err) {
		
		err.printStackTrace();
		
	}

	
	// init styles
	CssLoader.setStyles(() -> new Styles());		
}



}
