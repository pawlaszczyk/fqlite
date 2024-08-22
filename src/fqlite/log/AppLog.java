package fqlite.log;

import java.nio.file.FileSystems;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import fqlite.base.GUI;
import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.control.TextArea;
import javafx.scene.text.Font;
import javafx.stage.Stage;

/**
 * This is the central Logger class for FQLite.
 * 
 * We using <code>java.util.logging.Logger</code> functionality 
 * to enable a rotating logging. 
 * 
 * A total of 3 log files are kept in the <user directory>/.fqlite folder. 
 * The maximum size of each individual log file is 4 MB.  
 * 
 * A text area is also updated with the log messages so that 
 * the last entries in the log file can be viewed via the interface 
 * if required. 
 * 
 * @author pawlaszc
 */
public class AppLog extends Application {

    public static final Logger LOGGER = Logger.getLogger(AppLog.class.getName());
    public static TextArea textArea;
    public static Scene myScene;
    
    static {
    	
        LOGGER.setUseParentHandlers(false);
        try { 
        	
        	textArea = new TextArea();
          	textArea.setEditable(false);
            textArea.setFont(Font.font("Monospaced", 13));
            
            // This block configures the logger with handler and formatter  
			FileHandler fh = new FileHandler(GUI.baseDir.getAbsolutePath() + FileSystems.getDefault().getSeparator() + "fqlite.log",4096*1000,3,true);  
            LOGGER.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();  
            fh.setFormatter(formatter);   
            LOGGER.addHandler(new TextAreaHandler(textArea));
            LOGGER.severe("<FQLite message log>");
            myScene = new Scene(textArea, 600, 400);
            
        } catch (Exception e) {  
            e.printStackTrace();  
        } 
    }

    public static void debug(String log){
    	LOGGER.finest(log);
    }
    
    public static void info(String log){
    	LOGGER.info(log);
    }
   
    public static void warning(String log){
    	LOGGER.warning(log);
    }
    
    public static void error(String log){
    	LOGGER.severe(log);
    }
    
    public static void setLevel(Level newLevel){
    	LOGGER.setLevel(newLevel);
    }
    
    @Override
    public void start(Stage primaryStage) {
       
    	primaryStage.setTitle("FQLite - Log");
        primaryStage.setScene(myScene);
        primaryStage.show();

        
    }
}
