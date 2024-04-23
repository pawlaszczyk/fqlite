 package fqlite.base;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

/**
* This is the main class, as the name says :-).
* Use this class to start analyzing a database file from the command line interface
* (CLI-mode). For the graphic mode please visit class <code>GUI</code>. 
* 
* To run the FQLite from the command line you can use the following command:
*
* $> java fqlite.base.MAIN <database.db>
*
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
* @version 1.5.6
*/
public class MAIN {

   
	
	
	public static void main(String[] args) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {

		System.out.println("**************************************************************");
		System.out.println("* FQlite - Forensic SQLite Data Recovery Tool                *");
		System.out.println("*                                              version: "+ Global.FQLITE_VERSION +" *");
		System.out.println("* Author: D. Pawlaszczyk                                     *");
		System.out.println("* "+Global.FQLITE_RELEASEDATE+"                                                 *");
		System.out.println("**************************************************************");

		
		System.out.println("       *                         ");
		System.out.println("      /|\\                       ");       
		System.out.println("     /*|O\\                      ");
		System.out.println("    /*/|\\*\\                    ");
		System.out.println("   /X/O|*\\X\\                   ");
		System.out.println("  /*/X/|\\X\\*\\                 ");
	    System.out.println(" /O/*/X|*\\O\\X\\                ");             
	   	System.out.println("/*/O/X/|\\X\\O\\*\\              ");
	   	System.out.println("      |X|                        ");      
	   	System.out.println("      |X|     <Christmas Edition>");
	   	System.out.println(" \n\n\n                          ");
	 	  
		/* create a new job-object to process database file */
		/* Note: There is a 1:1 connection between a database file and a job object */
		Job job = new Job();

		/*
		 * Missing arguments ? We need at least the path and the name of the database
		 * file to process
		 */
		if (args.length == 0) {
			printOptions();
		} else {
			job.path = args[args.length-1];
			long start = System.currentTimeMillis();

			if (args.length > 1) {
				
				for (int i = 0; i < args.length; i++)
				{	
					String option = args[i];
	
					/* check parameters */
					/*
					 * Note: you can also process WAl-archive files as well as rollback-Journal
					 * files with this class. Check out the correct parameters.
					 */
					if (option.contains("--wal:")) {
						job.readWAL = true;
						job.walpath = option.substring(6);
						System.out.println("wal-filename: " + job.walpath);
					} else if (option.contains("--rjournal:")) {
						job.readRollbackJournal = true;
						job.rollbackjournalpath = option.substring(11);
						System.out.println("rollbackjournal-filename: " + job.rollbackjournalpath);
					} 
					
					if (option.contains("--threads:"))
					{
						
						try
						{
							Global.numberofThreads = Integer.parseInt(option.substring(10));
							System.out.println("number of threads: " + Global.numberofThreads);

						}
						catch(NumberFormatException err)
						{
							System.out.println(" wrong parameter: " + option.substring(10));
						}
						
					}
					if (option.contains("--loglevel:"))
					{
					    String loglv = option.substring(11);
						
					    switch(loglv){
					    
						    case "ERROR" :  Global.LOGLEVEL = Level.SEVERE; 
						    				System.out.println("Loglevel was set to ERROR");
						    			    break;  
						    	
						    case "INFO" :   Global.LOGLEVEL = Level.INFO; 
						    				System.out.println("Loglevel was set to INFO");
						    				break;  
						    	
						    case "DEBUG" :  Global.LOGLEVEL = Level.FINEST; 
						    				System.out.println("Loglevel was set to DEBUG");
						    				break;  
						    
						    case "ALL" :  	Global.LOGLEVEL = Level.ALL;
						    				System.out.println("Loglevel was set to ALL");
						    				break;  
						    				
						    default: Global.LOGLEVEL = Level.INFO;
					    } 
						
					}
					
					
				}
				

			}

			try {
				/* start processing the db-file */
				job.processDB();

			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}

			long end = System.currentTimeMillis();
			System.out.println("Duration in ms: " + (end - start));
		}
	}

	protected static void printOptions() {

		System.out.println("    ");
		System.out.println("Usage: [mode] [options] <filename>");
		System.out.println("(to analyse a sqlite db-file)");
		System.out.println("    ");
		System.out.println("where mode could be one of the following: gui|nogui|cli ");
		System.out.println("    ");
		System.out.println(" \"gui\" or leave just blank");
		System.out.println("            start program in GUI mode");
		System.out.println(" \"nogui\" or \"cli\" ");
		System.out.println("            start program frome the command line without graphic frontend");		
		System.out.println("    ");
		System.out.println("where possible options include: ");
		System.out.println("    ");
		System.out.println("  --wal:<wal-file> ");
		System.out.println("            try to find a companion WAL-file and analyse it");
		System.out.println("  --rjournal:<journal-file> ");
		System.out.println("            try to find a companion rollback journal-file and analyse it");
		System.out.println("  --threads:<number of threads>");
		System.out.println("            start concurrent processing with x threads (only for large files)");
		System.out.println("  --loglevel:<ERROR|INFO|DEBUG|>");
		System.out.println("            logmessage details");
		System.out.println(" ");
		System.out.println("Example:");
		System.out.println("    ");
		System.out.println("  java jar fqlite_<version>.jar nogui --threads:4 --loglevel:ERROR foo.db ");
		System.out.println("  	    	start the program in command line mode ");
		System.out.println("    		use 4 threads to analyze the data records");
		System.out.println("    		print only ERROR messages to standard output");
		System.out.println("    		the name of the database file is <foo.db>");
		System.out.println("    ");		
		
		
	}

}
