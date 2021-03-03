package fqlite.base;

import java.util.concurrent.ExecutionException;

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
* @version 1.5
*/
public class MAIN {

	
	
	public static void main(String[] args) {

		System.out.println("**************************************************************");
		System.out.println("* FQlite - Forensic SQLite Data Recovery Tool                *");
		System.out.println("*                                               version: "+ Global.FQLITE_VERSION +" *");
		System.out.println("* Author: D. Pawlaszczyk                                     *");
		System.out.println("* "+Global.FQLITE_RELEASEDATE+"                                                 *");
		System.out.println("**************************************************************");

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
			job.path = args[0];
			long start = System.currentTimeMillis();

			if (args.length > 1) {
				String option = args[1];

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

		System.out.println("Usage: Job <filename> [options] ");
		System.out.println("(to analyse a sqlite db-file)");
		System.out.println("    ");
		System.out.println("where possible options include: ");
		System.out.println("    ");
		System.out.println("  --wal:<wal-file> ");
		System.out.println("            try to find a companion WAL-file and analyse it");
		System.out.println("  --rjournal::<journal-file> ");
		System.out.println("            try to find a companion rollback journal-file and analyse it");
		System.out.println(" ");
	}

}
