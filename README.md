                            

    __________    __    _ __     
   / ____/ __ \  / /   (_) /____ 
  / /_  / / / / / /   / / __/ _ \
 / __/ / /_/ / / /___/ / /_/  __/
/_/    \___\_\/_____/_/\__/\___/ 
                                 


# FQLite

FQLite is a tool to find and restore deleted records in sqlite databases. It therefore examines the database for entries marked as deleted. Those entries can be recovered and displayed. It is written with the Java programming language. The program can operate in two different modes. It can be started from the command line (CLI mode). A simple graphical user interface is also supported (GUI mode).  

The program is able to search a SQLite database file for regular as well as deleted records.

# Technical Background

To be done.

# Prerequisites

To run the tool you need at least a Java Runtime Environment 1.8 or higher.


# Example Usage

To run the *FQLite* in GUI mode the executable jar can normally be started with a double-click on the jar-archive file. If this does not work, since *javaw* is not linked correctly to *.jar* files, you can use the command line as well:

  $>java -jar fqlite.jar 


To run the *FQLite* from the command line you can use the following command:

  $>java -cp fqlite.jar fqlite.hsmw.de.RT <database.db>


# Licence and Author

Author: Dirk Pawlaszczyk <pawlaszc@hs-mittweida.de>

MOZRT for SQLite is bi-licensed under the Mozilla Public License Version 2, 
as well as the GNU General Public License Version 3 or later.

You can modify or redistribute it under the conditions of these licenses. 

