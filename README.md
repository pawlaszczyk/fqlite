<h1>FQLite - Forensic SQLite Data Recovery Tool<h1>

# Official Project Webpage

Check out the latest binary version (as a runnable jar-Archive) from the official project homepage

https://www.staff.hs-mittweida.de/~pawlaszc/fqlite/                          


# FQLite

FQLite is a tool to find and restore deleted records in sqlite databases. It therefore examines the database for entries marked as deleted. Those entries can be recovered and displayed. It is written with the Java programming language. The program can operate in two different modes. It can be started from the command line (CLI mode). A simple graphical user interface is also supported (GUI mode).  

The program is able to search a SQLite database file for regular as well as deleted records.

# Technical Background

On overview article highlighting the technical background of FQLite can be retrieved from 

https://conceptechint.net/index.php/CFATI/article/view/17/6

D. Pawlaszczyk, C. Hummert: (2021). 
Making the Invisible Visible – Techniques for Recovering Deleted SQLite Data Records. 
International Journal of Cyber Forensics and Advanced Threat Investigations,


# Prerequisites

To run the tool you need at least a Java Runtime Environment 1.8 or higher.


# Example Usage

To run the *FQLite* in GUI mode the executable jar can normally be started with a double-click on the jar-archive file. If this does not work, since *javaw* is not linked correctly to *.jar* files, you can use the command line as well:

  $>java -jar fqlite<versionnumber>.jar 


To run the *FQLite* from the command line you can use the following command:

  $>java -cp fqlite<versionnumber>.jar fqlite.base.MAIN <database.db>


# Licence and Author

Author: Dirk Pawlaszczyk <pawlaszc@hs-mittweida.de>

FQLite for SQLite is bi-licensed under the Mozilla Public License Version 2, 
as well as the GNU General Public License Version 3 or later.

You can modify or redistribute it under the conditions of these licenses. 

