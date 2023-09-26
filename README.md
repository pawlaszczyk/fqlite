
<p align="center">
  <img width="298" height="123" src="fqlite_logo_256.png?raw=true" alt="FQLite Logo"/>
</p>

### FQLite - Forensic SQLite Data Recovery Tool

FQLite is a tool to find and restore deleted records in SQLite databases. It therefore examines the database for entries marked as deleted. Those entries can be recovered and displayed. It is written in the Java programming language. The program can operate with a simple graphical user interface (GUI mode). The program is able to search a SQLite database file for regular as well as deleted records.


<p align="center">
  <img width="95%" height="95%" src="fqlite_screenshot.png?raw=true" alt="FQLite Screenshot"/>
</p>


### Features

FQLite allows you to:
* browse and recover the content of freelist pages
* recover records in all database pages including unallocated space
* support of UTF-8,UTF-16BE,UTF-16LE encoded databases
* support for multi-byte columns as well as overflow pages
* recover dropped tables
* create CSV-format data export
* support for Rollback-Journals and WAL-Archives
* integrated Hex-Viewer
* support a forensically sound investigation of database files

Some features:

* written with Java standard class library
* JavaFX-based graphical user interface
* open-source
* free of charge
* runs out of the box


### Official Project Webpage

Check out the latest binary version (for Intel as well as ARM-based systems) from the official project homepage

https://www.staff.hs-mittweida.de/~pawlaszc/fqlite/                          

### Technical Background

An overview article highlighting the technical background of FQLite can be retrieved from 

https://conceptechint.net/index.php/CFATI/article/view/17/6

D. Pawlaszczyk, C. Hummert: (2021). 
Making the Invisible Visible – Techniques for Recovering Deleted SQLite Data Records. 
International Journal of Cyber Forensics and Advanced Threat Investigations,


### Prerequisites

In the latest version, the FQLite is bundled with a Java Runtime Environment (JRE) and all required libraries.

> **Important note:** With version 2.0 the support for the command line mode was cancelled.


### Installation and  Usage

To run the FQLite in GUI mode the executable can normally be started with a double-click on the run file. FQLite is written in Java. For the convenience of the user, the runtime environment and all necessary libraries are included and shipped with the install file. Just download the version for your system environment and start immediately.

FQLite works well on **MAC OS**. First, download the DMG archive to your computer from this page. Double-click the DMG file to open it, and you'll see a Finder window. Often these will include the application itself, some form of arrow, and a shortcut to the Applications folder. Simply drag the application's icon to your Applications folder and you're done: the software is now installed and ready to use.

FQLite works well on **Windows**. First, download the zip archive to your computer from this page. Unpack the archive file. Now change to the subfolder fqlite. The application can be started by calling the batch file run.bat.

FQLite works well on **Linux***. First, download the tar archive to your computer from this page. Open a command shell and change to the download folder.

Now unpack the tar archive with the following command:
```
$ tar zxvf fqlite.tar.gz 
```
Change to the application folder:
```
$ cd fqlite
```
Then call the shell script to start FQLite:
```
$ ./run.sh
```

### Licence and Author

Author: Dirk Pawlaszczyk <pawlaszc@hs-mittweida.de>

FQLite for SQLite is bi-licensed under the Mozilla Public License Version 2, 
as well as the GNU General Public License Version 3 or later.

You can modify or redistribute it under the conditions of these licenses. 

