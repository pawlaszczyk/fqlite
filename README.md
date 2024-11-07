<p align="center">
  <img width="298" height="123" src="img/fqlite_logo_256.png?raw=true" alt="FQLite Logo"/>
</p>

### FQLite - Forensic SQLite Data Recovery Tool

FQLite is a tool to find and restore deleted records in SQLite databases. It therefore examines the database for entries marked as deleted. Those entries can be recovered and displayed. It is written in the Java programming language. The program can operate with a simple graphical user interface (GUI mode). The program is able to search a SQLite database file for regular as well as deleted records.


<p align="center">
  <img width="95%" height="95%" src="img/fqlite_screenshot.png?raw=true" alt="FQLite Screenshot"/>
</p>


### Features

FQLite allows you to:
* browse and recover the content of freelist pages
* recover records in all database pages including unallocated space and free blocks!
* support of UTF-8,UTF-16BE,UTF-16LE encoded databases
* support for multi-byte columns as well as overflow pages
* recover dropped tables
* create CSV/TSV-format data export
* support for Rollback-Journals and WAL-Archives
* integrated Hex-Viewer
* support a forensically sound investigation of database files
* support for decoding of bplist, protobuffer and BASE64 encoded cell values
* automatic detection of different BLOB types like .png, .bmp, .gif, .jpeg, .tiff, .heic, .pdf
* analyzing BLOB formats like google protobuffer, AVRO, Apple plist, Thrift,... 
* integrated SQL-Analyzer 
* Displaying database PRAGMA values

Some features:

* written with Java standard class library
* JavaFX-based graphical user interface
* open-source
* free of charge
* runs out of the box

<p align="center">
  <img width="95%" height="95%" src="img/fqlite_sqlanalyzer.png?raw=true" alt="SQL Analyzer"/>
</p>


### Official Project Webpage

Check out the the official project homepage

https://www.staff.hs-mittweida.de/~pawlaszc/fqlite/                          

### Official User guide

You can find the current version of the user manual under the following link: 

https://github.com/pawlaszczyk/fqlite/blob/master/resources/FQLite_UserGuide.pdf


### Technical Background

An overview article highlighting the technical background of FQLite can be retrieved from 

https://conceptechint.net/index.php/CFATI/article/view/17/6

D. Pawlaszczyk, C. Hummert: (2021). 
Making the Invisible Visible – Techniques for Recovering Deleted SQLite Data Records. 
International Journal of Cyber Forensics and Advanced Threat Investigations,


### Prerequisites

In the latest version, the FQLite is bundled with a Java Runtime Environment (JRE) and all required libraries.

> **Important note:** With version 2.0 the support for the command line mode was cancelled.



# Installation
## macOS
### Installation via .dmg File

1. Download the latest version of FQLite in .dmg format from the Release page.
2. Open the .dmg file and drag the application into the "Applications" folder.
3. You can now launch FQLite from the Applications folder.

## Important node: 
If you try to open an app by an unknown developer and you see a warning dialog on your Mac.
A dialog is displayed saying that the app is damaged. In fact, the app is simply not signed 
with a developer certificate. For this reason, Gatekeeper refuses to execute. 
The first method will allow a single program to run, without having to disable Gatekeeper. 
Open a terminal and run the following command:

```bash
sudo  xattr -dr com.apple.quarantine /Applications/fqlite.app
```
The app should then start without any further complaints. 

### Installation via Homebrew

1. Open the Terminal.
2. Install FQLite with the following command:

```bash
brew install --cask bocian67/fqlite/fqlite
```

3. After installation, FQLite can be launched directly from the Applications folder.

## Windows
### Installation via .exe File

1. Download the latest version of FQLite in .exe format from the Release page.
2. Run the .exe file and follow the installation instructions.
3. After installation, FQLite can be opened from the Start menu.

### Installation via Chocolatey

1. Download the .nupkg file for FQLite from the Release page.
2. Open Command Prompt or PowerShell with administrator privileges.
3. Install FQLite using the following command:

```bash
choco install fqlite --source ./fqlite.nupkg
```

4. After installation, FQLite can be opened from the Start menu.

## Linux
### Installation via .deb File
1. Download the latest version of FQLite in .deb format from the Release page.
2. Open a terminal and navigate to the directory where the .deb file is saved.
3. Install FQLite with the following command:
```bash
sudo apt install ./fqlite.deb
```

4. After installation, FQLite can be launched from the application menu.
