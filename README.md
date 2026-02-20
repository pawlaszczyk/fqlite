<p align="center">
  <img width="298" height="123" src="img/fqlite_logo_256.png?raw=true" alt="FQLite Logo"/>
</p>

### FQLite - Forensic SQLite Data Recovery Tool

FQLite is a tool to find and restore deleted records in SQLite databases. It therefore examines the database for entries marked as deleted. Those entries can be recovered and displayed. It is written in the Java programming language. The program can operate with a simple graphical user interface (GUI mode). The program is able to search a SQLite database file for regular as well as deleted records.


<p align="center">
  <img width="95%" height="95%" src="img/fqlite_mainscreen.jpg?raw=true" alt="FQLite Screenshot"/>
</p>


### Features

FQLite allows you to:
* browse and recover the content of freelist pages
* recover records in all database pages, including unallocated space and free blocks!
* support of UTF-8,UTF-16BE,UTF-16LE encoded databases
* support for multibyte columns as well as overflow pages
* recover dropped tables
* create CSV/TSV-format data export
* support for Rollback-Journals and WAL-Archives
* integrated Hex-Viewer
* support a forensically sound investigation of database files
* support for decoding of bplist, protobuf and BASE64 encoded cell values
* automatic detection of different BLOB types like .png, .bmp, .gif, .jpeg, .tiff, .heic, .pdf
* analysing BLOB formats like Google protobuf, AVRO, Apple plist, Thrift,... 
* integrated SQL-Analyser 
* HTML-export functionality (since. 4.0)
* Displaying database PRAGMA values
* Export recovered data into a new SQLite database 
* Text-to-SQL generation with AI-based Assistant (since 4.0)
* Graphic SQL-Schema analyser (based on Mermaid.js)

Some features:

* written with Java standard class library
* JavaFX-based graphical user interface
* open-source
* free of charge
* runs out of the box
* multi-platform support

<p align="center">
  <img width="95%" height="95%" src="img/fqlite_sqlanalyzer.png?raw=true" alt="SQL Analyzer"/>
</p>

<p align="center">
  <img width="80%" height="80%" src="img/fqlite_assistant.png?raw=true" alt="SQL Analyzer"/>
</p>


### Official Project Webpage

Check out the official project homepage

https://www.staff.hs-mittweida.de/~pawlaszc/fqlite/                          

### Official User Guide

You can find the current version of the user manual under the following link: 
http://dx.doi.org/10.13140/RG.2.2.20276.92800

### Technical Background

An overview article highlighting the technical background of FQLite can be retrieved from 

https://conceptechint.net/index.php/CFATI/article/view/17/6

D. Pawlaszczyk, C. Hummert: (2021). 
Making the Invisible Visible – Techniques for Recovering Deleted SQLite Data Records. 
International Journal of Cyber Forensics and Advanced Threat Investigations,


### Prerequisites

In the latest version, the FQLite is bundled with a Java Runtime Environment (JRE) and all required libraries.

> **Important note:** With version 2.0, the support for the command line mode was cancelled.



# Installation
## macOS
### Installation via .dmg File

1. Download the latest version of FQLite in .dmg format from the Release page.
2. Open the .dmg file and drag the application into the "Applications" folder.
3. You can now launch FQLite from the Applications folder.

## Important node: 
If you try to open an app by an unknown developer, and you see a warning dialogue on your Mac.
A dialogue is displayed saying that the app is damaged. In fact, the app is simply not signed 
with a developer certificate. For this reason, Gatekeeper refuses to execute. 
The first method will allow a single program to run without having to disable Gatekeeper. 
Open a terminal and run the following command:

```bash
sudo xattr -dr com.apple.quarantine /Applications/fqlite.app
```
The app should then start without any further complaints. 

3. After installation, FQLite can be launched directly from the Applications folder.

## Windows
### Installation via .exe File

1. Download the latest version of FQLite in .exe format from the Release page.
2. Run the .exe file and follow the installation instructions.
3. After installation, FQLite can be opened from the Start menu.


## Linux
### Installation via .deb File
1. Download the latest version of FQLite in .deb format from the Release page.
2. Open a terminal and navigate to the directory where the .deb file is saved.
3. Install FQLite with the following command:
```bash
sudo apt install ./fqlite.deb
```

4. After installation, FQLite can be launched from the application menu.

## Using AI within FQLite
To obtain AI support for formulating SQL statements, 
you must first download an LLM model. There are 4 models available, 
which differ in size and performance:

1. **GGUF Q4_K_M: ~2.3 GB:**
4-bit quantized model with K-Mean optimization. The smallest and fastest variant, ideal for systems with limited RAM or older hardware. Inference speed is highest among all variants. Output quality is slightly reduced compared to higher quantizations, but remains well-suited for most SQL generation tasks. Recommended for quick prototyping or resource-constrained environments.
https://huggingface.co/pawlaszc/DigitalForensicsText2SQLite/blob/main/forensic-sqlite-llama-3.2-3b-Q4_K_M.gguf

2. **GGUF Q5_K_M: ~2.8 GB:**
5-bit quantized model with K-Mean optimization. A well-balanced middle ground between size, speed, and output quality. Offers noticeably better SQL accuracy than Q4_K_M while still being significantly smaller than the full-precision model. Recommended as the default choice for most users running inference on consumer hardware (8–16 GB RAM).
https://huggingface.co/pawlaszc/DigitalForensicsText2SQLite/blob/main/forensic-sqlite-llama-3.2-3b-Q5_K_M.gguf

3. **GGUF Q8_0: ~3.8 GB:**
8-bit quantized model. Near full-precision quality with only minimal accuracy loss. Inference is slower than Q4/Q5 variants but the output quality is very close to the FP16 baseline. A good choice when output accuracy is a priority and sufficient RAM (16+ GB) is available. Suitable for production use cases where result correctness is critical.
https://huggingface.co/pawlaszc/DigitalForensicsText2SQLite/blob/main/forensic-sqlite-llama-3.2-3b-Q8_0.gguf

4. **Full (FP16): ~6 GB:**
Full 16-bit floating point model without any quantization. Delivers the highest possible output quality and serves as the reference baseline for all other variants. Requires the most memory (16–24 GB RAM recommended) and has the slowest inference speed. Recommended for benchmarking, fine-tuning experiments, or high-end systems where maximum accuracy is required.
https://huggingface.co/pawlaszc/DigitalForensicsText2SQLite/blob/main/forensic-sqlite-llama-3.2-3b-fp16.gguf

If you are unsure, we recommend choosing the smallest model. This should run on all common computer systems without significant delays. 
A GPU support is not necessary to run this model. 

To use the Text2SQL function, you must click on the brain icon:

  <img width="24" height="24" src="img/llm_logo.png?raw=true" alt="LLM Model Logo"/>

The first time you use this feature, you will be prompted to specify the path to the LLM: 

<p align="center">
  <img width="65%" height="65%" src="img/llm_config.png?raw=true" alt="LLM Model Configuration"/>
</p>

After accepting the change, the Text2SQL function can then be used in FQLite.
