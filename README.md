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
* analyzing BLOB formats like Google protobuf, AVRO, Apple plist, Thrift,... 
* transparent decompression of BLOBs: gzip, zlib, zstd, LZ4 and Apple LZFSE/LZVN (also nested, no native libraries)
* decoding of Apple typedstream (e.g. iMessage `attributedBody`) and NSKeyedArchiver archives
* fields of plist, protobuf, JSON and typedstream values can be shown as additional table columns
* row history: every version of a row across database file, WAL, rollback journal and carved records; deleted rows and older versions are highlighted
* missing row numbers (rowid gaps), deleted index entries and SHA-256 evidence hashes with export manifests
* case mode: scan a folder for SQLite files, overview and search across all open databases, timeline of all date columns
* triggers explained in plain English, including forensic notes
* command palette (Ctrl+K / Cmd+K), adjustable limits and font sizes
* integrated SQL-Analyser 
* HTML-export functionality (since. 4.0)
* Displaying database PRAGMA values
* Export recovered data into a new SQLite database 
* Text-to-SQL generation with AI-based Assistant (since 4.0)
* Graphic SQL-Schema analyzer (based on Mermaid.js)
* Export recovered data to CASE/UCO JSON-LD format
* white and black theme support
* support of sqlcipher encrypted databases
* Apple DMG Installer is signed (since 4.22)

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

D. Pawlaszczyk, C. Hummert: (2021). 
Making the Invisible Visible – Techniques for Recovering Deleted SQLite Data Records. 
International Journal of Cyber Forensics and Advanced Threat Investigations,
https://conceptechint.net/index.php/CFATI/article/view/17/6


An overview article highlighting the new AI-Assistant of FQLite can be retrieved from

Dirk Pawlaszczyk, Ronny Bodach, Philipp Engler, Jan Kolouch, Michael Spranger, Christian Hummert, Dirk Labudde,
AI-based automated SQL query generation for SQLite databases in Mobile forensics,
Forensic Science International: Digital Investigation,
Volume 57, 2026, 302100, ISSN 2666-2817,
https://doi.org/10.1016/j.fsidi.2026.302100



### Forensic Analysis and BLOB Decoding

Many apps store their actual content in BLOBs that are compressed, serialized or both
(iMessage `attributedBody`, NSKeyedArchiver plists, protobuf messages, zstd- or
LZFSE-compressed payloads). FQLite now unpacks and decodes these values directly in the
table view:

* **Compressed BLOBs** (gzip, zlib, zstd, LZ4, LZFSE/LZVN) are unpacked automatically, also
  several nested layers. The decoders are written in plain Java; a configurable size limit
  protects against decompression bombs, and damaged streams are decoded up to the error.
* **Apple typedstream** (NSArchiver) and **NSKeyedArchiver** archives are resolved to their
  values (text, attribute runs, dictionaries, arrays, dates, numbers).
* The format badge below the table, the cell tooltip and the BinViewer (double-click on a
  BLOB) show the decoded content, e.g. `ZSTD → BPLIST (KEYED)`.
* **Decode column into fields** (right-click on a column): FQLite samples the column, lists
  all fields found in plist / NSKeyedArchiver / typedstream / protobuf / JSON values with
  frequency and an example, and adds the selected fields as additional columns
  (marked with ▸). These columns exist in the view only – the database is never changed.

<p align="center">
  <img width="95%" src="img/fqlite_decoded_columns.png?raw=true" alt="Table with decoded columns: text of an iMessage attributedBody, fields of a compressed NSKeyedArchiver payload, JSON and protobuf fields"/>
</p>

<details>
  <summary>Decode column dialog</summary>
  <img width="80%" src="img/fqlite_decode_dialog.png?raw=true" alt="Dialog listing the fields found in a column with frequency and example values"/>
</details>

**Triggers explained** (Analyze menu): every trigger of the open databases – including
deleted triggers recovered from the schema – is described in plain English, together with
forensic notes, e.g. that rows are deleted automatically, that old values are copied into a
history table, or that changes are blocked. The list can be exported as CSV.

<p align="center">
  <img width="85%" src="img/fqlite_triggers.png?raw=true" alt="Triggers explained: trigger list with plain-English explanation and forensic notes"/>
</p>

Further additions:

* **Forensic analysis** (Analyze menu): changed rows with their complete row history across
  database file, WAL, rollback journal and carved records, missing row numbers, deleted index
  entries and evidence hashes (SHA-256 of database, `-wal`, `-journal`, `-shm`; verified on
  close). Every export writes a `.manifest.json` with the hashes of the evidence and the export.
* **Case menu**: open a folder as a case, overview of all open databases, search across all of
  them (live, recovered, WAL, journal) and a combined **timeline** of all date columns.
* **Command palette**: press **Ctrl+K** (macOS: also Cmd+K) to search all menu commands and all
  tables of the open databases.
* **Settings → Display & limits**: font size of the user interface and of hex dumps, size of the
  hex preview, BLOB decoding limits, sample size for field decoding and the maximum number of
  listed missing rows and search hits.

See the [changelog](CHANGELOG.md) for details.

### NodeQL Implementation

<p align="center">
  <img width="95%" src="img/nodeql-current-editor.png?raw=true" alt="NodeQL editor with connected blocks, SQL preview and inline query results"/>
</p>

Version **5.1** updates the visual SQLite editor to follow the current
[NodeQL](https://github.com/Kartoffelspalt/NodeQL) block shapes, colors and workflow.
This is a JavaFX integration, not the standalone Flutter application.

1. Open a SQLite database in FQLite and click the **NodeQL graph icon in the toolbar**.
   There is no separate NodeQL entry in the top menu bar.
2. Search the palette, snap statements below `EXECUTE QUERY` and insert oval
   reporters into input slots. Use structured conditions, sorting, aliases,
   SQLite functions, `DISTINCT` and `LIMIT`/`OFFSET` to compose your query.
   Every table input offers existing tables and views through **▾**; type to
   filter suggestions, or keep any custom SQL input. Suggested identifiers are
   quoted automatically, including names containing spaces or quotes.
   Column inputs provide the same suggestions for the selected table(s), including
   aliases and qualified JOIN columns. In SELECT lists, type a comma to add another
   suggested column; existing projections and custom SQL expressions are retained.
   Or click **☑** to select multiple columns with checkboxes and apply them together.
   Existing selections are prechecked, filtering keeps checked items, and Cancel
   leaves the input unchanged. SELECT, COLUMN reporters, GROUP BY and function
   argument lists support this picker; free SQL input is still available.
3. Click **Run Query** or the yellow start block. SQL and results stay in the
   NodeQL editor; **Expand results** opens a larger result view. Opening the SQL
   Analyzer is optional.

NodeQL workspaces are session-only: nodes are **not saved or restored with
FQLite projects**. Undo is available inside the editor.

The **Light/Dark switch** at the right of the toolbar uses FQLite's shared theme
setting and button style. Switching in either window updates the main app, all
open NodeQL builders and expanded results. There is no separate NodeQL theme
preference; like the main app, the selection applies to the current session.

Dragging nodes does not pan or recenter the workspace. Use the empty canvas with
the right/middle mouse button to pan; zoom preserves the current view. Clicking
the palette's `EXECUTE QUERY` card explicitly returns to the query chain.

<details>
  <summary>Light theme preview</summary>
  <img width="95%" src="img/nodeql-light-editor.png?raw=true" alt="NodeQL for FQLite using the main application's light theme"/>
</details>

<details>
  <summary>Column multi-selection preview</summary>
  <img src="img/nodeql-column-multiselect.png?raw=true" alt="Searchable column checklist with id and title selected, Apply and Cancel"/>
</details>

See the [integration and usage guide](NODEQL_INTEGRATION_GUIDE.md),
[verification notes](design-qa.md) and [5.1 changelog](CHANGELOG.md).
The upstream FQLite PDF user manual is unchanged; this guide documents the
NodeQL-specific workflow.
    
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
