# Unreleased – NIST CFTT "SQLite Data Recovery" requirements

-  CSV/HTML export: new columns `_SOURCE` (file the row was read from, SFT-CA-13) and `_CELLS` (file offset and length of every value of a recovered row, SFT-AO-06)
-  deleted records that are older versions of a live record are flagged "↻ old version (UPDATE)" (SFT-AO-04); carved records without rowid are matched by content
-  WAL: row versions are compared across frames and with the database file – "↻ updated in WAL frame n" / "✗ deleted in WAL frame n" (SFT-AO-05)
-  Analyze > Forensic analysis: changed rows (every version of a row across database file, WAL, journal and carved records; carved records without rowid matched by content), "Row history" in the table context menu
-  missing row numbers: rowid gaps and numbers up to sqlite_sequence, cross-checked with the recovered records
-  deleted index entries: carved from freeblocks, unallocated area and freed index pages (reconstructed freeblock entries only when unique), with the state of their row
-  evidence: SHA-256 of database, -wal, -journal, -shm at open, verified on close / exit (warning if changed), "Verify now" and comparison with an expected hash; every export writes <file>.manifest.json (tool, time UTC, evidence and export hashes)
-  Case menu: open a folder as a case (scan for SQLite files with name / size conditions, open the chosen ones one after the other), overview of all open databases, search over all open databases (live, recovered, WAL, journal)
-  Timeline (Analyze / Case): date columns detected by name and value (Unix s/ms/us/ns, Cocoa, WebKit, FILETIME, HFS+, .NET, OLE, ISO text), events of all open databases incl. deleted records and WAL / journal versions, density chart, CSV export
-  bug fix: WAL / journal version analysis read shifted fields when the GUI had already numbered the rows (DB rows flagged wrongly or not at all in the GUI)
-  bug fix: stale copies of live records were kept when another column held the same number as the rowid
-  table view: older versions of a row are highlighted amber, deleted records red; context menu "Go to current version" / "Go to newer version (WAL frame n)" jumps to the current row
-  WAL panel: new "Transactions" tab – frames grouped into transactions per WAL generation, with commit size and state (SFT-AO-08)
-  rollback journal: rows of the journal snapshot are compared with the database – "✗ deleted after journal snapshot (not in database)" / "↻ old version (UPDATE), newer version in database" (SFT-AO-04/05)
-  carved copies of live records whose INTEGER PRIMARY KEY was lost (b-tree page splits) are recognised as stale copies and no longer reported as deleted
-  bug fix: deleted records of tables with an INTEGER PRIMARY KEY were misparsed ("header≠payload", columns shifted) when the freeblock header covered only payload length + rowid + header length (e.g. rowid >= 128); the reconstructed payload length was also too short and not varint-encoded
-  bug fix: the last frame of a WAL file was never analysed
-  bug fix: records on WAL/journal pages beyond the end of the database file were lost (rowid not read)
-  header: journal mode label corrected (bytes 18/19 only distinguish legacy/WAL; 0 is invalid)
-  WAL/journal analyzer no longer calls System.exit() when a file is missing

# FQLite 5.1.1 

-  icon are now mainly .svg based
-  Many different improvements regarding robustness of the import process 
-  serveral anti-forensic measures are now fixed
-  bugfixing of WAL-import files (only one block was read)
-  overflow-handling was improved
-  b-tree analysis was reworked, fixed some smaller bugs
