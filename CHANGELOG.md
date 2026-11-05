# Unreleased – NIST CFTT "SQLite Data Recovery" requirements

-  CSV/HTML export: new columns `_SOURCE` (file the row was read from, SFT-CA-13) and `_CELLS` (file offset and length of every value of a recovered row, SFT-AO-06)
-  deleted records that are older versions of a live record are flagged "↻ old version (UPDATE)" (SFT-AO-04); carved records without rowid are matched by content
-  WAL: row versions are compared across frames and with the database file – "↻ updated in WAL frame n" / "✗ deleted in WAL frame n" (SFT-AO-05)
-  WAL panel: new "Transactions" tab – frames grouped into transactions per WAL generation, with commit size and state (SFT-AO-08)
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
