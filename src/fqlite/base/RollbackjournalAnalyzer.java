package fqlite.base;


import java.io.*;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
/**
 * SQLite Rollback Journal Analyzer — fully JDBC-free, pure binary parsing.
 *
 * Reads a SQLite rollback journal (.db-journal) and maps every page record
 * back to its owning table or index by traversing the B-Trees of the
 * accompanying database file.
 *
 * Important: the journal stores the *original* (pre-transaction) copies of
 * pages — i.e. the data that would be restored on a rollback. The current
 * (post-transaction) state lives in the .db file.
 *
 * Rollback journal format
 * ───────────────────────
 * Journal header (first sector, usually 512 bytes):
 *   Offset  0 :  Magic         8 bytes   0xD9D505F920A163D7
 *                              NOTE: zeroed while a transaction is in progress
 *                              (hot journal). SQLite writes the real magic only
 *                              on commit so that a crash leaves a detectable hot
 *                              journal on disk.
 *   Offset  8 :  Page count    4 bytes   number of records (-1 / 0xFFFFFFFF = unknown)
 *   Offset 12 :  Nonce         4 bytes   random value used in checksum
 *   Offset 16 :  DB page count 4 bytes   total pages in DB at journal creation
 *   Offset 20 :  Sector size   4 bytes   (0 → default 512)
 *   Offset 24 :  Page size     4 bytes   (0 → read from DB header)
 *   Offset 28+:  padding to sector boundary
 *
 * Page record (repeating after the header):
 *   Offset  0            :  Page number  4 bytes  big-endian (0 = end of segment)
 *   Offset  4            :  Page data    <pageSize> bytes   original content
 *   Offset  4 + pageSize :  Checksum     4 bytes
 *
 * A journal may contain multiple transaction segments separated by sector-aligned
 * headers. Each completed segment has its magic written; a trailing hot segment
 * may have zeroed magic.
 *
 * Usage:
 *   java RollbackJournalAnalyzer <database.db> <database.db-journal>
 *
 * No external dependencies — pure Java 17+.
 */
public class RollbackjournalAnalyzer {

    static final byte[] JOURNAL_MAGIC = {
            (byte)0xD9, (byte)0xD5, (byte)0x05, (byte)0xF9,
            (byte)0x20, (byte)0xA1, (byte)0x63, (byte)0xD7
    };
    static final byte[] ZERO_MAGIC = new byte[8]; // hot journal

    static final int PAGE_INTERIOR_INDEX = 0x02;
    static final int PAGE_INTERIOR_TABLE = 0x05;
    static final int PAGE_LEAF_INDEX     = 0x0A;
    static final int PAGE_LEAF_TABLE     = 0x0D;

    public record SchemaEntry(String type, String name, long rootPage) {}

    public record PageRecord(
            int     segmentIndex,
            int     recordIndex,
            long    recordOffset,   // byte offset of this record's start (page-number field) in the journal file
            long    pageNumber,
            long    checksum,
            boolean checksumValid,
            int     pageTypeByte,
            String  pageTypeName,
            String  ownerName,
            String  ownerType
    ) {}

    @FunctionalInterface interface PageSource { byte[] get(long pageNo) throws IOException; }


    public static void analyzeJournal(RollbackJournalReader reader) throws IOException {

        Path dbPath      = Path.of(reader.job.path);
        Path journalPath = Path.of(reader.path);

        if (!Files.exists(dbPath))      { System.err.println("Database file not found: " + dbPath);      System.exit(1); }
        if (!Files.exists(journalPath)) { System.err.println("Journal file not found:  " + journalPath); System.exit(1); }

        sep(); System.out.println("  SQLite Rollback Journal Analyzer  (JDBC-free, pure binary)");
        sep(); System.out.println("  DB     : " + dbPath.toAbsolutePath());
        System.out.println("  Journal: " + journalPath.toAbsolutePath()); sep();

        byte[] journal = Files.readAllBytes(journalPath);
        if (journal.length < 28) throw new IOException("Journal file too small.");

        // 1. Read page size — prefer journal header, fall back to DB header
        int pageSize = readJournalPageSize(journal, dbPath);
        System.out.printf("  Page size: %d bytes%n%n", pageSize);

        // 2. Parse all transaction segments
        List<Segment> segments = parseSegments(journal, pageSize);

        // 3. Page source: the DB file (holds current schema)
        PageSource dbSrc = (pageNo) -> readPageFromFile(dbPath, pageNo, pageSize);

        // 4. Load schema from DB (pure binary B-Tree traversal, no JDBC)
        Map<Long, SchemaEntry> schema = readSchema(dbSrc, pageSize);

        if (schema.isEmpty()) {
            System.out.println("    (none found)");
        } else {
            schema.forEach((root, e) ->
                    System.out.printf("    Root page %4d  %-8s  %s%n", root, e.type(), e.name()));
        }
        System.out.println();

        // 5. Build page-to-owner map by fully traversing every B-Tree
        Map<Long, SchemaEntry> pageOwner = buildPageOwnerMap(schema, dbSrc, pageSize);
        pageOwner.put(1L, new SchemaEntry("system", "sqlite_schema", 1L));

        // 6. Attribute each journal record and print results
        List<PageRecord> records = attributeRecords(segments, journal, pageSize, pageOwner);
        reader.records = records;
        reader.pageOwner = pageOwner;
        //printResults(records, segments);
    }


    // =========================================================================
    // MAIN
    // =========================================================================
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java RollbackJournalAnalyzer <database.db> <database.db-journal>");
            System.exit(1);
        }

        Path dbPath      = Path.of(args[0]);
        Path journalPath = Path.of(args[1]);

        if (!Files.exists(dbPath))      { System.err.println("Database file not found: " + dbPath);      System.exit(1); }
        if (!Files.exists(journalPath)) { System.err.println("Journal file not found:  " + journalPath); System.exit(1); }

        sep(); System.out.println("  SQLite Rollback Journal Analyzer  (JDBC-free, pure binary)");
        sep(); System.out.println("  DB     : " + dbPath.toAbsolutePath());
        System.out.println("  Journal: " + journalPath.toAbsolutePath()); sep();

        byte[] journal = Files.readAllBytes(journalPath);
        if (journal.length < 28) throw new IOException("Journal file too small.");

        // 1. Read page size — prefer journal header, fall back to DB header
        int pageSize = readJournalPageSize(journal, dbPath);
        System.out.printf("  Page size: %d bytes%n%n", pageSize);

        // 2. Parse all transaction segments
        List<Segment> segments = parseSegments(journal, pageSize);

        // 3. Collect the latest version of each page from the journal
        //    (later segments override earlier ones for the same page number)
        Map<Long, byte[]> journalPages = extractJournalPages(segments, journal, pageSize);

        // Page sources:
        //   dbSrc       — current DB only (for schema reading)
        //   mergedSrc   — journal-first: journal page if present, otherwise DB page
        //
        // Why two sources?
        //   The schema (sqlite_schema, page 1) is virtually never modified by normal
        //   DML transactions, so reading it from the current DB is correct and gives
        //   us the table names we need.
        //
        //   The page-owner map, however, must reflect the *pre-transaction* state of
        //   every B-Tree — because that is exactly what the journal pages belong to.
        //   If a transaction caused a B-Tree split (new child pages, updated interior
        //   pages), the current DB already shows the post-split layout, whose child
        //   pointers point to pages that did not exist before the transaction. Using
        //   the DB alone would therefore mis-attribute or miss journal pages entirely.
        //   By letting journal pages override DB pages we reconstruct the B-Tree
        //   topology as it was at the moment the journal was written.
        PageSource dbSrc     = (pageNo) -> readPageFromFile(dbPath, pageNo, pageSize);
        PageSource mergedSrc = (pageNo) -> {
            byte[] jp = journalPages.get(pageNo);
            return jp != null ? jp : readPageFromFile(dbPath, pageNo, pageSize);
        };

        // 4. Load schema from DB (table names do not change during DML transactions)
        Map<Long, SchemaEntry> schema = readSchema(dbSrc, pageSize);

        System.out.println("  Schema objects loaded (DB, no JDBC):");
        if (schema.isEmpty()) {
            System.out.println("    (none found)");
        } else {
            schema.forEach((root, e) ->
                    System.out.printf("    Root page %4d  %-8s  %s%n", root, e.type(), e.name()));
        }
        System.out.println();

        // 5. Build page-to-owner map using the merged (journal-first) view so that
        //    the pre-transaction B-Tree topology is used for attribution
        Map<Long, SchemaEntry> pageOwner = buildPageOwnerMap(schema, mergedSrc, pageSize);
        pageOwner.put(1L, new SchemaEntry("system", "sqlite_schema", 1L));

        // 6. Attribute each journal record and print results
        List<PageRecord> records = attributeRecords(segments, journal, pageSize, pageOwner);
        printResults(records, segments);
    }

    // =========================================================================
    // JOURNAL PARSING
    // =========================================================================

    record Segment(
            int      index,
            long     headerOffset,
            boolean  isHot,          // true = magic was zeroed (active/crashed transaction)
            int      sectorSize,
            int      declaredCount,  // -1 = unknown
            long     nonce,
            long     dbPageCount,
            List<Long[]> records     // each: [pageNumber, dataOffset, checksum]
    ) {}

    static int readJournalPageSize(byte[] journal, Path dbPath) throws IOException {
        long raw = u32(journal, 24);
        if (raw > 0) return (raw == 1) ? 65536 : (int) raw;
        // Fall back to DB header bytes 16-17
        byte[] dbHeader = readBytesFromFile(dbPath, 0, 100);
        int dbRaw = u16(dbHeader, 16);
        return (dbRaw == 1) ? 65536 : dbRaw;
    }

    static List<Segment> parseSegments(byte[] journal, int pageSize) {
        List<Segment> segments = new ArrayList<>();

        int sectorSize = (int) u32(journal, 20);
        if (sectorSize < 512 || sectorSize > 65536) sectorSize = 512;

        long offset = 0;
        int  segIdx = 0;

        while (offset + 28 <= journal.length) {
            boolean hasReal = matchesMagic(journal, (int) offset, JOURNAL_MAGIC);
            boolean hasZero = matchesMagic(journal, (int) offset, ZERO_MAGIC);

            // Accept both committed (real magic) and hot (zeroed magic) segments.
            // A zeroed header is only valid at offset 0 or after a committed segment,
            // so we stop if neither magic matches.
            if (!hasReal && !hasZero) break;

            // A segment of all zeros at an arbitrary offset is likely just padding
            if (hasZero && offset > 0 && !hasValidHeader(journal, (int) offset)) break;

            segIdx++;
            boolean isHot      = hasZero;
            int  declared      = (int) u32(journal, (int) offset + 8);
            long nonce         = u32(journal, (int) offset + 12);
            long dbPageCount   = u32(journal, (int) offset + 16);
            int  segSector     = (int) u32(journal, (int) offset + 20);
            if (segSector < 512 || segSector > 65536) segSector = sectorSize;

            long recordsStart  = offset + segSector;
            int  recordSize    = 4 + pageSize + 4;

            // Determine max records: use declared count if valid, otherwise scan
            int maxRecords = (declared > 0 && declared != 0xFFFFFFFF)
                    ? declared
                    : (int)((journal.length - recordsStart) / recordSize);

            List<Long[]> records = new ArrayList<>();
            for (int i = 0; i < maxRecords; i++) {
                long recOffset = recordsStart + (long) i * recordSize;
                if (recOffset + recordSize > journal.length) break;
                long pgNo = u32(journal, (int) recOffset);
                if (pgNo == 0) break; // end-of-segment sentinel
                long cs   = u32(journal, (int)(recOffset + 4 + pageSize));
                records.add(new Long[]{pgNo, recOffset + 4, cs});
            }

            segments.add(new Segment(segIdx, offset, isHot, segSector, declared,
                    nonce, dbPageCount, records));

            // Advance to next sector-aligned position after this segment's records
            long nextOffset = recordsStart + (long) records.size() * recordSize;
            long rem = nextOffset % segSector;
            if (rem != 0) nextOffset += segSector - rem;
            if (nextOffset <= offset) break;
            offset = nextOffset;
        }
        return segments;
    }

    /**
     * Extracts the latest pre-transaction page data for every page referenced in
     * the journal. When a page appears in multiple segments the last segment wins,
     * matching SQLite's own recovery behaviour.
     */
    static Map<Long, byte[]> extractJournalPages(List<Segment> segments,
                                                 byte[] journal, int pageSize) {
        Map<Long, byte[]> pages = new HashMap<>();
        for (Segment seg : segments) {
            for (Long[] rec : seg.records()) {
                long pageNo    = rec[0];
                long dataStart = rec[1]; // already points past the 4-byte page-number field
                byte[] data    = Arrays.copyOfRange(journal,
                        (int) dataStart,
                        (int)(dataStart + pageSize));
                pages.put(pageNo, data);
            }
        }
        return pages;
    }

    static boolean matchesMagic(byte[] data, int offset, byte[] magic) {
        if (offset + magic.length > data.length) return false;
        for (int i = 0; i < magic.length; i++)
            if (data[offset + i] != magic[i]) return false;
        return true;
    }

    /** Sanity-check a potential header: nonce and dbPageCount must be non-zero. */
    static boolean hasValidHeader(byte[] data, int offset) {
        if (offset + 28 > data.length) return false;
        long nonce = u32(data, offset + 12);
        long dbPgs = u32(data, offset + 16);
        return nonce != 0 || dbPgs != 0;
    }

    // =========================================================================
    // RECORD ATTRIBUTION
    // =========================================================================
    static List<PageRecord> attributeRecords(List<Segment> segments, byte[] journal,
                                             int pageSize,
                                             Map<Long, SchemaEntry> pageOwner) {
        List<PageRecord> result = new ArrayList<>();
        for (Segment seg : segments) {
            int recIdx = 0;
            for (Long[] rec : seg.records()) {
                recIdx++;
                long pageNo     = rec[0];
                long dataOffset = rec[1];
                long checksum   = rec[2];

                byte[] pageData = Arrays.copyOfRange(journal,
                        (int) dataOffset,
                        (int)(dataOffset + pageSize));

                // Verify checksum: XOR of each 32-bit word of page data, XOR'd with nonce
                boolean csValid = verifyChecksum(pageData, checksum, seg.nonce());

                int ho       = (pageNo == 1) ? 100 : 0;
                int typeByte = (ho < pageData.length)
                        ? Byte.toUnsignedInt(pageData[ho]) : 0;

                SchemaEntry owner = pageOwner.get(pageNo);
                result.add(new PageRecord(
                        seg.index(), recIdx,
                        dataOffset - 4,   // record starts 4 bytes before the page data (page-number field)
                        pageNo, checksum, csValid,
                        typeByte, pageTypeName(typeByte),
                        owner != null ? owner.name() : "<unattributed>",
                        owner != null ? owner.type() : "—"
                ));
            }
        }
        return result;
    }

    /**
     * SQLite rollback journal checksum:
     *   sum of all 32-bit big-endian words in the page data, added to the nonce,
     *   taken modulo 2^32.
     */
    static boolean verifyChecksum(byte[] pageData, long storedCs, long nonce) {
        long sum = nonce;
        for (int i = 0; i + 3 < pageData.length; i += 4) {
            sum = (sum + u32(pageData, i)) & 0xFFFFFFFFL;
        }
        return sum == storedCs;
    }

    // =========================================================================
    // SCHEMA READING (pure binary, no JDBC)
    // =========================================================================
    static Map<Long, SchemaEntry> readSchema(PageSource src, int pageSize)
            throws IOException {
        byte[] page1 = src.get(1L);
        if (page1 == null || page1.length < 108) return new TreeMap<>();
        int btreeType = Byte.toUnsignedInt(page1[100]);
        if (btreeType != PAGE_LEAF_TABLE && btreeType != PAGE_INTERIOR_TABLE) {
            System.err.printf("  Warning: page 1 has unexpected type 0x%02X%n", btreeType);
            return new TreeMap<>();
        }
        Map<Long, SchemaEntry> schema = new TreeMap<>();
        parseBTreePage(page1, 100, schema, src, pageSize);
        return schema;
    }

    // =========================================================================
    // B-TREE TRAVERSAL
    // =========================================================================
    static Map<Long, SchemaEntry> buildPageOwnerMap(Map<Long, SchemaEntry> schema,
                                                    PageSource src, int pageSize)
            throws IOException {
        Map<Long, SchemaEntry> owner = new HashMap<>();
        // visited is shared across all tables: once a page is attributed to one
        // table it cannot be overwritten by a later table's traversal.
        // Without this, table B could revisit a page already claimed by table A
        // (since each call previously got a fresh empty visited set) and silently
        // replace the correct attribution with a wrong one.
        Set<Long> visited = new HashSet<>();
        for (SchemaEntry entry : schema.values()) {
            collectBTreePages(entry.rootPage(), entry, src, pageSize, owner, visited);
        }
        return owner;
    }

    static void collectBTreePages(long pageNo, SchemaEntry owner, PageSource src,
                                  int pageSize, Map<Long, SchemaEntry> result,
                                  Set<Long> visited) throws IOException {
        if (pageNo <= 0 || !visited.add(pageNo)) return;
        byte[] pageData = src.get(pageNo);
        if (pageData == null || pageData.length == 0) return;
        result.put(pageNo, owner);
        int ho        = (pageNo == 1) ? 100 : 0;
        int type      = Byte.toUnsignedInt(pageData[ho]);
        int cellCount = u16(pageData, ho + 3);
        if (type == PAGE_INTERIOR_TABLE || type == PAGE_INTERIOR_INDEX) {
            collectBTreePages(u32(pageData, ho + 8), owner, src, pageSize, result, visited);
            for (int i = 0; i < cellCount; i++) {
                int cp = u16(pageData, ho + 12 + i * 2);
                if (cp > 0 && cp < pageData.length)
                    collectBTreePages(u32(pageData, cp), owner, src, pageSize, result, visited);
            }
        } else if (type == PAGE_LEAF_TABLE || type == PAGE_LEAF_INDEX) {
            collectOverflowPages(pageData, ho, type, pageSize, owner, src, result, visited);
        }
    }

    static void collectOverflowPages(byte[] pageData, int ho, int type, int pageSize,
                                     SchemaEntry owner, PageSource src,
                                     Map<Long, SchemaEntry> result,
                                     Set<Long> visited) throws IOException {
        int maxLocal = pageSize - 35;
        int cellCount = u16(pageData, ho + 3);
        for (int i = 0; i < cellCount; i++) {
            int cp = u16(pageData, ho + 8 + i * 2);
            if (cp <= 0 || cp >= pageData.length) continue;
            try {
                int pos = cp;
                int[] r1 = readVarint(pageData, pos); pos += r1[1];
                long ps  = Integer.toUnsignedLong(r1[0]);
                if (type == PAGE_LEAF_TABLE) pos += readVarint(pageData, pos)[1];
                if (ps > maxLocal) {
                    int ptrOff = pos + maxLocal;
                    if (ptrOff + 4 <= pageData.length)
                        followOverflowChain(u32(pageData, ptrOff), owner, src,
                                pageSize, result, visited);
                }
            } catch (Exception ignored) {}
        }
    }

    static void followOverflowChain(long pageNo, SchemaEntry owner, PageSource src,
                                    int pageSize, Map<Long, SchemaEntry> result,
                                    Set<Long> visited) throws IOException {
        while (pageNo != 0 && visited.add(pageNo)) {
            result.put(pageNo, owner);
            byte[] p = src.get(pageNo);
            if (p == null || p.length < 4) break;
            pageNo = u32(p, 0);
        }
    }

    // =========================================================================
    // sqlite_schema B-TREE PARSER
    // =========================================================================
    static void parseBTreePage(byte[] pageData, int headerOffset,
                               Map<Long, SchemaEntry> schema,
                               PageSource src, int pageSize) throws IOException {
        int type      = Byte.toUnsignedInt(pageData[headerOffset]);
        int cellCount = u16(pageData, headerOffset + 3);
        if (type == PAGE_INTERIOR_TABLE) {
            long rc = u32(pageData, headerOffset + 8);
            for (int i = 0; i < cellCount; i++) {
                int cp = u16(pageData, headerOffset + 12 + i * 2);
                byte[] child = src.get(u32(pageData, cp));
                if (child != null) parseBTreePage(child, 0, schema, src, pageSize);
            }
            byte[] right = src.get(rc);
            if (right != null) parseBTreePage(right, 0, schema, src, pageSize);
            return;
        }
        if (type != PAGE_LEAF_TABLE) return;
        for (int i = 0; i < cellCount; i++) {
            int cp = u16(pageData, headerOffset + 8 + i * 2);
            if (cp <= 0 || cp >= pageData.length) continue;
            try {
                SchemaEntry e = parseSchemaCellAt(pageData, cp, pageSize, src);
                if (e != null && e.rootPage() > 0) schema.put(e.rootPage(), e);
            } catch (Exception ignored) {}
        }
    }

    static SchemaEntry parseSchemaCellAt(byte[] page, int offset,
                                         int pageSize, PageSource src) throws IOException {
        int[] r1 = readVarint(page, offset);
        long payloadSize = Integer.toUnsignedLong(r1[0]);
        int[] r2 = readVarint(page, offset + r1[1]);
        int ps   = offset + r1[1] + r2[1];
        int maxLocal = pageSize - 35;
        byte[] payload = (payloadSize <= maxLocal)
                ? Arrays.copyOfRange(page, ps, (int)(ps + payloadSize))
                : readWithOverflow(page, ps, (int)payloadSize, maxLocal, src, pageSize);
        if (payload == null || payload.length == 0) return null;
        int[] hv = readVarint(payload, 0);
        int hdrEnd = hv[0], pos = hv[1];
        List<Long> st = new ArrayList<>();
        while (pos < hdrEnd) { int[] sv = readVarint(payload, pos); st.add((long)sv[0]); pos += sv[1]; }
        int dp = hdrEnd; String[] txt = new String[4]; long rootPage = 0;
        for (int col = 0; col < Math.min(4, st.size()); col++) {
            long s = st.get(col);
            if      (s==0)               {}
            else if (s>=1&&s<=4)         { long v=readSignedInt(payload,dp,(int)s); if(col==3)rootPage=v; dp+=(int)s; }
            else if (s==5)               { long v=readSignedInt(payload,dp,6); if(col==3)rootPage=v; dp+=6; }
            else if (s==6)               { long v=readSignedInt(payload,dp,8); if(col==3)rootPage=v; dp+=8; }
            else if (s==7)               { dp+=8; }
            else if (s==8)               { if(col==3)rootPage=0; }
            else if (s==9)               { if(col==3)rootPage=1; }
            // Use the database's actual text encoding (detected from the file
            // header by Job.parseHeader() and published in
            // SqliteElement.db_encoding), not a hardcoded UTF-8. For a
            // UTF-16BE/LE database, decoding 2-byte-per-char schema text
            // (table/index names) as UTF-8 produces a garbled name (every
            // real character followed by a stray byte, e.g. "Weekly_Ratings"
            // -> " W e e k l y _ R a t i n g s"). That garbled name then
            // becomes the table key used downstream (PageRecord.ownerName(),
            // RollbackJournalReader.analyzePage()'s tname, updateResultSet()),
            // so journal-side rows never merge with the correctly-decoded
            // table the live DB import created - they end up filed under an
            // orphan table name and never show up in that table's GUI view.
            else if (s>=13&&s%2==1)      { int len=(int)((s-13)/2); txt[col]=new String(payload,dp,len,SqliteElement.db_encoding); dp+=len; }
            else if (s>=12&&s%2==0)      { dp+=(int)((s-12)/2); }
        }
        return new SchemaEntry(txt[0]!=null?txt[0]:"?", txt[1]!=null?txt[1]:"?", rootPage);
    }

    static byte[] readWithOverflow(byte[] page, int ps, int total, int local,
                                   PageSource src, int pageSize) throws IOException {
        byte[] r = new byte[total];
        int tc = Math.min(local, total); System.arraycopy(page, ps, r, 0, tc);
        int rem = total-tc, dp = tc;
        long next = u32(page, ps+local);
        while (rem>0&&next!=0) {
            byte[] op = src.get(next); if(op==null)break;
            next = u32(op,0); int chunk=Math.min(rem,pageSize-4);
            System.arraycopy(op,4,r,dp,chunk); dp+=chunk; rem-=chunk;
        }
        return r;
    }

    // =========================================================================
    // OUTPUT
    // =========================================================================
    static void printResults(List<PageRecord> records, List<Segment> segments) {
        sep();
        System.out.printf("  Transaction segments found: %d%n%n", segments.size());
        for (Segment seg : segments) {
            String cnt = (seg.declaredCount() == -1 || seg.declaredCount() == 0xFFFFFFFF)
                    ? "unknown" : String.valueOf(seg.declaredCount());
            System.out.printf(
                    "  Segment %d  |  offset: 0x%06X  |  sector: %d  " +
                    "|  declared records: %-8s  |  read: %d  |  %s%n",
                    seg.index(), seg.headerOffset(), seg.sectorSize(), cnt,
                    seg.records().size(),
                    seg.isHot() ? "HOT (active/crashed transaction)" : "committed");
        }
        System.out.println();

        sep();
        System.out.println("  Record analysis  " +
                           "(page data shown is the pre-transaction / original content)");
        sep();
        System.out.printf("  %-4s  %-6s  %-12s  %-8s  %-4s  %-28s  %-8s  %s%n",
                "Seg", "Rec", "Offset", "Page", "CS?", "Page type", "DB type", "Object");
        System.out.println("  " + "-".repeat(92));

        for (PageRecord r : records) {
            System.out.printf("  %-4d  %-6d  0x%08X    %-8d  %-4s  %-28s  %-8s  %s%n",
                    r.segmentIndex(), r.recordIndex(), r.recordOffset(), r.pageNumber(),
                    r.checksumValid() ? "✓" : "✗",
                    r.pageTypeName(), r.ownerType(), r.ownerName());
        }

        sep();
        System.out.println("\n  Summary by database object:");
        System.out.println("  " + "-".repeat(60));
        Map<String, Set<Long>> byOwner = new LinkedHashMap<>();
        for (PageRecord r : records) {
            String key = r.ownerType().equals("—")
                    ? r.ownerName()
                    : r.ownerType() + " / " + r.ownerName();
            byOwner.computeIfAbsent(key, k -> new TreeSet<>()).add(r.pageNumber());
        }
        byOwner.forEach((obj, pages) ->
                System.out.printf("  %-45s  pages: %s%n", obj,
                        pages.stream().map(Object::toString)
                                .reduce((a, b) -> a + ", " + b).orElse("—")));
        sep();
    }

    // =========================================================================
    // HELPERS
    // =========================================================================
    static byte[] readBytesFromFile(Path path, long offset, int length) throws IOException {
        byte[] buf = new byte[length];
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            raf.seek(offset); raf.readFully(buf);
        }
        return buf;
    }

    static byte[] readPageFromFile(Path path, long pageNo, int pageSize) {
        try { return readBytesFromFile(path, (pageNo - 1) * (long) pageSize, pageSize); }
        catch (IOException e) { return null; }
    }

    static int u16(byte[] b, int off) {
        return ((b[off]&0xFF)<<8)|(b[off+1]&0xFF);
    }

    static long u32(byte[] b, int off) {
        return ((long)(b[off]&0xFF)<<24)|((long)(b[off+1]&0xFF)<<16)
               |((long)(b[off+2]&0xFF)<< 8)|(long)(b[off+3]&0xFF);
    }

    static long readSignedInt(byte[] b, int off, int bytes) {
        long v=0; for(int i=0;i<bytes;i++) v=(v<<8)|(b[off+i]&0xFF);
        int s=64-bytes*8; return (v<<s)>>s;
    }

    static int[] readVarint(byte[] b, int off) {
        long v=0;
        for(int i=0;i<9;i++){
            int bt=b[off+i]&0xFF;
            if(i<8){v=(v<<7)|(bt&0x7F);if((bt&0x80)==0)return new int[]{(int)v,i+1};}
            else return new int[]{(int)((v<<8)|bt),9};
        }
        return new int[]{(int)v,9};
    }

    static String pageTypeName(int t) {
        return switch(t) {
            case PAGE_INTERIOR_INDEX -> "Interior index B-Tree (0x02)";
            case PAGE_INTERIOR_TABLE -> "Interior table B-Tree (0x05)";
            case PAGE_LEAF_INDEX     -> "Leaf index B-Tree     (0x0A)";
            case PAGE_LEAF_TABLE     -> "Leaf table B-Tree     (0x0D)";
            case 0x00                -> "Overflow page         (0x00)";
            default -> String.format("Unknown               (0x%02X)", t);
        };
    }

    static void sep() { System.out.println("=".repeat(90)); }
}
