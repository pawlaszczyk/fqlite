package fqlite.base;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.io.*;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * SQLite WAL Analyzer — fully JDBC-free, pure binary parsing.
 *
 * Reads the schema directly from the raw .db file and respects pages
 * that are still in the WAL (including Page 1 / sqlite_schema).
 * Traverses all B-Trees to correctly attribute non-root pages as well.
 * JDBC is never opened, so the WAL archive remains untouched.
 *
 * Usage:
 *   java WalAnalyzer <database.db> <database.db-wal>
 *
 * No external dependencies — pure Java 17+.
 */
public class WALAnalyzer {

    static final int WAL_HEADER_SIZE   = 32;
    static final int FRAME_HEADER_SIZE = 24;

    // SQLite B-Tree page types
    static final int PAGE_INTERIOR_INDEX = 0x02;
    static final int PAGE_INTERIOR_TABLE = 0x05;
    static final int PAGE_LEAF_INDEX     = 0x0A;
    static final int PAGE_LEAF_TABLE     = 0x0D;

    record SchemaEntry(String type, String name, long rootPage) {}

    record FrameInfo(
            int    frameIndex,
            long   pageNumber,
            int    commitSize,
            int    pageTypeByte,
            String pageTypeName,
            String ownerName,
            String ownerType
    ) {}

    @FunctionalInterface interface PageSource { byte[] get(long pageNo) throws IOException; }

   public static void analyzeWAL(WALReader reader) throws Exception {

        Path dbPath  = Path.of(reader.job.path);
        Path walPath = Path.of(reader.path);

        if (!Files.exists(dbPath))  { System.err.println("Database file not found: " + dbPath);  System.exit(1); }
        if (!Files.exists(walPath)) { System.err.println("WAL file not found:      " + walPath); System.exit(1); }

        sep(); System.out.println("  DB : " + dbPath.toAbsolutePath());
        System.out.println("  WAL: " + walPath.toAbsolutePath()); sep();

        // 1. Read page size from DB header (bytes 16-17)
        byte[] dbHeader = readBytesFromFile(dbPath, 0, 100);
        int pageSize = readPageSize(dbHeader);
        System.out.printf("  Page size: %d bytes%n%n", pageSize);

        // 2. Read WAL file
        byte[] wal = Files.readAllBytes(walPath);
        if (wal.length < WAL_HEADER_SIZE) throw new IOException("WAL file too small.");
        printWalHeader(wal);

        int frameSize  = FRAME_HEADER_SIZE + pageSize;
        int frameCount = (wal.length - WAL_HEADER_SIZE) / frameSize;
        System.out.printf("  Frames in WAL: %d%n%n", frameCount);

        // 3. Extract the latest version of each page from the WAL
        //    (later frames override earlier ones for the same page number)
        Map<Long, byte[]> walPages = extractLatestWalPages(wal, pageSize, frameCount);
        System.out.printf("  Distinct pages in WAL: %s%n%n",
                walPages.keySet().stream().sorted()
                        .map(Object::toString)
                        .reduce((a, b) -> a + ", " + b).orElse("—"));

        // Page source: WAL takes precedence over the DB file
        PageSource src = (pageNo) -> {
            byte[] p = walPages.get(pageNo);
            return p != null ? p : readPageFromFile(dbPath, pageNo, pageSize);
        };

        // 4. Read schema from Page 1 (WAL-first)
        Map<Long, SchemaEntry> schema = readSchemaMerged(src, pageSize);

        if (schema.isEmpty()) {
            System.out.println("    (none found — sqlite_schema may not be committed yet)");
        } else {
            schema.forEach((root, e) ->
                    System.out.printf("    Root page %4d  %-8s  %s%n", root, e.type(), e.name()));
        }
        System.out.println();

        // 5. Build a complete page-to-owner map by traversing all B-Trees
        //    pageOwner: pageNumber -> SchemaEntry
        Map<Long, SchemaEntry> pageOwner = buildPageOwnerMap(schema, src, pageSize);

        // Register sqlite_schema itself (page 1)
        pageOwner.put(1L, new SchemaEntry("system", "sqlite_schema", 1L));

        // 6. Analyze frames and print results
        List<FrameInfo> frames = analyzeFrames(wal, pageSize, frameCount, pageOwner);
        //printResults(frames);
       reader.frames = frames;
       reader.pageOwner = pageOwner;
    }


    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java WalAnalyzer <database.db> <database.db-wal>");
            System.exit(1);
        }

        Path dbPath  = Path.of(args[0]);
        Path walPath = Path.of(args[1]);

        if (!Files.exists(dbPath))  { System.err.println("Database file not found: " + dbPath);  System.exit(1); }
        if (!Files.exists(walPath)) { System.err.println("WAL file not found:      " + walPath); System.exit(1); }

        sep(); System.out.println("  SQLite WAL Analyzer  (JDBC-free, pure binary)");
        sep(); System.out.println("  DB : " + dbPath.toAbsolutePath());
        System.out.println("  WAL: " + walPath.toAbsolutePath()); sep();

        // 1. Read page size from DB header (bytes 16-17)
        byte[] dbHeader = readBytesFromFile(dbPath, 0, 100);
        int pageSize = readPageSize(dbHeader);
        System.out.printf("  Page size: %d bytes%n%n", pageSize);

        // 2. Read WAL file
        byte[] wal = Files.readAllBytes(walPath);
        if (wal.length < WAL_HEADER_SIZE) throw new IOException("WAL file too small.");
        printWalHeader(wal);

        int frameSize  = FRAME_HEADER_SIZE + pageSize;
        int frameCount = (wal.length - WAL_HEADER_SIZE) / frameSize;
        System.out.printf("  Frames in WAL: %d%n%n", frameCount);

        // 3. Extract the latest version of each page from the WAL
        //    (later frames override earlier ones for the same page number)
        Map<Long, byte[]> walPages = extractLatestWalPages(wal, pageSize, frameCount);
        System.out.printf("  Distinct pages in WAL: %s%n%n",
                walPages.keySet().stream().sorted()
                        .map(Object::toString)
                        .reduce((a, b) -> a + ", " + b).orElse("—"));

        // Page source: WAL takes precedence over the DB file
        PageSource src = (pageNo) -> {
            byte[] p = walPages.get(pageNo);
            return p != null ? p : readPageFromFile(dbPath, pageNo, pageSize);
        };

        // 4. Read schema from Page 1 (WAL-first)
        Map<Long, SchemaEntry> schema = readSchemaMerged(src, pageSize);

        System.out.println("  Schema objects loaded (WAL + DB, no JDBC):");
        if (schema.isEmpty()) {
            System.out.println("    (none found — sqlite_schema may not be committed yet)");
        } else {
            schema.forEach((root, e) ->
                    System.out.printf("    Root page %4d  %-8s  %s%n", root, e.type(), e.name()));
        }
        System.out.println();

        // 5. Build a complete page-to-owner map by traversing all B-Trees
        //    pageOwner: pageNumber -> SchemaEntry
        Map<Long, SchemaEntry> pageOwner = buildPageOwnerMap(schema, src, pageSize);

        // Register sqlite_schema itself (page 1)
        pageOwner.put(1L, new SchemaEntry("system", "sqlite_schema", 1L));

        // 6. Analyze frames and print results
        List<FrameInfo> frames = analyzeFrames(wal, pageSize, frameCount, pageOwner);
        printResults(frames);
    }

    // =========================================================================
    // B-TREE TRAVERSAL: map every page to its owning schema object
    // =========================================================================

    /**
     * Traverses every B-Tree in the schema completely and builds a map
     * from page number to SchemaEntry, covering interior, leaf, and overflow pages.
     */
    static Map<Long, SchemaEntry> buildPageOwnerMap(Map<Long, SchemaEntry> schema,
                                                    PageSource src, int pageSize)
            throws IOException {
        Map<Long, SchemaEntry> owner = new HashMap<>();
        // visited is shared across all tables so that a page claimed by one
        // table cannot be overwritten by a later table's traversal.
        Set<Long> visited = new HashSet<>();
        for (SchemaEntry entry : schema.values()) {
            collectBTreePages(entry.rootPage(), entry, src, pageSize, owner, visited);
        }
        return owner;
    }

    static void collectBTreePages(long pageNo, SchemaEntry owner,
                                  PageSource src, int pageSize,
                                  Map<Long, SchemaEntry> result,
                                  Set<Long> visited) throws IOException {
        if (pageNo <= 0 || !visited.add(pageNo)) return; // cycle guard

        byte[] pageData = src.get(pageNo);
        if (pageData == null || pageData.length == 0) return;

        // Attribute this page to the owner
        result.put(pageNo, owner);

        // Page 1 carries a 100-byte DB header before the B-Tree header
        int ho   = (pageNo == 1) ? 100 : 0;
        int type = Byte.toUnsignedInt(pageData[ho]);

        int cellCount = u16(pageData, ho + 3);

        if (type == PAGE_INTERIOR_TABLE || type == PAGE_INTERIOR_INDEX) {
            // Right-most child pointer sits at offset 8 of the B-Tree header
            long rightChild = u32(pageData, ho + 8);
            collectBTreePages(rightChild, owner, src, pageSize, result, visited);

            // Left children are referenced from the cell pointer array
            // Interior header is 12 bytes (8 base + 4 right-child pointer)
            for (int i = 0; i < cellCount; i++) {
                int cellPtr    = u16(pageData, ho + 12 + i * 2);
                if (cellPtr <= 0 || cellPtr >= pageData.length) continue;
                long leftChild = u32(pageData, cellPtr);
                collectBTreePages(leftChild, owner, src, pageSize, result, visited);
            }
        } else if (type == PAGE_LEAF_TABLE || type == PAGE_LEAF_INDEX) {
            // Leaf pages have no child pointers, but may reference overflow pages
            collectOverflowPages(pageData, ho, type, pageSize, owner, src, result, visited);
        }
    }

    /**
     * Finds overflow pages referenced by cells on a leaf page.
     */
    static void collectOverflowPages(byte[] pageData, int ho, int type,
                                     int pageSize, SchemaEntry owner,
                                     PageSource src,
                                     Map<Long, SchemaEntry> result,
                                     Set<Long> visited) throws IOException {
        int cellCount = u16(pageData, ho + 3);
        int maxLocal  = pageSize - 35;

        for (int i = 0; i < cellCount; i++) {
            int cellPtr = u16(pageData, ho + 8 + i * 2);
            if (cellPtr <= 0 || cellPtr >= pageData.length) continue;

            try {
                int pos = cellPtr;
                int[] r1 = readVarint(pageData, pos);
                long payloadSize = Integer.toUnsignedLong(r1[0]);
                pos += r1[1];

                if (type == PAGE_LEAF_TABLE) {
                    // Skip rowid varint
                    int[] r2 = readVarint(pageData, pos);
                    pos += r2[1];
                }

                if (payloadSize > maxLocal) {
                    // Overflow pointer is stored after the local payload portion
                    int overflowPtrOffset = pos + maxLocal;
                    if (overflowPtrOffset + 4 <= pageData.length) {
                        long overflowPage = u32(pageData, overflowPtrOffset);
                        followOverflowChain(overflowPage, owner, src, pageSize, result, visited);
                    }
                }
            } catch (Exception ignored) {}
        }
    }

    static void followOverflowChain(long pageNo, SchemaEntry owner,
                                    PageSource src, int pageSize,
                                    Map<Long, SchemaEntry> result,
                                    Set<Long> visited) throws IOException {
        while (pageNo != 0 && visited.add(pageNo)) {
            result.put(pageNo, owner);
            byte[] p = src.get(pageNo);
            if (p == null || p.length < 4) break;
            pageNo = u32(p, 0); // next overflow page
        }
    }

    // =========================================================================
    // WAL: extract the latest version of each page
    // =========================================================================
    static Map<Long, byte[]> extractLatestWalPages(byte[] wal, int pageSize, int frameCount) {
        // Later frames win: iterating forward means each put() overwrites older data
        Map<Long, byte[]> pages = new TreeMap<>();
        for (int i = 0; i < frameCount; i++) {
            int  base   = WAL_HEADER_SIZE + i * (FRAME_HEADER_SIZE + pageSize);
            long pageNo = u32(wal, base);
            byte[] data = Arrays.copyOfRange(wal, base + FRAME_HEADER_SIZE,
                    base + FRAME_HEADER_SIZE + pageSize);
            pages.put(pageNo, data);
        }
        return pages;
    }

    // =========================================================================
    // SCHEMA: WAL pages take precedence over the DB file
    // =========================================================================
    static Map<Long, SchemaEntry> readSchemaMerged(PageSource src, int pageSize)
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
    // B-TREE PARSER (sqlite_schema)
    // =========================================================================
    static void parseBTreePage(byte[] pageData, int headerOffset,
                               Map<Long, SchemaEntry> schema,
                               PageSource src, int pageSize) throws IOException {
        int type      = Byte.toUnsignedInt(pageData[headerOffset]);
        int cellCount = u16(pageData, headerOffset + 3);

        if (type == PAGE_INTERIOR_TABLE) {
            long rightChild = u32(pageData, headerOffset + 8);
            for (int i = 0; i < cellCount; i++) {
                int cellPtr    = u16(pageData, headerOffset + 12 + i * 2);
                long leftChild = u32(pageData, cellPtr);
                byte[] child   = src.get(leftChild);
                if (child != null) parseBTreePage(child, 0, schema, src, pageSize);
            }
            byte[] right = src.get(rightChild);
            if (right != null) parseBTreePage(right, 0, schema, src, pageSize);
            return;
        }

        if (type != PAGE_LEAF_TABLE) return;

        for (int i = 0; i < cellCount; i++) {
            int cellPtr = u16(pageData, headerOffset + 8 + i * 2);
            if (cellPtr <= 0 || cellPtr >= pageData.length) continue;
            try {
                SchemaEntry e = parseSchemaCellAt(pageData, cellPtr, pageSize, src);
                if (e != null && e.rootPage() > 0)
                    schema.put(e.rootPage(), e);
            } catch (Exception ignored) {}
        }
    }

    static SchemaEntry parseSchemaCellAt(byte[] page, int offset,
                                         int pageSize, PageSource src) throws IOException {
        // Leaf table cell layout: payload_size (varint), rowid (varint), payload
        int[] r1 = readVarint(page, offset);
        long payloadSize = Integer.toUnsignedLong(r1[0]);
        int[] r2 = readVarint(page, offset + r1[1]); // rowid — not needed
        int payloadStart = offset + r1[1] + r2[1];

        int maxLocal = pageSize - 35;
        byte[] payload;
        if (payloadSize <= maxLocal) {
            payload = Arrays.copyOfRange(page, payloadStart, (int)(payloadStart + payloadSize));
        } else {
            payload = readWithOverflow(page, payloadStart, (int)payloadSize, maxLocal, src, pageSize);
        }
        if (payload == null || payload.length == 0) return null;

        // Parse record header to get serial types
        int[] hv   = readVarint(payload, 0);
        int hdrEnd = hv[0];
        int pos    = hv[1];

        List<Long> serialTypes = new ArrayList<>();
        while (pos < hdrEnd) {
            int[] sv = readVarint(payload, pos);
            serialTypes.add((long) sv[0]);
            pos += sv[1];
        }

        // Read columns: 0=type, 1=name, 2=tbl_name, 3=rootpage
        int dataPos = hdrEnd;
        String[] txt = new String[4];
        long rootPage = 0;

        for (int col = 0; col < Math.min(4, serialTypes.size()); col++) {
            long st = serialTypes.get(col);
            if      (st == 0)                  { /* NULL */ }
            else if (st >= 1 && st <= 4)       { long v = readSignedInt(payload, dataPos, (int)st); if(col==3) rootPage=v; dataPos+=(int)st; }
            else if (st == 5)                  { long v = readSignedInt(payload, dataPos, 6); if(col==3) rootPage=v; dataPos+=6; }
            else if (st == 6)                  { long v = readSignedInt(payload, dataPos, 8); if(col==3) rootPage=v; dataPos+=8; }
            else if (st == 7)                  { dataPos+=8; }
            else if (st == 8)                  { if(col==3) rootPage=0; }
            else if (st == 9)                  { if(col==3) rootPage=1; }
            else if (st >= 13 && st % 2 == 1) { int len=(int)((st-13)/2); txt[col]=new String(payload,dataPos,len,StandardCharsets.UTF_8); dataPos+=len; }
            else if (st >= 12 && st % 2 == 0) { dataPos+=(int)((st-12)/2); }
        }

        String type = txt[0] != null ? txt[0] : "?";
        String name = txt[1] != null ? txt[1] : "?";
        return new SchemaEntry(type, name, rootPage);
    }

    // =========================================================================
    // OVERFLOW PAGES
    // =========================================================================
    static byte[] readWithOverflow(byte[] page, int payloadStart, int totalSize,
                                   int localSize, PageSource src, int pageSize) throws IOException {
        byte[] result = new byte[totalSize];
        int toCopy    = Math.min(localSize, totalSize);
        System.arraycopy(page, payloadStart, result, 0, toCopy);
        int remaining = totalSize - toCopy;
        int destPos   = toCopy;
        long nextPage = u32(page, payloadStart + localSize);
        while (remaining > 0 && nextPage != 0) {
            byte[] op = src.get(nextPage);
            if (op == null) break;
            nextPage  = u32(op, 0);
            int chunk = Math.min(remaining, pageSize - 4);
            System.arraycopy(op, 4, result, destPos, chunk);
            destPos   += chunk;
            remaining -= chunk;
        }
        return result;
    }

    // =========================================================================
    // FRAME ANALYSIS
    // =========================================================================
    static List<FrameInfo> analyzeFrames(byte[] wal, int pageSize, int frameCount,
                                         Map<Long, SchemaEntry> pageOwner) {
        List<FrameInfo> result = new ArrayList<>();
        for (int i = 0; i < frameCount; i++) {
            int  base    = WAL_HEADER_SIZE + i * (FRAME_HEADER_SIZE + pageSize);
            long pageNum = u32(wal, base);
            int  commit  = (int) u32(wal, base + 4);

            byte[] pageData = Arrays.copyOfRange(wal, base + FRAME_HEADER_SIZE,
                    base + FRAME_HEADER_SIZE + pageSize);

            // Page 1 carries a 100-byte DB header before the B-Tree header
            int headerOff = (pageNum == 1) ? 100 : 0;
            int typeByte  = (headerOff < pageData.length)
                    ? Byte.toUnsignedInt(pageData[headerOff]) : 0;

            SchemaEntry owner = pageOwner.get(pageNum);
            String ownerName = owner != null ? owner.name() : "<unattributed>";
            String ownerType = owner != null ? owner.type() : "—";

            result.add(new FrameInfo(i + 1, pageNum, commit,
                    typeByte, pageTypeName(typeByte),
                    ownerName, ownerType));
        }
        return result;
    }

    // =========================================================================
    // OUTPUT
    // =========================================================================
    static void printWalHeader(byte[] wal) {
        ByteBuffer h = ByteBuffer.wrap(wal, 0, WAL_HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
        System.out.printf("  WAL header:%n");
        System.out.printf("    Magic               : 0x%08X%n", h.getInt());
        System.out.printf("    File format version : %d%n",     h.getInt());
        System.out.printf("    Page size (WAL)     : %d%n",     h.getInt());
        System.out.printf("    Checkpoint sequence : %d%n",     h.getInt());
        System.out.printf("    Salt-1 / Salt-2     : 0x%08X / 0x%08X%n%n", h.getInt(), h.getInt());
    }

    static void printResults(List<FrameInfo> frames) {
        sep();
        System.out.println("  Frame analysis");
        sep();
        System.out.printf("  %-6s  %-8s  %-10s  %-28s  %-8s  %s%n",
                "Frame", "Page", "Commit", "Page type", "DB type", "Object");
        System.out.println("  " + "-".repeat(80));

        for (FrameInfo f : frames) {
            String commit = f.commitSize() > 0
                    ? String.format("yes (%d)", f.commitSize()) : "no";
            System.out.printf("  %-6d  %-8d  %-10s  %-28s  %-8s  %s%n",
                    f.frameIndex(), f.pageNumber(), commit,
                    f.pageTypeName(), f.ownerType(), f.ownerName());
        }

        sep();
        System.out.println("\n  Summary by database object:");
        System.out.println("  " + "-".repeat(60));
        Map<String, Set<Long>> byOwner = new LinkedHashMap<>();
        for (FrameInfo f : frames) {
            String key = f.ownerType().equals("—")
                    ? f.ownerName()
                    : f.ownerType() + " / " + f.ownerName();
            byOwner.computeIfAbsent(key, k -> new TreeSet<>()).add(f.pageNumber());
        }
        byOwner.forEach((obj, pages) ->
                System.out.printf("  %-45s  pages: %s%n", obj,
                        pages.stream().map(Object::toString)
                                .reduce((a, b) -> a + ", " + b).orElse("—")));

        long uncommitted = frames.stream().filter(f -> f.commitSize() == 0).count();
        if (uncommitted > 0) {
            System.out.println();
            System.out.printf("  ⚠  %d frame(s) not yet committed (not written to DB)%n",
                    uncommitted);
        }
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

    static int readPageSize(byte[] h) {
        // Bytes 16-17, big-endian; value 1 means 65536
        int raw = u16(h, 16); return (raw == 1) ? 65536 : raw;
    }

    static int u16(byte[] b, int off) {
        return ((b[off] & 0xFF) << 8) | (b[off+1] & 0xFF);
    }

    static long u32(byte[] b, int off) {
        return ((long)(b[off]&0xFF)<<24) | ((long)(b[off+1]&0xFF)<<16)
               | ((long)(b[off+2]&0xFF)<< 8) |  (long)(b[off+3]&0xFF);
    }

    static long readSignedInt(byte[] b, int off, int bytes) {
        long v = 0;
        for (int i = 0; i < bytes; i++) v = (v << 8) | (b[off+i] & 0xFF);
        int shift = 64 - bytes * 8;
        return (v << shift) >> shift;
    }

    /**
     * Reads a SQLite variable-length integer (varint).
     * Returns int[]{value, bytesRead}.
     * Each byte contributes 7 bits; the MSB signals whether more bytes follow.
     * The 9th byte (if reached) uses all 8 bits.
     */
    static int[] readVarint(byte[] b, int off) {
        long val = 0;
        for (int i = 0; i < 9; i++) {
            int byt = b[off+i] & 0xFF;
            if (i < 8) { val = (val << 7) | (byt & 0x7F); if ((byt & 0x80) == 0) return new int[]{(int)val, i+1}; }
            else        { return new int[]{(int)((val << 8) | byt), 9}; }
        }
        return new int[]{(int)val, 9};
    }

    static String pageTypeName(int t) {
        return switch (t) {
            case PAGE_INTERIOR_INDEX -> "Interior index B-Tree (0x02)";
            case PAGE_INTERIOR_TABLE -> "Interior table B-Tree (0x05)";
            case PAGE_LEAF_INDEX     -> "Leaf index B-Tree     (0x0A)";
            case PAGE_LEAF_TABLE     -> "Leaf table B-Tree     (0x0D)";
            case 0x00                -> "Overflow page         (0x00)";
            default -> String.format("Unknown               (0x%02X)", t);
        };
    }

    static void sep() { System.out.println("=".repeat(80)); }
}