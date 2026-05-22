package fqlite.util;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.imageio.ImageIO;
import fqlite.base.*;
import fqlite.descriptor.AbstractDescriptor;
import fqlite.descriptor.IndexDescriptor;
import fqlite.descriptor.TableDescriptor;
import fqlite.log.AppLog;
import fqlite.parser.SQLiteSchemaParser;
import fqlite.pattern.HeaderPattern;
import fqlite.pattern.IntegerConstraint;
import fqlite.types.BLOBElement;
import fqlite.types.BLOBTYPE;
import fqlite.types.SerialTypes;
import fqlite.types.StorageClass;
import fqlite.types.TimeStamp;
import fqlite.viewer.util.HexDump;
import javafx.embed.swing.SwingFXUtils;
import javafx.scene.image.Image;

/**
 * This class offers several useful methods that are needed from time to time
 * for the acquisition process.
 *
 * @author pawlaszc
 */
public class Auxiliary {

    // -------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------

    public static final String TABLELEAFPAGE    = "0d";
    public static final String TABLEINTERIORPAGE = "05";
    public static final String INDEXLEAFPAGE    = "0a";
    public static final String INDEXINTERIORPAGE = "02";
    public static final String OVERFLOWPAGE     = "00";

    /** UNIX Epoch in seconds — valid range 2010–2050 */
    static final long UNIX_MIN_SECONDS   = 1_262_304_000L;
    static final long UNIX_MAX_SECONDS   = 2_524_608_000L;

    /** UNIX Epoch in milliseconds — valid range 2010–2050 */
    static final long UNIX_MIN_DATE      = 1_262_304_000_000L;
    static final long UNIX_MAX_DATE      = 2_524_608_000_000L;

    /** UNIX Epoch in microseconds */
    static final long UNIX_MIN_DATE_NANO = 1_262_304_000_000_000L;
    static final long UNIX_MAX_DATE_NANO = 2_524_608_000_000_000L;

    /** UNIX Epoch in nanoseconds */
    static final long UNIX_MIN_DATE_PICO = 1_262_304_000_000_000_000L;
    static final long UNIX_MAX_DATE_PICO = 2_524_608_000_000_000_000L;

    /** CFAbsoluteTime: seconds since 2001-01-01 — valid range Jul 2010 – Sep 2064 */
    static final double MAC_MIN_DATE      = 300_000_000;
    static final double MAC_MAX_DATE      = 800_000_000;

    /** CFAbsoluteTime in nanoseconds */
    static final long MAC_NANO_MIN_DATE   = 300_000_000_000_000_000L;
    static final long MAC_NANO_MAX_DATE   = 800_000_000_000_000_000L;

    /** MAC_EPOCH_OFFSET: seconds between 1970-01-01 and 2001-01-01 */
    private static final long MAC_EPOCH_OFFSET = 978_307_200L;

    /**
     * WEBKIT_EPOCH_OFFSET: seconds between 1601-01-01 (Windows / WebKit epoch)
     * and the Unix epoch 1970-01-01.
     * Used by Chromium / WebKit to store timestamps as microseconds since 1601-01-01.
     */
    private static final long WEBKIT_EPOCH_OFFSET = 11_644_473_600L;

    /**
     * WebKit / Chrome timestamp in microseconds since 1601-01-01.
     * Valid range 2010–2050 expressed in that epoch:
     *   lower = UNIX_MIN_SECONDS + WEBKIT_EPOCH_OFFSET (in µs)
     *   upper = UNIX_MAX_SECONDS + WEBKIT_EPOCH_OFFSET (in µs)
     */
    static final long WEBKIT_MIN_DATE = (UNIX_MIN_SECONDS + 11_644_473_600L) * 1_000_000L;
    static final long WEBKIT_MAX_DATE = (UNIX_MAX_SECONDS + 11_644_473_600L) * 1_000_000L;

    /** Shared timestamp formatter (thread-safe, immutable) */
    static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("MM/dd/yyyy - HH:mm:ss Z");

    protected static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

    // Pre-allocated singleton SqliteElement instances to avoid repeated allocation
    static final SqliteElement PRIMKEY  = new SqliteElement(SerialTypes.PRIMARY_KEY,  StorageClass.INT,   0);
    static final SqliteElement INT8     = new SqliteElement(SerialTypes.INT8,         StorageClass.INT,   1);
    static final SqliteElement INT16    = new SqliteElement(SerialTypes.INT16,        StorageClass.INT,   2);
    static final SqliteElement INT24    = new SqliteElement(SerialTypes.INT24,        StorageClass.INT,   3);
    static final SqliteElement INT32    = new SqliteElement(SerialTypes.INT32,        StorageClass.INT,   4);
    static final SqliteElement INT48    = new SqliteElement(SerialTypes.INT48,        StorageClass.INT,   6);
    static final SqliteElement INT64    = new SqliteElement(SerialTypes.INT64,        StorageClass.INT,   8);
    static final SqliteElement FLOAT64  = new SqliteElement(SerialTypes.FLOAT64,      StorageClass.FLOAT, 8);
    static final SqliteElement CONST0   = new SqliteElement(SerialTypes.INT0,         StorageClass.INT,   0);
    static final SqliteElement CONST1   = new SqliteElement(SerialTypes.INT1,         StorageClass.INT,   0);

    // -------------------------------------------------------------------------
    // Fields
    // -------------------------------------------------------------------------

    public AtomicInteger found = new AtomicInteger();
    public Job job;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * Constructor. An object reference to the job is required so that results
     * can be returned to the calling job environment.
     *
     * @param job the current recovery job
     */
    public Auxiliary(Job job) {
        this.job = job;
    }

    // -------------------------------------------------------------------------
    // Page-type helpers
    // -------------------------------------------------------------------------

    /**
     * Returns the numeric page type for the given 2-nibble hex prefix.
     * <p>
     * There are four basic B-tree page types in SQLite:
     * <ol>
     *   <li>Table-leaf page (0x0D) → 8</li>
     *   <li>Table-interior page (0x05) → 5</li>
     *   <li>Index-leaf page (0x0A) → 10</li>
     *   <li>Index-interior page (0x02) → 2</li>
     * </ol>
     * Overflow/dropped pages start with 0x00 and return 0.
     * Any unrecognised prefix returns -1.
     *
     * @param content the page content as a hex string (first two chars used)
     * @return numeric page type, or -1 for unknown
     */
    public static int getPageType(String content) {
        switch (content.substring(0, 2)) {
            case TABLELEAFPAGE:     return 8;
            case TABLEINTERIORPAGE: return 5;
            case INDEXLEAFPAGE:     return 10;
            case INDEXINTERIORPAGE: return 2;
            case OVERFLOWPAGE:      return 0;   // overflow or dropped page
            default:                return -1;
        }
    }

    // -------------------------------------------------------------------------
    // Master-table record reading
    // -------------------------------------------------------------------------

    /**
     * Reads a schema record from the SQLite master table (sqlite_master) into
     * the job's descriptor structures.
     *
     * @param job    the current recovery job
     * @param start  byte offset of the record within the buffer
     * @param buffer the database content
     * @param header hex-encoded header string for the record
     * @return {@code true} if the record was parsed successfully
     * @throws IOException on read errors
     */
    public boolean readMasterTableRecord(Job job, long start, BigByteBuffer buffer, String header)
            throws IOException {

        int mt = (int) start / job.ps;
        job.mastertable.add(mt + 1);

        SqliteElement[] columns = masterRecordToColumns(header);
        if (columns == null) {
            return false;
        }

        String cl  = header.substring(2);
        int    pll = this.computePayloadLength(cl.substring(0, 12));

        buffer.position(start + 8);
        int so       = computePayload(pll);
        int overflow = -1;

        /* Handle overflow pages */
        if (so < pll) {
            long last = buffer.position();

            overflow = buffer.getInt(job.ps - 4);
            if (overflow > job.numberofpages) {
                return false;
            }

            job.mastertable.add(overflow);

            byte[] extended = readOverflow(job, overflow - 1);

            byte[] originalbuffer = new byte[job.ps];
            for (int bb = 0; bb < job.ps; bb++) {
                originalbuffer[bb] = buffer.get(bb);
            }

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            outputStream.write(originalbuffer);
            outputStream.write(extended);

            buffer = BigByteBuffer.wrap(outputStream.toByteArray());
            buffer.position(last);
        }

        int    con        = 0;
        String objecttype = null;
        String namespace  = null;
        String tablename  = null;
        int    rootpage   = -1;
        String statement  = null;
        boolean autoindex = false;

        for (SqliteElement en : columns) {
            if (en == null) {
                con++;
                continue;
            }

            byte[] value = new byte[en.getlength()];
            try {
                buffer.get(value);
            } catch (BufferUnderflowException bue) {
                return false;
            }

            switch (con) {
                case 0:
                    objecttype = en.toString(value, true, true);
                    break;
                case 1:
                    namespace = en.toString(value, true, true);
                    if (namespace.startsWith("sqlite_autoindex_")) {
                        /*
                         * UNIQUE and PRIMARY KEY constraints create internal indexes with names
                         * of the form "sqlite_autoindex_TABLE_N".
                         */
                        autoindex = true;
                    }
                    break;
                case 2:
                    tablename = en.toString(value, true, true);
                    break;
                case 3:
                    if (value.length == 0) {
                        AppLog.debug("Seems to be a virtual component -> no root page");
                    } else {
                        try {
                            if      (en.type == SerialTypes.INT8)  rootpage = SqliteElement.decodeInt8(value[0]);
                            else if (en.type == SerialTypes.INT16)  rootpage = SqliteElement.decodeInt16(new byte[]{value[0], value[1]});
                            else if (en.type == SerialTypes.INT24)  rootpage = SqliteElement.decodeInt24(new byte[]{value[0], value[1], value[2]});
                            else if (en.type == SerialTypes.INT32)  rootpage = SqliteElement.decodeInt32(new byte[]{value[0], value[1], value[2], value[3]});
                            else return false;
                        } catch (Exception err) {
                            return false;
                        }
                    }
                    if (autoindex) {
                        createAutoIndexRecord(objecttype, namespace, tablename, rootpage);
                        break;
                    }
                    break;
                case 4:
                    statement = en.toString(value, true, true);
                    con++;
                    // All needed data collected — stop reading
                    if (!autoindex) {
                        SQLiteSchemaParser.parse(job, tablename, rootpage, statement);
                    } else {
                        autoindex = false;
                    }
                    job.mastertable.add(mt);
                    return true;
                default:
                    break;
            }
            con++;
        }

        if (!autoindex) {
            SQLiteSchemaParser.parse(job, tablename, rootpage, statement);
        }

        job.mastertable.add(mt);
        return true;
    }

    // -------------------------------------------------------------------------
    // Auto-index helpers
    // -------------------------------------------------------------------------

    private boolean createAutoIndexRecord(String objecttype, String namespace,
                                          String tablename, int rootpage) {
        ArrayList<String> colnames = new ArrayList<>();
        colnames.add("col1");

        IndexDescriptor ids = new IndexDescriptor(job, namespace, tablename, "", colnames);
        ids.root = rootpage;

        if (!job.indices.contains(ids)) {
            job.indices.add(ids);
        }
        return true;
    }

    // -------------------------------------------------------------------------
    // Deleted-record carving
    // -------------------------------------------------------------------------

    /**
     * Extracts a previously deleted record from unallocated (free) space.
     *
     * @param job        the current recovery job
     * @param buffer     data page to analyse
     * @param bs         bit-set tracking already-visited byte ranges
     * @param m          the pattern match describing the record header
     * @param next       the next match on the same page (may be {@code null})
     * @param pagenumber the 1-based page number being analysed
     * @param fallback   fallback serial-type string used when the first column is unknown
     * @return a {@link CarvingResult} or {@code null} if the record is invalid
     * @throws IOException on buffer read errors
     */
    public CarvingResult readDeletedRecord(AbstractDescriptor ad, Job job, ByteBuffer buffer, BitSet bs,
                                           Match m, Match next, int pagenumber,
                                           String fallback) throws IOException {

        boolean repeat = false;
        int round = 1;

        do{

            if(round > 10)
            {
                return null;
            }
            else
                repeat = true;

            int cnumber = -1;
            if (ad != null && ad.columnnames != null) {
                ad.columnnames.size();
            }

            LinkedList<String> record = new LinkedList<>();
            LinkedList<byte[]> hexdump = new LinkedList<>();
            List<SqliteElement> columns;
            int rowid = -1;

            // Guard: m.end must be within buffer bounds
            if (m.end < 0 || m.end > buffer.limit()) {
                AppLog.debug("readDeletedRecord: m.end=" + m.end + " out of buffer limit=" + buffer.limit() + ", skipping.");
                return null;
            }
            buffer.position(m.end);

            /* CASE 1: Complete but deleted record — try to read ROWID */
            if (m.match.startsWith("RI")) {
                String withoutRI = m.match.substring(2);
                String headerlengthbyte = withoutRI.substring(0, 2);
                int headerlength = varintHexString2Integer(headerlengthbyte);

                if (headerlength != (withoutRI.length() / 2)) {
                    return null;
                }

                m.match = m.match.replace("RI", "");
                // Guard: re-check after RI-strip
                if (m.end < 0 || m.end > buffer.limit()) {
                    AppLog.debug("readDeletedRecord: m.end=" + m.end + " out of buffer limit after RI-strip, skipping.");
                    return null;
                }
                buffer.position(m.end);
            }

            String header = m.match.substring(2);

            /* CASE 2: Partial header — first column overwritten */
            if (header.startsWith("XX")) {
                header = resolvePartialHeader(header, buffer, m, next, round);
                if (m.end < 0 || m.end > buffer.limit()) {
                    AppLog.debug("readDeletedRecord: m.end=" + m.end + " out of buffer limit after resolvePartialHeader, skipping.");
                    return null;
                }
                buffer.position(m.end);
            }

            columns = toColumns(header);
            if (columns == null) {
                AppLog.debug("No valid header-string: " + header);
                return null;
            }

            int pll = computePayloadLength(header);
            int so = computePayload(pll);

            record.add((pagenumber - 1) * job.ps + m.begin + "");
            hexdump.add(null);

            if (so < pll) {
                /* Spilled payload — read overflow */
                int phl = header.length() / 2;
                int last = buffer.position();
                AppLog.debug("Deleted spilled payload: " + so);
                AppLog.debug("Deleted pll payload: " + pll);

                int overflowSeekPos = buffer.position() + so - phl - 1;
                if (overflowSeekPos < 0 || overflowSeekPos + 4 > buffer.limit()) {
                    AppLog.debug("readDeletedRecord: overflow seek position " + overflowSeekPos + " out of buffer limit=" + buffer.limit() + ", treating as no-overflow.");
                    pll = so;
                    // fall through to the no-overflow branch below by adjusting so
                    // We set bf=buffer and skip the getInt entirely
                    ByteBuffer bf = buffer;
                    bf.position(last);
                    int blobcolidx = 0;
                    for (SqliteElement en : columns) {
                        if (en == null) continue;
                        if ((bf.position() + en.getlength()) > bf.limit()) break;
                        byte[] value = new byte[en.getlength()];
                        bf.get(value);
                        blobcolidx = appendColumn(record, hexdump, en, value, blobcolidx, false, 2);
                    }
                    buffer.position(Math.min(last + so - phl - 1, buffer.limit()));
                    bs.set(m.end, buffer.position(), true);
                    long cursor = ((pagenumber - 1L) * job.ps) + buffer.position();
                    record.add(0, "[" + pll + "|" + header.length() / 2 + "]");
                    record.add(1, "" + rowid);
                    hexdump.add(0, null);
                    hexdump.add(1, null);
                    round++;
                    if ((record.size() - columns.size()) < 2) {
                        repeat = true;
                        continue;
                    }
                    return new CarvingResult(buffer.position(), cursor, new StringBuffer(), record, hexdump);
                }

                buffer.position(overflowSeekPos);
                int overflow = buffer.getInt();
                AppLog.debug("Deleted overflow: " + overflow + " " + Integer.toHexString(overflow));
                buffer.position(last);

                ByteBuffer bf;
                if (overflow > 0 && overflow < job.numberofpages) {
                    byte[] extended = readOverflowIterativ(overflow - 1, false);
                    byte[] originalbuffer = copyBufferToArray(buffer);
                    byte[] c = new byte[pll + job.ps];
                    buffer.position(last);

                    System.arraycopy(originalbuffer, buffer.position(), c, 0, so + 7);
                    try {
                        if (extended != null) {
                            System.arraycopy(extended, 0, c, so - phl - 1, pll - so);
                        }
                        bf = ByteBuffer.wrap(c);
                    } catch (ArrayIndexOutOfBoundsException | NullPointerException err) {
                        AppLog.debug("Overflow merge error: " + err.getMessage());
                        bf = null;
                    }
                } else {
                    pll = so;
                    bf = buffer;
                }

                if (bf == null) return null;
                bf.position(0);

                int blobcolidx = 0;
                for (SqliteElement en : columns) {
                    if (en == null) continue;
                    if ((bf.position() + en.getlength()) > bf.limit()) {
                        AppLog.debug("readDeletedRecord: overflow bf underflow at pos=" + bf.position() + " needed=" + en.getlength() + " limit=" + bf.limit());
                        break;
                    }
                    byte[] value = new byte[en.getlength()];
                    bf.get(value);
                    //hexdump.add(value);
                    blobcolidx = appendColumn(record, hexdump, en, value, blobcolidx, false, 2);
                }

                int finalPos = last + so - phl - 1;
                buffer.position(Math.min(finalPos, buffer.limit()));

            }
            else {
                /* No overflow */
                if (pll < 42) {
                    int blobcolidx = 0;
                    int number = -1;
                    for (SqliteElement en : columns) {
                        number++;
                        if (en == null) continue;

                        if (rowid >= 0 && en.getlength() == 0 && m.rowidcolum == number) {
                            record.add(rowid + "");
                            byte[] bytes = ByteBuffer.allocate(4).putInt(rowid).array();
                            hexdump.add(bytes);
                            continue;
                        }

                        byte[] value = new byte[en.getlength()];
                        if ((buffer.position() + en.getlength()) > buffer.limit()) {
                            return null;
                        }
                        buffer.get(value);
                        //hexdump.add(value);
                        blobcolidx = appendColumn(record, hexdump, en, value, blobcolidx, false, 0);
                    }
                }
                else {
                    /* Large record — may be partial */
                    int nextrecord = bs.nextSetBit(buffer.position());
                    if (nextrecord == -1) nextrecord = job.ps;

                    boolean partial = false;
                    int blobcolidx = 0;
                    int cc = 0;

                    for (SqliteElement en : columns) {
                        if (partial) {
                            record.add("");
                            hexdump.add(null);
                            continue;
                        }
                        if (en == null){
                            record.add("");
                            continue;
                        }

                        if (rowid >= 0 && en.getlength() == 0 && m.rowidcolum == cc) {
                            record.add(rowid + "");
                            byte[] bytes = ByteBuffer.allocate(4).putLong(rowid).array();
                            hexdump.add(bytes);
                            cc++;
                            continue;
                        }

                        if ((buffer.position() + en.getlength()) > buffer.limit()) {
                            return null;
                        }

                        if ((buffer.position() + en.getlength()) > nextrecord) {
                            /* Partial column — truncate */
                            int newlength = nextrecord - buffer.position();
                            if (newlength > 0 &&
                                (en.type == SerialTypes.BLOB || en.type == SerialTypes.STRING)) {
                                SqliteElement truncated = en.clone(en, newlength);
                                byte[] value = new byte[truncated.getlength()];
                                buffer.get(value);
                                //hexdump.add(value);
                                blobcolidx = appendColumn(record, hexdump, truncated, value, blobcolidx, false, 0);
                            }
                            partial = true;
                            continue;
                        }

                        byte[] value = new byte[en.getlength()];
                        buffer.get(value);
                        //hexdump.add(value);
                        blobcolidx = appendColumn(record, hexdump, en, value, blobcolidx, false, 0);
                        cc++;
                    }
                }
            }

            /* Mark bytes as visited */
            bs.set(m.end, buffer.position(), true);
            long cursor = ((pagenumber - 1L) * job.ps) + buffer.position();
            AppLog.debug("Visited: " + m.end + " to " + buffer.position());

            record.add(0, "[" + pll + "|" + header.length() / 2 + "]");
            record.add(1, "" + rowid);
            hexdump.add(0, null);
            hexdump.add(1, null);

            String id = record.get(3);
            // in most cases the column byte length was set to 2 instead of 3 bytes for the int/rowid column
            // therefore, the 2-byte varint is only read half  -> 195 instead of 50001 or 50020
            round++;
            if ((record.size() - columns.size()) < 2) {
                repeat = true;
                continue;
            }
            else {
                // successful recovery -> number of columns does match to the number of fields
                return new CarvingResult(buffer.position(), cursor, new StringBuffer(), record, hexdump);
            }
        }
        while(repeat);
        return null;
    }

    /**
     * Resolves a partially-overwritten header that starts with the placeholder "XX".
     */
    private String resolvePartialHeader(String header, ByteBuffer buffer, Match m, Match next, int round) {

        //System.out.println(" header " + header + " round " + round);
        int number = 1 + round;

        if (true)
            return "0" + number + header.substring(2);

        // skip the old stuff:

        String match      = header.substring(2);
        int headerlength  = match.length() / 2;

        buffer.position(m.end - headerlength - 2);
        short lg = buffer.getShort();

        if (next != null && next.begin < m.begin + lg) {
            lg = (short) (lg - (m.begin + lg - next.begin));
        }

        int first = lg - 4 - headerlength - getPayloadLength(match);
        if (first < 0) {
            return "02" + header.substring(4);
        } else if (first <= 6) {
            String repl = Integer.toHexString(first);
            if (repl.length() % 2 != 0) repl = "0" + repl;
            return header.replace("XX", repl);
        } else {
            return header.replace("XX", "02");
        }
    }

    // -------------------------------------------------------------------------
    // Active-record reading
    // -------------------------------------------------------------------------

    /**
     * Reads an active (non-deleted) data record from the given page buffer.
     * <p>
     * A B-tree cell has the following on-disk layout:<br>
     * {@code [Payload length varint][Row ID varint][Header length varint][Serial types…][Data…]}
     *
     * @param cellstart      byte offset of the cell within the page
     * @param buffer         the page data
     * @param pagenumber_db  1-based page number in the database
     * @param bs             visited-bytes bit-set (may be {@code null})
     * @param maxlength      maximum bytes readable before the content area starts
     * @param withoutROWID   {@code true} for WITHOUT ROWID tables
     * @param filetype       one of {@link Global#ROLLBACK_JOURNAL_FILE},
     *                       {@link Global#WAL_ARCHIVE_FILE}, or a regular db file constant
     * @param offset         physical file offset override; use {@code -1} to derive from page
     * @return the record fields as a list, or {@code null} on error
     * @throws IOException on buffer read errors
     */
    public DataRow readRecord(int cellstart, ByteBuffer buffer, int pagenumber_db,
                              BitSet bs, int maxlength, boolean withoutROWID,
                              int filetype, long offset, String tblname) throws IOException {

        LinkedList<String> record = new LinkedList<>();
        LinkedList<byte[]> hexdump = new LinkedList<>();

        boolean  unknown    = false;
        boolean  isVT       = false;
        int      rowid_col  = -1;

        buffer.position(0);

        AbstractDescriptor td = null;

        if(tblname != null && tblname.equals("room_master_table")) {
            AppLog.debug("readRecord: room_master_table hit.");
        }

        /* Populate table-name and offset metadata */

        // case 1: WAL-Archive
        if (filetype == Global.WAL_ARCHIVE_FILE) {

            if(tblname == null)
                return null;

            record.add(tblname);
            hexdump.add(null);

            if (pagenumber_db < job.pages.length && job.pages[pagenumber_db] != null) {
                td = job.pages[pagenumber_db];
                rowid_col = td.rowid_col;
                if ( td instanceof TableDescriptor) {
                    isVT  = td.sql.toUpperCase().contains("CREATE VIRTUAL");
                }

            }

            record.add(Global.REGULAR_RECORD.intern());
            hexdump.add(null);
            record.add(offset > -1 ? offset + "" : cellstart + "");
            hexdump.add(null);

        }

        // case 2: Rollback-Journal
        else if (filetype == Global.ROLLBACK_JOURNAL_FILE) {

            if(tblname == null)
                return null;

            record.add(tblname);
            hexdump.add(null);

            if (pagenumber_db < job.pages.length && job.pages[pagenumber_db] != null) {
                td = job.pages[pagenumber_db];
                rowid_col = td.rowid_col;
                if ( td instanceof TableDescriptor) {
                    isVT  = td.sql.toUpperCase().contains("CREATE VIRTUAL");
                }

            }

            record.add(Global.REGULAR_RECORD.intern());
            hexdump.add(null);
            record.add(offset > -1 ? offset + "" : cellstart + "");
            hexdump.add(null);

        }
        // case 3: regular db-page
        else if (job.pages[pagenumber_db] != null) {
            td = job.pages[pagenumber_db];
            rowid_col = td.rowid_col;
            if (td instanceof TableDescriptor) {
                td   = (TableDescriptor) td;
                isVT = isVirtualTable(td.getName());
            }
            record.add(td.getName().intern());
            hexdump.add(null);
            record.add(Global.REGULAR_RECORD.intern());
            hexdump.add(null);
            record.add((((pagenumber_db - 1L) * job.ps) + cellstart) + "");
            hexdump.add(null);
        } else {
            unknown = true;
        }

        AppLog.debug("Cellstart for pll: " + (((pagenumber_db - 1L) * job.ps) + cellstart));

        try {
            buffer.position(cellstart);
        } catch (Exception err) {
            AppLog.debug("ERROR: cellstart not in buffer: " + cellstart
                         + " pagenumber_db=" + (pagenumber_db - 1L) + " pagesize=" + job.ps);
            return null;
        }

        int pll = readUnsignedVarInt(buffer);
        AppLog.debug("Payload length: " + pll + " (" + Integer.toHexString(pll) + ")");

        if (pll < 4) return null;

        int rowid = 0;
        if (!withoutROWID) {
            if (unknown) {
                rowid = readUnsignedVarInt(buffer);
            } else if (pagenumber_db < job.pages.length
                       && (job.pages[pagenumber_db] == null || job.pages[pagenumber_db].ROWID)) {
                rowid = readUnsignedVarInt(buffer);
            }
        }

        int phl = readUnsignedVarInt(buffer);
        if (phl == 0) return null;
        phl--;

        maxlength -= phl;
        if (phl == 0) return null;

        int    pp   = buffer.position();
        String hh   = getHeaderString(phl, buffer);
        buffer.position(pp);

        List<SqliteElement> columns = getColumns(phl, buffer);
        if (columns == null) {
            AppLog.debug("No valid header — skip recovery.");
            return null;
        }

        int co = 0;
        try {
            // handling of free list entries
            if (unknown) {
                td = matchTable(columns);
                if (td == null) {
                    record.add(Global.DELETED_RECORD_IN_PAGE.intern());
                    hexdump.add(null);
                }
                else {
                    record.add(td.tblname.intern());
                    hexdump.add(null);
                    job.pages[pagenumber_db] = td;
                    if (td != null) {
                        isVT = isVirtualTable(td.getName());
                    }
                    rowid_col = td.rowid_col;
                    record.add(Global.FREELIST_ENTRY.intern());
                }
                //record.add(Global.REGULAR_RECORD.intern());
                hexdump.add(null);
                record.add((((pagenumber_db - 1L) * job.ps) + cellstart) + "");
                hexdump.add(null);
            }
        } catch (NullPointerException err) {
            AppLog.debug("NPE during table matching: " + err.getMessage());
        }

        boolean error = false;
        int     so    = computePayload(pll);

        if (so < pll) {
            /* Spilled payload */
            int last = buffer.position();
            AppLog.debug("Regular spilled payload: " + so);

            if ((buffer.position() + so - phl - 1) > (buffer.limit() - 4)) return null;
            try {
                buffer.position(buffer.position() + so - phl - 1);
            } catch (Exception err) {
                return null;
            }

            int overflow = buffer.getInt();
            if (overflow < 0) return null;
            AppLog.debug("Regular overflow: " + overflow + " (" + Integer.toHexString(overflow) + ")");

            buffer.position(last);

            byte[] extended;
            if (filetype == Global.WAL_ARCHIVE_FILE) {
                extended = readOverflowIterativ(overflow, true);
            } else {
                extended = readOverflowIterativ(overflow - 1, false);
            }

            byte[] originalbuffer = copyBufferToArray(buffer);
            byte[] c = new byte[pll + job.ps];
            buffer.position(last);

            if (so - phl > 0) {
                System.arraycopy(originalbuffer, buffer.position(), c, 0, so - phl);
            }
            try {
                if (extended != null) {
                    System.arraycopy(extended, 0, c, so - phl - 1, pll - so);
                }
            } catch (ArrayIndexOutOfBoundsException | NullPointerException err) {
                AppLog.debug("Overflow merge error: " + err.getMessage());
            }

            ByteBuffer bf = ByteBuffer.wrap(c);
            bf.position(0);
            int blobcolidx = 0;

            for (SqliteElement en : columns) {
                if (en == null) {
                    record.add("NULL".intern());
                    hexdump.add(null);
                    continue;
                }
                if (rowid_col == co && !withoutROWID) {
                    record.add(rowid + "".intern());
                    hexdump.add(null);
                    co++;
                    continue;
                }

                byte[] value = new byte[en.getlength()];
                if ((bf.limit() - bf.position()) < value.length) {
                    AppLog.debug("Buffer underflow: available=" + (bf.limit() - bf.position())
                                 + " needed=" + value.length);
                }
                try {
                    //hex - representation of the field value
                    bf.get(value);
                } catch (BufferUnderflowException err) {
                    AppLog.debug("readRecord() buffer underflow: " + err);
                    return null;
                }

                blobcolidx = appendValue(record, hexdump, en, value, blobcolidx, isVT, td, co, 2);
                co++;
                if (maxlength <= 0) break;
            }

        } else {
            /* No overflow */
            int blobcolidx = 0;
            for (SqliteElement en : columns) {
                if (en == null) {
                    record.add("NULL".intern());
                    hexdump.add(null);
                    continue;
                }
                if (rowid_col == co && !withoutROWID) {
                    record.add(rowid + "");
                    byte[] bytes = ByteBuffer.allocate(8).putInt(rowid).array();
                    hexdump.add(bytes);
                    co++;
                    continue;
                }

                byte[] value;
                if (maxlength >= en.getlength()) {
                    value = new byte[Math.max(en.getlength(), 0)];
                } else {
                    value = null;
                }
                maxlength -= en.getlength();
                if (value == null) break;

                try {
                    buffer.get(value);
                } catch (BufferUnderflowException err) {
                    AppLog.debug("readRecord() buffer underflow: " + err);
                    return null;
                }

                blobcolidx = appendValue(record, hexdump, en, value, blobcolidx, isVT, td, co, 2);
                co++;
                if (maxlength <= 0) break;
            }
        }

        record.add(1, "[" + pll + "|" + hh.length() / 2 + "]");
        hexdump.add(1,null);
        record.add(2, "" + rowid);
        hexdump.add(2,null);



        if (bs != null) {
            bs.set(cellstart, so < pll ? cellstart + so + 4 : buffer.position());
        }

        if (error) {
            AppLog.error("Spilled overflow page error");
            return null;
        }

        //if (ad == null) {
        //    record.set(0, "fqlite_freelist".intern());
        //}

        return new DataRow(record,hexdump);
    }

    // -------------------------------------------------------------------------
    // Column append helpers (DRY replacements for duplicated BLOB blocks)
    // -------------------------------------------------------------------------

    /**
     * Appends a single column value to {@code record}, handling BLOB detection and
     * BLOB caching. Returns the updated {@code blobcolidx}.
     */
    private int appendColumn(LinkedList<String> record, LinkedList<byte[]> raw, SqliteElement en, byte[] value,
                             int blobcolidx, boolean isVT, int offsetidx) {
        if (en.serial == StorageClass.BLOB) {
            return appendBlobColumn(record, raw, en, value, blobcolidx, isVT, offsetidx);
        }
        record.add(en.toString(value, false, true));
        raw.add(value);
        return blobcolidx;
    }

    /**
     * Like {@link #appendColumn} but additionally handles TIMESTAMP columns using
     * the table descriptor {@code td}.
     */
    private int appendValue(LinkedList<String> record, LinkedList<byte[]> hexdump, SqliteElement en,
                            byte[] value, int blobcolidx, boolean isVT,
                            AbstractDescriptor td, int co, int offsetidx) {
        if (en.serial == StorageClass.BLOB) {
            hexdump.add(value);
            return appendBlobColumn(record, hexdump, en, value, blobcolidx, isVT, offsetidx);
        }

        if (td == null) {
            record.add(en.toString(value, false, true).intern());
            hexdump.add(value);
            return blobcolidx;
        }

        // Check for TIMESTAMP column type
        if (co < td.sqltypes.size()) {

            if(td.sqltypes.size() > 0) {
                String coltype = td.sqltypes.get(co);
                if ("TIMESTAMP".equals(coltype)) {
                    TimeStamp ts = timestamp2String(en, value);
                    if (ts != null) {
                        job.timestamps.put(ts.text, found);
                        record.add(ts.text.intern());
                        hexdump.add(value);
                        return blobcolidx;
                    }
                }
            }
        }

        if (en.type == SerialTypes.PRIMARY_KEY) {
            record.add("null".intern());
            hexdump.add(null);
        } else {
            String vv = en.toString(value, false, true);
            record.add(vv != null ? vv : "null".intern());
            if (vv != null) {
                hexdump.add(value);
            }
            else{
                hexdump.add(null);
            }
        }
        return blobcolidx;
    }

    /**
     * Appends a BLOB column entry to {@code record} and caches the binary data.
     * Returns the updated blob index.
     */
    private int appendBlobColumn(LinkedList<String> record,LinkedList<byte[]> raw, SqliteElement en, byte[] value,
                                 int blobcolidx, boolean isVT, int offsetidx) {
        String text = en.getBLOB(value, !isVT);
        if (!text.isEmpty()) {
            String display = "[BLOB-" + blobcolidx + "] "; // + text;
            //if (!isVT && text.length() > 32) display += "..";
            if (isVT)
                display = "..";
            record.add(display);
            storeBLOB(record, blobcolidx, text, value, offsetidx);
            return blobcolidx + 1;
        }

        record.add("");
        raw.add(null);
        return blobcolidx;
    }

    // -------------------------------------------------------------------------
    // BLOB storage & thumbnail generation
    // -------------------------------------------------------------------------

    private void storeBLOB(LinkedList<String> record, int blobcolidx,
                           String tablecelltext, byte[] value, int offsetidx) {
        long hash = -1;
        if (record.get(offsetidx).length() > 2) {
            try {
                hash = Long.parseLong(record.get(offsetidx));
            } catch (NumberFormatException e) {
                System.out.println("Number format exception: compute hash failed ");
            }
        } else {
            hash = blobcolidx;
        }

        String base  = hash + "-" + blobcolidx;
        saveBLOB(base, tablecelltext, value);

    }

    /**
     * Determines the full path (with extension) for a BLOB, stores it in
     * {@code job.bincache}, and returns the path.
     */
    private void saveBLOB(String base, String text, byte[] value) {

        BinaryFormatDetector.BinaryFormat format = BinaryFormatDetector.detect(value);

        switch (format) {
            case PNG  -> {
                job.bincache.put(base, new BLOBElement(value, BLOBTYPE.PNG));
            }
            case JPEG -> {
                job.bincache.put(base,  new BLOBElement(value, BLOBTYPE.JPG));
            }
            case PDF  -> {
                job.bincache.put(base,  new BLOBElement(value, BLOBTYPE.PDF));
            }
            case GIF  -> {
                job.bincache.put(base,  new BLOBElement(value, BLOBTYPE.GIF));
            }
            case BMP  -> {
                job.bincache.put(base,  new BLOBElement(value, BLOBTYPE.BMP));
            }
            case TIFF  -> {
                job.bincache.put(base,  new BLOBElement(value, BLOBTYPE.TIFF));
            }
            case HEIC  -> {
                job.bincache.put(base,  new BLOBElement(value, BLOBTYPE.HEIC));
            }
            case PLIST  -> {
                job.bincache.put(base,  new BLOBElement(value, BLOBTYPE.PLIST));
            }
            case GZIP  -> {
                job.bincache.put(base,  new BLOBElement(value, BLOBTYPE.GZIP));
            }
            case AVRO  -> {
                job.bincache.put(base,  new BLOBElement(value, BLOBTYPE.AVRO));
            }
            case BIN  -> {
                job.bincache.put(base,  new BLOBElement(value, BLOBTYPE.UNKOWN));
            }
        }
    }

    // -------------------------------------------------------------------------
    // Conversion helpers
    // -------------------------------------------------------------------------

    /**
     * Converts a UTF-8 string to ISO-8859-1 representation.
     *
     * @param s the source string
     * @return converted string, or {@code null} on error
     */
    public static String convertToUTF8(String s) {
        return new String(s.getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1);
    }

    /**
     * Converts a byte array value to a semicolon-prefixed StringBuffer entry.
     *
     * @param col   column index (separator is prepended when col > 0)
     * @param en    the SQLite element descriptor
     * @param value the raw bytes
     * @return formatted StringBuffer
     */
    public StringBuffer write(int col, SqliteElement en, byte[] value) {
        StringBuffer val = new StringBuffer();
        if (col > 0) val.append(";");
        val.append(en.toString(value, false, true));
        return val;
    }

    // -------------------------------------------------------------------------
    // Timestamp conversion
    // -------------------------------------------------------------------------

    /**
     * Converts a 64-bit integer string representing various timestamp formats
     * (UNIX seconds/millis/micros/nanos, CFAbsoluteTime seconds/nanos) into a
     * human-readable UTC string.
     *
     * @param time the integer value as a string
     * @return formatted timestamp, or empty string on failure
     */
    public static String int2Timestamp(String time) {
        if (time == null || time.isEmpty() || "null".equals(time)) {
            return "";
        }

        long l;
        try {
            l = Long.parseLong(time);
        } catch (NumberFormatException err) {
            return "";
        }

        if (l > UNIX_MIN_SECONDS && l < UNIX_MAX_SECONDS) {
            return formatEpochSecond(l);
        }
        if (l > MAC_MIN_DATE && l < MAC_MAX_DATE) {
            return formatEpochSecond(MAC_EPOCH_OFFSET + l);
        }
        if (l > MAC_NANO_MIN_DATE && l < MAC_NANO_MAX_DATE) {
            return formatEpochSecond(MAC_EPOCH_OFFSET + l / 1_000_000_000L);
        }
        if (l > UNIX_MIN_DATE && l < UNIX_MAX_DATE) {
            return formatEpochMilli(l);
        }
        if (l > UNIX_MIN_DATE_NANO && l < UNIX_MAX_DATE_NANO) {
            return formatEpochMilli(l / 1_000L);
        }
        if (l > UNIX_MIN_DATE_PICO && l < UNIX_MAX_DATE_PICO) {
            return formatEpochMilli(l / 1_000_000L);
        }
        // WebKit / Chrome: microseconds since 1601-01-01 (Windows FILETIME epoch)
        if (l > WEBKIT_MIN_DATE && l < WEBKIT_MAX_DATE) {
            return formatEpochMilli((l / 1_000L) - (WEBKIT_EPOCH_OFFSET * 1_000L));
        }
        return "";
    }

    /**
     * Konvertiert einen Unix-Timestamp der Form "1682694647.05921500"
     * (Sekunden.Nanosekunden) in eine lesbare Zeitangabe.
     *
     * @param raw  z.B. "1682694647.05921500"
     * @return     z.B. "2023-04-28  17:30:47.059"
     */
    public static String convertTimestamp(String raw) {
        if (raw == null || raw.isBlank())
            return "";

        try {
            String[] parts = raw.split("\\.");

            long seconds = Long.parseLong(parts[0]);
            long nanos   = 0;

            if (parts.length > 1) {
                // Auf 9 Stellen normalisieren (Nanosekunden)
                String nanoPart = String.format("%-9s", parts[1]).replace(' ', '0');
                nanos = Long.parseLong(nanoPart.substring(0, 9));
            }

            Instant instant = Instant.ofEpochSecond(seconds, nanos);
            LocalDateTime dt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

            return dt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd  HH:mm:ss.SSS"));

        } catch (NumberFormatException | DateTimeException e) {
            return "invalid timestamp: " + raw;
        }
    }


    private static String formatEpochSecond(long epochSecond) {
        return ZonedDateTime.ofInstant(Instant.ofEpochSecond(epochSecond), ZoneOffset.UTC)
                .format(TIMESTAMP_FORMATTER);
    }

    private static String formatEpochMilli(long epochMilli) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC)
                .format(TIMESTAMP_FORMATTER);
    }

    /**
     * Converts a typed SQLite element value into a {@link TimeStamp}.
     *
     * @param en    element descriptor (determines the integer width)
     * @param value raw bytes from the database
     * @return a {@link TimeStamp}, or {@code null} when the value cannot be parsed
     */
    private TimeStamp timestamp2String(SqliteElement en, byte[] value) {
        try {
            if (en.type == SerialTypes.INT48) {
                long l    = SqliteElement.decodeInt48ToLong(value) / 100;
                long time = MAC_EPOCH_OFFSET + l;
                return new TimeStamp(formatEpochSecond(time), l);

            } else if (en.type == SerialTypes.INT64) {
                long l = ByteBuffer.wrap(value).getLong();
                return new TimeStamp(formatEpochSecond(l), l);

            } else if (en.type == SerialTypes.FLOAT64) {
                double d    = ByteBuffer.wrap(value).getDouble();
                long   time = MAC_EPOCH_OFFSET + (long) d;
                return new TimeStamp(formatEpochSecond(time), d);

            } else if (en.type == SerialTypes.INT32) {
                int  l    = ByteBuffer.wrap(value).getInt();
                long time = MAC_EPOCH_OFFSET + (long) l;
                return new TimeStamp(formatEpochSecond(time), l);
            }
        } catch (Exception err) {
            AppLog.debug("DateTimeException in timestamp2String: " + err.getMessage());
        }
        return new TimeStamp("null", 0);
    }


    // -------------------------------------------------------------------------
    // Hash / export helpers
    // -------------------------------------------------------------------------

    /**
     * Computes a hash over the data columns of a record (skipping the first 5
     * metadata columns).
     */
    public static int computeHash(LinkedList<String> record) {
        return record.subList(5, record.size()).hashCode();
    }

    // -------------------------------------------------------------------------
    // Table matching
    // -------------------------------------------------------------------------

    private TableDescriptor matchTable(List<SqliteElement> header) {
        for (TableDescriptor table : job.headers) {
            if (table.getColumntypes().size() != header.size()) continue;

            boolean eq  = true;
            int     idx = 0;
            for (SqliteElement s : header) {
                if (!s.serial.name().equals(table.getColumntypes().get(idx))) {
                    eq = false;
                    break;
                }
                idx++;
            }
            if (eq) return table;
        }
        return null;
    }


    // -------------------------------------------------------------------------
    // Overflow page reading
    // -------------------------------------------------------------------------

    /**
     * Recursively reads all overflow pages that form a linked list starting at
     * {@code pagenumber} and concatenates their payloads.
     * <p>
     * Each overflow page begins with a 4-byte big-endian pointer to the next page
     * (0 = last page), followed by payload bytes.
     *
     * @param job        current recovery job
     * @param pagenumber 0-based page number of the first overflow page
     * @return all overflow payload bytes concatenated
     */
    /**
     * Reads all overflow pages iteratively (cycle-safe, no stack-overflow risk).
     * Replaces the former recursive implementation.
     *
     * @param job        current recovery job
     * @param pagenumber 0-based page number of the first overflow page
     * @return all overflow payload bytes concatenated
     */
    public static byte[] readOverflow(Job job, int pagenumber) {
        List<byte[]>    parts   = new LinkedList<>();
        Set<Integer>    visited = new HashSet<>();
        int             next    = pagenumber;

        while (true) {
            if (next < 0 || next >= job.numberofpages) break;
            if (visited.contains(next)) {
                AppLog.debug("readOverflow: cycle detected at page " + next + " — stopping.");
                break;
            }
            visited.add(next);

            ByteBuffer overflowpage = job.readPageWithNumber(next, job.ps);
            if (overflowpage == null) break;

            overflowpage.position(0);
            int nextRaw = overflowpage.getInt();

            byte[] current = new byte[job.ps - 4];
            overflowpage.position(4);
            overflowpage.get(current, 0, job.ps - 4);
            parts.add(current);

            if (nextRaw == 0) break;
            next = nextRaw - 1;
        }

        if (parts.isEmpty()) return new byte[0];
        int total = parts.stream().mapToInt(b -> b.length).sum();
        byte[] merged = new byte[total];
        int off = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, merged, off, part.length);
            off += part.length;
        }
        return merged;
    }

    /**
     * Iterative equivalent of {@link #readOverflow} — avoids stack-overflow risk
     * for very long overflow chains, and supports WAL-file overflow pages.
     */
    private byte[] readOverflowIterativ(int pagenumber, boolean fromWAL) {
        List<ByteBuffer> parts   = new LinkedList<>();
        Set<Integer>     visited = new HashSet<>();
        int              next    = pagenumber;
        int              saved   = -1;
        int              frame   = -1;

        if (fromWAL) {
            saved = job.wal.wal.position();
            frame = saved / (job.ps + 24) + 1;
        }

        while (true) {
            ByteBuffer overflowpage;

            if (visited.contains(next)) {
                AppLog.debug("readOverflowIterativ: cycle detected at page " + next + " — stopping.");
                break;
            }

            if (fromWAL) {
                overflowpage = job.readWALOverflowPage(frame, next, job.ps, pagenumber);
            } else {
                overflowpage = job.readPageWithNumber(next, job.ps);
            }
            visited.add(next);

            if (overflowpage == null) break;

            overflowpage.position(0);
            int nextRaw = overflowpage.getInt();
            next = fromWAL ? nextRaw : nextRaw - 1;

            byte[] current = new byte[job.ps - 4];
            overflowpage.position(4);
            overflowpage.get(current, 0, job.ps - 4);
            parts.add(ByteBuffer.wrap(current));

            boolean more = fromWAL ? (nextRaw > 0) : (next >= 0);
            if (!more) break;
        }

        if (fromWAL && saved > 0) {
            job.wal.wal.position(saved);
        }

        if (parts.isEmpty()) {
            return ByteBuffer.allocate(0).array();
        } else if (parts.size() == 1) {
            return parts.get(0).array();
        } else {
            ByteBuffer full = ByteBuffer.allocate(parts.stream().mapToInt(Buffer::capacity).sum());
            parts.forEach(full::put);
            full.flip();
            return full.array();
        }
    }

    // -------------------------------------------------------------------------
    // Payload length / spill computation
    // -------------------------------------------------------------------------

    /**
     * Computes the total payload length (header + data) from the hex-encoded
     * header string.
     *
     * @param header hex-encoded SQLite record header (without the header-length byte)
     * @return total number of bytes in the record
     */
    public int computePayloadLength(String header) {
        byte[]   bcol   = Auxiliary.decode(header);
        VIntIter columns = VIntIter.wrap(bcol, 1);
        int      pll    = header.length() / 2 + 1;

        while (columns.hasNext()) {
            pll += serialTypeByteLength(columns.next());
        }
        return pll;
    }

    /**
     * Static variant of {@link #computePayloadLength} operating on a hex string.
     */
    public static int computePayloadLengthS(String header) {
        return computePayloadLengthByte(Auxiliary.decode(header), header.length() / 2 + 1);
    }

    /**
     * Computes the payload byte length from raw header bytes plus a pre-calculated
     * header offset.
     */
    public static int computePayloadLengthByte(byte[] bcol, int hoffset) {
        int[] columns = readVarInt(bcol);
        int   pll     = hoffset;
        for (int col : columns) {
            pll += serialTypeByteLength(col);
        }
        return pll;
    }

    /** Returns the data byte length for a given SQLite serial type code. */
    private static int serialTypeByteLength(int serialType) {
        switch (serialType) {
            case 0: case 8: case 9: case 10: case 11: return 0;
            case 1: return 1;
            case 2: return 2;
            case 3: return 3;
            case 4: return 4;
            case 5: return 6;
            case 6: case 7: return 8;
            default:
                return (serialType % 2 == 0)
                        ? (serialType - 12) / 2   // BLOB
                        : (serialType - 13) / 2;  // STRING
        }
    }

    /**
     * Computes how many payload bytes fit on the primary B-tree page (before
     * spilling to overflow pages).
     *
     * @param p total payload size
     * @return bytes stored on the primary page
     */
    private int computePayload(int p) {
        int u = job.ps;
        int x = u - 35;
        int m = ((u - 12) * 32 / 255) - 23;
        int k = m + ((p - m) % (u - 4));

        if (p <= x)           return p;
        if (k <= x)           return k;
        return m;
    }


    // -------------------------------------------------------------------------
    // Header / column parsing
    // -------------------------------------------------------------------------

    /**
     * Returns the sum of all column payload lengths for the given hex header string.
     */
    public static int getPayloadLength(String header) {
        int sum = 0;
        List<SqliteElement> cols = toColumns(header);
        for (SqliteElement e : cols) {
            if (e != null) sum += e.getlength();
        }
        return sum;
    }

    /**
     * Converts a hex-encoded header string into a list of {@link SqliteElement}s.
     */
    public static List<SqliteElement> toColumns(String header) {
        return get(decode(header));
    }

    private static SqliteElement[] masterRecordToColumns(String header) {
        if (header.startsWith("07") || header.startsWith("06")) {
            header = header.substring(2);
        }
        return getMaster(decode(header));
    }

    /**
     * Reads {@code headerlength} bytes from {@code buffer} and returns them as a
     * hex string. The buffer position is advanced accordingly.
     */
    public String getHeaderString(int headerlength, ByteBuffer buffer) {
        if (headerlength <= 0) return "";
        byte[] header = new byte[headerlength];
        try {
            buffer.get(header);
        } catch (Exception err) {
            AppLog.debug("getHeaderString error: " + err);
            return "";
        }
        return bytesToHex3(header);
    }

    /**
     * Reads {@code headerlength} bytes from {@code buffer} and parses them into
     * a list of {@link SqliteElement} column descriptors.
     *
     * @param headerlength number of header bytes to read (must be &gt; 0 and &le; 1024)
     * @param buffer       source buffer (position is advanced)
     * @return column list, or {@code null} on error
     * @throws IOException on buffer errors
     */
    public List<SqliteElement> getColumns(int headerlength, ByteBuffer buffer) throws IOException {
        if (headerlength < 0 || headerlength > 1024) return null;

        byte[] header = new byte[headerlength];
        try {
            buffer.get(header);
        } catch (Exception err) {
            AppLog.debug("getColumns error: " + err + " headerlength=" + headerlength
                         + " capacity=" + buffer.capacity());
            return null;
        }
        return get(header);
    }

    private static SqliteElement[] getMaster(byte[] header) {
        int[] columns = readMasterHeaderVarInts(header);
        return (columns == null) ? null : getElements(columns);
    }

    private static List<SqliteElement> get(byte[] header) {
        return getElements(VIntIter.wrap(header, 3));
    }

    private static SqliteElement[] getElements(int[] columns) {
        SqliteElement[] result = new SqliteElement[columns.length];
        for (int i = 0; i < columns.length; i++) {
            result[i] = toSqliteElement(columns[i]);
        }
        return result;
    }

    private static List<SqliteElement> getElements(VIntIter columns) {
        LinkedList<SqliteElement> result = new LinkedList<>();
        while (columns.hasNext()) {
            result.add(toSqliteElement(columns.next()));
        }
        columns.setBack();
        return result;
    }

    /** Maps a single SQLite serial type code to a (possibly cached) SqliteElement. */
    private static SqliteElement toSqliteElement(int type) {
        switch (type) {
            case 0:  return PRIMKEY;
            case 1:  return INT8;
            case 2:  return INT16;
            case 3:  return INT24;
            case 4:  return INT32;
            case 5:  return INT48;
            case 6:  return INT64;
            case 7:  return FLOAT64;
            case 8:  return CONST0;
            case 9:  return CONST1;
            case 10: case 11: return null;
            default:
                return (type % 2 == 0)
                        ? new SqliteElement(SerialTypes.BLOB,   StorageClass.BLOB, (type - 12) / 2)
                        : new SqliteElement(SerialTypes.STRING, StorageClass.TEXT, (type - 13) / 2);
        }
    }


    // -------------------------------------------------------------------------
    // VarInt reading
    // -------------------------------------------------------------------------

    /**
     * Reads a single unsigned variable-length integer from {@code buffer}.
     * Each byte contributes 7 bits; the MSB signals whether more bytes follow.
     *
     * @param buffer source buffer (position is advanced past the varint)
     * @return decoded integer value
     */
    public static int readUnsignedVarInt(ByteBuffer buffer) {
        byte b     = buffer.get();
        int  value = b & 0x7F;
        int  count = 1;
        while ((b & 0x80) != 0 && count < 9 && buffer.hasRemaining()) {
            b      = buffer.get();
            value  <<= 7;
            value  |= (b & 0x7F);
            count++;
        }
        return value;
    }

    /**
     * Reads all variable-length integers contained in {@code values}.
     *
     * @param values byte array potentially containing multiple packed varints
     * @return array of decoded integer values
     */
    public static int[] readVarInt(byte[] values) {
        int   position  = 0;
        int   limit     = values.length;
        List<Integer> resultlist = new ArrayList<>();

        do {
            if (position < limit) {
                byte b     = values[position++];
                int  value = b & 0x7F;
                int  count = 1;
                while ((b & 0x80) != 0 && position < limit && count < 9) {
                    b      = values[position++];
                    value  <<= 7;
                    value  |= (b & 0x7F);
                    count++;
                }
                resultlist.add(value);
            }
        } while (position < limit);

        return resultlist.stream().mapToInt(Integer::intValue).toArray();
    }

    public static int[] readMasterHeaderVarInts(byte[] values) {
        return Arrays.copyOfRange(readVarInt(values), 0, 5);
    }


    public static int varintHexString2Integer(String s) {
        return readUnsignedVarInt(ByteBuffer.wrap(hexStringToByteArray(s)));
    }

    // -------------------------------------------------------------------------
    // Hex / byte conversion utilities
    // -------------------------------------------------------------------------

    /** Converts a 4-byte integer to a hex string. */
    public static String intToHex(int i) {
        return bytesToHex3(new byte[]{
                (byte) (i >>> 24), (byte) (i >>> 16), (byte) (i >>> 8), (byte) i
        });
    }

    /** @deprecated Use {@link #intToHex(int)} instead. */
    @Deprecated
    public static String Int2Hex(int i) { return intToHex(i); }

    /** Converts a ByteBuffer to its hex-string representation. */
    public static String bytesToHex2(ByteBuffer bb) {
        int limit = bb.limit();
        if (limit <= 0) return null;

        char[] hexChars = new char[limit * 2];
        bb.position(0);
        int counter = 0;
        while (bb.position() < limit) {
            int v = bb.get() & 0xFF;
            hexChars[counter * 2]     = HEX_ARRAY[v >>> 4];
            hexChars[counter * 2 + 1] = HEX_ARRAY[v & 0x0F];
            counter++;
        }
        return new String(hexChars);
    }

    /** Converts a byte array to its hex-string representation. */
    public static String bytesToHex3(byte[] bytes) {
        return bytesToHex2(ByteBuffer.wrap(bytes));
    }

    /** Converts a single byte to a 2-char hex string. */
    public static String byteToHex(byte b) {
        return bytesToHex3(new byte[]{b});
    }

    /**
     * Converts a range of a byte array to a hex string.
     *
     * @param bytes   source array
     * @param fromidx start index (inclusive)
     * @param toidx   end index (exclusive)
     */
    public static String bytesToHex1(byte[] bytes, int fromidx, int toidx) {
        char[] hexChars = new char[(toidx - fromidx) * 2];
        for (int j = fromidx; j < toidx; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[(j - fromidx) * 2]     = HEX_ARRAY[v >>> 4];
            hexChars[(j - fromidx) * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    /**
     * Decodes a lowercase hex string into a byte array.
     * The string length must be even.
     *
     * @param s even-length hex string
     * @return decoded byte array
     */
    public static byte[] hexStringToByteArray(String s) {
        int    len  = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                                  + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    /**
     * Decodes a lowercase hex string into a byte array.
     *
     * @deprecated Use {@link #hexStringToByteArray(String)} instead.
     */
    @Deprecated
    public static byte[] decode(String s) {
        return hexStringToByteArray(s);
    }

    /**
     * Converts a 2-character hex string to its integer page-type value.
     * Delegates to {@link #getPageType(String)}.
     */
    public static int twoByteBufferToInt(ByteBuffer b) {
        byte[] ret = new byte[]{0, 0, b.get(), b.get()};
        return ByteBuffer.wrap(ret).getInt();
    }

    /** @deprecated Use {@link #twoByteBufferToInt(ByteBuffer)} instead. */
    @Deprecated
    public static int TwoByteBuffertoInt(ByteBuffer b) { return twoByteBufferToInt(b); }

    // -------------------------------------------------------------------------
    // ASCII / text helpers
    // -------------------------------------------------------------------------

    /**
     * Extracts the printable ASCII portion of a hex dump string, replacing
     * non-printable bytes with '.'.
     */
    public static String hex2ASCII(String hex) {
        hex = hex.replace("..", "");

        int    idx  = hex.indexOf("] ");
        String tail = hex.substring(idx + 2);

        int gtIdx = hex.indexOf(">");
        if (gtIdx > 0) {
            tail = hex.substring(gtIdx + 1);
        }

        return hexToPrintableAscii(tail);
    }

    /** Converts a raw hex string to printable ASCII, replacing non-printables with '.'. */
    public static String hex2ASCII_v2(String hex) {
        return hexToPrintableAscii(hex);
    }

    private static String hexToPrintableAscii(String hex) {
        StringBuilder output = new StringBuilder();
        for (int i = 0; i < hex.length(); i += 2) {
            char next = (char) Integer.parseInt(hex.substring(i, i + 2), 16);
            output.append((next > 31 && next < 127) ? next : '.');
        }
        return output.toString();
    }

    // -------------------------------------------------------------------------
    // OS detection
    // -------------------------------------------------------------------------

    public static boolean isWindowsSystem() {
        return System.getProperty("os.name").contains("Windows");
    }

    public static boolean isMacOS() {
        return System.getProperty("os.name").contains("Mac");
    }

    // -------------------------------------------------------------------------
    // BLOB disk I/O
    // -------------------------------------------------------------------------

    /**
     * Writes a cached BLOB entry to disk at the given path.
     *
     * @param job  current recovery job (provides the binary cache)
     * @param path target file path (also used as cache key)
     * @return {@code true} on success, {@code false} on any error
     */
    public static boolean writeBLOB2Disk(Job job, String path) {
        try {
            BLOBElement e = job.bincache.get(path);
            try (BufferedOutputStream buffer =
                         new BufferedOutputStream(new FileOutputStream(path))) {
                buffer.write(e.binary);
            }
            return true;
        } catch (Exception err) {
            AppLog.debug("writeBLOB2Disk error for " + path + ": " + err.getMessage());
            return false;
        }
    }

    // -------------------------------------------------------------------------
    // Entropy
    // -------------------------------------------------------------------------

    /**
     * Calculates the Shannon entropy (bits per byte) of the given byte array.
     *
     * @param input byte array to measure
     * @return entropy in bits per byte, or 0.0 for empty input
     */
    public static double entropy(byte[] input) {
        if (input.length == 0) return 0.0;

        int[] charCounts = new int[256];
        for (byte b : input) charCounts[b & 0xFF]++;

        double entropy = 0.0;
        for (int count : charCounts) {
            if (count == 0) continue;
            double freq = (double) count / input.length;
            entropy -= freq * (Math.log(freq) / Math.log(2));
        }
        return entropy;
    }

    // -------------------------------------------------------------------------
    // Index pattern helpers
    // -------------------------------------------------------------------------

    /**
     * Creates and attaches a matching {@link HeaderPattern} to the given
     * {@link IndexDescriptor}.
     *
     * @param id the index descriptor to configure
     */
    public static void addHeadPattern2Idx(IndexDescriptor id) {
        HeaderPattern  pattern = new HeaderPattern();
        pattern.addHeaderConstraint(id.columnnames.size() + 1, id.columnnames.size() + 1);

        for (String coltype : id.columntypes) {
            switch (coltype) {
                case "INT":     pattern.add(new IntegerConstraint(false)); break;
                case "TEXT":    pattern.addStringConstraint();             break;
                case "BLOB":    pattern.addBLOBConstraint();               break;
                case "REAL":    pattern.addFloatingConstraint();           break;
                case "NUMERIC": pattern.addNumericConstraint();            break;
                default:
                    AppLog.debug("Unknown column type in index pattern: " + coltype);
            }
        }
        id.hpattern = pattern;
    }

    // -------------------------------------------------------------------------
    // Miscellaneous
    // -------------------------------------------------------------------------

    /** Returns {@code true} if {@code tblname} refers to a virtual-table shadow table. */
    private boolean isVirtualTable(String tblname) {
        int p = tblname.indexOf("_node");
        if (p > 0) {
            return job.virtualTables.containsKey(tblname.substring(0, p));
        }
        return false;
    }

    /** Returns the N-th zero-based occurrence of {@code ch} in {@code str}, or -1. */
    public static int findNthOccur(String str, char ch, int N) {
        int occur = 0;
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == ch && ++occur == N) return i;
        }
        return -1;
    }

    static boolean contains(ByteBuffer bb, String searchText) {
        return new String(bb.array()).contains(searchText);
    }

    /** Prints the current stack trace to the application log. */
    public static void printStackTrace() {
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        StringBuilder sb = new StringBuilder("Stack trace:\n");
        for (int i = 1; i < elements.length; i++) {
            StackTraceElement s = elements[i];
            sb.append("\tat ").append(s.getClassName()).append(".")
                    .append(s.getMethodName()).append("(")
                    .append(s.getFileName()).append(":").append(s.getLineNumber()).append(")\n");
        }
        AppLog.debug(sb.toString());
    }

    // -------------------------------------------------------------------------
    // Private buffer utility
    // -------------------------------------------------------------------------

    /** Copies the entire backing array of a ByteBuffer into a fresh byte[]. */
    private byte[] copyBufferToArray(ByteBuffer buffer) {
        byte[] result = new byte[job.ps];
        for (int i = 0; i < job.ps; i++) {
            result[i] = buffer.get(i);
        }
        return result;
    }

    /**
     * Parst einen Hex-Dump im klassischen Format
     *   "XXXXXXXX  HH HH HH ...  |ASCII|"
     * und gibt die rohen Bytes zurück.
     *
     * Funktioniert mit der Ausgabe von HexDump.dump() sowie
     * mit den meisten anderen Standard-Hex-Dump-Formaten.
     */
    public static byte[] fromHexDump(String hexDump) {
        java.util.List<Byte> bytes = new java.util.ArrayList<>();

        for (String line : hexDump.split("\\R")) {       // \\R = alle Zeilenenden
            // Offset-Teil (bis zum ersten Leerzeichen nach der Adresse) abschneiden
            int hexStart = line.indexOf(' ');
            if (hexStart < 0) continue;

            // Alles ab dem '|' (ASCII-Sidebar) ignorieren
            int sidebarPos = line.indexOf('|');
            String hexPart = sidebarPos >= 0
                    ? line.substring(hexStart, sidebarPos)
                    : line.substring(hexStart);

            // Alle Hex-Paare (genau 2 Hex-Ziffern) extrahieren
            java.util.regex.Matcher m =
                    java.util.regex.Pattern.compile("\\b([0-9A-Fa-f]{2})\\b")
                            .matcher(hexPart);
            while (m.find()) {
                bytes.add((byte) Integer.parseInt(m.group(1), 16));
            }
        }

        // List<Byte> → byte[]
        byte[] result = new byte[bytes.size()];
        for (int i = 0; i < result.length; i++) result[i] = bytes.get(i);
        return result;
    }

}
