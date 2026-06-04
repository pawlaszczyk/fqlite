package fqlite.sqlcipher;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.logging.Logger;

/**
 * Forensic decryptor for SQLCipher rollback journal files ({@code database.db-journal}).
 *
 * <p>A rollback journal is created in DELETE journal mode (the classic SQLite mode
 * before WAL). It contains the ORIGINAL page data from BEFORE a transaction began,
 * allowing SQLite to roll back if the transaction fails.
 *
 * <p>Forensically, the journal contains an earlier state of the database —
 * pages that were about to be modified or deleted. This makes it extremely
 * valuable: it may contain data that has since been overwritten in the main file.
 *
 * <h3>Journal file layout</h3>
 * <pre>
 *   [Journal Header: 512 bytes  – plaintext]
 *     Offset  0: magic number (8 bytes): 0xd9d505f920a163d7
 *     Offset  8: page count   (4 bytes): -1 if not yet committed
 *     Offset 12: nonce        (4 bytes)
 *     Offset 16: initial DB page count (4 bytes)
 *     Offset 20: sector size  (4 bytes)
 *     Offset 24: page size    (4 bytes)
 *
 *   [Record 1]
 *     Page number  (4 bytes, big-endian)
 *     Page data    (PAGE_SIZE bytes, AES-CBC encrypted)
 *     Checksum     (4 bytes)
 *   [Record 2] ...
 * </pre>
 *
 * <p>Note: a journal may contain multiple sections separated by additional
 * 512-byte headers (if the journal was written in multiple passes). This
 * implementation handles single-section journals; multi-section journals
 * are uncommon in practice.
 */
public class SQLCipherJournalDecryptor {

    private static final Logger LOG =
            Logger.getLogger(SQLCipherJournalDecryptor.class.getName());

    // Journal format constants
    public static final int JOURNAL_HEADER_SIZE  = 512;
    public static final int RECORD_PAGE_NO_SIZE  = 4;
    public static final int RECORD_CHECKSUM_SIZE = 4;

    // Magic number at the start of a journal header
    private static final byte[] JOURNAL_MAGIC = {
        (byte)0xd9, (byte)0xd5, (byte)0x05, (byte)0xf9,
        (byte)0x20, (byte)0xa1, (byte)0x63, (byte)0xd7
    };

    // Offset of page size field within journal header
    private static final int JOURNAL_PAGE_SIZE_OFFSET = 24;

    private SQLCipherJournalDecryptor() { /* Utility class */ }

    // ── Public API ────────────────────────────────────────────────────────

    /**
     * Decrypts a rollback journal to a forensically intact copy.
     * The journal header and record page-number/checksum fields are kept
     * as-is; only the page data within each record is decrypted.
     *
     * @param journalFile  source journal ({@code database.db-journal})
     * @param outputFile   destination for the decrypted journal
     * @param keys         derived keys from the main database
     */
    public static void decryptToFile(File journalFile,
                                     File outputFile,
                                     SQLCipherForensicDecryptor.DerivedKeys keys)
            throws Exception {

        validateJournal(journalFile);
        int pageSize = readPageSizeFromJournal(journalFile);

        LOG.info(String.format("Journal: %s  (%,d bytes, page size %d)",
                journalFile.getName(), journalFile.length(), pageSize));

        try (FileChannel src = FileChannel.open(journalFile.toPath(),
                                                StandardOpenOption.READ);
             FileChannel dst = FileChannel.open(outputFile.toPath(),
                                                StandardOpenOption.WRITE,
                                                StandardOpenOption.CREATE,
                                                StandardOpenOption.TRUNCATE_EXISTING)) {

            // Copy journal header as-is (512 bytes, plaintext)
            ByteBuffer headerBuf = ByteBuffer.allocate(JOURNAL_HEADER_SIZE);
            src.read(headerBuf);
            headerBuf.flip();
            dst.write(headerBuf);

            int recordNum = 0;
            int recordSize = RECORD_PAGE_NO_SIZE + pageSize + RECORD_CHECKSUM_SIZE;
            ByteBuffer recordBuf = ByteBuffer.allocate(recordSize);

            while (src.position() + recordSize <= src.size()) {
                recordBuf.clear();
                int read = src.read(recordBuf);
                if (read < recordSize) break;
                recordBuf.flip();

                // Record layout: [pageNo(4)] [pageData(pageSize)] [checksum(4)]
                byte[] pageNoBytes  = new byte[RECORD_PAGE_NO_SIZE];
                byte[] pageData     = new byte[pageSize];
                byte[] checksumBytes = new byte[RECORD_CHECKSUM_SIZE];

                recordBuf.get(pageNoBytes);
                recordBuf.get(pageData);
                recordBuf.get(checksumBytes);

                int pageNo = ByteBuffer.wrap(pageNoBytes).getInt();
                recordNum++;

                // Decrypt page data
                byte[] decrypted = decryptJournalPage(pageData, keys.encKey(), pageSize);

                // Write record with decrypted page data
                dst.write(ByteBuffer.wrap(pageNoBytes));
                dst.write(ByteBuffer.wrap(decrypted));
                dst.write(ByteBuffer.wrap(checksumBytes));

                LOG.fine(String.format("  Record %d: page %d decrypted", recordNum, pageNo));
            }

            LOG.info(String.format("Journal decrypted: %d records written to %s",
                    recordNum, outputFile.getName()));
        }
    }

    /**
     * Extracts all journal records as a map of {@code pageNumber → decrypted page bytes}.
     *
     * <p>The journal contains the BEFORE-image of each page — the state
     * the page was in BEFORE the interrupted transaction. This is the
     * forensically interesting content.
     *
     * @return map: page number → decrypted before-image page data
     */
    public static Map<Integer, byte[]> extractPages(
            File journalFile,
            SQLCipherForensicDecryptor.DerivedKeys keys) throws Exception {

        validateJournal(journalFile);
        int pageSize = readPageSizeFromJournal(journalFile);
        Map<Integer, byte[]> pages = new LinkedHashMap<>();

        try (FileChannel src = FileChannel.open(journalFile.toPath(),
                                                StandardOpenOption.READ)) {
            src.position(JOURNAL_HEADER_SIZE);

            int recordSize = RECORD_PAGE_NO_SIZE + pageSize + RECORD_CHECKSUM_SIZE;
            ByteBuffer recordBuf = ByteBuffer.allocate(recordSize);

            while (src.position() + recordSize <= src.size()) {
                recordBuf.clear();
                if (src.read(recordBuf) < recordSize) break;
                recordBuf.flip();

                byte[] pageNoBytes = new byte[RECORD_PAGE_NO_SIZE];
                byte[] pageData    = new byte[pageSize];
                recordBuf.get(pageNoBytes);
                recordBuf.get(pageData);
                // skip checksum

                int    pageNo    = ByteBuffer.wrap(pageNoBytes).getInt();
                byte[] decrypted = decryptJournalPage(pageData, keys.encKey(), pageSize);
                pages.put(pageNo, decrypted);
            }
        }

        LOG.info(String.format("Journal: extracted %d page before-images from %s",
                pages.size(), journalFile.getName()));
        return pages;
    }

    // ── Page decryption ───────────────────────────────────────────────────

    /**
     * Decrypts a single journal page.
     * Layout is identical to main database pages (AES-CBC, IV at end).
     */
    private static byte[] decryptJournalPage(byte[] pageData,
                                             byte[] encKey,
                                             int pageSize) throws Exception {
        int reserved    = SQLCipherForensicDecryptor.RESERVED_SIZE;
        int contentSize = pageSize - reserved;

        byte[] encrypted     = Arrays.copyOfRange(pageData, 0, contentSize);
        byte[] iv            = Arrays.copyOfRange(pageData, contentSize,
                                                  contentSize + SQLCipherForensicDecryptor.IV_SIZE);
        byte[] reservedBytes = Arrays.copyOfRange(pageData, contentSize, pageSize);

        Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE,
                new SecretKeySpec(encKey, "AES"),
                new IvParameterSpec(iv));
        byte[] decrypted = cipher.doFinal(encrypted);

        byte[] result = new byte[pageSize];
        System.arraycopy(decrypted,     0, result, 0,           decrypted.length);
        System.arraycopy(reservedBytes, 0, result, contentSize, reservedBytes.length);
        return result;
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static int readPageSizeFromJournal(File f) throws IOException {
        try (DataInputStream dis = new DataInputStream(new FileInputStream(f))) {
            dis.skipBytes(JOURNAL_PAGE_SIZE_OFFSET);
            int pageSize = dis.readInt();
            return (pageSize == 0) ? SQLCipherForensicDecryptor.PAGE_SIZE : pageSize;
        }
    }

    private static void validateJournal(File f) throws IOException {
        if (!f.exists())
            throw new FileNotFoundException("Journal file not found: " + f);
        if (f.length() < JOURNAL_HEADER_SIZE)
            throw new IOException("File too small to be a journal: " + f);
    }
}
