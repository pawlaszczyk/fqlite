package fqlite.sqlcipher;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.logging.Logger;

/**
 * Forensic decryptor for SQLCipher WAL (Write-Ahead Log) files.
 *
 * <p>A WAL file ({@code database.db-wal}) contains page frames that have been
 * written but not yet checkpointed back into the main database file. It is
 * encrypted with the same key as the main database.
 *
 * <h3>WAL file layout</h3>
 * <pre>
 *   [WAL Header       :  32 bytes  – plaintext]
 *   [Frame 1 Header   :  24 bytes  – plaintext (page number, checksum, ...)]
 *   [Frame 1 Page Data: PAGE_SIZE  – AES-CBC encrypted, same layout as main DB]
 *   [Frame 2 Header   :  24 bytes]
 *   [Frame 2 Page Data: PAGE_SIZE]
 *   ...
 * </pre>
 *
 * <h3>Output options</h3>
 * <ul>
 *   <li>{@link #decryptToFile}  – writes a decrypted copy of the raw WAL file
 *       (all frames, headers intact, pages decrypted)</li>
 *   <li>{@link #extractPages}   – extracts individual page data as a Map
 *       (page number → latest decrypted page bytes)</li>
 *   <li>{@link #applyToDatabase} – applies WAL pages on top of a decrypted
 *       main database, producing a fully up-to-date SQLite file</li>
 * </ul>
 */
public class SQLCipherWalDecryptor {

    private static final Logger LOG =
            Logger.getLogger(SQLCipherWalDecryptor.class.getName());

    // WAL format constants (SQLite specification)
    public static final int WAL_HEADER_SIZE   = 32;
    public static final int FRAME_HEADER_SIZE = 24;

    // Offsets within the WAL header
    private static final int WAL_MAGIC_OFFSET     = 0;   // 4 bytes: 0x377f0682 or 0x377f0683
    private static final int WAL_PAGE_SIZE_OFFSET = 8;   // 4 bytes: database page size

    // Offsets within a frame header
    private static final int FRAME_PAGE_NO_OFFSET = 0;   // 4 bytes: 1-based page number

    private SQLCipherWalDecryptor() { /* Utility class */ }

    // ── Public API ────────────────────────────────────────────────────────

    /**
     * Decrypts a WAL file to a forensically intact copy.
     * WAL headers and frame headers are kept as-is; only page data is decrypted.
     *
     * @param walFile      source WAL file ({@code database.db-wal})
     * @param outputFile   destination for the decrypted WAL
     * @param keys         derived keys from the main database (same passphrase)
     */
    public static void decryptToFile(File walFile,
                                     File outputFile,
                                     SQLCipherForensicDecryptor.DerivedKeys keys)
            throws Exception {

        validateWal(walFile);
        int pageSize = readPageSizeFromWal(walFile);

        LOG.info(String.format("WAL: %s  (%,d bytes, page size %d)",
                walFile.getName(), walFile.length(), pageSize));

        try (FileChannel src = FileChannel.open(walFile.toPath(),
                                                StandardOpenOption.READ);
             FileChannel dst = FileChannel.open(outputFile.toPath(),
                                                StandardOpenOption.WRITE,
                                                StandardOpenOption.CREATE,
                                                StandardOpenOption.TRUNCATE_EXISTING)) {

            // Copy WAL header as-is (32 bytes, plaintext)
            ByteBuffer walHeader = ByteBuffer.allocate(WAL_HEADER_SIZE);
            src.read(walHeader);
            walHeader.flip();
            dst.write(walHeader);

            int frameNum = 0;
            ByteBuffer frameHeaderBuf = ByteBuffer.allocate(FRAME_HEADER_SIZE);
            ByteBuffer pageBuf        = ByteBuffer.allocate(pageSize);

            while (src.position() < src.size()) {
                frameHeaderBuf.clear();
                pageBuf.clear();

                int hRead = src.read(frameHeaderBuf);
                int pRead = src.read(pageBuf);

                if (hRead < FRAME_HEADER_SIZE || pRead < pageSize) break;

                frameHeaderBuf.flip();
                pageBuf.flip();

                // Read page number from frame header (big-endian, offset 0)
                int pageNo = frameHeaderBuf.getInt(FRAME_PAGE_NO_OFFSET);
                frameNum++;

                // Write frame header unchanged (plaintext)
                frameHeaderBuf.rewind();
                dst.write(frameHeaderBuf);

                // Decrypt page data
                byte[] pageData  = new byte[pageSize];
                pageBuf.get(pageData);
                byte[] decrypted = decryptWalPage(pageData, pageNo, keys.encKey(), pageSize);
                dst.write(ByteBuffer.wrap(decrypted));

                LOG.fine(String.format("  Frame %d: page %d decrypted", frameNum, pageNo));
            }

            LOG.info(String.format("WAL decrypted: %d frames written to %s",
                    frameNum, outputFile.getName()));
        }
    }

    /**
     * Extracts all WAL frames as a map of {@code pageNumber → decrypted page bytes}.
     * If the same page appears multiple times (multiple versions in WAL),
     * the LAST (most recent) version is kept — matching SQLite's own WAL replay logic.
     *
     * @return ordered map: page number → latest decrypted page data
     */
    public static Map<Integer, byte[]> extractPages(
            File walFile,
            SQLCipherForensicDecryptor.DerivedKeys keys) throws Exception {

        validateWal(walFile);
        int pageSize = readPageSizeFromWal(walFile);

        // LinkedHashMap preserves insertion order; later entries overwrite earlier ones
        Map<Integer, byte[]> pages = new LinkedHashMap<>();

        try (FileChannel src = FileChannel.open(walFile.toPath(),
                                                StandardOpenOption.READ)) {
            // Skip WAL header
            src.position(WAL_HEADER_SIZE);

            ByteBuffer frameHeaderBuf = ByteBuffer.allocate(FRAME_HEADER_SIZE);
            ByteBuffer pageBuf        = ByteBuffer.allocate(pageSize);

            while (src.position() < src.size()) {
                frameHeaderBuf.clear();
                pageBuf.clear();

                if (src.read(frameHeaderBuf) < FRAME_HEADER_SIZE) break;
                if (src.read(pageBuf)        < pageSize)           break;

                frameHeaderBuf.flip();
                pageBuf.flip();

                int    pageNo   = frameHeaderBuf.getInt(FRAME_PAGE_NO_OFFSET);
                byte[] pageData = new byte[pageSize];
                pageBuf.get(pageData);

                byte[] decrypted = decryptWalPage(pageData, pageNo, keys.encKey(), pageSize);
                pages.put(pageNo, decrypted);   // overwrites older version of same page
            }
        }

        LOG.info(String.format("WAL: extracted %d unique pages from %s",
                pages.size(), walFile.getName()));
        return pages;
    }

    /**
     * Applies WAL pages on top of a (already decrypted) main database file,
     * producing a fully up-to-date SQLite database as SQLite would see it
     * after a checkpoint.
     *
     * <p>This is the correct way to get the current state of a WAL-mode database:
     * some pages may only exist in the WAL and not yet in the main file.
     *
     * @param decryptedMainDb  output of {@link SQLCipherForensicDecryptor#decrypt}
     * @param walFile          the corresponding {@code .db-wal} file
     * @param keys             derived keys
     * @param outputFile       merged output (main DB + WAL pages applied)
     */
    public static void applyToDatabase(File decryptedMainDb,
                                       File walFile,
                                       SQLCipherForensicDecryptor.DerivedKeys keys,
                                       File outputFile) throws Exception {

        // Start with a copy of the decrypted main database
        try (InputStream in   = new FileInputStream(decryptedMainDb);
             OutputStream out = new FileOutputStream(outputFile)) {
            in.transferTo(out);
        }

        // Extract the latest version of each page from the WAL
        Map<Integer, byte[]> walPages = extractPages(walFile, keys);
        int pageSize = readPageSizeFromWal(walFile);

        // Overwrite pages in the output file with their WAL versions
        try (RandomAccessFile raf = new RandomAccessFile(outputFile, "rw")) {
            for (Map.Entry<Integer, byte[]> entry : walPages.entrySet()) {
                int    pageNo   = entry.getKey();
                byte[] pageData = entry.getValue();

                long offset = (long)(pageNo - 1) * pageSize;
                raf.seek(offset);
                raf.write(pageData);

                LOG.fine(String.format("  Applied WAL page %d at offset %d", pageNo, offset));
            }
        }

        LOG.info(String.format(
                "WAL applied: %d pages merged into %s",
                walPages.size(), outputFile.getName()));
    }

    // ── Page decryption ───────────────────────────────────────────────────

    /**
     * Decrypts a single WAL page.
     *
     * WAL page layout is identical to main database pages:
     * <pre>
     *   [0..pageSize-RESERVED-1]  AES-CBC encrypted content
     *   [pageSize-80..pageSize-65] IV   (16 bytes)
     *   [pageSize-64..pageSize-1]  HMAC (64 bytes)
     * </pre>
     *
     * WAL page 1 does NOT have the salt-header special case — that only
     * applies to the very first page of the main database file.
     */
    private static byte[] decryptWalPage(byte[] pageData, int pageNo,
                                         byte[] encKey, int pageSize)
            throws Exception {

        int reserved    = SQLCipherForensicDecryptor.RESERVED_SIZE;
        int contentSize = pageSize - reserved;

        byte[] encrypted = Arrays.copyOfRange(pageData, 0, contentSize);
        byte[] iv        = Arrays.copyOfRange(pageData, contentSize,
                                              contentSize + SQLCipherForensicDecryptor.IV_SIZE);
        byte[] reservedBytes = Arrays.copyOfRange(pageData, contentSize, pageSize);

        Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE,
                new SecretKeySpec(encKey, "AES"),
                new IvParameterSpec(iv));
        byte[] decrypted = cipher.doFinal(encrypted);

        byte[] result = new byte[pageSize];
        System.arraycopy(decrypted,      0, result, 0,           decrypted.length);
        System.arraycopy(reservedBytes,  0, result, contentSize, reservedBytes.length);
        return result;
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static int readPageSizeFromWal(File walFile) throws IOException {
        try (DataInputStream dis = new DataInputStream(new FileInputStream(walFile))) {
            dis.skipBytes(WAL_PAGE_SIZE_OFFSET);    // skip to offset 8
            int pageSize = dis.readInt();
            return (pageSize == 1) ? 65536 : pageSize;   // SQLite encoding: 1 = 65536
        }
    }

    private static void validateWal(File walFile) throws IOException {
        if (!walFile.exists())
            throw new FileNotFoundException("WAL file not found: " + walFile);
        if (walFile.length() < WAL_HEADER_SIZE)
            throw new IOException("File too small to be a WAL file: " + walFile);
    }
}
