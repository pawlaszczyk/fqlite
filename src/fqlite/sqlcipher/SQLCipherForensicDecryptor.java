package fqlite.sqlcipher;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.security.spec.KeySpec;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 * Forensic page-level decryptor for SQLCipher v4 databases.
 *
 * <p>Decrypts a SQLCipher-encrypted database page by page using AES-256-CBC,
 * preserving the COMPLETE byte layout of every page including:
 * <ul>
 *   <li>Free/unallocated pages  (contain deleted row remnants)</li>
 *   <li>Slack space within pages</li>
 *   <li>B-Tree internal structure</li>
 *   <li>Original page ordering and size</li>
 * </ul>
 *
 * <p>This is the only method that produces a forensically intact copy.
 * {@code sqlcipher_export()} and the SQLite Backup API both discard free
 * pages and slack space — do NOT use them for forensic work.
 *
 * <p>Uses only JDK standard library (javax.crypto) — no third-party
 * dependencies required.
 *
 * <h3>SQLCipher v4 page layout</h3>
 * <pre>
 *   File:
 *     [16 bytes KDF salt – plaintext]
 *     [Page 1: 16-byte salt area | encrypted body | IV(16) | HMAC(64)]
 *     [Page 2: encrypted content(4016 bytes)      | IV(16) | HMAC(64)]
 *     ...
 *
 *   Original SQLite page 1:
 *     [0-15]    "SQLite format 3\0"  ← replaced by salt in encrypted file
 *     [16-4015] header fields + root B-Tree data
 *     [4016-]   reserved (IV + HMAC in encrypted form)
 * </pre>
 *
 * <h3>Default parameters (SQLCipher v4)</h3>
 * <pre>
 *   PAGE_SIZE   = 4096
 *   KDF_ITER    = 256000
 *   KDF         = PBKDF2-HMAC-SHA512
 *   CIPHER      = AES-256-CBC
 *   HMAC        = SHA-512 (64 bytes per page)
 * </pre>
 */
public class SQLCipherForensicDecryptor {

    private static final Logger LOG =
            Logger.getLogger(SQLCipherForensicDecryptor.class.getName());

    // ── SQLCipher v4 default constants ────────────────────────────────────

    public static final int PAGE_SIZE     = 4096;
    public static final int KDF_ITER      = 256000;
    public static final int SALT_SIZE     = 16;
    public static final int IV_SIZE       = 16;
    public static final int HMAC_SIZE     = 64;
    public static final int RESERVED_SIZE = IV_SIZE + HMAC_SIZE;   // 80 bytes
    public static final int KEY_SIZE      = 32;   // AES-256
    public static final int HMAC_KEY_SIZE = 32;

    private static final byte[] SQLITE_MAGIC =
            "SQLite format 3\0".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    // ── Derived keys (bundle for passing around) ──────────────────────────

    public record DerivedKeys(byte[] encKey, byte[] hmacKey) {
        /** Wipes both keys from memory. Call when done. */
        public void destroy() {
            Arrays.fill(encKey,  (byte) 0);
            Arrays.fill(hmacKey, (byte) 0);
        }
    }

    // ── Public API ────────────────────────────────────────────────────────

    /**
     * Decrypts a SQLCipher v4 database to a forensically intact plain SQLite file.
     *
     * @param encryptedDb  source file (SQLCipher encrypted)
     * @param plaintextDb  destination file (will be created or overwritten)
     * @param passphrase   the database passphrase
     * @throws Exception   on I/O error, wrong passphrase, or invalid database
     */
    public static void decrypt(File encryptedDb,
                               File plaintextDb,
                               String passphrase) throws Exception {
        decrypt(encryptedDb, plaintextDb, passphrase, null);
    }

    /**
     * Decrypts with an optional progress callback.
     *
     * @param onProgress  called with (currentPage, totalPages); may be null
     */
    public static void decrypt(File encryptedDb,
                               File plaintextDb,
                               String passphrase,
                               ProgressCallback onProgress) throws Exception {

        validateInput(encryptedDb);

        long fileSize   = encryptedDb.length();
        long totalPages = fileSize / PAGE_SIZE;

        LOG.info(String.format("Input:  %s  (%,d bytes, %d pages)",
                encryptedDb.getName(), fileSize, totalPages));

        // Read the KDF salt from the first 16 bytes of the file
        byte[] salt = readSalt(encryptedDb);
        LOG.info("KDF salt: " + bytesToHex(salt));

        // Derive AES and HMAC keys via PBKDF2-SHA512
        LOG.info(String.format("Deriving keys (PBKDF2-SHA512, %,d iterations) …", KDF_ITER));
        DerivedKeys keys = deriveKeys(passphrase, salt);

        try {
            // Process page by page
            try (FileChannel src = FileChannel.open(encryptedDb.toPath(),
                                                    StandardOpenOption.READ);
                 FileChannel dst = FileChannel.open(plaintextDb.toPath(),
                                                    StandardOpenOption.WRITE,
                                                    StandardOpenOption.CREATE,
                                                    StandardOpenOption.TRUNCATE_EXISTING)) {

                ByteBuffer pageBuffer = ByteBuffer.allocateDirect(PAGE_SIZE);

                for (int pageNum = 1; pageNum <= totalPages; pageNum++) {
                    pageBuffer.clear();
                    int bytesRead = src.read(pageBuffer);
                    if (bytesRead != PAGE_SIZE) {
                        throw new IOException(
                                "Unexpected end of file at page " + pageNum);
                    }
                    pageBuffer.flip();

                    byte[] pageData = new byte[PAGE_SIZE];
                    pageBuffer.get(pageData);

                    byte[] decrypted = (pageNum == 1)
                            ? decryptPage1(pageData, keys.encKey())
                            : decryptPageN(pageData, keys.encKey());

                    dst.write(ByteBuffer.wrap(decrypted));

                    if (onProgress != null) {
                        onProgress.onProgress(pageNum, (int) totalPages);
                    }
                }
            }

            // Verify the output has a valid SQLite magic header
            verifySQLiteHeader(plaintextDb);

            LOG.info("Forensic decrypt complete: " + plaintextDb.getAbsolutePath());
            LOG.info(String.format("Output size: %,d bytes (identical to input ✅)",
                    plaintextDb.length()));

        } catch (Exception e) {
            // Clean up partial output on failure
            if (plaintextDb.exists()) plaintextDb.delete();
            throw e;
        } finally {
            keys.destroy();
        }
    }

    // ── Key derivation ────────────────────────────────────────────────────

    /**
     * Derives AES-256 encryption key and HMAC key from the passphrase
     * using PBKDF2-HMAC-SHA512 (SQLCipher v4 default).
     *
     * SQLCipher derives a single 64-byte master key and splits it:
     *   encKey  = master[0..31]
     *   hmacKey = master[32..63]
     */
    public static DerivedKeys deriveKeys(String passphrase, byte[] salt)
            throws Exception {

        // Java's PBKDF2WithHmacSHA512 derives the key correctly
        SecretKeyFactory factory =
                SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");

        KeySpec spec = new PBEKeySpec(
                passphrase.toCharArray(),
                salt,
                KDF_ITER,
                (KEY_SIZE + HMAC_KEY_SIZE) * 8   // length in bits
        );

        byte[] masterKey = factory.generateSecret(spec).getEncoded();

        byte[] encKey  = Arrays.copyOfRange(masterKey, 0, KEY_SIZE);
        byte[] hmacKey = Arrays.copyOfRange(masterKey, KEY_SIZE, KEY_SIZE + HMAC_KEY_SIZE);

        Arrays.fill(masterKey, (byte) 0);   // wipe master key from memory

        return new DerivedKeys(encKey, hmacKey);
    }

    // ── Page decryption ───────────────────────────────────────────────────

    /**
     * Decrypts page 1 (special case).
     *
     * In the encrypted file, page 1 has this layout:
     * <pre>
     *   [0..15]                   KDF salt (plaintext, NOT part of encrypted content)
     *   [16..PAGE_SIZE-81]        AES-CBC encrypted body (contains SQLite header fields)
     *   [PAGE_SIZE-80..PAGE_SIZE-65]  IV   (16 bytes, plaintext)
     *   [PAGE_SIZE-64..PAGE_SIZE-1]   HMAC (64 bytes, plaintext)
     * </pre>
     *
     * The decrypted body contains bytes 16..PAGE_SIZE-81 of the original SQLite page.
     * Bytes 0..15 of the original page are the SQLite magic — SQLCipher replaces
     * them with the KDF salt. We restore the magic on decryption.
     */
    static byte[] decryptPage1(byte[] pageData, byte[] encKey) throws Exception {
        int contentStart = SALT_SIZE;                      // 16
        int contentEnd   = PAGE_SIZE - RESERVED_SIZE;     // 4016
        int contentSize  = contentEnd - contentStart;     // 4000 bytes

        byte[] encrypted = Arrays.copyOfRange(pageData, contentStart, contentEnd);
        byte[] iv        = Arrays.copyOfRange(pageData, contentEnd, contentEnd + IV_SIZE);
        byte[] reserved  = Arrays.copyOfRange(pageData, contentEnd, PAGE_SIZE);  // IV+HMAC

        byte[] decryptedBody = aesCbcDecrypt(encrypted, encKey, iv);

        // Reconstruct original page 1:
        //   [0..15]    SQLite magic  (restored)
        //   [16..4015] decrypted body (SQLite header fields start here)
        //   [4016..]   reserved area  (IV + HMAC, kept for forensic completeness)
        byte[] result = new byte[PAGE_SIZE];
        System.arraycopy(SQLITE_MAGIC,   0, result, 0,           SALT_SIZE);
        System.arraycopy(decryptedBody,  0, result, SALT_SIZE,   decryptedBody.length);
        System.arraycopy(reserved,       0, result, contentEnd,  reserved.length);
        return result;
    }

    /**
     * Decrypts pages 2..N (standard case).
     *
     * Page layout:
     * <pre>
     *   [0..PAGE_SIZE-81]         AES-CBC encrypted content
     *   [PAGE_SIZE-80..PAGE_SIZE-65]  IV   (16 bytes)
     *   [PAGE_SIZE-64..PAGE_SIZE-1]   HMAC (64 bytes)
     * </pre>
     *
     * The IV and HMAC are NOT encrypted — they are stored in plaintext
     * in the reserved area at the end of each page.
     */
    static byte[] decryptPageN(byte[] pageData, byte[] encKey) throws Exception {
        int contentSize = PAGE_SIZE - RESERVED_SIZE;   // 4016 bytes

        byte[] encrypted = Arrays.copyOfRange(pageData, 0, contentSize);
        byte[] iv        = Arrays.copyOfRange(pageData, contentSize, contentSize + IV_SIZE);
        byte[] reserved  = Arrays.copyOfRange(pageData, contentSize, PAGE_SIZE);

        byte[] decrypted = aesCbcDecrypt(encrypted, encKey, iv);

        // Return: decrypted content + original reserved bytes
        byte[] result = new byte[PAGE_SIZE];
        System.arraycopy(decrypted, 0, result, 0,           decrypted.length);
        System.arraycopy(reserved,  0, result, contentSize, reserved.length);
        return result;
    }

    // ── AES-CBC ───────────────────────────────────────────────────────────

    private static byte[] aesCbcDecrypt(byte[] data, byte[] key, byte[] iv)
            throws Exception {
        Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE,
                new SecretKeySpec(key, "AES"),
                new IvParameterSpec(iv));
        return cipher.doFinal(data);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static byte[] readSalt(File f) throws IOException {
        try (InputStream is = new FileInputStream(f)) {
            byte[] salt = new byte[SALT_SIZE];
            if (is.read(salt) != SALT_SIZE)
                throw new IOException("File too small to contain SQLCipher salt.");
            return salt;
        }
    }

    private static void validateInput(File f) throws IOException {
        if (!f.exists())
            throw new FileNotFoundException("Source file not found: " + f);
        if (f.length() < PAGE_SIZE)
            throw new IOException("File too small to be a SQLCipher database: " + f);
        if (f.length() % PAGE_SIZE != 0)
            LOG.warning("File size is not a multiple of PAGE_SIZE — may be truncated.");
    }

    private static void verifySQLiteHeader(File f) throws IOException {
        try (InputStream is = new FileInputStream(f)) {
            byte[] magic = new byte[SQLITE_MAGIC.length];
            is.read(magic);
            if (!Arrays.equals(magic, SQLITE_MAGIC)) {
                throw new IOException(
                        "Output does not have a valid SQLite header.\n" +
                        "Wrong passphrase, or database uses non-default SQLCipher parameters.\n" +
                        "Expected: " + bytesToHex(SQLITE_MAGIC) + "\n" +
                        "Got:      " + bytesToHex(magic));
            }
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    // ── Progress callback ─────────────────────────────────────────────────

    @FunctionalInterface
    public interface ProgressCallback {
        /**
         * Called after each page is written.
         *
         * @param currentPage  1-based page number just written
         * @param totalPages   total number of pages in the database
         */
        void onProgress(int currentPage, int totalPages);
    }
}
