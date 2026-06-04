package fqlite.sqlcipher;

import java.io.File;
import java.util.logging.Logger;

/**
 * High-level facade for forensic SQLCipher export.
 *
 * Automatically detects and processes all three file types:
 * <ul>
 *   <li>Main database ({@code .db})</li>
 *   <li>WAL file      ({@code .db-wal})   – if present</li>
 *   <li>Journal file  ({@code .db-journal}) – if present</li>
 * </ul>
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * SQLCipherForensicExport.Result result =
 *     SQLCipherForensicExport.exportAll(encryptedDb, outputDir, params);
 *
 * result.mainDb();        // decrypted main database
 * result.walDb();         // WAL pages applied on top of main DB (null if no WAL)
 * result.decryptedWal();  // raw decrypted WAL file (null if no WAL)
 * result.decryptedJournal(); // raw decrypted journal (null if no journal)
 * }</pre>
 */
public class SQLCipherForensicExport {

    private static final Logger LOG =
            Logger.getLogger(SQLCipherForensicExport.class.getName());

    private SQLCipherForensicExport() { /* Utility class */ }

    // ── Result record ─────────────────────────────────────────────────────

    /**
     * Holds all output files produced by a forensic export.
     * Fields are null if the corresponding source file did not exist.
     */
    public record Result(
            /** Decrypted main database (page-level, forensically intact). */
            File mainDb,
            /**
             * Main database with WAL pages applied on top —
             * represents the current state as SQLite would see it.
             * Null if no WAL file was present.
             */
            File walDb,
            /**
             * Raw decrypted WAL file (headers intact, pages decrypted).
             * Useful for manual frame-level inspection.
             * Null if no WAL file was present.
             */
            File decryptedWal,
            /**
             * Raw decrypted rollback journal (headers intact, pages decrypted).
             * Contains before-images of pages from interrupted transactions.
             * Null if no journal file was present.
             */
            File decryptedJournal
    ) {}

    // ── Public API ────────────────────────────────────────────────────────

    /**
     * Exports a SQLCipher database and all associated auxiliary files
     * (WAL, journal) to the specified output directory.
     *
     * @param encryptedDb  main database file ({@code database.db})
     * @param outputDir    directory to write all output files into
     * @param params       decryption parameters (passphrase, algorithm, ...)
     * @param onProgress   optional progress callback (may be null)
     * @return             result object with paths to all created files
     */
    public static Result exportAll(File encryptedDb,
                                   File outputDir,
                                   SQLCipherParams params,
                                   SQLCipherForensicDecryptor.ProgressCallback onProgress)
            throws Exception {

        outputDir.mkdirs();

        String baseName = encryptedDb.getName();
            //    .replaceFirst("(\\.[^.]+)$", "");  // strip extension

        // Derive keys once — reuse for all three file types
        byte[] salt = readSalt(encryptedDb);
        LOG.info("Deriving keys …");
        SQLCipherForensicDecryptor.DerivedKeys keys =
                SQLCipherForensicDecryptor.deriveKeys(params.getKey(), salt);

        try {
            return exportWithKeys(encryptedDb, outputDir, baseName, keys, onProgress);
        } finally {
            keys.destroy();   // wipe keys from memory when done
        }
    }

    private static Result exportWithKeys(File encryptedDb,
                                         File outputDir,
                                         String baseName,
                                         SQLCipherForensicDecryptor.DerivedKeys keys,
                                         SQLCipherForensicDecryptor.ProgressCallback onProgress)
            throws Exception {

        // ── 1. Main database ──────────────────────────────────────────────
        File mainOut = new File(outputDir, baseName + "_forensic.db");
        LOG.info("Decrypting main database → " + mainOut.getName());
        decryptWithKeys(encryptedDb, mainOut, keys, onProgress);

        // ── 2. WAL file ───────────────────────────────────────────────────
        File walFile       = new File(encryptedDb.getParent(),
                encryptedDb.getName() + "-wal");
        File walOut        = null;
        File walMergedOut  = null;

        if (walFile.exists()) {
            LOG.info("WAL file found: " + walFile.getName());

            walOut = new File(outputDir, baseName + "_decrypted.db-wal");
            LOG.info("Decrypting WAL → " + walOut.getName());
            SQLCipherWalDecryptor.decryptToFile(walFile, walOut, keys);

            walMergedOut = new File(outputDir, baseName + "_forensic_wal_applied.db");
            LOG.info("Applying WAL to main DB → " + walMergedOut.getName());
            SQLCipherWalDecryptor.applyToDatabase(mainOut, walFile, keys, walMergedOut);
        } else {
            LOG.info("No WAL file found at: " + walFile.getPath());
        }

        // ── 3. Rollback journal ───────────────────────────────────────────
        File journalFile = new File(encryptedDb.getParent(),
                encryptedDb.getName() + "-journal");
        File journalOut  = null;

        if (journalFile.exists()) {
            LOG.info("Journal file found: " + journalFile.getName());
            journalOut = new File(outputDir, baseName + "_decrypted.db-journal");
            LOG.info("Decrypting journal → " + journalOut.getName());
            SQLCipherJournalDecryptor.decryptToFile(journalFile, journalOut, keys);
        } else {
            LOG.info("No journal file found at: " + journalFile.getPath());
        }

        // ── Summary ───────────────────────────────────────────────────────
        LOG.info("=== Forensic export complete ===");
        LOG.info("Main DB:          " + mainOut.getAbsolutePath());
        if (walMergedOut  != null) LOG.info("WAL merged DB:    " + walMergedOut.getAbsolutePath());
        if (walOut        != null) LOG.info("Decrypted WAL:    " + walOut.getAbsolutePath());
        if (journalOut    != null) LOG.info("Decrypted journal:" + journalOut.getAbsolutePath());

        return new Result(mainOut, walMergedOut, walOut, journalOut);
    }

    /** Decrypts using already-derived keys (avoids re-running PBKDF2). */
    private static void decryptWithKeys(File encryptedDb,
                                        File plaintextDb,
                                        SQLCipherForensicDecryptor.DerivedKeys keys,
                                        SQLCipherForensicDecryptor.ProgressCallback onProgress)
            throws Exception {

        long totalPages = encryptedDb.length() / SQLCipherForensicDecryptor.PAGE_SIZE;

        try (java.nio.channels.FileChannel src =
                     java.nio.channels.FileChannel.open(encryptedDb.toPath(),
                             java.nio.file.StandardOpenOption.READ);
             java.nio.channels.FileChannel dst =
                     java.nio.channels.FileChannel.open(plaintextDb.toPath(),
                             java.nio.file.StandardOpenOption.WRITE,
                             java.nio.file.StandardOpenOption.CREATE,
                             java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)) {

            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocateDirect(
                    SQLCipherForensicDecryptor.PAGE_SIZE);

            for (int pageNum = 1; pageNum <= totalPages; pageNum++) {
                buf.clear();
                src.read(buf);
                buf.flip();
                byte[] pageData = new byte[SQLCipherForensicDecryptor.PAGE_SIZE];
                buf.get(pageData);

                byte[] decrypted = (pageNum == 1)
                        ? SQLCipherForensicDecryptor.decryptPage1(pageData, keys.encKey())
                        : SQLCipherForensicDecryptor.decryptPageN(pageData, keys.encKey());

                dst.write(java.nio.ByteBuffer.wrap(decrypted));

                if (onProgress != null) onProgress.onProgress(pageNum, (int) totalPages);
            }
        }
    }

    private static byte[] readSalt(File f) throws java.io.IOException {
        try (java.io.InputStream is = new java.io.FileInputStream(f)) {
            byte[] salt = new byte[SQLCipherForensicDecryptor.SALT_SIZE];
            if (is.read(salt) != SQLCipherForensicDecryptor.SALT_SIZE)
                throw new java.io.IOException("File too small for salt");
            return salt;
        }
    }
}
