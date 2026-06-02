package fqlite.sqlcipher;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Decrypts a SQLCipher database and saves it as a forensically intact,
 * unencrypted SQLite file.
 *
 * <p>Uses the system {@code sqlcipher} CLI tool via {@link ProcessBuilder} to
 * perform a true page-level export via {@code sqlcipher_export()}. This
 * preserves the full database structure including free pages, slack space,
 * and deleted row remnants that are essential for forensic analysis.
 *
 * <p>The source file is never modified.
 *
 * <p><b>Prerequisite:</b> {@code sqlcipher} must be installed and on the PATH.
 * Install via: {@code brew install sqlcipher}  (macOS)
 *              {@code apt install sqlcipher}    (Linux)
 */
public class SQLCipherExporter {

    private static final Logger LOG = Logger.getLogger(SQLCipherExporter.class.getName());

    private SQLCipherExporter() { /* Utility class */ }

    // ── Public API ────────────────────────────────────────────────────────

    public static void export(File encryptedDb,
                              File plaintextDb,
                              SQLCipherParams params) throws SQLException {
        export(encryptedDb, plaintextDb, params, false);
    }

    public static void export(File encryptedDb,
                              File plaintextDb,
                              SQLCipherParams params,
                              boolean overwrite) throws SQLException {

        if (!encryptedDb.exists())
            throw new IllegalArgumentException("Source file not found: " + encryptedDb);
        if (plaintextDb.exists()) {
            if (!overwrite)
                throw new IllegalStateException("Target file already exists: " + plaintextDb);
            if (!plaintextDb.delete())
                throw new IllegalStateException("Could not delete target file: " + plaintextDb);
        }

        // Verify sqlcipher is available before starting
        String sqlcipherBin = findSqlcipher();

        LOG.info("Exporting via sqlcipher CLI: " + encryptedDb.getName()
                 + " → " + plaintextDb.getName());

        // Build the SQL script passed to sqlcipher via stdin
        String script = buildExportScript(params, plaintextDb);

        try {
            ProcessBuilder pb = new ProcessBuilder(sqlcipherBin,
                    encryptedDb.getAbsolutePath());
            pb.redirectErrorStream(true);   // merge stderr into stdout for logging

            Process proc = pb.start();

            // Write SQL script to sqlcipher's stdin
            proc.getOutputStream().write(script.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            proc.getOutputStream().close();

            // Capture output for logging / error reporting
            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(proc.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append('\n');
                    LOG.fine("sqlcipher: " + line);
                }
            }

            int exitCode = proc.waitFor();

            if (exitCode != 0 || !plaintextDb.exists() || plaintextDb.length() == 0) {
                plaintextDb.delete();
                throw new SQLException(
                        "sqlcipher export failed (exit code " + exitCode + ").\n" +
                        "Output:\n" + output +
                        "\nHint: wrong password or SQLCipher version mismatch?");
            }

        } catch (IOException | InterruptedException e) {
            plaintextDb.delete();
            throw new SQLException("Failed to run sqlcipher process: " + e.getMessage(), e);
        }

        LOG.info("Export complete: " + plaintextDb.getAbsolutePath()
                 + " (" + plaintextDb.length() + " bytes)");
    }

    public static File exportToTempFile(File encryptedDb, SQLCipherParams params)
            throws SQLException {
        String name = encryptedDb.getName().replaceFirst("(\\.[^.]+)$", "_decrypted$1");
        File target = new File(encryptedDb.getParent(), name);
        export(encryptedDb, target, params, true);
        return target;
    }

    public static void export(File encryptedDb, File plaintextDb, SQLCipherParams params,
                              boolean overwrite, java.util.function.IntConsumer onProgress)
            throws SQLException {
        if (onProgress != null) onProgress.accept(0);
        export(encryptedDb, plaintextDb, params, overwrite);
        if (onProgress != null) onProgress.accept(100);
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    /**
     * Builds the SQL script that sqlcipher executes to decrypt and export.
     * Uses sqlcipher_export() for a true page-level copy.
     */
    private static String buildExportScript(SQLCipherParams params, File plaintextDb) {
        String destPath = plaintextDb.getAbsolutePath().replace("'", "\\'");
        StringBuilder sb = new StringBuilder();

        // 1. Key
        sb.append("PRAGMA key = ").append(params.getPragmaKeyExpression()).append(";\n");

        // 2. Cipher configuration (must match the database's original settings)
        if (params.getCompatibilityVersion() < 4) {
            sb.append("PRAGMA cipher_compatibility = ")
                    .append(params.getCompatibilityVersion()).append(";\n");
        }
        sb.append("PRAGMA cipher_page_size = ").append(params.getPageSize()).append(";\n");
        sb.append("PRAGMA kdf_iter = ").append(params.getKdfIter()).append(";\n");
        sb.append("PRAGMA cipher_hmac_algorithm = ").append(params.getHmacAlgorithm()).append(";\n");
        sb.append("PRAGMA cipher_kdf_algorithm = ").append(params.getKdfAlgorithm()).append(";\n");

        // 3. Attach plain target and export (page-level copy)
        sb.append("ATTACH DATABASE '").append(destPath).append("' AS plaintext KEY '';\n");
        sb.append("SELECT sqlcipher_export('plaintext');\n");
        sb.append("DETACH DATABASE plaintext;\n");
        sb.append(".quit\n");

        return sb.toString();
    }

    private static String findSqlcipher() throws SQLException {

        // 1. Relativ zur laufenden App (gebündeltes Binary)
        String javaHome = System.getProperty("java.home", "");
        Path base = Path.of(javaHome);

        List<Path> bundled = List.of(
                // macOS: fqlite.app/Contents/MacOS/sqlcipher
                base.getParent().getParent().resolve("MacOS/sqlcipher"),
                // Linux: /opt/fqlite/lib/../bin/sqlcipher  o.ä.
                base.getParent().getParent().resolve("bin/sqlcipher"),
                // Windows: fqlite/app/sqlcipher.exe
                base.getParent().getParent().resolve("app/sqlcipher.exe"),
                base.getParent().getParent().resolve("app/sqlcipher")
        );

        for (Path p : bundled) {
            if (Files.exists(p) && p.toFile().canExecute()) {
                LOG.info("Using bundled sqlcipher: " + p);
                return p.toString();
            }
        }

        // 2. Fallback: System-PATH (Entwicklermaschine)
        List<String> systemPaths = List.of(
                "/opt/homebrew/bin/sqlcipher",   // macOS Apple Silicon
                "/usr/local/bin/sqlcipher",      // macOS Intel / Linux
                "/usr/bin/sqlcipher"             // Linux apt
        );

        for (String p : systemPaths) {
            if (new File(p).canExecute()) {
                LOG.info("Using system sqlcipher: " + p);
                return p;
            }
        }

        // 3. which/where
        try {
            String cmd = System.getProperty("os.name","").toLowerCase()
                    .contains("win") ? "where" : "which";
            Process p = new ProcessBuilder(cmd, "sqlcipher").start();
            String found = new BufferedReader(
                    new InputStreamReader(p.getInputStream())).readLine();
            if (found != null && !found.isBlank()) return found.trim();
        } catch (IOException ignored) {}

        throw new SQLException(
                "sqlcipher not found. Please install it:\n" +
                "  macOS:  brew install sqlcipher\n" +
                "  Linux:  sudo apt install sqlcipher\n" +
                "  Windows: wird normalerweise mitgeliefert.");
    }
}
