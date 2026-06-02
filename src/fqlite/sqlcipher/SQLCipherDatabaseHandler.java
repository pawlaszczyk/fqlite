package fqlite.sqlcipher;

import org.sqlite.mc.HmacAlgorithm;
import org.sqlite.mc.KdfAlgorithm;
import org.sqlite.mc.SQLiteMCConfig;
import org.sqlite.mc.SQLiteMCSqlCipherConfig;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * Opens a SQLCipher-encrypted database via the sqlite-jdbc-crypt driver.
 *
 * <p>Uses the driver's built-in {@link SQLiteMCSqlCipherConfig} builder which
 * applies cipher parameters in the correct order internally (cipher engine
 * first, key last). This is the only reliable API for this driver.
 */
public class SQLCipherDatabaseHandler {

    private static final Logger LOG = Logger.getLogger(SQLCipherDatabaseHandler.class.getName());

    private SQLCipherDatabaseHandler() { /* Utility class */ }

    // ── Public API ────────────────────────────────────────────────────────

    public static Connection openConnection(File dbFile, SQLCipherParams params)
            throws SQLException {

        ensureDriverLoaded();

        SQLiteMCConfig config = buildConfig(params);
        String url = toJdbcUrl(dbFile);
        LOG.fine("Connecting to: " + dbFile.getName());

        Connection conn = DriverManager.getConnection(url, config.toProperties());
        LOG.info("SQLCipher database opened successfully: " + dbFile.getName());
        return conn;
    }

    public static boolean looksEncrypted(File dbFile) {
        try (var is = new java.io.FileInputStream(dbFile)) {
            byte[] header = new byte[16];
            int read = is.read(header);
            if (read < 16) return false;
            String magic = new String(header, java.nio.charset.StandardCharsets.US_ASCII);
            return !magic.startsWith("SQLite format 3");
        } catch (Exception e) {
            return false;
        }
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    private static void ensureDriverLoaded() throws SQLException {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            throw new SQLException(
                    "SQLite JDBC driver not found. " +
                    "Please add 'io.github.willena:sqlite-jdbc' to your dependencies.", e);
        }
    }

    /**
     * Builds a {@link SQLiteMCConfig} using the driver's dedicated
     * {@link SQLiteMCSqlCipherConfig} builder.
     *
     * <p>The builder sets cipher parameters in the correct order and uses
     * the proper typed enums ({@link HmacAlgorithm}, {@link KdfAlgorithm})
     * that the driver expects — no raw strings or integer constants needed.
     */
    static SQLiteMCConfig buildConfig(SQLCipherParams params) {
        SQLiteMCSqlCipherConfig builder;

        // Start from a version preset to get all defaults right
        switch (params.getCompatibilityVersion()) {
            case 1 -> builder = SQLiteMCSqlCipherConfig.getV1Defaults();
            case 2 -> builder = SQLiteMCSqlCipherConfig.getV2Defaults();
            case 3 -> builder = SQLiteMCSqlCipherConfig.getV3Defaults();
            default -> builder = SQLiteMCSqlCipherConfig.getV4Defaults();
        }

        // Override individual parameters if the user customized them
        builder.setKdfIter(params.getKdfIter());
        builder.setHmacAlgorithm(toHmacAlgorithm(params.getHmacAlgorithm()));
        builder.setKdfAlgorithm(toKdfAlgorithm(params.getKdfAlgorithm()));

        // Set the key last
        if (params.isHexKey()) {
            // Strip surrounding x'...' — withHexKey() re-adds the wrapper
            String rawHex = params.getKey()
                    .replaceFirst("(?i)^x'", "")
                    .replaceFirst("'$", "");
            builder.withHexKey(rawHex);
        } else {
            builder.withKey(params.getKey());
        }

        return builder.build();
    }

    /** Maps UI algorithm name to the typed {@link HmacAlgorithm} enum. */
    private static HmacAlgorithm toHmacAlgorithm(String name) {
        if (name.contains("512")) return HmacAlgorithm.SHA512;
        if (name.contains("256")) return HmacAlgorithm.SHA256;
        return HmacAlgorithm.SHA1;
    }

    /** Maps UI algorithm name to the typed {@link KdfAlgorithm} enum. */
    private static KdfAlgorithm toKdfAlgorithm(String name) {
        if (name.contains("512")) return KdfAlgorithm.SHA512;
        if (name.contains("256")) return KdfAlgorithm.SHA256;
        return KdfAlgorithm.SHA1;
    }

    static String toJdbcUrl(File f) {
        return "jdbc:sqlite:" + f.getAbsolutePath().replace('\\', '/');
    }
}
