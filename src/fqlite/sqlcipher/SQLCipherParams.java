package fqlite.sqlcipher;

/**
 * Holds all parameters required to decrypt a SQLCipher-encrypted database.
 */
public class SQLCipherParams {

    /** Passphrase or hex key (if hexKey == true, must be in "x'...'" format) */
    private final String key;

    /** true = key is a raw hex key ("x'<hex>'"), false = passphrase */
    private final boolean hexKey;

    // ── SQLCipher configuration parameters ────────────────────────────────

    /** cipher_page_size  (default: 4096) */
    private final int pageSize;

    /** kdf_iter  (PBKDF2 iterations, default: 256000) */
    private final int kdfIter;

    /** cipher_hmac_algorithm  (HMAC_SHA1 | HMAC_SHA256 | HMAC_SHA512) */
    private final String hmacAlgorithm;

    /** cipher_kdf_algorithm  (PBKDF2_HMAC_SHA1 | PBKDF2_HMAC_SHA256 | PBKDF2_HMAC_SHA512) */
    private final String kdfAlgorithm;

    /** SQLCipher compatibility mode  (1 | 2 | 3 | 4 / current) */
    private final int compatibilityVersion;

    // ── Constructor ───────────────────────────────────────────────────────

    public SQLCipherParams(String key,
                           boolean hexKey,
                           int pageSize,
                           int kdfIter,
                           String hmacAlgorithm,
                           String kdfAlgorithm,
                           int compatibilityVersion) {
        this.key                  = key;
        this.hexKey               = hexKey;
        this.pageSize             = pageSize;
        this.kdfIter              = kdfIter;
        this.hmacAlgorithm        = hmacAlgorithm;
        this.kdfAlgorithm         = kdfAlgorithm;
        this.compatibilityVersion = compatibilityVersion;
    }

    // ── Default preset (SQLCipher 4) ──────────────────────────────────────

    public static SQLCipherParams defaultV4(String passphrase) {
        return new SQLCipherParams(passphrase, false,
                4096, 256000,
                "HMAC_SHA512", "PBKDF2_HMAC_SHA512", 4);
    }

    /** Preset for SQLCipher 3 (legacy) */
    public static SQLCipherParams legacyV3(String passphrase) {
        return new SQLCipherParams(passphrase, false,
                1024, 64000,
                "HMAC_SHA1", "PBKDF2_HMAC_SHA1", 3);
    }

    // ── Getters ───────────────────────────────────────────────────────────

    public String getKey()               { return key; }
    public boolean isHexKey()            { return hexKey; }
    public int getPageSize()             { return pageSize; }
    public int getKdfIter()              { return kdfIter; }
    public String getHmacAlgorithm()     { return hmacAlgorithm; }
    public String getKdfAlgorithm()      { return kdfAlgorithm; }
    public int getCompatibilityVersion() { return compatibilityVersion; }

    // ── Helper: key string for PRAGMA ─────────────────────────────────────

    /**
     * Returns the correct key expression for "PRAGMA key = …":
     *   – Hex key  : x'DEADBEEF…'  (passed through as-is)
     *   – Passphrase: 'myPassword'
     */
    public String getPragmaKeyExpression() {
        if (hexKey) {
            // User already provided the "x'...'" format
            return key;
        }
        // Plain passphrase – escape single quotes if necessary
        return "'" + key.replace("'", "''") + "'";
    }
}
