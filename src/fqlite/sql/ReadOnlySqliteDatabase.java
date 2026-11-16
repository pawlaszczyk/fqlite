package fqlite.sql;

import fqlite.log.AppLog;
import javafx.stage.Stage;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteOpenMode;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/// A SQLite database file opened directly and strictly read-only.
///
/// Used by the Lighthouse plugin for databases written by the ETSI importer
/// (see `fqlite.importer.EtsiXmlImporter`). In contrast to
/// [InMemoryDatabase] no records are copied into RAM: every query runs
/// against the file itself, so databases larger than the available memory
/// can be analysed, and the importer's indexes (IMSI, MSISDN, IMEI,
/// start_time, cell, ...) speed up the analysis presets.
///
/// **Evidence integrity.** The file is opened with `mode=ro` and the
/// `SQLITE_OPEN_READONLY` flag; every attempt to write to it fails with
/// `SQLITE_READONLY`. If there is no
/// non-empty `-wal` file next to the database, it is also opened with
/// `immutable=1`: SQLite then takes no locks and does not create `-shm`/`-wal`
/// files, so neither the database file nor its directory is touched (this
/// also works on write-protected media). If a non-empty `-wal` file exists,
/// `immutable=1` would silently ignore the records in it, so the database is
/// opened without it; SQLite then needs to create a `-shm` file next to it.
///
/// Helper objects that the query assistant needs (see
/// `fqlite.rag.EtsiContext#createViews`) are created as TEMP views, which
/// live only in this connection and never reach the file.
///
/// @author D. Pawlaszczyk
public class ReadOnlySqliteDatabase implements QueryableDatabase, AutoCloseable {

    static {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            AppLog.error("SQLite JDBC driver was not found: " + e.getMessage());
        }
    }

    private final File file;
    private final String url;
    private final boolean immutable;
    private final Connection connection;
    private Stage stage;

    private ReadOnlySqliteDatabase(File file) throws SQLException {
        this.file = file.getAbsoluteFile();
        File wal = new File(this.file.getPath() + "-wal");
        this.immutable = !(wal.isFile() && wal.length() > 0);
        // toPath().toUri() yields "file:///..." on every platform (also
        // "file:///C:/..." on Windows) with special characters percent-encoded,
        // which is the form SQLite expects for URI filenames.
        this.url = "jdbc:sqlite:" + this.file.toPath().toUri()
                + "?mode=ro" + (immutable ? "&immutable=1" : "");
        this.connection = openConnection();
    }

    /// Opens `file` read-only.
    ///
    /// @param file a SQLite database file
    /// @return the opened database
    /// @throws SQLException if the file cannot be opened or is not a SQLite database
    public static ReadOnlySqliteDatabase open(File file) throws SQLException {
        if (file == null || !file.isFile()) {
            throw new SQLException("Database file not found: " + file);
        }
        ReadOnlySqliteDatabase db = new ReadOnlySqliteDatabase(file);
        // Touch the schema once: fails early with a clear message if the file
        // is no (or a damaged) SQLite database.
        try (Statement st = db.connection.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM sqlite_master")) {
            rs.next();
        } catch (SQLException e) {
            db.close();
            throw e;
        }
        return db;
    }

    /// Opens an additional, independent read-only connection to the same file.
    /// Used for long-running scans (e.g. loading all records for the map), so
    /// they do not block the queries of the presets or the query assistant on
    /// the main connection. The caller has to close it.
    ///
    /// @return a new connection
    /// @throws SQLException if the connection cannot be opened
    public Connection openConnection() throws SQLException {
        // Read-only at the driver level as well, independent of the URI
        // parameter; OPEN_URI makes SQLite honour mode=ro / immutable=1.
        SQLiteConfig cfg = new SQLiteConfig();
        cfg.setReadOnly(true);
        cfg.setOpenMode(SQLiteOpenMode.OPEN_URI);
        // No "PRAGMA query_only": it would also block the TEMP views of the
        // query assistant; the read-only open flags already make any write
        // to the database file fail with SQLITE_READONLY.
        return cfg.createConnection(url);
    }

    /// True if the database contains the schema of the Lighthouse ETSI importer
    /// (tables `response_records` and `request_metadata`).
    public boolean isLighthouseDatabase() {
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
                     + "AND name IN ('response_records','request_metadata')")) {
            return rs.next() && rs.getInt(1) == 2;
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public ResultSet execute(String sql) {
        try {
            PreparedStatement pstmt = connection.prepareStatement(sql);
            // Close the statement together with the result set; callers only
            // ever close the ResultSet they get back.
            pstmt.closeOnCompletion();
            return pstmt.executeQuery();
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? e.toString() : e.getMessage();
            if (msg.contains("SQLITE_INTERRUPT") || msg.contains("interrupted")) {
                AppLog.info("Lighthouse query cancelled");
                return null;   // cancelled by the analyst: no error dialog
            }
            AppLog.error("Lighthouse query failed: " + msg);
            if (msg.contains("syntax error")) {
                InMemoryDatabase.showErrorSELECT(stage, msg);
            } else if (msg.contains("no such table")) {
                InMemoryDatabase.showErrorTableDoesNotExist(stage, msg);
            } else {
                InMemoryDatabase.showErrorDatabase(stage, msg);
            }
            return null;
        }
    }

    @Override
    public Connection getConnectionObject() {
        return connection;
    }

    @Override
    public String getDbName() {
        return file.getName();
    }

    /// @return the database file
    public File getFile() {
        return file;
    }

    /// @return true if the file was opened with `immutable=1` (no `-wal` content)
    public boolean isImmutable() {
        return immutable;
    }

    /// @param stage owner window for error dialogs (may be `null`)
    public void setStage(Stage stage) {
        this.stage = stage;
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            AppLog.error("Closing read-only database failed: " + e.getMessage());
        }
    }
}
