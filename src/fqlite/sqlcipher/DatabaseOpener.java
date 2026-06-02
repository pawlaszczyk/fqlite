package fqlite.sqlcipher;

import javafx.stage.FileChooser;
import javafx.stage.Window;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Central entry point for opening SQLite databases,
 * both encrypted and unencrypted.
 *
 * Usage example inside an existing controller:
 * <pre>{@code
 *   // In a MenuBar handler or toolbar button:
 *   DatabaseOpener opener = new DatabaseOpener(primaryStage);
 *   opener.openDatabaseInteractive()
 *         .ifPresent(conn -> forensicsController.loadDatabase(conn));
 * }</pre>
 */
public class DatabaseOpener {

    private static final Logger LOG = Logger.getLogger(DatabaseOpener.class.getName());

    private static Window ownerWindow = null;

    public DatabaseOpener(Window owner) {
        ownerWindow = owner;
    }

    // ── Main method ───────────────────────────────────────────────────────

    /**
     * Shows a file chooser dialog, automatically detects whether the selected
     * database is encrypted, and returns an open {@link Connection}.
     *
     * @return open connection, or {@link Optional#empty()} if cancelled
     */
    public Optional<Connection> openDatabaseInteractive() {

        // 1. Choose file
        File dbFile = chooseFile();
        if (dbFile == null) return Optional.empty();

        return openDatabase(dbFile);
    }

    /**
     * Opens a specific file with automatic encryption detection.
     */
    public Optional<Connection> openDatabase(File dbFile) {

        boolean encrypted = SQLCipherDatabaseHandler.looksEncrypted(dbFile);
        LOG.info("File: " + dbFile.getName() + " | encrypted: " + encrypted);

        if (encrypted) {
            return openEncrypted(dbFile);
        } else {
            return openPlain(dbFile);
        }
    }

    // ── Open encrypted ────────────────────────────────────────────────────

    public static Optional<Connection> openEncrypted(File dbFile) {

        // Show decryption dialog
        Optional<SQLCipherParams> paramsOpt =
                SQLCipherDecryptDialog.show(ownerWindow);

        if (paramsOpt.isEmpty()) {
            LOG.info("User cancelled the decryption dialog.");
            return Optional.empty();
        }

        SQLCipherParams params = paramsOpt.get();

        try {
            Connection conn = SQLCipherDatabaseHandler.openConnection(dbFile, params);
            return Optional.of(conn);

        } catch (SQLException ex) {
            LOG.log(Level.WARNING, "Failed to open encrypted database", ex);
            // Offer retry
            //return askRetry(dbFile);
        }
        return null;
    }

    /** Asks the user whether they want to try again with different parameters. */
    private Optional<Connection> askRetry(File dbFile) {
        javafx.scene.control.Alert alert = new javafx.scene.control.Alert(
                javafx.scene.control.Alert.AlertType.CONFIRMATION);
        alert.initOwner(ownerWindow);
        alert.setTitle("Retry?");
        alert.setHeaderText("Decryption failed");
        alert.setContentText("Would you like to try again with different parameters?");

        Optional<javafx.scene.control.ButtonType> btn = alert.showAndWait();
        if (btn.isPresent() && btn.get() == javafx.scene.control.ButtonType.OK) {
            return openEncrypted(dbFile);
        }
        return Optional.empty();
    }

    // ── Open plain ────────────────────────────────────────────────────────

    public Optional<Connection> openPlain(File dbFile) {
        try {
            Class.forName("org.sqlite.JDBC");
            String url = "jdbc:sqlite:" + dbFile.getAbsolutePath().replace('\\', '/');
            Connection conn = DriverManager.getConnection(url);
            LOG.info("Unencrypted database opened: " + dbFile.getName());
            return Optional.of(conn);
        } catch (ClassNotFoundException | SQLException ex) {
            LOG.log(Level.SEVERE, "Failed to open database", ex);
            showError("Database error",
                      "The database could not be opened.",
                      ex.getMessage());
            return Optional.empty();
        }
    }

    // ── File dialog ───────────────────────────────────────────────────────

    private File chooseFile() {
        FileChooser chooser = new FileChooser();
        chooser.setTitle("Open SQLite database");
        chooser.getExtensionFilters().addAll(
                new FileChooser.ExtensionFilter("SQLite databases",
                        "*.db", "*.sqlite", "*.sqlite3", "*.db3"),
                new FileChooser.ExtensionFilter("All files", "*.*")
        );
        return chooser.showOpenDialog(ownerWindow);
    }

    // ── Error dialog ──────────────────────────────────────────────────────

    private void showError(String title, String header, String detail) {
        javafx.scene.control.Alert alert =
                new javafx.scene.control.Alert(
                        javafx.scene.control.Alert.AlertType.ERROR);
        alert.initOwner(ownerWindow);
        alert.setTitle(title);
        alert.setHeaderText(header);
        alert.setContentText(detail);
        alert.showAndWait();
    }
}
