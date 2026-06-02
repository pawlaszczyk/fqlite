package fqlite.sqlcipher;

import javafx.concurrent.Task;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.stage.FileChooser;
import javafx.stage.Modality;
import javafx.stage.Window;

import java.io.File;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * JavaFX dialog that manages the export of a SQLCipher database
 * to an unencrypted SQLite file.
 *
 * <p>The export runs on a background thread. {@link #show} returns only
 * after the export has fully completed (or failed), so the caller can
 * immediately open the resulting file without any additional waiting.
 *
 * <pre>{@code
 * File plain = SQLCipherExportDialog.show(ownerWindow, encryptedFile, params);
 * if (plain != null) {
 *     // safe to open right here – export is guaranteed to be finished
 *     loadDatabase(plain);
 * }
 * }</pre>
 */
public class SQLCipherExportDialog {

    private SQLCipherExportDialog() { /* Utility class */ }

    /**
     * Runs the full export flow:
     * choose target file → show progress → report success or error.
     *
     * <p>This method blocks the calling (JavaFX application) thread until
     * the export background task has finished, so the returned file is
     * guaranteed to be fully written and ready to open.
     *
     * @return the created plaintext file, or {@code null} if cancelled or failed
     */
    public static File show(Window owner, File encryptedDb, SQLCipherParams params) {

        // ── 1. Choose target file ─────────────────────────────────────
        File target = chooseTargetFile(owner, encryptedDb);
        if (target == null) return null;

        // ── 2. Atomic slot that receives the result from the background thread.
        //       null  = still running / cancelled
        //       File  = success  (the plaintext file)
        //       Exception in task → handled via onFailed, slot stays null
        final AtomicReference<File> result = new AtomicReference<>(null);

        // ── 3. Build progress dialog ──────────────────────────────────
        Dialog<Void> progressDialog = buildProgressDialog(owner);

        // ── 4. Export task ────────────────────────────────────────────
        final File finalTarget = target;
        Task<File> exportTask = new Task<>() {
            @Override
            protected File call() throws Exception {
                SQLCipherExporter.export(encryptedDb, finalTarget, params, true);
                return finalTarget;
            }
        };

        exportTask.setOnSucceeded(e -> {
            // Store the result BEFORE closing the dialog.
            // showAndWait() will only return after this handler completes,
            // so the caller is guaranteed to see a non-null value.
            result.set(exportTask.getValue());
            progressDialog.close();
        });

        exportTask.setOnFailed(e -> {
            // result stays null – close first, then show the error alert
            // so the progress dialog is gone before the error appears.
            progressDialog.close();
            Throwable ex = exportTask.getException();
            ex.printStackTrace();
            showError(owner,
                    "Export failed",
                    ex instanceof SQLException
                            ? "Decryption failed – please check the password or parameters."
                            : "Unexpected error during export.",
                    ex.getMessage());
        });

        // ── 5. Start background thread, then block via showAndWait() ──
        //    showAndWait() pumps the JavaFX event loop, so onSucceeded /
        //    onFailed handlers (which run on the FX thread) are processed
        //    before showAndWait() returns. No external latch needed.
        Thread thread = new Thread(exportTask, "sqlcipher-export");
        thread.setDaemon(true);
        thread.start();

        progressDialog.showAndWait();
        // ← execution resumes here only after progressDialog.close()
        //   has been called from onSucceeded or onFailed, meaning the
        //   task is 100% done and result has already been set.

        // ── 6. Show success notification (outside the progress dialog) ─
        if (result.get() != null) {
            showSuccess(owner, result.get());
        }

        return result.get();
    }

    // ── Helper methods ────────────────────────────────────────────────────

    private static File chooseTargetFile(Window owner, File source) {
        FileChooser chooser = new FileChooser();
        chooser.setTitle("Save decrypted database as …");
        chooser.getExtensionFilters().addAll(
                new FileChooser.ExtensionFilter("SQLite database", "*.db", "*.sqlite"),
                new FileChooser.ExtensionFilter("All files", "*.*")
        );

        // Suggest the same directory with "_decrypted" appended to the name
        String suggestedName = source.getName()
                .replaceFirst("(\\.[^.]+)$", "_decrypted$1");
        chooser.setInitialFileName(suggestedName);
        if (source.getParentFile() != null) {
            chooser.setInitialDirectory(source.getParentFile());
        }

        return chooser.showSaveDialog(owner);
    }

    private static Dialog<Void> buildProgressDialog(Window owner) {
        Dialog<Void> dialog = new Dialog<>();
        dialog.initOwner(owner);
        dialog.initModality(Modality.APPLICATION_MODAL);
        dialog.setTitle("Exporting database");
        dialog.setHeaderText(null);

        ProgressIndicator pi = new ProgressIndicator(-1);  // indeterminate
        pi.setPrefSize(48, 48);

        Label lbl = new Label("Decrypting and exporting database …");

        VBox content = new VBox(16, pi, lbl);
        content.setAlignment(Pos.CENTER);
        content.setPadding(new Insets(28, 40, 20, 40));

        dialog.getDialogPane().setContent(content);
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CANCEL);

        // Cancel button is disabled – the export is atomic and cannot be interrupted
        dialog.getDialogPane().lookupButton(ButtonType.CANCEL).setDisable(true);

        return dialog;
    }

    private static void showSuccess(Window owner, File target) {
        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.initOwner(owner);
        alert.setTitle("Export successful");
        alert.setHeaderText("Database decrypted successfully");
        alert.setContentText("Saved to:\n" + target.getAbsolutePath());
        alert.showAndWait();
    }

    private static void showError(Window owner, String title,
                                  String header, String detail) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.initOwner(owner);
        alert.setTitle(title);
        alert.setHeaderText(header);
        alert.setContentText(detail);
        alert.showAndWait();
    }
}
