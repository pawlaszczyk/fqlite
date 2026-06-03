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
import java.util.concurrent.atomic.AtomicReference;

/**
 * JavaFX dialog for forensic page-level decryption of a SQLCipher database.
 *
 * <p>Runs {@link SQLCipherForensicDecryptor} on a background thread and shows
 * a progress bar. Returns the decrypted file once complete.
 *
 * <pre>{@code
 * File plain = ForensicDecryptDialog.show(stage, encryptedFile, params);
 * if (plain != null) {
 *     // forensically intact plain SQLite file — open it directly
 *     loadDatabase(plain);
 * }
 * }</pre>
 */
public class DecryptProgressDialog {

    private DecryptProgressDialog() { /* Utility class */ }

    /**
     * Shows the target file chooser, runs the decryption, and returns
     * the output file. Blocks the calling (FX application) thread until
     * the background task is fully complete.
     *
     * @return decrypted file, or {@code null} if cancelled or failed
     */
    public static File show(Window owner, File encryptedDb, SQLCipherParams params) {

        // Choose target file
        File target = chooseTargetFile(owner, encryptedDb);
        if (target == null) return null;

        // Result slot — set by onSucceeded before the dialog closes
        AtomicReference<File> result = new AtomicReference<>(null);

        // Progress dialog
        Dialog<Void> progressDialog = buildProgressDialog(owner);
        ProgressBar   progressBar   = (ProgressBar) progressDialog
                .getDialogPane().lookup("#progressBar");
        Label         statusLabel   = (Label) progressDialog
                .getDialogPane().lookup("#statusLabel");

        // Background task
        final File finalTarget = target;
        Task<File> task = new Task<>() {
            @Override
            protected File call() throws Exception {
                int[] lastPct = {-1};
                SQLCipherForensicDecryptor.decrypt(
                        encryptedDb,
                        finalTarget,
                        params.getKey(),
                        (current, total) -> {
                            int pct = current * 100 / total;
                            if (pct != lastPct[0]) {
                                lastPct[0] = pct;
                                updateProgress(current, total);
                                updateMessage(String.format(
                                        "Decrypting page %,d / %,d  (%d %%)",
                                        current, total, pct));
                            }
                        }
                );
                return finalTarget;
            }
        };

        if (progressBar != null) {
            progressBar.progressProperty().bind(task.progressProperty());
        }
        if (statusLabel != null) {
            statusLabel.textProperty().bind(task.messageProperty());
        }

        task.setOnSucceeded(e -> {
            // Set result BEFORE closing — showAndWait() returns only
            // after this handler completes, so the caller sees the value.
            result.set(task.getValue());
            progressDialog.close();
        });

        task.setOnFailed(e -> {
            progressDialog.close();
            Throwable ex = task.getException();
            showError(owner,
                    "Decryption failed",
                    "Could not decrypt the database.",
                    ex.getMessage());
        });

        Thread thread = new Thread(task, "forensic-decrypt");
        thread.setDaemon(true);
        thread.start();

        progressDialog.showAndWait();
        // ← returns only after task completion (onSucceeded/onFailed ran)

        if (result.get() != null) {
            showSuccess(owner, result.get());
        }

        return result.get();
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static File chooseTargetFile(Window owner, File source) {
        FileChooser chooser = new FileChooser();
        chooser.setTitle("Save forensic decrypted database as …");
        chooser.getExtensionFilters().addAll(
                new FileChooser.ExtensionFilter("SQLite database", "*.db", "*.sqlite"),
                new FileChooser.ExtensionFilter("All files", "*.*")
        );
        String suggested = source.getName();
                //.replaceFirst("(\\.[^.]+)$", "_forensic$1");
        suggested += "_encrypted";
        chooser.setInitialFileName(suggested);
        if (source.getParentFile() != null)
            chooser.setInitialDirectory(source.getParentFile());
        return chooser.showSaveDialog(owner);
    }

    private static Dialog<Void> buildProgressDialog(Window owner) {
        Dialog<Void> dialog = new Dialog<>();
        dialog.initOwner(owner);
        dialog.initModality(Modality.APPLICATION_MODAL);
        dialog.setTitle("Forensic decryption");
        dialog.setHeaderText(null);

        ProgressBar pi = new ProgressBar(0);
        pi.setId("progressBar");
        pi.setPrefWidth(360);

        Label lbl = new Label("Preparing …");
        lbl.setId("statusLabel");

        Label hint = new Label(
                "Page-level decryption — preserves free pages,\n" +
                "slack space, and deleted row remnants.");
        hint.setStyle("-fx-font-size: 11px; -fx-text-fill: #666;");

        VBox content = new VBox(12, pi, lbl, hint);
        content.setAlignment(Pos.CENTER_LEFT);
        content.setPadding(new Insets(24, 40, 16, 40));

        dialog.getDialogPane().setContent(content);
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CANCEL);
        dialog.getDialogPane().lookupButton(ButtonType.CANCEL).setDisable(true);

        return dialog;
    }

    private static void showSuccess(Window owner, File target) {
        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.initOwner(owner);
        alert.setTitle("Decryption complete");
        alert.setHeaderText("Forensically intact database created");
        alert.setContentText(
                "All pages decrypted 1:1 — free pages, slack space,\n" +
                "and deleted row remnants are preserved.\n\n" +
                "Saved to:\n" + target.getAbsolutePath());
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
