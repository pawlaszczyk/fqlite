package fqlite.importer;

import javafx.concurrent.Task;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.*;
import javafx.scene.layout.VBox;
import javafx.stage.Modality;
import javafx.stage.Window;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/// JavaFX progress dialog for {@link EtsiXmlImporter}.
///
/// Runs the XML-to-SQLite conversion on a background thread and shows a
/// progress bar while doing so. Returns the freshly created SQLite database
/// once the import is complete, so the caller can simply continue opening
/// it like any other database file.
///
/// One or more XML files can be imported in a single call; if more than one
/// is given, all of them are merged into the same target database (each
/// imported row records the file it came from in its {@code source_file}
/// column — see {@link EtsiXmlImporter}).
///
/// <pre>{@code
/// File db = EtsiXmlImportDialog.show(stage, xmlFiles, targetDb);
/// if (db != null) {
///     open_db(db);
/// }
/// }</pre>
///
/// @author D. Pawlaszczyk
public class EtsiXmlImportDialog {

    private EtsiXmlImportDialog() { /* utility class */ }

    /**
     * Runs the import and blocks the calling (FX application) thread until
     * the background task is fully complete.
     *
     * @param owner    owner window for the modal dialog
     * @param xmlFiles the ETSI Retained Data XML file(s) to import; if more
     *                 than one, all are merged into the same {@code dbFile}
     * @param dbFile   target SQLite database to create
     * @return the created SQLite database file, or {@code null} on failure
     */
    public static File show(Window owner, List<File> xmlFiles, File dbFile) {

        AtomicReference<File> result = new AtomicReference<>(null);

        Dialog<Void> progressDialog = buildProgressDialog(owner);
        ProgressBar progressBar = (ProgressBar) progressDialog.getDialogPane().lookup("#progressBar");
        Label statusLabel = (Label) progressDialog.getDialogPane().lookup("#statusLabel");

        Task<File> task = new Task<>() {
            @Override
            protected File call() throws Exception {
                final String[] currentFileLabel = { "" };
                updateMessage("Reading " + xmlFiles.get(0).getName() + " ...");
                EtsiXmlImporter.importXmlFilesToSqlite(xmlFiles, dbFile,
                        (fileIndex, totalFiles, fileName) -> {
                            currentFileLabel[0] = (totalFiles > 1)
                                    ? String.format("Datei %d/%d (%s)", fileIndex, totalFiles, fileName)
                                    : fileName;
                            updateMessage("Reading " + currentFileLabel[0] + " ...");
                        },
                        (current, total) -> {
                            if (total > 0) {
                                updateProgress(current, total);
                                updateMessage(String.format("%s: Importing record %,d / %,d ...",
                                        currentFileLabel[0], current, total));
                            }
                        });
                return dbFile;
            }
        };

        if (progressBar != null) {
            progressBar.progressProperty().bind(task.progressProperty());
        }
        if (statusLabel != null) {
            statusLabel.textProperty().bind(task.messageProperty());
        }

        task.setOnSucceeded(e -> {
            result.set(task.getValue());
            progressDialog.close();
        });

        task.setOnFailed(e -> {
            progressDialog.close();
            Throwable ex = task.getException();
            showError(owner, "Import failed",
                    xmlFiles.size() > 1 ? "Could not import the ETSI XML files." : "Could not import the ETSI XML file.",
                    ex != null ? ex.getMessage() : "Unknown error.");
        });

        Thread thread = new Thread(task, "etsi-xml-import");
        thread.setDaemon(true);
        thread.start();

        progressDialog.showAndWait();
        // returns only after task completion (onSucceeded/onFailed ran)

        return result.get();
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static Dialog<Void> buildProgressDialog(Window owner) {
        Dialog<Void> dialog = new Dialog<>();
        dialog.initOwner(owner);
        dialog.initModality(Modality.APPLICATION_MODAL);
        dialog.setTitle("ETSI XML Import");
        dialog.setHeaderText(null);

        ProgressBar pi = new ProgressBar(0);
        pi.setId("progressBar");
        pi.setPrefWidth(360);

        Label lbl = new Label("Preparing ...");
        lbl.setId("statusLabel");

        Label hint = new Label("Converting ETSI Retained Data XML (TS 102 657)\ninto a SQLite database ...");
        hint.setStyle("-fx-font-size: 11px; -fx-text-fill: #666;");

        VBox content = new VBox(12, pi, lbl, hint);
        content.setAlignment(Pos.CENTER_LEFT);
        content.setPadding(new Insets(24, 40, 16, 40));

        dialog.getDialogPane().setContent(content);
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CANCEL);
        dialog.getDialogPane().lookupButton(ButtonType.CANCEL).setDisable(true);

        return dialog;
    }

    private static void showError(Window owner, String title, String header, String detail) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.initOwner(owner);
        alert.setTitle(title);
        alert.setHeaderText(header);
        alert.setContentText(detail);
        alert.showAndWait();
    }
}
