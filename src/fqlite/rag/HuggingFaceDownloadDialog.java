package fqlite.rag;

import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.VBox;
import javafx.scene.layout.HBox;
import javafx.stage.DirectoryChooser;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.concurrent.Task;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.prefs.Preferences;

/**
 *  This class realizes a download dialog window for
 *  huggingface.co
 *
 *  @author pawlaszc
 */
public class HuggingFaceDownloadDialog {

    private Stage dialog;
    private TextField modelIdField;
    private TextField savePathField;
    private TextField fileNameField;
    private ProgressBar progressBar;
    private Label statusLabel;
    private Label speedLabel;
    private Button downloadButton;
    private Button pauseResumeButton;
    private Button cancelButton;
    private Button historyButton;
    private Spinner<Integer> threadSpinner;
    private String downloadPath;

    private final AtomicBoolean isPaused = new AtomicBoolean(false);
    private final AtomicBoolean isCancelled = new AtomicBoolean(false);
    private Task<Void> currentDownloadTask;
    private final AtomicLong totalBytesDownloaded = new AtomicLong(0);

    private static final String HISTORY_KEY = "download_history";
    private static final int MAX_HISTORY_ENTRIES = 50;
    private final Preferences prefs = Preferences.userNodeForPackage(HuggingFaceDownloadDialog.class);

    /**
     * Download-Historie Eintrag
     */
    public static class DownloadHistoryEntry implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        String modelId;
        String fileName;
        String savePath;
        String timestamp;
        boolean successful;
        long fileSize;

        public DownloadHistoryEntry(String modelId, String fileName, String savePath,
                                    boolean successful, long fileSize) {
            this.modelId = modelId;
            this.fileName = fileName;
            this.savePath = savePath;
            this.successful = successful;
            this.fileSize = fileSize;
            this.timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }

        @Override
        public String toString() {
            String status = successful ? "✓" : "✗";
            String size = fileSize > 0 ? String.format("%.1f MB", fileSize / 1024.0 / 1024.0) : "?";
            return String.format("%s [%s] %s (%s) - %s", status, timestamp, modelId, size,
                    fileName.isEmpty() ? "Repository" : fileName);
        }
    }

    /**
     * Zeigt den Download-Dialog
     *
     * @param owner Das Hauptfenster
     */
    public void show(Stage owner) {
        dialog = new Stage();
        dialog.initModality(Modality.APPLICATION_MODAL);
        dialog.initOwner(owner);
        dialog.setTitle("LLM download from HuggingFace");

        VBox mainLayout = new VBox(15);
        mainLayout.setPadding(new Insets(20));
        mainLayout.setAlignment(Pos.TOP_LEFT);

        // Model ID Eingabe
        Label modelLabel = new Label("Hugging Face Model ID:");
        modelLabel.setStyle("-fx-font-weight: bold;");

        modelIdField = new TextField();
        modelIdField.setPromptText("i.e. meta-llama/Llama-2-7b-chat-hf");
        modelIdField.setPrefWidth(400);

        // Kategorisierte Modell-Vorschläge
        Label categoryLabel = new Label("Category:");
        ComboBox<String> categoryBox = new ComboBox<>();
        categoryBox.setPromptText("Choose a category...");
        categoryBox.setPrefWidth(400);
        categoryBox.getItems().addAll(
                "small model (< 2GB) fastest, but lowest accuracy",
                "midsize model (2-6GB) compromise between speed and accuracy",
                "large model (> 6GB) best quality, but slow"
        );

        ComboBox<String> modelBox = new ComboBox<>();
        modelBox.setPromptText("Choose a model...");
        modelBox.setPrefWidth(400);

        // Modell-Listen nach Kategorie
        categoryBox.setOnAction(e -> {
            String category = categoryBox.getValue();
            modelBox.getItems().clear();

            if (category != null) {
                switch (category) {
                    case "small model (< 2GB) fastest, but lowest accuracy":
                        modelBox.getItems().addAll(
                                "pawlaszc/DigitalForensicsText2SQLite (forensic-sqlite-llama-3.2-3b-Q4_K_M.gguf)");
                        break;
                    case "midsize model (2-6GB) compromise between speed and accuracy":
                        modelBox.getItems().addAll(
                                "pawlaszc/DigitalForensicsText2SQLite (forensic-sqlite-llama-3.2-3b-Q5_K_M.gguf)",
                                "pawlaszc/DigitalForensicsText2SQLite (forensic-sqlite-llama-3.2-3b-Q8_0.gguf)"
                        );
                        break;
                    case "large model (> 6GB) best quality, but slow":
                        modelBox.getItems().addAll(
                                "pawlaszc/DigitalForensicsText2SQLite (forensic-sqlite-llama-3.2-3b-fp16.gguf)"
                        );

                        break;
                }
            }
        });

        modelBox.setOnAction(e -> {
            String selected = modelBox.getValue();
            if (selected != null) {
                String modelId = selected.split(" \\(")[0];
                modelIdField.setText(modelId);

                if (selected.contains("(") && selected.contains(")")) {
                    String fileName = selected.substring(selected.indexOf("(") + 1, selected.indexOf(")"));
                    fileNameField.setText(fileName);
                } else {
                    fileNameField.setText("");
                }
            }
        });

        Label pathLabel = new Label("Save to:");
        pathLabel.setStyle("-fx-font-weight: bold;");

        HBox pathBox = new HBox(10);
        savePathField = new TextField();
        savePathField.setPromptText("Choose a folder...");
        savePathField.setPrefWidth(320);
        savePathField.setEditable(false);

        String defaultPath = System.getProperty("user.home") + File.separator + "llm_models";
        savePathField.setText(defaultPath);
        downloadPath = defaultPath;

        Button browseButton = new Button("Browse...");
        browseButton.setOnAction(e -> chooseDirectory(owner));

        pathBox.getChildren().addAll(savePathField, browseButton);

        // File-Chooser (for GGUF models)
        Label fileLabel = new Label("File (.gguf):");
        fileNameField = new TextField();
        fileNameField.setPromptText("z.B. mistral-7b-instruct-v0.2.Q4_K_M.gguf");

        Label threadLabel = new Label("Number of parallel Download-Threads:");
        HBox threadBox = new HBox(10);
        threadBox.setAlignment(Pos.CENTER_LEFT);

        threadSpinner = new Spinner<>(1, 8, 4);
        threadSpinner.setPrefWidth(80);
        threadSpinner.setEditable(true);

        Label threadInfoLabel = new Label("(more threads = faster, but higher server load)");
        threadInfoLabel.setStyle("-fx-font-size: 10px; -fx-text-fill: #666;");

        threadBox.getChildren().addAll(threadSpinner, threadInfoLabel);

        // Progress
        progressBar = new ProgressBar(0);
        progressBar.setPrefWidth(400);
        progressBar.setVisible(false);

        statusLabel = new Label("");
        statusLabel.setStyle("-fx-text-fill: #666;");

        speedLabel = new Label("");
        speedLabel.setStyle("-fx-text-fill: #666; -fx-font-size: 10px;");

        // Buttons
        HBox buttonBox = new HBox(10);
        buttonBox.setAlignment(Pos.CENTER);

        downloadButton = new Button("Download");
        downloadButton.setStyle("-fx-background-color: #4CAF50; -fx-text-fill: white; -fx-font-weight: bold;");
        downloadButton.setPrefWidth(120);

        pauseResumeButton = new Button("Pause");
        pauseResumeButton.setStyle("-fx-background-color: #FF9800; -fx-text-fill: white; -fx-font-weight: bold;");
        pauseResumeButton.setPrefWidth(120);
        pauseResumeButton.setVisible(false);

        cancelButton = new Button("Cancel");
        cancelButton.setPrefWidth(120);
        cancelButton.setVisible(false);

        historyButton = new Button("History");
        historyButton.setStyle("-fx-background-color: #9C27B0; -fx-text-fill: white; -fx-font-weight: bold;");
        historyButton.setPrefWidth(120);
        historyButton.setOnAction(e -> showHistory(owner));

        Button closeButton = new Button("Close");
        closeButton.setPrefWidth(120);
        closeButton.setOnAction(e -> dialog.close());

        downloadButton.setOnAction(e -> {
            String modelId = modelIdField.getText().trim();
            String fileName = fileNameField.getText().trim();

            if (modelId.isEmpty()) {
                showError("Please enter a valid model id!");
                return;
            }

            if (fileName.isEmpty()) {
                showError("Please supply a file name!");
                return;
            }

            if (!fileName.endsWith(".gguf")) {
                showError("The filename must end with .gguf!");
                return;
            }

            if (downloadPath == null || downloadPath.isEmpty()) {
                showError("Please choose a download path!");
                return;
            }

            int threads = threadSpinner.getValue();
            startDownload(modelId, fileName, threads);
        });

        pauseResumeButton.setOnAction(e -> {
            if (isPaused.get()) {
                resumeDownload();
            } else {
                pauseDownload();
            }
        });

        cancelButton.setOnAction(e -> cancelDownload());

        buttonBox.getChildren().addAll(downloadButton, pauseResumeButton, cancelButton, historyButton, closeButton);

        // Info-Text
        Label infoLabel = new Label(
                "Note: For quantised models (GGUF), the file name is filled in automatically.\n" +
                "Multi-threading significantly speeds up large downloads."
        );
        infoLabel.setStyle("-fx-font-size: 10px; -fx-text-fill: #666;");
        infoLabel.setWrapText(true);

        mainLayout.getChildren().addAll(
                modelLabel, modelIdField,
                categoryLabel, categoryBox,
                modelBox,
                pathLabel, pathBox,
                fileLabel, fileNameField,
                threadLabel, threadBox,
                new Separator(),
                progressBar, statusLabel, speedLabel,
                infoLabel,
                buttonBox
        );

        Scene scene = new Scene(mainLayout, 550, 720);
        dialog.setScene(scene);
        dialog.show();
    }

    private void chooseDirectory(Stage owner) {
        DirectoryChooser chooser = new DirectoryChooser();
        chooser.setTitle("Please Choose Directory");

        File selectedDir = chooser.showDialog(owner);
        if (selectedDir != null) {
            downloadPath = selectedDir.getAbsolutePath();
            savePathField.setText(downloadPath);
        }
    }

    private void startDownload(String modelId, String fileName, int threadCount) {
        downloadButton.setVisible(false);
        pauseResumeButton.setVisible(true);
        cancelButton.setVisible(true);
        progressBar.setVisible(true);
        progressBar.setProgress(0);
        statusLabel.setText("Starting Download...");

        isPaused.set(false);
        isCancelled.set(false);
        totalBytesDownloaded.set(0);
        startSpeedMonitor();

        currentDownloadTask = new Task<>() {
            @Override
            protected Void call() throws Exception {
                boolean success = false;
                long fileSize = 0;

                try {
                    Path targetDir = Paths.get(downloadPath, modelId.replace("/", "_"));
                    Files.createDirectories(targetDir);

                    // NUR NOCH GGUF-Download
                    fileSize = downloadSingleFileMultiThreaded(modelId, fileName, targetDir, threadCount);
                    success = !isCancelled.get();

                    if (!isCancelled.get()) {
                        updateMessage("Download finished!");
                        updateProgress(1, 1);
                    }

                } catch (Exception e) {
                    if (!isCancelled.get()) {
                        updateMessage("Fehler: " + e.getMessage());
                        throw e;
                    }
                } finally {
                    if (success || isCancelled.get()) {
                        addToHistory(new DownloadHistoryEntry(modelId, fileName, downloadPath, success, fileSize));
                    }
                }
                return null;
            }
        };


        currentDownloadTask.setOnSucceeded(e -> {
            statusLabel.setText("✓ Download finished!");
            statusLabel.setStyle("-fx-text-fill: green; -fx-font-weight: bold;");
            resetButtons();
            showSuccess("Model was successfully downloaded!");
        });

        currentDownloadTask.setOnFailed(e -> {
            if (!isCancelled.get()) {
                statusLabel.setText("✗ Download failed!");
                statusLabel.setStyle("-fx-text-fill: red; -fx-font-weight: bold;");
                resetButtons();
                showError("Download failed: " + currentDownloadTask.getException().getMessage());
            }
        });

        currentDownloadTask.setOnCancelled(e -> {
            statusLabel.setText("Download cancelled!");
            statusLabel.setStyle("-fx-text-fill: orange;");
            resetButtons();
        });

        currentDownloadTask.messageProperty().addListener((obs, oldMsg, newMsg) -> Platform.runLater(() -> statusLabel.setText(newMsg)));

        currentDownloadTask.progressProperty().addListener((obs, oldProgress, newProgress) -> Platform.runLater(() -> progressBar.setProgress(newProgress.doubleValue())));

        new Thread(currentDownloadTask).start();
    }

    private void startSpeedMonitor() {
        Thread speedMonitor = new Thread(() -> {
            long lastBytes = 0;
            long lastTime = System.currentTimeMillis();

            while (!isCancelled.get() && currentDownloadTask != null && currentDownloadTask.isRunning()) {
                try {
                    Thread.sleep(1000);

                    if (!isPaused.get()) {
                        long currentBytes = totalBytesDownloaded.get();
                        long currentTime = System.currentTimeMillis();

                        long bytesPerSecond = (currentBytes - lastBytes) * 1000 / (currentTime - lastTime);
                        double mbPerSecond = bytesPerSecond / 1024.0 / 1024.0;

                        Platform.runLater(() -> speedLabel.setText(String.format("Download-Speed: %.2f MB/s", mbPerSecond)));

                        lastBytes = currentBytes;
                        lastTime = currentTime;
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        speedMonitor.setDaemon(true);
        speedMonitor.start();
    }

    private long downloadSingleFileMultiThreaded(String modelId, String fileName, Path targetDir, int threadCount)
            throws IOException, InterruptedException, ExecutionException {

        String urlString = String.format("https://huggingface.co/%s/resolve/main/%s", modelId, fileName);
        Path targetFile = targetDir.resolve(fileName);

        // determine file size
        HttpURLConnection connection = (HttpURLConnection) new URL(urlString).openConnection();
        connection.setRequestMethod("HEAD");
        connection.setRequestProperty("User-Agent", "Mozilla/5.0");

        long fileSize = connection.getContentLengthLong();
        connection.disconnect();

        if (fileSize <= 0) {
            // fallback to single-thread
            downloadSingleFileWithResume(modelId, fileName, targetDir);
            return fileSize;
        }

        // check if server support range downloads
        connection = (HttpURLConnection) new URL(urlString).openConnection();
        connection.setRequestMethod("HEAD");
        connection.setRequestProperty("User-Agent", "Mozilla/5.0");
        String acceptRanges = connection.getHeaderField("Accept-Ranges");
        connection.disconnect();

        if (acceptRanges == null || !acceptRanges.equals("bytes")) {
            // server does not accept ranges -> use fallback
            downloadSingleFileWithResume(modelId, fileName, targetDir);
            return fileSize;
        }

        // Multi-threaded Download
        long chunkSize = fileSize / threadCount;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<Void>> futures = new ArrayList<>();

        // create a temporary chunk file for the download
        List<Path> chunkFiles = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            long start = i * chunkSize;
            long end = (i == threadCount - 1) ? fileSize - 1 : (start + chunkSize - 1);
            Path chunkFile = targetDir.resolve(fileName + ".part" + i);
            chunkFiles.add(chunkFile);

            final int threadIndex = i;
            Future<Void> future = executor.submit(() -> {
                downloadChunk(urlString, chunkFile, start, end, threadIndex, fileSize);
                return null;
            });
            futures.add(future);
        }

        // wait for all threads to end
        for (Future<Void> future : futures) {
            future.get();
        }

        executor.shutdown();

        if (isCancelled.get()) {
            // Cleanup
            for (Path chunkFile : chunkFiles) {
                Files.deleteIfExists(chunkFile);
            }
            return fileSize;
        }

        // merge chunks
        Platform.runLater(() -> statusLabel.setText("merge chunks"));

        try (FileOutputStream out = new FileOutputStream(targetFile.toFile())) {
            for (Path chunkFile : chunkFiles) {
                Files.copy(chunkFile, out);
                Files.delete(chunkFile);
            }
        }

        return fileSize;
    }

    private void downloadChunk(String urlString, Path chunkFile, long start, long end,
                               int threadIndex, long totalFileSize) throws IOException, InterruptedException {

        long existingSize = 0;
        if (Files.exists(chunkFile)) {
            existingSize = Files.size(chunkFile);
            start += existingSize;
        }

        if (start > end) {
            return; // Chunk is already complete
        }

        HttpURLConnection connection = (HttpURLConnection) new URL(urlString).openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("User-Agent", "Mozilla/5.0");
        connection.setRequestProperty("Range", "bytes=" + start + "-" + end);

        try (InputStream in = connection.getInputStream();
             FileOutputStream out = new FileOutputStream(chunkFile.toFile(), existingSize > 0)) {

            byte[] buffer = new byte[8192];
            int bytesRead;

            while ((bytesRead = in.read(buffer)) != -1 && !isCancelled.get()) {
                while (isPaused.get() && !isCancelled.get()) {
                    synchronized (isPaused) {
                        isPaused.wait(100);
                    }
                }

                if (isCancelled.get()) {
                    break;
                }

                out.write(buffer, 0, bytesRead);

                long downloaded = totalBytesDownloaded.addAndGet(bytesRead);

                if (threadIndex == 0) {
                    double progress = (double) downloaded / totalFileSize;
                    Platform.runLater(() -> {
                        progressBar.setProgress(progress);
                        statusLabel.setText(String.format("%.1f MB / %.1f MB (%.1f%%)",
                                downloaded / 1024.0 / 1024.0,
                                totalFileSize / 1024.0 / 1024.0,
                                progress * 100));
                    });
                }
            }
        }
    }

    private void pauseDownload() {
        isPaused.set(true);
        pauseResumeButton.setText("Continue");
        pauseResumeButton.setStyle("-fx-background-color: #2196F3; -fx-text-fill: white; -fx-font-weight: bold;");
        statusLabel.setText("⏸ Download paused.");
    }

    private void resumeDownload() {
        isPaused.set(false);
        pauseResumeButton.setText("Pause");
        pauseResumeButton.setStyle("-fx-background-color: #FF9800; -fx-text-fill: white; -fx-font-weight: bold;");
        statusLabel.setText("▶ Download continued.");

        synchronized (isPaused) {
            isPaused.notifyAll();
        }
    }

    private void cancelDownload() {
        isCancelled.set(true);
        if (currentDownloadTask != null) {
            currentDownloadTask.cancel();
        }

        synchronized (isPaused) {
            isPaused.notifyAll();
        }
    }

    private void resetButtons() {
        downloadButton.setVisible(true);
        pauseResumeButton.setVisible(false);
        cancelButton.setVisible(false);
        pauseResumeButton.setText("Pause");
        speedLabel.setText("");
    }

    private void downloadSingleFileWithResume(String modelId, String fileName, Path targetDir)
            throws IOException, InterruptedException {

        String urlString = String.format("https://huggingface.co/%s/resolve/main/%s", modelId, fileName);
        Path targetFile = targetDir.resolve(fileName);

        long existingSize = 0;
        if (Files.exists(targetFile)) {
            existingSize = Files.size(targetFile);
        }

        final long finalExistingSize = existingSize;

        HttpURLConnection connection = (HttpURLConnection) new URL(urlString).openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("User-Agent", "Mozilla/5.0");

        if (finalExistingSize > 0) {
            connection.setRequestProperty("Range", "bytes=" + finalExistingSize + "-");
            Platform.runLater(() -> statusLabel.setText("Continue download at " + (finalExistingSize / 1024 / 1024) + " MB"));
        }

        long fileSize = connection.getContentLengthLong();
        if (finalExistingSize > 0) {
            fileSize += finalExistingSize;
        }

        final long finalFileSize = fileSize;

        try (InputStream in = connection.getInputStream();
             FileOutputStream out = new FileOutputStream(targetFile.toFile(), finalExistingSize > 0)) { // GEÄNDERT

            byte[] buffer = new byte[8192];
            long downloaded = finalExistingSize;
            int bytesRead;

            while ((bytesRead = in.read(buffer)) != -1 && !isCancelled.get()) {
                while (isPaused.get() && !isCancelled.get()) {
                    synchronized (isPaused) {
                        isPaused.wait(100);
                    }
                }

                if (isCancelled.get()) {
                    break;
                }

                out.write(buffer, 0, bytesRead);
                downloaded += bytesRead;
                totalBytesDownloaded.addAndGet(bytesRead);

                if (finalFileSize > 0) {
                    final long finalDownloaded = downloaded;
                    double progress = (double) finalDownloaded / finalFileSize;

                    Platform.runLater(() -> {
                        progressBar.setProgress(progress);
                        statusLabel.setText(String.format("%.1f MB / %.1f MB (%.1f%%)",
                                finalDownloaded / 1024.0 / 1024.0,
                                finalFileSize / 1024.0 / 1024.0,
                                progress * 100));
                    });
                }
            }
        }
    }


    /**
     * Show the download history
     */
    private void showHistory(Stage owner) {
        Stage historyStage = new Stage();
        historyStage.initModality(Modality.APPLICATION_MODAL);
        historyStage.initOwner(owner);
        historyStage.setTitle("Download-History");

        VBox layout = new VBox(15);
        layout.setPadding(new Insets(20));

        Label titleLabel = new Label("Previous Downloads:");
        titleLabel.setStyle("-fx-font-weight: bold; -fx-font-size: 14px;");

        ListView<DownloadHistoryEntry> listView = new ListView<>();
        listView.setPrefHeight(400);
        listView.setPrefWidth(600);

        ObservableList<DownloadHistoryEntry> historyList = FXCollections.observableArrayList(loadHistory());
        listView.setItems(historyList);

        HBox buttonBox = new HBox(10);
        buttonBox.setAlignment(Pos.CENTER);

        Button redownloadButton = new Button("Download again");
        redownloadButton.setOnAction(e -> {
            DownloadHistoryEntry selected = listView.getSelectionModel().getSelectedItem();
            if (selected != null) {
                modelIdField.setText(selected.modelId);
                // fileName würde hier gesetzt werden, wenn in der History gespeichert
                historyStage.close();
            } else {
                showError("Please select an entry!");
            }
        });

        Button clearButton = new Button("Remove History");
        clearButton.setOnAction(e -> {
            Alert confirm = new Alert(Alert.AlertType.CONFIRMATION);
            confirm.setTitle("Confirmation");
            confirm.setHeaderText("Do you really want to clear history?");
            confirm.setContentText("This action cannot be undone!");

            if (confirm.showAndWait().get() == ButtonType.OK) {
                clearHistory();
                historyList.clear();
            }
        });

        Button closeButton = new Button("Close");
        closeButton.setOnAction(e -> historyStage.close());

        buttonBox.getChildren().addAll(redownloadButton, clearButton, closeButton);

        layout.getChildren().addAll(titleLabel, listView, buttonBox);

        Scene scene = new Scene(layout, 650, 500);
        historyStage.setScene(scene);
        historyStage.show();
    }

    /**
     * Save a download to history.
     */
    private void addToHistory(DownloadHistoryEntry entry) {
        List<DownloadHistoryEntry> history = loadHistory();
        history.addFirst(entry); // Newest first

        // set a limit for MAX_HISTORY_ENTRIES
        if (history.size() > MAX_HISTORY_ENTRIES) {
            history = history.subList(0, MAX_HISTORY_ENTRIES);
        }

        saveHistory(history);
    }

    /**
     * Load download histroy
     */
    private List<DownloadHistoryEntry> loadHistory() {
        String historyData = prefs.get(HISTORY_KEY, "");
        List<DownloadHistoryEntry> history = new ArrayList<>();

        if (!historyData.isEmpty()) {
            try (ObjectInputStream ois = new ObjectInputStream(
                    new ByteArrayInputStream(java.util.Base64.getDecoder().decode(historyData)))) {
                history = (List<DownloadHistoryEntry>) ois.readObject();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return history;
    }

    /**
     * Save download history.
     */
    private void saveHistory(List<DownloadHistoryEntry> history) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(history);
            String encoded = java.util.Base64.getEncoder().encodeToString(baos.toByteArray());
            prefs.put(HISTORY_KEY, encoded);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Clear the complete history.
     */
    private void clearHistory() {
        prefs.remove(HISTORY_KEY);
    }

    private void showError(String message) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Error");
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.showAndWait();
    }

    private void showSuccess(String message) {
        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle("Success");
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.showAndWait();
    }
}