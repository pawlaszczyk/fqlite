package fqlite.rag;

import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.*;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javafx.stage.Window;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Controls for the local LLM: model file (with download), GPU offload and
 * sampling. Used as the "LLM" tab of the settings dialog and as content of
 * the stand-alone {@link LLMConfigDialog}; both read and write
 * {@link LlmSettings}.
 */
public class LlmSettingsPane extends VBox {

    private static final String[] GPU_MODES = {
            "Automatic (recommended)",
            "CPU only",
            "GPU – all layers",
            "GPU – number of layers" };

    /** result of the GPU detection, computed once */
    private static volatile String autoGpu;

    private final TextField modelPathField = new TextField();
    private final Label modelStatus = new Label();
    private final ComboBox<String> gpuMode = new ComboBox<>();
    private final Spinner<Integer> gpuLayers = intSpinner(1, 999, LlmSettings.DEF_GPU_LAYERS, 1);
    private final Label gpuInfo = new Label();
    private final Spinner<Double> temperature = doubleSpinner(0.0, 2.0, LlmSettings.DEF_TEMPERATURE, 0.1);
    private final Spinner<Double> topP = doubleSpinner(0.0, 1.0, LlmSettings.DEF_TOP_P, 0.05);
    private final Spinner<Integer> topK = intSpinner(1, 100, LlmSettings.DEF_TOP_K, 5);
    private final Spinner<Integer> maxTokens = intSpinner(16, LlmSettings.CONTEXT_SIZE, LlmSettings.DEF_MAX_TOKENS, 64);
    private final Spinner<Double> frequencyPenalty = doubleSpinner(-2.0, 2.0, LlmSettings.DEF_FREQUENCY_PENALTY, 0.1);
    private final Spinner<Double> presencePenalty = doubleSpinner(-2.0, 2.0, LlmSettings.DEF_PRESENCE_PENALTY, 0.1);
    private final Label temperatureWarning = new Label(
            "Temperature above 0: the same question can give a different SQL query each time.");

    public LlmSettingsPane() {
        setSpacing(10);
        setPadding(new Insets(12));

        // ── model file ──────────────────────────────────────────────────────
        modelPathField.setPromptText("Path to the GGUF model file");
        modelPathField.textProperty().addListener((o, a, b) -> updateModelStatus());
        HBox.setHgrow(modelPathField, Priority.ALWAYS);
        Button browse = new Button("Browse…");
        browse.setOnAction(e -> browseModelFile());
        Button download = new Button("Download LLM…");
        download.setTooltip(new Tooltip("Download a model from Hugging Face.\n"
                + "When the download is finished, the model is set here and saved."));
        download.setOnAction(e -> openDownloadDialog());
        HBox pathRow = new HBox(8, modelPathField, browse, download);
        pathRow.setAlignment(Pos.CENTER_LEFT);
        modelStatus.setWrapText(true);
        modelStatus.setStyle("-fx-font-size: 10; -fx-font-family: monospace;");

        // ── GPU ─────────────────────────────────────────────────────────────
        gpuMode.getItems().addAll(GPU_MODES);
        gpuMode.getSelectionModel().select(0);
        gpuMode.valueProperty().addListener((o, a, b) -> updateGpuControls());
        gpuMode.setTooltip(new Tooltip("Automatic: Metal on Apple Silicon, CUDA with an NVIDIA GPU\n"
                + "(nvidia-smi), otherwise CPU. If the model does not load on the GPU,\n"
                + "FQLite falls back to the CPU."));
        gpuLayers.setTooltip(new Tooltip("Layers of the model that run on the GPU (the 3B models have 28)"));
        HBox gpuRow = new HBox(8, gpuMode, new Label("Layers:"), gpuLayers);
        gpuRow.setAlignment(Pos.CENTER_LEFT);
        gpuInfo.setWrapText(true);
        gpuInfo.setStyle("-fx-font-size: 10;");

        // ── sampling ────────────────────────────────────────────────────────
        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(8);
        int r = 0;
        r = addRow(grid, r, "Temperature:", temperature, "(0.0 – 2.0)",
                "Randomness of the output. 0 = always the most likely token (reproducible).");
        r = addRow(grid, r, "Top P:", topP, "(0.0 – 1.0)",
                "Nucleus sampling: only tokens within this cumulative probability. No effect at temperature 0.");
        r = addRow(grid, r, "Top K:", topK, "(1 – 100)",
                "Only the K most likely tokens are considered. No effect at temperature 0.");
        r = addRow(grid, r, "Max. tokens:", maxTokens, "(16 – " + LlmSettings.CONTEXT_SIZE + ")",
                "Maximum length of the generated SQL. Limited to what is left of the\n"
                        + "context window (" + LlmSettings.CONTEXT_SIZE + " tokens) after the prompt.");
        r = addRow(grid, r, "Frequency penalty:", frequencyPenalty, "(-2.0 – 2.0)",
                "Penalises tokens by how often they already occur. SQL repeats column\n"
                        + "names on purpose, so values above 0 usually make the query worse.");
        addRow(grid, r, "Presence penalty:", presencePenalty, "(-2.0 – 2.0)",
                "Penalises tokens that already occur. Values above 0 usually make the query worse.");
        temperature.valueProperty().addListener((o, a, b) -> updateTemperatureWarning());
        temperatureWarning.setWrapText(true);
        temperatureWarning.setStyle("-fx-text-fill: #c06000; -fx-font-size: 11;");

        Button defaults = new Button("Recommended values");
        defaults.setTooltip(new Tooltip("Values the models were evaluated with:\n"
                + "temperature 0, top P 0.9, top K 40, max. 384 tokens, no penalties"));
        defaults.setOnAction(e -> setSampling(new LlmSettings()));

        Label samplingHint = new Label("Apply to the SQL generation in Lighthouse and in the Text2SQL "
                + "window. Recognising the kind of question always runs with temperature 0.");
        samplingHint.setWrapText(true);
        samplingHint.setStyle("-fx-opacity: 0.75; -fx-font-size: 11;");

        Label fileHint = new Label("Stored in " + LlmSettings.file()
                + ". Changing the model or the GPU setting reloads the model before the next question.");
        fileHint.setWrapText(true);
        fileHint.setStyle("-fx-opacity: 0.75; -fx-font-size: 11;");

        getChildren().addAll(
                heading("Model"), pathRow, modelStatus,
                heading("GPU"), gpuRow, gpuInfo,
                heading("Sampling"), grid, temperatureWarning, defaults, samplingHint,
                new Separator(), fileHint);

        load(LlmSettings.load());
    }

    // ── values ────────────────────────────────────────────────────────────

    /** Shows {@code s} in the controls. */
    public final void load(LlmSettings s) {
        modelPathField.setText(s.modelPath);
        gpuMode.getSelectionModel().select(s.gpuMode.ordinal());
        gpuLayers.getValueFactory().setValue(s.gpuLayers);
        setSampling(s);
        updateGpuControls();
        updateModelStatus();
    }

    private void setSampling(LlmSettings s) {
        temperature.getValueFactory().setValue(s.temperature);
        topP.getValueFactory().setValue(s.topP);
        topK.getValueFactory().setValue(s.topK);
        maxTokens.getValueFactory().setValue(s.maxTokens);
        frequencyPenalty.getValueFactory().setValue(s.frequencyPenalty);
        presencePenalty.getValueFactory().setValue(s.presencePenalty);
        updateTemperatureWarning();
    }

    /** The values of the controls (text typed into a spinner counts as well). */
    public LlmSettings values() {
        LlmSettings s = new LlmSettings();
        s.modelPath = modelPathField.getText() == null ? "" : modelPathField.getText().trim();
        int mode = Math.max(0, gpuMode.getSelectionModel().getSelectedIndex());
        s.gpuMode = LlmSettings.GpuMode.values()[mode];
        s.gpuLayers = commitInt(gpuLayers);
        s.temperature = commitDouble(temperature);
        s.topP = commitDouble(topP);
        s.topK = commitInt(topK);
        s.maxTokens = commitInt(maxTokens);
        s.frequencyPenalty = commitDouble(frequencyPenalty);
        s.presencePenalty = commitDouble(presencePenalty);
        return s.clamped();
    }

    /**
     * Saves the values. Shows an error and returns false if the file
     * cannot be written.
     */
    public boolean save() {
        try {
            values().save();
            return true;
        } catch (IOException e) {
            Alert a = new Alert(Alert.AlertType.ERROR,
                    "The LLM settings could not be saved:\n" + e.getMessage());
            a.initOwner(getScene() == null ? null : getScene().getWindow());
            a.setHeaderText(null);
            a.showAndWait();
            return false;
        }
    }

    // ── model file ────────────────────────────────────────────────────────

    private void browseModelFile() {
        FileChooser fc = new FileChooser();
        fc.setTitle("Select GGUF model file");
        fc.getExtensionFilters().addAll(
                new FileChooser.ExtensionFilter("GGUF model", "*.gguf"),
                new FileChooser.ExtensionFilter("All files", "*.*"));
        File current = new File(modelPathField.getText().trim());
        File dir = current.isDirectory() ? current : current.getParentFile();
        if (dir == null || !dir.isDirectory()) dir = new File(System.getProperty("user.home"), "llm_models");
        if (dir.isDirectory()) fc.setInitialDirectory(dir);
        File f = fc.showOpenDialog(window());
        if (f != null) modelPathField.setText(f.getAbsolutePath());
    }

    private void openDownloadDialog() {
        HuggingFaceDownloadDialog dd = new HuggingFaceDownloadDialog();
        dd.setOnDownloaded(this::modelDownloaded);
        Window w = window();
        dd.show(w instanceof Stage st ? st : null);
    }

    /**
     * A download has finished: enter the file and save it right away, so the
     * model is set even if this dialog is cancelled afterwards (the other,
     * unsaved values of the pane stay unsaved).
     */
    private void modelDownloaded(Path file) {
        Runnable r = () -> {
            modelPathField.setText(file.toAbsolutePath().toString());
            try {
                LlmSettings.saveModelPath(file.toAbsolutePath().toString());
                modelStatus.setText(modelStatus.getText() + "\nDownloaded model set and saved.");
            } catch (IOException e) {
                modelStatus.setText(modelStatus.getText() + "\nDownloaded model set; not saved yet: " + e.getMessage());
            }
        };
        if (Platform.isFxApplicationThread()) r.run(); else Platform.runLater(r);
    }

    private void updateModelStatus() {
        String t = modelPathField.getText() == null ? "" : modelPathField.getText().trim();
        String status;
        if (t.isEmpty()) {
            status = "⚪ No model file set";
        } else {
            Path p = Path.of(t);
            if (!Files.isRegularFile(p)) {
                status = "❌ File not found";
            } else if (!t.toLowerCase().endsWith(".gguf")) {
                status = "⚠️ Not a .gguf file";
            } else {
                long size = 0;
                try { size = Files.size(p); } catch (IOException ignored) { }
                status = String.format("✅ %.1f GB", size / 1e9);
            }
        }
        String loaded = RAGPipeline.loadedModelInfo();
        modelStatus.setText(status + (loaded != null ? "   ·   loaded now: " + loaded : "   ·   not loaded yet"));
    }

    // ── GPU ───────────────────────────────────────────────────────────────

    private void updateGpuControls() {
        int i = gpuMode.getSelectionModel().getSelectedIndex();
        gpuLayers.setDisable(i != LlmSettings.GpuMode.CUSTOM.ordinal());
        String info;
        if (RAGPipeline.gpuOverriddenBySystemProperty()) {
            info = "⚠️ The Java option -Dfqlite.llm.gpuLayers=" + System.getProperty("fqlite.llm.gpuLayers")
                    + " is set and takes precedence over this setting.";
        } else if (i == LlmSettings.GpuMode.AUTO.ordinal()) {
            String d = autoGpu;
            if (d == null) {
                info = "On this computer: detecting…";
                // nvidia-smi may take up to 2 s: not on the FX thread
                Thread t = new Thread(() -> {
                    autoGpu = RAGPipeline.autoGpuDescription();
                    Platform.runLater(this::updateGpuControls);
                }, "llm-gpu-detect");
                t.setDaemon(true);
                t.start();
            } else {
                info = "On this computer: " + d + ".";
            }
        } else {
            info = "";
        }
        gpuInfo.setText(info);
        gpuInfo.setVisible(!info.isEmpty());
        gpuInfo.setManaged(!info.isEmpty());
    }

    private void updateTemperatureWarning() {
        Double t = temperature.getValue();
        boolean on = t != null && t > 0.0;
        temperatureWarning.setVisible(on);
        temperatureWarning.setManaged(on);
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private Window window() {
        return getScene() == null ? null : getScene().getWindow();
    }

    private static Label heading(String text) {
        Label l = new Label(text);
        l.setFont(Font.font("Verdana", FontWeight.BOLD, 12));
        return l;
    }

    private static int addRow(GridPane g, int row, String label, Spinner<?> sp, String range, String tip) {
        Label l = new Label(label);
        l.setTooltip(new Tooltip(tip));
        sp.setTooltip(new Tooltip(tip));
        Label rl = new Label(range);
        rl.setStyle("-fx-opacity: 0.7;");
        g.add(l, 0, row);
        g.add(sp, 1, row);
        g.add(rl, 2, row);
        return row + 1;
    }

    private static Spinner<Integer> intSpinner(int min, int max, int initial, int step) {
        Spinner<Integer> sp = new Spinner<>(new SpinnerValueFactory.IntegerSpinnerValueFactory(min, max, initial, step));
        sp.setEditable(true);
        sp.setPrefWidth(110);
        return sp;
    }

    private static Spinner<Double> doubleSpinner(double min, double max, double initial, double step) {
        SpinnerValueFactory.DoubleSpinnerValueFactory f =
                new SpinnerValueFactory.DoubleSpinnerValueFactory(min, max, initial, step);
        // always with a dot and two decimals, independent of the locale
        f.setConverter(new javafx.util.StringConverter<>() {
            @Override public String toString(Double d) {
                return d == null ? "" : String.format(java.util.Locale.ROOT, "%.2f", d);
            }
            @Override public Double fromString(String s) {
                try {
                    return Double.parseDouble(s.trim().replace(',', '.'));
                } catch (NumberFormatException | NullPointerException e) {
                    return f.getValue();
                }
            }
        });
        Spinner<Double> sp = new Spinner<>(f);
        sp.setEditable(true);
        sp.setPrefWidth(110);
        return sp;
    }

    /** Value of an editable spinner including text that was typed but not committed yet. */
    private static int commitInt(Spinner<Integer> sp) {
        try {
            int v = Integer.parseInt(sp.getEditor().getText().trim().replace(".", "").replace(",", ""));
            sp.getValueFactory().setValue(v);
        } catch (NumberFormatException ignored) {
            // keep the last valid value
        }
        return sp.getValue();
    }

    private static double commitDouble(Spinner<Double> sp) {
        try {
            double v = Double.parseDouble(sp.getEditor().getText().trim().replace(',', '.'));
            sp.getValueFactory().setValue(v);
        } catch (NumberFormatException ignored) {
            // keep the last valid value
        }
        return sp.getValue();
    }
}
