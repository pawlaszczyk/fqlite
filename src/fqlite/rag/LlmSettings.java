package fqlite.rag;

import fqlite.base.Global;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Properties;

/**
 * Settings of the local LLM (model file, GPU offload, sampling), stored in
 * {@code ~/.fqlite/fqlitellm-config.properties}.
 *
 * <p>The model file and the GPU setting take effect when the model is
 * loaded; {@link RAGPipeline} reloads the model before the next request when
 * either has changed. The sampling values apply to every SQL generation
 * (free-form and ETSI-CDR, including the retry with the SQLite error). The
 * intent classification always runs greedily, because its JSON answer has
 * to be stable.</p>
 *
 * <p>Files written before version 2 come from the old dialog, whose values
 * apart from {@code model_path} were never used (e.g. {@code gpu_layers=0},
 * {@code max_tokens=4096}). Only the model path is taken from such a file;
 * everything else starts with the defaults, so that updating FQLite does not
 * suddenly switch an Apple-Silicon machine to CPU-only.</p>
 *
 * <p>Instances are plain value objects. {@link #get()} returns the
 * settings currently in force; {@link #save()} writes them and makes them
 * the current ones.</p>
 */
public final class LlmSettings {

    /** format version of the properties file */
    public static final int VERSION = 2;

    /** context window used when loading the model (tokens) */
    public static final int CONTEXT_SIZE = 4096;

    /** How the transformer layers are distributed between GPU and CPU. */
    public enum GpuMode {
        /** Metal on Apple Silicon, all layers on an NVIDIA GPU found by nvidia-smi, else CPU */
        AUTO,
        /** CPU only */
        CPU,
        /** all layers on the GPU */
        ALL,
        /** {@link #gpuLayers} layers on the GPU */
        CUSTOM
    }

    // ── defaults: the decoding the model was evaluated with ────────────────
    public static final double DEF_TEMPERATURE = 0.0;
    public static final double DEF_TOP_P = 0.9;
    public static final int DEF_TOP_K = 40;
    public static final int DEF_MAX_TOKENS = 384;
    public static final double DEF_FREQUENCY_PENALTY = 0.0;
    public static final double DEF_PRESENCE_PENALTY = 0.0;
    public static final int DEF_GPU_LAYERS = 32;

    public String modelPath = "";
    public GpuMode gpuMode = GpuMode.AUTO;
    /** layers on the GPU in mode {@link GpuMode#CUSTOM} */
    public int gpuLayers = DEF_GPU_LAYERS;
    public double temperature = DEF_TEMPERATURE;
    public double topP = DEF_TOP_P;
    public int topK = DEF_TOP_K;
    public int maxTokens = DEF_MAX_TOKENS;
    public double frequencyPenalty = DEF_FREQUENCY_PENALTY;
    public double presencePenalty = DEF_PRESENCE_PENALTY;

    private static volatile LlmSettings current;

    /** the configuration file */
    public static Path file() {
        return Global.llmconfig;
    }

    /** The settings in force (read from the file on first use). Do not modify the result. */
    public static LlmSettings get() {
        LlmSettings s = current;
        if (s == null) {
            s = load();
            current = s;
        }
        return s;
    }

    /** Reads the file; missing file or values give the defaults. */
    public static LlmSettings load() {
        LlmSettings s = new LlmSettings();
        Path f = file();
        if (!Files.exists(f)) return s;
        Properties p = new Properties();
        try (InputStream in = Files.newInputStream(f)) {
            p.load(in);
        } catch (IOException e) {
            System.err.println("Error loading LLM configuration: " + e.getMessage());
            return s;
        }
        s.modelPath = p.getProperty("model_path", "").trim();
        if (intOf(p, "settings_version", 1) < VERSION) return s;   // old dialog: see class comment
        try {
            s.gpuMode = GpuMode.valueOf(p.getProperty("gpu_mode", "AUTO").trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            s.gpuMode = GpuMode.AUTO;
        }
        s.gpuLayers = intOf(p, "gpu_layers", DEF_GPU_LAYERS);
        s.temperature = doubleOf(p, "temperature", DEF_TEMPERATURE);
        s.topP = doubleOf(p, "top_p", DEF_TOP_P);
        s.topK = intOf(p, "top_k", DEF_TOP_K);
        s.maxTokens = intOf(p, "max_tokens", DEF_MAX_TOKENS);
        s.frequencyPenalty = doubleOf(p, "frequency_penalty", DEF_FREQUENCY_PENALTY);
        s.presencePenalty = doubleOf(p, "presence_penalty", DEF_PRESENCE_PENALTY);
        return s.clamped();
    }

    /** Keeps every value inside the range the dialog offers. */
    public LlmSettings clamped() {
        LlmSettings s = copy();
        s.modelPath = modelPath == null ? "" : modelPath.trim();
        if (s.gpuMode == null) s.gpuMode = GpuMode.AUTO;
        s.gpuLayers = Math.max(1, Math.min(999, gpuLayers));
        s.temperature = clamp(temperature, 0.0, 2.0);
        s.topP = clamp(topP, 0.0, 1.0);
        s.topK = Math.max(1, Math.min(100, topK));
        s.maxTokens = Math.max(16, Math.min(CONTEXT_SIZE, maxTokens));
        s.frequencyPenalty = clamp(frequencyPenalty, -2.0, 2.0);
        s.presencePenalty = clamp(presencePenalty, -2.0, 2.0);
        return s;
    }

    /**
     * Writes the settings into the file (other keys in it are kept) and
     * makes them the current ones.
     */
    public void save() throws IOException {
        LlmSettings s = clamped();
        Properties p = readFile();
        p.setProperty("settings_version", String.valueOf(VERSION));
        p.setProperty("model_path", s.modelPath);
        p.setProperty("gpu_mode", s.gpuMode.name());
        p.setProperty("gpu_layers", String.valueOf(s.gpuLayers));
        p.setProperty("temperature", String.valueOf(s.temperature));
        p.setProperty("top_p", String.valueOf(s.topP));
        p.setProperty("top_k", String.valueOf(s.topK));
        p.setProperty("max_tokens", String.valueOf(s.maxTokens));
        p.setProperty("frequency_penalty", String.valueOf(s.frequencyPenalty));
        p.setProperty("presence_penalty", String.valueOf(s.presencePenalty));
        writeFile(p);
        current = s;
    }

    /**
     * Sets only the model file (e.g. right after a download) and keeps all
     * other settings of the file as they are.
     */
    public static void saveModelPath(String path) throws IOException {
        LlmSettings s = get().copy();
        s.modelPath = path == null ? "" : path.trim();
        s.save();
    }

    /** true if a model file is configured and exists */
    public boolean hasModel() {
        return modelPath != null && !modelPath.isBlank() && Files.isRegularFile(Path.of(modelPath));
    }

    public LlmSettings copy() {
        LlmSettings s = new LlmSettings();
        s.modelPath = modelPath;
        s.gpuMode = gpuMode;
        s.gpuLayers = gpuLayers;
        s.temperature = temperature;
        s.topP = topP;
        s.topK = topK;
        s.maxTokens = maxTokens;
        s.frequencyPenalty = frequencyPenalty;
        s.presencePenalty = presencePenalty;
        return s;
    }

    private static Properties readFile() {
        Properties p = new Properties();
        Path f = file();
        if (Files.exists(f)) {
            try (InputStream in = Files.newInputStream(f)) {
                p.load(in);
            } catch (IOException e) {
                System.err.println("Error loading LLM configuration: " + e.getMessage());
            }
        }
        return p;
    }

    private static void writeFile(Properties p) throws IOException {
        Path f = file();
        if (f.getParent() != null) Files.createDirectories(f.getParent());
        try (OutputStream out = Files.newOutputStream(f)) {
            p.store(out, "LLM Configuration Settings");
        }
    }

    private static int intOf(Properties p, String key, int def) {
        try {
            return Integer.parseInt(p.getProperty(key, "").trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static double doubleOf(Properties p, String key, double def) {
        try {
            return Double.parseDouble(p.getProperty(key, "").trim().replace(',', '.'));
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static double clamp(double v, double lo, double hi) {
        if (Double.isNaN(v)) return lo;
        return Math.max(lo, Math.min(hi, v));
    }
}
