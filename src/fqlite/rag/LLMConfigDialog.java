package fqlite.rag;

import fqlite.base.Global;
import javafx.application.Application;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javafx.util.converter.DoubleStringConverter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 *  This class realises a Property dialogue window.
 *  The LLM needs to be configured before we can use it.
 *
 *  Original Author:  Dirk Pawlaszczyk
 *
 */
public class LLMConfigDialog extends Application {

    private static final String CONFIG_FILE = Global.CONFIG_FILE;
    private Path configPath;
    private TextField modelPathField;
    private Spinner<Integer> gpuLayersSpinner;
    // Randomness of the model output. Higher -> more variation.
    private Spinner<Double> temperatureSpinner;
    // Top-P Nucleus sampling factor. Lower -> more predictable 0.4
    private Spinner<Double> topPSpinner;
    // Size of selection pool for tokens -> 40
    private Spinner<Integer> topKSpinner;
    // Maximum response length (e.g. 10000)
    private Spinner<Integer> maxTokensSpinner;
    // reduces repetitions
    private Spinner<Double> frequencyPenaltySpinner;
    private Spinner<Double> presencePenaltySpinner;
    //private TextField systemPromptField;

    @Override
    public void start(Stage primaryStage) {
        configPath = Paths.get(Global.baseDir.toString(), CONFIG_FILE);

        Dialog<ButtonType> dialog = new Dialog<>();
        dialog.setTitle("LLM Configuration");
        dialog.setHeaderText("Configuration Settings for APM LLM Agent");

        // Layout
        GridPane grid = new GridPane();
        grid.setHgap(15);
        grid.setVgap(12);
        grid.setPadding(new Insets(20));

        int row = 0;

        Button downloadLLMButton = new Button("Download LLM");
        downloadLLMButton.setOnAction(e -> {
            HuggingFaceDownloadDialog dd = new HuggingFaceDownloadDialog();
            dd.show(primaryStage);
        });


        // Model Path (.gguf file)
        Label modelPathLabel = new Label("Model Path (.gguf):");
        modelPathLabel.setTooltip(new Tooltip("Path to the GGUF model file"));
        modelPathField = new TextField();
        modelPathField.setPrefWidth(250);
        modelPathField.setPromptText("Select model file...");
        Button browseButton = new Button("Browse...");
        browseButton.setOnAction(e -> browseModelFile(dialog.getOwner()));

        HBox modelPathBox = new HBox(10, modelPathField, browseButton, downloadLLMButton);
        modelPathBox.setAlignment(Pos.CENTER_LEFT);
        grid.add(modelPathLabel, 0, row);
        grid.add(modelPathBox, 1, row, 2, 1);
        row++;

        // GPU Layers (0 - 100)
        Label gpuLayersLabel = new Label("GPU Layers:");
        gpuLayersLabel.setTooltip(new Tooltip("Number of layers to offload to GPU (0 = CPU only)"));
        gpuLayersSpinner = createIntegerSpinner(0, 100, 0, 1);
        grid.add(gpuLayersLabel, 0, row);
        grid.add(gpuLayersSpinner, 1, row);
        grid.add(new Label("(0 - 100)"), 2, row);
        row++;

        // Temperature (0.0 - 2.0)
        Label tempLabel = new Label("Temperature:");
        tempLabel.setTooltip(new Tooltip("Controls randomness (0 = deterministic, 2 = very creative)"));
        temperatureSpinner = createDoubleSpinner(0.0, 2.0, 0.0, 0.1);
        grid.add(tempLabel, 0, row);
        grid.add(temperatureSpinner, 1, row);
        grid.add(new Label("(0.0 - 2.0)"), 2, row);
        row++;

        // Top P (0.0 - 1.0)
        Label topPLabel = new Label("Top P:");
        topPLabel.setTooltip(new Tooltip("Nucleus Sampling - considers only the most likely tokens"));
        topPSpinner = createDoubleSpinner(0.0, 1.0, 0.9, 0.05);
        grid.add(topPLabel, 0, row);
        grid.add(topPSpinner, 1, row);
        grid.add(new Label("(0.0 - 1.0)"), 2, row);
        row++;

        // Top K (1 - 100)
        Label topKLabel = new Label("Top K:");
        topKLabel.setTooltip(new Tooltip("Limits selection to the K most likely tokens"));
        topKSpinner = createIntegerSpinner(1, 100, 40, 5);
        grid.add(topKLabel, 0, row);
        grid.add(topKSpinner, 1, row);
        grid.add(new Label("(1 - 100)"), 2, row);
        row++;

        // Max Tokens (1 - 8192)
        Label maxTokensLabel = new Label("Max. Tokens:");
        maxTokensLabel.setTooltip(new Tooltip("Maximum number of tokens to generate"));
        maxTokensSpinner = createIntegerSpinner(1, 8192, 4096, 256);
        grid.add(maxTokensLabel, 0, row);
        grid.add(maxTokensSpinner, 1, row);
        grid.add(new Label("(1 - 8192)"), 2, row);
        row++;

        // Frequency Penalty (-2.0 - 2.0)
        Label freqLabel = new Label("Frequency Penalty:");
        freqLabel.setTooltip(new Tooltip("Reduces repetitions (negative = more repetitions)"));
        frequencyPenaltySpinner = createDoubleSpinner(-2.0, 2.0, 0.0, 0.1);
        grid.add(freqLabel, 0, row);
        grid.add(frequencyPenaltySpinner, 1, row);
        grid.add(new Label("(-2.0 - 2.0)"), 2, row);
        row++;

        // Presence Penalty (-2.0 - 2.0)
        Label presLabel = new Label("Presence Penalty:");
        presLabel.setTooltip(new Tooltip("Encourages new topics (positive = more diversity)"));
        presencePenaltySpinner = createDoubleSpinner(-2.0, 2.0, 0.0, 0.1);
        grid.add(presLabel, 0, row);
        grid.add(presencePenaltySpinner, 1, row);
        grid.add(new Label("(-2.0 - 2.0)"), 2, row);
        row++;

        // System Prompt
        //Label sysPromptLabel = new Label("System Prompt:");
        //sysPromptLabel.setTooltip(new Tooltip("Instructions for the model's behavior"));
        //systemPromptField = new TextField();
        //systemPromptField.setPrefWidth(300);
        //systemPromptField.setPromptText("You are a helpful assistant...");
        //grid.add(sysPromptLabel, 0, row);
        //grid.add(systemPromptField, 1, row, 2, 1);

        dialog.getDialogPane().setContent(grid);

        // Buttons
        ButtonType applyButton = new ButtonType("Apply", ButtonBar.ButtonData.OK_DONE);
        ButtonType cancelButton = new ButtonType("Cancel", ButtonBar.ButtonData.CANCEL_CLOSE);
        dialog.getDialogPane().getButtonTypes().addAll(applyButton, cancelButton);

        // Load saved values
        loadConfig();

        // Show dialog
        dialog.showAndWait().ifPresent(response -> {
            if (response == applyButton) {
                saveConfig();
                showConfirmation();
            }
        });

        primaryStage.close();
    }

    private Spinner<Double> createDoubleSpinner(double min, double max, double initial, double step) {
        Spinner<Double> spinner = new Spinner<>();
        SpinnerValueFactory<Double> valueFactory =
                new SpinnerValueFactory.DoubleSpinnerValueFactory(min, max, initial, step);
        spinner.setValueFactory(valueFactory);
        spinner.setEditable(true);
        spinner.setPrefWidth(120);

        // Format for better display
        TextFormatter<Double> formatter = new TextFormatter<>(
                new DoubleStringConverter(),
                initial,
                change -> {
                    String newText = change.getControlNewText();
                    if (newText.matches("-?\\d*\\.?\\d*")) {
                        return change;
                    }
                    return null;
                }
        );
        //spinner.getEditor().setTextFormatter(formatter);

        return spinner;
    }

    private Spinner<Integer> createIntegerSpinner(int min, int max, int initial, int step) {
        Spinner<Integer> spinner = new Spinner<>();
        SpinnerValueFactory<Integer> valueFactory =
                new SpinnerValueFactory.IntegerSpinnerValueFactory(min, max, initial, step);
        spinner.setValueFactory(valueFactory);
        spinner.setEditable(true);
        spinner.setPrefWidth(120);
        return spinner;
    }

    private void loadConfig() {
        if (!Files.exists(configPath)) {
            return;
        }

        Properties props = new Properties();
        try (InputStream input = new FileInputStream(configPath.toFile())) {
            props.load(input);

            modelPathField.setText(props.getProperty("model_path", ""));
            gpuLayersSpinner.getValueFactory().setValue(
                    Integer.parseInt(props.getProperty("gpu_layers", "17")));
            temperatureSpinner.getValueFactory().setValue(
                    Double.parseDouble(props.getProperty("temperature", "0.3")));
            topPSpinner.getValueFactory().setValue(
                    Double.parseDouble(props.getProperty("top_p", "0.9")));
            topKSpinner.getValueFactory().setValue(
                    Integer.parseInt(props.getProperty("top_k", "40")));
            maxTokensSpinner.getValueFactory().setValue(
                    Integer.parseInt(props.getProperty("max_tokens", "1024")));
            frequencyPenaltySpinner.getValueFactory().setValue(
                    Double.parseDouble(props.getProperty("frequency_penalty", "0.0")));
            presencePenaltySpinner.getValueFactory().setValue(
                    Double.parseDouble(props.getProperty("presence_penalty", "0.0")));
            //systemPromptField.setText(
            //        props.getProperty("system_prompt", ""));

            System.out.println("Configuration loaded from: " + configPath);
        } catch (IOException | NumberFormatException e) {
            System.err.println("Error loading configuration: " + e.getMessage());
        }
    }

    private void saveConfig() {
        Properties props = new Properties();
        props.setProperty("model_path", modelPathField.getText());
        props.setProperty("gpu_layers", String.valueOf(gpuLayersSpinner.getValue()));
        props.setProperty("temperature", String.valueOf(temperatureSpinner.getValue()));
        props.setProperty("top_p", String.valueOf(topPSpinner.getValue()));
        props.setProperty("top_k", String.valueOf(topKSpinner.getValue()));
        props.setProperty("max_tokens", String.valueOf(maxTokensSpinner.getValue()));
        props.setProperty("frequency_penalty", String.valueOf(frequencyPenaltySpinner.getValue()));
        props.setProperty("presence_penalty", String.valueOf(presencePenaltySpinner.getValue()));
        //props.setProperty("system_prompt", systemPromptField.getText());

        try (OutputStream output = new FileOutputStream(configPath.toFile())) {
            props.store(output, "LLM Configuration Settings");
            System.out.println("Configuration saved to: " + configPath);
        } catch (IOException e) {
            System.err.println("Error saving configuration: " + e.getMessage());
            showError("Save Error", "The configuration could not be saved.");
        }
    }

    private void showConfirmation() {
        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle("Success");
        alert.setHeaderText(null);
        alert.setContentText("Configuration has been successfully saved to\n" + configPath);
        alert.showAndWait();
    }

    private void showError(String title, String message) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle(title);
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.showAndWait();
    }

    private void browseModelFile(javafx.stage.Window owner) {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Select GGUF Model File");
        fileChooser.getExtensionFilters().add(
                new FileChooser.ExtensionFilter("GGUF Files", "*.gguf")
        );

        // Set initial directory if path exists
        String currentPath = modelPathField.getText();
        if (!currentPath.isEmpty()) {
            File currentFile = new File(currentPath);
            if (currentFile.getParentFile() != null && currentFile.getParentFile().exists()) {
                fileChooser.setInitialDirectory(currentFile.getParentFile());
            }
        }

        File selectedFile = fileChooser.showOpenDialog(owner);
        if (selectedFile != null) {
            modelPathField.setText(selectedFile.getAbsolutePath());
        }
    }

    public static void main(String[] args) {
        launch(args);
    }
}