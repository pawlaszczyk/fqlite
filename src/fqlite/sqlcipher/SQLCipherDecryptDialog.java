package fqlite.sqlcipher;

import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.stage.Modality;
import javafx.stage.StageStyle;
import java.util.Optional;

/**
 * Modal dialog for entering all SQLCipher decryption parameters.
 *
 * <pre>{@code
 * SQLCipherDecryptDialog dialog = new SQLCipherDecryptDialog(ownerStage);
 * Optional<SQLCipherParams> result = dialog.showAndWait();
 * result.ifPresent(params -> openEncryptedDatabase(file, params));
 * }</pre>
 */
public class SQLCipherDecryptDialog extends Dialog<SQLCipherParams> {

    // ── UI fields ─────────────────────────────────────────────────────────

    private final PasswordField pfPassphrase    = new PasswordField();
    private final TextField     tfPassphraseTxt = new TextField();   // visible clone
    private final CheckBox      cbShowPassword  = new CheckBox("Show password");
    private final RadioButton   rbPassphrase    = new RadioButton("Passphrase (text)");
    private final RadioButton   rbHexKey        = new RadioButton("Hex key  x'…'");
    private final TextField     tfHexKey        = new TextField();

    private final ComboBox<String> cbVersion     = new ComboBox<>();
    private final Spinner<Integer> spPageSize    = new Spinner<>();
    private final Spinner<Integer> spKdfIter     = new Spinner<>();
    private final ComboBox<String> cbHmac        = new ComboBox<>();
    private final ComboBox<String> cbKdf         = new ComboBox<>();

    private final Label lblHint = new Label();

    // ── Constructor ───────────────────────────────────────────────────────

    public SQLCipherDecryptDialog() {
        this(null);
    }

    public SQLCipherDecryptDialog(javafx.stage.Window owner) {
        setTitle("Open encrypted SQLCipher database");
        setHeaderText(null);
        initModality(Modality.APPLICATION_MODAL);
        initStyle(StageStyle.DECORATED);
        if (owner != null) initOwner(owner);

        getDialogPane().setContent(buildContent());
        getDialogPane().getStyleClass().add("sqlcipher-dialog");
        getDialogPane().setMinWidth(520);
        getDialogPane().setMinHeight(550);

        ButtonType btnOpen   = new ButtonType("Open",   ButtonBar.ButtonData.OK_DONE);
        ButtonType btnCancel = new ButtonType("Cancel", ButtonBar.ButtonData.CANCEL_CLOSE);
        getDialogPane().getButtonTypes().addAll(btnOpen, btnCancel);

        // Open button stays disabled until a key has been entered
        Node openButton = getDialogPane().lookupButton(btnOpen);
        openButton.setDisable(true);
        pfPassphrase.textProperty().addListener((o, ov, nv) -> validateInput(openButton));
        tfPassphraseTxt.textProperty().addListener((o, ov, nv) -> validateInput(openButton));
        tfHexKey.textProperty().addListener((o, ov, nv) -> validateInput(openButton));
        rbPassphrase.selectedProperty().addListener((o, ov, nv) -> validateInput(openButton));
        rbHexKey.selectedProperty().addListener((o, ov, nv) -> validateInput(openButton));

        // Result converter
        setResultConverter(buttonType -> {
            if (buttonType == btnOpen) return buildParams();
            return null;
        });

        // Focus the passphrase field on open
        Platform.runLater(pfPassphrase::requestFocus);
    }

    // ── Content construction ──────────────────────────────────────────────

    private Node buildContent() {
        VBox root = new VBox(16);
        root.setPadding(new Insets(20, 24, 4, 24));

        // ── Title row ──
        Label title = new Label("🔐  SQLCipher decryption");
        title.setFont(Font.font(null, FontWeight.BOLD, 15));
        root.getChildren().add(title);

        // ── Key section ──
        root.getChildren().add(buildSectionLabel("Key"));
        root.getChildren().add(buildKeySection());

        // ── Version preset ──
        root.getChildren().add(buildSectionLabel("SQLCipher version / preset"));
        root.getChildren().add(buildVersionSection());

        // ── Advanced parameters (collapsible) ──
        TitledPane advanced = new TitledPane("Advanced parameters", buildAdvancedSection());
        advanced.setExpanded(false);
        advanced.setAnimated(true);
        root.getChildren().add(advanced);

        // ── Hint label ──
        lblHint.setStyle("-fx-text-fill: #b00020; -fx-font-size: 11px;");
        root.getChildren().add(lblHint);

        return root;
    }

    private Label buildSectionLabel(String text) {
        Label lbl = new Label(text);
        lbl.setFont(Font.font(null, FontWeight.SEMI_BOLD, 12));
        lbl.setStyle("-fx-text-fill: #555;");
        return lbl;
    }

    // ── Key section ───────────────────────────────────────────────────────

    private Node buildKeySection() {
        ToggleGroup tg = new ToggleGroup();
        rbPassphrase.setToggleGroup(tg);
        rbHexKey.setToggleGroup(tg);
        rbPassphrase.setSelected(true);

        // Passphrase row (with visibility toggle)
        pfPassphrase.setPromptText("Enter passphrase …");
        tfPassphraseTxt.setPromptText("Enter passphrase …");
        tfPassphraseTxt.setVisible(false);
        tfPassphraseTxt.setManaged(false);

        // Keep both fields in sync
        pfPassphrase.textProperty().bindBidirectional(tfPassphraseTxt.textProperty());

        cbShowPassword.setOnAction(e -> {
            boolean show = cbShowPassword.isSelected();
            pfPassphrase.setVisible(!show);
            pfPassphrase.setManaged(!show);
            tfPassphraseTxt.setVisible(show);
            tfPassphraseTxt.setManaged(show);
        });

        StackPane passphraseStack = new StackPane(pfPassphrase, tfPassphraseTxt);

        GridPane passGrid = new GridPane();
        passGrid.setHgap(8);
        passGrid.setVgap(6);
        passGrid.add(passphraseStack, 0, 0);
        passGrid.add(cbShowPassword,  1, 0);
        GridPane.setHgrow(passphraseStack, Priority.ALWAYS);

        // Hex key row
        tfHexKey.setPromptText("x'0102AABB…'");
        tfHexKey.setDisable(true);
        tfHexKey.setFont(Font.font("Monospace", 12));

        // Toggle visibility based on selection
        rbPassphrase.selectedProperty().addListener((o, ov, nv) -> {
            passGrid.setDisable(!nv);
            tfHexKey.setDisable(nv);
            if (nv) pfPassphrase.requestFocus();
            else    tfHexKey.requestFocus();
        });
        rbHexKey.selectedProperty().addListener((o, ov, nv) -> {
            tfHexKey.setDisable(!nv);
            passGrid.setDisable(nv);
        });

        VBox box = new VBox(6,
                rbPassphrase, passGrid,
                rbHexKey, tfHexKey);
        box.setPadding(new Insets(0, 0, 0, 4));
        return box;
    }

    // ── Version preset ────────────────────────────────────────────────────

    private Node buildVersionSection() {
        cbVersion.getItems().addAll(
                "SQLCipher 4  (default, recommended)",
                "SQLCipher 3  (legacy)",
                "SQLCipher 2  (legacy)",
                "SQLCipher 1  (legacy)",
                "Custom"
        );
        cbVersion.getSelectionModel().selectFirst();
        cbVersion.setPrefWidth(300);

        cbVersion.setOnAction(e -> applyPreset(cbVersion.getValue()));

        HBox row = new HBox(10, new Label("Version:"), cbVersion);
        row.setAlignment(Pos.CENTER_LEFT);
        return row;
    }

    // ── Advanced parameters ───────────────────────────────────────────────

    private Node buildAdvancedSection() {
        // Page size
        SpinnerValueFactory<Integer> pvf =
                new SpinnerValueFactory.IntegerSpinnerValueFactory(512, 65536, 4096, 512);
        spPageSize.setValueFactory(pvf);
        spPageSize.setEditable(true);
        spPageSize.setPrefWidth(110);

        // KDF iterations
        SpinnerValueFactory<Integer> kvf =
                new SpinnerValueFactory.IntegerSpinnerValueFactory(1, 2_000_000, 256000, 10000);
        spKdfIter.setValueFactory(kvf);
        spKdfIter.setEditable(true);
        spKdfIter.setPrefWidth(110);

        // HMAC
        cbHmac.getItems().addAll("HMAC_SHA512", "HMAC_SHA256", "HMAC_SHA1");
        cbHmac.getSelectionModel().selectFirst();

        // KDF
        cbKdf.getItems().addAll("PBKDF2_HMAC_SHA512", "PBKDF2_HMAC_SHA256", "PBKDF2_HMAC_SHA1");
        cbKdf.getSelectionModel().selectFirst();

        // Switch to "Custom" when the user changes any parameter manually
        spPageSize.valueProperty().addListener((o, ov, nv) -> markCustom());
        spKdfIter.valueProperty().addListener((o, ov, nv)  -> markCustom());
        cbHmac.setOnAction(e -> markCustom());
        cbKdf.setOnAction(e  -> markCustom());

        GridPane grid = new GridPane();
        grid.setHgap(12);
        grid.setVgap(10);
        grid.setPadding(new Insets(10, 0, 10, 4));

        grid.add(new Label("Page size (bytes):"),  0, 0);
        grid.add(spPageSize,                        1, 0);
        grid.add(new Label("KDF iterations:"),      0, 1);
        grid.add(spKdfIter,                         1, 1);
        grid.add(new Label("HMAC algorithm:"),      0, 2);
        grid.add(cbHmac,                            1, 2);
        grid.add(new Label("KDF algorithm:"),       0, 3);
        grid.add(cbKdf,                             1, 3);

        return grid;
    }

    // ── Preset logic ──────────────────────────────────────────────────────

    private void applyPreset(String preset) {
        if (preset.startsWith("SQLCipher 4")) {
            setAdvanced(4096, 256000, "HMAC_SHA512", "PBKDF2_HMAC_SHA512");
        } else if (preset.startsWith("SQLCipher 3")) {
            setAdvanced(1024, 64000, "HMAC_SHA1", "PBKDF2_HMAC_SHA1");
        } else if (preset.startsWith("SQLCipher 2")) {
            setAdvanced(1024, 4000, "HMAC_SHA1", "PBKDF2_HMAC_SHA1");
        } else if (preset.startsWith("SQLCipher 1")) {
            setAdvanced(1024, 4000, "HMAC_SHA1", "PBKDF2_HMAC_SHA1");
            // v1 had no HMAC – no relevant field to adjust here
        }
        // "Custom" → do nothing
    }

    private void setAdvanced(int page, int kdf, String hmac, String kdfAlgo) {
        spPageSize.getValueFactory().setValue(page);
        spKdfIter.getValueFactory().setValue(kdf);
        cbHmac.getSelectionModel().select(hmac);
        cbKdf.getSelectionModel().select(kdfAlgo);
    }

    private void markCustom() {
        cbVersion.getSelectionModel().select("Custom");
    }

    // ── Validation ────────────────────────────────────────────────────────

    private void validateInput(Node openButton) {
        boolean valid;
        if (rbPassphrase.isSelected()) {
            valid = !pfPassphrase.getText().isEmpty();
        } else {
            String hex = tfHexKey.getText().trim();
            valid = hex.matches("x'[0-9A-Fa-f]+'");
            if (!valid && !hex.isEmpty()) {
                lblHint.setText("Hex key must match the format  x'<HEX>'.");
            } else {
                lblHint.setText("");
            }
        }
        openButton.setDisable(!valid);
    }

    // ── Result ────────────────────────────────────────────────────────────

    private SQLCipherParams buildParams() {
        boolean hexKey = rbHexKey.isSelected();
        String key     = hexKey ? tfHexKey.getText().trim() : pfPassphrase.getText();

        int versionNum = parseVersionNumber(cbVersion.getValue());

        return new SQLCipherParams(
                key, hexKey,
                spPageSize.getValue(),
                spKdfIter.getValue(),
                cbHmac.getValue(),
                cbKdf.getValue(),
                versionNum
        );
    }

    private int parseVersionNumber(String versionString) {
        if (versionString.startsWith("SQLCipher 1")) return 1;
        if (versionString.startsWith("SQLCipher 2")) return 2;
        if (versionString.startsWith("SQLCipher 3")) return 3;
        return 4; // default / custom → 4
    }

    // ── Static helper ─────────────────────────────────────────────────────

    /**
     * Shows the dialog and returns the result, or {@code Optional.empty()}
     * if the user cancelled.
     */
    public static Optional<SQLCipherParams> show(javafx.stage.Window owner) {
        return new SQLCipherDecryptDialog(owner).showAndWait();
    }
}
