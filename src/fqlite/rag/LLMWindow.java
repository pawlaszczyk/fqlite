package fqlite.rag;

import fqlite.base.GUI;
import fqlite.base.Job;
import fqlite.sql.DBManager;
import fqlite.sql.InMemoryDatabase;
import fqlite.ui.NodeObject;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TextArea;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.KeyCode;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.Text;
import javafx.scene.text.TextFlow;
import javafx.stage.Modality;
import javafx.stage.Screen;
import javafx.stage.Stage;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * FQLite Chat agent frame
 *
 * Features:
 * - Chat-Interface with input prompt
 * - Output area for conversations
 * - Toolbar with "back" and "configuration" buttons
 * - Chat-History
 *     The llama.cpp library is a C/C++ implementation of Meta's LLaMA model, optimised for CPU usage.
 *     It allows running the LLaMA model on consumer hardware without requiring high-end GPUs.
 *     LocalAI is a framework that enables running AI models locally without relying on cloud services.
 *     It provides APIs compatible with OpenAI's interfaces, allowing developers to use their own models with the same
 *     code they would use for OpenAI services.
 *
 *  Original Author:  Dirk Pawlaszczyk
 */
public class LLMWindow extends Application {

    // UI components
    private TextFlow outputArea;
    private TextArea promptField;
    private Button resetButton;
    private Button backButton;
    private Button configButton;
    private Label statusLabel;
    private RAGPipeline pipline;
    ProgressBar progress;
    private Stage primaryStage;
    private TreeItem<NodeObject> db_node;

    private String agent_emoji = "🤖💬";
    private String user_emoji = "👤💬";
    // Chat management
    private List<FQLiteChatMessage> chatHistory = new ArrayList<>();
    private int historyIndex;
    private final GUI parent;


    public LLMWindow(GUI parent, TreeItem<NodeObject> node) {
        this.parent = parent;
        this.db_node = node;
    }

    public Stage getPrimaryStage() { return primaryStage; }

    /**
     * The main function of this class. It prepares and shows the window.
     * @param primaryStage
     */
    @Override
    public void start(Stage primaryStage) {

        this.primaryStage = primaryStage;
        primaryStage.getIcons().add(new Image(Objects.requireNonNull(LLMWindow.class.getResourceAsStream("/cognition_small.png"))));

        primaryStage.setTitle("FQLite Assistant [" + db_node.getValue().name + "]" );

        // Main layout
        BorderPane root = new BorderPane();
        root.setPadding(new Insets(10));

        // Toolbar top
        ToolBar toolbar = createToolbar();
        root.setTop(toolbar);

        // Output area in the centre
        VBox centerBox = createCenterArea();
        root.setCenter(centerBox);

        // Input area at the bottom
        VBox bottomBox = createBottomArea();
        root.setBottom(bottomBox);

        // create scene
        Scene scene = new Scene(root, Screen.getPrimary().getVisualBounds().getWidth() * 0.9, Screen.getPrimary().getVisualBounds().getHeight() * 0.9);

        //chatHistory = new ArrayList<>();
        if (chatHistory.size()==0){historyIndex = -1;}
        else{
            historyIndex = chatHistory.size()-1;
            recoverLastChat();
        }

        primaryStage.setOnCloseRequest(event -> {
            event.consume();   // verhindert echtes Schließen
            primaryStage.hide();    // nur verstecken
        });

        primaryStage.setScene(scene);
        primaryStage.show();
        primaryStage.toFront();

        promptField.setText("");
        promptField.setOnKeyReleased(e -> { if(e.getCode() == KeyCode.ENTER ) handleRun();
        }
);

        // focus on the input prompt
        promptField.requestFocus();
    }

    /**
     * Creates a Toolbar with buttons.
     */
    private ToolBar createToolbar() {
        ToolBar toolbar = new ToolBar();

        // Back-Button
        resetButton = new Button(); //("← Reset");
        String sReset = Objects.requireNonNull(LLMWindow.class.getResource("/delete_history_small.png")).toExternalForm();
        ImageView iv = new javafx.scene.image.ImageView(sReset);
        resetButton.setTooltip(new Tooltip("Clear chat history."));
        resetButton.setGraphic(iv);
        resetButton.setOnAction(e -> handleReset());
        resetButton.setDisable(true);

        // Separator
        Separator separator = new Separator();

        backButton = new Button(); //" \u2716 Back");
        backButton.setTooltip(new Tooltip("Close this Window"));
        String sBack = Objects.requireNonNull(LLMWindow.class.getResource("/exit_small.png")).toExternalForm();
        iv = new javafx.scene.image.ImageView(sBack);
        backButton.setGraphic(iv);
        backButton.setOnAction(e -> primaryStage.close());

        // Spacer
        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        Region spacer2 = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);


        configButton = new Button(); //" \u2716 Back");
        configButton.setTooltip(new Tooltip("Settings..."));
        String sConfig = Objects.requireNonNull(LLMWindow.class.getResource("/settings_small.png")).toExternalForm();
        iv = new javafx.scene.image.ImageView(sConfig);
        configButton.setGraphic(iv);
        configButton.setOnAction(e -> showConfigDialog());

        // Status Label
        statusLabel = new Label("Ready");
        statusLabel.setFont(Font.font("Inter Medium", 16));

        progress = new ProgressBar();
        progress.setVisible(false);

        Label logoLabel = new Label();
        String s = Objects.requireNonNull(LLMWindow.class.getResource("/cognition_small.png")).toExternalForm();
        iv = new javafx.scene.image.ImageView(s);
        logoLabel.setGraphic(iv);

        toolbar.getItems().addAll(
                resetButton,
                configButton,
                separator,
                spacer2,
                backButton,
                spacer,
                logoLabel
        );

        return toolbar;
    }

    /**
     * Creates the central output area.
     */
    private VBox createCenterArea() {
        VBox centerBox = new VBox(10);
        centerBox.setPadding(new Insets(10, 0, 10, 0));
        centerBox.setStyle("""
            -fx-control-inner-background: #F5E6C8; """);


        // Label
        Label outputLabel = new Label("Chat:");
        outputLabel.setFont(Font.font("System", 18));

        // Output TextArea
        outputArea = new TextFlow();
        outputArea.setStyle("""
            -fx-control-inner-background: #F5E6C8;
            -fx-background-color: #F5E6C8;
            -fx-text-fill: #3A2F1B;
            -fx-opacity: 1;
        """);
        Text hint1 = new Text();
        //hint1.setText("#".repeat(80) + "\n\n" + Job.db_info);
        Text hint2= new Text();
        hint2.setFont(Font.font("Inter Medium", 16));
        hint2.setText(agent_emoji + "Hi. I'm the FQLite assistant. What would you like to know about the database❓ \n\n\n\n");
        outputArea.getChildren().addAll(hint1, hint2);

        ScrollPane scrollPane = new ScrollPane(outputArea);
        scrollPane.setFitToWidth(true);
        scrollPane.setFitToHeight(true);
        scrollPane.setVbarPolicy(ScrollPane.ScrollBarPolicy.ALWAYS);    // Immer anzeigen
        VBox.setVgrow(scrollPane, Priority.ALWAYS);
        centerBox.getChildren().addAll(outputLabel, scrollPane);
        return centerBox;
    }

    List<String> history = new ArrayList<>();
    int[] phistoryIndex = { -1 };

    /**
     * Creates the lower input area.
     */
    private VBox createBottomArea() {
        VBox bottomBox = new VBox(10);
        bottomBox.setPadding(new Insets(10, 0, 0, 0));

        // Label
        Label promptLabel = new Label("Your input:");
        promptLabel.setFont(Font.font("System", 18));

        // Input field with button
        HBox inputBox = new HBox(10);

        promptField = new TextArea();
        promptField.setWrapText(true);
        promptField.setFont(Font.font("System", 16));
        HBox.setHgrow(promptField, Priority.ALWAYS);
        enableHistory();

        Button sendenButton = new Button("Ask \u2753");
        sendenButton.setDefaultButton(true);
        sendenButton.setOnAction(e -> handleRun());
        sendenButton.setPrefWidth(100);

        Button templateButton = new Button("Example \uD83D\uDCD1");
        templateButton.setDefaultButton(true);
        templateButton.setOnAction(e -> copyExample2Input());
        templateButton.setPrefWidth(100);

        VBox btnfield = new VBox(10);
        btnfield.getChildren().addAll(sendenButton,templateButton);

        inputBox.getChildren().addAll(promptField, btnfield);

        progress = new ProgressBar();
        progress.setVisible(false);

        HBox statusline = new HBox(10);
        statusline.getChildren().addAll(statusLabel,progress);

        bottomBox.getChildren().addAll(promptLabel, inputBox, statusline);
        return bottomBox;
    }



    private void enableHistory() {

        promptField.setOnKeyPressed(event -> {

            switch (event.getCode()) {


                case UP -> {
                    if (!history.isEmpty() && phistoryIndex[0] > 0) {
                        phistoryIndex[0]--;
                        promptField.setText(history.get(phistoryIndex[0]));
                        promptField.positionCaret(promptField.getText().length());
                    }
                    event.consume();
                }

                case DOWN -> {
                    if (!history.isEmpty() && phistoryIndex[0] < history.size() - 1) {
                        phistoryIndex[0]++;
                        promptField.setText(history.get(phistoryIndex[0]));
                        promptField.positionCaret(promptField.getText().length());
                    } else {
                        phistoryIndex[0] = history.size();
                        promptField.clear();
                    }
                    event.consume();
                }
            }


        });

    }

    private void copyExample2Input(){
        promptField.setText(
                " How many entries has the table <tablename>? ");
        // focus on the input prompt
        promptField.requestFocus();
    }


    /**
     * Handling method for the 'go' button.
     */
    private void handleRun() {
        String userInput = promptField.getText().trim();

        if (userInput.isEmpty()) {
            return;
        }

        history.add(userInput);
        phistoryIndex[0] = history.size(); // hinter letztem Eintrag


        // add next user message to history
        addMessage(user_emoji, userInput);

        // clear the prompt for the next question
        promptField.clear();
        progress.setVisible(true);

        // Status update
        statusLabel.setText("Processing...");

        // forward current user prompt to LLM
        askLLM(userInput);


    }

    private void showConfigDialog(){

        LLMConfigDialog dl = new LLMConfigDialog();
        Stage configstage = new Stage();
        configstage.initModality(Modality.APPLICATION_MODAL);
        dl.start(configstage);

    }

    /**
     * This method actually starts the inference process of the LLM.
     * @param userinput the prompt
     */
    private void askLLM(String userinput){

        new Thread(() -> {
            try {

                // forward to LLM
                //String response = agent.run(userinput);
                String response = pipline.generateSQL(userinput);

                // UI update as soon as the response is available
                Platform.runLater(() -> {
                    addMessage(agent_emoji +" ", response);
                    statusLabel.setText("Ready");
                    resetButton.setDisable(false);
                    progress.setVisible(false);
                });

            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    /**
     * Add a new message to the output screen.
     */
    private void addMessage(String sender, String message) {
        FQLiteChatMessage chatMessage = new FQLiteChatMessage(sender, message);
        chatHistory.add(chatMessage);
        historyIndex = chatHistory.size() - 1;

        // add to the output window
        String timestamp = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));

        String emoji = null;

        boolean anwser = false;
        String formattedMessage = null;
        if (sender.startsWith(agent_emoji)){
            emoji = ""; //\uD83D\uDD39;
            anwser = true;
            formattedMessage = String.format("[%s] %s: %s",
                    timestamp, emoji + sender, message );
        }
        else {
            emoji = ""; //""\uD83D\uDD38";
            formattedMessage = String.format("[%s] %s: %s",
                    timestamp, emoji + sender, message + "\n\n");
        }

        final Text t = new Text(formattedMessage);
        t.setFont(Font.font("Inter Medium", 16));
        if (sender.startsWith(agent_emoji)) {
            t.setFill(Color.BLUE);
            // create Hyperlink
            Hyperlink link1 = new Hyperlink("[RUN] ✅ ");
            link1.setOnAction(e -> {
                // Open SQL-Analyzer
                String sql = t.getText();
                System.out.println(">>> " + sql.indexOf(":"));
                if (sql.indexOf("SELECT") != -1) {
                    sql = sql.substring(sql.indexOf("SELECT"), sql.length());
                }
                parent.showSqlWindow(sql, db_node);
            });
            Text nl = new Text("\n\n");
            outputArea.getChildren().addAll(t,link1,nl);
        }
        else {
            t.setFill(Color.BLACK);
            outputArea.getChildren().add(t);
        }

    }

    /**
     * Removes the last chat from history.
     */
    private void handleReset() {
        if (chatHistory.isEmpty()) {
            return;
        }

        // Remove the last two messages (User + Agent)
        if (chatHistory.size() >= 2) {
            chatHistory.remove(chatHistory.size() - 1); // Agent
            chatHistory.remove(chatHistory.size() - 1); // User
        } else if (chatHistory.size() == 1) {
            chatHistory.remove(chatHistory.size() - 1);
        }

        refreshOutput();

        // deactivate Button if history is empty
        if (chatHistory.isEmpty()) {
            resetButton.setDisable(true);
        }

        statusLabel.setText("Removed last messages");
    }

    /**
     * Update chat window. Append the latest request/answer to the output area.
     */
    private void refreshOutput() {
        outputArea.getChildren().clear();

        for (FQLiteChatMessage msg: chatHistory) {
            String timestamp = msg.timestamp.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            String formattedMessage = String.format("[%s] %s:\n%s\n\n",
                    timestamp, msg.sender, msg.message);
            Text t = new Text(formattedMessage);
            outputArea.getChildren().add(t);
        }
    }
    private void recoverLastChat() {

        for (FQLiteChatMessage msg: chatHistory) {
            String timestamp = msg.timestamp.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            String formattedMessage = String.format("[%s] %s:\n%s\n\n",
                    timestamp, msg.sender, msg.message);
            Text t = new Text(formattedMessage);
            outputArea.getChildren().add(t);
        }
    }

    /**
     * Show a short notification.
     */
    private void showNotification(String message){
        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle("Information");
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.show();

        // Auto-close after two seconds
        new Thread(() -> {
            try {
                Thread.sleep(2000);
                Platform.runLater(() -> alert.close());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    /**
     * Apply CSS Styling
     */
    private void applyStyling(Scene scene) {
        scene.getRoot().setStyle(
                "-fx-background-color: #f5f5f5;"
        );

        outputArea.setStyle(
                "-fx-control-inner-background: white;" +
                "-fx-border-color: #cccccc;" +
                "-fx-border-radius: 5;" +
                "-fx-background-radius: 5;"
        );

        promptField.setStyle(
                "-fx-background-radius: 5;" +
                "-fx-border-color: #cccccc;" +
                "-fx-border-radius: 5;"
        );
    }

    public void show(){
        Platform.runLater(() -> {
         if(primaryStage != null) {
             primaryStage.show();
             primaryStage.toFront();
         }
        });
    }

    /**
     * This method is used to initialize the modell and handover the database schema to the LLM.
     *
     */
    public void prepareRAG() {

        /* Create a RAG-Pipline object */
        pipline = new RAGPipeline("/Users/pawel/llm_models/forensic-sqlite-llama-3.2-3b-Q4_K_M.gguf");

        String dbname = db_node.getValue().name;

        /* Check, if InMemory DB already exists */
        InMemoryDatabase mdb;
        if(DBManager.exists(dbname)) {
            mdb = DBManager.get(dbname);
        }
        else{
            // 1. load recovered data to Memory (SQLite in Memory DB)
            mdb = parent.createInMemoryDB(db_node.getValue());
        }

        /* handover the connection objet to retrieve the database schema */
        pipline.initializeRetriever(mdb.getConnectionObject());
    }

    public static void main(String[] args) {
        launch(args);
    }
}


