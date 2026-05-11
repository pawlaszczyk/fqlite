package fqlite.rag;

import fqlite.base.GUI;
import fqlite.base.ThemeManager;
import fqlite.sql.DBManager;
import fqlite.sql.InMemoryDatabase;
import fqlite.ui.NodeObject;
import fqlite.util.AutoCompletion;
import fqlite.util.WordListCreator;
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
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.scene.text.Text;
import javafx.scene.text.TextFlow;
import javafx.stage.Modality;
import javafx.stage.Screen;
import javafx.stage.Stage;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
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
 *
 *  Original Author:  Dirk Pawlaszczyk
 */
public class LLMWindow extends Application {

    private static List<String> WORD_LIST = new ArrayList<>();

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
        WordListCreator.updateWordList(node,WORD_LIST);
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
        Scene scene = new Scene(root, Screen.getPrimary().getVisualBounds().getWidth() * 0.7, Screen.getPrimary().getVisualBounds().getHeight() * 0.7);
        ThemeManager.register(scene);   // ← apply global theme + stay in sync

        if (chatHistory.size()==0){historyIndex = -1;}
        else{
            historyIndex = chatHistory.size()-1;
            recoverLastChat();
        }

        primaryStage.setOnCloseRequest(event -> {
            event.consume();
            primaryStage.hide();    // just hide
        });

        promptField.setPromptText("Write your prompt here...");

        List<String> listWithoutDuplicates = new ArrayList<>(new LinkedHashSet<String>(WORD_LIST));
        AutoCompletion.installAutoComplete(promptField,listWithoutDuplicates);
        // focus on the input prompt
        promptField.requestFocus();

        primaryStage.setScene(scene);
        primaryStage.show();
        primaryStage.toFront();


    }

    /**
     * Creates a Toolbar with buttons.
     */
    private ToolBar createToolbar() {
        ToolBar toolbar = new ToolBar();

        // Back-Button
        resetButton = new Button(); //("← Reset");
        String s = Objects.requireNonNull(LLMWindow.class.getResource("/icon24_rb.png")).toExternalForm();
        ImageView iv = new javafx.scene.image.ImageView(s);
        iv = new ImageView(s);
        iv.smoothProperty().setValue(true);
        iv.preserveRatioProperty().setValue(true);
        iv.setFitHeight(24);

        resetButton.setTooltip(new Tooltip("Clear chat history."));
        resetButton.setGraphic(iv);
        resetButton.setOnAction(e -> handleReset());

        // Separator
        Separator separator = new Separator();

        backButton = new Button();
        backButton.setTooltip(new Tooltip("Close this Window"));
        s = Objects.requireNonNull(GUI.class.getResource("/icon24_back.png")).toExternalForm();
        iv = new ImageView(s);
        iv.smoothProperty().setValue(true);
        iv.preserveRatioProperty().setValue(true);
        iv.setFitHeight(24);
        backButton.setGraphic(iv);
        backButton.setOnAction(e -> primaryStage.close());

        // Spacer
        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        Region spacer2 = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);


        configButton = new Button();
        configButton.setTooltip(new Tooltip("Settings..."));
        s = Objects.requireNonNull(LLMWindow.class.getResource("/icon24_settingss.png")).toExternalForm();
        iv = new javafx.scene.image.ImageView(s);
        iv.smoothProperty().setValue(true);
        iv.preserveRatioProperty().setValue(true);
        iv.setFitHeight(24);
        iv.setFitWidth(24);
        configButton.setGraphic(iv);
        configButton.setOnAction(e -> showConfigDialog());

        // Status Label
        statusLabel = new Label("Ready");

        progress = new ProgressBar();
        progress.setVisible(false);

        Label logoLabel = new Label();
        s = Objects.requireNonNull(LLMWindow.class.getResource("/icon24_reasoning.png")).toExternalForm();
        iv = new javafx.scene.image.ImageView(s);
        iv.setFitWidth(40);
        iv.setFitHeight(40);
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
        //centerBox.setStyle("""-fx-control-inner-background: #F5E6C8; """);


        // Label
        Label outputLabel = new Label("Chat:");

        // Output TextArea
        outputArea = new TextFlow();
        ThemeManager.applyToTextFlow(outputArea);
        ThemeManager.subscribe(outputArea);

        Text hint1 = new Text();
        Text hint2= new Text();
        hint2.setText(agent_emoji + "Hi. I'm the FQLite assistant. What would you like to know about the database? \n\n\n\n");
        outputArea.getChildren().addAll(hint1, hint2);

        ScrollPane scrollPane = new ScrollPane(outputArea);
        scrollPane.setFitToWidth(true);
        scrollPane.setFitToHeight(true);
        scrollPane.setVbarPolicy(ScrollPane.ScrollBarPolicy.ALWAYS);
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
        Label promptLabel = new Label("Your prompt:");

        // Input field with button
        HBox inputBox = new HBox(10);
        promptField = new TextArea();
        promptField.setPromptText("Enter prompt text");
        promptField.setWrapText(true);
        HBox.setHgrow(promptField, Priority.ALWAYS);
        enableHistory();

        Button runButton = new Button("Run");
        String s = Objects.requireNonNull(LLMWindow.class.getResource("/icon24_execute.png")).toExternalForm();
        ImageView iv = new javafx.scene.image.ImageView(s);
        iv.smoothProperty().setValue(true);
        iv.preserveRatioProperty().setValue(true);
        iv.setFitHeight(24);
        runButton.setGraphic(iv);

        runButton.setDefaultButton(true);
        runButton.setOnAction(e -> handleRun());
        runButton.setPrefWidth(100);

        Button templateButton = new Button("Example");
        s = Objects.requireNonNull(LLMWindow.class.getResource("/icon24_example.png")).toExternalForm();
        iv = new javafx.scene.image.ImageView(s);
        iv.smoothProperty().setValue(true);
        iv.preserveRatioProperty().setValue(true);
        iv.setFitHeight(24);
        templateButton.setGraphic(iv);
        templateButton.setDefaultButton(true);
        templateButton.setOnAction(e -> copyExample2Input());
        templateButton.setPrefWidth(100);

        VBox btnfield = new VBox(10);
        btnfield.getChildren().addAll(runButton,templateButton);

        inputBox.getChildren().addAll(promptField, btnfield);

        progress = new ProgressBar();
        progress.setVisible(false);

        HBox statusline = new HBox(10);
        statusline.getChildren().addAll(statusLabel,progress);

        bottomBox.getChildren().addAll(promptLabel, inputBox, statusline);

        bottomBox.setPrefHeight(Screen.getPrimary().getVisualBounds().getHeight()*0.1);
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
                " How many entries does the table <tablename> have? ");
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
        t.setFill(Color.GREEN);
        if (sender.startsWith(agent_emoji)) {
            t.setFill(Color.BLUE);
            // create Hyperlink
            Hyperlink link1 = new Hyperlink("[RUN] ✅ ");
            link1.setOnAction(e -> {
                // Open SQL-Analyzer
                String sql = t.getText();
                if (sql.indexOf("SELECT") != -1) {
                    sql = sql.substring(sql.indexOf("SELECT"), sql.length());
                }
                parent.showSqlWindow(sql, db_node);
            });
            Text nl = new Text("\n\n");
            outputArea.getChildren().addAll(t,link1,nl);
        }
        else {
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
    public void prepareRAG(String model_path) {

        /* Create a RAG-Pipline object */

        pipline = new RAGPipeline(model_path);
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

        /* handover the connection object to retrieve the database schema */
        pipline.initializeRetriever(mdb.getConnectionObject());
    }

    public static void main(String[] args) {
        launch(args);
    }
}


