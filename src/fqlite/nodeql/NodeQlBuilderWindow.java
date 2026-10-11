package fqlite.nodeql;

import digital.codespiresolutions.nodeql.BlockNode;
import digital.codespiresolutions.nodeql.BlockType;
import digital.codespiresolutions.nodeql.Position;
import digital.codespiresolutions.nodeql.SqlCompileResult;
import digital.codespiresolutions.nodeql.SqlCompiler;
import fqlite.base.GUI;
import fqlite.sql.InMemoryDatabase;
import fqlite.ui.NodeObject;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Point2D;
import javafx.geometry.Pos;
import javafx.scene.Cursor;
import javafx.scene.Scene;
import javafx.scene.effect.DropShadow;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Control;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.SplitPane;
import javafx.scene.control.Tooltip;
import javafx.scene.control.ToolBar;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.input.KeyCode;
import javafx.scene.input.MouseButton;
import javafx.scene.input.ScrollEvent;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.shape.ClosePath;
import javafx.scene.shape.Line;
import javafx.scene.shape.LineTo;
import javafx.scene.shape.MoveTo;
import javafx.scene.shape.Path;
import javafx.scene.shape.QuadCurveTo;
import javafx.scene.shape.StrokeLineJoin;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.scene.text.Text;
import javafx.stage.Modality;
import javafx.stage.Stage;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;




public final class NodeQlBuilderWindow {
    private static final double BLOCK_WIDTH = 260;
    private static final double BLOCK_HEIGHT = 50;
    private static final double SNAP_DISTANCE = 36;
    private static final double CHAIN_X = 3800;
    private static final double CHAIN_START_Y = 3880;
    private static final double CHAIN_GAP = 8;
    private static final double CANVAS_WIDTH = 9000;
    private static final double CANVAS_HEIGHT = 7600;
    private static final double GRID_SIZE = 40;
    private static final double MIN_ZOOM = 0.45;
    private static final double MAX_ZOOM = 2.2;

    private final GUI app;
    private final TreeItem<NodeObject> databaseNode;
    private final InMemoryDatabase database;
    private final Stage owner;
    private final Stage stage = new Stage();
    private final Pane workspace = new Pane();
    private final TextArea sqlPreview = new TextArea();
    private final Label zoomLabel = new Label("100%");
    private final Label resultStatus = new Label("Ready to run a query");
    private final TableView<List<String>> resultTable = new TableView<>();
    private final Font materialIconFont = Font.loadFont(
            Objects.requireNonNull(GUI.class.getResourceAsStream("/nodeql-material-icons.ttf")), 24);
    private Button runQueryButton;
    private BlockView paletteDragging;
    private QueryResult lastResult;
    private final List<BlockView> blocks = new ArrayList<>();
    private final List<WorkspaceSnapshot> undoStack = new ArrayList<>();
    private final BlockNode triggerNode = new BlockNode("event_1", BlockType.EVENT_GREEN_FLAG);

    private BlockView dragging;
    private BlockView selected;
    private double dragOffsetX;
    private double dragOffsetY;
    private double zoom = 1.0;
    private ScrollPane workspaceScroll;
    private boolean panning;
    private boolean restoring;
    private double panStartX;
    private double panStartY;
    private double panStartH;
    private double panStartV;
    private WorkspaceSnapshot pendingDragSnapshot;
    private WorkspaceSnapshot pendingTextEditSnapshot;

    public NodeQlBuilderWindow(GUI app, TreeItem<NodeObject> databaseNode, InMemoryDatabase database, Stage owner) {
        this.app = app;
        this.databaseNode = databaseNode;
        this.database = database;
        this.owner = owner;
    }

    public void show() {
        stage.setTitle("NodeQL — FQLite");
        stage.initOwner(owner);
        stage.initModality(Modality.NONE);

        BorderPane root = new BorderPane();
        root.getStyleClass().add("nodeql-builder");
        SplitPane workbench = new SplitPane(createPalette(), createWorkspace(), createPreview());
        workbench.setDividerPositions(0.25, 0.73);
        workbench.getStyleClass().add("workbench");
        root.setCenter(workbench);
        root.setTop(createToolbar());

        Scene scene = new Scene(root, 1440, 860);
        stage.setMinWidth(1080);
        stage.setMinHeight(640);
        scene.setOnKeyPressed(event -> {
            if (event.isShortcutDown() && event.getCode() == KeyCode.Z && !event.isShiftDown()) {
                undoLastAction();
                event.consume();
            } else if (!(scene.getFocusOwner() instanceof javafx.scene.control.TextInputControl)
                    && (event.getCode() == KeyCode.DELETE || event.getCode() == KeyCode.BACK_SPACE)) {
                deleteSelected();
                event.consume();
            }
        });
        scene.getStylesheets().add(Objects.requireNonNull(GUI.class.getResource("/nodeql-builder.css")).toExternalForm());
        stage.setScene(scene);
        stage.show();
        root.applyCss();
        root.layout();
        refreshSql();
    }

    private ToolBar createToolbar() {
        Label brand = new Label("NodeQL for FQLite");
        brand.getStyleClass().add("brand");
        runQueryButton = new Button("Run Query");
        runQueryButton.setOnAction(e -> executeQuery());
        runQueryButton.setStyle(primaryButtonStyle());
        Label databaseLabel = new Label(databaseNode == null ? "SQLite workspace" : databaseNode.getValue().name);
        databaseLabel.getStyleClass().add("database-label");

        Button sendToAnalyzer = new Button("Open in SQL Analyzer");
        sendToAnalyzer.setOnAction(e -> app.showSqlWindow(sqlPreview.getText(), databaseNode));
        Button undo = new Button("Undo");
        undo.setOnAction(e -> undoLastAction());

        Button delete = new Button("Delete");
        delete.setOnAction(e -> deleteSelected());

        Button clear = new Button("Clear");
        clear.setOnAction(e -> clearWorkspace());

        Button zoomOut = new Button("-");
        zoomOut.setOnAction(e -> setZoom(zoom - 0.1));

        Button zoomIn = new Button("+");
        zoomIn.setOnAction(e -> setZoom(zoom + 0.1));

        Button resetZoom = new Button("100%");
        resetZoom.setOnAction(e -> setZoom(1.0));

        zoomLabel.setMinWidth(48);
        ToolBar toolbar = new ToolBar(brand, databaseLabel, runQueryButton, undo, delete, clear,
                zoomOut, zoomIn, resetZoom, zoomLabel, sendToAnalyzer);
        return toolbar;
    }

    private void executeQuery() {
        if (runQueryButton.isDisable()) return;
        String sql = sqlPreview.getText().trim();
        if (sql.isEmpty()) {
            showExecutionError("Add and snap query blocks before running the query.", null);
            return;
        }
        runQueryButton.setDisable(true);
        resultStatus.setText("Running query…");

        Thread execution = new Thread(() -> {
            try (Statement statement = database.getConnectionObject().createStatement()) {
                boolean hasResultSet = statement.execute(sql);
                QueryResult result = hasResultSet
                        ? readQueryResult(statement.getResultSet())
                        : QueryResult.updateCount(statement.getUpdateCount());
                Platform.runLater(() -> showQueryResult(result));
            } catch (SQLException exception) {
                Platform.runLater(() -> {
                    runQueryButton.setDisable(false);
                    resultStatus.setText("Query failed · " + exception.getMessage());
                    showExecutionError("The NodeQL query could not be executed.", exception);
                });
            }
        }, "nodeql-query-execution");
        execution.setDaemon(true);
        execution.start();
    }

    private QueryResult readQueryResult(ResultSet resultSet) throws SQLException {
        try (ResultSet results = resultSet) {
            ResultSetMetaData metadata = results.getMetaData();
            List<String> columns = new ArrayList<>(metadata.getColumnCount());
            for (int column = 1; column <= metadata.getColumnCount(); column++) {
                columns.add(metadata.getColumnLabel(column));
            }

            List<List<String>> rows = new ArrayList<>();
            while (results.next()) {
                List<String> row = new ArrayList<>(columns.size());
                for (int column = 1; column <= columns.size(); column++) {
                    row.add(results.getString(column));
                }
                rows.add(row);
            }
            return new QueryResult(columns, rows, "Query completed · " + rows.size() + " row(s)");
        }
    }

    private void showQueryResult(QueryResult result) {
        lastResult = result;
        populateResultTable(resultTable, result);
        resultStatus.setText(result.status());
        runQueryButton.setDisable(false);
    }

    private void populateResultTable(TableView<List<String>> table, QueryResult result) {
        table.getColumns().clear();
        for (int index = 0; index < result.columns().size(); index++) {
            final int columnIndex = index;
            TableColumn<List<String>, String> column = new TableColumn<>(result.columns().get(index));
            column.setCellValueFactory(cell -> new SimpleStringProperty(
                    cell.getValue().get(columnIndex) == null ? "" : cell.getValue().get(columnIndex)));
            column.setPrefWidth(150);
            table.getColumns().add(column);
        }
        ObservableList<List<String>> rows = FXCollections.observableArrayList(result.rows());
        table.setItems(rows);
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);
    }

    private void showResultsFullscreen() {
        if (lastResult == null) return;
        TableView<List<String>> table = new TableView<>();
        populateResultTable(table, lastResult);

        Label status = new Label(lastResult.status());
        VBox root = new VBox(status, table);
        root.getStyleClass().add("nodeql-builder");
        VBox.setVgrow(table, Priority.ALWAYS);

        Stage resultsStage = new Stage();
        resultsStage.setTitle("NodeQL Results — " + databaseNode.getValue().name);
        resultsStage.initOwner(stage);
        resultsStage.setScene(new Scene(root, 960, 640));
        resultsStage.getScene().getStylesheets().add(
                Objects.requireNonNull(GUI.class.getResource("/nodeql-builder.css")).toExternalForm());
        resultsStage.setMaximized(true);
        resultsStage.show();
    }

    private void showExecutionError(String message, Exception exception) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("NodeQL execution");
        alert.setHeaderText(message);
        if (exception != null) {
            alert.setContentText(exception.getMessage());
        }
        alert.initOwner(stage);
        alert.showAndWait();
    }

    private record QueryResult(List<String> columns, List<List<String>> rows, String status) {
        private static QueryResult updateCount(int count) {
            return new QueryResult(List.of(), List.of(), "Query completed · " + count + " row(s) affected");
        }
    }

    private HBox createPalette() {
        HBox panel = new HBox();
        panel.setMinWidth(285);
        panel.setPrefWidth(340);
        panel.getStyleClass().add("palette-panel");
        VBox rail = new VBox(10);
        rail.setPadding(new Insets(12, 8, 12, 8));
        rail.getStyleClass().add("category-rail");
        VBox palette = new VBox(10);
        palette.setPadding(new Insets(12));
        HBox.setHgrow(palette, Priority.ALWAYS);
        TextField search = new TextField();
        search.setPromptText("Search commands");
        search.getStyleClass().add("palette-search");
        Label title = new Label("Query data");
        title.getStyleClass().add("panel-title");
        VBox cards = new VBox(10);
        ScrollPane scroll = new ScrollPane(cards);
        scroll.setFitToWidth(true);
        scroll.setHbarPolicy(ScrollPane.ScrollBarPolicy.NEVER);
        VBox.setVgrow(scroll, Priority.ALWAYS);
        Category[] active = {Category.DQL};
        List<Button> categoryButtons = new ArrayList<>();
        Runnable refresh = () -> {
            cards.getChildren().clear();
            String query = search.getText().strip().toLowerCase(java.util.Locale.ROOT);
            title.setText(query.isEmpty() ? active[0].title : "Search results");
            if (query.isEmpty() && active[0] == Category.DQL || !query.isEmpty() && "execute query".contains(query)) {
                VBox triggerCard = new VBox(10);
                triggerCard.setPadding(new Insets(10));
                triggerCard.getStyleClass().add("palette-card");
                StackPane trigger = createTriggerView();
                trigger.setScaleX(0.82);
                trigger.setScaleY(0.82);
                trigger.setTranslateX(-23);
                trigger.setOnMouseClicked(e -> centerOnChain());
                Label description = new Label("Start the SQL query chain in the workspace.");
                description.setWrapText(true);
                description.getStyleClass().add("block-description");
                triggerCard.getChildren().addAll(trigger, description);
                cards.getChildren().add(triggerCard);
            }
            for (BlockTemplate template : BlockTemplate.values()) {
                if (query.isEmpty() ? template.category != active[0]
                        : !(template.paletteLabel + " " + descriptionFor(template)).toLowerCase(java.util.Locale.ROOT).contains(query)) {
                    continue;
                }
                cards.getChildren().add(createPaletteCard(template));
            }
            for (int i = 0; i < categoryButtons.size(); i++) {
                Button button = categoryButtons.get(i);
                Category category = Category.values()[i];
                boolean selectedCategory = category == active[0];
                Color color = Color.web(category.railColor());
                button.setStyle("-fx-background-color: " + (selectedCategory ? category.railColor()
                        : String.format(java.util.Locale.ROOT, "rgba(%d,%d,%d,0.16)",
                        (int) (color.getRed() * 255), (int) (color.getGreen() * 255), (int) (color.getBlue() * 255))) + ";");
                Label icon = (Label) button.getGraphic();
                icon.setStyle("-fx-font-family: '" + materialIconFont.getFamily() + "'; -fx-font-size: 24px;"
                        + "-fx-text-fill: " + (selectedCategory ? "white" : category.railColor()) + ";");
            }
        };
        for (Category category : Category.values()) {
            Button button = new Button();
            Label icon = materialIcon(category.icon(), 24);
            button.setGraphic(icon);
            button.setAccessibleText(category.title);
            button.getStyleClass().add("category-button");
            button.setTooltip(new Tooltip(category.title));
            button.setOnAction(e -> {
                active[0] = category;
                search.clear();
                refresh.run();
            });
            categoryButtons.add(button);
            rail.getChildren().add(button);
        }
        search.textProperty().addListener((obs, old, value) -> refresh.run());
        palette.getChildren().addAll(search, title, scroll);
        panel.getChildren().addAll(rail, palette);
        refresh.run();
        return panel;
    }

    private VBox createPaletteCard(BlockTemplate template) {
        VBox card = new VBox(10);
        card.setPadding(new Insets(10));
        card.getStyleClass().add("palette-card");
        double width = 216;
        double height = template.visualKind().height(template);
        StackPane preview = new StackPane();
        preview.setAlignment(Pos.TOP_LEFT);
        preview.setMinHeight(height + (template.visualKind().hasBottom ? CHAIN_GAP : 0));
        preview.setPrefHeight(preview.getMinHeight());
        preview.setMinWidth(0);
        Path shape = createBlockShape(template.visualKind(), width, height, Color.web(template.nodeColor()));
        Label label = new Label(template.previewLabel());
        label.setFont(Font.font("System", FontWeight.BOLD, 15));
        label.setTextFill(Color.WHITE);
        label.setMaxWidth(width - 30);
        label.setPadding(new Insets(template.visualKind() == VisualKind.JOIN ? 8 : (height - 20) / 2, 10, 0,
                template.visualKind() == VisualKind.JOIN ? 26 : 14));
        label.setMouseTransparent(true);
        preview.getChildren().addAll(shape, label);
        preview.widthProperty().addListener((obs, old, available) -> {
            double previewWidth = Math.max(150, available.doubleValue());
            shape.getElements().setAll(createBlockShape(template.visualKind(), previewWidth, height,
                    Color.web(template.nodeColor())).getElements());
            label.setMaxWidth(previewWidth - 20);
        });
        Label description = new Label(descriptionFor(template));
        description.setWrapText(true);
        description.getStyleClass().add("block-description");
        card.getChildren().addAll(preview, description);
        card.setCursor(Cursor.HAND);
        card.setOnMousePressed(e -> {
            if (e.getButton() != MouseButton.PRIMARY) return;
            addBlock(template);
            paletteDragging = selected;
            e.consume();
        });
        card.setOnMouseDragged(e -> {
            if (paletteDragging == null) return;
            Point2D point = workspace.sceneToLocal(e.getSceneX(), e.getSceneY());
            paletteDragging.relocate(point.getX() - 40, point.getY() - 20);
            highlightReporterSlots(paletteDragging, point);
            e.consume();
        });
        card.setOnMouseReleased(e -> {
            if (paletteDragging == null) return;
            snapOrFloat(paletteDragging);
            clearSlotHighlights();
            paletteDragging = null;
            refreshSql();
            e.consume();
        });
        return card;
    }

    private String descriptionFor(BlockTemplate template) {
        return switch (template.type) {
            case SQL_SELECT -> "Retrieve columns from a table in the active database.";
            case SQL_COLUMN -> "A column or expression. Drop this reporter into a round input.";
            case SQL_FROM -> "Choose the source table for the query.";
            case SQL_WHERE -> "Filter rows by comparing a column with a value.";
            case SQL_ORDER_BY -> "Sort query results in ascending or descending order.";
            case SQL_GROUP_BY -> "Group matching rows before calculating aggregates.";
            case SQL_HAVING -> "Filter grouped rows using an aggregate condition.";
            default -> template.visualKind() == VisualKind.EXPRESSION
                    ? "Use " + template.paletteLabel + " inside a round expression input."
                    : "Add " + template.paletteLabel + " to the connected SQL query.";
        };
    }

    private ScrollPane createWorkspace() {
        workspace.setMinSize(CANVAS_WIDTH, CANVAS_HEIGHT);
        workspace.setPrefSize(CANVAS_WIDTH, CANVAS_HEIGHT);
        workspace.setStyle("-fx-background-color: #111827;");
        workspace.getChildren().addAll(createGridLines());
        workspace.getChildren().add(createTriggerView());
        workspace.addEventFilter(ScrollEvent.SCROLL, event -> {
            if (event.isShortcutDown()) {
                setZoom(zoom + (event.getDeltaY() > 0 ? 0.08 : -0.08));
                event.consume();
            }
        });
        workspace.setOnZoom(event -> {
            setZoom(zoom * event.getZoomFactor());
            event.consume();
        });
        installCanvasPanning();

        workspaceScroll = new ScrollPane(workspace);
        workspaceScroll.setFitToWidth(false);
        workspaceScroll.setFitToHeight(false);
        workspaceScroll.setPannable(true);
        workspaceScroll.getStyleClass().add("workspace-scroll");
        workspaceScroll.viewportBoundsProperty().addListener((obs, old, bounds) -> centerOnChain());
        return workspaceScroll;
    }

    private VBox createPreview() {
        VBox box = new VBox(8);
        box.setPadding(new Insets(14));
        box.setMinWidth(260);
        box.setPrefWidth(370);
        box.getStyleClass().add("output-panel");

        Label label = new Label("SQL-Command Output");
        label.getStyleClass().add("panel-title");

        sqlPreview.setEditable(false);
        sqlPreview.setWrapText(true);
        sqlPreview.getStyleClass().add("sql-output");
        sqlPreview.setPrefRowCount(7);
        sqlPreview.setMinHeight(130);
        Button fullscreen = new Button("Expand results");
        fullscreen.setOnAction(e -> showResultsFullscreen());
        HBox resultHeader = new HBox(8, new Label("Query results"), fullscreen);
        resultHeader.setAlignment(Pos.CENTER_LEFT);
        resultStatus.setWrapText(true);
        resultStatus.getStyleClass().add("muted");
        resultTable.setPlaceholder(new Label("Run a connected query to see its results here."));
        VBox.setVgrow(resultTable, Priority.ALWAYS);

        Label credit = new Label("NodeQL Implementation by Paul Bodach (CodeSpire-Solutions)");
        credit.setStyle("-fx-text-fill: #64748b; -fx-font-size: 10px;");

        box.getChildren().addAll(label, sqlPreview, resultHeader, resultStatus, resultTable, credit);
        return box;
    }

    private StackPane createTriggerView() {
        StackPane trigger = new StackPane();
        trigger.setLayoutX(CHAIN_X);
        trigger.setLayoutY(CHAIN_START_Y - BLOCK_HEIGHT);
        trigger.setPrefSize(BLOCK_WIDTH, BLOCK_HEIGHT + CHAIN_GAP);
        trigger.setMinSize(BLOCK_WIDTH, BLOCK_HEIGHT + CHAIN_GAP);
        trigger.setMaxSize(BLOCK_WIDTH, BLOCK_HEIGHT + CHAIN_GAP);

        Path shape = createBlockShape(VisualKind.TRIGGER, BLOCK_WIDTH, BLOCK_HEIGHT, Color.web("#ffbf00"));
        HBox content = new HBox(9);
        content.setAlignment(Pos.CENTER_LEFT);
        content.setPadding(new Insets(0, 12, CHAIN_GAP, 16));
        Label play = materialIcon("\ue037", 18);
        play.setStyle(play.getStyle() + "-fx-text-fill: white;"
                + "-fx-background-color: rgba(255,255,255,0.22); -fx-background-radius: 18;"
                + "-fx-padding: 4 7 4 8;");
        Label label = new Label("EXECUTE QUERY");
        label.setStyle("-fx-text-fill: white; -fx-font-size: 16px; -fx-font-weight: bold;");
        content.getChildren().addAll(play, label);
        trigger.getChildren().addAll(shape, content);
        trigger.setCursor(Cursor.HAND);
        trigger.setOnMouseClicked(e -> executeQuery());
        Tooltip.install(trigger, new Tooltip("Run the connected query"));
        return trigger;
    }

    private Label materialIcon(String character, double size) {
        Label icon = new Label(character);
        icon.setFont(Font.font(materialIconFont.getFamily(), size));
        icon.setStyle("-fx-font-family: '" + materialIconFont.getFamily() + "'; -fx-font-size: " + size + "px;");
        return icon;
    }

    private Path createBlockShape(VisualKind kind, double width, double bodyHeight, Color color) {
        double radius = kind == VisualKind.EXPRESSION ? bodyHeight / 2 : kind == VisualKind.TRIGGER ? 11 : 10;
        double notchX = 34;
        double notchWidth = 22;
        double notchHeight = CHAIN_GAP;

        Path path = new Path();
        path.getElements().add(new MoveTo(kind == VisualKind.JOIN ? 18 : radius, 0));
        if (kind.hasTop) {
            path.getElements().addAll(
                    new LineTo(notchX, 0),
                    new LineTo(notchX + 4, notchHeight),
                    new LineTo(notchX + notchWidth - 4, notchHeight),
                    new LineTo(notchX + notchWidth, 0));
        }
        path.getElements().addAll(new LineTo(width - radius, 0),
                new QuadCurveTo(width, 0, width, radius));
        if (kind == VisualKind.SET_OPERATOR) {
            path.getElements().add(new QuadCurveTo(width - 12, bodyHeight / 2, width, bodyHeight - radius));
        } else {
            path.getElements().add(new LineTo(width, bodyHeight - radius));
        }
        path.getElements().add(new QuadCurveTo(width, bodyHeight, width - radius, bodyHeight));
        if (kind.hasBottom) path.getElements().addAll(
                new LineTo(notchX + notchWidth, bodyHeight),
                new LineTo(notchX + notchWidth - 4, bodyHeight + notchHeight),
                new LineTo(notchX + 4, bodyHeight + notchHeight),
                new LineTo(notchX, bodyHeight));
        if (kind == VisualKind.JOIN) {
            path.getElements().addAll(new LineTo(18, bodyHeight), new LineTo(0, bodyHeight / 2),
                    new LineTo(18, 0));
        } else {
            path.getElements().addAll(
                new LineTo(radius, bodyHeight),
                new QuadCurveTo(0, bodyHeight, 0, bodyHeight - radius));
            path.getElements().add(kind == VisualKind.SET_OPERATOR
                    ? new QuadCurveTo(12, bodyHeight / 2, 0, radius) : new LineTo(0, radius));
            path.getElements().add(new QuadCurveTo(0, 0, radius, 0));
        }
        path.getElements().add(new ClosePath());
        path.setFill(color);
        path.setStroke(Color.rgb(0, 0, 0, 0.32));
        path.setStrokeWidth(4);
        path.setStrokeLineJoin(StrokeLineJoin.ROUND);
        path.setEffect(new DropShadow(8, 0, 3, Color.rgb(0, 0, 0, 0.28)));
        path.setMouseTransparent(true);
        path.setManaged(false);
        return path;
    }

    private void addBlock(BlockTemplate template) {
        rememberUndoState();
        BlockView view = new BlockView(template);
        view.setLayoutX(CHAIN_X + 80);
        view.setLayoutY(CHAIN_START_Y + 130 + (blocks.size() % 6) * 58);
        blocks.add(view);
        workspace.getChildren().add(view);
        installDrag(view);
        select(view);
        refreshSql();
    }

    private void installDrag(BlockView view) {
        view.setCursor(Cursor.HAND);
        view.setOnMousePressed(event -> {
            select(view);
            javafx.scene.Node target = event.getTarget() instanceof javafx.scene.Node node ? node : null;
            for (javafx.scene.Node current = target; current != null && current != view; current = current.getParent()) {
                if (current instanceof Control) {
                    event.consume();
                    return;
                }
            }
            if (event.getButton() != MouseButton.PRIMARY) return;
            pendingDragSnapshot = captureWorkspaceState();
            if (view.reporterOwner != null) detachReporter(view);
            view.snapped = false;
            layoutSnappedBlocks();
            dragging = view;
            dragOffsetX = event.getX();
            dragOffsetY = event.getY();
            view.toFront();
            event.consume();
        });
        view.setOnMouseDragged(event -> {
            if (dragging != view) {
                return;
            }
            Point2D local = workspace.sceneToLocal(event.getSceneX(), event.getSceneY());
            view.setLayoutX(local.getX() - dragOffsetX);
            view.setLayoutY(local.getY() - dragOffsetY);
            highlightReporterSlots(view, local);
            event.consume();
        });
        view.setOnMouseReleased(event -> {
            if (dragging != view) return;
            snapOrFloat(view);
            clearSlotHighlights();
            dragging = null;
            refreshSql();
            rememberUndoState(pendingDragSnapshot);
            pendingDragSnapshot = null;
            event.consume();
        });
    }

    private List<Line> createGridLines() {
        List<Line> lines = new ArrayList<>();
        for (double x = 0; x <= CANVAS_WIDTH; x += GRID_SIZE) {
            Line line = new Line(x, 0, x, CANVAS_HEIGHT);
            line.setStroke(Color.rgb(148, 163, 184, x % (GRID_SIZE * 4) == 0 ? 0.06 : 0.025));
            line.setStrokeWidth(x % (GRID_SIZE * 4) == 0 ? 1.2 : 0.7);
            line.setMouseTransparent(true);
            lines.add(line);
        }
        for (double y = 0; y <= CANVAS_HEIGHT; y += GRID_SIZE) {
            Line line = new Line(0, y, CANVAS_WIDTH, y);
            line.setStroke(Color.rgb(148, 163, 184, y % (GRID_SIZE * 4) == 0 ? 0.06 : 0.025));
            line.setStrokeWidth(y % (GRID_SIZE * 4) == 0 ? 1.2 : 0.7);
            line.setMouseTransparent(true);
            lines.add(line);
        }
        return lines;
    }

    private void installCanvasPanning() {
        workspace.setOnMousePressed(event -> {
            if (event.getTarget() != workspace) {
                return;
            }
            if (event.getButton() == MouseButton.MIDDLE || event.getButton() == MouseButton.SECONDARY) {
                panning = true;
                panStartX = event.getSceneX();
                panStartY = event.getSceneY();
                panStartH = workspaceScroll == null ? 0 : workspaceScroll.getHvalue();
                panStartV = workspaceScroll == null ? 0 : workspaceScroll.getVvalue();
                workspace.setCursor(Cursor.MOVE);
                event.consume();
            }
        });
        workspace.setOnMouseDragged(event -> {
            if (!panning || workspaceScroll == null) {
                return;
            }
            double dx = event.getSceneX() - panStartX;
            double dy = event.getSceneY() - panStartY;
            double contentWidth = CANVAS_WIDTH * zoom;
            double contentHeight = CANVAS_HEIGHT * zoom;
            double viewportWidth = Math.max(1, workspaceScroll.getViewportBounds().getWidth());
            double viewportHeight = Math.max(1, workspaceScroll.getViewportBounds().getHeight());
            double maxH = Math.max(1, contentWidth - viewportWidth);
            double maxV = Math.max(1, contentHeight - viewportHeight);
            workspaceScroll.setHvalue(clamp(panStartH - dx / maxH, 0, 1));
            workspaceScroll.setVvalue(clamp(panStartV - dy / maxV, 0, 1));
            event.consume();
        });
        workspace.setOnMouseReleased(event -> {
            if (panning) {
                panning = false;
                workspace.setCursor(Cursor.DEFAULT);
                event.consume();
            }
        });
    }

    private void snapOrFloat(BlockView released) {
        if (released.template.visualKind() == VisualKind.EXPRESSION) {
            released.snapped = false;
            attachToNearestSlot(released);
            rebuildModelChain();
            return;
        }
        int insertionIndex = snapIndexFor(released);
        if (insertionIndex < 0) {
            released.snapped = false;
            rebuildModelChain();
            return;
        }

        List<BlockView> chain = snappedBlocksExcept(released);
        blocks.remove(released);
        int blockIndex = insertionIndex < chain.size() ? blocks.indexOf(chain.get(insertionIndex))
                : chain.isEmpty() ? 0 : blocks.indexOf(chain.getLast()) + 1;
        blocks.add(blockIndex, released);
        released.snapped = true;
        layoutSnappedBlocks();
        rebuildModelChain();
    }

    private int snapIndexFor(BlockView released) {
        if (Math.abs(released.getLayoutX() - CHAIN_X) > SNAP_DISTANCE * 2) {
            return -1;
        }

        double y = released.getLayoutY();
        if (Math.abs(y - CHAIN_START_Y) <= SNAP_DISTANCE) {
            return 0;
        }

        List<BlockView> snapped = snappedBlocksExcept(released);
        double nextY = CHAIN_START_Y;
        for (int index = 0; index <= snapped.size(); index++) {
            if (Math.abs(y - nextY) <= SNAP_DISTANCE
                    && (index == 0 || snapped.get(index - 1).template.visualKind().hasBottom)) return index;
            if (index < snapped.size()) nextY += snapped.get(index).bodyHeight;
        }
        return -1;
    }

    private List<BlockView> snappedBlocksExcept(BlockView excluded) {
        return blocks.stream()
                .filter(block -> block != excluded)
                .filter(block -> block.snapped)
                .toList();
    }

    private void layoutSnappedBlocks() {
        double y = CHAIN_START_Y;
        for (BlockView block : blocks) {
            if (!block.snapped) {
                continue;
            }
            block.setLayoutX(CHAIN_X);
            block.setLayoutY(y);
            y += block.bodyHeight;
        }
    }

    private void rebuildModelChain() {
        BlockNode previous = triggerNode;
        triggerNode.setNext(null);
        for (BlockView block : blocks) {
            if (!block.snapped) {
                block.node.setNext(null);
                continue;
            }
            block.applyInputs();
            previous.setNext(block.node);
            previous = block.node;
        }
        previous.setNext(null);
    }

    private void refreshSql() {
        rebuildModelChain();
        sqlPreview.setText(composeSql());
    }

    private String composeSql() {
        List<BlockView> snapped = blocks.stream().filter(block -> block.snapped).toList();
        if (snapped.isEmpty()) {
            return "";
        }

        for (BlockView block : snapped) {
            block.applyInputs();
        }

        BlockView select = snapped.stream()
                .filter(block -> block.template.type == BlockType.SQL_SELECT)
                .findFirst()
                .orElse(null);

        boolean selectLikeChain = snapped.stream().anyMatch(block ->
                block.template.category == Category.DQL
                        || block.template.category == Category.JOIN_SET
                        || block.template.category == Category.AGGREGATE
                        || block.template.category == Category.FUNCTION);

        if (select == null && !selectLikeChain) {
            SqlCompileResult result = new SqlCompiler().compileWorkspace(List.of(triggerNode));
            return result.sql();
        }

        List<String> projections = new ArrayList<>();
        String selectedColumns = select == null ? "*" : input(select, "columns", "*").trim();
        if (!selectedColumns.isEmpty()) {
            projections.add(selectedColumns);
        }

        String table = select == null ? "table_name" : input(select, "table", "table_name");
        if (select != null && !input(select, "table_alias", "").isBlank()) {
            table += " AS " + input(select, "table_alias", "");
        }
        List<String> joins = new ArrayList<>();
        List<String> wheres = new ArrayList<>();
        List<String> groups = new ArrayList<>();
        List<String> havings = new ArrayList<>();
        List<String> orders = new ArrayList<>();
        List<String> setOps = new ArrayList<>();
        String limit = "";

        for (BlockView block : snapped) {
            if (block == select) {
                continue;
            }
            switch (block.template.type) {
                case SQL_COLUMN -> projections.add(input(block, "column", "*"));
                case SQL_COUNT -> projections.add("COUNT(" + input(block, "expr", "*") + ")");
                case SQL_SUM -> projections.add("SUM(" + input(block, "expr", "amount") + ")");
                case SQL_AVG -> projections.add("AVG(" + input(block, "expr", "amount") + ")");
                case SQL_MIN -> projections.add("MIN(" + input(block, "expr", "amount") + ")");
                case SQL_MAX -> projections.add("MAX(" + input(block, "expr", "amount") + ")");
                case SQL_CONCAT -> projections.add("CONCAT(" + input(block, "a", "''") + ", " + input(block, "b", "''") + ")");
                case SQL_SUBSTRING -> projections.add("SUBSTRING(" + input(block, "expr", "''") + ", "
                        + input(block, "start", "1") + ", " + input(block, "len", "1") + ")");
                case SQL_LENGTH -> projections.add("LENGTH(" + input(block, "expr", "''") + ")");
                case SQL_UPPER -> projections.add("UPPER(" + input(block, "expr", "''") + ")");
                case SQL_LOWER -> projections.add("LOWER(" + input(block, "expr", "''") + ")");
                case SQL_TRIM -> projections.add("TRIM(" + input(block, "expr", "''") + ")");
                case SQL_LEFT -> projections.add("LEFT(" + input(block, "expr", "''") + ", " + input(block, "n", "1") + ")");
                case SQL_RIGHT -> projections.add("RIGHT(" + input(block, "expr", "''") + ", " + input(block, "n", "1") + ")");
                case SQL_REPLACE -> projections.add("REPLACE(" + input(block, "expr", "''") + ", "
                        + input(block, "from", "''") + ", " + input(block, "to", "''") + ")");
                case SQL_CURRENT_DATE -> projections.add("CURRENT_DATE");
                case SQL_CURRENT_TIME -> projections.add("CURRENT_TIME");
                case SQL_CURRENT_TIMESTAMP -> projections.add("CURRENT_TIMESTAMP");
                case SQL_COALESCE -> projections.add("COALESCE(" + input(block, "a", "NULL") + ", "
                        + input(block, "b", "NULL") + ")");
                case SQL_NULL_IF -> projections.add("NULLIF(" + input(block, "a", "1") + ", " + input(block, "b", "1") + ")");
                case SQL_FROM -> table = input(block, "table", table);
                case SQL_WHERE, SQL_AND -> wheres.add((wheres.isEmpty() ? "" : "AND ") + input(block, "predicate", "1 = 1"));
                case SQL_OR -> wheres.add((wheres.isEmpty() ? "" : "OR ") + input(block, "predicate", "1 = 1"));
                case SQL_JOIN -> joins.add("JOIN " + input(block, "table", "table_name") + " ON " + input(block, "on", "1 = 1"));
                case SQL_INNER_JOIN -> joins.add("INNER JOIN " + input(block, "table", "table_name") + " ON " + input(block, "on", "1 = 1"));
                case SQL_LEFT_JOIN -> joins.add("LEFT JOIN " + input(block, "table", "table_name") + " ON " + input(block, "on", "1 = 1"));
                case SQL_RIGHT_JOIN -> joins.add("RIGHT JOIN " + input(block, "table", "table_name") + " ON " + input(block, "on", "1 = 1"));
                case SQL_FULL_JOIN -> joins.add("FULL JOIN " + input(block, "table", "table_name") + " ON " + input(block, "on", "1 = 1"));
                case SQL_CROSS_JOIN -> joins.add("CROSS JOIN " + input(block, "table", "table_name"));
                case SQL_NATURAL_JOIN -> joins.add("NATURAL JOIN " + input(block, "table", "table_name"));
                case SQL_SELF_JOIN -> joins.add(", " + input(block, "table", "table_name") + " t2");
                case SQL_GROUP_BY -> groups.add(input(block, "expr", "id"));
                case SQL_HAVING -> havings.add(input(block, "predicate", "COUNT(*) > 0"));
                case SQL_ORDER_BY -> orders.add(input(block, "expr", "id DESC"));
                case SQL_LIMIT -> limit = input(block, "count", "5")
                        + (input(block, "offset", "").isBlank() ? "" : " OFFSET " + input(block, "offset", ""));
                case SQL_UNION -> setOps.add("UNION " + input(block, "sql", "SELECT 1"));
                case SQL_INTERSECT -> setOps.add("INTERSECT " + input(block, "sql", "SELECT 1"));
                case SQL_EXCEPT -> setOps.add("EXCEPT " + input(block, "sql", "SELECT 1"));
                default -> {
                    // Blocks that do not fit into a SELECT chain are ignored here.
                }
            }
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        if (select != null && "DISTINCT".equals(input(select, "select_mode", "ALL"))) sql.append("DISTINCT ");
        sql.append(String.join(", ", compactProjectionList(projections)));
        sql.append("\nFROM ").append(table);
        for (String join : joins) {
            sql.append("\n").append(join);
        }
        if (!wheres.isEmpty()) {
            sql.append("\nWHERE ").append(String.join(" ", wheres));
        }
        if (!groups.isEmpty()) {
            sql.append("\nGROUP BY ").append(String.join(", ", groups));
        }
        if (!havings.isEmpty()) {
            sql.append("\nHAVING ").append(String.join(" AND ", havings));
        }
        if (!orders.isEmpty()) {
            sql.append("\nORDER BY ").append(String.join(", ", orders));
        }
        for (String op : setOps) {
            sql.append("\n").append(op);
        }
        if (!limit.isEmpty()) sql.append("\nLIMIT ").append(limit);
        sql.append(";");
        return sql.toString();
    }

    private List<String> compactProjectionList(List<String> projections) {
        List<String> cleaned = projections.stream()
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .distinct()
                .toList();
        if (cleaned.isEmpty()) {
            return List.of("*");
        }
        if (cleaned.size() > 1 && cleaned.contains("*")) {
            return cleaned.stream().filter(value -> !value.equals("*")).toList();
        }
        return cleaned;
    }

    private String input(BlockView block, String key, String fallback) {
        BlockView reporter = block.reporters.get(key);
        if (reporter != null) return reporterSql(reporter);
        Object value = block.node.inputs().get(key);
        if (value == null) {
            return fallback;
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? fallback : text;
    }

    private void select(BlockView view) {
        if (selected != null) {
            selected.updateStyle(false);
        }
        selected = view;
        if (selected != null) {
            selected.updateStyle(true);
        }
    }

    private void deleteSelected() {
        if (selected == null) {
            return;
        }
        rememberUndoState();
        BlockView target = selected;
        selected = null;
        removeBlock(target);
        layoutSnappedBlocks();
        refreshSql();
    }

    private void removeBlock(BlockView target) {
        for (BlockView reporter : List.copyOf(target.reporters.values())) removeBlock(reporter);
        if (target.reporterOwner != null) {
            BlockView parent = target.reporterOwner;
            parent.reporters.remove(target.reporterKey);
            StackPane slot = parent.inputSlots.get(target.reporterKey);
            slot.getChildren().remove(target);
            parent.fields.get(target.reporterKey).setVisible(true);
            parent.fields.get(target.reporterKey).setManaged(true);
            parent.refreshGeometry();
        }
        blocks.remove(target);
        workspace.getChildren().remove(target);
    }

    private String reporterSql(BlockView reporter) {
        reporter.applyInputs();
        String sql = new SqlCompiler().compileWorkspace(List.of(reporter.node)).sql().strip();
        return sql.endsWith(";") ? sql.substring(0, sql.length() - 1) : sql;
    }

    private boolean acceptsReporter(BlockView owner, String key, BlockView reporter) {
        if (!List.of("columns", "column", "expr", "predicate", "a", "b", "lhs", "on", "value", "args",
                "when", "then", "else", "cond").contains(key)) return false;
        for (BlockView ancestor = owner; ancestor != null; ancestor = ancestor.reporterOwner) {
            if (ancestor == reporter) return false;
        }
        return true;
    }

    private void highlightReporterSlots(BlockView reporter, Point2D point) {
        clearSlotHighlights();
        if (reporter.template.visualKind() != VisualKind.EXPRESSION) return;
        for (BlockView block : blocks) {
            for (Map.Entry<String, StackPane> entry : block.inputSlots.entrySet()) {
                if (acceptsReporter(block, entry.getKey(), reporter)
                        && entry.getValue().getBoundsInLocal().contains(
                        entry.getValue().sceneToLocal(workspace.localToScene(point)))) {
                    entry.getValue().getStyleClass().add("reporter-target");
                }
            }
        }
    }

    private void clearSlotHighlights() {
        for (BlockView block : blocks) {
            for (StackPane slot : block.inputSlots.values()) slot.getStyleClass().remove("reporter-target");
        }
    }

    private void attachToNearestSlot(BlockView reporter) {
        Point2D scenePoint = reporter.localToScene(reporter.getPrefWidth() / 2, reporter.bodyHeight / 2);
        Point2D pointer = workspace.localToScene(reporter.getLayoutX() + 40, reporter.getLayoutY() + 20);
        for (BlockView owner : blocks) {
            for (Map.Entry<String, StackPane> entry : owner.inputSlots.entrySet()) {
                StackPane slot = entry.getValue();
                if (acceptsReporter(owner, entry.getKey(), reporter)
                        && (slot.getBoundsInLocal().contains(slot.sceneToLocal(scenePoint))
                        || slot.getBoundsInLocal().contains(slot.sceneToLocal(pointer)))) {
                    attachReporter(reporter, owner, entry.getKey());
                    return;
                }
            }
        }
    }

    private void attachReporter(BlockView reporter, BlockView parent, String key) {
        BlockView existing = parent.reporters.get(key);
        if (existing != null) detachReporter(existing);
        workspace.getChildren().remove(reporter);
        reporter.reporterOwner = parent;
        reporter.reporterKey = key;
        reporter.snapped = false;
        reporter.setLayoutX(0);
        reporter.setLayoutY(0);
        parent.reporters.put(key, reporter);
        parent.fields.get(key).setVisible(false);
        parent.fields.get(key).setManaged(false);
        parent.inputSlots.get(key).getChildren().add(reporter);
        parent.refreshGeometry();
    }

    private void detachReporter(BlockView reporter) {
        BlockView parent = reporter.reporterOwner;
        if (parent == null) return;
        Point2D location = workspace.sceneToLocal(reporter.localToScene(0, 0));
        StackPane slot = parent.inputSlots.get(reporter.reporterKey);
        slot.getChildren().remove(reporter);
        parent.reporters.remove(reporter.reporterKey);
        parent.fields.get(reporter.reporterKey).setVisible(true);
        parent.fields.get(reporter.reporterKey).setManaged(true);
        reporter.reporterOwner = null;
        reporter.reporterKey = null;
        workspace.getChildren().add(reporter);
        reporter.relocate(location.getX(), location.getY());
        parent.refreshGeometry();
    }

    private void clearWorkspace() {
        if (blocks.isEmpty()) {
            return;
        }
        rememberUndoState();
        workspace.getChildren().removeIf(node -> node instanceof BlockView);
        blocks.clear();
        selected = null;
        triggerNode.setNext(null);
        refreshSql();
    }

    private void undoLastAction() {
        if (undoStack.isEmpty()) {
            return;
        }
        WorkspaceSnapshot snapshot = undoStack.removeLast();
        restoreWorkspaceState(snapshot);
    }

    private void rememberUndoState() {
        pushUndoState(captureWorkspaceState());
    }

    private void rememberUndoState(WorkspaceSnapshot snapshot) {
        if (restoring || snapshot == null || snapshot.equals(captureWorkspaceState())) {
            return;
        }
        pushUndoState(snapshot);
    }

    private void pushUndoState(WorkspaceSnapshot snapshot) {
        if (restoring || snapshot == null) {
            return;
        }
        if (undoStack.isEmpty() || !undoStack.getLast().equals(snapshot)) {
            undoStack.add(snapshot);
        }
        if (undoStack.size() > 100) {
            undoStack.removeFirst();
        }
    }

    private WorkspaceSnapshot captureWorkspaceState() {
        List<BlockState> states = blocks.stream()
                .map(block -> new BlockState(
                        block.undoId,
                        block.template,
                        block.getLayoutX(),
                        block.getLayoutY(),
                        block.snapped,
                        block.fieldValues(),
                        block.reporterOwner == null ? null : block.reporterOwner.undoId,
                        block.reporterKey))
                .toList();
        return new WorkspaceSnapshot(states, selected == null ? null : selected.undoId);
    }

    private void restoreWorkspaceState(WorkspaceSnapshot snapshot) {
        restoring = true;
        workspace.getChildren().removeIf(node -> node instanceof BlockView);
        blocks.clear();
        selected = null;

        for (BlockState state : snapshot.blocks()) {
            BlockView view = new BlockView(state.template(), state.id(), state.inputs());
            view.setLayoutX(state.x());
            view.setLayoutY(state.y());
            view.snapped = state.snapped();
            blocks.add(view);
            workspace.getChildren().add(view);
            installDrag(view);
            if (Objects.equals(state.id(), snapshot.selectedId())) {
                selected = view;
            }
        }

        for (BlockView block : blocks) {
            block.updateStyle(block == selected);
        }
        for (BlockState state : snapshot.blocks()) {
            if (state.parentId() == null) continue;
            BlockView reporter = blocks.stream().filter(block -> block.undoId.equals(state.id())).findFirst().orElseThrow();
            BlockView parent = blocks.stream().filter(block -> block.undoId.equals(state.parentId())).findFirst().orElseThrow();
            attachReporter(reporter, parent, state.slotKey());
        }
        pendingDragSnapshot = null;
        pendingTextEditSnapshot = null;
        restoring = false;
        refreshSql();
    }

    private void setZoom(double nextZoom) {
        zoom = Math.max(MIN_ZOOM, Math.min(MAX_ZOOM, nextZoom));
        workspace.setScaleX(zoom);
        workspace.setScaleY(zoom);
        zoomLabel.setText(Math.round(zoom * 100) + "%");
    }

    private void centerOnChain() {
        if (workspaceScroll == null) {
            return;
        }
        double centerX = CHAIN_X + BLOCK_WIDTH / 2;
        double centerY = CHAIN_START_Y;
        workspaceScroll.setHvalue(clamp(centerX / CANVAS_WIDTH, 0, 1));
        workspaceScroll.setVvalue(clamp(centerY / CANVAS_HEIGHT, 0, 1));
    }

    private double clamp(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }

    private String primaryButtonStyle() {
        return "-fx-background-color: #1d4ed8; -fx-text-fill: white;"
                + "-fx-font-weight: bold; -fx-background-radius: 6; -fx-padding: 6 12 6 12;";
    }

    private Label inlineKeyword(String text) {
        Label keyword = new Label(text);
        keyword.setFont(Font.font("System", FontWeight.BOLD, 15));
        keyword.setMinWidth(Region.USE_PREF_SIZE);
        keyword.setStyle("-fx-text-fill: white; -fx-font-size: 15px; -fx-font-weight: bold;");
        return keyword;
    }

    private boolean isJoin(BlockType type) {
        return switch (type) {
            case SQL_JOIN, SQL_INNER_JOIN, SQL_LEFT_JOIN, SQL_RIGHT_JOIN,
                    SQL_FULL_JOIN, SQL_CROSS_JOIN, SQL_NATURAL_JOIN, SQL_SELF_JOIN -> true;
            default -> false;
        };
    }

    private final class BlockView extends StackPane {
        private final BlockTemplate template;
        private final BlockNode node;
        private final Map<String, TextField> fields = new LinkedHashMap<>();
        private final Map<String, StackPane> inputSlots = new LinkedHashMap<>();
        private final Map<String, BlockView> reporters = new LinkedHashMap<>();
        private final String undoId;
        private Path shape;
        private Path highlight;
        private final VBox content = new VBox(6);
        private double bodyHeight;
        private BlockView reporterOwner;
        private String reporterKey;
        private boolean snapped;

        private BlockView(BlockTemplate template) {
            this(template, UUID.randomUUID().toString(), Map.of());
        }

        private BlockView(BlockTemplate template, String undoId, Map<String, String> initialValues) {
            this.template = template;
            this.undoId = undoId;
            Map<String, Object> defaults = new LinkedHashMap<>(template.defaults);
            initialValues.forEach(defaults::put);
            this.node = new BlockNode(
                    template.idPrefix + "_" + UUID.randomUUID().toString().replace("-", ""),
                    template.type,
                    Position.zero(),
                    null,
                    List.of(),
                    defaults
            );

            setAlignment(Pos.TOP_LEFT);
            bodyHeight = template.visualKind().height(template);
            double inset = template.visualKind() == VisualKind.JOIN ? 26 : 14;
            content.setPadding(new Insets(0, 12, 0, inset));
            content.setAlignment(Pos.CENTER_LEFT);
            content.setPrefHeight(bodyHeight);
            HBox contentRow = new HBox(6);
            contentRow.setAlignment(Pos.CENTER_LEFT);
            if (template.type != BlockType.SQL_COLUMN && !template.blockLabel.isBlank()) {
                contentRow.getChildren().add(inlineKeyword(template.blockLabel));
            }
            content.getChildren().add(contentRow);
            boolean function = template.visualKind() == VisualKind.EXPRESSION
                    && template.type != BlockType.SQL_COLUMN && template.type != BlockType.SQL_TEXT
                    && template.type != BlockType.SQL_ALIAS && template.type != BlockType.SQL_FUNCTION;
            if (function) contentRow.getChildren().add(inlineKeyword("("));
            int index = 0;
            for (String key : template.fieldKeys()) {
                if (template.type == BlockType.SQL_SELECT && "table".equals(key)) {
                    contentRow.getChildren().add(inlineKeyword("FROM"));
                } else if ("table_alias".equals(key)) {
                    contentRow.getChildren().add(inlineKeyword("AS"));
                } else if (template.type == BlockType.SQL_ALIAS && "alias".equals(key)) {
                    contentRow.getChildren().add(inlineKeyword("AS"));
                } else if (template.type == BlockType.SQL_LIMIT && "offset".equals(key)) {
                    contentRow.getChildren().add(inlineKeyword("OFFSET"));
                } else if (template.type == BlockType.SQL_FUNCTION && "args".equals(key)) {
                    contentRow.getChildren().add(inlineKeyword("("));
                } else if (isJoin(template.type) && "on".equals(key)) {
                    contentRow = new HBox(6);
                    contentRow.setAlignment(Pos.CENTER_LEFT);
                    content.getChildren().add(contentRow);
                    contentRow.getChildren().add(inlineKeyword("ON"));
                } else if (function && index > 0) {
                    contentRow.getChildren().add(inlineKeyword(","));
                }
                String fieldValue = initialValues.getOrDefault(
                        key,
                        String.valueOf(template.defaults.getOrDefault(key, ""))
                );
                TextField field = new TextField(fieldValue);
                field.setPromptText(template.fieldLabels.get(key));
                field.getStyleClass().add("node-input");
                if (template.type == BlockType.SQL_SELECT && "columns".equals(key)) {
                    field.getStyleClass().add("expression-input");
                    field.setPrefHeight(34);
                }
                StackPane slot = new StackPane(field);
                slot.setAlignment(Pos.CENTER_LEFT);
                inputSlots.put(key, slot);
                if (List.of("order", "operator", "select_mode", "function").contains(key)) {
                    ComboBox<String> choices = new ComboBox<>(FXCollections.observableArrayList(
                            "order".equals(key) ? List.of("ASC", "DESC")
                                    : "select_mode".equals(key) ? List.of("ALL", "DISTINCT")
                                    : "function".equals(key) ? List.of("datetime", "date", "time", "strftime", "unixepoch",
                                    "julianday", "abs", "round", "length", "lower", "upper", "coalesce", "ifnull",
                                    "nullif", "substr", "replace", "printf", "json_extract")
                                    : List.of("=", "!=", ">", ">=", "<", "<=", "LIKE", "NOT LIKE", "IS", "IS NOT")));
                    choices.getStyleClass().add("node-choice");
                    choices.setValue(fieldValue);
                    field.setVisible(false);
                    field.setManaged(false);
                    choices.valueProperty().addListener((obs, old, value) -> field.setText(value));
                    field.textProperty().addListener((obs, old, value) -> {
                        if (!Objects.equals(choices.getValue(), value)) choices.setValue(value);
                    });
                    slot.getChildren().add(choices);
                    choices.setPrefWidth("operator".equals(key) ? 72 : "function".equals(key) ? 122 : 90);
                    choices.setMinWidth(Region.USE_PREF_SIZE);
                }
                field.focusedProperty().addListener((obs, old, focused) -> {
                    if (focused && !restoring) {
                        pendingTextEditSnapshot = captureWorkspaceState();
                    } else if (!focused) {
                        pendingTextEditSnapshot = null;
                    }
                });
                field.textProperty().addListener((obs, old, value) -> {
                    if (!restoring && !Objects.equals(old, value)) {
                        rememberUndoState(pendingTextEditSnapshot == null
                                ? captureWorkspaceStateWith(this, key, old)
                                : pendingTextEditSnapshot);
                    }
                    refreshGeometry();
                    refreshSql();
                });
                fields.put(key, field);
                contentRow.getChildren().add(slot);
                index++;
            }
            if (function || template.type == BlockType.SQL_FUNCTION) contentRow.getChildren().add(inlineKeyword(")"));
            getChildren().add(content);
            sceneProperty().addListener((obs, old, scene) -> {
                if (scene != null) {
                    applyCss();
                    refreshGeometry();
                }
            });
            refreshGeometry();
        }

        private void refreshGeometry() {
            for (Map.Entry<String, StackPane> entry : inputSlots.entrySet()) {
                BlockView reporter = reporters.get(entry.getKey());
                if (reporter != null) {
                    entry.getValue().setPrefSize(reporter.getPrefWidth(), reporter.bodyHeight);
                    entry.getValue().setMinSize(reporter.getPrefWidth(), reporter.bodyHeight);
                    continue;
                }
                TextField field = fields.get(entry.getKey());
                if (field == null) continue;
                Text measure = new Text(field.getText().isEmpty() ? field.getPromptText() : field.getText());
                measure.setFont(Font.font("System", FontWeight.BOLD, 13));
                double width = Math.max(42, Math.min(220, measure.getLayoutBounds().getWidth() + 24));
                field.setPrefWidth(width);
                field.setMinWidth(width);
                field.setMaxWidth(width);
                entry.getValue().setMinWidth(Region.USE_PREF_SIZE);
                entry.getValue().setPrefWidth(Region.USE_COMPUTED_SIZE);
                entry.getValue().setPrefHeight(Region.USE_COMPUTED_SIZE);
                entry.getValue().setMinHeight(Region.USE_PREF_SIZE);
            }
            double width = Math.max(template.visualKind() == VisualKind.EXPRESSION ? 120 : BLOCK_WIDTH,
                    content.getChildren().stream().mapToDouble(row -> ((Region) row).prefWidth(-1)).max().orElse(0)
                            + content.getPadding().getLeft() + content.getPadding().getRight());
            double totalHeight = bodyHeight + (template.visualKind().hasBottom ? CHAIN_GAP : 0);
            setPrefSize(width, totalHeight);
            setMinSize(width, totalHeight);
            setMaxSize(width, totalHeight);
            content.setPrefWidth(width);
            if (shape != null) getChildren().removeAll(shape, highlight);
            shape = createBlockShape(template.visualKind(), width, bodyHeight, Color.web(template.nodeColor()));
            highlight = createBlockShape(template.visualKind(), width, bodyHeight, Color.TRANSPARENT);
            highlight.setEffect(null);
            getChildren().addAll(0, List.of(shape, highlight));
            updateStyle(selected == this);
            if (reporterOwner != null) reporterOwner.refreshGeometry();
        }

        private void updateStyle(boolean selected) {
            highlight.setStroke(selected ? Color.web("#fff176") : Color.rgb(255, 255, 255, 0.34));
            highlight.setStrokeWidth(selected ? 3 : 1.4);
        }

        private void applyInputs() {
            for (Map.Entry<String, TextField> entry : fields.entrySet()) {
                BlockView reporter = reporters.get(entry.getKey());
                node.inputs().put(entry.getKey(), reporter == null ? entry.getValue().getText() : reporterSql(reporter));
            }
            if (template.type == BlockType.SQL_WHERE || template.type == BlockType.SQL_AND || template.type == BlockType.SQL_OR) {
                node.inputs().put("predicate", input(this, "column", "id") + " "
                        + input(this, "operator", "=") + " " + input(this, "value", "1"));
            } else if (template.type == BlockType.SQL_ORDER_BY) {
                node.inputs().put("expr", input(this, "column", "id") + " " + input(this, "order", "ASC"));
            }
        }

        private Map<String, String> fieldValues() {
            Map<String, String> values = new LinkedHashMap<>();
            for (Map.Entry<String, TextField> entry : fields.entrySet()) {
                values.put(entry.getKey(), entry.getValue().getText());
            }
            return values;
        }
    }

    private WorkspaceSnapshot captureWorkspaceStateWith(BlockView editedBlock, String editedKey, String oldValue) {
        List<BlockState> states = blocks.stream()
                .map(block -> {
                    Map<String, String> values = block.fieldValues();
                    if (block == editedBlock) {
                        values.put(editedKey, oldValue);
                    }
                    return new BlockState(
                            block.undoId,
                            block.template,
                            block.getLayoutX(),
                            block.getLayoutY(),
                            block.snapped,
                            values,
                            block.reporterOwner == null ? null : block.reporterOwner.undoId,
                            block.reporterKey);
                })
                .toList();
        return new WorkspaceSnapshot(states, selected == null ? null : selected.undoId);
    }

    private record WorkspaceSnapshot(List<BlockState> blocks, String selectedId) {
    }

    private record BlockState(
            String id,
            BlockTemplate template,
            double x,
            double y,
            boolean snapped,
            Map<String, String> inputs,
            String parentId,
            String slotKey) {
        private BlockState {
            inputs = Map.copyOf(inputs);
        }
    }

    private enum VisualKind {
        TRIGGER(false, true), STATEMENT(true, true), CLAUSE(true, true),
        JOIN(true, true), SET_OPERATOR(true, true), EXPRESSION(false, false), TERMINAL(true, false);

        private final boolean hasTop;
        private final boolean hasBottom;

        VisualKind(boolean hasTop, boolean hasBottom) {
            this.hasTop = hasTop;
            this.hasBottom = hasBottom;
        }

        private double height(BlockTemplate template) {
            return switch (this) {
                case TRIGGER, CLAUSE -> 50;
                case STATEMENT -> 56;
                case JOIN -> template.fieldLabels.containsKey("on") ? 86 : 66;
                case SET_OPERATOR -> 54;
                case EXPRESSION -> 44;
                case TERMINAL -> 48;
            };
        }
    }

    private enum Category {
        DQL("SQL", "Query data"), JOIN_SET("JOIN", "Joins & sets"),
        AGGREGATE("Σ", "Aggregates"), FUNCTION("ƒx", "Expressions"),
        DML("DML", "Edit data"), DDL("DDL", "Schema"),
        DCL("DCL", "Permissions"), TCL("TCL", "Transactions");

        private final String railLabel;
        private final String title;

        Category(String railLabel, String title) {
            this.railLabel = railLabel;
            this.title = title;
        }

        private String icon() {
            return switch (this) {
                case DQL -> "\ue86f";
                case JOIN_SET, TCL -> "\ue97a";
                case AGGREGATE -> "\ue24a";
                case FUNCTION -> "\uead3";
                case DML -> "\ue745";
                case DDL -> "\ue4fd";
                case DCL -> "\ue897";
            };
        }

        private String railColor() {
            return switch (this) {
                case DQL -> "#4c97ff";
                case JOIN_SET -> "#7c3aed";
                case AGGREGATE -> "#15803d";
                case FUNCTION -> "#5cb1d6";
                case DML -> "#ffab19";
                case DDL -> "#59c059";
                case DCL -> "#ffbf00";
                case TCL -> "#ff8c1a";
            };
        }
    }

    private enum BlockTemplate {
        SELECT("SELECT", "SELECT", Category.DQL, BlockType.SQL_SELECT, "select", "#2563eb",
                Map.of("select_mode", "mode", "columns", "columns", "table", "table", "table_alias", "alias"),
                Map.of("select_mode", "ALL", "columns", "*", "table", "table_name", "table_alias", "")),
        COLUMN("COLUMN", "COLUMN", Category.DQL, BlockType.SQL_COLUMN, "column", "#6d28d9",
                Map.of("column", "column"),
                Map.of("column", "*")),
        TEXT("TEXT", "TEXT", Category.DQL, BlockType.SQL_TEXT, "text", "#6d28d9",
                Map.of("text", "text"), Map.of("text", "Text")),
        ALIAS("AS / ALIAS", "", Category.DQL, BlockType.SQL_ALIAS, "alias", "#6d28d9",
                Map.of("value", "expression", "alias", "alias"), Map.of("value", "id", "alias", "alias")),
        SQLITE_FUNCTION("SQLITE FUNCTION", "", Category.FUNCTION, BlockType.SQL_FUNCTION, "function", "#6d28d9",
                Map.of("function", "function", "args", "arguments"),
                Map.of("function", "datetime", "args", "unix_timestamp, 'unixepoch'")),
        FROM("FROM", "FROM", Category.DQL, BlockType.SQL_FROM, "from", "#0f766e",
                Map.of("table", "table"),
                Map.of("table", "table_name")),
        WHERE("WHERE", "WHERE", Category.DQL, BlockType.SQL_WHERE, "where", "#0284c7",
                Map.of("column", "column", "operator", "operator", "value", "value"),
                Map.of("column", "id", "operator", "=", "value", "1")),
        AND("AND", "AND", Category.DQL, BlockType.SQL_AND, "and", "#0284c7",
                Map.of("column", "column", "operator", "operator", "value", "value"),
                Map.of("column", "id", "operator", "=", "value", "1")),
        OR("OR", "OR", Category.DQL, BlockType.SQL_OR, "or", "#0284c7",
                Map.of("column", "column", "operator", "operator", "value", "value"),
                Map.of("column", "id", "operator", "=", "value", "1")),
        GROUP_BY("GROUP BY", "GROUP BY", Category.DQL, BlockType.SQL_GROUP_BY, "group", "#0284c7",
                Map.of("expr", "column"),
                Map.of("expr", "id")),
        HAVING("HAVING", "HAVING", Category.DQL, BlockType.SQL_HAVING, "having", "#0284c7",
                Map.of("predicate", "condition"),
                Map.of("predicate", "COUNT(*) > 0")),
        ORDER_BY("ORDER BY", "ORDER BY", Category.DQL, BlockType.SQL_ORDER_BY, "order", "#0284c7",
                Map.of("column", "column", "order", "direction"),
                Map.of("column", "id", "order", "ASC")),
        LIMIT("LIMIT", "LIMIT", Category.DQL, BlockType.SQL_LIMIT, "limit", "#0284c7",
                Map.of("count", "count", "offset", "offset"), Map.of("count", "5", "offset", "")),

        JOIN("JOIN", "JOIN", Category.JOIN_SET, BlockType.SQL_JOIN, "join", "#7c3aed",
                Map.of("table", "table", "on", "condition"),
                Map.of("table", "table_name", "on", "1 = 1")),
        INNER_JOIN("INNER JOIN", "INNER JOIN", Category.JOIN_SET, BlockType.SQL_INNER_JOIN, "inner_join", "#7c3aed",
                Map.of("table", "table", "on", "condition"),
                Map.of("table", "table_name", "on", "1 = 1")),
        LEFT_JOIN("LEFT JOIN", "LEFT JOIN", Category.JOIN_SET, BlockType.SQL_LEFT_JOIN, "left_join", "#7c3aed",
                Map.of("table", "table", "on", "condition"),
                Map.of("table", "table_name", "on", "1 = 1")),
        RIGHT_JOIN("RIGHT JOIN", "RIGHT JOIN", Category.JOIN_SET, BlockType.SQL_RIGHT_JOIN, "right_join", "#7c3aed",
                Map.of("table", "table", "on", "condition"),
                Map.of("table", "table_name", "on", "1 = 1")),
        FULL_JOIN("FULL JOIN", "FULL JOIN", Category.JOIN_SET, BlockType.SQL_FULL_JOIN, "full_join", "#7c3aed",
                Map.of("table", "table", "on", "condition"),
                Map.of("table", "table_name", "on", "1 = 1")),
        CROSS_JOIN("CROSS JOIN", "CROSS JOIN", Category.JOIN_SET, BlockType.SQL_CROSS_JOIN, "cross_join", "#7c3aed",
                Map.of("table", "table"),
                Map.of("table", "table_name")),
        NATURAL_JOIN("NATURAL JOIN", "NATURAL JOIN", Category.JOIN_SET, BlockType.SQL_NATURAL_JOIN, "natural_join", "#7c3aed",
                Map.of("table", "table"),
                Map.of("table", "table_name")),
        SELF_JOIN("SELF JOIN", "SELF JOIN", Category.JOIN_SET, BlockType.SQL_SELF_JOIN, "self_join", "#7c3aed",
                Map.of("table", "table", "on", "condition"),
                Map.of("table", "table_name", "on", "t1.id = t2.id")),
        UNION("UNION", "UNION", Category.JOIN_SET, BlockType.SQL_UNION, "union", "#4338ca",
                Map.of("sql", "sql"),
                Map.of("sql", "SELECT 1")),
        INTERSECT("INTERSECT", "INTERSECT", Category.JOIN_SET, BlockType.SQL_INTERSECT, "intersect", "#4338ca",
                Map.of("sql", "sql"),
                Map.of("sql", "SELECT 1")),
        EXCEPT("EXCEPT", "EXCEPT", Category.JOIN_SET, BlockType.SQL_EXCEPT, "except", "#4338ca",
                Map.of("sql", "sql"),
                Map.of("sql", "SELECT 1")),
        SUBQUERY_IN("IN SUBQUERY", "IN", Category.JOIN_SET, BlockType.SQL_SUBQUERY_IN, "in", "#4338ca",
                Map.of("lhs", "lhs", "sql", "sql"),
                Map.of("lhs", "id", "sql", "SELECT id FROM t")),
        SUBQUERY_ANY("ANY SUBQUERY", "ANY", Category.JOIN_SET, BlockType.SQL_SUBQUERY_ANY, "any", "#4338ca",
                Map.of("lhs", "lhs", "sql", "sql"),
                Map.of("lhs", "id", "sql", "SELECT id FROM t")),
        SUBQUERY_ALL("ALL SUBQUERY", "ALL", Category.JOIN_SET, BlockType.SQL_SUBQUERY_ALL, "all", "#4338ca",
                Map.of("lhs", "lhs", "sql", "sql"),
                Map.of("lhs", "id", "sql", "SELECT id FROM t")),

        COUNT("COUNT", "COUNT", Category.AGGREGATE, BlockType.SQL_COUNT, "count", "#15803d",
                Map.of("expr", "expr"),
                Map.of("expr", "*")),
        SUM("SUM", "SUM", Category.AGGREGATE, BlockType.SQL_SUM, "sum", "#15803d",
                Map.of("expr", "expr"),
                Map.of("expr", "amount")),
        AVG("AVG", "AVG", Category.AGGREGATE, BlockType.SQL_AVG, "avg", "#15803d",
                Map.of("expr", "expr"),
                Map.of("expr", "amount")),
        MIN("MIN", "MIN", Category.AGGREGATE, BlockType.SQL_MIN, "min", "#15803d",
                Map.of("expr", "expr"),
                Map.of("expr", "amount")),
        MAX("MAX", "MAX", Category.AGGREGATE, BlockType.SQL_MAX, "max", "#15803d",
                Map.of("expr", "expr"),
                Map.of("expr", "amount")),

        CONCAT("CONCAT", "CONCAT", Category.FUNCTION, BlockType.SQL_CONCAT, "concat", "#be185d",
                Map.of("a", "a", "b", "b"),
                Map.of("a", "''", "b", "''")),
        SUBSTRING("SUBSTRING", "SUBSTRING", Category.FUNCTION, BlockType.SQL_SUBSTRING, "substring", "#be185d",
                Map.of("expr", "expr", "start", "start", "len", "len"),
                Map.of("expr", "''", "start", "1", "len", "1")),
        LENGTH("LENGTH", "LENGTH", Category.FUNCTION, BlockType.SQL_LENGTH, "length", "#be185d",
                Map.of("expr", "expr"),
                Map.of("expr", "''")),
        UPPER("UPPER", "UPPER", Category.FUNCTION, BlockType.SQL_UPPER, "upper", "#be185d",
                Map.of("expr", "expr"),
                Map.of("expr", "''")),
        LOWER("LOWER", "LOWER", Category.FUNCTION, BlockType.SQL_LOWER, "lower", "#be185d",
                Map.of("expr", "expr"),
                Map.of("expr", "''")),
        TRIM("TRIM", "TRIM", Category.FUNCTION, BlockType.SQL_TRIM, "trim", "#be185d",
                Map.of("expr", "expr"),
                Map.of("expr", "''")),
        LEFT("LEFT", "LEFT", Category.FUNCTION, BlockType.SQL_LEFT, "left", "#be185d",
                Map.of("expr", "expr", "n", "n"),
                Map.of("expr", "''", "n", "1")),
        RIGHT("RIGHT", "RIGHT", Category.FUNCTION, BlockType.SQL_RIGHT, "right", "#be185d",
                Map.of("expr", "expr", "n", "n"),
                Map.of("expr", "''", "n", "1")),
        REPLACE("REPLACE", "REPLACE", Category.FUNCTION, BlockType.SQL_REPLACE, "replace", "#be185d",
                Map.of("expr", "expr", "from", "from", "to", "to"),
                Map.of("expr", "''", "from", "''", "to", "''")),
        CURRENT_DATE("CURRENT DATE", "CURRENT_DATE", Category.FUNCTION, BlockType.SQL_CURRENT_DATE, "current_date", "#be185d",
                Map.of(),
                Map.of()),
        CURRENT_TIME("CURRENT TIME", "CURRENT_TIME", Category.FUNCTION, BlockType.SQL_CURRENT_TIME, "current_time", "#be185d",
                Map.of(),
                Map.of()),
        CURRENT_TIMESTAMP("CURRENT TIMESTAMP", "CURRENT_TIMESTAMP", Category.FUNCTION, BlockType.SQL_CURRENT_TIMESTAMP, "current_timestamp", "#be185d",
                Map.of(),
                Map.of()),
        DATE_PART("DATE_PART", "DATE_PART", Category.FUNCTION, BlockType.SQL_DATE_PART, "date_part", "#be185d",
                Map.of("part", "part", "expr", "date"),
                Map.of("part", "'day'", "expr", "CURRENT_DATE")),
        DATE_ADD("DATE_ADD", "DATE_ADD", Category.FUNCTION, BlockType.SQL_DATE_ADD, "date_add", "#be185d",
                Map.of("expr", "date", "n", "n", "unit", "unit"),
                Map.of("expr", "CURRENT_DATE", "n", "1", "unit", "DAY")),
        DATE_SUB("DATE_SUB", "DATE_SUB", Category.FUNCTION, BlockType.SQL_DATE_SUB, "date_sub", "#be185d",
                Map.of("expr", "date", "n", "n", "unit", "unit"),
                Map.of("expr", "CURRENT_DATE", "n", "1", "unit", "DAY")),
        EXTRACT("EXTRACT", "EXTRACT", Category.FUNCTION, BlockType.SQL_EXTRACT, "extract", "#be185d",
                Map.of("part", "part", "expr", "date"),
                Map.of("part", "DAY", "expr", "CURRENT_DATE")),
        TO_CHAR("TO_CHAR", "TO_CHAR", Category.FUNCTION, BlockType.SQL_TO_CHAR, "to_char", "#be185d",
                Map.of("expr", "date", "fmt", "format"),
                Map.of("expr", "CURRENT_DATE", "fmt", "'YYYY-MM-DD'")),
        TIMESTAMP_DIFF("TIMESTAMPDIFF", "TIMESTAMPDIFF", Category.FUNCTION, BlockType.SQL_TIMESTAMP_DIFF, "timestamp_diff", "#be185d",
                Map.of("unit", "unit", "a", "a", "b", "b"),
                Map.of("unit", "DAY", "a", "CURRENT_DATE", "b", "CURRENT_DATE")),
        DATE_DIFF("DATEDIFF", "DATEDIFF", Category.FUNCTION, BlockType.SQL_DATE_DIFF, "date_diff", "#be185d",
                Map.of("a", "a", "b", "b"),
                Map.of("a", "CURRENT_DATE", "b", "CURRENT_DATE")),
        CASE("CASE", "CASE", Category.FUNCTION, BlockType.SQL_CASE, "case", "#be185d",
                Map.of("when", "when", "then", "then", "else", "else"),
                Map.of("when", "1=1", "then", "'x'", "else", "'y'")),
        IF("IF", "IF", Category.FUNCTION, BlockType.SQL_IF, "if", "#be185d",
                Map.of("cond", "cond", "a", "a", "b", "b"),
                Map.of("cond", "1=1", "a", "'x'", "b", "'y'")),
        COALESCE("COALESCE", "COALESCE", Category.FUNCTION, BlockType.SQL_COALESCE, "coalesce", "#be185d",
                Map.of("a", "a", "b", "b"),
                Map.of("a", "NULL", "b", "NULL")),
        NULL_IF("NULLIF", "NULLIF", Category.FUNCTION, BlockType.SQL_NULL_IF, "nullif", "#be185d",
                Map.of("a", "a", "b", "b"),
                Map.of("a", "1", "b", "1")),

        INSERT("INSERT", "INSERT", Category.DML, BlockType.SQL_INSERT, "insert", "#16a34a",
                Map.of("table", "table", "values", "values"),
                Map.of("table", "table_name", "values", "")),
        UPDATE("UPDATE", "UPDATE", Category.DML, BlockType.SQL_UPDATE, "update", "#ca8a04",
                Map.of("table", "table", "set", "set"),
                Map.of("table", "table_name", "set", "col = value")),
        DELETE("DELETE", "DELETE", Category.DML, BlockType.SQL_DELETE, "delete", "#dc2626",
                Map.of("table", "table"),
                Map.of("table", "table_name")),

        CREATE_TABLE("CREATE TABLE", "CREATE TABLE", Category.DDL, BlockType.SQL_CREATE_TABLE, "create", "#4f46e5",
                Map.of("table", "table", "definition", "columns"),
                Map.of("table", "new_table", "definition", "id INTEGER PRIMARY KEY")),
        ALTER_TABLE("ALTER TABLE", "ALTER TABLE", Category.DDL, BlockType.SQL_ALTER_TABLE, "alter", "#4f46e5",
                Map.of("table", "table", "alter", "alter"),
                Map.of("table", "table_name", "alter", "ADD COLUMN c TEXT")),
        TRUNCATE("TRUNCATE", "TRUNCATE", Category.DDL, BlockType.SQL_TRUNCATE, "truncate", "#4f46e5",
                Map.of("table", "table"),
                Map.of("table", "table_name")),
        DROP_TABLE("DROP TABLE", "DROP TABLE", Category.DDL, BlockType.SQL_DROP_TABLE, "drop", "#991b1b",
                Map.of("table", "table"),
                Map.of("table", "table_name")),

        GRANT("GRANT", "GRANT", Category.DCL, BlockType.SQL_GRANT, "grant", "#64748b",
                Map.of("privilege", "privilege", "table", "table", "user", "user"),
                Map.of("privilege", "SELECT", "table", "table_name", "user", "user")),
        REVOKE("REVOKE", "REVOKE", Category.DCL, BlockType.SQL_REVOKE, "revoke", "#64748b",
                Map.of("privilege", "privilege", "table", "table", "user", "user"),
                Map.of("privilege", "SELECT", "table", "table_name", "user", "user")),

        COMMIT("COMMIT", "COMMIT", Category.TCL, BlockType.SQL_COMMIT, "commit", "#475569",
                Map.of(),
                Map.of()),
        ROLLBACK("ROLLBACK", "ROLLBACK", Category.TCL, BlockType.SQL_ROLLBACK, "rollback", "#475569",
                Map.of(),
                Map.of()),
        SAVEPOINT("SAVEPOINT", "SAVEPOINT", Category.TCL, BlockType.SQL_SAVEPOINT, "savepoint", "#475569",
                Map.of("name", "name"),
                Map.of("name", "sp1")),
        ROLLBACK_TO_SAVEPOINT("ROLLBACK TO SAVEPOINT", "ROLLBACK TO SAVEPOINT", Category.TCL, BlockType.SQL_ROLLBACK_TO_SAVEPOINT, "rollback_sp", "#475569",
                Map.of("name", "name"),
                Map.of("name", "sp1")),
        SET_TRANSACTION("SET TRANSACTION", "SET TRANSACTION", Category.TCL, BlockType.SQL_SET_TRANSACTION, "transaction", "#475569",
                Map.of("level", "level"),
                Map.of("level", "READ COMMITTED"));

        private final String paletteLabel;
        private final String blockLabel;
        private final Category category;
        private final BlockType type;
        private final String idPrefix;
        private final String color;
        private final Map<String, String> fieldLabels;
        private final Map<String, Object> defaults;

        private VisualKind visualKind() {
            if (category == Category.AGGREGATE || category == Category.FUNCTION || type == BlockType.SQL_COLUMN
                    || type == BlockType.SQL_TEXT || type == BlockType.SQL_ALIAS
                    || type == BlockType.SQL_SUBQUERY_IN || type == BlockType.SQL_SUBQUERY_ANY
                    || type == BlockType.SQL_SUBQUERY_ALL) return VisualKind.EXPRESSION;
            if (category == Category.JOIN_SET) {
                return type == BlockType.SQL_UNION || type == BlockType.SQL_INTERSECT || type == BlockType.SQL_EXCEPT
                        ? VisualKind.SET_OPERATOR : VisualKind.JOIN;
            }
            if (type == BlockType.SQL_COMMIT || type == BlockType.SQL_ROLLBACK) return VisualKind.TERMINAL;
            return category == Category.DQL && type != BlockType.SQL_SELECT ? VisualKind.CLAUSE : VisualKind.STATEMENT;
        }

        private String nodeColor() {
            return switch (visualKind()) {
                case EXPRESSION -> "#6d28d9";
                case JOIN -> "#7c3aed";
                case SET_OPERATOR -> "#4338ca";
                case TERMINAL -> "#b91c1c";
                case TRIGGER -> "#ffbf00";
                case STATEMENT -> category == Category.DML ? "#b45309"
                        : category == Category.DDL || category == Category.DCL ? "#be185d"
                        : category == Category.TCL ? "#b91c1c" : "#2563eb";
                case CLAUSE -> type == BlockType.SQL_FROM ? "#0f766e"
                        : type == BlockType.SQL_GROUP_BY || type == BlockType.SQL_HAVING ? "#15803d" : "#0284c7";
            };
        }

        private List<String> fieldKeys() {
            List<String> order = switch (type) {
                case SQL_SELECT -> List.of("select_mode", "columns", "table", "table_alias");
                case SQL_WHERE, SQL_AND, SQL_OR -> List.of("column", "operator", "value");
                case SQL_ORDER_BY -> List.of("column", "order");
                case SQL_LIMIT -> List.of("count", "offset");
                case SQL_ALIAS -> List.of("value", "alias");
                case SQL_FUNCTION -> List.of("function", "args");
                case SQL_JOIN, SQL_INNER_JOIN, SQL_LEFT_JOIN, SQL_RIGHT_JOIN, SQL_FULL_JOIN, SQL_SELF_JOIN -> List.of("table", "on");
                case SQL_SUBQUERY_IN, SQL_SUBQUERY_ANY, SQL_SUBQUERY_ALL -> List.of("lhs", "sql");
                case SQL_SUBSTRING -> List.of("expr", "start", "len");
                case SQL_REPLACE -> List.of("expr", "from", "to");
                case SQL_DATE_ADD, SQL_DATE_SUB -> List.of("expr", "n", "unit");
                case SQL_DATE_PART, SQL_EXTRACT -> List.of("part", "expr");
                case SQL_TIMESTAMP_DIFF -> List.of("unit", "a", "b");
                case SQL_CASE -> List.of("when", "then", "else");
                case SQL_IF -> List.of("cond", "a", "b");
                case SQL_TO_CHAR -> List.of("expr", "fmt");
                case SQL_LEFT, SQL_RIGHT -> List.of("expr", "n");
                case SQL_INSERT -> List.of("table", "values");
                case SQL_UPDATE -> List.of("table", "set");
                case SQL_CREATE_TABLE -> List.of("table", "definition");
                case SQL_ALTER_TABLE -> List.of("table", "alter");
                case SQL_GRANT, SQL_REVOKE -> List.of("privilege", "table", "user");
                default -> fieldLabels.keySet().stream().sorted().toList();
            };
            return order;
        }

        private String previewLabel() {
            return switch (type) {
                case SQL_SELECT -> "SELECT [columns] FROM…";
                case SQL_COLUMN -> "[column]";
                case SQL_TEXT -> "TEXT {text}";
                case SQL_ALIAS -> "[value] AS [alias]";
                case SQL_FUNCTION -> "[function] ([args])";
                case SQL_WHERE -> "WHERE [condition]";
                case SQL_ORDER_BY -> "ORDER BY [column]";
                default -> blockLabel + (visualKind() == VisualKind.JOIN ? " [table]\nON [condition]"
                        : fieldLabels.isEmpty() ? "" : " [" + fieldKeys().getFirst() + "]");
            };
        }

        BlockTemplate(
                String paletteLabel,
                String blockLabel,
                Category category,
                BlockType type,
                String idPrefix,
                String color,
                Map<String, String> fieldLabels,
                Map<String, Object> defaults) {
            this.paletteLabel = paletteLabel;
            this.blockLabel = blockLabel;
            this.category = category;
            this.type = type;
            this.idPrefix = idPrefix;
            this.color = color;
            this.fieldLabels = fieldLabels;
            this.defaults = defaults;
        }
    }
}
