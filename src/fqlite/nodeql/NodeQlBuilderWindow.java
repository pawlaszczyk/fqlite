package fqlite.nodeql;

import digital.codespiresolutions.nodeql.BlockNode;
import digital.codespiresolutions.nodeql.BlockType;
import digital.codespiresolutions.nodeql.Position;
import digital.codespiresolutions.nodeql.SqlCompileResult;
import digital.codespiresolutions.nodeql.SqlCompiler;
import fqlite.base.GUI;
import fqlite.ui.NodeObject;
import javafx.geometry.Insets;
import javafx.geometry.Point2D;
import javafx.scene.Cursor;
import javafx.scene.Scene;
import javafx.scene.control.Accordion;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.TitledPane;
import javafx.scene.control.ToolBar;
import javafx.scene.control.TreeItem;
import javafx.scene.input.KeyCode;
import javafx.scene.input.MouseButton;
import javafx.scene.input.ScrollEvent;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.shape.Line;
import javafx.scene.shape.Rectangle;
import javafx.stage.Modality;
import javafx.stage.Stage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class NodeQlBuilderWindow {
    private static final double BLOCK_WIDTH = 310;
    private static final double BLOCK_HEIGHT = 70;
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
    private final Stage owner;
    private final Stage stage = new Stage();
    private final Pane workspace = new Pane();
    private final TextArea sqlPreview = new TextArea();
    private final Label zoomLabel = new Label("100%");
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

    public NodeQlBuilderWindow(GUI app, TreeItem<NodeObject> databaseNode, Stage owner) {
        this.app = app;
        this.databaseNode = databaseNode;
        this.owner = owner;
    }

    public void show() {
        stage.setTitle("Forensic SQLite NodeQL Builder");
        stage.initOwner(owner);
        stage.initModality(Modality.NONE);

        BorderPane root = new BorderPane();
        root.getStyleClass().add("nodeql-builder");
        root.setStyle("-fx-background-color: #f4f7fb;");
        root.setLeft(createPalette());
        root.setCenter(createWorkspace());
        root.setRight(createPreview());
        root.setTop(createToolbar());

        Scene scene = new Scene(root, 1240, 760);
        scene.setOnKeyPressed(event -> {
            if (event.isShortcutDown() && event.getCode() == KeyCode.Z && !event.isShiftDown()) {
                undoLastAction();
                event.consume();
            } else if (event.getCode() == KeyCode.DELETE || event.getCode() == KeyCode.BACK_SPACE) {
                deleteSelected();
                event.consume();
            }
        });
        scene.getStylesheets().add(Objects.requireNonNull(GUI.class.getResource("/nodeql-builder.css")).toExternalForm());
        stage.setScene(scene);
        stage.show();
        refreshSql();
    }

    private ToolBar createToolbar() {
        Button sendToAnalyzer = new Button("Open in SQL Analyzer");
        sendToAnalyzer.setOnAction(e -> app.showSqlWindow(sqlPreview.getText(), databaseNode));
        sendToAnalyzer.setStyle(primaryButtonStyle());

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
        zoomLabel.setStyle("-fx-text-fill: #1f2937; -fx-font-weight: bold;");
        ToolBar toolbar = new ToolBar(sendToAnalyzer, delete, clear, zoomOut, zoomIn, resetZoom, zoomLabel);
        toolbar.setStyle("-fx-background-color: #ffffff; -fx-border-color: #d7dee9; -fx-border-width: 0 0 1 0;"
                + "-fx-padding: 8 10 8 10;");
        return toolbar;
    }

    private VBox createPalette() {
        VBox palette = new VBox(8);
        palette.setPadding(new Insets(14));
        palette.setPrefWidth(285);
        palette.setStyle("-fx-background-color: #ffffff; -fx-border-color: #d7dee9; -fx-border-width: 0 1 0 0;");

        Label title = new Label("Forensic SQLite Blocks");
        title.setStyle("-fx-text-fill: #111827; -fx-font-size: 16px; -fx-font-weight: bold;");
        Label subtitle = new Label("Build audit queries for recovered SQLite data");
        subtitle.setStyle("-fx-text-fill: #5b677a; -fx-font-size: 11px;");

        Accordion accordion = new Accordion();
        accordion.getPanes().addAll(
                paneFor("DQL", Category.DQL),
                paneFor("Joins & Sets", Category.JOIN_SET),
                paneFor("Aggregates", Category.AGGREGATE),
                paneFor("Functions", Category.FUNCTION),
                paneFor("DML", Category.DML),
                paneFor("DDL", Category.DDL),
                paneFor("DCL", Category.DCL),
                paneFor("TCL", Category.TCL)
        );
        accordion.setExpandedPane(accordion.getPanes().getFirst());
        VBox.setVgrow(accordion, Priority.ALWAYS);

        palette.getChildren().addAll(title, subtitle, accordion);
        return palette;
    }

    private TitledPane paneFor(String title, Category category) {
        VBox content = new VBox(6);
        content.setPadding(new Insets(8));
        content.setStyle("-fx-background-color: #f8fafc;");
        for (BlockTemplate template : BlockTemplate.values()) {
            if (template.category != category) {
                continue;
            }
            Button button = new Button(template.paletteLabel);
            button.setMaxWidth(Double.MAX_VALUE);
            button.setStyle(paletteButtonStyle(template.color));
            button.setOnAction(e -> addBlock(template));
            content.getChildren().add(button);
        }
        TitledPane pane = new TitledPane(title, content);
        pane.setStyle("-fx-text-fill: #1f2937; -fx-font-weight: bold;"
                + "-fx-background-color: #ffffff; -fx-border-color: #d7dee9;");
        return pane;
    }

    private ScrollPane createWorkspace() {
        workspace.setMinSize(CANVAS_WIDTH, CANVAS_HEIGHT);
        workspace.setPrefSize(CANVAS_WIDTH, CANVAS_HEIGHT);
        workspace.setStyle("-fx-background-color: #f5f8fc;");
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
        workspaceScroll.setStyle("-fx-background: #f5f8fc; -fx-background-color: #f5f8fc;");
        workspaceScroll.viewportBoundsProperty().addListener((obs, old, bounds) -> centerOnChain());
        return workspaceScroll;
    }

    private VBox createPreview() {
        VBox box = new VBox(8);
        box.setPadding(new Insets(14));
        box.setPrefWidth(410);
        box.setStyle("-fx-background-color: #ffffff; -fx-border-color: #d7dee9; -fx-border-width: 0 0 0 1;");

        Label label = new Label("Forensic SQL Preview");
        label.setStyle("-fx-text-fill: #111827; -fx-font-size: 16px; -fx-font-weight: bold;");
        Label hint = new Label("Generated query for the active SQLite evidence database");
        hint.setStyle("-fx-text-fill: #5b677a; -fx-font-size: 11px;");

        sqlPreview.setEditable(false);
        sqlPreview.setWrapText(true);
        sqlPreview.setStyle("-fx-control-inner-background: #f8fafc; -fx-text-fill: #1f2937; "
                + "-fx-font-family: 'Menlo', 'Monospaced'; -fx-font-size: 12px; "
                + "-fx-border-color: #cbd5e1; -fx-border-radius: 6; "
                + "-fx-highlight-fill: #2563eb; -fx-highlight-text-fill: white;");
        VBox.setVgrow(sqlPreview, Priority.ALWAYS);

        Label credit = new Label("NodeQL Implementation by Paul Bodach (CodeSpire-Solutions)");
        credit.setStyle("-fx-text-fill: #64748b; -fx-font-size: 10px;");

        box.getChildren().addAll(label, hint, sqlPreview, credit);
        return box;
    }

    private VBox createTriggerView() {
        VBox trigger = new VBox(4);
        trigger.setLayoutX(CHAIN_X);
        trigger.setLayoutY(CHAIN_START_Y - BLOCK_HEIGHT - CHAIN_GAP);
        trigger.setPadding(new Insets(10));
        trigger.setPrefSize(BLOCK_WIDTH, BLOCK_HEIGHT);
        trigger.setStyle("-fx-background-color: #166534; -fx-background-radius: 8;"
                + "-fx-border-color: #14532d; -fx-border-radius: 8;"
                + "-fx-effect: dropshadow(gaussian, rgba(15,23,42,0.18), 12, 0, 0, 4);");

        Label label = new Label("EXECUTE QUERY");
        label.setStyle("-fx-text-fill: white; -fx-font-size: 16px; -fx-font-weight: bold;");
        Label hint = new Label("Snap SQL blocks below");
        hint.setStyle("-fx-text-fill: rgba(255,255,255,0.82); -fx-font-size: 11px;");
        trigger.getChildren().addAll(label, hint);
        return trigger;
    }

    private void addBlock(BlockTemplate template) {
        rememberUndoState();
        BlockView view = new BlockView(template);
        view.setLayoutX(CHAIN_X + 420);
        view.setLayoutY(CHAIN_START_Y - 80 + (blocks.size() % 12) * 82);
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
            pendingDragSnapshot = captureWorkspaceState();
            dragging = view;
            dragOffsetX = event.getX();
            dragOffsetY = event.getY();
            view.toFront();
            event.consume();
        });
        view.setOnMouseDragged(event -> {
            if (dragging == null) {
                return;
            }
            Point2D local = workspace.sceneToLocal(event.getSceneX(), event.getSceneY());
            view.setLayoutX(local.getX() - dragOffsetX);
            view.setLayoutY(local.getY() - dragOffsetY);
            event.consume();
        });
        view.setOnMouseReleased(event -> {
            snapOrFloat(view);
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
            line.setStroke(Color.rgb(148, 163, 184, x % (GRID_SIZE * 4) == 0 ? 0.36 : 0.18));
            line.setStrokeWidth(x % (GRID_SIZE * 4) == 0 ? 1.2 : 0.7);
            line.setMouseTransparent(true);
            lines.add(line);
        }
        for (double y = 0; y <= CANVAS_HEIGHT; y += GRID_SIZE) {
            Line line = new Line(0, y, CANVAS_WIDTH, y);
            line.setStroke(Color.rgb(148, 163, 184, y % (GRID_SIZE * 4) == 0 ? 0.36 : 0.18));
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
        int insertionIndex = snapIndexFor(released);
        if (insertionIndex < 0) {
            released.snapped = false;
            rebuildModelChain();
            return;
        }

        blocks.remove(released);
        blocks.add(Math.min(insertionIndex, blocks.size()), released);
        released.snapped = true;
        layoutSnappedBlocks();
        rebuildModelChain();
    }

    private int snapIndexFor(BlockView released) {
        double centerX = released.getLayoutX() + BLOCK_WIDTH / 2;
        if (Math.abs(centerX - (CHAIN_X + BLOCK_WIDTH / 2)) > BLOCK_WIDTH * 0.65) {
            return -1;
        }

        double y = released.getLayoutY();
        if (Math.abs(y - CHAIN_START_Y) <= SNAP_DISTANCE || y < CHAIN_START_Y) {
            return 0;
        }

        List<BlockView> snapped = snappedBlocksExcept(released);
        double projectedIndex = (y - CHAIN_START_Y) / (BLOCK_HEIGHT + CHAIN_GAP);
        int rounded = (int) Math.round(projectedIndex);
        if (rounded >= 0 && rounded <= snapped.size() && Math.abs(projectedIndex - rounded) < 0.55) {
            return rounded;
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
        int index = 0;
        for (BlockView block : blocks) {
            if (!block.snapped) {
                continue;
            }
            block.setLayoutX(CHAIN_X);
            block.setLayoutY(CHAIN_START_Y + index * (BLOCK_HEIGHT + CHAIN_GAP));
            index++;
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
        List<String> joins = new ArrayList<>();
        List<String> wheres = new ArrayList<>();
        List<String> groups = new ArrayList<>();
        List<String> havings = new ArrayList<>();
        List<String> orders = new ArrayList<>();
        List<String> setOps = new ArrayList<>();

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
                case SQL_WHERE -> wheres.add(input(block, "predicate", "1 = 1"));
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
                case SQL_UNION -> setOps.add("UNION " + input(block, "sql", "SELECT 1"));
                case SQL_INTERSECT -> setOps.add("INTERSECT " + input(block, "sql", "SELECT 1"));
                case SQL_EXCEPT -> setOps.add("EXCEPT " + input(block, "sql", "SELECT 1"));
                default -> {
                    // Blocks that do not fit into a SELECT chain are ignored here.
                }
            }
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(String.join(", ", compactProjectionList(projections)));
        sql.append("\nFROM ").append(table);
        for (String join : joins) {
            sql.append("\n").append(join);
        }
        if (!wheres.isEmpty()) {
            sql.append("\nWHERE ").append(String.join(" AND ", wheres));
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
        blocks.remove(target);
        workspace.getChildren().remove(target);
        layoutSnappedBlocks();
        refreshSql();
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
                        block.fieldValues()))
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

    private String paletteButtonStyle(String color) {
        return "-fx-background-color: " + color + "; -fx-text-fill: white;"
                + "-fx-font-weight: bold; -fx-background-radius: 8;"
                + "-fx-border-color: rgba(255,255,255,0.28); -fx-border-radius: 8;"
                + "-fx-padding: 8 10 8 10;";
    }

    private final class BlockView extends VBox {
        private final BlockTemplate template;
        private final BlockNode node;
        private final Map<String, TextField> fields = new LinkedHashMap<>();
        private final String undoId;
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

            setPadding(new Insets(8));
            setSpacing(4);
            setPrefSize(BLOCK_WIDTH, BLOCK_HEIGHT);
            setMinSize(BLOCK_WIDTH, BLOCK_HEIGHT);
            setMaxSize(BLOCK_WIDTH, BLOCK_HEIGHT);
            updateStyle(false);

            HBox titleRow = new HBox(6);
            Label title = new Label(template.blockLabel);
            title.setTextFill(Color.WHITE);
            title.setStyle("-fx-font-size: 14px; -fx-font-weight: bold;");
            Button delete = new Button("x");
            delete.setStyle("-fx-background-color: rgba(0,0,0,0.20); -fx-text-fill: white;"
                    + "-fx-background-radius: 12; -fx-padding: 1 7 2 7;");
            delete.setOnAction(e -> {
                select(this);
                deleteSelected();
            });
            HBox.setHgrow(title, Priority.ALWAYS);
            titleRow.getChildren().addAll(title, delete);
            getChildren().add(titleRow);

            HBox inputRow = new HBox(5);
            for (Map.Entry<String, String> entry : template.fieldLabels.entrySet()) {
                String fieldValue = initialValues.getOrDefault(
                        entry.getKey(),
                        String.valueOf(template.defaults.get(entry.getKey()))
                );
                TextField field = new TextField(fieldValue);
                field.setPromptText(entry.getValue());
                field.setPrefColumnCount(Math.max(5, Math.min(12, field.getText().length())));
                field.setStyle("-fx-background-color: rgba(255,255,255,0.92);"
                        + "-fx-background-radius: 7; -fx-border-color: rgba(0,0,0,0.18);"
                        + "-fx-border-radius: 7; -fx-padding: 4 7 4 7;"
                        + "-fx-text-fill: #111827; -fx-prompt-text-fill: #64748b;");
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
                                ? captureWorkspaceStateWith(this, entry.getKey(), old)
                                : pendingTextEditSnapshot);
                    }
                    refreshSql();
                });
                fields.put(entry.getKey(), field);
                inputRow.getChildren().add(field);
            }
            getChildren().add(inputRow);

            Rectangle notch = new Rectangle(46, 4, Color.rgb(255, 255, 255, 0.35));
            notch.setArcHeight(4);
            notch.setArcWidth(4);
            getChildren().add(notch);
        }

        private void updateStyle(boolean selected) {
            String border = selected ? "#0f172a" : "rgba(15,23,42,0.22)";
            String width = selected ? "3" : "1";
            setStyle("-fx-background-color: " + template.color
                    + "; -fx-background-radius: 8;"
                    + "-fx-border-color: " + border
                    + "; -fx-border-width: " + width
                    + "; -fx-border-radius: 8;"
                    + "-fx-effect: dropshadow(gaussian, rgba(15,23,42,0.16), 10, 0, 0, 3);");
        }

        private void applyInputs() {
            for (Map.Entry<String, TextField> entry : fields.entrySet()) {
                node.inputs().put(entry.getKey(), entry.getValue().getText());
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
                            values);
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
            Map<String, String> inputs) {
        private BlockState {
            inputs = Map.copyOf(inputs);
        }
    }

    private enum Category {
        DQL,
        JOIN_SET,
        AGGREGATE,
        FUNCTION,
        DML,
        DDL,
        DCL,
        TCL
    }

    private enum BlockTemplate {
        SELECT("SELECT", "SELECT", Category.DQL, BlockType.SQL_SELECT, "select", "#276ef1",
                Map.of("columns", "columns", "table", "table"),
                Map.of("columns", "*", "table", "table_name")),
        COLUMN("COLUMN", "COLUMN", Category.DQL, BlockType.SQL_COLUMN, "column", "#3b82f6",
                Map.of("column", "column"),
                Map.of("column", "*")),
        FROM("FROM", "FROM", Category.DQL, BlockType.SQL_FROM, "from", "#2563eb",
                Map.of("table", "table"),
                Map.of("table", "table_name")),
        WHERE("WHERE", "WHERE", Category.DQL, BlockType.SQL_WHERE, "where", "#f59e0b",
                Map.of("predicate", "condition"),
                Map.of("predicate", "1 = 1")),
        GROUP_BY("GROUP BY", "GROUP BY", Category.DQL, BlockType.SQL_GROUP_BY, "group", "#0891b2",
                Map.of("expr", "column"),
                Map.of("expr", "id")),
        HAVING("HAVING", "HAVING", Category.DQL, BlockType.SQL_HAVING, "having", "#0e7490",
                Map.of("predicate", "condition"),
                Map.of("predicate", "COUNT(*) > 0")),
        ORDER_BY("ORDER BY", "ORDER BY", Category.DQL, BlockType.SQL_ORDER_BY, "order", "#0d9488",
                Map.of("expr", "column direction"),
                Map.of("expr", "id DESC")),

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
        CROSS_JOIN("CROSS JOIN", "CROSS JOIN", Category.JOIN_SET, BlockType.SQL_CROSS_JOIN, "cross_join", "#6d28d9",
                Map.of("table", "table"),
                Map.of("table", "table_name")),
        NATURAL_JOIN("NATURAL JOIN", "NATURAL JOIN", Category.JOIN_SET, BlockType.SQL_NATURAL_JOIN, "natural_join", "#6d28d9",
                Map.of("table", "table"),
                Map.of("table", "table_name")),
        SELF_JOIN("SELF JOIN", "SELF JOIN", Category.JOIN_SET, BlockType.SQL_SELF_JOIN, "self_join", "#6d28d9",
                Map.of("table", "table", "on", "condition"),
                Map.of("table", "table_name", "on", "t1.id = t2.id")),
        UNION("UNION", "UNION", Category.JOIN_SET, BlockType.SQL_UNION, "union", "#9333ea",
                Map.of("sql", "sql"),
                Map.of("sql", "SELECT 1")),
        INTERSECT("INTERSECT", "INTERSECT", Category.JOIN_SET, BlockType.SQL_INTERSECT, "intersect", "#9333ea",
                Map.of("sql", "sql"),
                Map.of("sql", "SELECT 1")),
        EXCEPT("EXCEPT", "EXCEPT", Category.JOIN_SET, BlockType.SQL_EXCEPT, "except", "#9333ea",
                Map.of("sql", "sql"),
                Map.of("sql", "SELECT 1")),
        SUBQUERY_IN("IN SUBQUERY", "IN", Category.JOIN_SET, BlockType.SQL_SUBQUERY_IN, "in", "#9333ea",
                Map.of("lhs", "lhs", "sql", "sql"),
                Map.of("lhs", "id", "sql", "SELECT id FROM t")),
        SUBQUERY_ANY("ANY SUBQUERY", "ANY", Category.JOIN_SET, BlockType.SQL_SUBQUERY_ANY, "any", "#9333ea",
                Map.of("lhs", "lhs", "sql", "sql"),
                Map.of("lhs", "id", "sql", "SELECT id FROM t")),
        SUBQUERY_ALL("ALL SUBQUERY", "ALL", Category.JOIN_SET, BlockType.SQL_SUBQUERY_ALL, "all", "#9333ea",
                Map.of("lhs", "lhs", "sql", "sql"),
                Map.of("lhs", "id", "sql", "SELECT id FROM t")),

        COUNT("COUNT", "COUNT", Category.AGGREGATE, BlockType.SQL_COUNT, "count", "#db2777",
                Map.of("expr", "expr"),
                Map.of("expr", "*")),
        SUM("SUM", "SUM", Category.AGGREGATE, BlockType.SQL_SUM, "sum", "#db2777",
                Map.of("expr", "expr"),
                Map.of("expr", "amount")),
        AVG("AVG", "AVG", Category.AGGREGATE, BlockType.SQL_AVG, "avg", "#db2777",
                Map.of("expr", "expr"),
                Map.of("expr", "amount")),
        MIN("MIN", "MIN", Category.AGGREGATE, BlockType.SQL_MIN, "min", "#db2777",
                Map.of("expr", "expr"),
                Map.of("expr", "amount")),
        MAX("MAX", "MAX", Category.AGGREGATE, BlockType.SQL_MAX, "max", "#db2777",
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
