package fqlite.viewer.ui;

import fqlite.base.GUI;
import fqlite.base.ThemeManager;
import fqlite.viewer.parser.BinaryNode;
import fqlite.viewer.model.BPListNode;
import fqlite.viewer.parser.JsonNode;
import fqlite.viewer.model.ProtoField;
import fqlite.viewer.parser.XmlNode;
import fqlite.viewer.parser.*;
import fqlite.viewer.util.HexDump;
import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.image.ImageView;
import javafx.scene.input.TransferMode;
import javafx.scene.layout.*;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.stage.FileChooser;
import javafx.stage.Screen;
import javafx.stage.Stage;

import fqlite.viewer.model.SegbFile;
import fqlite.viewer.model.SegbRecord;
import fqlite.viewer.parser.SegbParseException;
import fqlite.viewer.parser.SegbParser;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Main application window – unified viewer for Protobuf, Apple BPList, XML and JSON.
 * Transparently decodes Base64-wrapped payloads before parsing.
 * Supports dark (default) and light theme switching at runtime.
 */
public class BinViewer{


    static Stage primaryStage;

    // ══════════════════════════════════════════════════════════════════════
    // Theme definitions
    // ══════════════════════════════════════════════════════════════════════

    private enum Theme { DARK, LIGHT }

    private record ThemeColors(
            String bgRoot, String bgPane, String bgControl, String bgToolbar,
            String bgSelected, String bgHover, String divider, String border,
            String textMain, String textMuted, String textTab, String textTabSel,
            String textArea, String textDropHint,
            String toggleBg, String toggleText, String toggleLabel
    ) {}

    private static final ThemeColors DARK = new ThemeColors(
            "#1e2330", "#181d2a", "#181d2a", "#1a1f2e",
            "#3a4a7a", "#2d3a5e", "#2d3550", "#2d3550",
            "#c8d0e8", "#7080a8", "#8898c0", "#ffffff",
            "#a8c8a0", "#3a4a7a",
            "#2d3a5e", "#c8d0e8", "☀  Light"
    );

    private static final ThemeColors LIGHT = new ThemeColors(
            "#f0f2f8", "#ffffff", "#ffffff", "#e4e8f4",
            "#c0d0f0", "#dce4f8", "#b8c4e0", "#c8d0e0",
            "#1a2040", "#6070a0", "#4050a0", "#0a1030",
            "#1a3a1a", "#a0b0d0",
            "#d0d8f0", "#1a2040", "🌙  Dark"
    );

    private static final String[] PROTO_COLORS_DARK  = { "#7ec8e3","#f0c060","#e0a0d0","#a0e8a0","#f0a060","#c0c0c0" };
    private static final String[] PROTO_COLORS_LIGHT = { "#1a6080","#806000","#802060","#206020","#804020","#505050" };

    private record NodeColorSet(String string, String intReal, String bool_, String date,
                                String data, String uid, String container, String dict) {}

    private static final NodeColorSet BPLIST_DARK  = new NodeColorSet(
            "#e0a0d0","#7ec8e3","#f0c060","#f0c060","#c0b0d0","#f08070","#a0e8a0","#a0c8ff");
    private static final NodeColorSet BPLIST_LIGHT = new NodeColorSet(
            "#801060","#104080","#705000","#705000","#504070","#802010","#205020","#103060");

    // XML colours (dark / light)
    private static final String[] XML_COLORS_DARK  = { "#a0c8ff","#e0a0d0","#f0c060","#7a9070","#c0b0d0" };
    private static final String[] XML_COLORS_LIGHT = { "#103060","#801060","#705000","#2a4020","#504070" };

    // JSON colours (dark / light)
    private static final String[] JSON_COLORS_DARK  = { "#a0c8ff","#a0e8a0","#e0a0d0","#7ec8e3","#f0c060","#c0c0c0" };
    private static final String[] JSON_COLORS_LIGHT = { "#103060","#205020","#801060","#104080","#705000","#505050" };

    // BinaryNode (BSON / MsgPack / Thrift / FlatBuffers) colours – reuse colorHex() from model
    // The model already embeds colour logic; we just need the theme-aware override for dark→light.
    private static final String BINARY_NODE_LIGHT_ELEMENT = "#103060";
    private static final String BINARY_NODE_LIGHT_ARRAY   = "#205020";
    private static final String BINARY_NODE_LIGHT_STRING  = "#801060";
    private static final String BINARY_NODE_LIGHT_NUMBER  = "#104080";
    private static final String BINARY_NODE_LIGHT_BINARY  = "#504070";
    private static final String BINARY_NODE_LIGHT_UNKNOWN = "#a00000";

    // SEGB colours (dark / light)
    private static final String SEGB_COLOR_HEADER_DARK  = "#a0c8ff";   // blue  – version / entry-count
    private static final String SEGB_COLOR_TS_DARK      = "#f0c060";   // gold  – timestamp
    private static final String SEGB_COLOR_PAYLOAD_DARK = "#a0e8a0";   // green – payload bytes
    private static final String SEGB_COLOR_HEADER_LIGHT = "#103060";
    private static final String SEGB_COLOR_TS_LIGHT     = "#705000";
    private static final String SEGB_COLOR_PAYLOAD_LIGHT = "#205020";

    // Colour for the Base64-decoded wrapper node (both themes)
    private static final String B64_WRAPPER_COLOR_DARK  = "#ffd080";  // amber
    private static final String B64_WRAPPER_COLOR_LIGHT = "#8a5a00";  // dark amber

    /**
     * Sentinel object that represents a "▶ [Base64 decoded: FORMAT]" virtual
     * parent node inserted into the tree whenever a string/data/text value
     * contains a valid Base64-encoded payload that can itself be parsed.
     *
     * The node carries the already-decoded bytes and the detected inner format
     * so that the tree-builder can attach the real child nodes below it.
     */
    record Base64WrapperNode(String label, FormatDetector.Format innerFormat, byte[] decoded) {
        @Override public String toString() { return label; }
    }

    // ══════════════════════════════════════════════════════════════════════
    // State
    // ══════════════════════════════════════════════════════════════════════

    private Theme currentTheme = Theme.DARK;

    /** Raw bytes as read from disk. */
    private byte[]                  fileBytes;
    /** Bytes actually parsed (may differ from fileBytes when Base64-decoded). */
    private byte[]                  parsedBytes;
    /** True when fileBytes were Base64-decoded before parsing. */
    private boolean                 wasBase64;

    private FormatDetector.Format   currentFormat = FormatDetector.Format.UNKNOWN;
    private List<ProtoField>        protoFields;
    private final ProtobufParser    protoParser  = new ProtobufParser();
    private BPListNode              bplistRoot;
    private final BPListParser      bplistParser = new BPListParser();
    private XmlNode                 xmlRoot;
    private final XmlParser         xmlParser    = new XmlParser();
    private JsonNode                jsonRoot;
    private final JsonParser        jsonParser   = new JsonParser();
    private BinaryNode              binaryRoot;
    private final BsonParser        bsonParser        = new BsonParser();
    private final MsgPackParser     msgPackParser     = new MsgPackParser();
    private final ThriftParser      thriftParser      = new ThriftParser();
    private final FlatBuffersParser flatBuffersParser = new FlatBuffersParser();
    private final JavaSerialParser  javaSerialParser  = new JavaSerialParser();

    private SegbFile                segbFile;
    private final SegbParser        segbParser        = new SegbParser();
    private static final DateTimeFormatter SEGB_TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd  HH:mm:ss.SSS z").withZone(ZoneId.systemDefault());

    // ── UI ────────────────────────────────────────────────────────────────
    private final Stage     stage;
    private       Scene     scene;
    private TreeView<Object>treeView;
    private Label           dropHint;
    private Label           formatBadge;
    private Button          themeBtn;
    private TextArea        infoArea;
    private TextArea        fieldHexArea;
    private ListView<String>altList;
    private TextArea        hexDumpArea;
    private Label           statusLabel;
    private File            activeStyleFile;

    // ══════════════════════════════════════════════════════════════════════
    // Construction
    // ══════════════════════════════════════════════════════════════════════

    public BinViewer(Stage stage) {
        this.stage = stage;
        stage.setTitle("BinViewer");
        stage.setMinWidth(960);
        stage.setMinHeight(620);
        // Sync initial theme state with the global ThemeManager
        currentTheme = ThemeManager.isDark() ? Theme.DARK : Theme.LIGHT;
        scene = new Scene(buildRoot(), Screen.getPrimary().getVisualBounds().getWidth() * 0.7, Screen.getPrimary().getVisualBounds().getHeight() * 0.7);
        applyTheme();
        ThemeManager.register(scene);   // ← participate in global theme switching
        stage.setScene(scene);
        stage.show();
    }

    // ══════════════════════════════════════════════════════════════════════
    // Layout
    // ══════════════════════════════════════════════════════════════════════

    private BorderPane buildRoot() {
        BorderPane root = new BorderPane();
        root.setTop(buildTopBar());
        root.setCenter(buildCenter());
        root.setBottom(buildStatusBar());
        setupDragAndDrop(root);
        return root;
    }

    private Node buildTopBar() {
        MenuBar menuBar = new MenuBar();

        Menu fileMenu  = new Menu("_File");
        MenuItem openItem  = new MenuItem("Open…");
        MenuItem closeItem = new MenuItem("Close");
        MenuItem exitItem  = new MenuItem("Exit");
        openItem.setOnAction(e  -> openFile());
        closeItem.setOnAction(e -> closeFile());
        exitItem.setOnAction(e  -> closeWindow());
        fileMenu.getItems().addAll(openItem, closeItem, new SeparatorMenuItem(), exitItem);

        Menu viewMenu  = new Menu("_View");
        MenuItem expandAll   = new MenuItem("Expand All");
        MenuItem collapseAll = new MenuItem("Collapse All");
        MenuItem toggleTheme = new MenuItem("Toggle Light / Dark Theme");
        expandAll.setOnAction(e   -> expandAll(treeView.getRoot(), true));
        collapseAll.setOnAction(e -> expandAll(treeView.getRoot(), false));
        toggleTheme.setOnAction(e -> toggleTheme());
        viewMenu.getItems().addAll(expandAll, collapseAll, new SeparatorMenuItem(), toggleTheme);

        menuBar.getMenus().addAll(fileMenu, viewMenu);

        String s = Objects.requireNonNull(GUI.class.getResource("/icon24_open.png")).toExternalForm();
        Button openBtn  = toolButton("",  this::openFile);
        ImageView iv = new ImageView(s);
        iv.smoothProperty().setValue(true);
        iv.setCache(true);
        iv.preserveRatioProperty().setValue(true);
        iv.setFitHeight(24);
        openBtn.setGraphic(iv);

        s = Objects.requireNonNull(GUI.class.getResource("/icon24_back.png")).toExternalForm();
        Button closeBtn = toolButton("", this::closeWindow);
        iv = new ImageView(s);
        iv.smoothProperty().setValue(true);
        iv.setCache(true);
        iv.preserveRatioProperty().setValue(true);
        iv.setFitHeight(24);
        closeBtn.setGraphic(iv);


        themeBtn = new Button(DARK.toggleLabel());
        themeBtn.setOnAction(e -> toggleTheme());
        themeBtn.setStyle(themeBtnStyle(DARK));

        formatBadge = new Label("–");
        formatBadge.getStyleClass().add("format-badge");
        formatBadge.setPadding(new Insets(3, 10, 3, 10));

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        ToolBar toolBar = new ToolBar(
                openBtn, new Separator(), closeBtn,
                spacer, themeBtn, new Separator(), formatBadge);

        return new VBox(menuBar, toolBar);
    }

    private SplitPane buildCenter() {
        StackPane treePane = buildTreePane();
        TabPane   details  = buildDetailPanel();
        SplitPane hSplit   = new SplitPane(treePane, details);
        hSplit.setDividerPositions(0.42);

        VBox      hexPane  = buildFileHexPane();
        SplitPane vSplit   = new SplitPane(hSplit, hexPane);
        vSplit.setOrientation(Orientation.VERTICAL);
        vSplit.setDividerPositions(0.65);
        return vSplit;
    }

    private StackPane buildTreePane() {
        treeView = new TreeView<>();
        treeView.setShowRoot(false);

        treeView.setCellFactory(tv -> new TreeCell<>() {
            @Override
            protected void updateItem(Object item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setText(null); setStyle(null); return; }
                if      (item instanceof ProtoField f)        { setText(f.treeLabel()); setStyle("-fx-text-fill:" + protoColor(f)    + ";"); }
                else if (item instanceof BPListNode n)        { setText(n.treeLabel()); setStyle("-fx-text-fill:" + bplistColor(n)   + ";"); }
                else if (item instanceof XmlNode    x)        { setText(x.treeLabel()); setStyle("-fx-text-fill:" + xmlColor(x)      + ";"); }
                else if (item instanceof JsonNode   j)        { setText(j.treeLabel()); setStyle("-fx-text-fill:" + jsonColor(j)     + ";"); }
                else if (item instanceof BinaryNode b)        { setText(b.treeLabel()); setStyle("-fx-text-fill:" + binaryColor(b)   + ";"); }
                else if (item instanceof SegbRecord r)        { setText(segbLabel(r));  setStyle("-fx-text-fill:" + segbColor(r)     + ";"); }
                else if (item instanceof Base64WrapperNode w) { setText(w.label());     setStyle("-fx-text-fill:" + b64WrapperColor() + "; -fx-font-weight: bold;"); }
            }
        });

        treeView.getSelectionModel().selectedItemProperty()
                .addListener((obs, old, sel) -> onNodeSelected(sel));

        dropHint = new Label(
                "Drag & Drop\n.pb  |  .plist  |  .bplist  |  .xml  |  .json  |  .segb\nfiles here\n\nor  📂 Open\n\n(Base64-encoded content is auto-detected)");
        dropHint.getStyleClass().add("drop-hint");
        dropHint.setAlignment(Pos.CENTER);

        StackPane pane = new StackPane(treeView, dropHint);
        dropHint.setVisible(true);
        treeView.setVisible(false);
        return pane;
    }

    private TabPane buildDetailPanel() {
        infoArea     = monospace();
        fieldHexArea = monospace();
        altList      = new ListView<>();
        altList.getStyleClass().add("alt-list");
        return new TabPane(
                closableTab("Info",            infoArea),
                closableTab("Hex (Field)",     fieldHexArea),
                closableTab("Interpretations", altList));
    }

    private VBox buildFileHexPane() {
        Label title = new Label("Full File Hex Dump");
        title.setFont(Font.font("System", FontWeight.BOLD, 12));
        title.getStyleClass().add("hex-dump-title");
        hexDumpArea = monospace();
        VBox pane = new VBox(4, title, hexDumpArea);
        pane.setPadding(new Insets(4));
        VBox.setVgrow(hexDumpArea, Priority.ALWAYS);
        return pane;
    }

    private Node buildStatusBar() {
        statusLabel = new Label("Ready – drag & drop a file or use Open");
        statusLabel.setPadding(new Insets(3, 8, 3, 8));
        HBox bar = new HBox(statusLabel);
        bar.getStyleClass().add("status-bar");
        return bar;
    }

    // ══════════════════════════════════════════════════════════════════════
    // Theme
    // ══════════════════════════════════════════════════════════════════════

    private void toggleTheme() {
        currentTheme = (currentTheme == Theme.DARK) ? Theme.LIGHT : Theme.DARK;
        applyTheme();
        treeView.refresh();
        updateFormatBadge();
        // Propagate the new theme to all other open windows
        ThemeManager.setDark(currentTheme == Theme.DARK);
    }

    private ThemeColors colors() { return currentTheme == Theme.DARK ? DARK : LIGHT; }

    private void applyTheme() {
        ThemeColors c = colors();
        scene.getRoot().setStyle(
                "-fx-font-family: 'Segoe UI', 'Helvetica Neue', sans-serif;" +
                "-fx-font-size: 13px;" +
                "-fx-base: " + c.bgPane() + ";" +
                "-fx-background: " + c.bgRoot() + ";" +
                "-fx-control-inner-background: " + c.bgControl() + ";" +
                "-fx-text-base-color: " + c.textMain() + ";" +
                "-fx-text-background-color: " + c.textMain() + ";"
        );
        String css = buildCss(c);
        try {
            if (activeStyleFile != null)
                scene.getStylesheets().remove(activeStyleFile.toURI().toString());
            File tmp = File.createTempFile("pfv_style_", ".css");
            tmp.deleteOnExit();
            Files.writeString(tmp.toPath(), css);
            activeStyleFile = tmp;
            scene.getStylesheets().add(tmp.toURI().toString());
        } catch (IOException ignored) {}
        if (themeBtn != null) {
            themeBtn.setText(c.toggleLabel());
            themeBtn.setStyle(themeBtnStyle(c));
        }
    }

    private String buildCss(ThemeColors c) {
        return """
            .root { -fx-background-color: %s; }
            .split-pane { -fx-background-color: %s; -fx-padding: 0; }
            .split-pane-divider { -fx-background-color: %s; -fx-pref-width: 3px; -fx-pref-height: 3px; }
            .tree-view { -fx-background-color: %s; -fx-border-color: %s; -fx-border-width: 0 1 0 0; }
            .tree-cell { -fx-background-color: transparent; -fx-text-fill: %s; -fx-padding: 3 6; }
            .tree-cell:selected { -fx-background-color: %s; -fx-text-fill: %s; }
            .tree-cell:hover    { -fx-background-color: %s; }
            .text-area { -fx-control-inner-background: %s; -fx-text-fill: %s; -fx-border-color: %s; }
            .text-area .content { -fx-background-color: %s; }
            .tab-pane .tab-header-area { -fx-background-color: %s; }
            .tab-pane .tab-header-background { -fx-background-color: %s; }
            .tab { -fx-background-color: %s; -fx-text-fill: %s; }
            .tab:selected { -fx-background-color: %s; -fx-text-fill: %s; }
            .tool-bar { -fx-background-color: %s; -fx-border-color: %s; -fx-border-width: 0 0 1 0; }
            .menu-bar { -fx-background-color: %s; }
            .menu-bar .menu .label { -fx-text-fill: %s; }
            .menu-item .label { -fx-text-fill: %s; }
            .context-menu { -fx-background-color: %s; }
            .status-bar { -fx-background-color: %s; -fx-border-color: %s; -fx-border-width: 1 0 0 0; }
            .status-bar .label { -fx-text-fill: %s; -fx-font-size: 11px; }
            .drop-hint { -fx-text-fill: %s; -fx-font-size: 15px; -fx-alignment: center; -fx-text-alignment: center; }
            .hex-dump-title { -fx-text-fill: %s; }
            .list-view { -fx-background-color: %s; -fx-border-color: %s; }
            .list-cell { -fx-background-color: transparent; -fx-text-fill: %s; -fx-padding: 3 6; -fx-font-family: monospace; -fx-font-size: 12px; }
            .list-cell:selected { -fx-background-color: %s; -fx-text-fill: %s; }
            .scroll-bar { -fx-background-color: %s; }
            .scroll-bar .thumb { -fx-background-color: %s; -fx-background-radius: 3; }
            .scroll-bar .track { -fx-background-color: %s; }
            """.formatted(
                c.bgRoot(), c.bgPane(), c.divider(),
                c.bgRoot(), c.border(), c.textMain(),
                c.bgSelected(), c.textTabSel(), c.bgHover(),
                c.bgControl(), c.textArea(), c.border(), c.bgControl(),
                c.bgRoot(), c.bgToolbar(),
                c.bgPane(), c.textTab(), c.bgSelected(), c.textTabSel(),
                c.bgToolbar(), c.border(),
                c.bgToolbar(), c.textMain(), c.textMain(), c.bgToolbar(),
                c.bgToolbar(), c.border(), c.textMuted(),
                c.textDropHint(), c.textMuted(),
                c.bgControl(), c.border(), c.textMain(),
                c.bgSelected(), c.textTabSel(),
                c.bgPane(), c.divider(), c.bgControl()
        );
    }

    private String themeBtnStyle(ThemeColors c) {
        return "-fx-background-color: " + c.toggleBg() + ";"
               + "-fx-text-fill: " + c.toggleText() + ";"
               + "-fx-background-radius: 4;"
               + "-fx-font-size: 12px;"
               + "-fx-padding: 4 10 4 10;";
    }

    // ══════════════════════════════════════════════════════════════════════
    // Colour helpers (theme-aware)
    // ══════════════════════════════════════════════════════════════════════

    private String protoColor(ProtoField f) {
        String[] p = (currentTheme == Theme.DARK) ? PROTO_COLORS_DARK : PROTO_COLORS_LIGHT;
        return switch (f.getWireType()) {
            case ProtoField.WIRE_VARINT -> p[0];
            case ProtoField.WIRE_64BIT  -> p[1];
            case ProtoField.WIRE_LEN    -> f.isNestedMessage() ? p[3] : p[2];
            case ProtoField.WIRE_32BIT  -> p[4];
            default                     -> p[5];
        };
    }

    private String bplistColor(BPListNode n) {
        NodeColorSet p = (currentTheme == Theme.DARK) ? BPLIST_DARK : BPLIST_LIGHT;
        return switch (n.getType()) {
            case STRING         -> p.string();
            case INT, REAL      -> p.intReal();
            case BOOL           -> p.bool_();
            case DATE           -> p.date();
            case DATA           -> p.data();
            case UID            -> p.uid();
            case ARRAY, SET     -> p.container();
            case DICT           -> p.dict();
            default             -> colors().textMain();
        };
    }

    private String xmlColor(XmlNode x) {
        String[] p = (currentTheme == Theme.DARK) ? XML_COLORS_DARK : XML_COLORS_LIGHT;
        return switch (x.getType()) {
            case ELEMENT   -> p[0];
            case TEXT      -> p[1];
            case ATTRIBUTE -> p[2];
            case COMMENT   -> p[3];
            case CDATA     -> p[4];
        };
    }

    private String jsonColor(JsonNode j) {
        String[] p = (currentTheme == Theme.DARK) ? JSON_COLORS_DARK : JSON_COLORS_LIGHT;
        return switch (j.getType()) {
            case OBJECT  -> p[0];
            case ARRAY   -> p[1];
            case STRING  -> p[2];
            case NUMBER  -> p[3];
            case BOOLEAN -> p[4];
            case NULL    -> p[5];
        };
    }

    private String binaryColor(BinaryNode b) {
        if (currentTheme == Theme.DARK) return b.colorHex(); // model has dark colours built in
        // Remap to light-theme variants
        return switch (b.getKind()) {
            case DOCUMENT, MAP -> BINARY_NODE_LIGHT_ELEMENT;
            case ARRAY         -> BINARY_NODE_LIGHT_ARRAY;
            case BINARY        -> BINARY_NODE_LIGHT_BINARY;
            case UNKNOWN       -> BINARY_NODE_LIGHT_UNKNOWN;
            default -> {
                String tn = b.getTypeName();
                if (tn != null) {
                    if (tn.contains("String") || tn.contains("Str")) yield BINARY_NODE_LIGHT_STRING;
                    if (tn.contains("Int") || tn.contains("Long") || tn.contains("Float")
                        || tn.contains("Double") || tn.contains("Byte") || tn.contains("Short")
                        || tn.contains("uint") || tn.contains("int"))
                        yield BINARY_NODE_LIGHT_NUMBER;
                }
                yield colors().textMain();
            }
        };
    }

    // ── SEGB colour + label helpers ──────────────────────────────────────

    private String segbLabel(SegbRecord r) {
        // index == -2  → virtual header node
        if (r.getIndex() == -2) {
            if (segbFile == null) return "Header";
            String outer = "";
            if (segbFile.hasOuterHeader() && segbFile.getOuterHeader().streamName() != null
                && !segbFile.getOuterHeader().streamName().isEmpty())
                outer = "  │  Stream: " + segbFile.getOuterHeader().streamName();
            return String.format("SEGB %s  │  %d record(s)%s",
                    segbFile.getVersion(), segbFile.getEntryCount(), outer);
        }
        // index == -1  → virtual root (should not reach the cell)
        if (r.getIndex() == -1) return segbFile != null ? segbFile.getFilePath() : "SEGB";

        String ts = r.getTimestamp() != null ? SEGB_TS_FMT.format(r.getTimestamp()) : "\u2013";
        // Fixed-Size-Variant: Marker und ggf. Dauer anzeigen
        if (r.isFixedSize()) {
            String markerStr;
            if      (r.getMarker() == SegbRecord.MARKER_ACTIVE)   markerStr = "";
            else if (r.getMarker() == SegbRecord.MARKER_LAST)     markerStr = "  [last]";
            else if (segbFile != null && segbFile.getVersion() == SegbFile.Version.ACTIVITY)
                markerStr = String.format("  [Index-Event, Typ=%d]", r.getMarker());
            else markerStr = String.format("  [marker=0x%02X]", r.getMarker());
            String dur = r.hasDuration()
                    ? "  \u2192  " + SEGB_TS_FMT.format(r.getTimestampEnd()) : "";
            return String.format("#%d  |  %s%s%s  |  %d bytes",
                    r.getIndex(), ts, dur, markerStr, r.getPayload().length);
        }
        String[] activity = tryDecodeActivityEvent(r.getPayload());
        if (activity != null) {
            return String.format("#%d  |  %s  |  %s  |  %s",
                    r.getIndex(), ts, activity[1], activity[0]);
        }
        return String.format("#%d  |  %s  |  %d bytes",
                r.getIndex(), ts, r.getPayload().length);
    }

    /**
     * Versucht, ein Activity-Log-Event (siehe {@link fqlite.viewer.parser.SegbParser}) aus der
     * Record-Payload zu dekodieren. Die Payload beginnt mit einem 8-Byte-Vorspann
     * (Pr\u00fcfsumme/Hash + Reserviert), gefolgt von einer Protobuf-Nachricht mit
     * Feld 1 = UUID-String (36 Zeichen) und Feld 2 = Aktivit\u00e4tsname.
     *
     * @return {@code {uuid, name}} oder {@code null}, wenn die Payload nicht passt
     */
    private static String[] tryDecodeActivityEvent(byte[] payload) {
        final int hdr = 8;
        if (payload == null || payload.length < hdr + 2 + 36 + 2) return null;
        if ((payload[hdr] & 0xFF) != 0x0A || (payload[hdr + 1] & 0xFF) != 0x24) return null;
        for (int i = 0; i < 36; i++) {
            int c = payload[hdr + 2 + i] & 0xFF;
            boolean dash = (i == 8 || i == 13 || i == 18 || i == 23);
            boolean hex  = (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f');
            if (dash ? c != '-' : !hex) return null;
        }
        String uuid = new String(payload, hdr + 2, 36, StandardCharsets.US_ASCII);
        int p = hdr + 2 + 36;
        if ((payload[p] & 0xFF) != 0x12) return null;
        int nameLen = payload[p + 1] & 0xFF;
        if (p + 2 + nameLen > payload.length) return null;
        String name = new String(payload, p + 2, nameLen, StandardCharsets.US_ASCII);
        return new String[]{uuid, name};
    }

    private String segbColor(SegbRecord r) {
        boolean dark = currentTheme == Theme.DARK;
        if (r.getIndex() < 0)   return dark ? SEGB_COLOR_HEADER_DARK  : SEGB_COLOR_HEADER_LIGHT;
        if (r.getTimestamp() != null) return dark ? SEGB_COLOR_TS_DARK : SEGB_COLOR_TS_LIGHT;
        return dark ? SEGB_COLOR_PAYLOAD_DARK : SEGB_COLOR_PAYLOAD_LIGHT;
    }

    // ── SEGB detail panel ────────────────────────────────────────────────

    private void showSegbDetail(SegbRecord r) {
        // Virtual header node
        if (r.getIndex() == -2) {
            if (segbFile == null) return;
            StringBuilder hdr = new StringBuilder();
            hdr.append("SEGB File\n");
            hdr.append("─────────────────────────────────────────\n");
            hdr.append("Version       :  ").append(segbFile.getVersion()).append("\n");
            hdr.append("Entry count   :  ").append(segbFile.getEntryCount()).append("\n");
            hdr.append("Records read  :  ").append(segbFile.getRecords().size()).append("\n");
            hdr.append("File          :  ").append(segbFile.getFilePath()).append("\n");
            hdr.append("File size     :  ").append(fileBytes != null ? fileBytes.length : 0).append(" bytes\n");
            if (segbFile.hasOuterHeader()) {
                fqlite.viewer.parser.SegbParser.OuterHeader oh = segbFile.getOuterHeader();
                hdr.append("\n─── Biome Outer-Header ──────────────────\n");
                hdr.append("Stream-Typ    :  0x").append(Integer.toHexString(oh.streamType()).toUpperCase()).append("\n");
                hdr.append("Outer-Version :  ").append(oh.outerVersion()).append("\n");
                if (oh.streamName() != null && !oh.streamName().isEmpty())
                    hdr.append("Stream-Name   :  ").append(oh.streamName()).append("\n");
                if (oh.fileTimestamp() != null)
                    hdr.append("Datei-Ts      :  ").append(SEGB_TS_FMT.format(oh.fileTimestamp())).append("\n");
                hdr.append("SEGB-Offset   :  0x").append(Integer.toHexString(oh.segbOffset()).toUpperCase()).append("\n");
            }
            if (segbFile.getVersion() == SegbFile.Version.FIXED) {
                hdr.append("\nFormat: Fixed-Size-Ring-Buffer (72 Bytes/Record)\n");
                hdr.append("Payloads dieser Variante sind oft leer (reine Timestamp-Events).\n");
            } else if (segbFile.getVersion() == SegbFile.Version.ACTIVITY) {
                hdr.append("\nFormat: Activity-Log (UUID + Aktivitätsname, Apple Biome)\n");
                hdr.append("Records mit Zeitstempel + Text sind Activity-Events (z. B. \"Share\",\n");
                hdr.append("\"View N to M seconds\"); Records mit [Index-Event] sind unkorrelierte\n");
                hdr.append("Zeitstempel-Markierungen ohne eigene Protobuf-Payload.\n");
            } else {
                hdr.append("\nPayloads sind typischerweise Protobuf (Apple Biome).\n");
            }
            infoArea.setText(hdr.toString());
            fieldHexArea.setText(fileBytes != null ? HexDump.dump(fileBytes, 0) : "");
            altList.getItems().clear();
            return;
        }
        if (r.getIndex() == -1) return;   // root sentinel

        StringBuilder sb = new StringBuilder();
        sb.append("Record Index  :  ").append(r.getIndex()).append("\n");
        sb.append("Byte Offset   :  0x")
                .append(Long.toHexString(r.getOffset()).toUpperCase())
                .append("  (").append(r.getOffset()).append(")\n");
        sb.append("Payload Size  :  ").append(r.getPayload().length).append(" bytes\n");
        sb.append("Timestamp     :  ").append(
                r.getTimestamp() != null ? SEGB_TS_FMT.format(r.getTimestamp()) : "–").append("\n");
        if (r.getTimestamp() != null) {
            sb.append("  (epoch sec) :  ").append(r.getTimestamp().getEpochSecond()).append("\n");
            long apple = r.getTimestamp().getEpochSecond() - 978_307_200L;
            sb.append("  (apple ts)  :  ").append(apple).append("  (CFAbsoluteTime)\n");
        }
        if (r.getTimestampEnd() != null) {
            sb.append("Timestamp-End :  ").append(SEGB_TS_FMT.format(r.getTimestampEnd())).append("\n");
            if (r.hasDuration()) {
                long ms = r.getTimestampEnd().toEpochMilli() - r.getTimestamp().toEpochMilli();
                sb.append("  (Dauer)     :  ").append(ms).append(" ms\n");
            }
        }
        if (r.isFixedSize()) {
            String markerName = (r.getMarker() == SegbRecord.MARKER_ACTIVE)   ? "aktiv (0x21)"
                    : (r.getMarker() == SegbRecord.MARKER_LAST)     ? "letzter Record (0x2D)"
                    : (r.getMarker() == SegbRecord.MARKER_SENTINEL) ? "Sentinel (0x00)"
                    : String.format("0x%02X", r.getMarker());
            sb.append("Marker        :  ").append(markerName).append("\n");
        }
        String[] activity = tryDecodeActivityEvent(r.getPayload());
        if (activity != null) {
            sb.append("UUID          :  ").append(activity[0]).append("\n");
            sb.append("Aktivität     :  ").append(activity[1]).append("\n");
        }
        sb.append("\n─── Payload ─────────────────────────────\n");
        byte[] p = r.getPayload();
        if (p.length == 0) {
            sb.append("(leer – reines Timestamp-Event)\n");
        } else {
            int preview = Math.min(p.length, 64);
            for (int i = 0; i < preview; i++) sb.append(String.format("%02X ", p[i] & 0xFF));
            if (p.length > 64) sb.append("\n… (").append(p.length - 64).append(" more bytes)");
        }
        infoArea.setText(sb.toString());

        fieldHexArea.setText(r.getPayload().length > 0
                ? HexDump.dump(r.getPayload(), r.getOffset()) : "");
        altList.setItems(javafx.collections.FXCollections.observableArrayList(buildSegbAlternatives(r)));
    }

    private List<String> buildSegbAlternatives(SegbRecord r) {
        List<String> alts = new ArrayList<>();
        if (r.getTimestamp() != null) {
            long appleEpoch = r.getTimestamp().getEpochSecond() - 978_307_200L;
            alts.add("timestamp ISO   :  " + r.getTimestamp());
            alts.add("epoch seconds   :  " + r.getTimestamp().getEpochSecond());
            alts.add("epoch millis    :  " + r.getTimestamp().toEpochMilli());
            alts.add("apple time      :  " + appleEpoch + "  (CFAbsoluteTime)");
        }
        if (r.getTimestampEnd() != null && !r.getTimestampEnd().equals(r.getTimestamp())) {
            long appleEnd = r.getTimestampEnd().getEpochSecond() - 978_307_200L;
            alts.add("ts-end ISO      :  " + r.getTimestampEnd());
            alts.add("apple time end  :  " + appleEnd + "  (CFAbsoluteTime)");
            if (r.getTimestamp() != null) {
                long ms = r.getTimestampEnd().toEpochMilli() - r.getTimestamp().toEpochMilli();
                alts.add("duration        :  " + ms + " ms");
            }
        }
        if (r.isFixedSize()) {
            alts.add(String.format("marker          :  0x%02X  (%d)", r.getMarker(), r.getMarker()));
        }
        String[] activity = tryDecodeActivityEvent(r.getPayload());
        if (activity != null) {
            alts.add("uuid            :  " + activity[0]);
            alts.add("activity        :  " + activity[1]);
        }
        alts.add("offset          :  0x" + Long.toHexString(r.getOffset()).toUpperCase());
        alts.add("payload bytes   :  " + r.getPayload().length);
        if (r.getPayload().length > 0) {
            alts.add("payload hex     :  " + HexDump.toHexString(r.getPayload(), 32));
            alts.add("base64          :  " + java.util.Base64.getEncoder().encodeToString(r.getPayload()));
        }
        return alts;
    }

    private String b64WrapperColor() {        return currentTheme == Theme.DARK ? B64_WRAPPER_COLOR_DARK : B64_WRAPPER_COLOR_LIGHT;
    }

    // ══════════════════════════════════════════════════════════════════════
    // Base64 inline detection & child-tree builder
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Tries to interpret {@code value} as a Base64-encoded payload.
     * If successful, attaches a virtual child node under {@code parent}:
     *
     *   ▶ [Base64 decoded: FORMAT – N bytes]     ← Base64WrapperNode (amber)
     *     ├─ … parsed child nodes …              (for known formats)
     *     │   or
     *     └─ ⬡ hex: D5 3E A5 …                  (for raw binary / unknown)
     *
     * The method always attaches a node as long as Base64 decoding succeeds,
     * even when the decoded payload is raw binary (SHA-256 hash, key material,
     * encrypted data, …) that cannot be further parsed.
     *
     * @return true when a decoded sub-tree was attached
     */
    private boolean tryAttachBase64Children(TreeItem<Object> parent, String value) {
        if (value == null || value.length() < 8) return false;

        // Only attempt when the string looks like Base64
        byte[] raw = value.strip().getBytes(StandardCharsets.US_ASCII);
        Base64Detector.Result result = Base64Detector.detect(raw);
        if (!result.isBase64()) return false;

        byte[] decoded = result.decodedBytes();
        FormatDetector.Format fmt = FormatDetector.detect(decoded);

        // Resolve one more level of Base64 wrapping if needed
        byte[] parseable = decoded;
        FormatDetector.Format inner = FormatDetector.innerFormat(fmt);
        if (FormatDetector.isBase64Wrapped(fmt)) {
            Base64Detector.Result inner64 = Base64Detector.detect(decoded);
            if (inner64.isBase64()) parseable = inner64.decodedBytes();
        }

        // ── Build the label ──────────────────────────────────────────────
        String formatHint = inner == FormatDetector.Format.UNKNOWN
                ? guessRawBinaryHint(decoded)
                : FormatDetector.formatName(inner);
        String wrapLabel = "▶  [Base64 decoded: " + formatHint + "  –  " + decoded.length + " byte"
                           + (decoded.length == 1 ? "" : "s") + "]";

        Base64WrapperNode wrapper = new Base64WrapperNode(wrapLabel, inner, parseable);
        TreeItem<Object> wrapItem = new TreeItem<>(wrapper);
        wrapItem.setExpanded(true);

        // ── Attach child nodes ───────────────────────────────────────────
        if (inner != FormatDetector.Format.UNKNOWN) {
            // Known structured format → parse recursively
            try {
                switch (inner) {
                    case BPLIST -> {
                        BPListNode bpRoot = new BPListParser().parse(parseable);
                        TreeItem<Object> rootItem = new TreeItem<>(bpRoot);
                        addBPListChildren(rootItem, bpRoot);
                        rootItem.setExpanded(true);
                        wrapItem.getChildren().add(rootItem);
                    }
                    case XML -> {
                        XmlNode xRoot = new XmlParser().parse(parseable);
                        TreeItem<Object> rootItem = new TreeItem<>(xRoot);
                        addXmlChildren(rootItem, xRoot);
                        rootItem.setExpanded(true);
                        wrapItem.getChildren().add(rootItem);
                    }
                    case JSON -> {
                        String jsonText = new String(parseable, StandardCharsets.UTF_8);
                        JsonNode jRoot = new JsonParser().parse(jsonText);
                        TreeItem<Object> rootItem = new TreeItem<>(jRoot);
                        addJsonChildren(rootItem, jRoot);
                        rootItem.setExpanded(true);
                        wrapItem.getChildren().add(rootItem);
                    }
                    case PROTOBUF -> {
                        List<ProtoField> fields = new ProtobufParser().parse(parseable);
                        addProtoItems(wrapItem, fields);
                    }
                    case BSON -> {
                        BinaryNode bRoot = new BsonParser().parse(parseable);
                        TreeItem<Object> rootItem = new TreeItem<>(bRoot);
                        addBinaryChildren(rootItem, bRoot);
                        rootItem.setExpanded(true);
                        wrapItem.getChildren().add(rootItem);
                    }
                    case MSGPACK -> {
                        BinaryNode mRoot = new MsgPackParser().parse(parseable);
                        TreeItem<Object> rootItem = new TreeItem<>(mRoot);
                        addBinaryChildren(rootItem, mRoot);
                        rootItem.setExpanded(true);
                        wrapItem.getChildren().add(rootItem);
                    }
                    case THRIFT_BINARY -> {
                        BinaryNode tRoot = new ThriftParser().parseBinary(parseable);
                        TreeItem<Object> rootItem = new TreeItem<>(tRoot);
                        addBinaryChildren(rootItem, tRoot);
                        rootItem.setExpanded(true);
                        wrapItem.getChildren().add(rootItem);
                    }
                    case THRIFT_COMPACT -> {
                        BinaryNode tRoot = new ThriftParser().parseCompact(parseable);
                        TreeItem<Object> rootItem = new TreeItem<>(tRoot);
                        addBinaryChildren(rootItem, tRoot);
                        rootItem.setExpanded(true);
                        wrapItem.getChildren().add(rootItem);
                    }
                    case FLATBUFFERS -> {
                        BinaryNode fRoot = new FlatBuffersParser().parse(parseable);
                        TreeItem<Object> rootItem = new TreeItem<>(fRoot);
                        addBinaryChildren(rootItem, fRoot);
                        rootItem.setExpanded(true);
                        wrapItem.getChildren().add(rootItem);
                    }
                    case JAVA_SERIAL -> {
                        BinaryNode jRoot = new JavaSerialParser().parse(parseable);
                        TreeItem<Object> rootItem = new TreeItem<>(jRoot);
                        addBinaryChildren(rootItem, jRoot);
                        rootItem.setExpanded(true);
                        wrapItem.getChildren().add(rootItem);
                    }
                    case AVRO, TIFF, JPEG, BMP, PDF, PNG, GIF, GZIP, HEIC -> addRawBinaryChildren(wrapItem, parseable);

                    default -> addRawBinaryChildren(wrapItem, decoded);
                }
            } catch (Exception e) {
                // Parsing failed despite format detection → fall back to raw display
                addRawBinaryChildren(wrapItem, decoded);
            }
        } else {
            // Raw binary (hash, key material, encrypted data, …) → hex rows
            addRawBinaryChildren(wrapItem, decoded);
        }

        parent.getChildren().add(wrapItem);
        return true;
    }

    /**
     * Adds human-readable child nodes for a raw (unparseable) binary payload:
     * one node per 16-byte row of hex, plus summary nodes for common sizes.
     */
    private void addRawBinaryChildren(TreeItem<Object> parent, byte[] bytes) {
        // Summary / type-hint node
        parent.getChildren().add(syntheticLeaf("⬡  " + bytes.length + " bytes  –  " + guessRawBinaryHint(bytes)));

        // Hex rows (max 16 rows shown inline to keep the tree tidy)
        int rows = (int) Math.ceil(bytes.length / 16.0);
        int shownRows = Math.min(rows, 16);
        for (int row = 0; row < shownRows; row++) {
            int from = row * 16;
            int to   = Math.min(from + 16, bytes.length);
            StringBuilder hex = new StringBuilder();
            StringBuilder asc = new StringBuilder();
            for (int i = from; i < to; i++) {
                hex.append(String.format("%02X ", bytes[i] & 0xFF));
                char c = (char)(bytes[i] & 0xFF);
                asc.append(c >= 0x20 && c < 0x7F ? c : '.');
            }
            String rowLabel = String.format("%04X  %-48s  |%s|", from, hex.toString().trim(), asc);
            parent.getChildren().add(syntheticLeaf(rowLabel));
        }
        if (rows > shownRows) {
            parent.getChildren().add(syntheticLeaf("… " + (rows - shownRows) + " more row(s) – see Hex tab for full dump"));
        }
    }

    /** Returns a short descriptive hint for raw binary data based on its length and entropy. */
    private String guessRawBinaryHint(byte[] bytes) {
        return switch (bytes.length) {
            case 16  -> "Raw binary  (128-bit  –  e.g. MD5 / UUID / AES-128 key)";
            case 20  -> "Raw binary  (160-bit  –  e.g. SHA-1 hash)";
            case 28  -> "Raw binary  (224-bit  –  e.g. SHA-224 hash)";
            case 32  -> "Raw binary  (256-bit  –  e.g. SHA-256 hash / AES-256 key)";
            case 48  -> "Raw binary  (384-bit  –  e.g. SHA-384 hash)";
            case 64  -> "Raw binary  (512-bit  –  e.g. SHA-512 hash)";
            default  -> "Raw binary  (" + bytes.length * 8 + "-bit)";
        };
    }

    /**
     * Creates a synthetic (non-model) leaf TreeItem whose label is displayed
     * in the default text colour. Used for hex-row nodes inside raw-binary wrappers.
     */
    @SuppressWarnings({"unchecked","rawtypes"})
    private TreeItem<Object> syntheticLeaf(String label) {
        // We reuse Base64WrapperNode with UNKNOWN format as a plain label carrier
        return new TreeItem<>(new Base64WrapperNode(label, FormatDetector.Format.UNKNOWN, new byte[0]));
    }

    // ══════════════════════════════════════════════════════════════════════
    // Drag & Drop
    // ══════════════════════════════════════════════════════════════════════

    private void setupDragAndDrop(Node target) {
        target.setOnDragOver(e -> {
            if (e.getDragboard().hasFiles()) e.acceptTransferModes(TransferMode.COPY);
            e.consume();
        });
        target.setOnDragDropped(e -> {
            if (e.getDragboard().hasFiles())
                e.getDragboard().getFiles().stream().findFirst().ifPresent(this::loadFile);
            e.setDropCompleted(true);
            e.consume();
        });
    }

    // ══════════════════════════════════════════════════════════════════════
    // File loading
    // ══════════════════════════════════════════════════════════════════════

    private void openFile() {
        FileChooser fc = new FileChooser();
        fc.setTitle("Open File");
        fc.getExtensionFilters().addAll(
                new FileChooser.ExtensionFilter("Supported Files",
                        "*.pb","*.bin","*.dat","*.plist","*.bplist","*.proto.bin",
                        "*.xml","*.json","*.txt","*.bson","*.msgpack","*.thrift","*.fbs","*.ser","*.segb"),
                new FileChooser.ExtensionFilter("Protobuf",         "*.pb","*.bin","*.dat"),
                new FileChooser.ExtensionFilter("Apple BPList",     "*.plist","*.bplist"),
                new FileChooser.ExtensionFilter("Apple SEGB",       "*.segb","*"),
                new FileChooser.ExtensionFilter("XML",              "*.xml"),
                new FileChooser.ExtensionFilter("JSON",             "*.json"),
                new FileChooser.ExtensionFilter("BSON",             "*.bson"),
                new FileChooser.ExtensionFilter("MessagePack",      "*.msgpack","*.mp"),
                new FileChooser.ExtensionFilter("Thrift",           "*.thrift","*.bin"),
                new FileChooser.ExtensionFilter("FlatBuffers",      "*.fbs","*.bin"),
                new FileChooser.ExtensionFilter("Java Serialized",  "*.ser","*.bin"),
                new FileChooser.ExtensionFilter("All Files",        "*.*")
        );
        File file = fc.showOpenDialog(stage);
        if (file != null) loadFile(file);
    }

    private void loadFile(File file) {
        try {
            fileBytes = Files.readAllBytes(file.toPath());
        } catch (IOException ex) {
            showError("File Error", "Could not read file:\n" + ex.getMessage());
            return;
        }

        // ── SEGB fast-path: magic "SEGB" (53 45 47 42) ───────────────────
        // Viele Biome-Dateien tragen vor dem SEGB-Block einen proprietären
        // Outer-Header (siehe SegbParser-Javadoc), das Magic liegt dann
        // nicht bei Offset 0 — daher dieselbe Scan-Distanz wie dort.
        if (findSegbMagic(fileBytes) >= 0) {
            stage.setTitle("Binary Viewer – " + file.getName() + "  [SEGB]");
            hexDumpArea.setText(HexDump.dump(fileBytes));
            loadSegb(file);
            dropHint.setVisible(false);
            treeView.setVisible(true);
            return;
        }

        // Detect format (includes Base64 unwrapping)
        currentFormat = FormatDetector.detect(fileBytes);

        // Resolve the bytes to actually parse
        wasBase64 = FormatDetector.isBase64Wrapped(currentFormat);
        if (wasBase64) {
            Base64Detector.Result b64 = Base64Detector.detect(fileBytes);
            parsedBytes = b64.decodedBytes();
        } else {
            parsedBytes = fileBytes;
        }

        // Window title
        stage.setTitle("Binary Viewer – " + file.getName()
                       + "  [" + FormatDetector.formatName(currentFormat) + "]");
        updateFormatBadge();

        // Hex dump shows the *raw* file bytes
        hexDumpArea.setText(HexDump.dump(fileBytes));

        // Dispatch to the right loader
        FormatDetector.Format inner = FormatDetector.innerFormat(currentFormat);
        switch (inner) {
            case BPLIST        -> loadBPList(file);
            case XML           -> loadXml(file);
            case JSON          -> loadJson(file);
            case BSON,
                 MSGPACK,
                 THRIFT_BINARY,
                 THRIFT_COMPACT,
                 FLATBUFFERS,
                 JAVA_SERIAL   -> loadBinary(file, inner);
            default            -> loadProtobuf(file);
        }

        dropHint.setVisible(false);
        treeView.setVisible(true);
    }

    // ── Protobuf ─────────────────────────────────────────────────────────

    private void loadProtobuf(File file) {
        try {
            protoFields = protoParser.parse(parsedBytes);
            buildProtoTree(protoFields);
            treeView.setShowRoot(false);
            String b64hint = wasBase64 ? "  [Base64-decoded]" : "";
            setStatus("Protobuf" + b64hint + "  |  " + file.getName()
                      + "  |  " + parsedBytes.length + " bytes"
                      + "  |  " + protoFields.size() + " top-level field(s)");
        } catch (ProtobufParseException ex) {
            showError("Parsers Error", "Unknown binary format.");
            setStatus("Unsupported format.");
        }
    }

    // ── BPList ───────────────────────────────────────────────────────────

    private void loadBPList(File file) {
        try {
            bplistRoot = bplistParser.parse(parsedBytes);
            buildBPListTree(bplistRoot);
            treeView.setShowRoot(true);
            int topCount = switch (bplistRoot.getType()) {
                case DICT       -> bplistRoot.getDictEntries().size();
                case ARRAY, SET -> bplistRoot.getArrayElements().size();
                default         -> 1;
            };
            String b64hint = wasBase64 ? "  [Base64-decoded]" : "";
            setStatus("Apple BPList" + b64hint + "  |  " + file.getName()
                      + "  |  " + parsedBytes.length + " bytes"
                      + "  |  Root: " + bplistRoot.getType()
                      + "  |  " + topCount + " entries");
        } catch (BPListUnsupportedVersionException ex) {
            showBplistVersionHint(ex.getVersion(), file);
        } catch (BPListParseException ex) {
            showError("BPList Parse Error", ex.getMessage());
            setStatus("BPList parse error: " + ex.getMessage());
        }
    }

    /**
     * Zeigt einen nicht-fatalen Hinweisdialog für bplist-Versionen ohne
     * öffentliche Spezifikation (z. B. bplist15/16). Die Datei bleibt dabei
     * im Viewer geöffnet — der Hex-Dump-Tab zeigt die Rohdaten weiterhin an.
     */
    private void showBplistVersionHint(String version, File file) {
        Alert a = new Alert(Alert.AlertType.WARNING);
        a.setTitle("Nicht unterstütztes BPList-Format");
        a.setHeaderText("bplist" + version + " wird nicht unterstützt");
        a.setContentText(
                "Diese Datei verwendet das Apple-Binary-Property-List-Format \"bplist" + version + "\".\n\n" +
                "Anders als beim Standardformat bplist00 hat Apple für diese Variante keine\n" +
                "Spezifikation veröffentlicht. \"bplist16\" hat z. B. keinen Trailer und zusätzliche,\n" +
                "undokumentierte Datentypen (UUID, URL, Sets, NULL) — ein Parsing-Versuch nach\n" +
                "dem bplist00-Schema würde daher mit hoher Wahrscheinlichkeit falsche Werte\n" +
                "anzeigen statt eines klaren Fehlers.\n\n" +
                "Die Rohdaten sind im Hex-Dump-Tab weiterhin einsehbar.");
        a.showAndWait();
        setStatus("bplist" + version + "  |  " + file.getName()
                  + "  |  " + parsedBytes.length + " bytes"
                  + "  |  Format nicht unterstützt (keine öffentliche Spezifikation) – siehe Hex-Dump");
    }

    // ── XML ──────────────────────────────────────────────────────────────

    private void loadXml(File file) {
        try {
            xmlRoot = xmlParser.parse(parsedBytes);
            buildXmlTree(xmlRoot);
            treeView.setShowRoot(true);
            String b64hint = wasBase64 ? "  [Base64-decoded]" : "";
            setStatus("XML" + b64hint + "  |  " + file.getName()
                      + "  |  " + parsedBytes.length + " bytes"
                      + "  |  Root: <" + xmlRoot.getName() + ">"
                      + "  |  " + xmlRoot.getChildren().size() + " child(ren)");
        } catch (XmlParseException ex) {
            showError("XML Parse Error", ex.getMessage());
            setStatus("XML parse error: " + ex.getMessage());
        }
    }

    // ── JSON ─────────────────────────────────────────────────────────────

    private void loadJson(File file) {
        try {
            String jsonText = new String(parsedBytes, StandardCharsets.UTF_8);
            jsonRoot = jsonParser.parse(jsonText);
            buildJsonTree(jsonRoot);
            treeView.setShowRoot(true);
            String b64hint = wasBase64 ? "  [Base64-decoded]" : "";
            int topCount = switch (jsonRoot.getType()) {
                case OBJECT -> jsonRoot.getObjectEntries().size();
                case ARRAY  -> jsonRoot.getArrayElements().size();
                default     -> 1;
            };
            setStatus("JSON" + b64hint + "  |  " + file.getName()
                      + "  |  " + parsedBytes.length + " bytes"
                      + "  |  Root: " + jsonRoot.getType()
                      + "  |  " + topCount + " entries");
        } catch (JsonParseException ex) {
            showError("JSON Parse Error", ex.getMessage());
            setStatus("JSON parse error: " + ex.getMessage());
        }
    }

    // ── Binary formats (BSON / MessagePack / Thrift / FlatBuffers) ───────

    private void loadBinary(File file, FormatDetector.Format fmt) {
        String fmtName = FormatDetector.formatName(fmt);
        try {
            binaryRoot = switch (fmt) {
                case BSON           -> bsonParser.parse(parsedBytes);
                case MSGPACK        -> msgPackParser.parse(parsedBytes);
                case THRIFT_BINARY  -> thriftParser.parseBinary(parsedBytes);
                case THRIFT_COMPACT -> thriftParser.parseCompact(parsedBytes);
                case FLATBUFFERS    -> flatBuffersParser.parse(parsedBytes);
                case JAVA_SERIAL    -> javaSerialParser.parse(parsedBytes);
                default -> throw new BinaryParseException(fmtName, "Unexpected format");
            };
            buildBinaryTree(binaryRoot);
            treeView.setShowRoot(true);
            String b64hint = wasBase64 ? "  [Base64-decoded]" : "";
            int topCount = binaryRoot.getChildren().size();
            setStatus(fmtName + b64hint + "  |  " + file.getName()
                      + "  |  " + parsedBytes.length + " bytes"
                      + "  |  " + topCount + " top-level field(s)");
        } catch (BinaryParseException ex) {
            showError(fmtName + " Parse Error", ex.getMessage());
            setStatus(fmtName + " parse error: " + ex.getMessage());
        }
    }

    // ── SEGB (Apple Biome) ───────────────────────────────────────────────

    /**
     * Sucht das SEGB-Magic (53 45 47 42) in den ersten 256 Bytes — dieselbe
     * Scan-Distanz wie {@code SegbParser.MAX_OUTER_SCAN}, da Biome-Dateien
     * häufig einen Outer-Header vor dem eigentlichen SEGB-Block tragen.
     *
     * @return Byte-Offset des Magic, oder -1 wenn keines gefunden wurde
     */
    private static int findSegbMagic(byte[] data) {
        if (data == null) return -1;
        int limit = Math.min(256, data.length - 4);
        for (int i = 0; i <= limit; i++) {
            if (data[i] == 0x53 && data[i + 1] == 0x45
             && data[i + 2] == 0x47 && data[i + 3] == 0x42) {
                return i;
            }
        }
        return -1;
    }

    private void loadSegb(File file) {
        try {
            segbFile = segbParser.parse(fileBytes, file.getName());
            buildSegbTree(segbFile);
            treeView.setShowRoot(true);
            // Set the format badge manually (SEGB is detected outside FormatDetector)
            String versionLabel = switch (segbFile.getVersion()) {
                case V1    -> "v1";
                case V2    -> "v2";
                case FIXED -> "fixed";
                default    -> "?";
            };
            formatBadge.setText("SEGB " + versionLabel);
            formatBadge.setStyle(
                    "-fx-background-color: #1a5a6b;"
                    + "-fx-text-fill: #ffffff;"
                    + "-fx-background-radius: 4;"
                    + "-fx-font-weight: bold; -fx-font-size: 11px;");
            setStatus("Apple SEGB  |  " + file.getName()
                      + "  |  Version: " + segbFile.getVersion()
                      + "  |  " + fileBytes.length + " bytes"
                      + "  |  " + segbFile.getEntryCount() + " record(s)");
        } catch (SegbParseException ex) {
            showError("SEGB Parse Error", ex.getMessage());
            setStatus("SEGB parse error: " + ex.getMessage());
        }
    }

    @SuppressWarnings({"unchecked","rawtypes"})
    private void buildSegbTree(SegbFile sf) {
        // Root node: file summary
        SegbRecord summaryMarker = new SegbRecord(-1, 0, null, new byte[0]);
        TreeItem<Object> root = new TreeItem<>(summaryMarker);
        root.setExpanded(true);

        // Version + entry-count info node (reuse SegbRecord with index -2 as a header marker)
        SegbRecord headerMarker = new SegbRecord(-2, 0, null, new byte[0]);
        TreeItem<Object> headerItem = new TreeItem<>(headerMarker);
        root.getChildren().add(headerItem);

        // One tree item per record
        for (SegbRecord rec : sf.getRecords()) {
            TreeItem<Object> item = new TreeItem<>(rec);
            // If the payload looks like a known format, attach decoded child sub-tree
            if (rec.getPayload().length >= 8) {
                // Skip the first 8 bytes (entry-header / Biome internal), try to parse the rest
                byte[] innerPayload = java.util.Arrays.copyOfRange(rec.getPayload(), 8, rec.getPayload().length);
                if (innerPayload.length > 0) {
                    FormatDetector.Format innerFmt = FormatDetector.detect(innerPayload);
                    FormatDetector.Format parseFmt = FormatDetector.innerFormat(innerFmt);
                    try {
                        switch (parseFmt) {
                            case BPLIST -> {
                                BPListNode bpRoot = new BPListParser().parse(innerPayload);
                                TreeItem<Object> bpItem = new TreeItem<>(bpRoot);
                                addBPListChildren(bpItem, bpRoot);
                                bpItem.setExpanded(true);
                                item.getChildren().add(bpItem);
                            }
                            case XML -> {
                                XmlNode xRoot = new XmlParser().parse(innerPayload);
                                TreeItem<Object> xItem = new TreeItem<>(xRoot);
                                addXmlChildren(xItem, xRoot);
                                xItem.setExpanded(true);
                                item.getChildren().add(xItem);
                            }
                            case JSON -> {
                                JsonNode jRoot = new JsonParser().parse(new String(innerPayload, StandardCharsets.UTF_8));
                                TreeItem<Object> jItem = new TreeItem<>(jRoot);
                                addJsonChildren(jItem, jRoot);
                                jItem.setExpanded(true);
                                item.getChildren().add(jItem);
                            }
                            case PROTOBUF -> {
                                List<ProtoField> fields = new ProtobufParser().parse(innerPayload);
                                addProtoItems(item, fields);
                            }
                            default -> {
                                // raw payload — try as Protobuf (most common in Biome)
                                try {
                                    List<ProtoField> fields = protoParser.parse(innerPayload);
                                    if (!fields.isEmpty()) addProtoItems(item, fields);
                                } catch (Exception ignored) {}
                            }
                        }
                    } catch (Exception ignored) {
                        // inner parse failed – leave as leaf, Hex tab will show raw bytes
                    }
                }
            }
            root.getChildren().add(item);
        }
        treeView.setRoot((TreeItem) root);
    }

    private void closeFile() {
        fileBytes     = null;
        parsedBytes   = null;
        wasBase64     = false;
        protoFields   = null;
        bplistRoot    = null;
        xmlRoot       = null;
        jsonRoot      = null;
        binaryRoot    = null;
        segbFile      = null;
        currentFormat = FormatDetector.Format.UNKNOWN;
        treeView.setRoot(null);
        treeView.setVisible(false);
        dropHint.setVisible(true);
        hexDumpArea.clear();
        fieldHexArea.clear();
        infoArea.clear();
        altList.getItems().clear();
        formatBadge.setText("–");
        formatBadge.setStyle("");
        stage.setTitle("ProtoForensic Viewer");
        setStatus("Ready");
    }

    // ══════════════════════════════════════════════════════════════════════
    // Tree building
    // ══════════════════════════════════════════════════════════════════════

    @SuppressWarnings({"unchecked","rawtypes"})
    private void buildProtoTree(List<ProtoField> fields) {
        TreeItem<Object> root = new TreeItem<>();
        addProtoItems(root, fields);
        treeView.setRoot((TreeItem) root);
    }

    private void addProtoItems(TreeItem<Object> parent, List<ProtoField> fields) {
        for (ProtoField f : fields) {
            TreeItem<Object> item = new TreeItem<>(f);
            if (f.isNestedMessage() && !f.getChildren().isEmpty()) {
                addProtoItems(item, f.getChildren());
                item.setExpanded(true);
            }
            // For LEN-delimited string fields, check whether the value is Base64
            if (f.getWireType() == ProtoField.WIRE_LEN && !f.isNestedMessage()) {
                tryAttachBase64Children(item, f.getInterpretedValue());
            }
            parent.getChildren().add(item);
        }
    }

    @SuppressWarnings({"unchecked","rawtypes"})
    private void buildBPListTree(BPListNode root) {
        TreeItem<Object> rootItem = new TreeItem<>(root);
        addBPListChildren(rootItem, root);
        rootItem.setExpanded(true);
        treeView.setRoot((TreeItem) rootItem);
    }

    private void addBPListChildren(TreeItem<Object> parent, BPListNode node) {
        switch (node.getType()) {
            case DICT -> node.getDictEntries().values().forEach(val -> {
                TreeItem<Object> item = new TreeItem<>(val);
                addBPListChildren(item, val);
                parent.getChildren().add(item);
            });
            case ARRAY, SET -> node.getArrayElements().forEach(child -> {
                TreeItem<Object> item = new TreeItem<>(child);
                addBPListChildren(item, child);
                parent.getChildren().add(item);
            });
            case STRING -> tryAttachBase64Children(parent, node.getScalarValue());
            case DATA   -> {
                // DATA scalar value starts with the hex preview – extract the Base64 line
                String sv = node.getScalarValue();
                if (sv != null) {
                    // The second line (if present) contains "Base64: <value>"
                    for (String line : sv.split("\n")) {
                        String trimmed = line.strip();
                        if (trimmed.startsWith("Base64:")) {
                            tryAttachBase64Children(parent, trimmed.substring("Base64:".length()).strip());
                            break;
                        }
                    }
                }
            }
            default -> {}
        }
    }

    @SuppressWarnings({"unchecked","rawtypes"})
    private void buildXmlTree(XmlNode root) {
        TreeItem<Object> rootItem = new TreeItem<>(root);
        addXmlChildren(rootItem, root);
        rootItem.setExpanded(true);
        treeView.setRoot((TreeItem) rootItem);
    }

    private void addXmlChildren(TreeItem<Object> parent, XmlNode node) {
        // Attribute nodes as direct children (synthetic, leaf nodes)
        node.getAttributes().forEach((k, v) -> {
            XmlNode attrNode = new XmlNode(XmlNode.Type.ATTRIBUTE, k, v);
            TreeItem<Object> attrItem = new TreeItem<>(attrNode);
            tryAttachBase64Children(attrItem, v);
            parent.getChildren().add(attrItem);
        });
        // Element / text / comment / cdata children
        for (XmlNode child : node.getChildren()) {
            TreeItem<Object> item = new TreeItem<>(child);
            if (child.getType() == XmlNode.Type.ELEMENT) {
                addXmlChildren(item, child);
                item.setExpanded(child.getChildren().size() <= 8);
            } else if (child.getType() == XmlNode.Type.TEXT
                       || child.getType() == XmlNode.Type.CDATA) {
                tryAttachBase64Children(item, child.getTextValue());
            }
            parent.getChildren().add(item);
        }
    }

    @SuppressWarnings({"unchecked","rawtypes"})
    private void buildBinaryTree(BinaryNode root) {
        TreeItem<Object> rootItem = new TreeItem<>(root);
        addBinaryChildren(rootItem, root);
        rootItem.setExpanded(true);
        treeView.setRoot((TreeItem) rootItem);
    }

    private void addBinaryChildren(TreeItem<Object> parent, BinaryNode node) {
        switch (node.getKind()) {
            case DOCUMENT, ARRAY -> {
                for (BinaryNode child : node.getChildren()) {
                    TreeItem<Object> item = new TreeItem<>(child);
                    addBinaryChildren(item, child);
                    if (child.getKind() == BinaryNode.Kind.DOCUMENT
                        || child.getKind() == BinaryNode.Kind.ARRAY
                        || child.getKind() == BinaryNode.Kind.MAP)
                        item.setExpanded(true);
                    parent.getChildren().add(item);
                }
            }
            case MAP -> {
                node.getMapEntries().forEach((k, v) -> {
                    TreeItem<Object> item = new TreeItem<>(v);
                    addBinaryChildren(item, v);
                    parent.getChildren().add(item);
                });
            }
            case SCALAR, BINARY -> {
                // Leaf – check for embedded Base64
                if (node.getValue() != null) {
                    String val = node.getValue();
                    // For BINARY nodes the value starts with Base64 content before "  (N bytes)"
                    int spaceIdx = val.indexOf("  (");
                    String candidate = spaceIdx > 0 ? val.substring(0, spaceIdx) : val;
                    // Strip surrounding quotes for strings
                    if (candidate.startsWith("\"") && candidate.endsWith("\""))
                        candidate = candidate.substring(1, candidate.length() - 1);
                    tryAttachBase64Children(parent, candidate);
                }
            }
            default -> {}
        }
    }


    @SuppressWarnings({"unchecked","rawtypes"})
    public void buildJsonTree(JsonNode root) {
        TreeItem<Object> rootItem = new TreeItem<>(root);
        addJsonChildren(rootItem, root);   // ← korrigiert
        rootItem.setExpanded(true);
        treeView.setRoot((TreeItem) rootItem);
    }

    private void addJsonChildren(TreeItem<Object> parent, JsonNode node) {
        switch (node.getType()) {
            case OBJECT -> node.getObjectEntries().values().forEach(val -> {
                TreeItem<Object> item = new TreeItem<>(val);
                addJsonChildren(item, val);
                if (val.getType() == JsonNode.Type.OBJECT || val.getType() == JsonNode.Type.ARRAY)
                    item.setExpanded(true);
                parent.getChildren().add(item);
            });
            case ARRAY -> {
                int idx = 0;
                for (JsonNode el : node.getArrayElements()) {
                    if (el.getKeyLabel() == null) el.setKeyLabel("[" + idx + "]");
                    TreeItem<Object> item = new TreeItem<>(el);
                    addJsonChildren(item, el);
                    if (el.getType() == JsonNode.Type.OBJECT || el.getType() == JsonNode.Type.ARRAY)
                        item.setExpanded(true);
                    parent.getChildren().add(item);
                    idx++;
                }
            }
            case STRING -> tryAttachBase64Children(parent, node.getRawValue());
            default -> {}
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Detail panel
    // ══════════════════════════════════════════════════════════════════════

    private void onNodeSelected(TreeItem<Object> item) {
        if (item == null || item.getValue() == null) return;
        Object val = item.getValue();
        if      (val instanceof ProtoField f)        showProtoDetail(f);
        else if (val instanceof BPListNode n)        showBPListDetail(n);
        else if (val instanceof XmlNode    x)        showXmlDetail(x);
        else if (val instanceof JsonNode   j)        showJsonDetail(j);
        else if (val instanceof BinaryNode b)        showBinaryDetail(b);
        else if (val instanceof SegbRecord r)        showSegbDetail(r);
        else if (val instanceof Base64WrapperNode w) showBase64WrapperDetail(w);
    }

    // ── Binary (BSON / MsgPack / Thrift / FlatBuffers) detail ────────────

    private void showBinaryDetail(BinaryNode b) {
        StringBuilder sb = new StringBuilder();
        sb.append("Format        :  ").append(b.getFormat()).append("\n");
        sb.append("Kind          :  ").append(b.getKind()).append("\n");
        if (b.getTypeName() != null)
            sb.append("Type          :  ").append(b.getTypeName()).append("\n");
        if (b.getFieldName() != null)
            sb.append("Field         :  ").append(b.getFieldName()).append("\n");

        sb.append("\n─── Value ────────────────────────────────\n");
        switch (b.getKind()) {
            case DOCUMENT -> sb.append("{ ").append(b.getChildren().size()).append(" field(s) }\n");
            case ARRAY    -> sb.append("[ ").append(b.getChildren().size()).append(" element(s) ]\n");
            case MAP      -> sb.append("{ ").append(b.getMapEntries().size()).append(" entry/ies }\n");
            case BINARY   -> {
                sb.append(b.getValue() != null ? b.getValue() : "<empty>").append("\n");
                if (b.getRawBytes() != null)
                    sb.append("\nRaw size: ").append(b.getRawBytes().length).append(" bytes\n");
            }
            default -> sb.append(b.getValue() != null ? b.getValue() : "<null>").append("\n");
        }
        infoArea.setText(sb.toString());

        if (b.getRawBytes() != null && b.getRawBytes().length > 0) {
            fieldHexArea.setText(HexDump.dump(b.getRawBytes()));
        } else {
            fieldHexArea.clear();
        }
        altList.setItems(FXCollections.observableArrayList(buildBinaryAlternatives(b)));
    }

    private List<String> buildBinaryAlternatives(BinaryNode b) {
        List<String> alts = new ArrayList<>();
        String v = b.getValue();
        if (v == null) return alts;

        // Strip surrounding quotes for strings
        String stripped = (v.startsWith("\"") && v.endsWith("\""))
                ? v.substring(1, v.length() - 1) : v;

        // Numeric interpretations
        try {
            long l = Long.parseLong(stripped);
            alts.add("int64:          " + l);
            alts.add("uint64:         " + Long.toUnsignedString(l));
            alts.add("hex:            0x" + Long.toHexString(l).toUpperCase());
            alts.add("bool:           " + (l != 0));
            if (l >= 1_000_000_000L && l <= 9_999_999_999L)
                alts.add("unix-timestamp: " + new java.util.Date(l * 1000));
            if (l >= 1_000_000_000_000L && l <= 9_999_999_999_999L)
                alts.add("unix-millis:    " + new java.util.Date(l));
        } catch (NumberFormatException ignored) {}

        try {
            double d = Double.parseDouble(stripped);
            alts.add("double:  " + d);
            alts.add("float:   " + (float) d);
        } catch (NumberFormatException ignored) {}

        // Raw bytes alternatives
        if (b.getRawBytes() != null && b.getRawBytes().length > 0) {
            alts.add("hex:     " + BsonParser.toHex(b.getRawBytes()));
            alts.add("base64:  " + java.util.Base64.getEncoder().encodeToString(b.getRawBytes()));
            alts.add("size:    " + b.getRawBytes().length + " bytes");
        }

        // Base64 hint for string values
        if (stripped.length() >= 8 && stripped.length() % 4 == 0
            && stripped.matches("[A-Za-z0-9+/=]+"))
            alts.add("→ Hint: value may be Base64-encoded data");

        return alts;
    }

    private void showBase64WrapperDetail(Base64WrapperNode w) {
        // Synthetic leaf nodes (hex-row labels inside a raw-binary wrapper) have no bytes
        boolean isSyntheticLeaf = w.decoded().length == 0
                                  && w.innerFormat() == FormatDetector.Format.UNKNOWN;
        if (isSyntheticLeaf) {
            infoArea.setText(w.label());
            fieldHexArea.clear();
            altList.getItems().clear();
            return;
        }

        String formatName = w.innerFormat() == FormatDetector.Format.UNKNOWN
                ? guessRawBinaryHint(w.decoded())
                : FormatDetector.formatName(w.innerFormat());

        infoArea.setText(
                "Base64-decoded payload\n" +
                "─────────────────────────────────────────\n" +
                "Inner format  :  " + formatName + "\n" +
                "Decoded size  :  " + w.decoded().length + " bytes\n\n" +
                (w.innerFormat() == FormatDetector.Format.UNKNOWN
                        ? "Raw binary data – no further structure detected.\n" +
                          "The Hex tab shows the full decoded byte sequence.\n"
                        : "The child nodes below show the parsed structure\n" +
                          "of the decoded content.\n"));
        fieldHexArea.setText(HexDump.dump(w.decoded()));
        altList.setItems(FXCollections.observableArrayList(
                "format:  " + formatName,
                "size:    " + w.decoded().length + " bytes",
                "hex:     " + ProtobufParser.toHex(w.decoded()),
                "base64:  " + java.util.Base64.getEncoder().encodeToString(w.decoded())
        ));
    }

    // ── Protobuf detail ──────────────────────────────────────────────────

    private void showProtoDetail(ProtoField f) {
        StringBuilder sb = new StringBuilder();
        sb.append("Field Number  :  ").append(f.getFieldNumber()).append("\n");
        sb.append("Wire Type     :  ").append(f.getWireType())
                .append("  (").append(f.getWireTypeName()).append(")\n");
        sb.append("Byte Offset   :  0x")
                .append(Long.toHexString(f.getByteOffset()).toUpperCase())
                .append("  (").append(f.getByteOffset()).append(")\n");
        sb.append("Raw Data      :  ").append(f.getRawBytes().length).append(" bytes\n");
        sb.append("\n─── Primary Value ────────────────────────\n");
        sb.append(f.getInterpretedValue()).append("\n");
        if (f.isNestedMessage())
            sb.append("\n▶ Nested Message  (").append(f.getChildren().size()).append(" sub-field(s))\n");
        infoArea.setText(sb.toString());
        fieldHexArea.setText(HexDump.dump(f.getRawBytes(), 0, f.getRawBytes().length, f.getByteOffset()));
        altList.setItems(FXCollections.observableArrayList(
                f.getAlternatives().stream().map(ProtoField.InterpretedAs::toString).toList()));
    }

    // ── BPList detail ────────────────────────────────────────────────────

    private void showBPListDetail(BPListNode n) {
        StringBuilder sb = new StringBuilder();
        sb.append("PList Type    :  ").append(n.getType()).append("\n");
        if (n.getKeyLabel() != null) sb.append("Key           :  ").append(n.getKeyLabel()).append("\n");
        sb.append("Byte Offset   :  0x")
                .append(Long.toHexString(n.getByteOffset()).toUpperCase())
                .append("  (").append(n.getByteOffset()).append(")\n");
        sb.append("Raw Data      :  ").append(n.getRawBytes().length).append(" bytes\n");
        sb.append("\n─── Value ────────────────────────────────\n");
        switch (n.getType()) {
            case DICT       -> sb.append("[Dictionary – ").append(n.getDictEntries().size())
                    .append(" entries]\n\nKeys:\n").append(dictKeyList(n));
            case ARRAY, SET -> sb.append("[").append(n.getType())
                    .append(" – ").append(n.getArrayElements().size()).append(" elements]\n");
            default         -> sb.append(n.getScalarValue() != null ? n.getScalarValue() : "<null>").append("\n");
        }
        infoArea.setText(sb.toString());
        fieldHexArea.setText(HexDump.dump(n.getRawBytes(), 0, n.getRawBytes().length, n.getByteOffset()));
        altList.setItems(FXCollections.observableArrayList(buildBPListAlternatives(n)));
    }

    private String dictKeyList(BPListNode dict) {
        StringBuilder sb = new StringBuilder();
        for (BPListNode key : dict.getDictEntries().keySet())
            sb.append("  • ").append(key.getScalarValue() != null ? key.getScalarValue() : key.treeLabel()).append("\n");
        return sb.toString();
    }

    private List<String> buildBPListAlternatives(BPListNode n) {
        List<String> alts = new ArrayList<>();
        String sv = n.getScalarValue();
        if (sv == null) return alts;
        switch (n.getType()) {
            case INT -> {
                try {
                    long v = Long.parseLong(sv);
                    alts.add("uint64:         " + Long.toUnsignedString(v));
                    alts.add("int64:          " + v);
                    alts.add("hex:            0x" + Long.toHexString(v).toUpperCase());
                    alts.add("bool:           " + (v != 0));
                    if (v >= 1_000_000_000L && v <= 9_999_999_999L)
                        alts.add("unix-timestamp: " + new java.util.Date(v * 1000));
                    if (v >= 1_000_000_000_000L && v <= 9_999_999_999_999L)
                        alts.add("unix-millis:    " + new java.util.Date(v));
                } catch (NumberFormatException ignored) {}
            }
            case REAL -> {
                try {
                    double d = Double.parseDouble(sv);
                    alts.add("double: " + d);
                    alts.add("float:  " + (float)d);
                    alts.add("hex:    0x" + Long.toHexString(Double.doubleToLongBits(d)).toUpperCase());
                } catch (NumberFormatException ignored) {}
            }
            case DATA -> {
                String[] lines = sv.split("\n");
                alts.add("hex:    " + lines[0]);
                if (lines.length > 1) alts.add(lines[1].trim());
                alts.add("→ Hint: raw bytes may be a nested Protobuf / BPList / XML / JSON");
            }
            case STRING -> {
                alts.add("length:  " + sv.length() + " characters");
                alts.add("UTF-8:   " + sv);
                if (sv.length() % 4 == 0 && sv.matches("[A-Za-z0-9+/=]+"))
                    alts.add("→ Hint: value may be Base64-encoded data");
            }
            case UID -> alts.add("NSKeyedArchiver UID – $objects[" + sv.replaceAll("[^0-9]","") + "]");
            default  -> alts.add(n.getType() + ": " + sv);
        }
        return alts;
    }

    // ── XML detail ───────────────────────────────────────────────────────

    private void showXmlDetail(XmlNode x) {
        StringBuilder sb = new StringBuilder();
        sb.append("XML Type      :  ").append(x.getType()).append("\n");
        sb.append("Name          :  ").append(x.getName()).append("\n");

        if (!x.getAttributes().isEmpty()) {
            sb.append("\n─── Attributes ──────────────────────────\n");
            x.getAttributes().forEach((k, v) -> sb.append("  @").append(k).append("  =  \"").append(v).append("\"\n"));
        }
        sb.append("\n─── Value ────────────────────────────────\n");
        switch (x.getType()) {
            case ELEMENT -> {
                sb.append("Children : ").append(x.getChildren().size()).append("\n");
                sb.append("Attrs    : ").append(x.getAttributes().size()).append("\n");
            }
            case TEXT, COMMENT, CDATA -> sb.append(x.getTextValue() != null ? x.getTextValue() : "<empty>").append("\n");
            case ATTRIBUTE -> sb.append(x.getTextValue()).append("\n");
        }
        infoArea.setText(sb.toString());
        fieldHexArea.clear();   // XML nodes don't have individual byte offsets
        altList.setItems(FXCollections.observableArrayList(buildXmlAlternatives(x)));
    }

    private List<String> buildXmlAlternatives(XmlNode x) {
        List<String> alts = new ArrayList<>();
        if (x.getTextValue() != null) {
            String v = x.getTextValue();
            alts.add("length:  " + v.length() + " characters");
            if (v.length() % 4 == 0 && v.matches("[A-Za-z0-9+/=\\s]+"))
                alts.add("→ Hint: value may be Base64-encoded data");
            try { alts.add("long:    " + Long.parseLong(v.strip())); } catch (NumberFormatException ignored) {}
            try { alts.add("double:  " + Double.parseDouble(v.strip())); } catch (NumberFormatException ignored) {}
        }
        return alts;
    }

    // ── JSON detail ──────────────────────────────────────────────────────

    private void showJsonDetail(JsonNode j) {
        StringBuilder sb = new StringBuilder();
        sb.append("JSON Type     :  ").append(j.getType()).append("\n");
        if (j.getKeyLabel() != null) sb.append("Key           :  \"").append(j.getKeyLabel()).append("\"\n");
        sb.append("\n─── Value ────────────────────────────────\n");
        switch (j.getType()) {
            case OBJECT -> sb.append("{ ").append(j.getObjectEntries().size()).append(" key(s) }\n\nKeys:\n")
                    .append(jsonKeyList(j));
            case ARRAY  -> sb.append("[ ").append(j.getArrayElements().size()).append(" element(s) ]\n");
            case STRING -> sb.append("\"").append(j.getRawValue()).append("\"\n");
            default     -> sb.append(j.getRawValue()).append("\n");
        }
        infoArea.setText(sb.toString());
        fieldHexArea.clear();
        altList.setItems(FXCollections.observableArrayList(buildJsonAlternatives(j)));
    }

    private String jsonKeyList(JsonNode obj) {
        StringBuilder sb = new StringBuilder();
        obj.getObjectEntries().keySet().forEach(k -> sb.append("  • \"").append(k).append("\"\n"));
        return sb.toString();
    }

    private List<String> buildJsonAlternatives(JsonNode j) {
        List<String> alts = new ArrayList<>();
        String v = j.getRawValue();
        if (v == null) return alts;
        switch (j.getType()) {
            case NUMBER -> {
                try { alts.add("long:    " + Long.parseLong(v)); } catch (NumberFormatException ignored) {}
                try {
                    double d = Double.parseDouble(v);
                    alts.add("double:  " + d);
                    alts.add("float:   " + (float)d);
                    long bits = Double.doubleToLongBits(d);
                    alts.add("hex:     0x" + Long.toHexString(bits).toUpperCase());
                    if (d >= 1_000_000_000 && d <= 9_999_999_999.0)
                        alts.add("unix-ts: " + new java.util.Date((long)(d * 1000)));
                    if (d >= 1_000_000_000_000.0 && d <= 9_999_999_999_999.0)
                        alts.add("unix-ms: " + new java.util.Date((long)d));
                } catch (NumberFormatException ignored) {}
            }
            case STRING -> {
                alts.add("length:  " + v.length() + " characters");
                if (v.length() % 4 == 0 && v.matches("[A-Za-z0-9+/=]+"))
                    alts.add("→ Hint: value may be Base64-encoded data");
                try { alts.add("long:    " + Long.parseLong(v)); } catch (NumberFormatException ignored) {}
                try { alts.add("double:  " + Double.parseDouble(v)); } catch (NumberFormatException ignored) {}
            }
            case BOOLEAN -> alts.add("bool:  " + v);
            default -> {}
        }
        return alts;
    }

    // ══════════════════════════════════════════════════════════════════════
    // Expand / collapse
    // ══════════════════════════════════════════════════════════════════════

    private void expandAll(TreeItem<?> item, boolean expand) {
        if (item == null) return;
        item.setExpanded(expand);
        item.getChildren().forEach(c -> expandAll(c, expand));
    }

    // ══════════════════════════════════════════════════════════════════════
    // UI helpers
    // ══════════════════════════════════════════════════════════════════════

    private void updateFormatBadge() {
        record BadgeStyle(String label, String color) {}
        BadgeStyle bs = switch (currentFormat) {

            case BMP				   -> new BadgeStyle("BITMAP (.bmp)",  "#1a4d9e");
            case GIF 				   -> new BadgeStyle("GIF (.gif)", 	 "#1a6b2a");
            case PDF 				   -> new BadgeStyle("PDF (.pdf)", 	 "#7a4a00");
            case PNG 				   -> new BadgeStyle("PNG (.png)", 	 "#5a1a6a");
            case JPEG 				   -> new BadgeStyle("JPEG (.jpg)", 	 "#1a5a6b");
            case TIFF 				   -> new BadgeStyle("TIFF (.tiff)", 	 "#6b3a1a");
            case HEIC 				   -> new BadgeStyle("HEIC (.heic)", 	 "#4a1a6b");
            case GZIP 				   -> new BadgeStyle("GZIP (.gz)", 	 "#6b1a4a");
            case AVRO 				   -> new BadgeStyle("AVRO (.avro)", 	 "#1a6b5a");
            case PROTOBUF              -> new BadgeStyle("PROTOBUF",       "#1a4d9e");
            case BPLIST                -> new BadgeStyle("BPLIST",         "#1a6b2a");            case XML                   -> new BadgeStyle("XML",            "#7a4a00");
            case JSON                  -> new BadgeStyle("JSON",           "#5a1a6a");
            case BSON                  -> new BadgeStyle("BSON",           "#1a5a6b");
            case MSGPACK               -> new BadgeStyle("MSGPACK",        "#6b3a1a");
            case THRIFT_BINARY         -> new BadgeStyle("THRIFT-BIN",     "#4a1a6b");
            case THRIFT_COMPACT        -> new BadgeStyle("THRIFT-CMP",     "#6b1a4a");
            case FLATBUFFERS           -> new BadgeStyle("FLATBUF",        "#1a6b5a");
            case JAVA_SERIAL           -> new BadgeStyle("JAVA-SER",       "#6b3a6b");
            case BASE64_BPLIST         -> new BadgeStyle("B64→BPLIST",     "#1a6b2a");
            case BASE64_XML            -> new BadgeStyle("B64→XML",        "#7a4a00");
            case BASE64_JSON           -> new BadgeStyle("B64→JSON",       "#5a1a6a");
            case BASE64_BSON           -> new BadgeStyle("B64→BSON",       "#1a5a6b");
            case BASE64_MSGPACK        -> new BadgeStyle("B64→MSGPACK",    "#6b3a1a");
            case BASE64_THRIFT_BINARY  -> new BadgeStyle("B64→THRIFT-B",   "#4a1a6b");
            case BASE64_THRIFT_COMPACT -> new BadgeStyle("B64→THRIFT-C",   "#6b1a4a");
            case BASE64_FLATBUFFERS    -> new BadgeStyle("B64→FLATBUF",    "#1a6b5a");
            case BASE64_JAVA_SERIAL    -> new BadgeStyle("B64→JAVA-SER",   "#6b3a6b");
            case BASE64_PROTO          -> new BadgeStyle("B64→PROTO",      "#1a4d9e");
            default                    -> new BadgeStyle("UNKNOWN",        "#7a3a3a");
        };
        formatBadge.setText(bs.label());
        formatBadge.setStyle(
                "-fx-background-color: " + bs.color() + ";"
                + "-fx-text-fill: #ffffff;"
                + "-fx-background-radius: 4;"
                + "-fx-font-weight: bold; -fx-font-size: 11px;");
    }

    private void setStatus(String text) { statusLabel.setText(text); }

    private void showError(String title, String msg) {
        Alert a = new Alert(Alert.AlertType.ERROR);
        a.setTitle(title); a.setHeaderText(title); a.setContentText(msg);
        a.showAndWait();
        setStatus("Error: " + " Unsupported format."); //msg.lines().findFirst().orElse(""));
        stage.close();
    }

    private Button toolButton(String label, Runnable action) {
        Button b = new Button(label);
        b.setOnAction(e -> action.run());
        return b;
    }

    private TextArea monospace() {
        TextArea ta = new TextArea();
        ta.setEditable(false);
        ta.setFont(Font.font("Monospaced", 12));
        return ta;
    }

    private Tab closableTab(String title, Node content) {
        Tab t = new Tab(title, content); t.setClosable(false); return t;
    }


    private void info(String title, String header, String body) {
        Alert a = new Alert(Alert.AlertType.INFORMATION);
        a.setTitle(title); a.setHeaderText(header); a.setContentText(body);
        a.showAndWait();
    }

    public void loadBytes(byte[] data, String displayName) {
        this.fileBytes    = data;
        this.currentFormat = FormatDetector.detect(data);

        boolean isB64 = FormatDetector.isBase64Wrapped(currentFormat);
        this.wasBase64    = isB64;
        this.parsedBytes  = isB64
                ? Base64Detector.detect(data).decodedBytes()
                : data;

        // ── SEGB fast-path (Magic kann hinter einem Biome-Outer-Header liegen) ──
        if (findSegbMagic(data) >= 0) {
            stage.setTitle("BinViewer – " + displayName + "  [SEGB]");
            hexDumpArea.setText(HexDump.dump(fileBytes));
            loadSegb(new File(displayName));
            dropHint.setVisible(false);
            treeView.setVisible(true);
            return;
        }

        stage.setTitle("BinViewer – " + displayName
                       + "  [" + FormatDetector.formatName(currentFormat) + "]");
        updateFormatBadge();
        hexDumpArea.setText(HexDump.dump(fileBytes));

        FormatDetector.Format inner = FormatDetector.innerFormat(currentFormat);
        File dummy = new File(displayName);
        switch (inner) {
            case BPLIST        -> loadBPList(dummy);
            case XML           -> loadXml(dummy);
            case JSON          -> loadJson(dummy);
            case BSON,
                 MSGPACK,
                 THRIFT_BINARY,
                 THRIFT_COMPACT,
                 FLATBUFFERS,
                 JAVA_SERIAL   -> loadBinary(dummy, inner);
            default            -> loadProtobuf(dummy);
        }
        dropHint.setVisible(false);
        treeView.setVisible(true);
    }

    /**
     * Opens the viewer window with raw bytes,i.e., from a BLOB.
     *
     * @param data      die zu analysierenden Rohdaten
     * @param displayName  Name der im Fenstertitel angezeigt wird (z. B. Spaltenname)
     */
    public static void openBytes(byte[] data, String displayName) {
        primaryStage = new Stage();
        BinViewer viewer = new BinViewer(primaryStage);
        viewer.loadBytes(data, displayName);
    }

    private void closeWindow(){
        primaryStage.close();
    }
}
