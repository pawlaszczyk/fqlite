// Desktop integration smoke test; private editor state is inspected intentionally.
// No user files or databases are opened. Run with ./gradlew nodeQlSmokeTest.
import fqlite.nodeql.NodeQlBuilderWindow;
import fqlite.base.GUI;
import fqlite.base.ThemeManager;
import fqlite.sql.InMemoryDatabase;
import fqlite.ui.NodeObject;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.animation.PauseTransition;
import javafx.util.Duration;
import javafx.stage.Stage;
import javafx.scene.layout.Region;
import javafx.scene.control.TextField;
import javafx.scene.control.TextArea;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TableView;
import javafx.scene.control.Button;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.Dialog;
import javafx.scene.control.ButtonType;
import javafx.beans.property.BooleanProperty;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.SplitPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.StackPane;
import javafx.geometry.Point2D;
import javafx.geometry.Point3D;
import javafx.scene.input.MouseEvent;
import javafx.scene.input.MouseButton;
import javafx.scene.input.PickResult;
import javafx.event.Event;
import javafx.event.EventType;
import javafx.scene.Scene;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.stage.Window;
import javafx.beans.value.ChangeListener;
import javafx.embed.swing.SwingFXUtils;
import javax.imageio.ImageIO;
import java.io.File;
import java.lang.reflect.*;
import java.util.*;

public class NodeQlSmokeTest extends Application {
    private NodeQlBuilderWindow builder;
    private InMemoryDatabase db;
    private int checks;
    private GUI mainApp;
    private Scene mainScene;
    private Button mainThemeButton;
    private ChangeListener<Boolean> mainThemeListener;
    private Object get(Object object, String key) throws Exception {
        Field field = object.getClass().getDeclaredField(key);
        field.setAccessible(true);
        return field.get(object);
    }
    private Object call(String method, Object... args) throws Exception {
        for (Method candidate : builder.getClass().getDeclaredMethods()) {
            if (candidate.getName().equals(method) && candidate.getParameterCount() == args.length) {
                candidate.setAccessible(true);
                return candidate.invoke(builder, args);
            }
        }
        throw new NoSuchMethodException(method);
    }
    private Object add(String type) throws Exception {
        Class<? extends Enum> templates = (Class<? extends Enum>) Class.forName("fqlite.nodeql.NodeQlBuilderWindow$BlockTemplate");
        call("addBlock", Enum.valueOf(templates, type));
        return get(builder, "selected");
    }
    private void input(Object block, String key, String value) throws Exception {
        ((Map<String, TextField>) get(block, "fields")).get(key).setText(value);
    }
    private void snap(Object block, double y) throws Exception {
        ((Region) block).relocate(3800, y);
        call("snapOrFloat", block);
        call("refreshSql");
    }
    private String sql() throws Exception { return ((TextArea) get(builder, "sqlPreview")).getText(); }
    private void check(boolean value, String message) {
        if (!value) throw new AssertionError(message);
        checks++;
    }

    private Color background(Scene scene) {
        scene.getRoot().applyCss();
        scene.getRoot().layout();
        return (Color) ((Region) scene.getRoot()).getBackground().getFills().get(0).getFill();
    }

    private void verifyInitialTheme() throws Exception {
        Stage stage = (Stage) get(builder, "stage");
        Button toggle = (Button) get(builder, "themeButton");
        Color light = background(stage.getScene());
        check(light.equals(background(mainScene)), "builder must inherit main app's light palette");
        check(toggle.getText().equals(ThemeManager.toggleLabel()), "initial theme button label");
        check(light.getBrightness() > 0.9, "builder opened dark despite light app setting");

        toggle.fire();
        check(ThemeManager.isDark() && mainApp.isDarkTheme(), "builder toggle must update app theme");
        check(mainThemeButton.getText().equals(toggle.getText()), "main toggle label is stale");
        check(mainThemeButton.getStyle().equals(toggle.getStyle()), "builder must reuse main toggle styling");
        check(background(stage.getScene()).getBrightness() < 0.3, "dark builder background");
        check(background(stage.getScene()).equals(background(mainScene)), "main scene did not change with builder");

        Method mainToggle = GUI.class.getDeclaredMethod("toggleTheme");
        mainToggle.setAccessible(true);
        mainToggle.invoke(mainApp);
        check(!ThemeManager.isDark(), "main app toggle failed");
        check(background(stage.getScene()).equals(light), "main toggle did not update builder");
        check(toggle.getText().equals(ThemeManager.toggleLabel()), "builder toggle label is stale");
        ThemeManager.setDark(true);
    }

    private int registeredScenes() throws Exception {
        Field scenes = ThemeManager.class.getDeclaredField("scenes");
        scenes.setAccessible(true);
        return ((List<?>) scenes.get(null)).size();
    }

    private void captureAfterPulse(Stage stage, File file, Runnable next) {
        // Let CSS, text skins and native window layout settle before taking a real frame.
        PauseTransition settle = new PauseTransition(Duration.millis(250));
        settle.setOnFinished(event -> {
            try {
                var snapshot = stage.getScene().snapshot(null);
                Region canvas = (Region) get(builder, "workspace");
                for (var node : canvas.lookupAll(".label")) {
                    if (!(node instanceof javafx.scene.control.Label label)
                            || !(label.getText().equals("SELECT") || label.getText().equals("EXECUTE QUERY"))) continue;
                    var bounds = label.localToScene(label.getBoundsInLocal());
                    int white = 0;
                    for (int y = Math.max(0, (int) bounds.getMinY()); y < Math.min(snapshot.getHeight(), bounds.getMaxY()); y++) {
                        for (int x = Math.max(0, (int) bounds.getMinX()); x < Math.min(snapshot.getWidth(), bounds.getMaxX()); x++) {
                            Color pixel = snapshot.getPixelReader().getColor(x, y);
                            if (pixel.getRed() > 0.9 && pixel.getGreen() > 0.9 && pixel.getBlue() > 0.9) white++;
                        }
                    }
                    check(white > 20, "block text did not render after theme switch: " + label.getText());
                }
                ImageIO.write(SwingFXUtils.fromFXImage(snapshot, null), "png", file);
                next.run();
            } catch (Throwable error) { error.printStackTrace(); System.exit(1); }
        });
        settle.play();
    }

    private void verifyResultThemes(Stage stage, File screenshot, Runnable finished) throws Exception {
        Button toggle = (Button) get(builder, "themeButton");
        String query = sql();
        Object selection = get(builder, "selected");
        call("showResultsFullscreen");
        Stage results = (Stage) Window.getWindows().stream()
                .filter(window -> window instanceof Stage s && s.getOwner() == stage
                        && s.getTitle().startsWith("NodeQL Results"))
                .findFirst().orElseThrow();
        check(background(results.getScene()).equals(background(stage.getScene())), "expanded results initial theme");

        toggle.fire();
        check(!ThemeManager.isDark(), "light toggle with results open");
        check(background(results.getScene()).equals(background(stage.getScene())), "expanded results must follow theme changes");
        Region canvas = (Region) get(builder, "workspace");
        check(((Color) canvas.getBackground().getFills().get(0).getFill()).getBrightness() > 0.9, "workspace stayed dark");
        checkBlockTextContrast(stage);
        check(sql().equals(query) && get(builder, "selected") == selection, "theme switch changed workspace state");
        check(((TableView<?>) get(builder, "resultTable")).getItems().size() == 3, "theme switch cleared results");
        int count = registeredScenes();
        NodeQlBuilderWindow other = new NodeQlBuilderWindow(mainApp,
                new TreeItem<>(new NodeObject("second.sqlite", true)), db, stage);
        other.show();
        Stage otherStage = (Stage) get(other, "stage");
        check(background(otherStage.getScene()).equals(background(stage.getScene())), "new builder must inherit current theme");
        ((Button) get(other, "themeButton")).fire();
        check(ThemeManager.isDark() && mainApp.isDarkTheme(), "second builder toggle must update app");
        check(background(stage.getScene()).equals(background(otherStage.getScene())), "open builders are not synchronized");
        check(background(results.getScene()).equals(background(stage.getScene())), "expanded results missed second toggle");
        otherStage.close();
        check(registeredScenes() == count, "closed builder scene leaked");
        check(!((Button) get(other, "themeButton")).textProperty().isBound(), "closed builder toggle binding leaked");
        results.close();
        check(registeredScenes() == count - 1, "closed results scene leaked");
        check(background(stage.getScene()).getBrightness() < 0.3, "dark theme did not restore");
        stage.toFront();
        toggle.fire();
        captureAfterPulse(stage, new File(screenshot.getParentFile(), "editor-light.png"), () -> {
            toggle.fire();
            captureAfterPulse(stage, screenshot, () -> {
                try { captureMultiPicker(screenshot, finished); }
                catch (Throwable error) { error.printStackTrace(); System.exit(1); }
            });
        });
    }

    private void captureMultiPicker(File screenshot, Runnable finished) throws Exception {
        Object reporter = null;
        for (Object block : (List<?>) get(builder, "blocks")) {
            if (((Enum<?>) get(block, "template")).name().equals("COLUMN")) reporter = block;
        }
        if (reporter == null) throw new AssertionError("Missing COLUMN reporter for screenshot");
        Dialog<?> picker = openMulti(reporter, "column");
        multiChoice(picker, "id").set(true);
        multiChoice(picker, "title").set(true);
        PauseTransition settle = new PauseTransition(Duration.millis(250));
        settle.setOnFinished(event -> {
            try {
                ImageIO.write(SwingFXUtils.fromFXImage(picker.getDialogPane().getScene().snapshot(null), null),
                        "png", new File(screenshot.getParentFile(), "columns-multi-selection.png"));
                cancelMulti(picker);
                finished.run();
            } catch (Throwable error) { error.printStackTrace(); System.exit(1); }
        });
        settle.play();
    }

    private void checkBlockTextContrast(Stage stage) {
        javafx.scene.control.Label blockLabel = (javafx.scene.control.Label) stage.getScene().lookup(".block-label");
        check(blockLabel.getTextFill().equals(Color.WHITE), "light theme reduced palette block text contrast");
    }

    private void mouse(javafx.scene.Node target, EventType<MouseEvent> type, Point2D point, MouseButton button) {
        Point2D local = target.sceneToLocal(point);
        boolean pressed = type != MouseEvent.MOUSE_RELEASED;
        Event.fireEvent(target, new MouseEvent(type, point.getX(), point.getY(), point.getX(), point.getY(),
                button, 1, false, false, false, false,
                pressed && button == MouseButton.PRIMARY, false, pressed && button == MouseButton.SECONDARY,
                false, false, false, new PickResult(target, new Point3D(local.getX(), local.getY(), 0), 1)));
    }

    private void dragTo(Object block, double x, double y) throws Exception {
        Region view = (Region) block;
        Region workspace = (Region) get(builder, "workspace");
        Point2D start = view.localToScene(10, 10);
        mouse(view, MouseEvent.MOUSE_PRESSED, start, MouseButton.PRIMARY);
        mouse(view, MouseEvent.DRAG_DETECTED, start, MouseButton.PRIMARY);
        Point2D end = workspace.localToScene(x + 10, y + 10);
        mouse(view, MouseEvent.MOUSE_DRAGGED, end, MouseButton.PRIMARY);
        mouse(view, MouseEvent.MOUSE_RELEASED, end, MouseButton.PRIMARY);
        ((Stage) get(builder, "stage")).getScene().getRoot().layout();
    }

    private void verifyViewport() throws Exception {
        Stage stage = (Stage) get(builder, "stage");
        ScrollPane scroll = (ScrollPane) get(builder, "workspaceScroll");
        Region canvas = (Region) get(builder, "workspace");
        double h = scroll.getHvalue() + 0.01;
        double v = scroll.getVvalue() + 0.01;
        scroll.setHvalue(h);
        scroll.setVvalue(v);
        SplitPane panels = (SplitPane) stage.getScene().lookup(".workbench");
        panels.setDividerPositions(0.28, 0.72);
        stage.getScene().getRoot().layout();
        check(Math.abs(scroll.getHvalue() - h) < 0.0001, "resizing panels reset horizontal camera");
        check(Math.abs(scroll.getVvalue() - v) < 0.0001, "resizing panels reset vertical camera");
        Object select = add("SELECT");
        input(select, "table", "film");
        snap(select, 3880);
        stage.getScene().getRoot().layout();
        for (double scale : new double[]{1, 0.7, 1.4}) {
            Point2D before = canvas.sceneToLocal(scroll.localToScene(
                    scroll.getViewportBounds().getWidth() / 2, scroll.getViewportBounds().getHeight() / 2));
            call("setZoom", scale);
            stage.getScene().getRoot().layout();
            Point2D after = canvas.sceneToLocal(scroll.localToScene(
                    scroll.getViewportBounds().getWidth() / 2, scroll.getViewportBounds().getHeight() / 2));
            check(before.distance(after) < 5, "zoom lost the current camera position");
            for (int cycle = 0; cycle < 3; cycle++) {
                h = scroll.getHvalue();
                v = scroll.getVvalue();
                dragTo(select, 4050, 3970);
                check(!(Boolean) get(select, "snapped"), "drag did not detach node");
                dragTo(select, 3800, 3880);
                check((Boolean) get(select, "snapped"), "drag did not reattach node");
                check(Math.abs(scroll.getHvalue() - h) < 0.0001, "node drag panned horizontally");
                check(Math.abs(scroll.getVvalue() - v) < 0.0001, "node drag panned vertically");
                check(sql().contains("FROM film"), "reattached query was not restored");
            }
        }
        call("setZoom", 1.0);
        h = scroll.getHvalue();
        v = scroll.getVvalue();
        Point2D start = scroll.localToScene(60, 60);
        mouse(canvas, MouseEvent.MOUSE_PRESSED, start, MouseButton.SECONDARY);
        mouse(canvas, MouseEvent.MOUSE_DRAGGED, start.add(60, 40), MouseButton.SECONDARY);
        mouse(canvas, MouseEvent.MOUSE_RELEASED, start.add(60, 40), MouseButton.SECONDARY);
        check(scroll.getHvalue() < h && scroll.getVvalue() < v, "explicit empty-canvas panning stopped working");
        call("clearWorkspace");
        panels.setDividerPositions(0.25, 0.73);
        stage.getScene().getRoot().layout();
        call("centerOnChain");
    }

    private HBox tableInput(Object block) throws Exception {
        return schemaInput(block, "table");
    }

    private HBox schemaInput(Object block, String key) throws Exception {
        return (HBox) ((Map<String, StackPane>) get(block, "inputSlots")).get(key).getChildren().get(0);
    }

    private ContextMenu openSuggestions(Object block, String key) throws Exception {
        HBox input = schemaInput(block, key);
        ((Button) input.getChildren().get(1)).fire();
        return (ContextMenu) get(input, "suggestions");
    }

    private void choose(Object block, String key, String label) throws Exception {
        ContextMenu menu = openSuggestions(block, key);
        menu.getItems().stream().filter(item -> item.getText().equals(label)).findFirst().orElseThrow(
                () -> new AssertionError("Missing suggestion: " + key + " / " + label)).fire();
    }

    private Dialog<?> openMulti(Object block, String key) throws Exception {
        HBox control = schemaInput(block, key);
        ((Button) control.getChildren().get(2)).fire();
        return (Dialog<?>) get(control, "activePicker");
    }

    private BooleanProperty multiChoice(Dialog<?> dialog, String label) throws Exception {
        for (Object choice : (List<?>) get(dialog, "choices")) {
            if (get(get(choice, "suggestion"), "label").equals(label)) return (BooleanProperty) get(choice, "selected");
        }
        throw new AssertionError("Missing multi-selection choice: " + label);
    }

    private void applyMulti(Dialog<?> dialog) {
        ((Button) dialog.getDialogPane().lookupButton(ButtonType.APPLY)).fire();
    }

    private void cancelMulti(Dialog<?> dialog) {
        ((Button) dialog.getDialogPane().lookupButton(ButtonType.CANCEL)).fire();
    }

    private void verifyMultiSelection() throws Exception {
        Object select = add("SELECT");
        input(select, "table", "customers");
        input(select, "table_alias", "c");
        snap(select, 3880);
        String before = sql();
        ScrollPane scroll = (ScrollPane) get(builder, "workspaceScroll");
        double h = scroll.getHvalue(), v = scroll.getVvalue();
        Dialog<?> picker = openMulti(select, "columns");
        int scenes = registeredScenes();
        check(picker != null && picker.isShowing(), "multi-picker did not open");
        check(multiChoice(picker, "*").get(), "existing wildcard was not preselected");
        multiChoice(picker, "customer_id").set(true);
        multiChoice(picker, "full name").set(true);
        check(!multiChoice(picker, "*").get(), "specific choices did not clear wildcard");
        check(sql().equals(before), "checkboxes changed SQL before Apply");
        ((TextField) get(picker, "filter")).setText("ODD");
        check(((List<?>) get(picker, "filteredChoices")).size() == 2, "multi-picker search is not case-insensitive");
        check(multiChoice(picker, "customer_id").get() && multiChoice(picker, "full name").get(), "filtering discarded hidden selections");
        ((TextField) get(picker, "filter")).clear();
        ThemeManager.setDark(false);
        check(picker.getDialogPane().getScene().getRoot().getStyle()
                .equals(((Stage) get(builder, "stage")).getScene().getRoot().getStyle()), "multi-picker missed shared theme update");
        ThemeManager.setDark(true);
        applyMulti(picker);
        check(sql().contains("SELECT \"customer_id\", \"full name\""), "multi-picker did not apply both columns");
        try (var statement = db.getConnectionObject().createStatement(); var result = statement.executeQuery(sql())) {
            check(result.next() && result.getInt(1) == 1 && result.getString(2).equals("Ada"), "multi-selected query did not execute");
        }
        check(Math.abs(scroll.getHvalue() - h) < 0.0001 && Math.abs(scroll.getVvalue() - v) < 0.0001, "multi-selection moved the workspace camera");
        check(registeredScenes() == scenes - 1, "multi-picker leaked its scene: " + scenes + " -> " + registeredScenes());
        call("undoLastAction");
        check(sql().equals(before), "multi-selection was not a single undo action");
        select = ((List<?>) get(builder, "blocks")).getFirst();
        picker = openMulti(select, "columns");
        multiChoice(picker, "odd\"col").set(true);
        cancelMulti(picker);
        check(sql().equals(before), "Cancel modified the query");

        input(select, "columns", "\"full name\", customer_id");
        picker = openMulti(select, "columns");
        check(multiChoice(picker, "full name").get() && multiChoice(picker, "customer_id").get(), "quoted/unquoted existing columns were not preselected");
        applyMulti(picker);
        check(sql().contains("SELECT \"full name\", \"customer_id\""), "preselected column order changed");
        input(select, "columns", "printf('%s,%s', 'a', 'b'), customer_id");
        picker = openMulti(select, "columns");
        multiChoice(picker, "full name").set(true);
        applyMulti(picker);
        check(sql().contains("printf('%s,%s', 'a', 'b'), \"customer_id\", \"full name\""), "multi-picker corrupted custom expression or its commas");
        picker = openMulti(select, "columns");
        ((Button) get(picker, "clear")).fire();
        check(picker.getDialogPane().lookupButton(ButtonType.APPLY).isDisabled(), "empty selection can be applied");
        multiChoice(picker, "odd\"col").set(true);
        multiChoice(picker, "full name").set(true);
        applyMulti(picker);
        check(!sql().contains("printf") && sql().contains("\"odd\"\"col\", \"full name\""), "clearing/choosing columns did not replace the previous selection");
        try (var statement = db.getConnectionObject().createStatement(); var result = statement.executeQuery(sql())) {
            check(result.next() && result.getString(1).equals("value") && result.getString(2).equals("Ada"), "multi-selection identifier quoting/order");
        }

        Object join = add("LEFT_JOIN");
        input(join, "table", "orders AS o");
        input(join, "on", "c.customer_id = o.customer_id");
        snap(join, 3936);
        input(select, "columns", "*");
        picker = openMulti(select, "columns");
        multiChoice(picker, "c.customer_id").set(true);
        multiChoice(picker, "o.amount").set(true);
        applyMulti(picker);
        Object group = add("GROUP_BY");
        snap(group, 4022);
        picker = openMulti(group, "expr");
        ((Button) get(picker, "clear")).fire();
        multiChoice(picker, "c.customer_id").set(true);
        multiChoice(picker, "o.amount").set(true);
        applyMulti(picker);
        check(sql().contains("GROUP BY \"c\".\"customer_id\", \"o\".\"amount\""), "GROUP BY multi-selection");
        try (var statement = db.getConnectionObject().createStatement(); var result = statement.executeQuery(sql())) {
            check(result.next() && result.getInt(1) == 1 && result.getInt(2) == 20, "qualified multi-selection with JOIN/grouping did not execute");
        }

        call("clearWorkspace");
        select = add("SELECT");
        input(select, "table", "film");
        snap(select, 3880);
        Object reporter = add("COLUMN");
        call("attachReporter", reporter, select, "columns");
        picker = openMulti(reporter, "column");
        multiChoice(picker, "id").set(true);
        multiChoice(picker, "title").set(true);
        applyMulti(picker);
        check(sql().contains("SELECT \"id\", \"title\""), "embedded column reporter multi-selection");
        Object snapshot = call("captureWorkspaceState");
        call("clearWorkspace");
        call("restoreWorkspaceState", snapshot);
        check(sql().contains("SELECT \"id\", \"title\""), "nested multi-selection undo restoration");

        try (var statement = db.getConnectionObject().createStatement()) {
            List<String> definitions = new ArrayList<>();
            for (int i = 0; i < 30; i++) definitions.add("c" + i + " INTEGER");
            statement.execute("CREATE TABLE wide_columns (" + String.join(", ", definitions) + ")");
        }
        call("clearWorkspace");
        select = add("SELECT");
        input(select, "table", "wide_columns");
        snap(select, 3880);
        picker = openMulti(select, "columns");
        multiChoice(picker, "c28").set(true);
        multiChoice(picker, "c29").set(true);
        applyMulti(picker);
        check(sql().contains("SELECT \"c28\", \"c29\""), "multi-picker is capped at the first 20 suggestions");
        call("clearWorkspace");
    }

    private void verifyColumnSuggestions() throws Exception {
        try (var statement = db.getConnectionObject().createStatement()) {
            statement.execute("CREATE TABLE customers (customer_id INTEGER, \"full name\" TEXT, \"odd\"\"col\" TEXT, "
                    + "derived TEXT GENERATED ALWAYS AS (\"full name\" || '!') VIRTUAL)");
            statement.execute("INSERT INTO customers(customer_id, \"full name\", \"odd\"\"col\") VALUES(1, 'Ada', 'value')");
            statement.execute("CREATE TABLE orders(order_id INTEGER, customer_id INTEGER, amount INTEGER)");
            statement.execute("INSERT INTO orders VALUES (10, 1, 20)");
        }
        Object select = add("SELECT");
        input(select, "table", "main.customers");
        input(select, "table_alias", "c");
        snap(select, 3880);
        ContextMenu menu = openSuggestions(select, "columns");
        check(menu.getItems().stream().anyMatch(item -> item.getText().equals("derived")), "generated columns omitted");
        check(menu.getItems().stream().noneMatch(item -> item.getText().equals("title")), "unrelated table columns suggested");
        menu.hide();
        choose(select, "columns", "full name");
        check(sql().contains("SELECT \"full name\""), "column identifier with spaces was not quoted");
        try (var statement = db.getConnectionObject().createStatement(); var result = statement.executeQuery(sql())) {
            check(result.next() && result.getString(1).equals("Ada"), "selected column did not execute");
        }
        choose(select, "columns", "c.customer_id");
        check(sql().contains("SELECT \"c\".\"customer_id\""), "SELECT alias qualification");
        input(select, "columns", "*");
        choose(select, "columns", "c.*");
        try (var statement = db.getConnectionObject().createStatement(); var result = statement.executeQuery(sql())) {
            check(result.next() && result.getMetaData().getColumnCount() == 4, "qualified wildcard suggestion did not execute");
        }
        input(select, "columns", "\"c\".\"customer_id\", fu");
        choose(select, "columns", "full name");
        check(sql().contains("\"c\".\"customer_id\", \"full name\""), "SELECT list prefix was overwritten");
        try (var statement = db.getConnectionObject().createStatement(); var result = statement.executeQuery(sql())) {
            check(result.next() && result.getInt(1) == 1 && result.getString(2).equals("Ada"), "multiple suggested columns did not execute");
        }
        input(select, "columns", "printf('%s,%s', 'a', 'b'), fu");
        choose(select, "columns", "full name");
        check(sql().contains("printf('%s,%s', 'a', 'b'), \"full name\""), "function/string commas damaged projection list");
        choose(select, "columns", "odd\"col");
        check(sql().contains("\"odd\"\"col\""), "column quote escaping");
        try (var statement = db.getConnectionObject().createStatement(); var result = statement.executeQuery(sql())) {
            check(result.next() && result.getString(2).equals("value"), "quoted column did not execute");
        }
        input(select, "columns", "COALESCE(\"full name\", 'unknown')");
        menu = openSuggestions(select, "columns");
        menu.hide();
        check(sql().contains("COALESCE(\"full name\", 'unknown')"), "opening/dismissing column suggestions changed custom expression");

        Object join = add("LEFT_JOIN");
        input(join, "table", "orders AS o");
        input(join, "on", "c.customer_id = o.customer_id");
        snap(join, 3936);
        menu = openSuggestions(select, "columns");
        List<String> labels = menu.getItems().stream().map(item -> item.getText()).toList();
        check(labels.containsAll(List.of("c.customer_id", "o.customer_id")), "join alias columns missing");
        check(!labels.contains("customer_id"), "ambiguous unqualified join column suggested");
        menu.hide();
        input(select, "columns", "\"c\".\"customer_id\", ");
        choose(select, "columns", "o.amount");
        try (var statement = db.getConnectionObject().createStatement(); var result = statement.executeQuery(sql())) {
            check(result.next() && result.getInt(1) == 1 && result.getInt(2) == 20, "qualified join column suggestion did not execute");
        }
        menu = openSuggestions(join, "on");
        check(menu.getItems().stream().anyMatch(item -> item.getText().equals("o.customer_id")), "JOIN condition has no column suggestions");
        menu.hide();
        for (String[] entry : List.of(new String[]{"WHERE", "column"}, new String[]{"ORDER_BY", "column"},
                new String[]{"GROUP_BY", "expr"}, new String[]{"COUNT", "expr"},
                new String[]{"SQLITE_FUNCTION", "args"}, new String[]{"ALIAS", "value"})) {
            Object block = add(entry[0]);
            menu = openSuggestions(block, entry[1]);
            check(menu.getItems().stream().anyMatch(item -> item.getText().equals("c.customer_id")), "missing column suggestions in " + entry[0]);
            menu.hide();
        }
        try (var statement = db.getConnectionObject().createStatement()) {
            statement.execute("ALTER TABLE customers ADD COLUMN bonus INTEGER");
        }
        menu = openSuggestions(select, "columns");
        check(menu.getItems().stream().anyMatch(item -> item.getText().equals("bonus")), "column schema refresh");
        menu.hide();
        input(select, "table", "film");
        menu = openSuggestions(select, "columns");
        check(menu.getItems().stream().anyMatch(item -> item.getText().equals("title"))
                && menu.getItems().stream().noneMatch(item -> item.getText().equals("full name")), "columns did not follow changed source table");
        menu.hide();
        input(select, "table", "customers");
        input(select, "table_alias", "\"");
        menu = openSuggestions(select, "columns");
        check(((Map<String, TextField>) get(select, "fields")).get("table_alias").getText().equals("\""), "incomplete custom alias was modified or crashed suggestions");
        menu.hide();
        input(select, "table_alias", "c");
        Object from = add("FROM");
        input(from, "table", "film_view");
        snap(from, 3936);
        menu = openSuggestions(select, "columns");
        check(menu.getItems().stream().anyMatch(item -> item.getText().equals("film_view.title"))
                && menu.getItems().stream().noneMatch(item -> item.getText().equals("c.customer_id")), "FROM override/view columns are wrong");
        menu.hide();
        call("clearWorkspace");
        select = add("SELECT");
        input(select, "table", "temp.temporary_choices");
        menu = openSuggestions(select, "columns");
        check(menu.getItems().stream().anyMatch(item -> item.getText().equals("id")), "temporary table columns missing");
        menu.hide();
        input(select, "table", "\"Order Items\" AS \"order alias\"");
        menu = openSuggestions(select, "columns");
        check(menu.getItems().stream().anyMatch(item -> item.getText().equals("order alias.id")), "quoted table/alias parsing");
        menu.hide();
        call("clearWorkspace");
        select = add("SELECT");
        input(select, "table", "customers");
        input(select, "table_alias", "c");
        snap(select, 3880);
        Object reporter = add("COLUMN");
        call("attachReporter", reporter, select, "columns");
        choose(reporter, "column", "c.full name");
        check(sql().contains("SELECT \"c\".\"full name\""), "nested reporter lost table/alias context");
        check(!schemaInput(select, "columns").isVisible(), "embedded reporter left a visible suggestion button behind");
        Object state = call("captureWorkspaceState");
        call("clearWorkspace");
        call("restoreWorkspaceState", state);
        check(sql().contains("SELECT \"c\".\"full name\""), "undo lost chosen column in nested reporter");
        select = ((List<?>) get(builder, "blocks")).get(0);
        reporter = ((List<?>) get(builder, "blocks")).get(1);
        call("detachReporter", reporter);
        check(schemaInput(select, "columns").isVisible(), "detaching reporter did not restore column suggestions");
        call("clearWorkspace");
    }

    private void verifyTableSuggestions() throws Exception {
        try (var statement = db.getConnectionObject().createStatement()) {
            statement.execute("CREATE TABLE \"Order Items\" (id INTEGER)");
            statement.execute("CREATE TABLE \"odd\"\"name\" (id INTEGER)");
            statement.execute("CREATE VIEW film_view AS SELECT * FROM film");
            statement.execute("CREATE TEMP TABLE temporary_choices (id INTEGER)");
            statement.execute("CREATE TABLE numbered (id INTEGER PRIMARY KEY AUTOINCREMENT)");
        }
        List<?> names = (List<?>) call("loadTableNames");
        check(names.containsAll(List.of("film", "film_view", "Order Items", "temporary_choices")), "schema suggestions omit tables, views or temporary tables");
        check(names.stream().noneMatch(name -> name.toString().startsWith("sqlite_")), "internal SQLite tables leaked into suggestions");

        Class<?> templates = Class.forName("fqlite.nodeql.NodeQlBuilderWindow$BlockTemplate");
        Method keys = templates.getDeclaredMethod("fieldKeys");
        keys.setAccessible(true);
        for (Object template : templates.getEnumConstants()) {
            if (!((List<?>) keys.invoke(template)).contains("table")) continue;
            Object block = add(((Enum<?>) template).name());
            HBox control = tableInput(block);
            ((Button) control.getChildren().get(1)).fire();
            ContextMenu menu = (ContextMenu) get(control, "suggestions");
            check(menu.getItems().stream().anyMatch(item -> item.getText().equals("film")), "table suggestions missing from " + template);
            menu.hide();
        }
        call("clearWorkspace");
        Object select = add("SELECT");
        snap(select, 3880);
        HBox control = tableInput(select);
        TextField editor = (TextField) control.getChildren().get(0);
        Button browse = (Button) control.getChildren().get(1);
        ContextMenu menu = (ContextMenu) get(control, "suggestions");
        browse.fire();
        menu.getItems().stream().filter(item -> item.getText().equals("Order Items")).findFirst().orElseThrow().fire();
        check(editor.getText().equals("\"Order Items\""), "selected schema name was not SQL-quoted");
        try (var statement = db.getConnectionObject().createStatement(); var result = statement.executeQuery(sql())) {
            check(result.getMetaData().getColumnCount() == 1, "suggested table with spaces does not execute");
        }
        browse.fire();
        menu.getItems().stream().filter(item -> item.getText().equals("odd\"name")).findFirst().orElseThrow().fire();
        check(editor.getText().equals("\"odd\"\"name\""), "identifier quote escaping");
        try (var statement = db.getConnectionObject().createStatement(); var result = statement.executeQuery(sql())) {
            check(result.getMetaData().getColumnCount() == 1, "suggested quoted table does not execute");
        }
        editor.requestFocus();
        editor.setText("FiLm");
        Method show = control.getClass().getDeclaredMethod("showSuggestions", boolean.class);
        show.setAccessible(true);
        show.invoke(control, true);
        check(menu.getItems().stream().map(item -> item.getText()).toList().equals(List.of("film", "film_view")), "suggestions are not filtered case-insensitively");
        editor.setText("(SELECT * FROM film) AS custom");
        menu.hide();
        check(editor.getText().equals("(SELECT * FROM film) AS custom") && sql().contains("FROM (SELECT * FROM film) AS custom"), "suggestions replaced custom SQL input");
        try (var statement = db.getConnectionObject().createStatement()) { statement.execute("CREATE TABLE recently_added (id INTEGER)"); }
        browse.fire();
        check(menu.getItems().stream().anyMatch(item -> item.getText().equals("recently_added")), "schema suggestions did not refresh");
        menu.hide();
        call("clearWorkspace");
    }

    private void verifyLiveSuggestions() throws Exception {
        Object select = ((List<?>) get(builder, "blocks")).get(0);
        HBox control = tableInput(select);
        TextField editor = (TextField) control.getChildren().get(0);
        ContextMenu menu = (ContextMenu) get(control, "suggestions");
        editor.requestFocus();
        editor.setText("FiLm");
        check(menu.isShowing(), "typing into a focused table field did not show suggestions");
        check(menu.getItems().stream().map(item -> item.getText()).toList().equals(List.of("film", "film_view")), "live typing did not filter table suggestions");
        Event.fireEvent(editor, new javafx.scene.input.KeyEvent(javafx.scene.input.KeyEvent.KEY_PRESSED,
                "", "", javafx.scene.input.KeyCode.ESCAPE, false, false, false, false));
        check(!menu.isShowing(), "Escape did not dismiss table suggestions");
        check(editor.getText().equals("FiLm"), "dismissing suggestions changed the custom input");
        editor.setText("film");
        ((Button) get(builder, "runQueryButton")).requestFocus();
        menu.hide();

        Object reporter = null;
        for (Object block : (List<?>) get(builder, "blocks")) {
            if (((Enum<?>) get(block, "template")).name().equals("COLUMN")) reporter = block;
        }
        if (reporter == null) throw new AssertionError("Missing final COLUMN reporter");
        String originalQuery = sql();
        control = schemaInput(reporter, "column");
        editor = (TextField) control.getChildren().get(0);
        menu = (ContextMenu) get(control, "suggestions");
        editor.requestFocus();
        editor.setText("TiT");
        check(menu.isShowing(), "typing into a focused column field did not show suggestions");
        check(menu.getItems().stream().map(item -> item.getText()).toList().equals(List.of("title", "film.title")), "column filtering is not case-insensitive/contextual");
        editor.setText("LOWER(title)");
        check(editor.getText().equals("LOWER(title)"), "column suggestions replaced custom expression");
        editor.setText("*");
        ((Button) get(builder, "runQueryButton")).requestFocus();
        menu.hide();
        check(sql().equals(originalQuery), "live column tests did not restore the original query");
    }
    @Override public void start(Stage owner) {
        try {
            db = new InMemoryDatabase("NodeQL verification");
            db.getConnectionObject().createStatement().execute("CREATE TABLE film (id INTEGER, title TEXT)");
            db.getConnectionObject().createStatement().execute("INSERT INTO film VALUES (1, 'First'), (2, 'Second'), (3, 'Third')");
            // Exercise actual GUI theme callbacks without starting importers or reading user settings.
            mainApp = new GUI();
            mainThemeButton = new Button();
            Field toggleField = GUI.class.getDeclaredField("themeBtn");
            toggleField.setAccessible(true);
            toggleField.set(mainApp, mainThemeButton);
            mainThemeListener = (ChangeListener<Boolean>) get(mainApp, "sharedThemeListener");
            ThemeManager.darkProperty().addListener(mainThemeListener);
            mainScene = new Scene(new VBox(mainThemeButton));
            ThemeManager.register(mainScene);
            ThemeManager.setDark(false);
            builder = new NodeQlBuilderWindow(mainApp, new TreeItem<>(new NodeObject("film.sqlite", true)), db, owner);
            builder.show();
            verifyInitialTheme();
            verifyViewport();
            verifyTableSuggestions();
            verifyColumnSuggestions();
            verifyMultiSelection();
            Object select = add("SELECT");
            input(select, "table", "film");
            snap(select, 3880);
            Object where = add("WHERE");
            input(where, "column", "id");
            input(where, "operator", ">");
            input(where, "value", "1");
            snap(where, 3936);
            Object order = add("ORDER_BY");
            input(order, "column", "id");
            input(order, "order", "DESC");
            snap(order, 3986);
            check(sql().contains("WHERE id > 1"), "structured WHERE was not compiled");
            check(sql().contains("ORDER BY id DESC"), "ORDER BY was not compiled");
            check(((Region) order).getLayoutY() == 3986, "chain did not use per-block heights");
            Object reporter = add("COUNT");
            Stage active = (Stage) get(builder, "stage");
            active.getScene().getRoot().applyCss();
            active.getScene().getRoot().layout();
            javafx.scene.layout.StackPane target = ((Map<String, javafx.scene.layout.StackPane>) get(select, "inputSlots")).get("columns");
            javafx.scene.layout.Pane workspace = (javafx.scene.layout.Pane) get(builder, "workspace");
            javafx.geometry.Point2D drop = workspace.sceneToLocal(target.localToScene(target.getWidth() / 2, target.getHeight() / 2));
            ((Region) reporter).relocate(drop.getX() - 40, drop.getY() - 20);
            call("snapOrFloat", reporter);
            check(get(reporter, "reporterOwner") == select, "reporter drop-target hit testing");
            call("refreshSql");
            check(sql().contains("SELECT COUNT(*)"), "nested reporter not compiled");
            check(!((Boolean) get(reporter, "snapped")), "reporter incorrectly joined statement chain");
            Object snapshot = call("captureWorkspaceState");
            call("clearWorkspace");
            call("restoreWorkspaceState", snapshot);
            check(sql().contains("SELECT COUNT(*)"), "undo did not restore reporter input");
            List<?> blocks = (List<?>) get(builder, "blocks");
            reporter = blocks.get(3);
            select = blocks.get(0);
            check(get(reporter, "reporterOwner") == select, "reporter owner not restored");
            call("detachReporter", reporter);
            call("refreshSql");
            check(sql().contains("SELECT *"), "detach did not restore original input");
            input(select, "table_alias", "f");
            check(sql().contains("FROM film AS f"), "table alias missing");
            input(select, "table_alias", "");
            Object join = add("LEFT_JOIN");
            check(((Region) join).getPrefHeight() == 94, "join does not use 86px body + tab");
            Object terminal = add("COMMIT");
            check(((Region) terminal).getPrefHeight() == 48, "terminal has a bottom connector");
            Object sum = add("SUM");
            check(((Region) sum).getPrefHeight() == 44, "expression is not an oval 44px reporter");
            call("clearWorkspace");
            select = add("SELECT");
            input(select, "table", "film");
            snap(select, 3880);
            Object alias = add("ALIAS");
            input(alias, "alias", "timestamp_text");
            Object function = add("SQLITE_FUNCTION");
            input(function, "function", "datetime");
            input(function, "args", "0, 'unixepoch'");
            call("attachReporter", function, alias, "value");
            call("attachReporter", alias, select, "columns");
            call("refreshSql");
            check(sql().contains("datetime(0, 'unixepoch') AS \"timestamp_text\""), "nested function alias SQL");
            Object nestedSnapshot = call("captureWorkspaceState");
            call("clearWorkspace");
            call("restoreWorkspaceState", nestedSnapshot);
            check(sql().contains("datetime(0, 'unixepoch') AS \"timestamp_text\""), "nested reporter undo tree");
            try (var statement = db.getConnectionObject().createStatement(); var result = statement.executeQuery(sql())) {
                check(result.next() && result.getString(1).equals("1970-01-01 00:00:00"), "SQLite function execution");
            }
            select = ((List<?>) get(builder, "blocks")).get(0);
            input(select, "select_mode", "DISTINCT");
            check(sql().startsWith("SELECT DISTINCT"), "distinct mode");
            Object limit = add("LIMIT");
            input(limit, "count", "1");
            input(limit, "offset", "1");
            snap(limit, 3936);
            check(sql().contains("LIMIT 1 OFFSET 1"), "limit offset compilation");
            Object text = add("TEXT");
            input(text, "text", "O'Brien");
            check(call("reporterSql", text).equals("'O''Brien'"), "text literal quoting");
            call("clearWorkspace");
            select = add("SELECT");
            input(select, "table", "film");
            snap(select, 3880);
            reporter = add("COLUMN");
            input(reporter, "column", "*");
            call("attachReporter", reporter, select, "columns");
            order = add("ORDER_BY");
            input(order, "column", "id");
            input(order, "order", "DESC");
            snap(order, 3936);
            call("refreshSql");
            call("executeQuery");
            Timeline wait = new Timeline();
            int[] attempts = {0};
            wait.getKeyFrames().add(new KeyFrame(Duration.millis(200), event -> {
                try {
                    if (get(builder, "lastResult") == null && ++attempts[0] < 40) return;
                    wait.stop();
                    TableView<?> table = (TableView<?>) get(builder, "resultTable");
                    check(table.getItems().size() == 3, "inline query result row count");
                    check(table.getColumns().size() == 2, "inline query result columns");
                    check(((List<?>) table.getItems().get(0)).get(0).equals("3"), "result sort order");
                    verifyLiveSuggestions();
                    Stage stage = (Stage) get(builder, "stage");
                    stage.getScene().getRoot().applyCss();
                    stage.getScene().getRoot().layout();
                    for (Object block : (List<?>) get(builder, "blocks")) {
                        Region content = (Region) get(block, "content");
                        check(content.getLayoutX() + content.getWidth() <= ((Region) block).getWidth() + 1,
                                "block content exceeds shape width");
                    }
                    File screenshot = new File(System.getProperty("nodeql.screenshot", "build/reports/nodeql/editor.png"));
                    java.nio.file.Files.createDirectories(screenshot.getAbsoluteFile().getParentFile().toPath());
                    // Explicitly exercise the palette's center action for the full query width.
                    call("centerOnChain");
                    verifyResultThemes(stage, screenshot, () -> {
                        try {
                            db.getConnectionObject().close();
                            stage.close();
                            ThemeManager.unregister(mainScene);
                            ThemeManager.darkProperty().removeListener(mainThemeListener);
                            check(registeredScenes() == 0, "builder or main test scene was not unregistered");
                            System.out.println("NODEQL VERIFICATION PASSED: " + checks + " checks; " + screenshot.getAbsolutePath());
                            Platform.exit();
                        } catch (Throwable error) { error.printStackTrace(); System.exit(1); }
                    });
                } catch (Throwable error) { error.printStackTrace(); System.exit(1); }
            }));
            wait.setCycleCount(Timeline.INDEFINITE);
            wait.play();
        } catch (Throwable error) { error.printStackTrace(); System.exit(1); }
    }
    public static void main(String[] args) { launch(args); }
}

class NodeQlSmokeTestLauncher {
    public static void main(String[] args) { Application.launch(NodeQlSmokeTest.class, args); }
}
