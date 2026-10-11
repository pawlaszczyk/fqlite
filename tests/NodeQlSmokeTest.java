// Desktop integration smoke test; private editor state is inspected intentionally.
// No user files or databases are opened. Run with ./gradlew nodeQlSmokeTest.
import fqlite.nodeql.NodeQlBuilderWindow;
import fqlite.sql.InMemoryDatabase;
import fqlite.ui.NodeObject;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.util.Duration;
import javafx.stage.Stage;
import javafx.scene.layout.Region;
import javafx.scene.control.TextField;
import javafx.scene.control.TextArea;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TableView;
import javafx.embed.swing.SwingFXUtils;
import javax.imageio.ImageIO;
import java.io.File;
import java.lang.reflect.*;
import java.util.*;

public class NodeQlSmokeTest extends Application {
    private NodeQlBuilderWindow builder;
    private InMemoryDatabase db;
    private int checks;
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
    @Override public void start(Stage owner) {
        try {
            db = new InMemoryDatabase("NodeQL verification");
            db.getConnectionObject().createStatement().execute("CREATE TABLE film (id INTEGER, title TEXT)");
            db.getConnectionObject().createStatement().execute("INSERT INTO film VALUES (1, 'First'), (2, 'Second'), (3, 'Third')");
            builder = new NodeQlBuilderWindow(null, new TreeItem<>(new NodeObject("film.sqlite", true)), db, owner);
            builder.show();
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
                    ImageIO.write(SwingFXUtils.fromFXImage(stage.getScene().snapshot(null), null), "png", screenshot);
                    System.out.println("NODEQL VERIFICATION PASSED: " + checks + " checks; " + screenshot.getAbsolutePath());
                    db.getConnectionObject().close();
                    stage.close();
                    Platform.exit();
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
