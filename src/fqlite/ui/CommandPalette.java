package fqlite.ui;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import fqlite.base.GUI;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.control.Menu;
import javafx.scene.control.MenuItem;
import javafx.scene.control.SeparatorMenuItem;
import javafx.scene.control.TextField;
import javafx.scene.control.TreeItem;
import javafx.scene.input.KeyCode;
import javafx.scene.layout.VBox;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.StageStyle;

/**
 * Command palette (Ctrl+K / Cmd+K): every menu command and every table of the open
 * databases, searchable by typing parts of the name in any order.
 */
public final class CommandPalette {

	private CommandPalette() {
	}

	private record Entry(String label, String kind, Runnable action) {
	}

	private static Stage open;

	public static void show(GUI gui) {
		if (open != null && open.isShowing()) {
			open.toFront();
			return;
		}
		List<Entry> entries = new ArrayList<>();
		if (gui.getMenuBar() != null)
			for (Menu m : gui.getMenuBar().getMenus())
				collect(m, m.getText(), entries);
		TreeItem<fqlite.ui.NodeObject> root = gui.getTreeRoot();
		if (root != null)
			tables(gui, root, "", entries, 0);

		Stage st = new Stage(StageStyle.UTILITY);
		st.initOwner(gui.getStage());
		st.initModality(Modality.NONE);
		st.setTitle("Command palette");
		TextField q = new TextField();
		q.setPromptText("Type a command or a table name…  (Enter runs, Esc closes)");
		ObservableList<Entry> all = FXCollections.observableArrayList(entries);
		FilteredList<Entry> shown = new FilteredList<>(all, e -> true);
		ListView<Entry> lv = new ListView<>(shown);
		lv.setCellFactory(x -> new ListCell<>() {
			@Override
			protected void updateItem(Entry e, boolean empty) {
				super.updateItem(e, empty);
				if (empty || e == null) {
					setText(null);
					setGraphic(null);
				} else {
					Label k = new Label(e.kind());
					k.setStyle("-fx-opacity: 0.6; -fx-font-size: 0.85em;");
					k.setMinWidth(70);
					setGraphic(k);
					setText(e.label());
				}
			}
		});
		q.textProperty().addListener((o, a, b) -> {
			String[] words = b == null ? new String[0] : b.toLowerCase(Locale.ROOT).trim().split("\\s+");
			shown.setPredicate(e -> {
				String l = (e.kind() + " " + e.label()).toLowerCase(Locale.ROOT);
				for (String w : words)
					if (!w.isEmpty() && !l.contains(w))
						return false;
				return true;
			});
			if (!shown.isEmpty())
				lv.getSelectionModel().select(0);
		});
		Runnable run = () -> {
			Entry e = lv.getSelectionModel().getSelectedItem();
			if (e == null && !shown.isEmpty())
				e = shown.get(0);
			if (e != null) {
				st.close();
				Entry fe = e;
				javafx.application.Platform.runLater(() -> fe.action().run());
			}
		};
		q.setOnKeyPressed(ev -> {
			if (ev.getCode() == KeyCode.DOWN) {
				lv.getSelectionModel().selectNext();
				lv.scrollTo(lv.getSelectionModel().getSelectedIndex());
				ev.consume();
			} else if (ev.getCode() == KeyCode.UP) {
				lv.getSelectionModel().selectPrevious();
				lv.scrollTo(lv.getSelectionModel().getSelectedIndex());
				ev.consume();
			} else if (ev.getCode() == KeyCode.ENTER) {
				run.run();
				ev.consume();
			} else if (ev.getCode() == KeyCode.ESCAPE) {
				st.close();
				ev.consume();
			}
		});
		lv.setOnMouseClicked(ev -> {
			if (ev.getClickCount() == 2)
				run.run();
		});
		lv.setOnKeyPressed(ev -> {
			if (ev.getCode() == KeyCode.ENTER)
				run.run();
			else if (ev.getCode() == KeyCode.ESCAPE)
				st.close();
		});
		lv.getSelectionModel().select(0);
		VBox box = new VBox(6, q, lv);
		box.setPadding(new Insets(8));
		Scene sc = new Scene(box, 620, 420);
		st.setScene(sc);
		UiUtil.theme(st, sc);
		st.focusedProperty().addListener((o, a, b) -> {
			if (!b && st.isShowing())
				st.close();
		});
		open = st;
		st.show();
		q.requestFocus();
	}

	private static void collect(Menu m, String path, List<Entry> out) {
		for (MenuItem it : m.getItems()) {
			if (it instanceof SeparatorMenuItem || it.isDisable() && !(it instanceof Menu))
				continue;
			String t = it.getText() == null ? "" : it.getText().replace("_", "");
			if (it instanceof Menu sub)
				collect(sub, path + " › " + t, out);
			else if (it.getOnAction() != null && !t.isBlank() && !t.startsWith("Command palette")) {
				String acc = it.getAccelerator() == null ? "" : "   (" + it.getAccelerator().getDisplayText() + ")";
				out.add(new Entry(path + " › " + t + acc, "command", it::fire));
			}
		}
	}

	private static void tables(GUI gui, TreeItem<fqlite.ui.NodeObject> item, String path, List<Entry> out, int depth) {
		if (depth > 4)
			return;
		for (TreeItem<fqlite.ui.NodeObject> c : item.getChildren()) {
			fqlite.ui.NodeObject n = c.getValue();
			if (n == null)
				continue;
			String p = path.isEmpty() ? n.name : path + " › " + n.name;
			String kind = depth == 0 ? "database" : n.isTable ? "table" : n.tabletype == 1 ? "index" : n.tabletype == 4 ? "trigger" : "node";
			out.add(new Entry(p, kind, () -> gui.selectTreeItem(c)));
			if (!c.getChildren().isEmpty())
				tables(gui, c, p, out, depth + 1);
		}
	}
}
