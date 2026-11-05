package fqlite.ui;

import java.util.ArrayList;
import java.util.List;

import fqlite.base.GUI;
import fqlite.base.Global;
import fqlite.base.Job;
import fqlite.forensics.CaseSearch;
import fqlite.forensics.RowLayout;
import fqlite.forensics.TriggerExplainer;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

/** all triggers of the open databases - also deleted ones recovered from free space - in plain English */
public final class TriggerWindow {

	private TriggerWindow() {
	}

	/** one trigger */
	public record Item(String db, String status, String sql, TriggerExplainer.Explanation ex, List<String> paths) {
	}

	public static List<Item> collect(List<CaseSearch.Target> dbs) {
		List<Item> l = new ArrayList<>();
		for (CaseSearch.Target t : dbs) {
			Job job = t.job();
			if (job == null || job.resultlist == null)
				continue;
			ObservableList<ObservableList<String>> rows = job.resultlist.get("sqlite_master");
			if (rows == null)
				continue;
			List<String> seen = new ArrayList<>();
			for (ObservableList<String> r : List.copyOf(rows)) {
				List<String> v = RowLayout.values(r);
				if (v.size() < 5 || !"trigger".equalsIgnoreCase(v.get(0)))
					continue;
				String sql = v.get(4);
				String st = RowLayout.status(r);
				String status = Global.REGULAR_RECORD.equals(st) ? "live" : "recovered (" + (st == null || st.isEmpty() ? "deleted" : st) + ")";
				String key = status + "|" + sql;
				if (seen.contains(key))
					continue;
				seen.add(key);
				l.add(new Item(t.name(), status, sql, TriggerExplainer.explain(sql), List.of(job.path)));
			}
		}
		return l;
	}

	public static void show(GUI gui) {
		List<Item> items = collect(gui.openDatabases());
		Stage st = new Stage();
		st.initOwner(gui.getStage());
		st.setTitle("Triggers explained");
		TableView<Item> tv = new TableView<>(FXCollections.observableArrayList(items));
		tv.getColumns().add(UiUtil.col("Database", i -> i.db(), 160));
		tv.getColumns().add(UiUtil.col("Trigger", i -> i.ex().name(), 170));
		tv.getColumns().add(UiUtil.col("Table", i -> i.ex().table(), 130));
		tv.getColumns().add(UiUtil.col("Fires", i -> (i.ex().timing() + " " + i.ex().event()).strip(), 150));
		tv.getColumns().add(UiUtil.col("Status", i -> i.status(), 130));
		tv.getColumns().add(UiUtil.col("Effect", i -> i.ex().actions().isEmpty() ? "" : String.join("; ", i.ex().actions()), 420));
		TextArea detail = new TextArea();
		detail.setEditable(false);
		detail.setWrapText(true);
		detail.setStyle("-fx-font-family: monospace;");
		tv.getSelectionModel().selectedItemProperty().addListener((o, a, b) -> detail.setText(b == null ? ""
				: b.ex().text() + "\n\n" + (b.status().equals("live") ? "" : "Status: " + b.status() + " - this trigger "
						+ "no longer exists in the current schema.\n\n") + "SQL:\n" + b.sql()));
		if (!items.isEmpty())
			tv.getSelectionModel().select(0);
		SplitPane sp = new SplitPane(tv, detail);
		sp.setOrientation(Orientation.VERTICAL);
		sp.setDividerPositions(0.45);
		VBox.setVgrow(sp, Priority.ALWAYS);
		Label head = new Label(items.isEmpty() ? "No triggers in the open databases." : items.size() + " trigger(s)");
		head.setStyle("-fx-font-weight: bold;");
		Button export = new Button("Export CSV...");
		export.setMinWidth(Region.USE_PREF_SIZE);
		export.setOnAction(e -> {
			List<List<String>> rows = new ArrayList<>();
			List<String> paths = new ArrayList<>();
			for (Item i : items) {
				rows.add(List.of(i.db(), i.ex().name(), i.ex().table(), (i.ex().timing() + " " + i.ex().event()).strip(), i.status(),
						i.ex().text(), i.sql()));
				for (String p : i.paths())
					if (!paths.contains(p))
						paths.add(p);
			}
			UiUtil.exportCsv(st, "triggers", List.of("database", "trigger", "table", "fires", "status", "explanation", "sql"), rows,
					"triggers", "all open databases", paths);
		});
		Label hint = UiUtil.hint("Triggers make SQLite change data on its own. Deletions or updates caused by a trigger leave the same "
				+ "traces as manual ones; history tables filled by triggers can hold earlier content. Recovered (deleted) schema "
				+ "entries are included.");
		HBox.setHgrow(hint, Priority.ALWAYS);
		HBox bar = new HBox(8, hint, export);
		VBox root = new VBox(8, head, sp, bar);
		root.setPadding(new Insets(10));
		Scene sc = new Scene(root, 1150, 620);
		st.setScene(sc);
		UiUtil.theme(st, sc);
		st.show();
	}
}
