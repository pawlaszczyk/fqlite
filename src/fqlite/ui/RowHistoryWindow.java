package fqlite.ui;

import java.util.ArrayList;
import java.util.List;

import fqlite.base.GUI;
import fqlite.base.Job;
import fqlite.forensics.RowHistory;
import fqlite.forensics.RowHistory.Version;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableRow;
import javafx.scene.control.TableView;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

/**
 * Every version of one row (database file, WAL, rollback journal, carved records) in time
 * order; values that differ from the previous version are highlighted, the current version is
 * marked with a dot.
 */
public class RowHistoryWindow {

	private final Stage stage = new Stage();

	public RowHistoryWindow(GUI gui, Job job, String table, long rowid) {
		List<Version> versions = RowHistory.history(job, table, rowid);
		List<String> cols = RowHistory.columnNames(job, table);
		stage.setTitle("Row history \u2014 " + table + ", rowid " + rowid);

		TableView<Version> tv = new TableView<>(FXCollections.observableArrayList(versions));
		tv.getColumns().add(UiUtil.col("", v -> v.current ? "\u25CF" : "", 28));
		tv.getColumns().add(UiUtil.col("#", v -> String.valueOf(versions.indexOf(v) + 1), 36));
		tv.getColumns().add(UiUtil.col("Source", v -> v.source.label, 80));
		tv.getColumns().add(UiUtil.col("State", v -> v.state, 190));
		tv.getColumns().add(UiUtil.col("Location", v -> v.location, 210));
		tv.getColumns().add(UiUtil.col("Flags", v -> v.flags, 220));
		int n = cols.size();
		for (Version v : versions)
			n = Math.max(n, v.values.size());
		for (int i = 0; i < n; i++) {
			final int k = i;
			TableColumn<Version, String> c = new TableColumn<>(k < cols.size() ? cols.get(k) : "column " + (k + 1));
			c.setCellValueFactory(p -> new SimpleStringProperty(k < p.getValue().values.size() ? p.getValue().values.get(k) : ""));
			c.setCellFactory(x -> new TableCell<>() {
				@Override
				protected void updateItem(String item, boolean empty) {
					super.updateItem(item, empty);
					setText(empty ? null : item);
					Version v = empty || getTableRow() == null ? null : getTableRow().getItem();
					boolean changed = v != null && v.changed.contains(k);
					setStyle(changed ? "-fx-font-weight: bold; -fx-background-color: rgba(245,158,11,0.35);" : "");
				}
			});
			c.setPrefWidth(140);
			tv.getColumns().add(c);
		}
		tv.setRowFactory(x -> new TableRow<>() {
			@Override
			protected void updateItem(Version v, boolean empty) {
				super.updateItem(v, empty);
				setStyle(!empty && v != null && v.current ? "-fx-font-weight: bold;" : "");
			}
		});
		VBox.setVgrow(tv, Priority.ALWAYS);

		Version cur = RowHistory.currentVersion(versions);
		String summary = versions.isEmpty() ? "No version of this row was found."
				: versions.size() + " version(s); " + (cur == null ? "the row no longer exists (deleted)" : "current: " + cur.source.label + ", " + cur.location);
		Label head = new Label(table + ", rowid " + rowid + " \u2014 " + summary);
		head.setStyle("-fx-font-weight: bold;");

		Button go = new Button("Show in table");
		go.setOnAction(e -> {
			Version v = tv.getSelectionModel().getSelectedItem();
			if (v != null)
				gui.navigateToRow(job, v.source == RowHistory.Source.WAL ? 1 : v.source == RowHistory.Source.JOURNAL ? 2 : 0, table, v.row);
		});
		tv.setOnMouseClicked(e -> {
			if (e.getClickCount() == 2)
				go.fire();
		});
		Button export = new Button("Export CSV...");
		export.setOnAction(e -> {
			List<String> h = new ArrayList<>(List.of("version", "current", "source", "state", "location", "flags", "changed columns"));
			for (int i = 0; i < tv.getColumns().size() - 6; i++)
				h.add(tv.getColumns().get(6 + i).getText());
			List<List<String>> rows = new ArrayList<>();
			for (Version v : versions) {
				List<String> r = new ArrayList<>();
				r.add(String.valueOf(versions.indexOf(v) + 1));
				r.add(v.current ? "yes" : "");
				r.add(v.source.label);
				r.add(v.state);
				r.add(v.location);
				r.add(v.flags);
				List<String> ch = new ArrayList<>();
				for (int k : v.changed)
					ch.add(k < cols.size() ? cols.get(k) : "column " + (k + 1));
				r.add(String.join(" ", ch));
				r.addAll(v.values);
				rows.add(r);
			}
			UiUtil.exportCsv(stage, "row-history-" + table + "-" + rowid, h, rows, "row history", table + " rowid " + rowid,
					List.of(job.path));
		});
		go.setMinWidth(Region.USE_PREF_SIZE);
		export.setMinWidth(Region.USE_PREF_SIZE);
		Label hint = UiUtil.hint("Highlighted: value changed against the previous version.  \u25CF current version.  "
				+ "Order: recovered records, journal (before the last transaction), database file, WAL frames - the order of "
				+ "recovered records among themselves is not known.");
		HBox.setHgrow(hint, Priority.ALWAYS);
		HBox bar = new HBox(8, hint, go, export);
		VBox root = new VBox(8, head, tv, bar);
		root.setPadding(new Insets(10));
		Scene sc = new Scene(root, 1100, 420);
		stage.setScene(sc);
		UiUtil.theme(stage, sc);
	}

	public void show() {
		stage.show();
	}
}
