package fqlite.ui;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import fqlite.base.Job;
import fqlite.base.Limits;
import javafx.application.Platform;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.cell.CheckBoxTableCell;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import javafx.stage.Window;

/**
 * Samples the values of one column, decodes them (compression, typedstream, plist,
 * NSKeyedArchiver, protobuf, JSON) and lists the fields found; the selected fields are added
 * to the table as additional, read-only columns.
 */
public class DecodeColumnDialog {

	private final Stage stage = new Stage();

	/** one field path with statistics */
	public static final class PathStat {
		final String path;
		int rows;
		String example = "";
		final SimpleBooleanProperty selected = new SimpleBooleanProperty(false);

		PathStat(String path) {
			this.path = path;
		}
	}

	@SuppressWarnings("rawtypes")
	public DecodeColumnDialog(Window owner, TableView table, Job job, TableColumn src) {
		String colName = DerivedColumns.title(src);
		stage.setTitle("Decode column — " + colName);
		if (owner != null)
			stage.initOwner(owner);

		Label head = new Label("Column \"" + colName + "\": sampling rows…");
		head.setStyle("-fx-font-weight: bold;");
		Label formats = new Label("");
		formats.setWrapText(true);
		ProgressBar progress = new ProgressBar(0);
		progress.setMaxWidth(Double.MAX_VALUE);

		ObservableList<PathStat> all = FXCollections.observableArrayList();
		FilteredList<PathStat> shown = new FilteredList<>(all, p -> true);
		TableView<PathStat> tv = new TableView<>(shown);
		tv.setEditable(true);
		TableColumn<PathStat, Boolean> sel = new TableColumn<>("");
		sel.setCellValueFactory(p -> p.getValue().selected);
		sel.setCellFactory(CheckBoxTableCell.forTableColumn(sel));
		sel.setEditable(true);
		sel.setPrefWidth(32);
		tv.getColumns().add(sel);
		tv.getColumns().add(UiUtil.col("Field", p -> p.path, 300));
		tv.getColumns().add(UiUtil.num("Rows", p -> p.rows, 70));
		tv.getColumns().add(UiUtil.col("Example", p -> p.example, 420));
		VBox.setVgrow(tv, Priority.ALWAYS);
		tv.setOnMouseClicked(e -> {
			if (e.getClickCount() == 2 && tv.getSelectionModel().getSelectedItem() != null) {
				PathStat p = tv.getSelectionModel().getSelectedItem();
				p.selected.set(!p.selected.get());
			}
		});

		TextField filter = new TextField();
		filter.setPromptText("Filter fields");
		filter.textProperty().addListener((o, a, b) -> {
			String f = b == null ? "" : b.toLowerCase();
			shown.setPredicate(p -> f.isEmpty() || p.path.toLowerCase().contains(f) || p.example.toLowerCase().contains(f));
		});
		HBox.setHgrow(filter, Priority.ALWAYS);
		Button selAll = new Button("Select shown");
		selAll.setOnAction(e -> shown.forEach(p -> p.selected.set(true)));
		Button selNone = new Button("Select none");
		selNone.setOnAction(e -> all.forEach(p -> p.selected.set(false)));
		HBox top = new HBox(8, filter, selAll, selNone);

		AtomicBoolean stop = new AtomicBoolean();
		Button add = new Button("Add selected as columns");
		add.setDefaultButton(true);
		add.setDisable(true);
		add.setOnAction(e -> {
			List<String> paths = new ArrayList<>();
			for (PathStat p : all)
				if (p.selected.get())
					paths.add(p.path);
			if (!paths.isEmpty())
				DerivedColumns.add(table, job, src, paths);
			stage.close();
		});
		Button close = new Button("Close");
		close.setCancelButton(true);
		close.setOnAction(e -> stage.close());
		Label hint = UiUtil.hint("The columns are computed for display only; the database is not changed. Compressed content "
				+ "(gzip, zlib, zstd, LZ4, LZFSE/LZVN) is unpacked first, NSKeyedArchiver archives are resolved, embedded "
				+ "plists/archives are decoded recursively. Limits: Settings → Display & limits.");
		HBox.setHgrow(hint, Priority.ALWAYS);
		add.setMinWidth(Region.USE_PREF_SIZE);
		close.setMinWidth(Region.USE_PREF_SIZE);
		HBox bar = new HBox(8, hint, add, close);

		VBox root = new VBox(8, head, progress, formats, top, tv, bar);
		root.setPadding(new Insets(10));
		Scene sc = new Scene(root, 960, 560);
		stage.setScene(sc);
		UiUtil.theme(stage, sc);
		stage.setOnHidden(e -> stop.set(true));

		// ---- sample in the background
		@SuppressWarnings("unchecked")
		List<ObservableList<String>> rows = new ArrayList<>((List<ObservableList<String>>) table.getItems());
		int n = Math.min(rows.size(), Limits.FIELD_SCAN_ROWS);
		Thread t = new Thread(() -> {
			Map<String, PathStat> stats = new LinkedHashMap<>();
			Map<String, Integer> fmt = new TreeMap<>();
			int empty = 0;
			for (int i = 0; i < n && !stop.get(); i++) {
				Map<String, String> f;
				try {
					f = DerivedColumns.decodeCell(DerivedColumns.cellBytes(job, src, rows.get(i)));
				} catch (RuntimeException | StackOverflowError ex) {
					f = Map.of(DerivedColumns.FORMAT, "error");
				}
				if (f.isEmpty())
					empty++;
				for (Map.Entry<String, String> en : f.entrySet()) {
					if (en.getKey().equals(DerivedColumns.FORMAT)) {
						fmt.merge(en.getValue().replaceAll(" \\(.*", ""), 1, Integer::sum);
						continue;
					}
					PathStat ps = stats.computeIfAbsent(en.getKey(), PathStat::new);
					ps.rows++;
					if (ps.example.isEmpty() && en.getValue() != null && !en.getValue().isEmpty())
						ps.example = en.getValue().length() > 200 ? en.getValue().substring(0, 200) + "…" : en.getValue();
				}
				if (i % 50 == 0) {
					double pr = (double) i / Math.max(1, n);
					Platform.runLater(() -> progress.setProgress(pr));
				}
			}
			int emptyRows = empty;
			Platform.runLater(() -> {
				progress.setProgress(1);
				List<PathStat> l = new ArrayList<>(stats.values());
				PathStat fmtCol = new PathStat(DerivedColumns.FORMAT);
				fmtCol.rows = n - emptyRows;
				fmtCol.example = fmt.isEmpty() ? "" : fmt.keySet().iterator().next();
				l.add(0, fmtCol);
				boolean structured = l.size() > 2;
				for (PathStat p : l)
					if (p.path.equals(DerivedColumns.SUMMARY))
						p.selected.set(true);
				// pre-select fields that occur in most rows, if there are few of them
				if (structured) {
					int many = 0;
					for (PathStat p : l)
						if (!p.path.startsWith("(") && p.rows >= 0.5 * (n - emptyRows))
							many++;
					if (many <= 8)
						for (PathStat p : l)
							if (!p.path.startsWith("(") && p.rows >= 0.5 * (n - emptyRows))
								p.selected.set(true);
				}
				all.setAll(l);
				StringBuilder sb = new StringBuilder();
				fmt.forEach((k, v) -> sb.append(sb.length() > 0 ? ";  " : "").append(k).append(": ").append(v));
				formats.setText("Content found: " + (sb.length() == 0 ? "-" : sb) + (emptyRows > 0 ? ";  empty: " + emptyRows : ""));
				head.setText("Column \"" + colName + "\": " + (stats.size()) + " field(s) in " + n + " sampled row(s)"
						+ (rows.size() > n ? " of " + rows.size() : ""));
				add.setDisable(false);
			});
		}, "decode-column");
		t.setDaemon(true);
		t.start();
	}

	public void show() {
		stage.show();
	}
}
