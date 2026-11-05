package fqlite.ui;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import fqlite.base.GUI;
import fqlite.base.Job;
import fqlite.forensics.Evidence;
import fqlite.forensics.IndexCarver;
import fqlite.forensics.RowHistory;
import fqlite.forensics.RowidGaps;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.SplitPane;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.TextInputDialog;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.geometry.Orientation;
import javafx.stage.Stage;

/**
 * Forensic analysis of one database: changed rows (row history), missing row numbers,
 * deleted index entries and the evidence integrity (hashes at open, verification).
 */
public class ForensicsWindow {

	private final Stage stage = new Stage();
	private final GUI gui;
	private final Job job;
	private final String name;

	public ForensicsWindow(GUI gui, Job job) {
		this.gui = gui;
		this.job = job;
		this.name = new File(job.path).getName();
		stage.setTitle("Forensic analysis \u2014 " + name);
		TabPane tabs = new TabPane(changedTab(), gapsTab(), indexTab(), evidenceTab());
		tabs.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
		Scene sc = new Scene(tabs, 1150, 620);
		stage.setScene(sc);
		UiUtil.theme(stage, sc);
	}

	public void show() {
		stage.show();
	}

	private static VBox box(javafx.scene.Node... n) {
		VBox b = new VBox(8, n);
		b.setPadding(new Insets(10));
		return b;
	}

	private static HBox bar(javafx.scene.Node... n) {
		HBox h = new HBox(8, n);
		return h;
	}

	private static Region spacer() {
		Region r = new Region();
		HBox.setHgrow(r, Priority.ALWAYS);
		return r;
	}

	/** runs work on a background thread and hands the result to the FX thread */
	private static <T> void background(Label status, String what, java.util.concurrent.Callable<T> work, java.util.function.Consumer<T> done) {
		status.setText(what + " \u2026");
		Thread t = new Thread(() -> {
			try {
				T r = work.call();
				Platform.runLater(() -> done.accept(r));
			} catch (Exception e) {
				Platform.runLater(() -> status.setText(what + " failed: " + e));
			}
		}, "forensics");
		t.setDaemon(true);
		t.start();
	}

	// ------------------------------------------------------------------ changed rows

	private Tab changedTab() {
		ObservableList<RowHistory.Change> data = FXCollections.observableArrayList();
		FilteredList<RowHistory.Change> filtered = new FilteredList<>(data, x -> true);
		TableView<RowHistory.Change> tv = new TableView<>(filtered);
		tv.getColumns().add(UiUtil.col("Table", RowHistory.Change::table, 160));
		tv.getColumns().add(UiUtil.num("rowid", RowHistory.Change::rowid, 80));
		tv.getColumns().add(UiUtil.col("Change", RowHistory.Change::kind, 110));
		tv.getColumns().add(UiUtil.num("Versions", RowHistory.Change::versions, 70));
		tv.getColumns().add(UiUtil.col("Found in", RowHistory.Change::sources, 170));
		tv.getColumns().add(UiUtil.col("Details", RowHistory.Change::summary, 480));
		VBox.setVgrow(tv, Priority.ALWAYS);
		Label status = new Label();
		ComboBox<String> kind = new ComboBox<>(FXCollections.observableArrayList("all changes", "updated", "deleted", "rowid reused"));
		kind.getSelectionModel().select(0);
		TextField find = new TextField();
		find.setPromptText("filter (table, values ...)");
		Runnable apply = () -> {
			String k = kind.getValue(), f = find.getText() == null ? "" : find.getText().toLowerCase();
			filtered.setPredicate(c -> (k == null || k.startsWith("all") || c.kind().equals(k))
					&& (f.isEmpty() || (c.table() + " " + c.rowid() + " " + c.summary()).toLowerCase().contains(f)));
			status.setText(filtered.size() + " of " + data.size() + " row(s)");
		};
		kind.setOnAction(e -> apply.run());
		find.textProperty().addListener((o, a, b) -> apply.run());
		tv.setOnMouseClicked(e -> {
			RowHistory.Change c = tv.getSelectionModel().getSelectedItem();
			if (e.getClickCount() == 2 && c != null)
				new RowHistoryWindow(gui, job, c.table(), c.rowid()).show();
		});
		Label unlinked = UiUtil.hint("");
		Button export = new Button("Export CSV...");
		export.setOnAction(e -> {
			List<List<String>> rows = new ArrayList<>();
			for (RowHistory.Change c : filtered)
				rows.add(List.of(c.table(), String.valueOf(c.rowid()), c.kind(), String.valueOf(c.versions()), c.sources(), c.summary()));
			UiUtil.exportCsv(stage, name + "-changed-rows", List.of("table", "rowid", "change", "versions", "found in", "details"),
					rows, "changed rows", name, List.of(job.path));
		});
		background(status, "Comparing row versions", () -> new Object[] { RowHistory.changedRows(job), RowHistory.recordsWithoutRowid(job) },
				r -> {
					@SuppressWarnings("unchecked")
					List<RowHistory.Change> l = (List<RowHistory.Change>) r[0];
					@SuppressWarnings("unchecked")
					Map<String, Integer> w = (Map<String, Integer>) r[1];
					data.setAll(l);
					apply.run();
					int sum = w.values().stream().mapToInt(Integer::intValue).sum();
					unlinked.setText(sum == 0 ? "" : sum + " recovered record(s) carry no row number and could not be matched to a row by content "
							+ "(" + w + ") - they are listed in the table views only.");
				});
		VBox b = box(UiUtil.hint("Rows with more than one version (UPDATE), rows that no longer exist (DELETE) and rowids used by a row "
				+ "with completely different values (reused). Double-click a row for its full history."),
				bar(kind, find, spacer(), status, export), tv, unlinked);
		return new Tab("Changed rows", b);
	}

	// ------------------------------------------------------------------ missing rowids

	private Tab gapsTab() {
		ObservableList<RowidGaps.TableSummary> sums = FXCollections.observableArrayList();
		ObservableList<RowidGaps.Gap> gaps = FXCollections.observableArrayList();
		TableView<RowidGaps.TableSummary> ts = new TableView<>(sums);
		ts.getColumns().add(UiUtil.col("Table", RowidGaps.TableSummary::table, 180));
		ts.getColumns().add(UiUtil.num("Current rows", RowidGaps.TableSummary::currentRows, 100));
		ts.getColumns().add(UiUtil.num("Max rowid", RowidGaps.TableSummary::maxRowid, 90));
		ts.getColumns().add(UiUtil.col("sqlite_sequence", t -> t.sequence() < 0 ? "" : String.valueOf(t.sequence()), 110));
		ts.getColumns().add(UiUtil.num("Missing", RowidGaps.TableSummary::missing, 80));
		ts.getColumns().add(UiUtil.num("of them recovered", RowidGaps.TableSummary::missingRecovered, 120));
		ts.getColumns().add(UiUtil.col("Note", RowidGaps.TableSummary::note, 360));
		FilteredList<RowidGaps.Gap> fg = new FilteredList<>(gaps, x -> true);
		TableView<RowidGaps.Gap> tg = new TableView<>(fg);
		tg.getColumns().add(UiUtil.col("Table", RowidGaps.Gap::table, 180));
		tg.getColumns().add(UiUtil.num("from rowid", RowidGaps.Gap::from, 90));
		tg.getColumns().add(UiUtil.num("to rowid", RowidGaps.Gap::to, 90));
		tg.getColumns().add(UiUtil.num("Count", RowidGaps.Gap::count, 70));
		tg.getColumns().add(UiUtil.col("Kind", RowidGaps.Gap::reason, 220));
		tg.getColumns().add(UiUtil.col("Recovered from", g -> g.recovered().isEmpty() ? "\u2014 nothing left" : g.recovered(), 360));
		ts.getSelectionModel().selectedItemProperty().addListener((o, a, b) -> fg.setPredicate(g -> b == null || g.table().equals(b.table())));
		tg.setOnMouseClicked(e -> {
			RowidGaps.Gap g = tg.getSelectionModel().getSelectedItem();
			if (e.getClickCount() == 2 && g != null && !g.recovered().isEmpty())
				new RowHistoryWindow(gui, job, g.table(), g.from()).show();
		});
		SplitPane sp = new SplitPane(ts, tg);
		sp.setOrientation(Orientation.VERTICAL);
		sp.setDividerPositions(0.35);
		VBox.setVgrow(sp, Priority.ALWAYS);
		Label status = new Label();
		Button export = new Button("Export CSV...");
		export.setOnAction(e -> {
			List<List<String>> rows = new ArrayList<>();
			for (RowidGaps.Gap g : fg)
				rows.add(List.of(g.table(), String.valueOf(g.from()), String.valueOf(g.to()), String.valueOf(g.count()), g.reason(), g.recovered()));
			UiUtil.exportCsv(stage, name + "-missing-rowids", List.of("table", "from", "to", "count", "kind", "recovered from"), rows,
					"missing row numbers", name, List.of(job.path));
		});
		background(status, "Looking for missing row numbers", () -> {
			List<RowidGaps.TableSummary> s = new ArrayList<>();
			List<RowidGaps.Gap> g = RowidGaps.analyse(job, s);
			return new Object[] { s, g };
		}, r -> {
			@SuppressWarnings("unchecked")
			List<RowidGaps.TableSummary> s = (List<RowidGaps.TableSummary>) r[0];
			@SuppressWarnings("unchecked")
			List<RowidGaps.Gap> g = (List<RowidGaps.Gap>) r[1];
			sums.setAll(s);
			gaps.setAll(g);
			long miss = s.stream().mapToLong(RowidGaps.TableSummary::missing).sum();
			status.setText(miss + " missing row number(s) in " + s.size() + " table(s)");
		});
		VBox b = box(UiUtil.hint("Row numbers below the highest current rowid that no row has, and (AUTOINCREMENT) the numbers up to the "
				+ "value in sqlite_sequence: deleted rows, even when nothing of them can be recovered. Note: rowids need not be "
				+ "consecutive (explicit ids, random rowids), so a gap is an indication, not proof. Select a table to see its gaps."),
				bar(spacer(), status, export), sp);
		return new Tab("Missing row numbers", b);
	}

	// ------------------------------------------------------------------ index entries

	private Tab indexTab() {
		ObservableList<IndexCarver.Entry> data = FXCollections.observableArrayList();
		TableView<IndexCarver.Entry> tv = new TableView<>(data);
		tv.getColumns().add(UiUtil.col("Index", IndexCarver.Entry::index, 160));
		tv.getColumns().add(UiUtil.col("Table", IndexCarver.Entry::table, 120));
		tv.getColumns().add(UiUtil.col("Indexed values", e -> {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < e.values().size() - 1; i++)
				sb.append(i > 0 ? " | " : "").append(i < e.columns().size() ? e.columns().get(i) + "=" : "").append(e.values().get(i));
			return sb.toString();
		}, 320));
		tv.getColumns().add(UiUtil.num("rowid", IndexCarver.Entry::rowid, 70));
		tv.getColumns().add(UiUtil.col("Row", IndexCarver.Entry::rowState, 270));
		tv.getColumns().add(UiUtil.col("Confidence", IndexCarver.Entry::confidence, 170));
		tv.getColumns().add(UiUtil.col("Found in", IndexCarver.Entry::where, 170));
		tv.getColumns().add(UiUtil.num("File offset", IndexCarver.Entry::offset, 90));
		VBox.setVgrow(tv, Priority.ALWAYS);
		tv.setOnMouseClicked(e -> {
			IndexCarver.Entry x = tv.getSelectionModel().getSelectedItem();
			if (e.getClickCount() == 2 && x != null && !x.table().isEmpty() && x.rowid() > 0)
				new RowHistoryWindow(gui, job, x.table(), x.rowid()).show();
		});
		Label status = new Label();
		Button export = new Button("Export CSV...");
		export.setOnAction(e -> {
			List<List<String>> rows = new ArrayList<>();
			for (IndexCarver.Entry x : data)
				rows.add(List.of(x.index(), x.table(), String.join(" | ", x.values().subList(0, x.values().size() - 1)),
						String.valueOf(x.rowid()), x.rowState(), x.confidence(), x.where(), String.valueOf(x.offset())));
			UiUtil.exportCsv(stage, name + "-deleted-index-entries",
					List.of("index", "table", "indexed values", "rowid", "row", "confidence", "found in", "file offset"), rows,
					"deleted index entries", name, List.of(job.path));
		});
		background(status, "Carving index pages", () -> IndexCarver.carve(job), l -> {
			data.setAll(l);
			long none = l.stream().filter(x -> x.rowState().contains("no other trace")).count();
			status.setText(l.size() + " deleted index entr" + (l.size() == 1 ? "y" : "ies") + (none > 0 ? ", " + none + " of a row with no other trace" : ""));
		});
		VBox b = box(UiUtil.hint("Entries of indexes (indexed values + rowid) carved from freeblocks, the unallocated area of index pages and "
				+ "freed index pages. Entries identical to a live entry are left out. They often survive after the row itself was "
				+ "overwritten (\"no other trace\"). Freeblock entries are reconstructed (medium confidence) and only listed when the "
				+ "reconstruction is unique. Double-click for the row history."), bar(spacer(), status, export), tv);
		return new Tab("Deleted index entries", b);
	}

	// ------------------------------------------------------------------ evidence

	private record EvRow(String path, String size, String modified, String sha256, String check) {
	}

	private Tab evidenceTab() {
		ObservableList<EvRow> data = FXCollections.observableArrayList();
		TableView<EvRow> tv = new TableView<>(data);
		tv.getColumns().add(UiUtil.col("File", EvRow::path, 380));
		tv.getColumns().add(UiUtil.col("Size", EvRow::size, 90));
		tv.getColumns().add(UiUtil.col("Modified (UTC)", EvRow::modified, 170));
		tv.getColumns().add(UiUtil.col("SHA-256 at open", EvRow::sha256, 460));
		tv.getColumns().add(UiUtil.col("Verification", EvRow::check, 260));
		VBox.setVgrow(tv, Priority.ALWAYS);
		Label status = new Label();
		Runnable load = () -> background(status, "Hashing the evidence", () -> Evidence.get(job.path), l -> {
			data.clear();
			for (Evidence.EvidenceFile f : l)
				data.add(new EvRow(f.path(), String.valueOf(f.size()), Instant.ofEpochMilli(f.mtime()).toString(), f.sha256(), ""));
			status.setText(l.size() + " evidence file(s), hashed when the database was opened");
		});
		Button verify = new Button("Verify now");
		verify.setOnAction(e -> background(status, "Verifying", () -> Evidence.verify(job.path), l -> {
			for (int i = 0; i < data.size(); i++) {
				EvRow r = data.get(i);
				for (Evidence.Check c : l)
					if (c.path().equals(r.path()))
						data.set(i, new EvRow(r.path(), r.size(), r.modified(), r.sha256(), c.unchanged() ? "unchanged" + (c.mtimeSame() ? "" : " (mtime changed)")
								: (c.exists() ? "CHANGED: " + c.sha256Now() : "MISSING")));
			}
			long bad = l.stream().filter(c -> !c.unchanged()).count();
			status.setText(bad == 0 ? "verified: all evidence files unchanged" : bad + " evidence file(s) CHANGED");
		}));
		Button compare = new Button("Compare with expected hash...");
		compare.setOnAction(e -> {
			TextInputDialog d = new TextInputDialog();
			d.setTitle("Compare with expected hash");
			d.setHeaderText("SHA-256 of the acquisition (paste one hash, or lines of a sha256sum list)");
			d.showAndWait().ifPresent(text -> {
				for (int i = 0; i < data.size(); i++) {
					EvRow r = data.get(i);
					String verdict = "not in the list";
					for (String line : text.split("\\R")) {
						String h = line.trim().split("\\s+")[0].toLowerCase();
						if (h.length() != 64)
							continue;
						boolean named = line.contains(new File(r.path()).getName());
						if (h.equals(r.sha256()))
							verdict = "MATCHES expected hash";
						else if (named || text.split("\\R").length == 1 && i == 0)
							verdict = "MISMATCH with expected hash";
					}
					data.set(i, new EvRow(r.path(), r.size(), r.modified(), r.sha256(), verdict));
				}
			});
		});
		load.run();
		VBox b = box(UiUtil.hint("The database and its -wal / -journal / -shm files are hashed (SHA-256) when the database is opened and "
				+ "verified again when it is closed. Every export writes <file>.manifest.json with the export's and the evidence's hashes."),
				bar(verify, compare, spacer(), status), tv);
		return new Tab("Evidence", b);
	}
}
