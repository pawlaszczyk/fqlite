package fqlite.ui;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import fqlite.base.GUI;
import fqlite.base.Global;
import fqlite.base.Job;
import fqlite.forensics.CaseScanner;
import fqlite.forensics.CaseSearch;
import fqlite.forensics.Evidence;
import fqlite.forensics.RowLayout;
import javafx.application.Platform;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.cell.CheckBoxTableCell;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.DirectoryChooser;
import javafx.stage.Stage;

/**
 * A case of several databases: scan a folder (an extraction) for SQLite databases, open the
 * chosen ones one after the other, an overview of every open database and a search over all of
 * them (live, recovered, WAL and journal rows).
 */
public class CaseWindow {

	private static CaseWindow instance;

	private final Stage stage = new Stage();
	private final GUI gui;
	private final TabPane tabs;
	private final ObservableList<OverviewRow> overview = FXCollections.observableArrayList();

	public static void show(GUI gui, int tab) {
		if (instance == null)
			instance = new CaseWindow(gui);
		instance.tabs.getSelectionModel().select(tab);
		instance.refreshOverview();
		instance.stage.show();
		instance.stage.toFront();
	}

	private CaseWindow(GUI gui) {
		this.gui = gui;
		stage.setTitle("Case");
		tabs = new TabPane(scanTab(), overviewTab(), searchTab());
		tabs.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
		tabs.getSelectionModel().selectedIndexProperty().addListener((o, a, b) -> {
			if (b.intValue() == 1)
				refreshOverview();
		});
		Scene sc = new Scene(tabs, 1150, 640);
		stage.setScene(sc);
		UiUtil.theme(stage, sc);
	}

	private static Region spacer() {
		Region r = new Region();
		HBox.setHgrow(r, Priority.ALWAYS);
		return r;
	}

	// ------------------------------------------------------------------ scan

	public static final class FoundRow {
		final CaseScanner.Found f;
		final SimpleBooleanProperty selected = new SimpleBooleanProperty(true);
		String state = "";

		FoundRow(CaseScanner.Found f) {
			this.f = f;
		}
	}

	private Tab scanTab() {
		TextField folder = new TextField();
		folder.setPromptText("folder of the extraction");
		HBox.setHgrow(folder, Priority.ALWAYS);
		Button browse = new Button("Browse...");
		browse.setOnAction(e -> {
			DirectoryChooser dc = new DirectoryChooser();
			dc.setTitle("Folder with the databases");
			File d = dc.showDialog(stage);
			if (d != null)
				folder.setText(d.getAbsolutePath());
		});
		CheckBox sub = new CheckBox("include sub folders");
		sub.setSelected(true);
		TextField names = new TextField();
		names.setPromptText("file names, e.g.  *.db, *.sqlite, History, msgstore*   (empty = every file)");
		TextField exclude = new TextField();
		exclude.setPromptText("exclude, e.g.  *.bak, cache*");
		TextField minSize = new TextField("512");
		minSize.setPrefWidth(90);
		TextField maxSize = new TextField();
		maxSize.setPromptText("no limit");
		maxSize.setPrefWidth(90);
		GridPane g = new GridPane();
		g.setHgap(8);
		g.setVgap(6);
		g.addRow(0, new Label("Folder"), new HBox(8, folder, browse, sub));
		g.addRow(1, new Label("File names"), names);
		g.addRow(2, new Label("Exclude"), exclude);
		g.addRow(3, new Label("Size (bytes)"), new HBox(8, new Label("at least"), minSize, new Label("at most"), maxSize));
		GridPane.setHgrow(names, Priority.ALWAYS);

		ObservableList<FoundRow> found = FXCollections.observableArrayList();
		TableView<FoundRow> tv = new TableView<>(found);
		tv.setEditable(true);
		TableColumn<FoundRow, Boolean> use = new TableColumn<>("Open");
		use.setCellValueFactory(p -> p.getValue().selected);
		use.setCellFactory(CheckBoxTableCell.forTableColumn(use));
		use.setEditable(true);
		use.setPrefWidth(50);
		tv.getColumns().add(use);
		tv.getColumns().add(UiUtil.col("Database", r -> r.f.relativePath(), 520));
		tv.getColumns().add(UiUtil.num("Size", r -> r.f.size(), 100));
		tv.getColumns().add(UiUtil.col("WAL", r -> r.f.wal() ? "yes" : "", 50));
		tv.getColumns().add(UiUtil.col("Journal", r -> r.f.journal() ? "yes" : "", 60));
		tv.getColumns().add(UiUtil.col("State", r -> r.state, 200));
		VBox.setVgrow(tv, Priority.ALWAYS);

		Label status = new Label();
		AtomicBoolean stop = new AtomicBoolean();
		Button scan = new Button("Scan for databases");
		Button stopBtn = new Button("Stop");
		stopBtn.setOnAction(e -> stop.set(true));
		scan.setOnAction(e -> {
			File root = new File(folder.getText().trim());
			if (!root.isDirectory()) {
				UiUtil.error("Case", "Please choose a folder.");
				return;
			}
			CaseScanner.Conditions c = new CaseScanner.Conditions();
			c.recursive = sub.isSelected();
			c.namePatterns = names.getText();
			c.excludePatterns = exclude.getText();
			try {
				c.minSize = minSize.getText().isBlank() ? 0 : Long.parseLong(minSize.getText().trim());
				c.maxSize = maxSize.getText().isBlank() ? Long.MAX_VALUE : Long.parseLong(maxSize.getText().trim());
			} catch (NumberFormatException ex) {
				UiUtil.error("Case", "The sizes must be numbers (bytes).");
				return;
			}
			found.clear();
			stop.set(false);
			status.setText("scanning \u2026");
			Thread t = new Thread(() -> {
				try {
					List<CaseScanner.Found> l = CaseScanner.scan(root, c, f -> Platform.runLater(() -> found.add(new FoundRow(f))),
							p -> Platform.runLater(() -> status.setText(p.filesSeen() + " files seen, " + found.size() + " databases"
									+ (p.current() == null ? (stop.get() ? " (stopped)" : " - complete") : " \u2026"))),
							stop::get);
					Platform.runLater(() -> status.setText(l.size() + " database(s) found" + (stop.get() ? " (stopped)" : "")));
				} catch (Exception ex) {
					Platform.runLater(() -> status.setText("scan failed: " + ex.getMessage()));
				}
			}, "case-scan");
			t.setDaemon(true);
			t.start();
		});
		Button all = new Button("Select all");
		all.setOnAction(e -> found.forEach(r -> r.selected.set(true)));
		Button none = new Button("Select none");
		none.setOnAction(e -> found.forEach(r -> r.selected.set(false)));
		Button open = new Button("Open selected");
		open.setOnAction(e -> openSelected(found, tv, status, stop));
		HBox bar1 = new HBox(8, scan, stopBtn, spacer(), status);
		bar1.setAlignment(Pos.CENTER_LEFT);
		HBox bar2 = new HBox(8, all, none, spacer(), open);
		VBox b = new VBox(8, UiUtil.hint("Lists every file that starts with the SQLite header (only its first 16 bytes are read; "
				+ "-wal / -journal / -shm files are shown with their database). The chosen databases are opened one after the other; "
				+ "each is hashed (SHA-256) when it is opened."), g, bar1, tv, bar2);
		b.setPadding(new Insets(10));
		return new Tab("Open folder", b);
	}

	private void openSelected(ObservableList<FoundRow> found, TableView<FoundRow> tv, Label status, AtomicBoolean stop) {
		List<FoundRow> todo = new ArrayList<>();
		for (FoundRow r : found)
			if (r.selected.get())
				todo.add(r);
		if (todo.isEmpty())
			return;
		stop.set(false);
		Thread t = new Thread(() -> {
			int n = 0, ok = 0;
			for (FoundRow r : todo) {
				if (stop.get())
					break;
				n++;
				final int i = n;
				Platform.runLater(() -> {
					r.state = "opening \u2026";
					tv.refresh();
					status.setText("opening " + r.f.file().getName() + " (" + i + " of " + todo.size() + ") \u2026");
				});
				CompletableFuture<Job> opened = new CompletableFuture<>();
				Platform.runLater(() -> {
					try {
						opened.complete(gui.openForCase(r.f.file()));
					} catch (Exception ex) {
						opened.completeExceptionally(ex);
					}
				});
				String state;
				try {
					Job job = opened.get();
					if (job == null)
						state = "not opened (already open or not readable)";
					else {
						job.importDone.get(30, TimeUnit.MINUTES);
						state = "opened";
						ok++;
					}
				} catch (Exception ex) {
					state = "failed: " + ex.getMessage();
				}
				final String s = state;
				Platform.runLater(() -> {
					r.state = s;
					tv.refresh();
				});
			}
			final int done = ok;
			Platform.runLater(() -> {
				status.setText(done + " of " + todo.size() + " database(s) opened" + (stop.get() ? " (stopped)" : ""));
				refreshOverview();
			});
		}, "case-open");
		t.setDaemon(true);
		t.start();
	}

	// ------------------------------------------------------------------ overview

	public static final class OverviewRow {
		String name, path, sha256 = "\u2026";
		long size, tables, live, recovered, wal, journal;
		boolean hasWal, hasJournal;
	}

	private Tab overviewTab() {
		TableView<OverviewRow> tv = new TableView<>(overview);
		tv.getColumns().add(UiUtil.col("Database", r -> r.name, 180));
		tv.getColumns().add(UiUtil.num("Size", r -> r.size, 90));
		tv.getColumns().add(UiUtil.num("Tables", r -> r.tables, 60));
		tv.getColumns().add(UiUtil.num("Live rows", r -> r.live, 80));
		tv.getColumns().add(UiUtil.num("Recovered", r -> r.recovered, 80));
		tv.getColumns().add(UiUtil.col("WAL rows", r -> r.hasWal ? String.valueOf(r.wal) : "", 70));
		tv.getColumns().add(UiUtil.col("Journal rows", r -> r.hasJournal ? String.valueOf(r.journal) : "", 90));
		tv.getColumns().add(UiUtil.col("SHA-256 (at open)", r -> r.sha256, 440));
		tv.getColumns().add(UiUtil.col("Path", r -> r.path, 400));
		VBox.setVgrow(tv, Priority.ALWAYS);
		Button refresh = new Button("Refresh");
		refresh.setOnAction(e -> refreshOverview());
		Button forensic = new Button("Forensic analysis...");
		forensic.setOnAction(e -> {
			OverviewRow r = tv.getSelectionModel().getSelectedItem();
			if (r != null)
				for (CaseSearch.Target t : gui.openDatabases())
					if (t.job().path.equals(r.path))
						new ForensicsWindow(gui, t.job()).show();
		});
		Button timeline = new Button("Timeline of the case...");
		timeline.setOnAction(e -> new TimelineWindow(gui).show());
		Button export = new Button("Export CSV...");
		export.setOnAction(e -> {
			List<List<String>> rows = new ArrayList<>();
			List<String> paths = new ArrayList<>();
			for (OverviewRow r : overview) {
				rows.add(List.of(r.name, String.valueOf(r.size), String.valueOf(r.tables), String.valueOf(r.live), String.valueOf(r.recovered),
						r.hasWal ? String.valueOf(r.wal) : "", r.hasJournal ? String.valueOf(r.journal) : "", r.sha256, r.path));
				paths.add(r.path);
			}
			UiUtil.exportCsv(stage, "case-overview", List.of("database", "size", "tables", "live rows", "recovered", "WAL rows", "journal rows",
					"sha256", "path"), rows, "case overview", overview.size() + " databases", paths);
		});
		HBox bar = new HBox(8, refresh, forensic, timeline, spacer(), export);
		VBox b = new VBox(8, UiUtil.hint("Every open database. Recovered = deleted and freelist records of the database file."), bar, tv);
		b.setPadding(new Insets(10));
		return new Tab("Overview", b);
	}

	private void refreshOverview() {
		List<CaseSearch.Target> dbs = gui.openDatabases();
		List<OverviewRow> l = new ArrayList<>();
		for (CaseSearch.Target t : dbs) {
			Job job = t.job();
			OverviewRow r = new OverviewRow();
			r.name = t.name();
			r.path = job.path;
			r.size = new File(job.path).length();
			r.tables = job.headers == null ? 0 : job.headers.size();
			if (job.resultlist != null)
				for (ObservableList<ObservableList<String>> rows : job.resultlist.values())
					for (ObservableList<String> row : List.copyOf(rows))
						if (Global.REGULAR_RECORD.equals(RowLayout.status(row)))
							r.live++;
						else
							r.recovered++;
			r.hasWal = job.wal != null;
			if (job.wal != null && job.wal.resultlist != null)
				r.wal = job.wal.resultlist.values().stream().mapToLong(List::size).sum();
			r.hasJournal = job.rol != null;
			if (job.rol != null && job.rol.resultlist != null)
				r.journal = job.rol.resultlist.values().stream().mapToLong(List::size).sum();
			l.add(r);
		}
		overview.setAll(l);
		Thread t = new Thread(() -> {
			for (OverviewRow r : l) {
				List<Evidence.EvidenceFile> ev = Evidence.get(r.path);
				r.sha256 = ev.isEmpty() ? "not hashed" : ev.get(0).sha256();
				Platform.runLater(() -> overview.set(overview.indexOf(r) < 0 ? 0 : overview.indexOf(r), r));
			}
		}, "case-hash");
		t.setDaemon(true);
		t.start();
	}

	// ------------------------------------------------------------------ search

	private Tab searchTab() {
		TextField term = new TextField();
		term.setPromptText("search all open databases");
		HBox.setHgrow(term, Priority.ALWAYS);
		ComboBox<CaseSearch.Mode> mode = new ComboBox<>(FXCollections.observableArrayList(CaseSearch.Mode.values()));
		mode.getSelectionModel().select(0);
		CheckBox rec = new CheckBox("recovered, WAL and journal rows");
		rec.setSelected(true);
		ObservableList<CaseSearch.Hit> hits = FXCollections.observableArrayList();
		FilteredList<CaseSearch.Hit> shown = new FilteredList<>(hits, h -> true);
		TableView<CaseSearch.Hit> tv = new TableView<>(shown);
		tv.getColumns().add(UiUtil.col("Database", CaseSearch.Hit::db, 150));
		tv.getColumns().add(UiUtil.col("Source", CaseSearch.Hit::source, 100));
		tv.getColumns().add(UiUtil.col("Status", CaseSearch.Hit::status, 100));
		tv.getColumns().add(UiUtil.col("Table", CaseSearch.Hit::table, 140));
		tv.getColumns().add(UiUtil.col("rowid", h -> h.rowid() > 0 ? String.valueOf(h.rowid()) : "", 60));
		tv.getColumns().add(UiUtil.col("Column", CaseSearch.Hit::column, 120));
		tv.getColumns().add(UiUtil.col("Value", CaseSearch.Hit::value, 480));
		VBox.setVgrow(tv, Priority.ALWAYS);
		tv.setOnMouseClicked(e -> {
			CaseSearch.Hit h = tv.getSelectionModel().getSelectedItem();
			if (e.getClickCount() == 2 && h != null)
				gui.navigateToRow(h.job(), h.sourceKind(), h.table(), h.row());
		});
		ComboBox<String> dbFilter = new ComboBox<>();
		dbFilter.setPromptText("all databases");
		dbFilter.setOnAction(e -> {
			String d = dbFilter.getValue();
			shown.setPredicate(h -> d == null || d.startsWith("all") || h.db().equals(d));
		});
		Label status = new Label();
		AtomicBoolean stop = new AtomicBoolean();
		Button go = new Button("Search");
		Button stopBtn = new Button("Stop");
		stopBtn.setOnAction(e -> stop.set(true));
		Runnable run = () -> {
			String q = term.getText();
			if (q == null || q.isEmpty())
				return;
			List<CaseSearch.Target> dbs = gui.openDatabases();
			hits.clear();
			stop.set(false);
			status.setText("searching " + dbs.size() + " database(s) \u2026");
			CaseSearch.Mode m = mode.getValue();
			boolean r = rec.isSelected();
			Thread t = new Thread(() -> {
				List<CaseSearch.Hit> buf = new ArrayList<>();
				int n;
				try {
					n = CaseSearch.search(dbs, q, m, r, fqlite.base.Limits.CASE_MAX_HITS, buf::add, stop::get);
				} catch (java.util.regex.PatternSyntaxException ex) {
					Platform.runLater(() -> status.setText("invalid regular expression: " + ex.getDescription()));
					return;
				}
				final int total = n;
				Platform.runLater(() -> {
					hits.setAll(buf);
					List<String> names = new ArrayList<>(List.of("all databases"));
					for (CaseSearch.Hit h : buf)
						if (!names.contains(h.db()))
							names.add(h.db());
					dbFilter.getItems().setAll(names);
					long withHits = names.size() - 1;
					status.setText(total + " hit(s) in " + withHits + " of " + dbs.size() + " database(s)" + (stop.get() ? " (stopped)" : "")
							+ (total >= fqlite.base.Limits.CASE_MAX_HITS ? " - limit of " + String.format("%,d", fqlite.base.Limits.CASE_MAX_HITS) + " reached (Settings)" : ""));
				});
			}, "case-search");
			t.setDaemon(true);
			t.start();
		};
		go.setOnAction(e -> run.run());
		term.setOnAction(e -> run.run());
		Button export = new Button("Export CSV...");
		export.setOnAction(e -> {
			List<List<String>> rows = new ArrayList<>();
			List<String> paths = new ArrayList<>();
			for (CaseSearch.Hit h : shown) {
				rows.add(List.of(h.db(), h.source(), h.status(), h.table(), h.rowid() > 0 ? String.valueOf(h.rowid()) : "", h.column(), h.value()));
				if (!paths.contains(h.job().path))
					paths.add(h.job().path);
			}
			UiUtil.exportCsv(stage, "case-search", List.of("database", "source", "status", "table", "rowid", "column", "value"), rows,
					"case search", "term '" + term.getText() + "', " + mode.getValue(), paths);
		});
		HBox bar = new HBox(8, term, mode, rec, go, stopBtn);
		bar.setAlignment(Pos.CENTER_LEFT);
		HBox bar2 = new HBox(8, dbFilter, spacer(), status, export);
		bar2.setAlignment(Pos.CENTER_LEFT);
		VBox b = new VBox(8, UiUtil.hint("Searches every value FQLite holds for the open databases (one line per row). "
				+ "Double-click a hit to show the row in its table."), bar, bar2, tv);
		b.setPadding(new Insets(10));
		return new Tab("Search", b);
	}
}
