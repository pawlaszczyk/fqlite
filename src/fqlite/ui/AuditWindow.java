package fqlite.ui;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import fqlite.base.GUI;
import fqlite.base.Job;
import fqlite.forensics.Audit;
import fqlite.forensics.Evidence;
import fqlite.forensics.PageMap;
import fqlite.forensics.WalAudit;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TableRow;
import javafx.scene.control.TableView;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

/**
 * Audit report of a database: header and structure checks, the use of every page, the state of
 * every WAL frame (checksums) and the "time travel" to the database state after each commit.
 */
public class AuditWindow {

	private final Stage stage = new Stage();
	private final GUI gui;
	private final Job job;
	private final String name;
	private final TabPane tabs = new TabPane();
	private final Label status = new Label("Analysing\u2026");
	private Audit.Report report;

	public AuditWindow(GUI gui, Job job) {
		this.gui = gui;
		this.job = job;
		this.name = new File(job.path).getName();
		stage.setTitle("Audit report \u2014 " + name);
		tabs.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
		VBox root = new VBox(6, tabs, status);
		root.setPadding(new Insets(0, 0, 6, 8));
		VBox.setVgrow(tabs, Priority.ALWAYS);
		Scene sc = new Scene(root, 1180, 660);
		stage.setScene(sc);
		UiUtil.theme(stage, sc);
		Thread t = new Thread(() -> {
			Audit.Report r = Audit.run(job.path);
			Platform.runLater(() -> fill(r));
		}, "audit");
		t.setDaemon(true);
		t.start();
	}

	public void show() {
		stage.show();
	}

	private static Region spacer() {
		Region r = new Region();
		HBox.setHgrow(r, Priority.ALWAYS);
		return r;
	}

	private static VBox box(javafx.scene.Node... n) {
		VBox b = new VBox(8, n);
		b.setPadding(new Insets(10));
		return b;
	}

	private void fill(Audit.Report r) {
		report = r;
		tabs.getTabs().add(reportTab(r));
		if (r.pages() != null)
			tabs.getTabs().add(pagesTab(r.pages()));
		if (r.wal() != null && r.wal().headerOk) {
			tabs.getTabs().add(framesTab(r.wal()));
			tabs.getTabs().add(timeTravelTab(r.wal()));
		}
		long w = r.checks().stream().filter(c -> c.level() == Audit.Level.WARN).count();
		long n = r.checks().stream().filter(c -> c.level() == Audit.Level.NOTE).count();
		status.setText(r.checks().size() + " checks: " + w + " warning(s), " + n + " note(s)");
	}

	// ------------------------------------------------------------------ report

	private Tab reportTab(Audit.Report r) {
		ObservableList<Audit.Check> data = FXCollections.observableArrayList(r.checks());
		FilteredList<Audit.Check> f = new FilteredList<>(data, x -> true);
		TableView<Audit.Check> tv = new TableView<>(f);
		tv.getColumns().add(UiUtil.col("File", Audit.Check::area, 80));
		tv.getColumns().add(UiUtil.col("Check", Audit.Check::item, 230));
		tv.getColumns().add(UiUtil.col("Result", Audit.Check::value, 330));
		tv.getColumns().add(UiUtil.col("", c -> c.level().label, 70));
		tv.getColumns().add(UiUtil.col("Meaning", Audit.Check::note, 520));
		tv.setRowFactory(x -> new TableRow<>() {
			@Override
			protected void updateItem(Audit.Check c, boolean empty) {
				super.updateItem(c, empty);
				setStyle(empty || c == null ? "" : switch (c.level()) {
				case WARN -> "-fx-background-color: rgba(239,68,68,0.22);";
				case NOTE -> "-fx-background-color: rgba(245,158,11,0.20);";
				case OK -> "";
				default -> "-fx-opacity: 0.85;";
				});
				if (c != null && !empty)
					setTooltip(new javafx.scene.control.Tooltip(c.item() + ": " + c.value() + (c.note().isEmpty() ? "" : "\n" + c.note())));
			}
		});
		VBox.setVgrow(tv, Priority.ALWAYS);
		CheckBox only = new CheckBox("only notes and warnings");
		only.selectedProperty().addListener((o, a, b) -> f.setPredicate(c -> !b || c.level() == Audit.Level.NOTE || c.level() == Audit.Level.WARN));
		Button export = new Button("Export CSV...");
		export.setOnAction(e -> {
			List<List<String>> rows = new ArrayList<>();
			for (Audit.Check c : data)
				rows.add(List.of(c.area(), c.item(), c.value(), c.level().label, c.note()));
			UiUtil.exportCsv(stage, name + "-audit", List.of("file", "check", "result", "level", "meaning"), rows, "audit report", name,
					List.of(job.path));
		});
		Label hint = UiUtil.hint("The database file and its -wal / -shm / -journal files are read directly (read-only), independent of the "
				+ "recovery. Warnings point to damage, truncation or manipulation, notes to findings that matter for the "
				+ "interpretation (e.g. secure_delete, stale WAL frames, unreferenced pages).");
		return new Tab("Report", box(hint, new HBox(8, only, spacer(), export), tv));
	}

	// ------------------------------------------------------------------ pages

	public record PageRow(int page, String use, String owner, boolean zeroed, int freeblocks, int unallocated) {
	}

	private Tab pagesTab(PageMap p) {
		List<PageRow> l = new ArrayList<>();
		for (int pg = 1; pg <= p.filePages; pg++) {
			int[] un = p.unallocated.get(pg);
			l.add(new PageRow(pg, p.kind[pg] == null ? "" : p.kind[pg].label, p.owner[pg] == null ? "" : p.owner[pg], p.zeroed.get(pg),
					p.freeblocks.getOrDefault(pg, List.of()).size(), un == null ? 0 : Math.max(0, un[1] - un[0])));
		}
		ObservableList<PageRow> data = FXCollections.observableArrayList(l);
		FilteredList<PageRow> f = new FilteredList<>(data, x -> true);
		TableView<PageRow> tv = new TableView<>(f);
		tv.getColumns().add(UiUtil.num("Page", PageRow::page, 70));
		tv.getColumns().add(UiUtil.num("File offset", r -> (long) (r.page() - 1) * p.pageSize, 100));
		tv.getColumns().add(UiUtil.col("Use", PageRow::use, 130));
		tv.getColumns().add(UiUtil.col("Belongs to", PageRow::owner, 220));
		tv.getColumns().add(UiUtil.col("Only zeros", r -> r.zeroed() ? "yes" : "", 80));
		tv.getColumns().add(UiUtil.num("Freeblocks", PageRow::freeblocks, 85));
		tv.getColumns().add(UiUtil.num("Unallocated bytes", PageRow::unallocated, 120));
		tv.setRowFactory(x -> new TableRow<>() {
			@Override
			protected void updateItem(PageRow r, boolean empty) {
				super.updateItem(r, empty);
				setStyle(empty || r == null ? "" : r.use().equals("unreferenced") ? "-fx-background-color: rgba(245,158,11,0.22);"
						: r.use().startsWith("freelist") ? "-fx-background-color: rgba(59,130,246,0.12);" : "");
			}
		});
		VBox.setVgrow(tv, Priority.ALWAYS);
		ComboBox<String> kind = new ComboBox<>();
		kind.getItems().add("all pages");
		for (PageMap.Kind k : PageMap.Kind.values())
			kind.getItems().add(k.label);
		kind.getItems().add("pages wiped with zeros");
		kind.setValue("all pages");
		kind.setOnAction(e -> {
			String v = kind.getValue();
			f.setPredicate(r -> v.equals("all pages") || r.use().equals(v) || (v.startsWith("pages wiped") && r.zeroed()));
		});
		Button hex = new Button("Show page in hex viewer");
		hex.setOnAction(e -> {
			PageRow r = tv.getSelectionModel().getSelectedItem();
			if (r != null)
				gui.showInHexViewer(job.path, (long) (r.page() - 1) * p.pageSize);
		});
		tv.setOnMouseClicked(e -> {
			if (e.getClickCount() == 2)
				hex.fire();
		});
		Map<PageMap.Kind, Integer> c = p.counts();
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<PageMap.Kind, Integer> en : c.entrySet())
			if (en.getValue() > 0)
				sb.append(sb.length() > 0 ? ", " : "").append(en.getValue()).append(" ").append(en.getKey().label);
		Label hint = UiUtil.hint("Use of every page, determined from the schema, the b-trees, overflow chains and the freelist. "
				+ "\"Unreferenced\" pages belong to none of them - remains of dropped tables, of damaged structures or of manipulated "
				+ "pointers. " + sb);
		return new Tab("Pages", box(hint, new HBox(8, kind, spacer(), hex), tv));
	}

	// ------------------------------------------------------------------ WAL frames

	private Tab framesTab(WalAudit w) {
		ObservableList<WalAudit.Frame> data = FXCollections.observableArrayList(w.frames);
		FilteredList<WalAudit.Frame> f = new FilteredList<>(data, x -> true);
		TableView<WalAudit.Frame> tv = new TableView<>(f);
		PageMap pm = report.pages();
		tv.getColumns().add(UiUtil.num("Frame", x -> x.index, 65));
		tv.getColumns().add(UiUtil.num("Offset", x -> x.offset, 90));
		tv.getColumns().add(UiUtil.num("DB page", x -> x.page, 75));
		tv.getColumns().add(UiUtil.col("Page belongs to", x -> pm != null && x.page >= 1 && x.page <= pm.filePages && pm.owner[x.page] != null
				? pm.owner[x.page] : "", 160));
		tv.getColumns().add(UiUtil.col("Tx", x -> x.tx > 0 ? String.valueOf(x.tx) : "", 50));
		tv.getColumns().add(UiUtil.col("Commit (DB size)", x -> x.commitSize > 0 ? String.valueOf(x.commitSize) : "", 130));
		tv.getColumns().add(UiUtil.col("Salt-1 / Salt-2", x -> x.salt1 + " / " + x.salt2, 190));
		tv.getColumns().add(UiUtil.col("Checksum", x -> !x.saltsMatch ? "\u2013" : x.checksumOk ? "valid" : "invalid", 85));
		tv.getColumns().add(UiUtil.col("State", x -> x.state.label, 190));
		tv.setRowFactory(x -> new TableRow<>() {
			@Override
			protected void updateItem(WalAudit.Frame r, boolean empty) {
				super.updateItem(r, empty);
				setStyle(empty || r == null ? "" : switch (r.state) {
				case SUPERSEDED -> "-fx-background-color: rgba(245,158,11,0.18);";
				case UNCOMMITTED -> "-fx-background-color: rgba(59,130,246,0.16);";
				case STALE -> "-fx-background-color: rgba(128,128,128,0.22);";
				case INVALID -> "-fx-background-color: rgba(239,68,68,0.22);";
				default -> "";
				});
			}
		});
		VBox.setVgrow(tv, Priority.ALWAYS);
		ComboBox<String> st = new ComboBox<>();
		st.getItems().add("all frames");
		for (WalAudit.State s : WalAudit.State.values())
			st.getItems().add(s.label);
		st.setValue("all frames");
		st.setOnAction(e -> f.setPredicate(x -> st.getValue().equals("all frames") || x.state.label.equals(st.getValue())));
		Button hex = new Button("Show frame in hex viewer");
		hex.setOnAction(e -> {
			WalAudit.Frame x = tv.getSelectionModel().getSelectedItem();
			if (x != null)
				gui.showInHexViewer(w.path, x.offset);
		});
		Button export = new Button("Export CSV...");
		export.setOnAction(e -> {
			List<List<String>> rows = new ArrayList<>();
			for (WalAudit.Frame x : data)
				rows.add(List.of(String.valueOf(x.index), String.valueOf(x.offset), String.valueOf(x.page), x.tx > 0 ? String.valueOf(x.tx) : "",
						String.valueOf(x.commitSize), String.valueOf(x.salt1), String.valueOf(x.salt2),
						!x.saltsMatch ? "" : x.checksumOk ? "valid" : "invalid", x.state.label));
			UiUtil.exportCsv(stage, name + "-wal-frames", List.of("frame", "offset", "db page", "tx", "commit db size", "salt-1", "salt-2",
					"checksum", "state"), rows, "WAL frames", name, List.of(job.path));
		});
		Label hint = UiUtil.hint("Every frame is checked the way SQLite does on recovery: salts of the WAL header and the cumulative "
				+ "checksum. current = latest committed version of the page, superseded = an older committed version, uncommitted = "
				+ "after the last commit (never committed), stale = earlier WAL generation, invalid = checksum chain broken (SQLite "
				+ "ignores it and all following frames).");
		return new Tab("WAL frames", box(hint, new HBox(8, st, spacer(), hex, export), tv));
	}

	// ------------------------------------------------------------------ time travel

	public record TxRow(int tx, String frames, String pages, int dbPages, String note) {
	}

	private Tab timeTravelTab(WalAudit w) {
		PageMap pm = report.pages();
		List<TxRow> l = new ArrayList<>();
		boolean backfilled = w.shmBackfill > 0 && w.shmSalt1 == w.salt1;
		l.add(new TxRow(0, "", "", pm == null ? 0 : pm.filePages, backfilled
				? "database file without the WAL - it already contains " + w.shmBackfill + " checkpointed frame(s), so it is NOT the state before them"
				: "database file without the WAL (state before the first frame)"));
		for (WalAudit.Commit c : w.commits) {
			StringBuilder sb = new StringBuilder();
			int i = 0;
			for (int p : c.pages()) {
				if (i++ > 0)
					sb.append(", ");
				if (i > 30) {
					sb.append("\u2026");
					break;
				}
				sb.append(p);
				if (pm != null && p >= 1 && p <= pm.filePages && pm.owner[p] != null)
					sb.append(" (").append(pm.owner[p]).append(")");
			}
			String note = backfilled && c.lastFrame() < w.shmBackfill ? "checkpointed later - some pages may show a newer state" : "";
			l.add(new TxRow(c.tx(), c.firstFrame() + "-" + c.lastFrame(), sb.toString(), c.dbPages(), note));
		}
		TableView<TxRow> tv = new TableView<>(FXCollections.observableArrayList(l));
		tv.getColumns().add(UiUtil.col("After commit", r -> r.tx() == 0 ? "\u2013" : String.valueOf(r.tx()), 100));
		tv.getColumns().add(UiUtil.col("Frames", TxRow::frames, 90));
		tv.getColumns().add(UiUtil.num("DB size (pages)", TxRow::dbPages, 120));
		tv.getColumns().add(UiUtil.col("Changed pages", TxRow::pages, 470));
		tv.getColumns().add(UiUtil.col("Note", TxRow::note, 380));
		VBox.setVgrow(tv, Priority.ALWAYS);
		Button open = new Button("Open this state in FQLite");
		Button save = new Button("Save as database file...");
		Label info = new Label();
		info.setWrapText(true);
		open.setOnAction(e -> {
			TxRow r = tv.getSelectionModel().getSelectedItem();
			if (r == null)
				return;
			File dir = new File(GUI.baseDir, "snapshots");
			dir.mkdirs();
			String base = name.replaceAll("\\.(sqlite3?|db)$", "");
			File out = new File(dir, base + (r.tx() == 0 ? "-without-wal" : "-after-commit-" + r.tx()) + ".sqlite");
			if (write(w, r.tx(), out, info))
				gui.openForCase(out);
		});
		save.setOnAction(e -> {
			TxRow r = tv.getSelectionModel().getSelectedItem();
			if (r == null)
				return;
			FileChooser fc = new FileChooser();
			fc.setTitle("Save database state");
			fc.setInitialFileName(name.replaceAll("\\.(sqlite3?|db)$", "") + (r.tx() == 0 ? "-without-wal" : "-after-commit-" + r.tx()) + ".sqlite");
			File out = fc.showSaveDialog(stage);
			if (out != null && write(w, r.tx(), out, info)) {
				Alert a = new Alert(Alert.AlertType.INFORMATION, "Written to " + out.getAbsolutePath() + "\n" + info.getText());
				a.setHeaderText(null);
				a.showAndWait();
			}
		});
		tv.setOnMouseClicked(e -> {
			if (e.getClickCount() == 2)
				open.fire();
		});
		Label hint = UiUtil.hint("Time travel: the database as it was after each committed transaction of the WAL - the database file "
				+ "overlaid with the newest frame of every page up to that commit (exactly what SQLite would have read at that time). "
				+ "The state is written to a new file and opened as a separate database; the evidence is not changed. If the "
				+ "database file was already checkpointed (-shm: nBackfill), it may contain newer versions of some pages - these "
				+ "are listed after the export.");
		HBox bar = new HBox(8, spacer(), open, save);
		open.setMinWidth(Region.USE_PREF_SIZE);
		save.setMinWidth(Region.USE_PREF_SIZE);
		return new Tab("Time travel", box(hint, bar, tv, info));
	}

	private boolean write(WalAudit w, int tx, File out, Label info) {
		try {
			WalAudit.Snapshot s = w.snapshot(job.path, tx);
			Files.write(out.toPath(), s.data());
			Evidence.writeManifest("database state" + (tx == 0 ? " without the WAL" : " after WAL commit " + tx), name,
					List.of(job.path, w.path), List.of(out));
			info.setText((tx == 0 ? "Database file without WAL" : "State after commit " + tx) + ": " + s.data().length / Math.max(1, w.pageSize)
					+ " pages, " + s.pagesFromWal() + " taken from the WAL"
					+ (s.uncertain().isEmpty() ? "." : "; " + s.uncertain().size() + " page(s) uncertain (already checkpointed, the "
							+ "database file may hold a newer version): " + s.uncertain()));
			return true;
		} catch (Exception ex) {
			UiUtil.error("Time travel", "The state could not be written: " + ex.getMessage());
			return false;
		}
	}
}
