package fqlite.ui;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import fqlite.base.GUI;
import fqlite.forensics.CaseSearch;
import fqlite.forensics.Timeline;
import fqlite.forensics.Timeline.DateColumn;
import fqlite.forensics.Timeline.Event;
import javafx.application.Platform;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.CheckMenuItem;
import javafx.scene.control.ComboBox;
import javafx.scene.control.DatePicker;
import javafx.scene.control.Label;
import javafx.scene.control.MenuButton;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableRow;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.control.cell.CheckBoxTableCell;
import javafx.scene.control.cell.ComboBoxTableCell;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.stage.Stage;

/**
 * Timeline over the open databases (a case): date columns found by name and value, every dated
 * row in time order with a density chart; deleted records and WAL / journal versions optionally,
 * each marked by its source and status.
 */
public class TimelineWindow {

	private final Stage stage = new Stage();
	private final GUI gui;
	private final ObservableList<DateColumn> columns = FXCollections.observableArrayList();
	private final ObservableList<Event> events = FXCollections.observableArrayList();
	private final FilteredList<Event> shown = new FilteredList<>(events, e -> true);
	private final List<CheckMenuItem> dbItems = new ArrayList<>();
	private final Label status = new Label();
	private final Canvas chart = new Canvas(600, 90);
	private Map<Instant, Integer> bins = Map.of();
	private long binWidth = 1;
	private Instant rangeFrom, rangeTo;
	private final AtomicBoolean stop = new AtomicBoolean();

	public TimelineWindow(GUI gui) {
		this.gui = gui;
		stage.setTitle("Timeline");

		// ---------------- scope and options
		MenuButton scope = new MenuButton();
		List<CaseSearch.Target> dbs = gui.openDatabases();
		for (CaseSearch.Target t : dbs) {
			CheckMenuItem mi = new CheckMenuItem(t.name());
			mi.setSelected(true);
			mi.setUserData(t);
			mi.selectedProperty().addListener((o, a, b) -> scope.setText(scopeText()));
			dbItems.add(mi);
			scope.getItems().add(mi);
		}
		scope.setText(scopeText());
		CheckBox deleted = new CheckBox("deleted records");
		deleted.setSelected(true);
		CheckBox versions = new CheckBox("WAL / journal versions");
		versions.setSelected(true);
		DatePicker from = new DatePicker(), to = new DatePicker();
		from.setPromptText("from (UTC)");
		to.setPromptText("to (UTC)");
		from.setPrefWidth(130);
		to.setPrefWidth(130);
		Button detect = new Button("Find date columns");
		Button build = new Button("Build timeline");
		Button stopBtn = new Button("Stop");
		stopBtn.setOnAction(e -> stop.set(true));
		detect.setOnAction(e -> detect());
		build.setOnAction(e -> build(deleted.isSelected(), versions.isSelected(), from.getValue(), to.getValue()));
		HBox top = new HBox(8, new Label("Databases:"), scope, deleted, versions, from, to, detect, build, stopBtn);
		top.setAlignment(Pos.CENTER_LEFT);

		// ---------------- date columns
		TableView<DateColumn> ct = new TableView<>(columns);
		ct.setEditable(true);
		TableColumn<DateColumn, Boolean> use = new TableColumn<>("Use");
		use.setCellValueFactory(p -> {
			SimpleBooleanProperty b = new SimpleBooleanProperty(p.getValue().selected);
			b.addListener((o, a, v) -> p.getValue().selected = v);
			return b;
		});
		use.setCellFactory(CheckBoxTableCell.forTableColumn(use));
		use.setEditable(true);
		use.setPrefWidth(40);
		ct.getColumns().add(use);
		ct.getColumns().add(UiUtil.col("Database", c -> c.db, 110));
		ct.getColumns().add(UiUtil.col("Table", c -> c.table, 110));
		ct.getColumns().add(UiUtil.col("Column", c -> c.column, 110));
		TableColumn<DateColumn, Timeline.Format> fmt = new TableColumn<>("Read as");
		fmt.setCellValueFactory(p -> new SimpleObjectProperty<>(p.getValue().format));
		fmt.setCellFactory(ComboBoxTableCell.forTableColumn(Timeline.Format.values()));
		fmt.setOnEditCommit(e -> e.getRowValue().format = e.getNewValue());
		fmt.setEditable(true);
		fmt.setPrefWidth(170);
		ct.getColumns().add(fmt);
		ct.getColumns().add(UiUtil.col("Plausible", DateColumn::confidence, 70));
		ct.getColumns().add(UiUtil.col("Example", c -> c.example, 260));
		ct.setRowFactory(x -> {
			TableRow<DateColumn> r = new TableRow<>() {
				@Override
				protected void updateItem(DateColumn c, boolean empty) {
					super.updateItem(c, empty);
					setTooltip(empty || c == null ? null : new Tooltip(c.reason));
				}
			};
			return r;
		});

		// ---------------- events
		TableView<Event> et = new TableView<>(shown);
		et.getColumns().add(UiUtil.col("Time (UTC)", e -> Timeline.formatUtc(e.time()), 150));
		et.getColumns().add(UiUtil.col("Database", Event::db, 110));
		et.getColumns().add(UiUtil.col("Table", Event::table, 110));
		et.getColumns().add(UiUtil.col("Column", Event::column, 110));
		et.getColumns().add(UiUtil.col("Status", Event::status, 110));
		et.getColumns().add(UiUtil.col("Source", Event::source, 100));
		et.getColumns().add(UiUtil.col("rowid", e -> e.rowid() > 0 ? String.valueOf(e.rowid()) : "", 60));
		et.getColumns().add(UiUtil.col("Raw value", Event::raw, 140));
		et.getColumns().add(UiUtil.col("Row", Event::description, 380));
		et.setRowFactory(x -> new TableRow<>() {
			@Override
			protected void updateItem(Event e, boolean empty) {
				super.updateItem(e, empty);
				String st = empty || e == null ? "" : e.status();
				setStyle(st.startsWith("deleted") || st.equals("freelist") || st.startsWith("row deleted") ? "-fx-background-color: rgba(220,38,38,0.20);"
						: st.equals("older version") ? "-fx-background-color: rgba(245,158,11,0.30);" : "");
			}
		});
		et.setOnMouseClicked(e -> {
			Event ev = et.getSelectionModel().getSelectedItem();
			if (e.getClickCount() == 2 && ev != null)
				gui.navigateToRow(ev.job(), ev.sourceKind(), ev.table(), ev.row());
		});
		TextField filter = new TextField();
		filter.setPromptText("filter events (any text)");
		filter.textProperty().addListener((o, a, b) -> applyFilter(b));
		Button reset = new Button("Whole range");
		reset.setOnAction(e -> {
			rangeFrom = rangeTo = null;
			applyFilter(filter.getText());
		});
		Button export = new Button("Export CSV...");
		export.setOnAction(e -> export());

		Pane chartPane = new Pane(chart);
		chartPane.setMinHeight(95);
		chart.widthProperty().bind(chartPane.widthProperty());
		chart.widthProperty().addListener((o, a, b) -> drawChart());
		chart.setOnMouseClicked(e -> clickChart(e.getX(), filter.getText()));
		Tooltip.install(chart, new Tooltip("Events over time. Click a bar to show only its period."));

		Region sp = new Region();
		HBox.setHgrow(sp, Priority.ALWAYS);
		HBox evbar = new HBox(8, filter, reset, sp, status, export);
		evbar.setAlignment(Pos.CENTER_LEFT);
		VBox right = new VBox(6, chartPane, evbar, et);
		VBox.setVgrow(et, Priority.ALWAYS);
		VBox left = new VBox(6, UiUtil.hint("Date columns found by name and by their values (plausible dates 1990-2040 in one format). "
				+ "Untick a column or choose another format, then build."), ct);
		VBox.setVgrow(ct, Priority.ALWAYS);
		SplitPane split = new SplitPane(left, right);
		split.setOrientation(Orientation.HORIZONTAL);
		split.setDividerPositions(0.38);
		VBox.setVgrow(split, Priority.ALWAYS);
		VBox root = new VBox(8, top, split);
		root.setPadding(new Insets(10));
		Scene sc = new Scene(root, 1400, 720);
		stage.setScene(sc);
		UiUtil.theme(stage, sc);
		stage.setOnShown(e -> detect());
	}

	public void show() {
		stage.show();
	}

	private String scopeText() {
		long n = dbItems.stream().filter(CheckMenuItem::isSelected).count();
		return n == dbItems.size() ? "all " + n + " databases" : n + " of " + dbItems.size() + " databases";
	}

	private List<CaseSearch.Target> scope() {
		List<CaseSearch.Target> l = new ArrayList<>();
		for (CheckMenuItem mi : dbItems)
			if (mi.isSelected())
				l.add((CaseSearch.Target) mi.getUserData());
		return l;
	}

	private void detect() {
		List<CaseSearch.Target> sc = scope();
		status.setText("finding date columns \u2026");
		Thread t = new Thread(() -> {
			List<DateColumn> l = new ArrayList<>();
			for (CaseSearch.Target tg : sc)
				l.addAll(Timeline.detect(tg.job(), tg.name()));
			Platform.runLater(() -> {
				columns.setAll(l);
				status.setText(l.size() + " date column(s) in " + sc.size() + " database(s)");
			});
		}, "timeline-detect");
		t.setDaemon(true);
		t.start();
	}

	private void build(boolean deleted, boolean versions, LocalDate from, LocalDate to) {
		List<DateColumn> sel = new ArrayList<>(columns);
		Instant f = from == null ? null : from.atStartOfDay().toInstant(ZoneOffset.UTC);
		Instant t2 = to == null ? null : to.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC);
		stop.set(false);
		status.setText("building \u2026");
		Thread t = new Thread(() -> {
			List<Event> l = Timeline.build(sel, deleted, versions, f, t2, 200_000, stop::get);
			Platform.runLater(() -> {
				events.setAll(l);
				rangeFrom = rangeTo = null;
				applyFilter(null);
				long del = l.stream().filter(e -> !e.status().equals("live") && !e.status().equals("version")).count();
				status.setText(l.size() + " event(s)" + (del > 0 ? ", " + del + " deleted / older" : "") + (stop.get() ? " (stopped)" : ""));
			});
		}, "timeline-build");
		t.setDaemon(true);
		t.start();
	}

	private void applyFilter(String text) {
		String f = text == null ? "" : text.toLowerCase();
		shown.setPredicate(e -> (rangeFrom == null || !e.time().isBefore(rangeFrom)) && (rangeTo == null || e.time().isBefore(rangeTo))
				&& (f.isEmpty() || (e.db() + " " + e.table() + " " + e.column() + " " + e.status() + " " + e.description() + " " + e.raw())
						.toLowerCase().contains(f)));
		bins = Timeline.density(new ArrayList<>(events), 80);
		if (bins.size() > 1) {
			List<Instant> k = new ArrayList<>(bins.keySet());
			binWidth = k.get(1).getEpochSecond() - k.get(0).getEpochSecond();
		} else
			binWidth = 1;
		drawChart();
	}

	private void drawChart() {
		GraphicsContext g = chart.getGraphicsContext2D();
		double w = chart.getWidth(), h = chart.getHeight();
		g.clearRect(0, 0, w, h);
		if (bins.isEmpty())
			return;
		int max = bins.values().stream().mapToInt(Integer::intValue).max().orElse(1);
		double bw = w / bins.size();
		int i = 0;
		boolean dark = fqlite.base.ThemeManager.isDark();
		Color base = dark ? Color.web("#5598e8") : Color.web("#2A78D6");
		for (Map.Entry<Instant, Integer> e : bins.entrySet()) {
			boolean inRange = rangeFrom == null || (!e.getKey().isBefore(rangeFrom) && e.getKey().isBefore(rangeTo));
			double bh = e.getValue() == 0 ? 0 : Math.max(2, (h - 18) * e.getValue() / (double) max);
			g.setFill(inRange ? base : base.deriveColor(0, 1, 1, 0.35));
			g.fillRect(i * bw + 1, h - 14 - bh, Math.max(1, bw - 2), bh);
			i++;
		}
		g.setFill(dark ? Color.web("#c8d0e8") : Color.web("#444444"));
		List<Instant> keys = new ArrayList<>(bins.keySet());
		g.fillText(Timeline.formatUtc(keys.get(0)).substring(0, 10), 2, h - 2);
		String last = Timeline.formatUtc(keys.get(keys.size() - 1).plusSeconds(binWidth)).substring(0, 10);
		g.fillText(last, Math.max(2, w - 70), h - 2);
		g.fillText("max " + max + " / bar", w / 2 - 30, h - 2);
	}

	private void clickChart(double x, String filterText) {
		if (bins.isEmpty())
			return;
		int i = (int) (x / (chart.getWidth() / bins.size()));
		List<Instant> keys = new ArrayList<>(bins.keySet());
		if (i < 0 || i >= keys.size())
			return;
		rangeFrom = keys.get(i);
		rangeTo = rangeFrom.plusSeconds(binWidth);
		applyFilter(filterText);
		status.setText(shown.size() + " event(s) between " + Timeline.formatUtc(rangeFrom) + " and " + Timeline.formatUtc(rangeTo));
	}

	private void export() {
		List<List<String>> rows = new ArrayList<>();
		List<String> dbPaths = new ArrayList<>();
		for (Event e : shown) {
			rows.add(List.of(Timeline.formatUtc(e.time()), e.time().toString(), e.db(), e.table(), e.column(), e.format().label, e.status(),
					e.source(), e.rowid() > 0 ? String.valueOf(e.rowid()) : "", e.raw(), e.description()));
			if (!dbPaths.contains(e.job().path))
				dbPaths.add(e.job().path);
		}
		UiUtil.exportCsv(stage, "timeline", List.of("time_utc", "iso8601", "database", "table", "column", "format", "status", "source",
				"rowid", "raw value", "row"), rows, "timeline", scopeText(), dbPaths);
	}
}
