package fqlite.ui;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.function.Function;

import fqlite.base.ThemeManager;
import fqlite.forensics.Evidence;
import javafx.beans.property.SimpleStringProperty;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javafx.stage.Window;

/** small helpers shared by the forensic windows */
public final class UiUtil {

	private UiUtil() {
	}

	/** applies FQLite's theme to a window (and follows theme switches while it is open) */
	public static void theme(Stage stage, Scene scene) {
		ThemeManager.register(scene);
		stage.setOnHidden(e -> ThemeManager.unregister(scene));
	}

	public static <T> TableColumn<T, String> col(String title, Function<T, String> f, double width) {
		TableColumn<T, String> c = new TableColumn<>(title);
		c.setCellValueFactory(p -> new SimpleStringProperty(p.getValue() == null ? "" : String.valueOf(f.apply(p.getValue()))));
		c.setPrefWidth(width);
		return c;
	}

	/** numeric column (sorted as numbers) */
	public static <T> TableColumn<T, Number> num(String title, Function<T, Number> f, double width) {
		TableColumn<T, Number> c = new TableColumn<>(title);
		c.setCellValueFactory(p -> new javafx.beans.property.SimpleObjectProperty<>(p.getValue() == null ? 0 : f.apply(p.getValue())));
		c.setPrefWidth(width);
		return c;
	}

	public static Label hint(String text) {
		Label l = new Label(text);
		l.setWrapText(true);
		l.setMaxWidth(Double.MAX_VALUE);
		l.setMinHeight(javafx.scene.layout.Region.USE_PREF_SIZE);
		l.setStyle("-fx-opacity: 0.75; -fx-font-size: 11px;");
		return l;
	}

	/**
	 * Writes rows as CSV (UTF-8 with BOM, spreadsheet-safe: a text starting with = + - @ gets a
	 * leading apostrophe) and a manifest with the SHA-256 of the export and of the evidence.
	 */
	public static void exportCsv(Window owner, String defaultName, List<String> header, List<List<String>> rows, String kind,
			String scope, List<String> dbPaths) {
		FileChooser fc = new FileChooser();
		fc.setTitle("Export " + kind);
		fc.setInitialFileName(defaultName.replaceAll("[\\\\/:*?\"<>|]", "_") + ".csv");
		fc.getExtensionFilters().add(new FileChooser.ExtensionFilter("CSV", "*.csv"));
		File f = fc.showSaveDialog(owner);
		if (f == null)
			return;
		try (Writer w = Files.newBufferedWriter(f.toPath(), StandardCharsets.UTF_8)) {
			w.write('\uFEFF');
			w.write(line(header));
			for (List<String> r : rows)
				w.write(line(r));
		} catch (IOException e) {
			error("Export failed", e.getMessage());
			return;
		}
		File m = Evidence.writeManifest(kind, scope, dbPaths, List.of(f));
		Alert a = new Alert(Alert.AlertType.INFORMATION);
		a.setTitle("Export");
		a.setHeaderText(null);
		a.setContentText(rows.size() + " row(s) written to\n" + f.getAbsolutePath()
				+ (m != null ? "\nManifest with SHA-256: " + m.getName() : ""));
		a.showAndWait();
	}

	static String line(List<String> cells) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < cells.size(); i++) {
			if (i > 0)
				sb.append(',');
			String v = cells.get(i) == null ? "" : cells.get(i);
			if (!v.isEmpty() && "=+-@\t\r".indexOf(v.charAt(0)) >= 0 && !v.matches("-?\\d+(\\.\\d+)?"))
				v = "'" + v;
			sb.append('"').append(v.replace("\"", "\"\"")).append('"');
		}
		return sb.append("\r\n").toString();
	}

	public static void error(String title, String text) {
		Alert a = new Alert(Alert.AlertType.ERROR);
		a.setTitle(title);
		a.setHeaderText(null);
		a.setContentText(text);
		a.showAndWait();
	}
}
