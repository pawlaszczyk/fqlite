package fqlite.analyzer;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.input.KeyCombination;
import javafx.scene.layout.Priority;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.FileChooser.ExtensionFilter;
import javafx.stage.Stage;

import java.awt.Desktop;
import java.io.File;

/**
 * A simple PDF viewer that delegates rendering to the OS default PDF application.
 * No external libraries are required.
 */
public class PDFViewer extends Application {

	/** The currently loaded PDF file (null = no file loaded). */
	private final ObjectProperty<File> currentFile = new SimpleObjectProperty<>(null);

	private FileChooser chooser;

	@Override
	public void start(Stage primaryStage) {

		// ── Placeholder area ─────────────────────────────────────────────────
		Label placeholder = new Label("No PDF loaded.\nUse File → Load PDF… to open a file.");
		placeholder.setStyle("-fx-text-fill: gray; -fx-font-size: 14; -fx-text-alignment: center;");
		placeholder.setAlignment(Pos.CENTER);

		Label fileLabel = new Label("No file selected.");
		fileLabel.setStyle("-fx-text-fill: #555; -fx-font-size: 12;");
		fileLabel.setPadding(new Insets(6, 10, 6, 10));

		currentFile.addListener((obs, old, file) ->
				fileLabel.setText(file != null ? "Loaded: " + file.getName() : "No file selected.")
		);

		StackPane contentArea = new StackPane(placeholder);
		contentArea.setStyle("-fx-background-color: #f4f4f4;");
		VBox.setVgrow(contentArea, Priority.ALWAYS);

		// ── Menu ─────────────────────────────────────────────────────────────
		MenuItem loadItem = new MenuItem("Load PDF…");
		loadItem.setAccelerator(KeyCombination.valueOf("SHORTCUT+O"));
		loadItem.setOnAction(evt -> {
			if (chooser == null) {
				chooser = new FileChooser();
				chooser.setTitle("Load PDF File");
				ExtensionFilter filter = new ExtensionFilter("PDF Files", "*.pdf");
				chooser.getExtensionFilters().add(filter);
				chooser.setSelectedExtensionFilter(filter);
			}
			File file = chooser.showOpenDialog(primaryStage);
			if (file != null) {
				loadAndOpen(file);
			}
		});

		MenuItem closeItem = new MenuItem("Close PDF");
		closeItem.setAccelerator(KeyCombination.valueOf("SHORTCUT+C"));
		closeItem.setOnAction(evt -> currentFile.set(null));
		closeItem.disableProperty().bind(currentFile.isNull());

		Menu fileMenu = new Menu("File");
		fileMenu.getItems().addAll(loadItem, closeItem);

		MenuBar menuBar = new MenuBar(fileMenu);
		menuBar.setUseSystemMenuBar(false);

		// ── Layout ───────────────────────────────────────────────────────────
		VBox box = new VBox(menuBar, fileLabel, contentArea);
		box.setFillWidth(true);

		primaryStage.setTitle("PDF Viewer");
		primaryStage.setWidth(1000);
		primaryStage.setHeight(900);
		primaryStage.setScene(new Scene(box));
		primaryStage.centerOnScreen();
		primaryStage.show();
	}

	/**
	 * Stores the file as the current document and opens it in the OS default
	 * PDF application via {@link Desktop#open(File)}.
	 */
	private void loadAndOpen(File file) {
		currentFile.set(file);

		Thread opener = new Thread(() -> {
			try {
				if (Desktop.isDesktopSupported()
					&& Desktop.getDesktop().isSupported(Desktop.Action.OPEN)) {
					Desktop.getDesktop().open(file);
				} else {
					Platform.runLater(() -> showError(
							"Cannot open PDF",
							"Desktop.open() is not supported on this system."
					));
				}
			} catch (Exception e) {
				e.printStackTrace();
				Platform.runLater(() -> showError("Cannot open PDF", e.getMessage()));
			}
		}, "pdf-opener");
		opener.setDaemon(true);
		opener.start();
	}

	private void showError(String title, String message) {
		Alert alert = new Alert(Alert.AlertType.ERROR);
		alert.setTitle(title);
		alert.setHeaderText(title);
		alert.setContentText(message != null ? message : "An unknown error occurred.");
		alert.showAndWait();
	}
}

	
	

