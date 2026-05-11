package fqlite.ui;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.layout.StackPane;
import javafx.stage.Screen;
import javafx.stage.Stage;
import java.awt.Desktop;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Opens the FQLite User Guide PDF in the system default PDF viewer.
 * No external libraries are required.
 */
public class UserGuideWindow extends Application {

	@Override
	public void start(Stage primaryStage) {

		// Show a small status window while the PDF is being prepared
		Label status = new Label("Opening User Guide …");
		status.setStyle("-fx-font-size: 14; -fx-text-fill: gray;");

		Scene scene = new Scene(new StackPane(status));
		primaryStage.setTitle("FQLite User Guide");
		primaryStage.setWidth(Screen.getPrimary().getVisualBounds().getWidth() * 0.3);
		primaryStage.setHeight(80);
		primaryStage.setScene(scene);
		primaryStage.centerOnScreen();
		primaryStage.setAlwaysOnTop(true);
		primaryStage.show();

		// Extract PDF to a temp file and hand it to the OS – run off the FX thread
		Thread opener = new Thread(() -> {
			try {
				Path tempPdf = Files.createTempFile("FQLite_UserGuide_", ".pdf");
				tempPdf.toFile().deleteOnExit();

				try (InputStream in = Objects.requireNonNull(
						getClass().getResourceAsStream("/FQLite_UserGuide.pdf"),
						"PDF resource not found: /FQLite_UserGuide.pdf");
					 OutputStream out = Files.newOutputStream(tempPdf)) {
					in.transferTo(out);
				}

				if (Desktop.isDesktopSupported()
					&& Desktop.getDesktop().isSupported(Desktop.Action.OPEN)) {

					Desktop.getDesktop().open(tempPdf.toFile());

					Platform.runLater(() -> status.setText("User Guide opened in your PDF viewer."));

					// Close the small status window automatically after 1.5 s
					Thread.sleep(1500);
					Platform.runLater(primaryStage::close);

				} else {
					Platform.runLater(() ->
							status.setText("Cannot open PDF: Desktop.open() is not supported on this system.")
					);
				}

			} catch (Exception e) {
				e.printStackTrace();
				Platform.runLater(() -> status.setText("Error: " + e.getMessage()));
			}
		}, "pdf-opener");

		opener.setDaemon(true);
		opener.start();
	}
}
