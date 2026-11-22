package fqlite.base;

import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.Label;
import javafx.scene.layout.Region;

/**
 * JavaFX dialogs opened by {@link Job}. They are kept out of {@code Job} so that the
 * bytecode verifier does not need the JavaFX control classes when {@code Job} is loaded:
 * the headless library ({@code fqlite.headless}) then only needs the module javafx.base.
 */
final class JobDialogs {

	private JobDialogs() {
	}

	/** result of the Lighthouse (ETSI) import-log check - must run on the JavaFX thread */
	static void showLighthouseCheck(boolean warn, String text) {
		Alert alert = new Alert(warn ? AlertType.WARNING : AlertType.INFORMATION);
		alert.setTitle("Lighthouse database");
		alert.setHeaderText(warn ? "Records differ from the import log" : "Lighthouse (ETSI) database recognised");
		Label body = new Label(text);
		body.setWrapText(true);
		body.setMaxWidth(580);
		body.setMinHeight(Region.USE_PREF_SIZE);
		alert.getDialogPane().setContent(body);
		alert.getDialogPane().setPrefWidth(620);
		alert.getDialogPane().setMinHeight(Region.USE_PREF_SIZE);
		alert.setResizable(true);
		alert.show();
	}
}
