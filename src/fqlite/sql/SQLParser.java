package fqlite.sql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import fqlite.base.Global;
import javafx.stage.Modality;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableColumn.CellDataFeatures;
import javafx.scene.control.TableView;
import javafx.stage.Stage;
import javafx.util.Callback;


/**
 * This class is used to evaluate the SQL statements. After checking the syntax
 * of the SELECT statement, the Parser loads the in-memory tables. Finally, the
 * query is executed using the JDBC interface. The result set is automatically
 * written to a table view. 
 * 
 * @author Dirk Pawlaszczyk
 */
public class SQLParser {

	private static SQLParser instance;
	static Label statusline;


	public static SQLParser getInstance(){
		if(instance == null){
			instance = new SQLParser();
		}
		return instance;
	}

	/**
	 * Parse and execute the user-defined SQL statement.
	 * 
	 * @param command the SQL SELECT statement to parse
	 * @param dbname the database name
	 * @param stage the current stage object (where changes should take place)
	 */
	@SuppressWarnings("rawtypes")
	public void parse(String command, String dbname, Stage stage, TableView resultview, Label statusline) {

		SQLParser.statusline = statusline;

		// clean the command - remove comments
		String noComments = command.replaceAll("(?m)--.*$", "");
		if (command.contains("SELECT")) {
			command = noComments.replaceAll("(?is).*?(SELECT\\b.*)", "$1");
		}
		if (command.contains("PRAGMA")) {
			command = noComments.replaceAll("(?is).*?(PRAGMA\\b.*)", "$1");
		}

		try {

			// query the inMemoryDatabase
			InMemoryDatabase mdb = DBManager.get(dbname);
			// for possible error windows we have to forward the main stage
			mdb.setStage(stage);
			ResultSet rs = mdb.execute(command);
			ObservableList<ObservableList> obdata = FXCollections
					.observableList(new ArrayList<>(rs.getFetchSize()));

			List<String> cnames = new ArrayList<>();

			// get column names from the resultset
			ResultSetMetaData rsmd = rs.getMetaData();
			int cols = rsmd.getColumnCount();

			int cc = rsmd.getColumnCount();
			for (int i = 1; i <= cc; i++) {
				String cname = rsmd.getColumnName(i);
				cnames.add(cname);
			}


			// iterate over the resultset an insert row by row into the tableview model
			while (rs.next()) {

				LinkedList<String> lrow = new LinkedList<>();
				for (int i = 1; i <= cols; i++) {
					lrow.add(rs.getString(i));
				}
				obdata.add(FXCollections.observableList(lrow));
			}

			// update Tableview of the result table
			fillTable(resultview, cnames, obdata);
			rs.close();


		}catch(Exception e){
			System.out.println(" parse() -> ERROR: " + command);
			e.printStackTrace();
			showErrorSQLValidation(stage, " Invalid SQL statement " + e.getMessage());
		}

	}



	private static void showErrorSQLValidation(Stage stage, String details) {
		Alert alert = new Alert(AlertType.ERROR);
		alert.setTitle("SQL Validation error");
		alert.setContentText(details);
		alert.initOwner(stage);
		alert.initModality(Modality.APPLICATION_MODAL);
		alert.showAndWait();
	}

	/**
	 * This method is used to update the table view in the SQL analyzer window with
	 * the latest result set.
	 * 
	 * @param table the TableView object to fill
	 * @param columns a list of columns for this table
	 * @param data the actual data rows as a list
	 */

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void fillTable(TableView table, List<String> columns, ObservableList<ObservableList> data) {
		/* first, remove results from last query, before continuing. */
		table.getColumns().clear();
		table.getItems().clear();

		Iterator<String> cli = columns.iterator();

		int i = 0;
		while (cli.hasNext()) {

			String colname = cli.next();
			final int j = i;
			TableColumn col = new TableColumn(colname);
			col.setCellValueFactory((Callback<CellDataFeatures<ObservableList, String>, ObservableValue<String>>) param -> new SimpleStringProperty(
                    param.getValue().get(j) != null ? param.getValue().get(j).toString() : ""));
			
			if(colname.equals(Global.col_no) || colname.equals(Global.col_tblname) 	||colname.equals(Global.col_offset) || colname.equals(Global.col_status) ||
											 colname.equals(Global.col_rowid) || colname.equals(Global.col_pll) || colname.equals(Global.col_salt2) ||
											 colname.equals(Global.col_salt1) || colname.equals(Global.col_walframe) || colname.equals(Global.col_dbpage) ||
											 colname.equals(Global.col_commit))
			{			
				col.setStyle( "-fx-text-fill: gray;-fx-alignment: TOP-RIGHT;");
				
			}

			table.getColumns().add(col);
			i++;
		}

		// finally update TableView with data set
		Platform.runLater(() -> {
			table.setItems(data);

			statusline.setText("successful select" + " | rows: " + data.size());
			statusline.setPrefHeight(50);
		});

	}


}
