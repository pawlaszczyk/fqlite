package fqlite.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import fqlite.log.AppLog;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import fqlite.base.GUI;
import fqlite.ui.FQTableView;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Node;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableColumn.CellDataFeatures;
import javafx.scene.control.TableView;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import javafx.util.Callback;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;


/**
 * This class is used to evaluate the sql-statements. After checking the syntax
 * of the SELECT statement, the Parser loads the in-memory tables. Finally the
 * query is executed using the JDBC-interface. The resultset are automatically  
 * written to a table view. 
 * 
 * @author Dirk Pawlaszczyk
 */
public class SQLParser {

	/* define a CALCITE schema object first */
	SchemaPlus rootSchema;
	CalciteConnection calciteConnection;
	HashMap<String, String> dbname2schema = new HashMap<String, String>();
	HashMap<String, MemTableSchema> subschemas = new HashMap<String, MemTableSchema>();
	static Label statusline;

	/**
	 * Parse and execute the user-defined SQL statement.
	 * 
	 * @param command
	 * @param dbname
	 * @param gui
	 * @param stage
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public String parse(String command, String dbname, GUI gui, Stage stage, TableView resultview, Label statusline) {

		Table table = null;
		Select select = null;
		SQLParser.statusline = statusline;
		String jtbname = null;

		try {
			//We use JSqlParser to validate the statement in the first step
			select = (Select) CCJSqlParserUtil.parse(command);
		} catch (JSQLParserException e) {
			e.printStackTrace();
			showErrorNonValid(stage);
			return null;
		}
		if (null == select) {
			showErrorSELECT(stage);
			return null;
		}

		/* get the FROM table name */
		PlainSelect ps = select.getPlainSelect();
		table = (Table) ps.getFromItem();

		/* get all parts of the table name including alias*/
		List<String> parts = table.getNameParts();

		/* the table name should be the first element in the name parts list */
		String kk = parts.get(0);

		/*  check the combo box to determine the currently selected database */
		//List<SelectItem<?>> selectlist = ps.getSelectItems();

		// build table path (the same as in the tree)
		String tablekey = "Databases/" + dbname + "/" + kk; // table;
		//System.out.println("SQLParser -> tablekey " + tablekey);

		// get table rows from the already existing dataset
		//Enumeration<String> keys = gui.datasets.keys();

		/* the table data are already in memory -> we just have to use it */
		ObservableList<ObservableList<String>> tb = gui.datasets.get(tablekey);

		
		String schemaid = null;
		// Is there already a table schema for the given database?
		if (!dbname2schema.containsKey(dbname)) {
			
			// for internal managing, we need a unique schema ID for each database
			schemaid = "schema" + (subschemas.size() + 1);

			// remember the connection between the database and chosen
			dbname2schema.put(dbname, schemaid);
		} else {
			// use the already existing schema
			schemaid = dbname2schema.get(dbname);
		}

		//Did the table path really exist?
		if (null != tb) {

			Node nd = gui.tables.get(tablekey);

			if (nd instanceof VBox) {

				VBox vb = (VBox) nd;
				FQTableView tbl = (FQTableView) vb.getChildren().get(1);

				try {

					MemTableSchema schema;

					if (null == subschemas.get(schemaid)) {

						/**
						 * Check if the table has been added to the analyser already.
						 */
						schema = new MemTableSchema();
						// register the newly defined schema to the root schema
						// the database name is used as the schema name
						rootSchema.add(schemaid, schema);
						subschemas.put(schemaid, schema);
					} else {
						schema = subschemas.get(schemaid);
					}

					parts = table.getNameParts();

					String tbname = parts.get(0); 

					// now we are ready to create the missing table
					if (schema.getTable(tbname) == null) {
						//System.out.println(" create new table::" + tbname);
						//System.out.println(" number of columns:: " + tbl.columns.size());
						//System.out.println(" number of types:: " + tbl.columntypes.size());

						prepareTable(tbl, schema, tbname);

						//System.out.println(" Fill table :: " + tbname);
						// fill the table of our calcite schema with the references of our memory table
						try {
							schema.fill(tbname, tb);
						} catch (Exception err) {
                            AppLog.error(err.getMessage());
						}
					}

					List<Join> joins = ps.getJoins();

					/*
					 * Is there a JOIN statement??? Before we can actually fire the statement we
					 * have to load the involved tables to our in-memory database.
					 */
					if (null != joins) {

						Iterator<Join> itj = joins.iterator();
						while (itj.hasNext()) {

							Join j = itj.next();
							Table jtable = (Table) j.getFromItem();
							//System.out.println("join table:: " + jtable);

							List<String> jparts = jtable.getNameParts();

							// only the table name
							String jkk = jparts.get(0);

							// build table path (the same as in the tree)
							String jtablekey = "Databases/" + dbname + "/" + jkk;

							Node jnd = gui.tables.get(jtablekey);

							if (jnd instanceof VBox) {

								VBox jvb = (VBox) jnd;
								FQTableView jtbl = (FQTableView) jvb.getChildren().get(1);

								MemTableSchema jschema;

								if (null == subschemas.get(schemaid)) {

									/**
									 * Check, if the table has been added to the analyser already.
									 */
									jschema = new MemTableSchema();
									// register the newly defined schema to the root schema
									// the database name is used as the schema name
									rootSchema.add(schemaid, jschema);
								} else {
									jschema = subschemas.get(schemaid);
								}

								jtbname = jtable.getName();

								// now we are ready to create the missing table
								if (jschema.getTable(jtbname) == null) {
								//	System.out.println(" create new table::" + jtbname);
								//	System.out.println(" number of columns:: " + jtbl.columns.size());
								//	System.out.println(" number of types:: " + jtbl.columntypes.size());
									prepareTable(jtbl, jschema, jtbname);

									// fill the table of our calcite schema with the references of our memory table
									try {
										ObservableList<ObservableList<String>> jtb = gui.datasets.get(jtablekey);
										if (null != jtb)
											jschema.fill(jtbname, jtb);
									} catch (Exception err) {
										showErrorFillTable(stage); 
										err.printStackTrace();
									}
								}
							}
						}
					}

					/* get a statement object */
					Statement statement = calciteConnection.createStatement();

					// calcite does not accept semicolons inside the SELECT statement
					command = command.replaceAll(";", "");
					// important: tbname will be expanded to schemaid.tbname

					System.out.println("§§§ tablename " + tbname);
					System.out.println("§§§ jtablename " + jtbname);
					
					System.out.println("For replace " + command);
					command = command.replaceFirst(" " + tbname, " " + schemaid + "." + tbname);

					if (jtbname != null)
						command = command.replaceFirst(" " + jtbname, " " + schemaid + "." + jtbname);

					
					
					System.out.println(" parse() -> ExcecuteQuery :: " + command);

					// let us fire the query
					// statement.executeQuery("SELECT COUNT(*) FROM schema1.batchStatus");
					ResultSet rs = statement.executeQuery(command);

					List<String> cnames = new ArrayList<String>();

					ResultSetMetaData rsmd = rs.getMetaData();
					int cols = rsmd.getColumnCount();

					int cc = rsmd.getColumnCount();
					for (int i = 1; i <= cc; i++) {
						String cname = rsmd.getColumnName(i);
						cnames.add(cname);
					}

					int counter = 0;
					ObservableList<ObservableList> obdata = FXCollections
							.observableList(new ArrayList<ObservableList>(rs.getFetchSize()));

					while (rs.next()) {

						LinkedList<String> lrow = new LinkedList<String>();
						for (int i = 1; i <= cols; i++) {

							lrow.add(rs.getString(i));
							//lrow.add(0, rs.getString(i));
						}
						//rlist.add(lrow);
						//lrow.remove(1);
						obdata.add(FXCollections.observableList(lrow));
						//if (counter % 10000 == 0)
						//	System.out.println("processed" + counter + " of " + rs.getFetchSize());
						counter++;
					}

					// update Tableview of the result table
					fillTable(resultview, cnames, obdata, false);

					// rlist = null;
					rs.close();
					statement.close();
					calciteConnection.close();

					return "";

				} catch (Exception e) {
					e.printStackTrace();
					showErrorSQLValidation(stage, " Invalid SQL statement " + e.getMessage());
				}

			}
			return "";

		} else {
			Alert alert = new Alert(AlertType.ERROR);
			alert.setTitle("Error");
			alert.setContentText("Couldn't find table " + dbname + "." + table + " .");
			alert.initOwner(stage);
			alert.showAndWait();
			return null;
		}

	}

	/**
	 * This method is used to add column types and column names.
	 * @param tbl
	 * @param schema
	 * @param tbname
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void prepareTable(FQTableView tbl, MemTableSchema schema, String tbname) {

		if (tbl.columns.get(0).equals("commit")) {

			// Attention: WAL-Archive table
			List<String> cextended = new ArrayList<String>();
			cextended.add("_NO");
			cextended.add("_TBLNAME");
			cextended.add("_PLL|HL");
			cextended.add("_ROWID");
			cextended.add("_STATUS");
			cextended.add("_OFFSET");
			cextended.addAll(tbl.columns);

			List<String> textended = new ArrayList<String>();
			textended.add("String");
			textended.add("String");
			textended.add("String");
			textended.add("String");
			textended.add("String");
			textended.add("String");
			textended.add("String");
			textended.add("String");
			textended.add("String");
			textended.add("String");
			textended.add("String");
			textended.addAll(tbl.columntypes);

			/* create the memory table structure */
			schema.createTable(tbname, cextended, textended);

		} else {
			// Normal database table -> add the standard fields to columns & types

			List<String> cextended = new ArrayList<String>();
			cextended.add("_NO");
			cextended.add("_TBLNAME");	
			cextended.add("_PLL|HL");
			cextended.add("_ROWID");
			cextended.add("_ST");
			cextended.add("_OFFSET");
			cextended.addAll(tbl.columns);

			List<String> textended = new ArrayList<String>();
			textended.add("INT");
			textended.add("String");			
			textended.add("INT");
			textended.add("INT");
			textended.add("String");
			textended.add("String");
			textended.addAll(tbl.columntypes);

			int diff = cextended.size() - textended.size();

			while (diff > 0) {
				textended.add("String");
				diff--;
			}

			/* create the memory table structure */
			schema.createTable(tbname, cextended, textended);

		}
	}

	private static void showErrorFillTable(Stage stage) {
		Alert alert = new Alert(AlertType.ERROR);
		alert.setTitle("Create Table Error");
		alert.setContentText("Could not create In-Memory table.");
		alert.initOwner(stage);
		alert.showAndWait();
	}
	
	private static void showErrorSELECT(Stage stage) {
		Alert alert = new Alert(AlertType.ERROR);
		alert.setTitle("Syntax Error");
		alert.setContentText("Please use a SELECT statement.");
		alert.initOwner(stage);
		alert.showAndWait();
	}

	private static void showErrorNonValid(Stage stage) {
		Alert alert = new Alert(AlertType.ERROR);
		alert.setTitle("Syntax Error");
		alert.setContentText("No valid statement. You can only use SELECT in this environment.");
		alert.initOwner(stage);
		alert.showAndWait();
	}

	private static void showErrorSQLValidation(Stage stage, String details) {
		Alert alert = new Alert(AlertType.ERROR);
		alert.setTitle("SQL Validation error");
		alert.setContentText(details);
		alert.initOwner(stage);
		alert.showAndWait();
	}

	/**
	 * This method is used to update the table view in the SQL analyzer window with
	 * the latest result set.
	 * 
	 * @param table
	 * @param columns
	 * @param data
	 * @param rowno
	 */

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void fillTable(TableView table, List<String> columns, ObservableList<ObservableList> data, boolean rowno) {
		/* first, remove results from last query, before continuing. */
		table.getColumns().clear();
		table.getItems().clear();

		Iterator<String> cli = columns.iterator();

		int i = 0;
		while (cli.hasNext()) {

			String colname = cli.next();
			final int j = i;
			TableColumn col = new TableColumn(colname);
			col.setCellValueFactory(new Callback<CellDataFeatures<ObservableList, String>, ObservableValue<String>>() {
				public ObservableValue<String> call(CellDataFeatures<ObservableList, String> param) {
					return new SimpleStringProperty(
							param.getValue().get(j) != null ? param.getValue().get(j).toString() : "");
				}
			});
			
			if( colname.equals("_TBLNAME") 	||  colname.equals("_OFFSET") 	|| colname.equals("_NO") || 
				colname.equals("_ROWID") || colname.equals("_PLL|HL") || colname.equals("salt2") || 
				colname.equals("salt1") || colname.equals("walframe") 	|| colname.equals("dbpage") || 
				colname.equals("commit"))
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

	/**
	 * The CALCITE "database" can be accessed with a normal jdbc-driver. Remember:
	 * This database is only managing java in-memory data structures.
	 */
	public void connectToInMemoryDatabase() {
		try {
			// load the official calcite jdbc driver
			Class.forName("org.apache.calcite.jdbc.Driver");

			Properties info = new Properties();
			info.setProperty("lex", "JAVA");

			// establish connection the calcite framework
			Connection connection = DriverManager.getConnection("jdbc:calcite:", info);

			this.calciteConnection = connection.unwrap(CalciteConnection.class);

			// we need add a self defined schema to the root schema
			this.rootSchema = calciteConnection.getRootSchema();

		} catch (Exception err) {
            AppLog.error(err.getMessage());
		}
	}
}
