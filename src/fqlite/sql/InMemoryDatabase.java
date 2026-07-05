package fqlite.sql;

import fqlite.analyzer.BLOBCache;
import fqlite.base.Global;
import fqlite.descriptor.TableDescriptor;
import fqlite.log.AppLog;
import javafx.application.Platform;
import javafx.collections.ObservableList;
import javafx.scene.control.Alert;
import javafx.stage.Modality;
import javafx.stage.Stage;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static java.sql.DriverManager.getConnection;


/// This class is used to write all recovered data into a
/// InMemory SQLite database.
///
/// This newly created database is intended to be used by the SQL Analyzer.
/// Every SELECT  statement inside the Analyzer is executed against this inMemory DB.
///
///
///
/// @author D. Pawlaszczyk
public class InMemoryDatabase {

    static{
        // load SQLite JDBC driver
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            AppLog.error(e.getMessage());
            System.err.println("SQLite JDBC driver was not found: " + e.getMessage());
        }

    }


    // this is for an internal database in RAM
    private final static String DB_URL = "jdbc:sqlite::memory:";
    /** Rows per JDBC batch flush in {@link #insertRows}; see comment there. */
    private static final int BATCH_FLUSH_SIZE = 2000;
    private Connection connection;
    private Stage stage;
    /** The logical database name (= key in {@link DBManager}). */
    private final String dbName;

    /**
     * Constructor. Used to create a new internal database object.
     * An internal database mirrors a original. It is used by
     * SQL-Analyzer for inspection purposes.
     * @param name database name
     */
    public InMemoryDatabase(String name){
        this.dbName = name;
        // create db-connection
        try {
            connection = getConnection(DB_URL);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        System.out.println("InMemoryDatabase has been started...");
        System.out.println("Connection to InMemory SQLite-database successfully created for db " + DB_URL);
    }

    public Connection getConnectionObject(){
        return connection;
    }

    /** Returns the logical name of this database (= key in {@link DBManager}). */
    public String getDbName() { return dbName; }
    public Stage getStage(){
        return stage;
    }

    public void setStage(Stage stage){
        this.stage = stage;
    }



    /**
     * This method is used to extend the original sql-statement by some fqlite columns.
     * @param tableName  name of table
     * @param columnNames  a list of all column names.
     * @param columnTypes a list with all SQL types.
     * @param isWAL true, if table belongs to an WAL-archive
     * @return the extended CREATE TABLE statement
     */
    private String createTableSql(String tableName, List<String> columnNames, List<String> columnTypes, boolean isWAL) {

        if (columnNames == null || columnTypes == null || columnNames.size() != columnTypes.size()) {
            throw new IllegalArgumentException("The number of columns and types in the lists has to be the same. Table:: " + tableName);
        }


        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(tableName).append(" ("+ Global.col_no +" INT, "+ Global.col_status+" VARCHAR(5),  "+ Global.col_offset +" BIGINT, "+ Global.col_pll+" VARCHAR(10), "+ Global.col_rowid +" BIGINT, ");

        if(isWAL)
            sql.append( Global.col_commit +  " VARCHAR(5), " + Global.col_dbpage +" INT," + Global.col_walframe +" INT, " + Global.col_salt1 + " INT, " + Global.col_salt2 +" INT, ");

        for (int i = 0; i < columnNames.size(); i++) {
            sql.append(" ")
                    .append(columnNames.get(i))
                    .append(" ")
                    .append(columnTypes.get(i));

            if (i < columnNames.size() - 1) {
                sql.append(",");
            }

        }

        sql.append(")");
        return sql.toString();
    }

    /// Creates a new SQLite database including the database schema.
    ///
    /// @param tables a list with all table descriptor objects inside.
    /// @throws SQLException in case of an error during the creation process.
    public void createDatabaseAndSchema(List<TableDescriptor> tables, String path, boolean isWAL) throws SQLException {

        Statement statement = null;

        try {


            // create statement object
            statement = connection.createStatement();

            // iterate over all Descriptor objects, extract the original SQL command and execute it
            for(TableDescriptor desc : tables) {

                if (desc.sql.isEmpty() || desc.tblname.startsWith("sqlite_"))
                    continue;

                String stm;

                if (desc.sql.contains("CREATE VIRTUAL TABLE"))
                {
                   // We skip the CREATE VIRTUAL TABLE statement since the tables have already been created.
                   // For fts4 we would also need the tokenizer. Normally this is not available in the forensic context.
                   continue;

                }
                else {

                    stm = createTableSql(desc.tblname, desc.columnnames, desc.sqltypes, isWAL);

                    stm = stm.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
                }

                System.out.println("statement‚ " + stm);

                boolean result = statement.execute(stm);

            }

        } catch (Exception e) {
            AppLog.error(e.getMessage());
            //System.err.println("SQLite JDBC driver was not found: " + e.getMessage());
            throw new SQLException("createDatabaseAndSchema():: ", e);
        } finally {
            // close resource
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    System.err.println("Error while closing last statement: " + e.getMessage());
                    AppLog.error(e.getMessage());
                }
            }
        }
    }


    /// Insert rows into a database table with a batch insert.
    ///
    /// @param tableName name of the table
    /// @param rows a list of rows
    /// @throws SQLException in case something went wrong
    public void insertRows(BLOBCache cache, String dbname, String tableName, List<TableDescriptor> tables, ObservableList<ObservableList<String>> rows, boolean isWAL) throws SQLException {
        if (rows == null || rows.isEmpty()) {
            return;
        }

        TableDescriptor desc = null;
        List<String> colNames = null;


        // first we have to find the correct TableDescriptor object from the list
        for (TableDescriptor d: tables) {
            if (d.tblname.equals(tableName)) {

                // skip virtual tables
                if(d.isVirtual())
                    return;
                desc = d;
                colNames = desc.columnnames;
                break;
            }
        }


        if (null == desc)
            return;


        PreparedStatement preparedStatement = null;
        StringBuilder sql;

        try {

            // first put the standard FQLite columns in front of the statement
            sql = new StringBuilder("INSERT INTO " + tableName + " ( " + Global.col_no + ", " + Global.col_pll + ", " + Global.col_rowid + ", " + Global.col_status + ", " + Global.col_offset + ",");

            if (isWAL)
                sql.append(" " + Global.col_commit + " , " + Global.col_dbpage + " , " + Global.col_walframe + " , " + Global.col_salt1 + " , " + Global.col_salt2 + " , ");

            for (int i = 0; i < colNames.size(); i++) {
                sql.append(" ")
                        .append(colNames.get(i));

                if (i < colNames.size() - 1) {
                    sql.append(",");
                }

            }

            sql.append(") VALUES (");

            int preColumns = 5;
            if (isWAL)
                preColumns = 10;

            // number of columns in the row without the table name column at index 1
            for (int i = 0; i < (preColumns + colNames.size()); i++) {
                sql.append("? ");
                if (i < (preColumns + colNames.size() - 1)) {
                    sql.append(", ");
                }
            }
            sql.append(")");


            preparedStatement = connection.prepareStatement(sql.toString());

            // Wrap the whole table import in a single transaction and only
            // flush the JDBC batch every BATCH_FLUSH_SIZE rows instead of
            // after every single row (see below). With autocommit left on
            // its JDBC default (true) and executeBatch()/clearBatch() called
            // once per row, every row became its own SQLite transaction —
            // for a response_records table with hundreds of thousands to
            // millions of recovered rows this turned import into that many
            // tiny commits in a row. Even for an in-memory database (no
            // fsync to disk) the per-row transaction bookkeeping and the
            // repeated JNI round-trips into SQLite add up to minutes of
            // single-threaded, silent work with no progress output — which
            // is exactly what looked like "the app is still frozen" well
            // after the LLM had already finished loading.
            connection.setAutoCommit(false);
            int batched = 0;

            // create a batch for all data rows to insert into this table
            for(ObservableList<String> record : rows) {
                // Copy each row into a random-access list ONCE: `record`
                // (like the row lists in DataAnalyzer — see the comments
                // there for the JVM thread dump that uncovered this same
                // pattern) may be backed by a sequential-access list rather
                // than an ArrayList further up the import pipeline. The
                // inner loop below does an indexed record.get(pos) for every
                // column of every row of every table during import, so on a
                // sequential list that's O(columns) per lookup instead of
                // O(1) — multiplied by every row in every table, this is a
                // real, broad contributor to the import's CPU/GC load. The
                // copy itself costs O(columns) once (an iterator walk).
                List<String> rec = new ArrayList<>(record);
                int j = 1;
                // add all lines to batch
                for (int pos = 0; pos < rec.size(); pos++,j++) {

                    if (pos == 1) {
                        // skip the column with the table name
                        j--;
                        continue;
                    }

                    String cvalue = rec.get(pos);
                    if (cvalue == null)
                        cvalue = "";

                    // BLOB handling is different
                    if (cvalue.startsWith("[BLOB-")) {
                        String key = "BLOB";
                        if(j == 38)
                            System.out.println("Stopppp");
                        try {
                            preparedStatement.setString(j, key);
                        }catch(SQLException e){
                          System.out.println("Error" + e.getMessage());
                        }
                    } else {
                        // for all remaining data types
                        switch (pos) {
                            case 0:
                                preparedStatement.setInt(j, Integer.parseUnsignedInt(cvalue));
                                break;
                            case 2, 4:
                                preparedStatement.setString(j, cvalue);
                                break;
                            case 3:
                                if(cvalue.isEmpty()) {
                                    preparedStatement.setBigDecimal(j,  BigDecimal.valueOf(0));
                                }
                                else
                                    preparedStatement.setBigDecimal(j, BigDecimal.valueOf(Long.parseLong(cvalue)));
                                break;
                            case 5:
                                if(cvalue.isEmpty()) {
                                    preparedStatement.setBigDecimal(j,  BigDecimal.valueOf(0));
                                }
                                else
                                    preparedStatement.setBigDecimal(j, BigDecimal.valueOf(Long.parseLong(cvalue)));
                                break;
                            default:
                                // let the JDBC driver do the job for you
                                preparedStatement.setObject(j, cvalue);

                        }

                    }
                }
                preparedStatement.addBatch();
                batched++;

                // Flush every BATCH_FLUSH_SIZE rows instead of every single
                // row — keeps memory bounded while avoiding a JDBC/JNI round
                // trip (and, with autocommit off, no extra transaction cost)
                // per individual row.
                if (batched >= BATCH_FLUSH_SIZE) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                    batched = 0;
                }
            }

            // flush any remaining rows that didn't fill a full batch
            if (batched > 0) {
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
            }

            // commit the single transaction that covers this whole table import
            connection.commit();

        } catch (Exception e) {
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackEx) {
                    System.err.println("Error while rollback: " + rollbackEx.getMessage());
                }
            }
            throw new SQLException("Error while Batch-Insert", e);
        } finally {
            if (connection != null) {
                try {
                    connection.setAutoCommit(true);
                } catch (SQLException e) {
                    System.err.println("Error while set back AutoCommit: " + e.getMessage());
                }
            }
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    System.err.println("Error while closing prepared statements: " + e.getMessage());
                }
            }

        }
    }

    /**
     * Execute a concrete SQL statement
     * @param select_statement the SQL statement to excute
     * @return a set of result objects
     */
    public ResultSet execute(String select_statement) {

        try {
            if (connection == null) {
                connection = getConnection(DB_URL);
            }

            if (connection.isValid(1))
                System.out.println("Connection is valid");
            else {
                System.out.println("Connection is not valid");
                diagnoseSQLiteConnection(connection);
            }
            // check syntax
            PreparedStatement pstmt = connection.prepareStatement(select_statement);

            // Execute SELECT
            return pstmt.executeQuery();

        }
        catch (SQLException e) {
            if (e.getMessage().contains("syntax error")) {
                showErrorSELECT(stage,e.getMessage());
            } else if (e.getMessage().contains("no such table")) {
                showErrorTableDoesNotExist(stage,e.getMessage());
            } else {
                showErrorDatabase(stage,e.getMessage());
            }
            //e.printStackTrace();
        }

        return null;
    }


    public void diagnoseSQLiteConnection(Connection conn) {
        try {
            System.out.println("=== SQLite Connection Diagnose ===");
            System.out.println("isClosed: " + conn.isClosed());
            System.out.println("isValid(2): " + conn.isValid(2));
            System.out.println("isReadOnly: " + conn.isReadOnly());

            DatabaseMetaData meta = conn.getMetaData();
            System.out.println("Driver: " + meta.getDriverName());
            System.out.println("Version: " + meta.getDriverVersion());
            System.out.println("URL: " + meta.getURL());

            // Test-Query
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT sqlite_version()")) {
                if (rs.next()) {
                    System.out.println("SQLite Version: " + rs.getString(1));
                }
            }
        } catch (SQLException e) {
            System.err.println("Fehler bei Diagnose: " + e.getMessage());
        }
    }


    ///
    /// Use this method to run a SELECT against the database
    ///
    public void executeQuery(String select_statement){


          try(Connection conn = connection;


              PreparedStatement pstmt = conn.prepareStatement(select_statement)) {

            // Execute SELECT
            ResultSet rs = pstmt.executeQuery();

            // generic solution
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // print column names
            System.out.println("=".repeat(80));
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(metaData.getColumnName(i));
                if (i < columnCount) {
                    System.out.print(" | ");
                }
            }
            System.out.println();
            System.out.println("-".repeat(80));

            // print row
            int rowCount = 0;
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    Object value = rs.getObject(i);
                    System.out.print(value != null ? value : "NULL");
                    if (i < columnCount) {
                        System.out.print(" | ");
                    }
                }
                System.out.println();
                rowCount++;
            }

            System.out.println("=".repeat(80));
            System.out.println("Number of records: " + rowCount + "\n");

            rs.close();

        } catch (SQLException e) {
            // Fehlerbehandlung
            if (e.getMessage().contains("syntax error")) {
                //System.err.println("SQL-Syntaxfehler: " + e.getMessage());
                showErrorSELECT(stage,e.getMessage());
            } else if (e.getMessage().contains("no such table")) {
                //System.err.println("Tabelle existiert nicht: " + e.getMessage());
                showErrorTableDoesNotExist(stage,e.getMessage());
            } else {
                //System.err.println("Datenbankfehler: " + e.getMessage());
                showErrorDatabase(stage,e.getMessage());
            }
            e.printStackTrace();
        }
    }


    /**
     * JavaFX message for the user.
     * @param stage the stage object of the parent window
     * @param message the message to display
     */
    private static void showErrorSELECT(Stage stage,String message) {
        Platform.runLater(() -> {
            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle("Syntax Error");
            alert.setContentText(message);
            alert.initOwner(stage);
            alert.initModality(Modality.APPLICATION_MODAL);
            alert.showAndWait();
        });
    }

    /**
     * JavaFX message for the user.
     * @param stage the stage object of the parent window
     * @param message the message to display
     */
    private static void showErrorDatabase(Stage stage,String message) {
        Platform.runLater(() -> {
            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle("Database Error");
            alert.setContentText(message);
            alert.initOwner(stage);
            alert.initModality(Modality.APPLICATION_MODAL);
            alert.showAndWait();
        });
    }

    /**
     * JavaFX message for the user.
     * @param stage the stage object of the parent window
     * @param message the message to display
     */
    private static void showErrorTableDoesNotExist(Stage stage,String message) {
        Platform.runLater(() -> {
            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle("Table Does Not Exist");
            alert.setContentText(message);
            alert.initOwner(stage);
            alert.initModality(Modality.APPLICATION_MODAL);
            alert.showAndWait();
        });
    }

    /**
     * close the connection of the in-memory database.
     * This method is used during cleanup.
     */
    public void closeConnection(){
        try {
            //connection.close();
            connection = null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}