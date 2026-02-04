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
    private Connection connection;
    private Stage stage;

    /**
     * Constructor. Used to create a new internal database object.
     * An internal database mirrors a original. It is used by
     * SQL-Analyzer for inspection purposes.
     * @param name database name
     */
    public InMemoryDatabase(String name){

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
    public Stage getStage(){
        return stage;
    }

    public void setStage(Stage stage){
        this.stage = stage;
    }

    /**
     * The freelist has to be created separately since it is not part
     * of the original database schema.
     * @param tdefault table information object
     * @param isWAL is write-ahead log
     * @return the final CREATE TABLE statement for the freelist
     */
    private static String createFREEList(TableDescriptor tdefault,boolean isWAL) {

        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS fqlite_freelist ("+ Global.col_no +" INT, "+ Global.col_status +" VARCHAR(5), "+ Global.col_offset +" BIGINT, "+ Global.col_pll +" VARCHAR(10), "+ Global.col_rowid+" BIGINT, ");

        if(isWAL)
            sql.append(" " + Global.col_commit + " VARCHAR(5), " + Global.col_dbpage +  " INT, " + Global.col_walframe + " INT, " + Global.col_salt1 +  " INT, " + Global.col_salt2 + " INT, ");

        int i = -1;

        for (String c: tdefault.columnnames){

            i++;
            // skip some of the internal fields (e.g. Commit Column is replaced during export with FQLite_Commit)
            if( i < 5)
                continue;

            sql.append(" ")
                    .append(c)
                    .append(" ")
                    .append("TEXT,");

        }
        // remove last comma
        sql.deleteCharAt(sql.length()-1);

        sql.append(")");
        return sql.toString();
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
    public void createDatabaseAndSchema(List<TableDescriptor> tables, TableDescriptor tdefault, String path, boolean isWAL) throws SQLException {

        Statement statement = null;

        try {


            // create statement object
            statement = connection.createStatement();

            // iterate over all Descriptor objects, extract the original SQL command and execute it
            for(TableDescriptor desc : tables) {

                if (desc.sql.isEmpty() || desc.tblname.startsWith("sqlite_"))
                    continue;

                String stm;

                stm = createTableSql(desc.tblname,desc.columnnames,desc.sqltypes, isWAL);

                stm = stm.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
                boolean result = statement.execute(stm);

            }

            // create table fqlite_freelist
            String freelist = createFREEList(tdefault,isWAL);
            statement.executeUpdate(freelist);

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
    public void insertRows(BLOBCache cache, String dbname, String tableName, List<TableDescriptor> tables, TableDescriptor tdefault, ObservableList<ObservableList<String>> rows, boolean isWAL) throws SQLException {
        if (rows == null || rows.isEmpty()) {
            return;
        }

        TableDescriptor desc = null;
        List<String> colNames = null;

        if (tableName.startsWith("fqlite_freelist")) {
            desc = tdefault;
            colNames = desc.columnnames.subList(5,desc.columnnames.size()-1);
        }
        else {
            // first we have to find the correct TableDescriptor object from the list
            for (TableDescriptor d: tables) {
                if (d.tblname.equals(tableName)) {
                    desc = d;
                    colNames = desc.columnnames;
                    break;
                }
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

            // create a batch for all data rows to insert into this table
            for(ObservableList<String> record : rows) {
                int j = 1;
                // add all lines to batch
                for (int pos = 0; pos < record.size(); pos++,j++) {

                    if (pos == 1) {
                        // skip the column with the table name
                        j--;
                        continue;
                    }

                    String cvalue = record.get(pos);
                    if (cvalue == null)
                        cvalue = "";

                    // BLOB handling is different
                    if (record.get(pos).startsWith("[BLOB-")) {
                        String key = "BLOB";
                        //String key = getBLOBKey(cvalue,dbname,record.get(5));
                        //   BLOBElement b = cache.get(key);
                           // write byte-array to statement
                        //   preparedStatement.setBytes(j,b.binary);
                        preparedStatement.setString(j,key);
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

                // execute directly
                preparedStatement.executeBatch();

                // clear parameters before inserting next row
                preparedStatement.clearBatch();
            }

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

        } catch (SQLException e) {
            if (e.getMessage().contains("syntax error")) {
                showErrorSELECT(stage,e.getMessage());
            } else if (e.getMessage().contains("no such table")) {
                showErrorTableDoesNotExist(stage,e.getMessage());
            } else {
                showErrorDatabase(stage,e.getMessage());
            }
            e.printStackTrace();
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