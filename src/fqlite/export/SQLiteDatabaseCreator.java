package fqlite.export;

import fqlite.analyzer.BLOBCache;
import fqlite.base.GUI;
import fqlite.base.Global;
import fqlite.descriptor.TableDescriptor;
import fqlite.log.AppLog;
import fqlite.types.BLOBElement;
import javafx.collections.ObservableList;
import java.math.BigDecimal;
import java.sql.*;
import java.util.List;

/// This class is used to export tables from a recovery run into
/// a new SQLite database.
///
/// @author D. Pawlaszczyk
public class SQLiteDatabaseCreator {

    private String DB_NAME;
    private String DB_URL;
    private static SQLiteDatabaseCreator instance;

    /**
     * Private Constructor. We use a Singleton pattern here.
     */
    private SQLiteDatabaseCreator(){

    }

    /**
     * Public Constructor. To get the runtime object of this class.
     * @return the runtime object of this class
     */
    public static SQLiteDatabaseCreator getInstance(){
        if (instance == null)
            instance =  new SQLiteDatabaseCreator();

        return instance;
    }


    private static String createFREEList(TableDescriptor tdefault,boolean isWAL) {

        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE fqlite_freelist (" + Global.col_no +" INT, "+ Global.col_status +" VARCHAR(5), "+ Global.col_offset +" BIGINT, "+ Global.col_pll +" VARCHAR(10), "+ Global.col_rowid  + " BIGINT, ");

        if(isWAL)
            sql.append(" " + Global.col_commit + " VARCHAR(5)," + Global.col_dbpage  +" INT, "+ Global.col_walframe +" INT,"+ Global.col_salt1 +" INT, " + Global.col_salt2 +" INT, ");

        int cnt = tdefault.columnnames.size();
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

    /// Quotes an identifier (table or column name) for use in a CREATE TABLE statement,
    /// so that names like "order" or names with blanks do not break the statement.
    public static String quoteIdentifier(String name) {
        if (name == null) return "\"\"";
        String n = name.trim();
        if (n.length() > 1 && ((n.startsWith("\"") && n.endsWith("\"")) || (n.startsWith("[") && n.endsWith("]"))
                || (n.startsWith("`") && n.endsWith("`"))))
            n = n.substring(1, n.length() - 1);
        return "\"" + n.replace("\"", "\"\"") + "\"";
    }

    /// Returns a declared column type that is safe to use in a CREATE TABLE statement.
    /// Column constraints that ended up in the type text (e.g. "REFERENCESconversations(id)")
    /// are dropped; without a usable type name the column gets no type (BLOB affinity).
    public static String safeColumnType(String type) {
        if (type == null) return "";
        String t = type.trim();
        String u = t.toUpperCase();
        for (String k : new String[]{"REFERENCES", "CONSTRAINT", "PRIMARY", "NOT", "NULL", "UNIQUE", "CHECK", "DEFAULT", "COLLATE", "GENERATED"})
            if (u.startsWith(k)) return "";
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("^[A-Za-z_][A-Za-z0-9_ ]*(\\(\\s*[-+]?[0-9.]+\\s*(,\\s*[-+]?[0-9.]+\\s*)?\\))?").matcher(t);
        return m.find() ? m.group().trim() : "";
    }

    private static String createTableSql(String tableName, List<String> columnNames, List<String> columnTypes, boolean isWAL) {
        if (columnNames == null || columnTypes == null || columnNames.size() != columnTypes.size()) {
            throw new IllegalArgumentException("The number of columns and types in the lists has to be the same.");
        }

        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(tableName).append(" ("+ Global.col_no +" INT, "+ Global.col_status +" VARCHAR(5),  "+ Global.col_offset +" BIGINT, "+ Global.col_pll  +" VARCHAR(10), "+ Global.col_rowid +" BIGINT, ");

        if(isWAL)
            sql.append(" " + Global.col_commit + " VARCHAR(5)," + Global.col_dbpage  + " INT, " +  Global.col_walframe + " INT, " + Global.col_salt1  + " INT, "+ Global.col_salt2  +" INT, ");

        for (int i = 0; i < columnNames.size(); i++) {
            sql.append(" ")
                    .append(quoteIdentifier(columnNames.get(i)))
                    .append(" ")
                    .append(safeColumnType(columnTypes.get(i)));

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
        Connection connection = null;
        Statement statement = null;

        try {
            // load SQLite JDBC driver
            Class.forName("org.sqlite.JDBC");

            DB_NAME = path;
            DB_URL = "jdbc:sqlite:" + path;

            // create db-connection
            connection = DriverManager.getConnection(DB_URL);

            // create statement object
            statement = connection.createStatement();

            // iterate over all Descriptor objects, extract the original SQL command and execute it
            for(TableDescriptor desc : tables) {
                if (desc.sql.isEmpty() || desc.tblname.startsWith("sqlite_") || desc.isVirtual())
                    continue;
                if (desc.isVirtual()) {
                    continue;
                }

                String stm = createTableSql(desc.tblname,desc.columnnames,desc.sqltypes, isWAL);

                // execute CREATE TABLE Statement
                int rowcnt = statement.executeUpdate(stm);
            }


            // close all
            statement.close();


        } catch (ClassNotFoundException e) {
            AppLog.error(e.getMessage());
            System.err.println("SQLite JDBC driver was not found: " + e.getMessage());
            throw new SQLException("No JDBC driver available.", e);
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
            if (connection != null) {
                try {
                    //connection.close();
                } catch (Exception e) {
                    AppLog.error(e.getMessage());
                    System.err.println("Error while closing the database connection: " + e.getMessage());
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
                if(d.isVirtual())
                    return;
                desc = d;
                colNames = desc.columnnames;
                break;
            }
        }


        if (null == desc)
            return;


        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            connection = DriverManager.getConnection(DB_URL);
            connection.setAutoCommit(false); // for better performance when inserting many records at once

            int columnCount = rows.size();

            // first put the standard FQLite columns in front of the statement
            StringBuilder sql = new StringBuilder("INSERT INTO " + tableName + " ( "+Global.col_no+", "+Global.col_pll+", "+Global.col_rowid+", "+Global.col_status+", "+Global.col_offset+",");

            if(isWAL)
                sql.append(" " + Global.col_commit + ", " + Global.col_dbpage + ", " + Global.col_walframe + ", " + Global.col_salt1 + " , " + Global.col_salt2 + ", ");

            for (int i = 0; i < colNames.size(); i++) {
                sql.append(" ")
                        .append(quoteIdentifier(colNames.get(i)));

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
                           String key = getBLOBKey(cvalue,dbname,record.get(5));
                           BLOBElement b = cache.get(key);
                           if (b != null) {
                           // write byte-array to statement
                           preparedStatement.setBytes(j,b.binary);
                           }
                    } else {
                        // for all remaining data types
                        switch (pos) {
                            case 0:
                                preparedStatement.setInt(j, Integer.parseUnsignedInt(cvalue));
                                break;
                            case 2:
                                preparedStatement.setString(j, cvalue);
                                break;
                            case 3:
                                preparedStatement.setBigDecimal(j, BigDecimal.valueOf(Long.parseLong(cvalue)));
                                break;
                            case 4:
                                preparedStatement.setString(j, cvalue);
                                break;
                            case 5:
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
                int[] results = preparedStatement.executeBatch();

                // clear parameters before inserting next row
                preparedStatement.clearBatch();
            }

            // make changes persistent with a commit
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
            if (connection != null) {
                try {
                    //connection.close();
                } catch (Exception e) {
                    System.err.println("Error while closing connection to database: " + e.getMessage());
                }
            }
        }
    }

    public static String getBLOBKey(String cellValue, String dbname, String off) {


        int from = cellValue.indexOf("-");
        int to = cellValue.indexOf("]");

        int number = Integer.parseInt(cellValue.substring(from+1, to));

        long hash = -1;
        if (off.length() > 2) {
            try {
                hash = Long.parseLong(off);
            } catch (NumberFormatException e) {
                System.out.println("Number format exception: compute hash failed ");
            }
        } else {
            hash = 0;
        }


        if (from > 0 && to > 0) {

            String base = hash + "-" + number;

            return base;

        }

        return null;
    }
}