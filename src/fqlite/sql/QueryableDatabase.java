package fqlite.sql;

import java.sql.Connection;
import java.sql.ResultSet;

/// Common read access to a SQLite database that the Lighthouse plugin
/// (map, timeline, analysis presets, query assistant) runs its queries on.
///
/// Two implementations exist:
///
/// - [InMemoryDatabase]: a RAM copy of the records FQLite recovered from a
///   file, including deleted records. Needed when the source file is damaged
///   or when deleted records are to be analysed, but it holds the whole
///   database in memory.
/// - [ReadOnlySqliteDatabase]: the database file opened directly and strictly
///   read-only through JDBC. Nothing is copied into RAM, so it also works for
///   databases of several gigabytes, and it uses the indexes the ETSI importer
///   created.
///
/// @author D. Pawlaszczyk
public interface QueryableDatabase {

    /// Executes a SELECT statement.
    ///
    /// @param sql the statement
    /// @return the result, or `null` if the statement failed (the error has
    ///         already been reported to the user)
    ResultSet execute(String sql);

    /// @return the JDBC connection, e.g. for schema inspection by the query assistant
    Connection getConnectionObject();

    /// @return a logical name of the database (used in titles and log output)
    String getDbName();
}
