package fqlite.sql;

import java.util.Hashtable;

/**
 * This class manages all internal SQLite-Database for the SQL-Analyzer.
 *
 * @author pawlaszc
 */
public class DBManager {

    static Hashtable<String, InMemoryDatabase> databases = new Hashtable<>();

    /**
     * Retrieve the InMemoryDatabase object for a given db name.
     * @param dbname database name
     * @return the object to access the in-memory db
     */
    public static InMemoryDatabase get(String dbname){

        if (databases.containsKey(dbname)){
            return databases.get(dbname);
        } else  {
            InMemoryDatabase mdb = new InMemoryDatabase(dbname);
            databases.put(dbname, mdb);
            return mdb;
        }

    }

    /**
     * Check, if there is already a database object for a given key.
     * @param dbname name of the database to check
     * @return true, if db already exists
     */
    public static boolean exists(String dbname){
        return databases.containsKey(dbname);
    }

    /**
     * Reset the internal list of databases.
     */
    static void clear(){

        for (String dbname : databases.keySet()){
            databases.get(dbname).closeConnection();
        }

        databases.clear();
    }

}
