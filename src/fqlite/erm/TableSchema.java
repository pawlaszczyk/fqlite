package fqlite.erm;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *  Represent a table schema.
 *  Each SQL statement is analyzed and deconstructed.
 *  @author pawlaszc
 */
public class TableSchema {
    String name;
    String createStatement;
    List<String> columns = new ArrayList<>();
    Set<String> foreignKeyReferences = new HashSet<>();

    TableSchema(String createStatement) {
        this.createStatement = createStatement;
        parse(createStatement);
    }

    private void parse(String sql) {
        int start = sql.toUpperCase().indexOf("CREATE TABLE") + 12;
        int end = sql.indexOf("(", start);
        this.name = sql.substring(start, end).trim();

        String columnPart = sql.substring(end + 1, sql.lastIndexOf(")"));
        for (String line : columnPart.split(",")) {
            line = line.trim();
            if (line.isEmpty()) continue;

            String col = line.split("\\s+")[0].trim();
            if (!isConstraint(col)) columns.add(col);

            if (line.toUpperCase().contains("REFERENCES")) {
                int refStart = line.toUpperCase().indexOf("REFERENCES") + 10;
                String refTable = line.substring(refStart).trim().split("\\s+")[0].split("\\(")[0];
                foreignKeyReferences.add(refTable);
            }
        }
    }

    private boolean isConstraint(String word) {
        String u = word.toUpperCase();
        return u.equals("FOREIGN") || u.equals("PRIMARY") || u.equals("CONSTRAINT");
    }
}
