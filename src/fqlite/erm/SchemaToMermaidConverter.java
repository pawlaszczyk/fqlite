package fqlite.erm;

import java.util.*;
import java.util.regex.*;

/**
    This class creates a ERM view ready to use for
    the mermaid.js library. You only have to hand over
    the database schema with all CREATE TABLE... statements to
    this class.
    @author pawlaszc
*/
public class SchemaToMermaidConverter {
    
    static class Table {
        String name;
        List<Column> columns = new ArrayList<>();
        List<ForeignKey> foreignKeys = new ArrayList<>();
    }
    
    static class Column {
        String name;
        String type;
        boolean isPrimaryKey;
    }
    
    static class ForeignKey {
        String columnName;
        String referencedTable;
        String referencedColumn;
    }
    
    public static String convertToMermaid(String sqlSchema) {
        List<Table> tables = parseSqlSchema(sqlSchema);
        return generateMermaidDiagram(tables);
    }
    
    private static List<Table> parseSqlSchema(String sqlSchema) {
        List<Table> tables = new ArrayList<>();
        
        // Pattern für CREATE TABLE statements
        Pattern tablePattern = Pattern.compile(
            "CREATE\\s+TABLE\\s+(\\w+)\\s*\\((.*?)\\);",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );

        sqlSchema = sqlSchema.replaceAll("\\[","");
        sqlSchema = sqlSchema.replaceAll("\\]","");

        Matcher tableMatcher = tablePattern.matcher(sqlSchema);
        
        while (tableMatcher.find()) {
            Table table = new Table();
            table.name = tableMatcher.group(1);
            String tableContent = tableMatcher.group(2);
            
            // Parse Spalten und Constraints
            String[] lines = tableContent.split(",(?![^()]*\\))");
            
            for (String line : lines) {
                line = line.trim();
                
                // PRIMARY KEY constraint
                if (line.matches("(?i).*PRIMARY\\s+KEY.*")) {
                    continue; // Wird inline behandelt
                }
                
                // FOREIGN KEY constraint
                Pattern fkPattern = Pattern.compile(
                    "(?i)FOREIGN\\s+KEY\\s*\\((\\w+)\\)\\s*REFERENCES\\s+(\\w+)\\s*\\((\\w+)\\)",
                    Pattern.CASE_INSENSITIVE
                );
                Matcher fkMatcher = fkPattern.matcher(line);
                if (fkMatcher.find()) {
                    ForeignKey fk = new ForeignKey();
                    fk.columnName = fkMatcher.group(1);
                    fk.referencedTable = fkMatcher.group(2);
                    fk.referencedColumn = fkMatcher.group(3);
                    table.foreignKeys.add(fk);
                    continue;
                }
                
                // Normale Spalten-Definition
                Pattern columnPattern = Pattern.compile(
                    "(\\w+)\\s+(\\w+)(?:\\s+(PRIMARY\\s+KEY))?",
                    Pattern.CASE_INSENSITIVE
                );
                Matcher columnMatcher = columnPattern.matcher(line);
                if (columnMatcher.find()) {
                    Column column = new Column();
                    column.name = columnMatcher.group(1);
                    column.type = columnMatcher.group(2);
                    column.isPrimaryKey = columnMatcher.group(3) != null;
                    table.columns.add(column);
                }
            }
            
            tables.add(table);
        }
        
        return tables;
    }
    
    private static String generateMermaidDiagram(List<Table> tables) {
        StringBuilder mermaid = new StringBuilder();
        mermaid.append("erDiagram\n");
        
        // define tables
        for (Table table : tables) {
            mermaid.append("    ").append(table.name).append(" {\n");
            for (Column column : table.columns) {
                String type = column.type;
                String key = column.isPrimaryKey ? " PK" : "";
                mermaid.append("        ")
                       .append(type).append(" ")
                       .append(column.name)
                       .append(key)
                       .append("\n");
            }
            mermaid.append("    }\n");
        }
        
        // create relations
        for (Table table : tables) {
            for (ForeignKey fk : table.foreignKeys) {
                // n:1 Beziehung (many-to-one)
                mermaid.append("    ")
                       .append(table.name)
                       .append(" }o--|| ")
                       .append(fk.referencedTable)
                       .append(" : \"")
                       .append(fk.columnName)
                       .append("\"\n");
            }
        }
        
        return mermaid.toString();
    }
    

}
