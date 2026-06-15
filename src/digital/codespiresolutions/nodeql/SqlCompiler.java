package digital.codespiresolutions.nodeql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class SqlCompiler {
    public SqlCompileResult compileWorkspace(List<BlockNode> roots) {
        if (roots == null || roots.isEmpty()) {
            return new SqlCompileResult("");
        }

        List<String> statements = new ArrayList<>();
        List<String> diagnostics = new ArrayList<>();
        Set<BlockNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());

        for (BlockNode root : roots) {
            compileChain(root, statements, diagnostics, visited);
        }

        return new SqlCompileResult(String.join("\n", statements), diagnostics);
    }

    private void compileChain(
            BlockNode root,
            List<String> statements,
            List<String> diagnostics,
            Set<BlockNode> visited) {
        BlockNode current = root;
        while (current != null) {
            if (!visited.add(current)) {
                diagnostics.add("Cycle detected at block " + current.id());
                return;
            }

            if (current.type() != BlockType.EVENT_GREEN_FLAG) {
                String sql = compileBlock(current, diagnostics);
                if (!sql.isBlank()) {
                    statements.add(terminate(sql));
                }
            }

            for (BlockNode child : current.children()) {
                compileChain(child, statements, diagnostics, visited);
            }
            current = current.next();
        }
    }

    private String compileBlock(BlockNode node, List<String> diagnostics) {
        Map<String, Object> inputs = node.inputs();
        return switch (node.type()) {
            case EVENT_GREEN_FLAG -> "";
            case SQL_SELECT -> "SELECT " + value(inputs, "columns", "*")
                    + "\nFROM " + value(inputs, "table", "table_name");
            case SQL_COLUMN -> value(inputs, "column", "*");
            case SQL_FROM -> "FROM " + value(inputs, "table", "table_name");
            case SQL_WHERE -> "WHERE " + value(inputs, "predicate", "1 = 1");
            case SQL_GROUP_BY -> "GROUP BY " + value(inputs, "expr", "id");
            case SQL_HAVING -> "HAVING " + value(inputs, "predicate", "COUNT(*) > 0");
            case SQL_ORDER_BY -> "ORDER BY " + value(inputs, "expr", "id DESC");

            case SQL_JOIN -> join("JOIN", inputs);
            case SQL_INNER_JOIN -> join("INNER JOIN", inputs);
            case SQL_LEFT_JOIN -> join("LEFT JOIN", inputs);
            case SQL_RIGHT_JOIN -> join("RIGHT JOIN", inputs);
            case SQL_FULL_JOIN -> join("FULL JOIN", inputs);
            case SQL_CROSS_JOIN -> "CROSS JOIN " + value(inputs, "table", "table_name");
            case SQL_NATURAL_JOIN -> "NATURAL JOIN " + value(inputs, "table", "table_name");
            case SQL_SELF_JOIN -> "JOIN " + value(inputs, "table", "table_name")
                    + " t2 ON " + value(inputs, "on", "t1.id = t2.id");
            case SQL_UNION -> "UNION " + value(inputs, "sql", "SELECT 1");
            case SQL_INTERSECT -> "INTERSECT " + value(inputs, "sql", "SELECT 1");
            case SQL_EXCEPT -> "EXCEPT " + value(inputs, "sql", "SELECT 1");
            case SQL_SUBQUERY_IN -> subquery(inputs, "IN");
            case SQL_SUBQUERY_ANY -> subquery(inputs, "ANY");
            case SQL_SUBQUERY_ALL -> subquery(inputs, "ALL");

            case SQL_COUNT -> function("COUNT", inputs, "expr", "*");
            case SQL_SUM -> function("SUM", inputs, "expr", "amount");
            case SQL_AVG -> function("AVG", inputs, "expr", "amount");
            case SQL_MIN -> function("MIN", inputs, "expr", "amount");
            case SQL_MAX -> function("MAX", inputs, "expr", "amount");

            case SQL_CONCAT -> binaryFunction("CONCAT", inputs, "a", "''", "b", "''");
            case SQL_SUBSTRING -> "SUBSTRING(" + value(inputs, "expr", "''") + ", "
                    + value(inputs, "start", "1") + ", " + value(inputs, "len", "1") + ")";
            case SQL_LENGTH -> function("LENGTH", inputs, "expr", "''");
            case SQL_UPPER -> function("UPPER", inputs, "expr", "''");
            case SQL_LOWER -> function("LOWER", inputs, "expr", "''");
            case SQL_TRIM -> function("TRIM", inputs, "expr", "''");
            case SQL_LEFT -> binaryFunction("LEFT", inputs, "expr", "''", "n", "1");
            case SQL_RIGHT -> binaryFunction("RIGHT", inputs, "expr", "''", "n", "1");
            case SQL_REPLACE -> "REPLACE(" + value(inputs, "expr", "''") + ", "
                    + value(inputs, "from", "''") + ", " + value(inputs, "to", "''") + ")";
            case SQL_CURRENT_DATE -> "CURRENT_DATE";
            case SQL_CURRENT_TIME -> "CURRENT_TIME";
            case SQL_CURRENT_TIMESTAMP -> "CURRENT_TIMESTAMP";
            case SQL_DATE_PART -> binaryFunction("DATE_PART", inputs, "part", "'day'", "expr", "CURRENT_DATE");
            case SQL_DATE_ADD -> "DATE_ADD(" + value(inputs, "expr", "CURRENT_DATE") + ", INTERVAL "
                    + value(inputs, "n", "1") + " " + value(inputs, "unit", "DAY") + ")";
            case SQL_DATE_SUB -> "DATE_SUB(" + value(inputs, "expr", "CURRENT_DATE") + ", INTERVAL "
                    + value(inputs, "n", "1") + " " + value(inputs, "unit", "DAY") + ")";
            case SQL_EXTRACT -> "EXTRACT(" + value(inputs, "part", "DAY") + " FROM "
                    + value(inputs, "expr", "CURRENT_DATE") + ")";
            case SQL_TO_CHAR -> binaryFunction("TO_CHAR", inputs, "expr", "CURRENT_DATE", "fmt", "'YYYY-MM-DD'");
            case SQL_TIMESTAMP_DIFF -> "TIMESTAMPDIFF(" + value(inputs, "unit", "DAY") + ", "
                    + value(inputs, "a", "CURRENT_DATE") + ", " + value(inputs, "b", "CURRENT_DATE") + ")";
            case SQL_DATE_DIFF -> binaryFunction("DATEDIFF", inputs, "a", "CURRENT_DATE", "b", "CURRENT_DATE");
            case SQL_CASE -> "CASE WHEN " + value(inputs, "when", "1=1") + " THEN "
                    + value(inputs, "then", "'x'") + " ELSE " + value(inputs, "else", "'y'") + " END";
            case SQL_IF -> "IF(" + value(inputs, "cond", "1=1") + ", "
                    + value(inputs, "a", "'x'") + ", " + value(inputs, "b", "'y'") + ")";
            case SQL_COALESCE -> binaryFunction("COALESCE", inputs, "a", "NULL", "b", "NULL");
            case SQL_NULL_IF -> binaryFunction("NULLIF", inputs, "a", "1", "b", "1");

            case SQL_INSERT -> "INSERT INTO " + value(inputs, "table", "table_name")
                    + " VALUES (" + value(inputs, "values", "") + ")";
            case SQL_UPDATE -> "UPDATE " + value(inputs, "table", "table_name")
                    + " SET " + value(inputs, "set", "col = value");
            case SQL_DELETE -> "DELETE FROM " + value(inputs, "table", "table_name");

            case SQL_CREATE_TABLE -> "CREATE TABLE " + value(inputs, "table", "new_table")
                    + " (" + value(inputs, "definition", "id INTEGER PRIMARY KEY") + ")";
            case SQL_ALTER_TABLE -> "ALTER TABLE " + value(inputs, "table", "table_name")
                    + " " + value(inputs, "alter", "ADD COLUMN c TEXT");
            case SQL_TRUNCATE -> "TRUNCATE TABLE " + value(inputs, "table", "table_name");
            case SQL_DROP_TABLE -> "DROP TABLE " + value(inputs, "table", "table_name");

            case SQL_GRANT -> "GRANT " + value(inputs, "privilege", "SELECT")
                    + " ON " + value(inputs, "table", "table_name")
                    + " TO " + value(inputs, "user", "user");
            case SQL_REVOKE -> "REVOKE " + value(inputs, "privilege", "SELECT")
                    + " ON " + value(inputs, "table", "table_name")
                    + " FROM " + value(inputs, "user", "user");

            case SQL_COMMIT -> "COMMIT";
            case SQL_ROLLBACK -> "ROLLBACK";
            case SQL_SAVEPOINT -> "SAVEPOINT " + value(inputs, "name", "sp1");
            case SQL_ROLLBACK_TO_SAVEPOINT -> "ROLLBACK TO SAVEPOINT " + value(inputs, "name", "sp1");
            case SQL_SET_TRANSACTION -> "SET TRANSACTION ISOLATION LEVEL "
                    + value(inputs, "level", "READ COMMITTED");
        };
    }

    private String join(String keyword, Map<String, Object> inputs) {
        return keyword + " " + value(inputs, "table", "table_name")
                + " ON " + value(inputs, "on", "1 = 1");
    }

    private String subquery(Map<String, Object> inputs, String keyword) {
        return value(inputs, "lhs", "id") + " " + keyword
                + " (" + value(inputs, "sql", "SELECT id FROM t") + ")";
    }

    private String function(
            String name,
            Map<String, Object> inputs,
            String key,
            String fallback) {
        return name + "(" + value(inputs, key, fallback) + ")";
    }

    private String binaryFunction(
            String name,
            Map<String, Object> inputs,
            String firstKey,
            String firstFallback,
            String secondKey,
            String secondFallback) {
        return name + "(" + value(inputs, firstKey, firstFallback)
                + ", " + value(inputs, secondKey, secondFallback) + ")";
    }

    private String value(Map<String, Object> inputs, String key, String fallback) {
        Object raw = inputs.get(key);
        if (raw == null) {
            return fallback;
        }
        String value = String.valueOf(raw).trim();
        return value.isEmpty() ? fallback : value;
    }

    private String terminate(String sql) {
        String trimmed = sql.trim();
        return trimmed.endsWith(";") ? trimmed : trimmed + ";";
    }
}
