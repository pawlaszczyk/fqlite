package fqlite.nodeql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/** Read-only schema discovery for simple table references, never evaluation of custom SQL. */
final class ColumnSuggestions {
    private static final String IDENTIFIER = "(?:\"(?:[^\"]|\"\")+\"|`(?:[^`]|``)+`|\\[[^\\]]+\\]|[\\p{L}_][\\p{L}\\p{N}_$]*)";
    private static final Pattern REFERENCE = Pattern.compile("^\\s*(" + IDENTIFIER + ")"
            + "(?:\\s*\\.\\s*(" + IDENTIFIER + "))?"
            + "(?:\\s+(?:(?i:AS)\\s+)?(" + IDENTIFIER + "))?\\s*$");

    record Table(String schema, String name, String qualifier) {
        static Table parse(String text, String explicitAlias) {
            var match = REFERENCE.matcher(text);
            if (!match.matches()) return null;
            String schema = match.group(2) == null ? "" : unquote(match.group(1));
            String name = unquote(match.group(2) == null ? match.group(1) : match.group(2));
            String alias = explicitAlias.isBlank() ? match.group(3) : explicitAlias.strip();
            return new Table(schema, name, alias == null || alias.isBlank() ? name : unquote(alias));
        }
    }

    private record Column(Table table, String name) {}

    static String quote(String name) { return "\"" + name.replace("\"", "\"\"") + "\""; }

    /** Compare identifier spellings without confusing a quoted dot with qualification. */
    static String referenceKey(String sql) {
        String text = sql.strip();
        if (text.equals("*")) return "*";
        if (text.endsWith(".*")) {
            String prefix = referenceKey(text.substring(0, text.length() - 2));
            return prefix == null ? null : prefix + "\u0000*";
        }
        var match = REFERENCE.matcher(text);
        if (!match.matches() || match.group(3) != null) return null;
        String first = unquote(match.group(1)).toLowerCase(Locale.ROOT);
        return match.group(2) == null ? "U:" + first
                : "Q:" + first + "\u0000" + unquote(match.group(2)).toLowerCase(Locale.ROOT);
    }

    private static String unquote(String text) {
        if (text.length() < 2) return text;
        char first = text.charAt(0), last = text.charAt(text.length() - 1);
        if ((first == '"' || first == '`') && first == last) {
            String delimiter = String.valueOf(first);
            return text.substring(1, text.length() - 1).replace(delimiter + delimiter, delimiter);
        }
        if (first == '[' && last == ']') return text.substring(1, text.length() - 1);
        return text;
    }

    static List<SchemaInput.Suggestion> load(Connection connection, List<Table> tables, boolean wildcard) {
        List<Column> columns = new ArrayList<>();
        for (Table table : tables.stream().distinct().toList()) {
            for (String name : columns(connection, table)) columns.add(new Column(table, name));
        }
        Map<String, Integer> occurrences = new LinkedHashMap<>();
        columns.forEach(column -> occurrences.merge(column.name().toLowerCase(Locale.ROOT), 1, Integer::sum));
        List<SchemaInput.Suggestion> suggestions = new ArrayList<>();
        if (wildcard) suggestions.add(new SchemaInput.Suggestion("*", "*"));
        for (Column column : columns) {
            if (occurrences.get(column.name().toLowerCase(Locale.ROOT)) == 1) {
                suggestions.add(new SchemaInput.Suggestion(column.name(), quote(column.name())));
            }
        }
        for (Table table : tables.stream().distinct().toList()) {
            if (wildcard) suggestions.add(new SchemaInput.Suggestion(table.qualifier() + ".*", quote(table.qualifier()) + ".*"));
            for (Column column : columns) {
                if (!column.table().equals(table)) continue;
                suggestions.add(new SchemaInput.Suggestion(table.qualifier() + "." + column.name(),
                        quote(table.qualifier()) + "." + quote(column.name())));
            }
        }
        return suggestions.stream().distinct().toList();
    }

    private static List<String> columns(Connection connection, Table table) {
        // Match SQLite's normal lookup precedence for unqualified main/temp names.
        List<String> schemas = table.schema().isEmpty() ? List.of("temp", "main") : List.of(table.schema());
        for (String schema : schemas) {
            List<String> names = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT name FROM pragma_table_xinfo(?, ?) WHERE hidden <> 1 ORDER BY cid")) {
                statement.setString(1, table.name());
                statement.setString(2, schema);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) names.add(result.getString(1));
                }
            } catch (SQLException exception) {
                // Optional suggestions must not prevent editing when metadata is unavailable.
                continue;
            }
            if (!names.isEmpty()) return names;
        }
        return List.of();
    }
}
