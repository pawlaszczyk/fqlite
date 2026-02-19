package fqlite.parser;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used to validate a sql-statement from master table.
 *
 *
 */
public class SQLValidator {

    // Result class for detailed feedback
    public static class ValidationResult {
        private final boolean valid;
        private final List<String> errors;

        public ValidationResult(boolean valid, List<String> errors) {
            this.valid = valid;
            this.errors = errors;
        }

        public boolean isValid() { return valid; }
        public List<String> getErrors() { return errors; }

        @Override
        public String toString() {
            return valid ? "✓ Valid" : "✗ Invalid: " + String.join(", ", errors);
        }
    }

    /**
     * Main method: Validates a SQL string for non-printable characters and basic syntax correctness.
     */
    public static ValidationResult validate(String sql) {
        List<String> errors = new ArrayList<>();

        // 1. Null check
        if (sql == null) {
            errors.add("SQL string is null");
            return new ValidationResult(false, errors);
        }

        // 2. Blank check
        if (sql.isBlank()) {
            errors.add("SQL string is empty");
            return new ValidationResult(false, errors);
        }

        // 3. Check for non-printable characters
        checkNonPrintableChars(sql, errors);

        // 4. Check basic SQL syntax
        checkSqlSyntax(sql.trim(), errors);

        // 5. Check for SQL injection patterns (optional, treated as warning)
        checkSqlInjectionPatterns(sql, errors);

        return new ValidationResult(errors.isEmpty(), errors);
    }

    /**
     * Checks for non-printable or unwanted control characters.
     */
    private static void checkNonPrintableChars(String sql, List<String> errors) {
        List<String> found = new ArrayList<>();

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);

            // Allowed whitespace characters: tab, newline, carriage return
            boolean isAllowedWhitespace = (c == '\t' || c == '\n' || c == '\r');

            // Non-printable ASCII characters (0x00–0x1F and 0x7F), except allowed whitespace
            if ((c < 0x20 || c == 0x7F) && !isAllowedWhitespace) {
                found.add(String.format("Position %d: U+%04X", i, (int) c));
            }

            // Explicitly check for null byte
            if (c == '\0') {
                found.add(String.format("Null byte at position %d", i));
            }

            // Unicode control characters (e.g. BOM, soft hyphen)
            if (Character.getType(c) == Character.CONTROL && !isAllowedWhitespace) {
                found.add(String.format("Control character at position %d: U+%04X", i, (int) c));
            }
        }

        if (!found.isEmpty()) {
            errors.add("Non-printable characters found: " + String.join("; ", found));
        }
    }

    /**
     * Checks basic SQL syntax (statement type, parentheses balance, quote balance).
     */
    private static void checkSqlSyntax(String sql, List<String> errors) {
        String upper = sql.toUpperCase();

        // Known SQL statement types
        boolean startsWithKnownKeyword =
                upper.startsWith("SELECT") ||
                upper.startsWith("INSERT") ||
                upper.startsWith("UPDATE") ||
                upper.startsWith("DELETE") ||
                upper.startsWith("CREATE") ||
                upper.startsWith("DROP")   ||
                upper.startsWith("ALTER")  ||
                upper.startsWith("WITH")   ||
                upper.startsWith("PRAGMA") ||
                upper.startsWith("EXPLAIN");

        if (!startsWithKnownKeyword) {
            errors.add("SQL does not start with a known keyword");
        }

        // Check parentheses balance
        int depth = 0;
        for (char c : sql.toCharArray()) {
            if (c == '(') depth++;
            else if (c == ')') depth--;
            if (depth < 0) {
                errors.add("Unbalanced parentheses: too many closing parentheses");
                break;
            }
        }
        if (depth > 0) {
            errors.add("Unbalanced parentheses: " + depth + " opening parenthesis/parentheses not closed");
        }

        // Check quote balance (single and double quotes)
        checkQuoteBalance(sql, '\'', "Single quotes", errors);
        checkQuoteBalance(sql, '"',  "Double quotes", errors);

        // Semicolon at end (optional but recommended)
        // if (!sql.endsWith(";")) {
        //     errors.add("Missing semicolon at end of statement");
        // }
    }

    /**
     * Checks whether quotes are properly opened and closed.
     */
    private static void checkQuoteBalance(String sql, char quote, String name, List<String> errors) {
        boolean inside = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == quote) {
                // Skip escaped quotes (e.g. '' in SQL)
                if (inside && i + 1 < sql.length() && sql.charAt(i + 1) == quote) {
                    i++; // skip the second quote character
                } else {
                    inside = !inside;
                }
            }
        }
        if (inside) {
            errors.add(name + " not closed");
        }
    }

    /**
     * Warns about typical SQL injection patterns.
     */
    private static void checkSqlInjectionPatterns(String sql, List<String> errors) {
        String upper = sql.toUpperCase();

        // Common SQL injection patterns
        String[] patterns = {
                "' OR '1'='1",
                "' OR 1=1",
                "'; DROP TABLE",
                "--",           // SQL comment as injection vector
                "/*",           // Block comment
                "UNION SELECT",
                "EXEC(",
                "EXECUTE(",
                "XP_CMDSHELL"
        };

        for (String pattern : patterns) {
            if (upper.contains(pattern.toUpperCase())) {
                errors.add("Possible SQL injection pattern detected: \"" + pattern + "\"");
            }
        }
    }

    // -------------------------------------------------------------------------
    // Example usage
    // -------------------------------------------------------------------------
    public static void main(String[] args) {
        String[] testCases = {
                "SELECT * FROM WiFiLocation WHERE id = 1",
                "SELECT * FROM WiFiAWD WHERE name = 'Test'",
                "INVALID SQL STRING",
                "SELECT * FROM test WHERE x = 'unclosed",
                "SELECT * FROM test\0WHERE id=1",           // Null byte
                "SELECT * FROM test WHERE x = '' OR 1=1",   // Injection
                "SELECT * FROM test WHERE (id = 1",         // Unclosed parenthesis
        };

        for (String sql : testCases) {
            System.out.println("SQL:    " + sql.replace("\0", "\\0"));
            System.out.println("Result: " + validate(sql));
            System.out.println();
        }
    }
}