package digital.codespiresolutions.nodeql;

import java.util.List;

public record SqlCompileResult(String sql, List<String> diagnostics) {
    public SqlCompileResult {
        sql = sql == null ? "" : sql;
        diagnostics = diagnostics == null ? List.of() : List.copyOf(diagnostics);
    }

    public SqlCompileResult(String sql) {
        this(sql, List.of());
    }
}
