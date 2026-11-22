package fqlite.headless;

import java.util.List;

/**
 * Schema object (table or index) FQLite found in the database - also tables that only
 * survive in deleted schema records - plus FQLite's internal buckets (name starting with
 * {@code "__"}, e.g. records that could not be assigned to a table).
 *
 * @author pawlaszc
 */
public final class TableInfo {

	public enum Kind { TABLE, INDEX, INTERNAL }

	private final String name;
	private final Kind kind;
	private final List<String> columnNames;
	private final List<String> columnTypes;
	private final String sql;

	TableInfo(String name, Kind kind, List<String> columnNames, List<String> columnTypes, String sql) {
		this.name = name;
		this.kind = kind;
		this.columnNames = columnNames == null ? List.of() : List.copyOf(columnNames);
		this.columnTypes = columnTypes == null ? List.of() : List.copyOf(nonNull(columnTypes));
		this.sql = sql;
	}

	private static List<String> nonNull(List<String> l) {
		return l.stream().map(s -> s == null ? "" : s).toList();
	}

	public String getName() { return name; }

	public Kind getKind() { return kind; }

	/** column names of the schema (without FQLite's meta columns) */
	public List<String> getColumnNames() { return columnNames; }

	/** declared SQL types of the columns (may be empty for an index) */
	public List<String> getColumnTypes() { return columnTypes; }

	/** CREATE statement from sqlite_master, if known */
	public String getSql() { return sql; }

	@Override
	public String toString() {
		return kind + " " + name + columnNames;
	}
}
