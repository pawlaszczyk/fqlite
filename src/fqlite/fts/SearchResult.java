package fqlite.fts;

public record SearchResult(
        String tableName,
        int rowIndex,
        int colIndex,
        String cellValue
) {
    @Override
    public String toString() {
        return String.format("'%s'  —  Tabelle: %s  |  Zeile: %d  |  Spalte: %d",
                cellValue, tableName, rowIndex, colIndex);
    }
}