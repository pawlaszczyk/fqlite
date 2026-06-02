package fqlite.analyzer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import fqlite.base.RollbackjournalAnalyzer;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;

/**
 * Vergleicht den aktuellen Datenbankzustand mit einem Rollback-Journal.
 *
 * Kernidee der seitenbasierten Auswertung:
 *   Das Rollback-Journal enthält den Zustand der Datenbankseiten *vor* der
 *   letzten Transaktion. Wenn eine Seite im Journal vorkommt, wissen wir, dass
 *   diese Seite von der Transaktion angefasst wurde.
 *
 *   → Ein Datensatz, der in der DB auf einer solchen Journal-Seite liegt, aber
 *     im Journal-Snapshot nicht vorhanden ist, wurde während der Transaktion
 *     *definitiv neu eingefügt* (CONFIRMED).
 *
 *   → Ein Datensatz, der im Journal vorkommt, aber nicht mehr in der DB,
 *     wurde *definitiv gelöscht* (CONFIRMED).
 *
 *   → Bei Tabellen, die gar nicht im Journal auftauchen, ist kein Vergleich
 *     möglich (ONLY_IN_DB) – die Tabelle wurde schlicht nicht angefasst.
 */
public class DatabaseComparator {

    // =========================================================================
    // Status-Enum
    // =========================================================================

    public enum Status {
        /** Tabelle in beiden Maps – zeilenweiser Diff durchgeführt */
        CHANGED,
        /** Tabelle nur in der DB, nicht im Journal – kein Vergleich möglich */
        ONLY_IN_DB,
        /** Tabelle nur im Journal – in der DB vollständig gelöscht */
        ONLY_IN_JOURNAL
    }

    // =========================================================================
    // Konfidenz-Enum für einzelne Zeilen
    // =========================================================================

    public enum Confidence {
        /**
         * Seite war im Journal: Einfügung/Löschung ist beweisbar.
         * Die Transaktion hat diese Seite definitiv modifiziert.
         */
        CONFIRMED,
        /**
         * Seite war NICHT im Journal: Die Änderung ist rechnerisch sichtbar,
         * aber wir können sie nicht eindeutig der letzten Transaktion zuordnen.
         */
        UNCONFIRMED
    }

    // =========================================================================
    // RowDiff – eine einzelne geänderte Zeile mit Konfidenz
    // =========================================================================

    public static class RowDiff {
        public final ObservableList<String> row;
        public final Confidence confidence;

        public RowDiff(ObservableList<String> row, Confidence confidence) {
            this.row        = row;
            this.confidence = confidence;
        }

        @Override
        public String toString() {
            return String.format("[%s] %s", confidence, row);
        }
    }

    // =========================================================================
    // TableDiff – Ergebnis für eine Tabelle
    // =========================================================================

    public static class TableDiff {
        public final String tableName;
        public Status status;

        /** In DB vorhanden, nicht im Journal-Snapshot (neu eingefügt) */
        public final List<RowDiff> inserted;
        /** Im Journal-Snapshot vorhanden, nicht in DB (gelöscht) */
        public final List<RowDiff> deleted;
        /** In beiden vorhanden, aber Inhalt geändert */
        public final List<RowDiff> modified;

        public TableDiff(String tableName) {
            this.tableName = tableName;
            this.status    = Status.CHANGED;
            this.inserted  = new ArrayList<>();
            this.deleted   = new ArrayList<>();
            this.modified  = new ArrayList<>();
        }

        /** Factory: Tabelle fehlt im Journal – kein Diff möglich */
        public static TableDiff onlyInDb(String tableName) {
            TableDiff d = new TableDiff(tableName);
            d.status = Status.ONLY_IN_DB;
            return d;
        }

        /** Factory: Tabelle fehlt in der DB – alle Journal-Zeilen sind gelöscht */
        public static TableDiff onlyInJournal(String tableName,
                                              ObservableList<ObservableList<String>> journalRows,
                                              boolean pagesKnown) {
            TableDiff d = new TableDiff(tableName);
            d.status = Status.ONLY_IN_JOURNAL;
            Confidence conf = pagesKnown ? Confidence.CONFIRMED : Confidence.UNCONFIRMED;
            for (ObservableList<String> row : journalRows) {
                row.set(1,"deleted");
                d.deleted.add(new RowDiff(row, conf));
            }
            return d;
        }

        public boolean hasChanges() {
            return !inserted.isEmpty() || !deleted.isEmpty() || !modified.isEmpty();
        }

        /** Liefert nur die bestätigten Einfügungen */
        public List<ObservableList<String>> getConfirmedInserts() {
            return inserted.stream()
                    .filter(r -> r.confidence == Confidence.CONFIRMED)
                    .map(r -> r.row)
                    .collect(Collectors.toList());
        }

        /** Liefert nur die bestätigten Löschungen */
        public List<ObservableList<String>> getConfirmedDeletes() {
            return deleted.stream()
                    .filter(r -> r.confidence == Confidence.CONFIRMED)
                    .map(r -> r.row)
                    .collect(Collectors.toList());
        }

        @Override
        public String toString() {
            return switch (status) {
                case ONLY_IN_DB ->
                        String.format("Tabelle '%s': nur in DB – nicht im Journal (nicht angefasst)",
                                tableName);
                case ONLY_IN_JOURNAL ->
                        String.format("Tabelle '%s': nur im Journal – %d Zeilen gelöscht",
                                tableName, deleted.size());
                default ->
                        String.format(
                                "Tabelle '%s': %d eingefügt (%d bestätigt), " +
                                "%d gelöscht (%d bestätigt), %d geändert",
                                tableName,
                                inserted.size(), getConfirmedInserts().size(),
                                deleted.size(),  getConfirmedDeletes().size(),
                                modified.size());
            };
        }
    }

    // =========================================================================
    // Haupt-Vergleichsmethode
    // =========================================================================

    /**
     * Vergleicht Datenbank-Map und Journal-Map unter Berücksichtigung der
     * Seiteninformation aus dem RollbackjournalAnalyzer.
     *
     * @param dbMap          Aktuelle Datenbank        (Schlüssel = Tabellenname)
     * @param journalMap     Rollback-Journal-Snapshot  (Schlüssel = Tabellenname)
     * @param journalRecords PageRecord-Liste aus RollbackjournalAnalyzer.analyzeJournal()
     *                       (enthält für jede Journal-Seite den zugehörigen Tabellennamen)
     * @param pkColumnIndex  Index der Primärschlüsselspalte (meist 0)
     * @return Map Tabellenname → TableDiff
     */
    public static Map<String, TableDiff> compare(
            ConcurrentHashMap<String, ObservableList<ObservableList<String>>> dbMap,
            ConcurrentHashMap<String, ObservableList<ObservableList<String>>> journalMap,
            List<RollbackjournalAnalyzer.PageRecord> journalRecords,
            int pkColumnIndex) {

        // Welche Tabellen haben mindestens eine Seite im Journal?
        // PageRecord.ownerName() liefert den Tabellennamen direkt aus dem Analyzer.
        Set<String> tablesWithJournalPages = journalRecords.stream()
                .map(RollbackjournalAnalyzer.PageRecord::ownerName)
                .collect(Collectors.toSet());

        Map<String, TableDiff> result = new LinkedHashMap<>();

        Set<String> allTables = new HashSet<>();
        allTables.addAll(dbMap.keySet());
        allTables.addAll(journalMap.keySet());

        for (String table : allTables) {
            boolean inDb       = dbMap.containsKey(table);
            boolean inJournal  = journalMap.containsKey(table);
            boolean pagesKnown = tablesWithJournalPages.contains(table);

            if (inDb && !inJournal) {
                // Tabelle fehlt im Journal-Snapshot:
                //   Wenn Seiten dieser Tabelle trotzdem im Journal sind, wurden
                //   vielleicht alle Zeilen gelöscht und neu eingefügt – wir melden
                //   ONLY_IN_DB, damit die UI den Nutzer gezielt informieren kann.
                //   Ohne Seitentreffer: Tabelle wurde schlicht nicht angefasst.
                result.put(table, TableDiff.onlyInDb(table));

            }
            else if (!inDb && inJournal) {
                // Tabelle existiert nicht mehr in der DB
                result.put(table, TableDiff.onlyInJournal(table, journalMap.get(table), pagesKnown));

            } else {
                // Tabelle in beiden Maps → zeilenweiser Vergleich mit Konfidenz
                TableDiff diff = compareTable(
                        table,
                        dbMap.get(table),
                        journalMap.get(table),
                        pkColumnIndex,
                        pagesKnown
                );
                if (diff.hasChanges()) {
                    result.put(table, diff);
                }
            }
        }

        return result;
    }

    /**
     * Überladung ohne PageRecord-Liste – alle Zeilen erhalten Konfidenz UNCONFIRMED.
     */
    public static Map<String, TableDiff> compare(
            ConcurrentHashMap<String, ObservableList<ObservableList<String>>> dbMap,
            ConcurrentHashMap<String, ObservableList<ObservableList<String>>> journalMap,
            int pkColumnIndex) {
        return compare(dbMap, journalMap, Collections.emptyList(), pkColumnIndex);
    }

    // =========================================================================
    // Interner Zeilenvergleich
    // =========================================================================

    private static TableDiff compareTable(
            String tableName,
            ObservableList<ObservableList<String>> dbRows,
            ObservableList<ObservableList<String>> journalRows,
            int pkColumnIndex,
            boolean pagesInJournal) {

        TableDiff diff = new TableDiff(tableName);

        Map<String, ObservableList<String>> dbByPk      = indexByPk(dbRows,      pkColumnIndex);
        Map<String, ObservableList<String>> journalByPk = indexByPk(journalRows, pkColumnIndex);

        // --- Eingefügte und geänderte Zeilen (iteriere über DB) ---
        for (Map.Entry<String, ObservableList<String>> entry : dbByPk.entrySet()) {
            String pk    = entry.getKey();
            ObservableList<String> dbRow = entry.getValue();

            if (!journalByPk.containsKey(pk)) {
                // PK nicht im Journal-Snapshot:
                //   Seite war im Journal → Einfügung während der Transaktion bewiesen
                //   Seite war nicht im Journal → Herkunft unbekannt
                Confidence conf = pagesInJournal ? Confidence.CONFIRMED : Confidence.UNCONFIRMED;

                // inserted
                diff.inserted.add(new RowDiff(dbRow, conf));
            } else {
                ObservableList<String> journalRow = journalByPk.get(pk);
                if (!rowsEqual(dbRow, journalRow)) {
                    Confidence conf = pagesInJournal ? Confidence.CONFIRMED : Confidence.UNCONFIRMED;
                    diff.modified.add(new RowDiff(dbRow, conf));
                }
            }
        }

        // --- Gelöschte Zeilen (iteriere über Journal) ---
        for (Map.Entry<String, ObservableList<String>> entry : journalByPk.entrySet()) {
            if (!dbByPk.containsKey(entry.getKey())) {
                Confidence conf = pagesInJournal ? Confidence.CONFIRMED : Confidence.UNCONFIRMED;
                diff.deleted.add(new RowDiff(entry.getValue(), conf));
                //for (ObservableList<String> e : journalRows){
                //    System.out.println("bin drin...");
                //    if(e.get(2).equals(entry.getKey())){
                //            e.set(3,"deleted");
                //    }

                //}
            }

        }

        return diff;
    }

    // =========================================================================
    // Hilfsmethoden
    // =========================================================================

    private static Map<String, ObservableList<String>> indexByPk(
            ObservableList<ObservableList<String>> rows,
            int pkColumnIndex) {
        Map<String, ObservableList<String>> map = new LinkedHashMap<>();
        for (ObservableList<String> row : rows) {
            if (row != null && row.size() > pkColumnIndex) {
                map.put(row.get(pkColumnIndex), row);
            }
        }
        return map;
    }

    private static boolean rowsEqual(ObservableList<String> a, ObservableList<String> b) {
        if (a.size() != b.size()) return false;
        for (int i = 0; i < a.size(); i++) {
            String va = a.get(i) == null ? "" : a.get(i);
            String vb = b.get(i) == null ? "" : b.get(i);
            if (!va.equals(vb)) return false;
        }
        return true;
    }

    // =========================================================================
    // Beispiel-Verwendung
    // =========================================================================

    public static void main(String[] args) {
        ConcurrentHashMap<String, ObservableList<ObservableList<String>>> dbMap =
                new ConcurrentHashMap<>();
        ConcurrentHashMap<String, ObservableList<ObservableList<String>>> journalMap =
                new ConcurrentHashMap<>();

        // journalRecords kommt aus: RollbackjournalAnalyzer.analyzeJournal(reader)
        // danach: reader.records
        List<RollbackjournalAnalyzer.PageRecord> journalRecords = new ArrayList<>();

        Map<String, TableDiff> differences =
                DatabaseComparator.compare(dbMap, journalMap, journalRecords, 0);

        for (TableDiff diff : differences.values()) {
            System.out.println(diff);

            if (diff.status == Status.CHANGED) {
                System.out.println("  Eingefügt:");
                diff.inserted.forEach(r ->
                        System.out.printf("    [%s] %s%n", r.confidence, r.row));

                System.out.println("  Gelöscht:");
                diff.deleted.forEach(r ->
                        System.out.printf("    [%s] %s%n", r.confidence, r.row));

                System.out.println("  Geändert (aktueller DB-Stand):");
                diff.modified.forEach(r ->
                        System.out.printf("    [%s] %s%n", r.confidence, r.row));
            }
        }
    }
}
