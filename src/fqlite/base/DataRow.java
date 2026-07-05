package fqlite.base;

import java.util.List;

// NOTE: line/hexdump are declared as the List interface (not LinkedList) on
// purpose. Auxiliary.readRecord() — the method that builds the vast majority
// of rows during a large import (called from Job.exploreBTree() for every
// live B-tree cell) — now backs these with ArrayList instead of LinkedList.
// Two reasons this matters at scale (e.g. 1.78M rows):
//  1. Memory: a LinkedList.Node costs ~40 bytes of pure structural overhead
//     per element on top of the payload; ArrayList's overhead is just an
//     array slot. At ~20 columns/row that is tens of millions of avoidable
//     Node objects — a multi-GB difference, a real contributor to the
//     "OutOfMemoryError: Java heap space" seen on large imports.
//  2. Speed: downstream code wraps line()/hexdump() in
//     FXCollections.observableList(...) for the GUI table (see
//     RecoveryTask/WALReader/RollbackJournalReader.updateResultSet()). That
//     factory method picks ObservableListWrapper for RandomAccess lists
//     (ArrayList) but falls back to ObservableSequentialListWrapper — O(n)
//     per get(index) instead of O(1) — for non-RandomAccess lists like
//     LinkedList. This was the root cause behind the O(n^2) access patterns
//     patched downstream in earlier rounds (DataAnalyzer, InMemoryDatabase).
// Some minor/secondary paths (deleted-record carving in
// Auxiliary.readDeletedRecord()/Carver.java, and the synthetic fts/rtree
// rows in RecoveryTask) still construct LinkedLists and pass them in here —
// that still compiles fine since LinkedList implements List, so this change
// does not require touching those lower-volume paths.
public record DataRow(
    List<String> line,
    List<byte[]> hexdump
){}

