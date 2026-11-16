package fqlite.base;

import javafx.collections.ObservableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Lets equal cell values of recovered rows share one {@code String} instance.
 *
 * <p>Recovered values are decoded into a new {@code String} per cell, so a
 * table with millions of rows holds millions of copies of the same few
 * values (status and page/offset markers, NULL and empty values, network
 * codes, APNs, country codes, ...). In large ETSI databases about three
 * quarters of the string memory are such duplicates.</p>
 *
 * <p>Pooling is decided per table and column: a column whose values are
 * clearly unique (rowids, offsets, timestamps, session ids) is detected after
 * {@link #PROBE} values by its hit rate below 5 % (identifiers such as IMSI
 * or MSISDN repeat only later in a large table) and is no longer pooled, so the
 * pool only holds values that actually repeat. Values themselves are never
 * changed - only identical strings are replaced by one shared instance.</p>
 *
 * <p>Thread-safe; call {@link #release()} when loading is done to drop the
 * lookup table (the shared strings stay referenced by the rows).</p>
 */
public final class StringPool {

    /** values per column after which the hit rate is checked */
    private static final int PROBE = 100_000;

    private Map<String, String> pool = new HashMap<>();
    private final Map<String, long[]> stats = new HashMap<>();   // table -> {seen, hits, disabled} per column

    /**
     * Creates the row object for {@code Job.resultlist}: equal values are
     * shared, the row is stored compactly (see {@link CompactList}).
     *
     * @param line the decoded values, starting with the table name
     * @return the row
     */
    public ObservableList<String> row(List<String> line) {
        CompactList<String> row = CompactList.of(line);
        if (!row.isEmpty())
            canonicalize(row);
        return row;
    }

    private synchronized void canonicalize(CompactList<String> row) {
        String table = row.get(0);
        long[] st = stats.computeIfAbsent(table == null ? "" : table, k -> new long[3 * 64]);
        int n = row.size();
        if (st.length < 3 * n) {
            long[] bigger = new long[3 * n];
            System.arraycopy(st, 0, bigger, 0, st.length);
            st = bigger;
            stats.put(table == null ? "" : table, st);
        }
        for (int i = 0; i < n; i++) {
            String v = row.get(i);
            int s = 3 * i;
            if (v == null || st[s + 2] != 0)
                continue;
            String shared = pool.putIfAbsent(v, v);
            st[s]++;
            if (shared != null) {
                st[s + 1]++;
                if (shared != v)
                    row.set(i, shared);
            } else if (st[s] >= PROBE && st[s + 1] * 20 < st[s]) {
                st[s + 2] = 1;          // clearly unique values (hit rate < 5 %): stop pooling this column
            }
        }
    }

    /** Drops the lookup table after loading; later calls start a new one. */
    public synchronized void release() {
        pool = new HashMap<>();
        stats.clear();
    }
}
