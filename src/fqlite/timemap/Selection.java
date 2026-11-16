package fqlite.timemap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The rowids of the records matching a condition, evaluated once and kept in
 * a shared in-memory SQLite database. Conditions with subqueries (e.g. the
 * "Wechsler" and "Wanderer" templates of the query assistant) are expensive;
 * evaluated again for counting, loading, every timeline page and every
 * identifier list they would take many times as long. With a selection those
 * steps use the cheap condition {@link #condition()} instead.
 *
 * <p>The database file stays untouched: the rowids live in a separate
 * in-memory database ({@code file:<name>?mode=memory&cache=shared}) that is
 * attached to the read-only connections by name (see {@link #attach}). It
 * exists as long as {@link #close()} has not been called or a connection still
 * has it attached.</p>
 */
public final class Selection implements AutoCloseable {

    private static final AtomicInteger COUNTER = new AtomicInteger();

    private final String name;
    private final Connection holder;
    private final long count;
    /** the rowids in memory as well, or null if one does not fit into an int */
    private final java.util.BitSet rows;

    private Selection(String name, Connection holder, long count, java.util.BitSet rows) {
        this.name = name;
        this.holder = holder;
        this.count = count;
        this.rows = rows;
    }

    /**
     * Evaluates {@code cond} once on {@code read} and stores the matching rowids.
     *
     * @param read connection to the database (read-only is fine)
     * @param cond SQL condition on {@code response_records}
     */
    public static Selection create(Connection read, String cond) throws SQLException {
        return create(read, cond, null, null);
    }

    /** decides per row whether it belongs to the selection */
    public interface RowFilter {
        /** @param rs positioned on the row: column 1 = rowid, then the extra columns */
        boolean accept(ResultSet rs) throws SQLException;
    }

    /**
     * As {@link #create(Connection, String)}, but keeps only the rows of
     * {@code cond} accepted by {@code filter}, which sees {@code extraColumns}
     * - e.g. membership in a set of identifiers computed beforehand, instead
     * of a subquery that would group the whole table once more.
     */
    public static Selection create(Connection read, String cond, String extraColumns, RowFilter filter) throws SQLException {
        String name = "lh_sel_" + COUNTER.incrementAndGet();
        Connection holder = DriverManager.getConnection("jdbc:sqlite:file:" + name + "?mode=memory&cache=shared");
        long n = 0;
        java.util.BitSet bits = new java.util.BitSet();
        try {
            try (Statement st = holder.createStatement()) {
                st.execute("CREATE TABLE r(rid INTEGER PRIMARY KEY)");
            }
            holder.setAutoCommit(false);
            try (Statement st = read.createStatement();
                 ResultSet rs = st.executeQuery("SELECT rowid" + (extraColumns != null ? ", " + extraColumns : "")
                         + " FROM response_records WHERE " + cond);
                 PreparedStatement ins = holder.prepareStatement("INSERT OR IGNORE INTO r VALUES (?)")) {
                while (rs.next()) {
                    if (filter != null && !filter.accept(rs)) continue;
                    long id = rs.getLong(1);
                    ins.setLong(1, id);
                    if (bits != null) {
                        if (id >= 0 && id <= Integer.MAX_VALUE) bits.set((int) id);
                        else bits = null;
                    }
                    ins.addBatch();
                    if (++n % 50_000 == 0) ins.executeBatch();
                }
                ins.executeBatch();
            }
            holder.commit();
            holder.setAutoCommit(true);
            try (Statement st = holder.createStatement(); ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM r")) {
                n = rs.next() ? rs.getLong(1) : n;
            }
        } catch (SQLException | RuntimeException e) {
            holder.close();
            throw e;
        }
        return new Selection(name, holder, n, bits);
    }

    /**
     * The selected rowids as a bit set (about 2 MB for 16 million rows), or
     * {@code null} if a rowid does not fit into an int. Lets a pass over the
     * table check membership in memory instead of in SQL.
     */
    public java.util.BitSet rows() { return rows; }

    /** number of selected records */
    public long count() { return count; }

    /**
     * Condition selecting these records; valid on connections that have this
     * selection {@link #attach attached}. The unary plus keeps SQLite from
     * looking up every rowid and lets it use the index of the rest of the
     * query (e.g. {@code start_time} for the timeline pages).
     */
    public String condition() {
        return "+rowid IN " + name + ".r";
    }

    /**
     * Condition selecting these records by looking up each rowid - faster than
     * {@link #condition()} when nothing else restricts the query (loading a
     * small selection completely).
     */
    public String lookupCondition() {
        return "rowid IN " + name + ".r";
    }

    /** Makes the selection available on {@code c} (once per connection). */
    public void attach(Connection c) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute("ATTACH DATABASE 'file:" + name + "?mode=memory&cache=shared' AS " + name);
        }
    }

    /** true if {@code cond} refers to this selection */
    public boolean isUsedBy(String cond) {
        return cond != null && cond.contains(name + ".r");
    }

    @Override
    public void close() {
        try { holder.close(); } catch (SQLException ignored) { }
    }
}
