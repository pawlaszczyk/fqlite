package fqlite.base;

import javafx.beans.InvalidationListener;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.RandomAccess;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Memory-saving {@link ObservableList} for a single recovered row (one entry
 * of {@code Job.resultlist} / {@code Job.hexdumplist}).
 *
 * <p>FQLite keeps every recovered record in memory, for large databases
 * millions of rows. {@code FXCollections.observableList(new ArrayList(...))}
 * costs per row an {@code ObservableListWrapper}, an eagerly allocated
 * {@code ListChangeBuilder}, the {@code ArrayList} and its backing array with
 * spare capacity - roughly 100 bytes plus up to 50 % unused slots, before a
 * single value is stored. This class stores the values in one exactly sized
 * array and allocates listener support only if a listener is actually
 * registered (no FQLite code does that for single rows; the table views
 * observe the outer list of rows).</p>
 *
 * <p>All list operations work as before, including {@code set} (flags
 * appended by the forensic analyses), {@code add(0, ...)} (line number
 * prepended by the GUI) and {@code remove}. Registered listeners are notified
 * of every change.</p>
 *
 * <p>{@link #nulls(int)} returns a shared, unmodifiable row of {@code null}
 * values: most rows of {@code hexdumplist} hold no raw bytes at all (only
 * BLOB cells do), so they all share one instance per row length.</p>
 *
 * @param <E> element type ({@code String} for value rows, {@code byte[]} for hexdump rows)
 */
public final class CompactList<E> extends AbstractList<E> implements ObservableList<E>, RandomAccess {

    private static final Object[] EMPTY = new Object[0];

    private Object[] values;
    /** created on first registration of a listener; null in the normal case */
    private List<Object> listeners;
    private final boolean readOnly;

    private CompactList(Object[] values, boolean readOnly) {
        this.values = values;
        this.readOnly = readOnly;
    }

    /**
     * Copies {@code source} into a new, exactly sized row.
     *
     * @param source the values (not kept)
     * @return the row
     */
    public static <E> CompactList<E> of(List<? extends E> source) {
        return new CompactList<>(source == null || source.isEmpty() ? EMPTY : source.toArray(), false);
    }

    private static final ConcurrentHashMap<Integer, CompactList<?>> NULL_ROWS = new ConcurrentHashMap<>();

    /**
     * Shared, unmodifiable row of {@code n} {@code null} values.
     *
     * @param n row length
     * @return the shared instance for this length
     */
    @SuppressWarnings("unchecked")
    public static <E> CompactList<E> nulls(int n) {
        return (CompactList<E>) NULL_ROWS.computeIfAbsent(n, k -> new CompactList<>(new Object[k], true));
    }

    /**
     * Row for {@code Job.hexdumplist}: the shared {@link #nulls(int)} instance
     * if no cell holds raw bytes, otherwise a compact copy.
     *
     * @param source raw bytes per cell (not kept)
     * @return the row
     */
    public static CompactList<byte[]> hexRow(List<byte[]> source) {
        if (source == null)
            return nulls(0);
        for (byte[] b : source)
            if (b != null)
                return of(source);
        return nulls(source.size());
    }

    // ------------------------------------------------------------------ List

    @Override
    @SuppressWarnings("unchecked")
    public E get(int index) {
        if (index < 0 || index >= values.length)
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + values.length);
        return (E) values[index];
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    @SuppressWarnings("unchecked")
    public E set(int index, E element) {
        checkWritable();
        E old = get(index);
        values[index] = element;
        modCount++;
        fire(new SingleChange<>(this, index, index + 1, Collections.singletonList(old)));
        return old;
    }

    @Override
    public void add(int index, E element) {
        checkWritable();
        if (index < 0 || index > values.length)
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + values.length);
        Object[] n = new Object[values.length + 1];
        System.arraycopy(values, 0, n, 0, index);
        n[index] = element;
        System.arraycopy(values, index, n, index + 1, values.length - index);
        values = n;
        modCount++;
        fire(new SingleChange<>(this, index, index + 1, Collections.emptyList()));
    }

    @Override
    @SuppressWarnings("unchecked")
    public E remove(int index) {
        checkWritable();
        E old = get(index);
        Object[] n = values.length == 1 ? EMPTY : new Object[values.length - 1];
        System.arraycopy(values, 0, n, 0, index);
        System.arraycopy(values, index + 1, n, index, values.length - index - 1);
        values = n;
        modCount++;
        fire(new SingleChange<>(this, index, index, Collections.singletonList(old)));
        return old;
    }

    @Override
    protected void removeRange(int fromIndex, int toIndex) {
        for (int i = toIndex - 1; i >= fromIndex; i--)
            remove(i);
    }

    @Override
    public Object[] toArray() {
        return values.clone();
    }

    private void checkWritable() {
        if (readOnly)
            throw new UnsupportedOperationException("shared read-only row");
    }

    // -------------------------------------------------------- ObservableList

    @Override
    @SafeVarargs
    public final boolean addAll(E... elements) {
        return addAll(Arrays.asList(elements));
    }

    @Override
    @SafeVarargs
    public final boolean setAll(E... elements) {
        return setAll(Arrays.asList(elements));
    }

    @Override
    public boolean setAll(Collection<? extends E> col) {
        checkWritable();
        List<E> old = new ArrayList<>(this);
        values = col.toArray();
        modCount++;
        fire(new SingleChange<>(this, 0, values.length, old));
        return true;
    }

    @Override
    @SafeVarargs
    public final boolean removeAll(E... elements) {
        return removeAll(Arrays.asList(elements));
    }

    @Override
    @SafeVarargs
    public final boolean retainAll(E... elements) {
        return retainAll(Arrays.asList(elements));
    }

    @Override
    public void remove(int from, int to) {
        removeRange(from, to);
    }

    @Override
    public void addListener(ListChangeListener<? super E> listener) {
        listeners().add(listener);
    }

    @Override
    public void removeListener(ListChangeListener<? super E> listener) {
        if (listeners != null)
            listeners.remove(listener);
    }

    @Override
    public void addListener(InvalidationListener listener) {
        listeners().add(listener);
    }

    @Override
    public void removeListener(InvalidationListener listener) {
        if (listeners != null)
            listeners.remove(listener);
    }

    private List<Object> listeners() {
        if (readOnly)
            return new ArrayList<>();      // shared rows never change: nothing to notify
        if (listeners == null)
            listeners = new ArrayList<>(1);
        return listeners;
    }

    @SuppressWarnings("unchecked")
    private void fire(SingleChange<E> change) {
        if (listeners == null || listeners.isEmpty())
            return;
        for (Object l : new ArrayList<>(listeners)) {
            if (l instanceof InvalidationListener il) {
                il.invalidated(this);
            } else {
                change.reset();
                ((ListChangeListener<? super E>) l).onChanged(change);
            }
        }
    }

    /** One contiguous change: elements [from, to) added, {@code removed} removed at {@code from}. */
    private static final class SingleChange<E> extends ListChangeListener.Change<E> {
        private final int from;
        private final int to;
        private final List<E> removed;
        private boolean started;

        SingleChange(ObservableList<E> list, int from, int to, List<E> removed) {
            super(list);
            this.from = from;
            this.to = to;
            this.removed = removed;
        }

        @Override
        public boolean next() {
            if (started)
                return false;
            started = true;
            return true;
        }

        @Override
        public void reset() {
            started = false;
        }

        @Override
        public int getFrom() {
            return from;
        }

        @Override
        public int getTo() {
            return to;
        }

        @Override
        public List<E> getRemoved() {
            return removed;
        }

        @Override
        protected int[] getPermutation() {
            return new int[0];
        }
    }
}
