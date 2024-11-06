package fqlite.sql;

import java.util.Iterator;
import java.util.stream.Stream;

public final class StreamIterable<T> implements Iterable<T> {

    private final Stream<T> stream;

    StreamIterable(Stream<T> stream) {
        this.stream = stream;
    }

    @Override
    public Iterator<T> iterator() {
        return stream.iterator();
    }

    public static <T> StreamIterable<T> of(Stream<T> stream) {
        return new StreamIterable<>(stream);
    }
}