package fqlite.blob;

import java.util.Arrays;

/**
 * Growable output buffer for the decompressors. Back-references (LZ77 matches) are resolved
 * against everything written so far; the size is capped so that a crafted or damaged BLOB
 * cannot exhaust the memory (decompression bomb).
 */
final class ByteSink {

	byte[] buf;
	int size;
	private final long limit;

	ByteSink(int initial, long limit) {
		buf = new byte[Math.max(64, Math.min(initial, 1 << 24))];
		this.limit = limit;
	}

	private void ensure(int extra) {
		long need = (long) size + extra;
		if (need > limit)
			throw new DecodeException("decompressed size exceeds the limit of " + limit + " bytes");
		if (need > buf.length) {
			long n = Math.max(need, Math.min((long) buf.length * 2, limit));
			buf = Arrays.copyOf(buf, (int) Math.min(n, Integer.MAX_VALUE - 16));
		}
	}

	void put(int b) {
		ensure(1);
		buf[size++] = (byte) b;
	}

	void put(byte[] src, int off, int len) {
		if (len < 0 || off < 0 || off + len > src.length)
			throw new DecodeException("literal run outside the input");
		ensure(len);
		System.arraycopy(src, off, buf, size, len);
		size += len;
	}

	/** copies {@code len} bytes starting {@code dist} bytes back (may overlap) */
	void match(int dist, int len) {
		if (dist <= 0 || dist > size)
			throw new DecodeException("invalid match distance " + dist + " at output position " + size);
		ensure(len);
		int from = size - dist;
		if (dist >= len) {
			System.arraycopy(buf, from, buf, size, len);
			size += len;
		} else {
			for (int i = 0; i < len; i++)
				buf[size++] = buf[from + i];
		}
	}

	void fill(int b, int len) {
		ensure(len);
		Arrays.fill(buf, size, size + len, (byte) b);
		size += len;
	}

	byte[] toArray() {
		return Arrays.copyOf(buf, size);
	}
}
