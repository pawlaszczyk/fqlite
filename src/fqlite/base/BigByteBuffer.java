package fqlite.base;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * BigByteBuffer with streaming / demand-paging support.
 *
 * Instead of mapping the entire file into memory at once, only the pages that
 * are actually touched are mapped. Evicted pages are unmapped immediately so
 * the JVM can release the native memory.
 *
 * Design parameters (tuneable via constructor overloads):
 *   PAGE_SIZE   – how many bytes one mmap-window covers  (default 8 MiB)
 *   MAX_PAGES   – how many windows stay resident at once  (default 8)
 *
 * Memory ceiling ≈ PAGE_SIZE × MAX_PAGES  (default ≈ 64 MiB).
 *
 * Performance notes:
 *   • Sequential reads hit the same page many times → effectively zero overhead.
 *   • Random reads across MAX_PAGES distinct windows are served from cache.
 *   • Only cross-page reads (a single logical read spanning two windows) require
 *     a slow-path copy; all other reads are a single native buffer.get().
 *
 * Lifecycle: BigByteBuffer owns the RandomAccessFile it opens internally.
 * If a RandomAccessFile is passed in by the caller, BigByteBuffer takes
 * ownership of it as well — do NOT close the RAF externally afterwards.
 * Call {@link #close()} when done to release all resources.
 */
public class BigByteBuffer {

	// -----------------------------------------------------------------------
	// Tuneable defaults
	// -----------------------------------------------------------------------

	/** Default page size: 8 MiB — large enough to amortise mmap overhead,
	 *  small enough to keep per-page cost low. */
	public static final int DEFAULT_PAGE_SIZE = 8 * 1024 * 1024;

	/** Default number of resident pages (LRU cache capacity). */
	public static final int DEFAULT_MAX_PAGES = 8;

	// -----------------------------------------------------------------------
	// State
	// -----------------------------------------------------------------------

	/**
	 * Kept alive for the entire lifetime of this buffer.
	 * Closing the RAF would also close its FileChannel, making all
	 * subsequent mmap calls fail with "channel not open".
	 * Null only when constructed from a plain ByteBuffer.
	 */
	private final RandomAccessFile raf;
	private final FileChannel      channel;
	private final long             fileLength;
	private final int              pageSize;

	/** LRU page cache: pageIndex → mapped ByteBuffer. */
	private final LinkedHashMap<Integer, ByteBuffer> pageCache;

	private long pos   = 0;
	private long limit = 0;
	private long cap   = 0;
	private long mark  = -1;

	// -----------------------------------------------------------------------
	// Constructors
	// -----------------------------------------------------------------------

	/**
	 * Opens {@code filename} and memory-maps it on demand.
	 * BigByteBuffer owns the file handle — call {@link #close()} when done.
	 */
	public BigByteBuffer(String filename) throws IOException {
		this(new RandomAccessFile(new File(filename), "r"),
				DEFAULT_PAGE_SIZE, DEFAULT_MAX_PAGES);
	}

	/**
	 * Takes ownership of {@code file}.
	 * Do NOT close {@code file} externally; call {@link #close()} instead.
	 */
	public BigByteBuffer(RandomAccessFile file) throws IOException {
		this(file, DEFAULT_PAGE_SIZE, DEFAULT_MAX_PAGES);
	}

	/**
	 * Takes ownership of {@code file} with custom paging parameters.
	 *
	 * @param file      the file to read — BigByteBuffer takes ownership
	 * @param pageSize  bytes per mmap window (e.g. {@code 8 * 1024 * 1024})
	 * @param maxPages  maximum number of windows kept in memory at once
	 */
	public BigByteBuffer(RandomAccessFile file, int pageSize, int maxPages) throws IOException {
		if (pageSize <= 0) throw new IllegalArgumentException("pageSize must be > 0");
		if (maxPages <= 0) throw new IllegalArgumentException("maxPages must be > 0");

		fileLength = file.length();
		if (fileLength == 0) throw new IllegalArgumentException("File is empty");

		// Hold the RAF open — closing it would also close channel, breaking mmap.
		this.raf      = file;
		this.channel  = file.getChannel();
		this.pageSize = pageSize;
		this.limit    = this.cap = fileLength;

		final int mp = maxPages;
		this.pageCache = new LinkedHashMap<>(maxPages * 2, 0.75f, /*accessOrder=*/true) {
			@Override
			protected boolean removeEldestEntry(Map.Entry<Integer, ByteBuffer> eldest) {
				if (size() > mp) {
					tryUnmap(eldest.getValue());
					return true;
				}
				return false;
			}
		};
	}

	/**
	 * Wraps a plain {@link ByteBuffer} (used for testing / small in-memory buffers).
	 * No file I/O is involved; {@link #close()} is a no-op.
	 */
	public BigByteBuffer(ByteBuffer buffer) {
		this.raf        = null;
		this.channel    = null;
		this.fileLength = buffer.limit();
		this.pageSize   = Integer.MAX_VALUE;   // single "page" = entire buffer
		this.limit      = this.cap = fileLength;

		this.pageCache  = new LinkedHashMap<>(2, 0.75f, true);
		this.pageCache.put(0, buffer.duplicate());
	}

	// -----------------------------------------------------------------------
	// Static wrap helpers (preserve original API surface)
	// -----------------------------------------------------------------------

	public static BigByteBuffer wrap(byte[] array) {
		return new BigByteBuffer(ByteBuffer.wrap(array));
	}

	public static BigByteBuffer wrap(byte[] array, int offset, int length) {
		// slice() resets position to 0 and limit to length — matches BigByteBuffer semantics.
		ByteBuffer bb = ByteBuffer.wrap(array, offset, length).slice();
		return new BigByteBuffer(bb);
	}

	// -----------------------------------------------------------------------
	// Buffer-state API (mirrors java.nio.Buffer)
	// -----------------------------------------------------------------------

	public final long capacity()        { return cap;   }
	public final long limit()           { return limit; }
	public final long position()        { return pos;   }
	public final long remaining()       { return limit - pos; }
	public final boolean hasRemaining() { return pos < limit; }

	public final void clear() {
		limit = cap;
		pos   = 0;
		mark  = -1;
	}

	public final BigByteBuffer flip() {
		limit = pos;
		pos   = 0;
		mark  = -1;
		return this;
	}

	public final BigByteBuffer limit(long newLimit) {
		if (newLimit < 0 || newLimit > cap) throw new IllegalArgumentException();
		if (newLimit < mark) mark = -1;
		if (pos > newLimit)  pos  = newLimit;
		limit = newLimit;
		return this;
	}

	public final BigByteBuffer position(long newPosition) {
		if (newPosition < 0 || newPosition > limit) {
			System.err.println(" new Position is " + newPosition);
			throw new IllegalArgumentException();
		}
		if (newPosition <= mark) mark = -1;
		pos = newPosition;
		return this;
	}

	// -----------------------------------------------------------------------
	// Read API
	// -----------------------------------------------------------------------

	/**
	 * Returns the byte at the current position and advances position by 1.
	 */
	public byte get() {
		checkUnderflow(1);
		byte b = readByteAt(pos);
		pos++;
		return b;
	}

	/**
	 * Seeks to {@code position} then returns the byte there, advancing by 1.
	 */
	public byte get(long position) {
		position(position);
		return get();
	}

	/**
	 * Reads {@code dst.length} bytes from the current position into {@code dst}.
	 */
	public BigByteBuffer get(byte[] dst) {
		return get(dst, 0, dst.length);
	}

	/**
	 * Reads {@code length} bytes from the current position into {@code dst}
	 * starting at {@code offset}.
	 */
	public BigByteBuffer get(byte[] dst, int offset, int length) {
		if (length > remaining())
			throw new BufferUnderflowException();

		int written = 0;
		while (written < length) {
			int        pageIdx = (int) (pos / pageSize);
			int        pageOff = (int) (pos % pageSize);
			ByteBuffer page    = getPage(pageIdx);
			int        avail   = Math.min(page.capacity() - pageOff, length - written);

			page.position(pageOff);
			page.get(dst, offset + written, avail);

			pos     += avail;
			written += avail;
		}
		return this;
	}

	/**
	 * Returns the big-endian {@code int} at {@code position} and advances by 4.
	 */
	public int getInt(long position) {
		this.pos = position;
		checkUnderflow(4);

		int        pageIdx = (int) (pos / pageSize);
		int        pageOff = (int) (pos % pageSize);
		ByteBuffer page    = getPage(pageIdx);
		int        avail   = page.capacity() - pageOff;

		if (avail >= 4) {
			// Fast path: all 4 bytes on the same page.
			page.position(pageOff);
			pos += 4;
			return page.getInt();
		} else {
			// Slow path: int straddles a page boundary — read byte by byte.
			byte[] tmp = new byte[4];
			get(tmp, 0, 4);   // advances pos correctly
			return ((tmp[0] & 0xFF) << 24)
				   | ((tmp[1] & 0xFF) << 16)
				   | ((tmp[2] & 0xFF) <<  8)
				   |  (tmp[3] & 0xFF);
		}
	}

	/**
	 * Returns a {@link ByteBuffer} slice starting at the current position.
	 * <p>
	 * The slice is backed by the mmap window that is currently resident in the
	 * LRU cache. It remains valid as long as that page is not evicted. Callers
	 * that need a long-lived view should copy the data out explicitly.
	 */
	public ByteBuffer slice() {
		int        pageIdx = (int) (pos / pageSize);
		int        pageOff = (int) (pos % pageSize);
		ByteBuffer page    = getPage(pageIdx);
		page.position(pageOff);
		return page.slice();
	}

	/**
	 * Reads {@code b.capacity()} bytes from {@code position} into {@code b},
	 * then rewinds {@code b} to position 0.
	 */
	public ByteBuffer read(ByteBuffer b, long position) {
		this.pos = position;
		int    howmany = b.capacity();
		byte[] dst     = new byte[howmany];
		get(dst, 0, howmany);
		b.clear();
		b.put(dst);
		b.flip();
		return b;
	}

	// -----------------------------------------------------------------------
	// Internal paging
	// -----------------------------------------------------------------------

	/**
	 * Returns the cached page for {@code pageIndex}, mapping it from the
	 * FileChannel on a cache miss.
	 */
	private ByteBuffer getPage(int pageIndex) {
		ByteBuffer page = pageCache.get(pageIndex);
		if (page != null) return page;

		long pageStart = (long) pageIndex * pageSize;
		long mapSize   = Math.min(fileLength - pageStart, pageSize);

		try {
			page = channel.map(FileChannel.MapMode.READ_ONLY, pageStart, mapSize);
			page.order(ByteOrder.BIG_ENDIAN);   // SQLite stores integers big-endian
		} catch (IOException e) {
			throw new RuntimeException(
					"Failed to map page " + pageIndex + " at offset " + pageStart
					+ ". Ensure the RandomAccessFile was not closed externally "
					+ "— BigByteBuffer owns the file handle; call close() on BigByteBuffer instead.", e);
		}

		pageCache.put(pageIndex, page);   // LRU eviction fires inside removeEldestEntry
		return page;
	}

	private byte readByteAt(long position) {
		int        pageIdx = (int) (position / pageSize);
		int        pageOff = (int) (position % pageSize);
		ByteBuffer page    = getPage(pageIdx);
		return page.get(pageOff);
	}

	private void checkUnderflow(int needed) {
		if (pos + needed > limit)
			throw new BufferUnderflowException();
	}

	// -----------------------------------------------------------------------
	// Best-effort unmap
	// -----------------------------------------------------------------------

	/**
	 * Attempts to force-unmap a MappedByteBuffer via the internal Cleaner so
	 * that native memory is released before the next GC cycle.
	 * Silently ignored on JDK versions that restrict reflective access.
	 */
	private static void tryUnmap(ByteBuffer buffer) {
		if (!buffer.isDirect()) return;
		try {
			java.lang.reflect.Method cleanerMethod =
					buffer.getClass().getMethod("cleaner");
			cleanerMethod.setAccessible(true);
			Object cleaner = cleanerMethod.invoke(buffer);
			if (cleaner != null) {
				java.lang.reflect.Method cleanMethod =
						cleaner.getClass().getMethod("clean");
				cleanMethod.setAccessible(true);
				cleanMethod.invoke(cleaner);
			}
		} catch (Exception ignored) {
			// JDK 9+ without --add-opens, or non-Sun JVM: let GC handle it.
		}
	}

	// -----------------------------------------------------------------------
	// Resource management
	// -----------------------------------------------------------------------

	/**
	 * Releases all cached pages, closes the FileChannel, and closes the
	 * underlying RandomAccessFile.
	 * <p>
	 * The buffer must not be used after this call.
	 */
	public void close() throws IOException {
		for (ByteBuffer buf : pageCache.values()) {
			tryUnmap(buf);
		}
		pageCache.clear();
		if (channel != null) channel.close();
		if (raf     != null) raf.close();
	}
}
