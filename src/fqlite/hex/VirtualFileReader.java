package fqlite.hex;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Reads only the currently visible chunk from a (potentially huge) file.
 * A LRU-cache keeps the last N pages in memory to avoid re-reads during
 * small scrolls.
 */
public class VirtualFileReader implements AutoCloseable {

    public static final int BYTES_PER_ROW  = 16;
    public static final int PAGE_ROWS      = 256;   // rows per cached page
    public static final int PAGE_BYTES     = BYTES_PER_ROW * PAGE_ROWS;
    private static final int CACHE_PAGES   = 8;

    private final FileChannel channel;
    private final long fileSize;

    // Simple array-based LRU: index = page index, value = cached bytes (or null)
    private final long[]   cacheKeys;
    private final byte[][] cacheData;
    private int            lruPtr = 0;

    public VirtualFileReader(Path path) throws IOException {
        channel  = FileChannel.open(path, StandardOpenOption.READ);
        fileSize = channel.size();
        cacheKeys = new long[CACHE_PAGES];
        cacheData = new byte[CACHE_PAGES][];
        java.util.Arrays.fill(cacheKeys, -1L);
    }

    public long getFileSize() { return fileSize; }

    public long getTotalRows() {
        return (fileSize + BYTES_PER_ROW - 1) / BYTES_PER_ROW;
    }

    /**
     * Returns the bytes for the given row range [firstRow, firstRow+rowCount).
     * Rows that exceed the file are zero-padded.
     */
    public byte[] readRows(long firstRow, int rowCount) throws IOException {
        byte[] result = new byte[rowCount * BYTES_PER_ROW];
        long byteOffset = firstRow * BYTES_PER_ROW;
        int  remaining  = (int) Math.min((long) rowCount * BYTES_PER_ROW,
                                         fileSize - byteOffset);
        if (remaining <= 0) return result;

        int written = 0;
        while (written < remaining) {
            long pageIndex  = (byteOffset + written) / PAGE_BYTES;
            int  pageOffset = (int) ((byteOffset + written) % PAGE_BYTES);
            byte[] page     = getPage(pageIndex);
            int toCopy      = Math.min(PAGE_BYTES - pageOffset, remaining - written);
            System.arraycopy(page, pageOffset, result, written, toCopy);
            written += toCopy;
        }
        return result;
    }

    private byte[] getPage(long pageIndex) throws IOException {
        // Check cache
        for (int i = 0; i < CACHE_PAGES; i++) {
            if (cacheKeys[i] == pageIndex) return cacheData[i];
        }
        // Miss – load page
        long fileOffset = pageIndex * PAGE_BYTES;
        int  toRead     = (int) Math.min(PAGE_BYTES, fileSize - fileOffset);
        byte[] data     = new byte[PAGE_BYTES];
        if (toRead > 0) {
            ByteBuffer buf = ByteBuffer.wrap(data, 0, toRead);
            channel.read(buf, fileOffset);
        }
        // Evict LRU slot
        cacheKeys[lruPtr] = pageIndex;
        cacheData[lruPtr] = data;
        lruPtr = (lruPtr + 1) % CACHE_PAGES;
        return data;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
