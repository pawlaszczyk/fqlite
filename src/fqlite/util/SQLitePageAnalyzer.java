package fqlite.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class SQLitePageAnalyzer {

    /**
     * Represents a single freeblock in a SQLite b-tree page.
     */
    public record FreeBlock(int offset, int nextOffset, int size, byte[] data) {
        @Override
        public String toString() {
            return String.format(
                    "FreeBlock { offset=%-6d | size=%-6d | nextOffset=%-6d }",
                    offset, size, nextOffset
            );
        }
    }

    /**
     * Reads and returns all freeblocks from a SQLite b-tree page stored in a ByteBuffer.
     *
     * The ByteBuffer must be positioned at the start of the page (position 0 = page start).
     * For page 1, the 100-byte database file header must be accounted for externally
     * or the buffer must be pre-sliced to skip it.
     *
     * @param buffer   the ByteBuffer containing the raw b-tree page bytes
     * @param pageSize the total size of the page in bytes (e.g. 4096)
     * @return a list of FreeBlock records found on the page, in chain order
     * @throws IllegalArgumentException if the page type byte is invalid
     */
    public static List<FreeBlock> parseFreeBlocks(ByteBuffer buffer, int pageSize) {
        // Work on a read-only duplicate so we don't mutate the caller's position
        ByteBuffer page = buffer.duplicate().order(ByteOrder.BIG_ENDIAN);
        page.position(0);

        // --- B-tree page header ---
        // Byte 0: page type
        byte pageType = page.get(0);
        validatePageType(pageType);

        // Bytes 1-2: offset of first freeblock (big-endian unsigned short)
        int firstFreeBlockOffset = Short.toUnsignedInt(page.getShort(1));

        System.out.println("=".repeat(60));
        System.out.printf("Page type      : %s (0x%02X)%n", describePageType(pageType), pageType);
        System.out.printf("First freeblock: offset = %d%n", firstFreeBlockOffset);

        List<FreeBlock> freeBlocks = new ArrayList<>();

        if (firstFreeBlockOffset == 0) {
            System.out.println("No freeblocks on this page.");
            System.out.println("=".repeat(60));
            return freeBlocks;
        }

        // --- Walk the freeblock chain ---
        int currentOffset = firstFreeBlockOffset;
        int iteration = 0;
        final int MAX_FREEBLOCKS = pageSize / 4; // safety cap: minimum freeblock is 4 bytes

        while (currentOffset != 0) {
            if (iteration++ > MAX_FREEBLOCKS) {
                throw new IllegalStateException(
                        "Freeblock chain exceeded maximum expected length — possible corruption or cycle."
                );
            }
            if (currentOffset + 4 > pageSize) {
                throw new IllegalStateException(String.format(
                        "Freeblock offset %d is out of page bounds (pageSize=%d).", currentOffset, pageSize
                ));
            }

            // Bytes 0-1 of freeblock: offset of next freeblock (0 = end of chain)
            int nextOffset = Short.toUnsignedInt(page.getShort(currentOffset));

            // Bytes 2-3 of freeblock: total size of this freeblock (including 4-byte header)
            int size = Short.toUnsignedInt(page.getShort(currentOffset + 2));

            if (size < 4) {
                throw new IllegalStateException(String.format(
                        "Freeblock at offset %d has invalid size %d (minimum is 4).", currentOffset, size
                ));
            }
            if (currentOffset + size > pageSize) {
                throw new IllegalStateException(String.format(
                        "Freeblock at offset %d with size %d exceeds page bounds.", currentOffset, size
                ));
            }

            // Read the full freeblock payload (header + free bytes)
            byte[] data = new byte[size];
            page.position(currentOffset);
            page.get(data);

            freeBlocks.add(new FreeBlock(currentOffset, nextOffset, size, data));
            currentOffset = nextOffset;
        }

        // --- Print results ---
        System.out.printf("Freeblocks found: %d%n", freeBlocks.size());
        System.out.println("-".repeat(60));
        for (int i = 0; i < freeBlocks.size(); i++) {
            FreeBlock fb = freeBlocks.get(i);
            System.out.printf("[%d] %s%n", i, fb);
            System.out.printf("    Raw bytes (hex): %s%n", toHex(fb.data()));
        }
        System.out.println("=".repeat(60));

        return freeBlocks;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void validatePageType(byte type) {
        if (type != 0x02 && type != 0x05 && type != 0x0A && type != 0x0D) {
            throw new IllegalArgumentException(
                    String.format("Unknown b-tree page type: 0x%02X", type)
            );
        }
    }

    private static String describePageType(byte type) {
        return switch (type) {
            case 0x02 -> "Interior Index B-Tree";
            case 0x05 -> "Interior Table B-Tree";
            case 0x0A -> "Leaf Index B-Tree";
            case 0x0D -> "Leaf Table B-Tree";
            default   -> "Unknown";
        };
    }

    private static String toHex(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return "(empty)";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            if (i > 0 && i % 16 == 0) sb.append("\n                 ");
            sb.append(String.format("%02X ", bytes[i]));
        }
        return sb.toString().trim();
    }
}
