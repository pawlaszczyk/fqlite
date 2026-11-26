package fqlite.mcp;

import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.Map;

import fqlite.headless.RecoveryResult;

/**
 * One analysed database kept in memory, addressed by its id
 *
 * It defines a structure to hold the data for the respective run including a handle to the resultset.
 *
 * @author pawalszc
 */
record Analysis(String id, Path path, Map<String, String> sha256, long size, OffsetDateTime analysedAt,
		boolean unchanged, RecoveryResult result) {
}
