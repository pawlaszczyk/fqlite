package fqlite.mcp;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.OffsetDateTime;

import com.google.gson.JsonObject;

/**
 * Append-only log of every tool call (JSON lines): time, tool, arguments, outcome. It documents
 * which questions were put to the evidence through the model - the basis of the chain of custody
 * of an AI-assisted analysis.
 *
 * @uauthor pawalszc
 *
 */
final class AuditLog {

	private final Path file;

	AuditLog(Path file) {
		this.file = file;
	}

	Path file() {
		return file;
	}

	synchronized void log(String tool, JsonObject args, boolean ok, String summary) {

		JsonObject e = Json.obj("time", OffsetDateTime.now().toString(), "tool", tool, "arguments",
				args == null ? new JsonObject() : args, "ok", ok, "result", summary);
		try {
			Files.writeString(file, Json.GSON.toJson(e) + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE,
					StandardOpenOption.APPEND);
		} catch (IOException ex) {
			System.err.println("fqlite-mcp: audit log not writable: " + ex.getMessage());
		}
	}
}
