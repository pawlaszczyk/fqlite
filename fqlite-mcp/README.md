# fqlite-mcp

MCP server (Model Context Protocol) for **FQLite**, the forensic SQLite recovery tool. It
lets an AI assistant such as Claude Desktop or Claude Code analyse SQLite databases -
including deleted and freelist records, WAL archives and rollback journals - through tools,
while the evidence stays read-only and every tool call is logged.

Engine: [`io.github.pawlaszczyk:fqlite-headless`](https://central.sonatype.com/artifact/io.github.pawlaszczyk/fqlite-headless).
Transport: stdio (JSON-RPC 2.0, MCP protocol versions 2024-11-05 to 2025-11-25).

## Build

```bash
cd fqlite-mcp
../gradlew fatJar          # build/libs/fqlite-mcp-0.1.0.jar (self-contained, Java 21+)
```

By default the engine comes from Maven Central (`fqlite-headless:5.3`). To build against
the engine of the current source tree instead:

```bash
./gradlew headlessJar                                        # in the FQLite root
./gradlew -p fqlite-mcp fatJar -PheadlessJar=$PWD/build/headless/fqlite-headless-<v>.jar
```

The GitHub workflow (`.github/workflows/build.yml`, job `jars`) does exactly this and attaches
`fqlite-mcp-<v>.jar`, `fqlite-headless-<v>.jar` and `fqlite-headless-<v>.zip` (with `lib/`)
to every release, next to the installers.

## Run

```bash
java -jar fqlite-mcp-0.1.0.jar --evidence ~/Evidence --output ~/fqlite-mcp-output
```

| Option | |
|---|---|
| `--evidence <dir>` | folder evidence may be read from; repeatable. Without it, any readable file is allowed |
| `--output <dir>` | the only folder the server writes to: exports, BLOBs, audit log (default `~/fqlite-mcp-output`) |

The server is started by the MCP client, not by hand.

### Claude Desktop

Settings > Developer > Edit Config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "fqlite": {
      "command": "/Library/Java/JavaVirtualMachines/<jdk-21>/Contents/Home/bin/java",
      "args": [
        "-jar", "/Users/<you>/fqlite-mcp/fqlite-mcp-0.1.0.jar",
        "--evidence", "/Users/<you>/Evidence",
        "--output", "/Users/<you>/fqlite-mcp-output"
      ]
    }
  }
}
```

Use the absolute path of a Java 21+ `java` (`/usr/libexec/java_home -v 21` shows it): the
app does not see the PATH of your shell. Restart Claude Desktop afterwards.

### Claude Code

```bash
claude mcp add fqlite -- java -jar /path/fqlite-mcp-0.1.0.jar --evidence /path/Evidence --output /path/out
```

## Tools

| Tool | |
|---|---|
| `analyze(path, wal?, journal?)` | runs the recovery; returns id, SHA-256 of the evidence (before = after check), tables and counts - no records |
| `list_tables(id)` | schema with columns, types, CREATE statement, counts per status/source |
| `get_records(id, table?, status?, source?, only_recovered?, offset?, limit?)` | records, filtered and paged (max 500 per call) |
| `search(id, query, regex?, case_sensitive?, table?, limit?)` | text / regex search over all values |
| `get_record(id, ref)` | one record in full |
| `export_blob(id, ref, column)` | writes a BLOB to the output folder; returns path, type, SHA-256 |
| `export(id, format)` | complete result as JSON or CSV into the output folder |
| `list_analyses()`, `close_analysis(id)` | analyses kept in memory |

Results are kept in memory per analysis (up to 8; the oldest is released). Give large
databases enough heap: `java -Xmx8g -jar ...`.

## Forensic safeguards

- **Read-only**: the evidence is never written. `analyze` hashes the database and its
  `-wal` / `-journal` before and after the analysis and reports whether they are unchanged.
- **Traceability**: every record carries source file, status, rowid and byte offset.
- **Audit log**: `<output>/fqlite-mcp-audit.jsonl` records every tool call (time, tool,
  arguments, outcome) - the questions that were put to the evidence through the model.
- **Folder limits**: evidence only from `--evidence` folders, writes only to `--output`.
- **Prompt injection**: recovered values are returned marked as untrusted data, and the
  server instructions tell the model never to follow instructions found in them. Evidence
  can contain such text on purpose; the examiner must still review the model's conclusions.
- **Data protection**: with a cloud model, the values the model requests leave the computer.
  For real case data use a client with a local model or an approved environment.

## Skill

`skill/fqlite-forensics/SKILL.md` teaches the assistant the forensic workflow (order of the
tools, interpretation of statuses and timestamps, report format, injection rule). In Claude,
add it as a skill (Settings > Capabilities > Skills, upload the folder as ZIP).

## Protocol test

The server was tested with the official MCP Python SDK client (`mcp` 2.3.0): initialize,
ping, tools/list and every tool including error cases.
