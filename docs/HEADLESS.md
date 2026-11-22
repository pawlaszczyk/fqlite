# FQLite headless

The recovery engine of FQLite as a library and command-line tool, without any user
interface: no JavaFX toolkit, no display. It runs on servers, in containers and inside
other Java applications.

## Build

```bash
./gradlew headlessJar          # build/headless/fqlite-headless-5.3.jar + build/headless/lib/
./gradlew headlessDist         # build/distributions/fqlite-headless-5.3.zip
./gradlew publishToMavenLocal  # org.fqlite:fqlite-headless:5.3 into ~/.m2
```

The JAR holds only FQLite's classes. At runtime it needs **Java 21** plus two libraries:

| Dependency | Why |
|---|---|
| `org.openjfx:javafx-base:22` | collection classes the engine stores its rows in (no toolkit, no native code) |
| `org.antlr:antlr4-runtime:4.8` | parser for the CREATE statements of the schema |

`headlessJar` copies both into `lib/` next to the JAR; the manifest refers to them, so
`java -jar` works without a classpath.

## Command line

```bash
java -jar fqlite-headless-5.3.jar evidence.db                 # JSON to stdout
java -jar fqlite-headless-5.3.jar --json out.json evidence.db
java -jar fqlite-headless-5.3.jar --csv out/ --summary evidence.db
```

| Option | |
|---|---|
| `--json <file\|->` | result as JSON (`-` = stdout; default if nothing else is chosen) |
| `--csv <dir>` | one CSV file per table (`;`, UTF-8); WAL / journal records in `<table>.wal.csv` / `<table>.journal.csv` |
| `--summary` | records per table and status on stderr |
| `--no-wal`, `--no-journal` | ignore `<db>-wal` / `<db>-journal` |
| `--threads <n>` | worker threads of the engine (default 1) |

Exit codes: 0 ok, 1 analysis failed, 2 wrong arguments. Stdout carries only the JSON;
messages of the engine go to the Java logger (`-Dfqlite.log.dir=<dir>` writes them to
`<dir>/fqlite.log`).

## Java API

```java
import fqlite.headless.*;
import java.nio.file.Path;

RecoveryResult r = FQLiteHeadless.analyze(Path.of("evidence.db"));

for (TableInfo t : r.getTables())                     // tables, indexes, internal buckets
    System.out.println(t.getName() + " " + t.getColumnNames());

for (RecoveredRecord rec : r.getRecords("messages")) {
    if (rec.getStatus() == Status.DELETED)            // LIVE, DELETED, FREELIST, UNKNOWN
        System.out.println(rec.getOffset() + " " + rec.getValues());
}

byte[] picture = r.getBlob(rec, 2);                  // bytes of a value "[BLOB-n]"
r.writeJson(Path.of("evidence.json"));
r.writeCsv(Path.of("evidence-csv"));
```

Options:

```java
FQLiteHeadless.analyze(db, Options.defaults()
        .wal(true)                  // analyse <db>-wal if present (default)
        .journal(true)              // analyse <db>-journal if present (default)
        .loadAllIndexEntries(true)  // also the live entries of large indexes (default)
        .threads(4));
```

`RecoveredRecord` per record: `getSource()` (DATABASE, WAL, ROLLBACK_JOURNAL), `getTable()`,
`getStatus()` / `getRawStatus()`, `getRowId()`, `getOffset()` (byte offset in the source
file), `getInfo()` (FQLite's `[pll|hl]` column with flags), `isPartial()` (truncated or
overflow overwritten), `isSuperseded()` (older version: UPDATE, deleted in a later WAL frame,
...), `getValues()`, and for WAL records `getWalFrame()`, `getWalDbPage()`, `getWalCommit()`,
`getWalSalt1()`, `getWalSalt2()`.

For WAL and journal records the status describes the record inside its frame/page; whether
it is the current state of the database follows from `isSuperseded()` and the frame data.

### Maven / Gradle

After `./gradlew publishToMavenLocal`:

```xml
<dependency>
  <groupId>org.fqlite</groupId>
  <artifactId>fqlite-headless</artifactId>
  <version>5.3</version>
</dependency>
```

Maven chooses the platform classifier of javafx-base itself. In Gradle, either apply the
`org.openjfx.javafxplugin` with `modules = ['javafx.base']` or name the classifier:
`runtimeOnly "org.openjfx:javafx-base:22:linux"` (`mac`, `mac-aarch64`, `win`).

## Notes

- The database file is opened read-only and never changed.
- The result is held in memory (as in the GUI); give large databases enough heap (`-Xmx`).
- For `java -jar`, the manifest opens the internal packages that `BigByteBuffer` uses to
  release memory-mapped windows early. When the JAR is used as a library, add
  `--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED`
  to the JVM options; without them the memory is released by the garbage collector.
- Every call uses its own engine instance. The number of worker threads is a JVM-wide
  setting of FQLite.
- The headless mode is switched on by the system property `fqlite.headless=true`, which
  `FQLiteHeadless` sets when it is loaded. In this mode the logger creates no log window
  and the engine writes nothing to stdout.
