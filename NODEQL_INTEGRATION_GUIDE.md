# NodeQL in FQLite einbinden

Diese Anleitung beschreibt, wie die lokale NodeQL-Java-Bibliothek in FQLite eingebunden ist und wie das Projekt ausgeführt werden kann.

## Ziel

FQLite bleibt die vollwertige Hauptanwendung. NodeQL ist nur eine optionale Ergänzung: FQLite bekommt einen eigenen NodeQL Builder, in dem SQL-Commands als Blöcke zusammengesteckt werden. Der Builder kompiliert die Blockkette zu SQL und übergibt das SQL an den bestehenden SQL Analyzer.

## Relevante Ordner

```text
fqlite/
  src/digital/codespiresolutions/nodeql/
    BlockNode.java
    BlockType.java
    Position.java
    SqlCompileResult.java
    SqlCompiler.java

  build.gradle
  src/fqlite/nodeql/NodeQlBuilderWindow.java
  src/fqlite/sql/SQLWindow.java
```

## Build-Einbindung

In `build.gradle` werden die FQLite- und NodeQL-Quellen gemeinsam aus `src` kompiliert:

```gradle
sourceSets {
    main {
        java {
            srcDirs = ['src']
        }
        resources {
            srcDirs "resources"
        }
    }
}
```

Dadurch ist weder ein separates JAR noch ein nicht versionierter Elternordner nötig. FQLite kann die Klassen direkt importieren:

```java
import digital.codespiresolutions.nodeql.SqlCompileResult;
import digital.codespiresolutions.nodeql.SqlCompiler;
```

## UI-Integration

Die Integration sitzt an zwei Stellen.

### 1. FQLite-Hauptfenster

Im Hauptfenster gibt es unter:

```text
Analyze > NodeQL Builder...
```

zusätzlich einen Toolbar-Button:

```text
NodeQL
```

Dieser Workflow ist die normale FQLite-Integration:

1. Eine SQLite-Datenbank in FQLite öffnen.
2. `Analyze > NodeQL Builder...` wählen oder den `NodeQL`-Toolbar-Button klicken.
3. Im NodeQL Builder SQL-Blöcke aus der Palette hinzufügen.
4. Die Blöcke unter `EXECUTE QUERY` ziehen, bis sie einschnappen.
5. Rechts die SQL-Vorschau prüfen.
6. `Open in SQL Analyzer` klicken.
7. Das SQL kann dort mit dem normalen Play-Button ausgeführt werden.

Der Builder unterstützt:

- Kategorien: DQL, Joins & Sets, Aggregates, Functions, DML, DDL, DCL, TCL
- Snapping unter `EXECUTE QUERY`
- Löschen über `Delete`, `Backspace`, den `Delete`-Toolbar-Button oder das `x` am Block
- große Grid-Arbeitsfläche mit Infinite-Canvas-Gefühl
- Panning im leeren Workspace-Bereich per rechter oder mittlerer Maustaste
- Zoom über `+`, `-`, `100%`, Trackpad/Pinch und `Cmd/Ctrl` + Scroll
- Live-SQL-Vorschau

Die Implementierung sitzt hier:

```text
src/fqlite/base/GUI.java
src/fqlite/nodeql/NodeQlBuilderWindow.java
```

Der Builder zeigt unten rechts außerdem die Attribution:

```text
NodeQL Implementation by Paul Bodach (CodeSpire-Solutions)
```

### 2. SQL Analyzer

```text
src/fqlite/sql/SQLWindow.java
```

Der SQL Analyzer bleibt FQLites normaler SQL-Editor. NodeQL-Dateien werden dort nicht geöffnet. Der relevante Flow ist:

1. Query im NodeQL Builder zusammenstecken.
2. `Open in SQL Analyzer` klicken.
3. Das generierte SQL wird in den SQL Analyzer übernommen.
4. Mit dem normalen Play-Button von FQLite ausführen.

## Projekt ausführen

Aus dem FQLite-Ordner:

```bash
cd "/path/to/fqlite"
./run-fqlite.sh
```

Das Script bevorzugt ein kompatibles JDK 21. Es prüft in dieser Reihenfolge:

1. Bereits gesetztes `JAVA_HOME`
2. Homebrew `openjdk@21`
3. Android Studio JBR 21

Fallback auf diesem Rechner:

```text
/Applications/Android Studio.app/Contents/jbr/Contents/Home
```

Warum: Gradle 8.10.2/FQLite läuft mit den sehr neuen systemweiten JDKs 25/26 nicht sauber. Typische Fehler sind `Unsupported class file major version 69` oder `25.0.2`.

Manuell geht es so:

```bash
cd "/path/to/fqlite"
JAVA_HOME="/Applications/Android Studio.app/Contents/jbr/Contents/Home" ./gradlew run
```

Für einen Compile-Test ohne UI:

```bash
JAVA_HOME="/Applications/Android Studio.app/Contents/jbr/Contents/Home" ./gradlew compileJava
```

Dieser Compile wurde erfolgreich getestet.

Falls Android Studio nicht vorhanden ist, installiere ein JDK 21 und starte Gradle damit:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
./gradlew run
```

Falls Java 21 nicht installiert ist, z. B.:

```bash
brew install openjdk@21
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
./gradlew run
```
 
## Kompilierung prüfen

```bash
cd "/path/to/fqlite"
JAVA_HOME="/Applications/Android Studio.app/Contents/jbr/Contents/Home" ./gradlew compileJava
```

Erwartetes Ergebnis:

```text
BUILD SUCCESSFUL
```

## NodeQL-Quellen

Die fünf von FQLite benötigten NodeQL-Klassen liegen direkt im Repository:

```text
src/digital/codespiresolutions/nodeql/
```

Damit enthält ein normaler Clone alle für den Build benötigten Dateien.

## Testablauf in der UI

1. FQLite starten.
2. Eine SQLite-Datenbank öffnen.
3. `Analyze > NodeQL Builder...` öffnen oder den Toolbar-Button `NodeQL` klicken.
4. Einen `SELECT`-Block hinzufügen.
5. Den Block unter `EXECUTE QUERY` ziehen, bis er einschnappt.
6. Optional `WHERE`, `JOIN`, `GROUP BY` oder `ORDER BY` hinzufügen und darunter einschnappen lassen.
7. Rechts prüfen, ob SQL generiert wird.
8. `Open in SQL Analyzer` klicken.
9. Mit Play ausführen.
