# NodeQL in FQLite einbinden

Diese Anleitung beschreibt, wie die lokale NodeQL-Java-Bibliothek in FQLite eingebunden ist und wie das Projekt ausgeführt werden kann.

## Ziel

FQLite bleibt die vollwertige Hauptanwendung. NodeQL ist eine optionale Ergänzung mit einem eigenen visuellen SQLite-Editor. Er kompiliert verbundene Blöcke zu SQL und führt sie direkt gegen die aktive In-Memory-Datenbank aus. Der SQL Analyzer ist zusätzlich erreichbar.

Diese Integration gehört zu FQLite **5.1**. Die visuelle Referenz ist die NodeQL-Anwendung
bei Commit `bfbb70c5a6e4035041af05809d2f0f28f484aabd` (NodeQL-Version `0.4.3+2`).
Die JavaFX-Integration übernimmt nicht die gesamte eigenständige Flutter-Anwendung.

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

Im Hauptfenster gibt es einen Toolbar-Button:

```text
NodeQL
```

Dieser Workflow ist die normale FQLite-Integration:

1. Eine SQLite-Datenbank in FQLite öffnen.
2. Den `NodeQL`-Toolbar-Button klicken.
3. Im NodeQL Builder SQL-Blöcke aus der Palette hinzufügen.
4. Anweisungen unter `EXECUTE QUERY` ziehen, bis sie einschnappen. Ovale Reporter in runde Eingabefelder einsetzen.
5. Rechts die SQL-Vorschau prüfen.
6. `Run Query` oder den gelben Startblock klicken, um die Query direkt auszuführen. Ergebnisse erscheinen rechts im NodeQL-Editor; `Expand results` öffnet eine große Ansicht.
7. Optional `Open in SQL Analyzer` wählen, wenn das generierte SQL dort weiterbearbeitet werden soll.

Der Einstieg ist ausschließlich das Graph-Icon in der Toolbar, nicht ein Eintrag
neben `File`, `Export`, `Analyze` und `Info`. NodeQL-Nodes bleiben nur für die
Editor-Sitzung erhalten. Beim Speichern oder Laden eines FQLite-Projekts wird der
NodeQL-Workspace nicht gespeichert oder wiederhergestellt.

### Gemeinsamer Hell-/Dunkelmodus

Der `Light`-/`Dark`-Schalter rechts in der NodeQL-Toolbar verwendet denselben
`ThemeManager`, dieselbe Beschriftung und denselben Button-Stil wie das
FQLite-Hauptfenster. Der Builder übernimmt den aktiven Modus beim Öffnen.
Änderungen aus dem Hauptfenster oder einem Builder wirken sofort auf alle
registrierten Fenster, einschließlich weiterer Builder, vergrößerter Ergebnisse
und Fehlermeldungen. Workspace, Nodes und Query-Ergebnisse bleiben dabei erhalten.

Es gibt keine eigene NodeQL-Theme-Konfiguration. Wie im bisherigen Hauptfenster
gilt die Auswahl für die laufende App-Sitzung; eine neue Speicherung über
App-Neustarts hinweg wird nicht eingeführt. Die vorhandenen FQLite-Farben und
UI-Grundeinstellungen kommen zentral vom ThemeManager, während die farbigen
NodeQL-Blöcke ihre Formen und gut lesbare weiße Beschriftung behalten.

Der Builder unterstützt:

- Kategorien: DQL, Joins & Sets, Aggregates, Functions, DML, DDL, DCL, TCL
- durchsuchbare Blockkarten mit Beschreibung und verstellbare Breiten der drei Editorbereiche
- Formen und Farben nach dem aktuellen [NodeQL-Referenzcode](https://github.com/Kartoffelspalt/NodeQL): Anweisungen, Joins, Mengenoperationen, ovale Reporter und Abschlussblöcke
- Reporter per Drag-and-drop in Eingabefelder einsetzen, wieder herausziehen und mit Undo wiederherstellen
- strukturierte WHERE-/AND-/OR-Bedingungen, ASC-/DESC-Auswahl, SELECT DISTINCT, Tabellenalias sowie LIMIT/OFFSET
- Vorschläge für Tabellen, Views und temporäre Tabellen an allen Tabellenfeldern:
  `▾` zeigt die Namen der aktiven Datenbank; Tippen filtert ohne Beachtung der
  Groß-/Kleinschreibung. Auch `Cmd/Ctrl` + `Leertaste` oder `↓` öffnet die Liste.
  `Esc` schließt sie. Freier Text und eigene SQL-Ausdrücke bleiben erlaubt.
  Gewählte Namen werden automatisch als SQL-Identifier zitiert.
- Spaltenvorschläge in SELECT, WHERE, GROUP BY, ORDER BY, JOIN-Bedingungen und
  passenden Reporter-/Funktionsfeldern. Die Auswahl berücksichtigt Tabellen,
  Views, temporäre Tabellen und Aliasse. Bei gleichnamigen JOIN-Spalten werden
  eindeutige Referenzen wie `c.customer_id` oder `o.customer_id` angeboten.
  Für mehrere SELECT-Spalten ein Komma tippen und die nächste Spalte auswählen;
  vorherige Listenelemente und Kommas in Funktionen/Strings bleiben erhalten.
  Ausdrücke wie `COALESCE(name, 'unknown')` können weiterhin frei eingegeben werden.
- Echte Mehrfachauswahl über `☑` neben Listenfeldern: Spalten anhaken und mit
  `Apply` gemeinsam übernehmen. Die Checkbox-Liste ist durchsuchbar, behält
  Markierungen beim Filtern und belegt bestehende Spalten vor. `Cancel` verändert
  die Eingabe nicht; die Übernahme lässt sich mit einem Undo zurücknehmen.
  Die Funktion steht in SELECT, COLUMN-Reportern, GROUP BY und Funktionsargumenten
  bereit. Eigene SQL-Ausdrücke werden als markierte `Custom SQL`-Einträge erhalten
  und können gezielt abgewählt werden. Konkrete Spalten ersetzen eine `*`-Auswahl.
- Text-, Alias- und SQLite-Funktionsreporter; beispielsweise `datetime(unix_timestamp, 'unixepoch')`
- Snapping unter `EXECUTE QUERY`
- Löschen ausgewählter Nodes über `Delete`, `Backspace` oder den `Delete`-Toolbar-Button; Texteingaben bleiben dabei editierbar
- große Grid-Arbeitsfläche mit Infinite-Canvas-Gefühl
- Panning im leeren Workspace-Bereich per rechter oder mittlerer Maustaste
- stabiler sichtbarer Ausschnitt beim Ab-/Andocken, beim Verstellen der
  Panelbreiten und beim Zoomen; kein automatisches Rezentrieren während eines Drags
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
2. `Run Query` für die direkte Ausführung und Ergebnisanzeige wählen.
3. Optional `Open in SQL Analyzer` klicken, um das SQL dort weiterzubearbeiten.

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

Die folgenden Quellen bilden die lokale Integration; die aktuelle Referenz wird
in [design-qa.md](design-qa.md) dokumentiert.

Die fünf von FQLite benötigten NodeQL-Klassen liegen direkt im Repository:

```text
src/digital/codespiresolutions/nodeql/
```

Damit enthält ein normaler Clone alle für den Build benötigten Dateien.

## Testablauf in der UI

### Automatisierter Integrationstest

Mit JDK 21 und einem verfügbaren grafischen Desktop:

```bash
./gradlew nodeQlSmokeTest
```

Der Test in `tests/NodeQlSmokeTest.java` öffnet einen temporären Editor und prüft
Blockgeometrie, Snapping, Reporter-Einsetzen/-Lösen, verschachtelte Reporter und Undo,
SQL-Kompilierung sowie die direkte SQLite-Ausführung und Ergebnisanzeige. Er nutzt
nur eine temporäre In-Memory-Datenbank und schließt das Fenster anschließend.
Der Test enthält außerdem Prüfungen für die Theme-Übernahme, Umschalten in beide
Richtungen, mehrere Builder, Ergebnisfenster, erhaltene Queries/Ergebnisse und
das Entfernen von Theme-Bindings beim Schließen. Die Blockbeschriftung wird
zusätzlich anhand der Screenshot-Pixel in beiden Modi geprüft.
Zusätzliche Regressionen prüfen wiederholtes Ab-/Andocken über die tatsächlichen
JavaFX-Maus-Handler bei 70/100/140 % Zoom, Kamerastabilität beim Verstellen der
Panels, explizites Panning und Zoom um den bisherigen Ausschnitt. Vorschläge werden
für jeden Block mit Tabellenfeld, Filterung bei laufender Eingabe, `Esc`, neue
Tabellen, Views/Temp-Tabellen, Identifier-Quoting und freie SQL-Eingaben geprüft.
Die Spaltentests prüfen zusätzlich Aliasse, JOIN-Mehrdeutigkeiten, generierte
Spalten, geänderte Quelltabellen und Schemas, Mehrfachprojektionen, verschachtelte
Reporter/Undo sowie tatsächliche Live-Filterung im Spaltenfeld.
Die Checkbox-Mehrfachauswahl wird außerdem auf Vorbelegung, Auswahlreihenfolge,
Filterzustand, Apply/Cancel, Wildcards, freie Ausdrücke, einheitliches Undo und
Dialog-/Theme-Cleanup geprüft, einschließlich mehr als 20 verfügbarer Spalten.
Screenshots entstehen unter `build/reports/nodeql/editor.png` (Dark) und
`build/reports/nodeql/editor-light.png` (Light).
Zusätzlich entsteht `build/reports/nodeql/columns-multi-selection.png`.
Ein headless System benötigt für diesen Test eine grafische Umgebung.

### Manueller Ablauf

1. FQLite starten.
2. Eine SQLite-Datenbank öffnen.
3. Den Toolbar-Button `NodeQL` klicken.
4. Einen `SELECT`-Block hinzufügen.
5. Den Block unter `EXECUTE QUERY` ziehen, bis er einschnappt.
6. Optional `WHERE`, `JOIN`, `GROUP BY` oder `ORDER BY` hinzufügen und darunter einschnappen lassen.
7. Rechts prüfen, ob SQL generiert wird.
8. `Run Query` klicken und das Ergebnis rechts im NodeQL-Editor prüfen.
