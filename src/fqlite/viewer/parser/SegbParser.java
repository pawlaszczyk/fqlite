package fqlite.viewer.parser;

import fqlite.viewer.model.SegbFile;
import fqlite.viewer.model.SegbRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Parser für Apple SEGB-Dateien (Biome), Version 1, 2 und Fixed-Size-Variant.
 *
 * <h2>Biome Outer-Container (optional)</h2>
 * <p>Viele Biome-Dateien tragen vor dem eigentlichen SEGB-Block einen
 * proprietären Outer-Header. Der Parser erkennt diesen automatisch und
 * überspringt ihn. Das SEGB-Magic {@code 53 45 47 42} wird in den ersten
 * 256 Bytes der Datei gesucht.
 *
 * <pre>
 *   Outer-Header (52 Bytes, beobachtetes Layout):
 *     Bytes  0–1  : Stream-Typ / Flags (uint16 LE)
 *     Bytes  2–3  : Outer-Version      (uint16 LE)
 *     Bytes  4–7  : Reserviert         (uint32 LE, = 0)
 *     Bytes  8–15 : Datei-Timestamp    (float64 LE, Apple-Epoch)
 *     Bytes 16–19 : Name-Länge         (uint32 LE)
 *     Bytes 20–…  : Dateiname          (UTF-8, NUL-aufgefüllt auf 32 Bytes)
 *     Dann folgt das SEGB-Magic bei Offset 52 (0x34)
 * </pre>
 *
 * <h2>SEGB v1 – Struktur</h2>
 * <pre>
 *   Header (16 Bytes):
 *     Bytes 0–3   : Magic "SEGB" (0x53454742)
 *     Bytes 4–7   : Versionsnummer (uint32 LE, = 1)
 *     Bytes 8–11  : Anzahl Einträge (uint32 LE)
 *     Bytes 12–15 : Reserviert
 *
 *   Einträge (variable Größe, direkt nach Header):
 *     Bytes 0–3   : Payload-Länge (uint32 LE)
 *     Bytes 4–11  : Zeitstempel   (float64 BE, Apple-Epoch)
 *     Bytes 12+   : Payload (typischerweise Protobuf)
 *     Padding bis nächstes Vielfaches von 8
 * </pre>
 *
 * <h2>SEGB v2 – Struktur</h2>
 * <pre>
 *   Header (32 Bytes):
 *     Bytes 0–3   : Magic "SEGB"
 *     Bytes 4–7   : Versionsnummer (uint32 LE, = 2)
 *     Bytes 8–11  : Anzahl Einträge (uint32 LE)
 *     Bytes 12–15 : Länge des Eintragsbereichs (uint32 LE)
 *     Bytes 16–31 : Reserviert
 *
 *   Trailer (je 24 Bytes pro Eintrag, wächst rückwärts nach dem Eintragsbereich):
 *     Bytes 0–7   : Entry-End-Offset (uint64 LE, relativ zu Eintragsbereich-Start)
 *     Bytes 8–15  : Zeitstempel      (float64 BE, Apple-Epoch)
 *     Bytes 16–23 : Metadaten / Reserviert
 *
 *   Einträge (nach Header, auf 4 Bytes ausgerichtet):
 *     Bytes 0–7   : Entry-interner Header (wird übersprungen)
 *     Bytes 8+    : Payload (Protobuf)
 * </pre>
 *
 * <h2>SEGB Fixed-Size-Variant (flags = 0x21, beobachtet in Biome-Dateien)</h2>
 * <pre>
 *   SEGB-Header (12 Bytes):
 *     Bytes 0–3   : Magic "SEGB"
 *     Bytes 4–7   : Flags 0x00000021 (uint32 LE) — kein klassisches Versionsfeld
 *     Bytes 8–11  : Anzahl aktiver Einträge (uint32 LE)
 *
 *   Einträge (72 Bytes / Record, Ring-Buffer):
 *     Bytes  0–7  : Zeitstempel-Start (float64 LE, Apple-Epoch)
 *     Bytes  8–15 : Zeitstempel-Ende  (float64 LE, Apple-Epoch; = Start bei punktuellen Events)
 *     Bytes 16–63 : Payload (48 Bytes; häufig leer bei reinen Timestamp-Events)
 *     Bytes 64–67 : Record-Marker (uint32 LE; 0x21 = aktiv, 0x2D = letzter, 0x00 = Sentinel)
 *     Bytes 68–71 : Flags / Reserviert (uint32 LE)
 * </pre>
 *
 * <h2>SEGB Fixed-Size-Variant, kompakt (flags = 2, reine Zeitstempel-Streams)</h2>
 * <pre>
 *   Header (12 Bytes): identisch zur regulären Fixed-Size-Variant, aber
 *   das Versions-/Flags-Feld trägt hier den Wert 2 — denselben Wert wie
 *   SEGB v2. Die Unterscheidung erfolgt über {@link #looksLikeV2}: ist der
 *   v2-Trailer-Header (entrySectionLen bei Offset 12) nicht plausibel,
 *   wird stattdessen dieses kompakte Fixed-Format angenommen.
 *
 *   Einträge (40 Bytes / Record, Ring-Buffer):
 *     Bytes  0–7  : Zeitstempel-Start (float64 LE, Apple-Epoch)
 *     Bytes  8–15 : Zeitstempel-Ende  (float64 LE, Apple-Epoch; = Start bei punktuellen Events)
 *     Bytes 16–35 : Reserviert (20 Bytes, in der Praxis meist 0)
 *     Bytes 36–39 : Record-Marker (uint32 LE; 0 = Sentinel/leerer Ring-Buffer-Slot)
 * </pre>
 *
 * <h2>SEGB Tagged-Entry-Variante (z. B. flags = 4, variable Eintragslängen)</h2>
 * <pre>
 *   Anders als bei den Fixed-Size-Varianten haben Einträge hier keine
 *   konstante Länge. Jeder Eintrag beginnt mit einem 8-Byte-Tag:
 *
 *     Bytes 0–3   : Typ-Code (uint32 LE; bestimmt die Payload-Länge,
 *                   aber nicht über eine bekannte Formel — siehe unten)
 *     Bytes 4–7   : Format-Konstante (uint32 LE) — bleibt über den
 *                   gesamten Stream hinweg konstant; derselbe Wert steht
 *                   auch im SEGB-Header an der Position von
 *                   "Anzahl Einträge" (Offset 8), da der SEGB-Header
 *                   zugleich den Tag des allerersten Eintrags bildet.
 *     Bytes 8–15  : Zeitstempel-Start (float64 LE, Apple-Epoch)
 *     Bytes 16–23 : Zeitstempel-Ende  (float64 LE, Apple-Epoch)
 *     Bytes 24+   : Payload (Länge variiert je nach Typ-Code)
 *
 *   Da die Payload-Länge nicht explizit codiert ist, wird der nächste
 *   Eintrag per Resynchronisation gefunden: ab Payload-Beginn wird in
 *   4-Byte-Schritten so lange gesucht, bis die Format-Konstante erneut an
 *   Position {@code p+4} auftritt — dort beginnt der nächste Eintrag.
 *   Diese Variante wird auch als Fallback für sonst nicht erkannte
 *   Flags-Werte versucht (siehe {@link #parse}).
 * </pre>
 *
 * <h2>SEGB Activity-Log-Variante (Header-Feld bei Offset 4 = Zeitstempel-Index-Größe)</h2>
 * <pre>
 *   Diese Variante (beobachtet z. B. bei "Activity"/App-Nutzungs-Tracking-
 *   Domains) hat ein komplett anderes Layout als alle obigen Varianten und
 *   wird nur als letzter Fallback versucht (siehe {@link #parse}):
 *
 *     Bytes 0–3   : Magic "SEGB"
 *     Bytes 4–7   : Anzahl Zeitstempel-Index-Einträge (uint32 LE) — wird im
 *                   generischen Dispatch fälschlich als "Flags" gelesen,
 *                   ist hier aber ein Zähler (siehe unten)
 *     Bytes 8–15  : Datei-Zeitstempel (float64 LE, Apple-Epoch)
 *     Bytes 16–27 : weitere Zähler/Reserviert (3× uint32 LE, Bedeutung noch
 *                   nicht vollständig geklärt)
 *
 *   Nach dem Header folgt ein weitgehend leerer (vorallokierter) Bereich;
 *   die eigentlichen Daten liegen in zwei nicht zusammenhängenden Blöcken:
 *
 *   Block 1 – Protobuf-Events (UUID + Aktivitätsname):
 *     Jeder Eintrag beginnt mit einem 8-Byte-Vorspann (Prüfsumme/Hash +
 *     4 Reserviert-Bytes = 0), gefolgt von einer Protobuf-Nachricht:
 *       Feld 1 (Tag 0x0A, length-delimited, Länge 36) : UUID-String
 *       Feld 2 (Tag 0x12, length-delimited)           : Aktivitätsname
 *                                                        (z. B. "Share",
 *                                                        "View 0 to 1 seconds")
 *       weitere Felder (Tags 0x18, 0x20, 0x48, …)      : kleine Zähler,
 *                                                        Bedeutung unklar
 *     Da auch hier keine explizite Eintragslänge codiert ist, wird der
 *     nächste Eintrag durch Suche nach dem nächsten plausiblen
 *     UUID-Muster (Tag 0x0A 0x24 + 36 Hex/Bindestrich-Zeichen + Tag 0x12)
 *     gefunden — der 8-Byte-Vorspann des nächsten Eintrags gehört nicht
 *     mehr zur aktuellen Payload.
 *
 *   Block 2 – Zeitstempel-Index (16 Bytes/Eintrag):
 *     Bytes 0–3  : absteigender Zähler (uint32 LE, Bedeutung unklar)
 *     Bytes 4–7  : Typ-Code (uint32 LE; 1 = korrespondiert 1:1 und in
 *                  derselben Reihenfolge mit den Block-1-Events, 3 = sonstige
 *                  Events ohne Protobuf-Payload)
 *     Bytes 8–15 : Zeitstempel (float64 LE, Apple-Epoch)
 *     Der Index-Block wird gefunden, indem 4-Byte-ausgerichtet so lange
 *     gesucht wird, bis vier aufeinanderfolgende 16-Byte-Fenster jeweils
 *     einen plausiblen Typ-Code (1 oder 3) und Zeitstempel ergeben.
 * </pre>
 */
public class SegbParser {

    // ── Konstanten ────────────────────────────────────────────────────────────

    private static final byte[] MAGIC              = {0x53, 0x45, 0x47, 0x42};
    private static final int    MAX_OUTER_SCAN     = 256;   // Bytes, in denen nach SEGB gesucht wird
    private static final int    FIXED_FLAGS        = 0x21;  // Kennzeichen für Fixed-Size-Variant
    private static final int    FIXED_RECORD_SIZE  = 72;    // Bytes pro Fixed-Size-Record
    private static final int    FIXED_HEADER_SIZE  = 12;    // SEGB-Header-Größe bei Fixed-Variant
    private static final int    FIXED_PAYLOAD_OFF  = 16;    // Payload-Start innerhalb eines Records
    private static final int    FIXED_PAYLOAD_LEN  = 48;    // Payload-Bytes pro Record
    private static final int    FIXED_MARKER_OFF   = 64;    // Marker-Offset innerhalb eines Records
    private static final int    MARKER_SENTINEL    = 0x00;  // leerer Ring-Buffer-Slot

    private static final int    FIXED_COMPACT_RECORD_SIZE = 40;  // Bytes pro Record (kompakte Variante)
    private static final int    FIXED_COMPACT_MARKER_OFF  = 36;  // Marker-Offset innerhalb eines Records

    /** Apple CFAbsoluteTime-Epoch: Sekunden zwischen 1970-01-01 und 2001-01-01. */
    private static final long APPLE_EPOCH_OFFSET_SECONDS = 978_307_200L;

    /** Plausibilitätsfenster für Apple-Epoch-Timestamps (2001-01-01 … 2060-01-01). */
    private static final double TS_MIN = 0.0;
    private static final double TS_MAX = 1_893_456_000.0;

    // ── Öffentliche API ───────────────────────────────────────────────────────

    /**
     * Liest eine Datei von {@code path} und parst sie.
     *
     * @param path Pfad zur SEGB- oder Biome-Datei
     * @return geparste {@link SegbFile}
     * @throws IOException        bei Lesefehlern
     * @throws SegbParseException wenn kein gültiges SEGB-Format erkannt wird
     */
    public SegbFile parse(Path path) throws IOException, SegbParseException {
        byte[] data = Files.readAllBytes(path);
        return parse(data, path.toString());
    }

    /**
     * Parst rohe Bytes. Der Outer-Biome-Container wird automatisch erkannt
     * und übersprungen — das SEGB-Magic muss nicht zwingend bei Offset 0 liegen.
     *
     * @param data       Dateiinhalt
     * @param sourcePath Pfad / Bezeichnung (nur für Anzeige im Modell)
     * @return geparste {@link SegbFile}
     * @throws SegbParseException wenn kein gültiges SEGB-Format gefunden wird
     */
    public SegbFile parse(byte[] data, String sourcePath) throws SegbParseException {
        if (data == null || data.length < 12) {
            throw new SegbParseException("Datei zu klein für einen SEGB-Header (<12 Bytes).");
        }

        // ── Schritt 1: SEGB-Magic lokalisieren ───────────────────────────────
        int segbOffset = locateMagic(data);

        // ── Schritt 2: Outer-Header-Metadaten extrahieren (optional) ─────────
        OuterHeader outer = segbOffset > 0 ? parseOuterHeader(data, segbOffset) : null;

        // ── Schritt 3: SEGB-Block ab segbOffset parsen ───────────────────────
        // Arbeite mit einem Slice, damit alle internen Offsets bei 0 beginnen
        byte[] segb = segbOffset == 0
                ? data
                : Arrays.copyOfRange(data, segbOffset, data.length);

        ByteBuffer buf = ByteBuffer.wrap(segb).order(ByteOrder.LITTLE_ENDIAN);
        int flags = buf.getInt(4);  // Bei v1/v2 steht hier die Versionsnummer

        return switch (flags) {
            case 1         -> parseV1(buf, segb, sourcePath, outer);
            // Reine Zeitstempel-Streams (z. B. manche Biome-Domains) tragen
            // hier ebenfalls den Wert 2, haben aber keinen gültigen
            // v2-Trailer-Header — looksLikeV2() unterscheidet anhand der
            // Plausibilität von entrySectionLen.
            case 2         -> looksLikeV2(buf, segb)
                    ? parseV2(buf, segb, sourcePath, outer)
                    : parseFixedCompact(buf, segb, sourcePath, outer);
            case FIXED_FLAGS -> parseFixed(buf, segb, sourcePath, outer);
            case 4         -> parseTagged(buf, segb, sourcePath, outer);
            default        -> {
                // Letzter Versuch: manche Biome-Domains verwenden weitere,
                // bisher nicht katalogisierte Flags-Werte für dasselbe
                // Tagged-Entry-Format wie flags=4. parseTagged() ist
                // selbstvalidierend (Resync über die Format-Konstante) –
                // liefert es mindestens einen Eintrag, übernehmen wir das
                // Ergebnis, sonst werfen wir den ursprünglichen Fehler.
                SegbFile tagged = parseTagged(buf, segb, sourcePath, outer);
                if (!tagged.getRecords().isEmpty()) {
                    yield tagged;
                }
                // Zweiter Fallback: Activity-Log-Variante (siehe Klassen-
                // Javadoc) — ebenfalls selbstvalidierend über das
                // UUID-Muster der Protobuf-Events.
                SegbFile activity = parseActivityEvents(buf, segb, sourcePath, outer);
                if (!activity.getRecords().isEmpty()) {
                    yield activity;
                }
                throw new SegbParseException(
                        String.format("Unbekannte SEGB-Version / Flags: 0x%08X (bei Datei-Offset 0x%X).",
                                flags, segbOffset + 4));
            }
        };
    }

    /**
     * Prüft, ob der SEGB-v2-Header (Trailer-basiert) bei {@code flags == 2}
     * plausibel ist. {@code entrySectionLen} (Offset 12) muss positiv sein
     * und der daraus berechnete Trailer-Start muss innerhalb der Datei
     * liegen — andernfalls handelt es sich um die kompakte Fixed-Size-
     * Variante (siehe {@link #parseFixedCompact}), die denselben Wert 2
     * im Versions-/Flags-Feld verwendet.
     */
    private static boolean looksLikeV2(ByteBuffer buf, byte[] segb) {
        if (segb.length < 32) return false;
        int entrySectionLen = buf.order(ByteOrder.LITTLE_ENDIAN).getInt(12);
        if (entrySectionLen < 0) return false;
        long trailerStart = 32L + entrySectionLen;
        return trailerStart <= segb.length;
    }

    // ── Outer-Header ─────────────────────────────────────────────────────────

    /** Metadaten aus dem Biome Outer-Container. */
    public record OuterHeader(int streamType, int outerVersion, Instant fileTimestamp,
                              String streamName, int segbOffset) {}

    /**
     * Extrahiert die Outer-Header-Metadaten (wird intern aufgerufen, wenn
     * das SEGB-Magic nicht bei Offset 0 liegt).
     */
    private static OuterHeader parseOuterHeader(byte[] data, int segbOffset) {
        if (data.length < 20) return null;
        try {
            ByteBuffer b = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            int  streamType    = b.getShort(0) & 0xFFFF;
            int  outerVersion  = b.getShort(2) & 0xFFFF;
            double fileTs      = b.getDouble(8);                // LE float64
            Instant fileInstant = isPlausibleTimestamp(fileTs)
                    ? appleTimestampToInstant(fileTs) : null;
            int nameLen        = Math.min(b.getInt(16), 64);
            String name        = nameLen > 0 && 20 + nameLen <= data.length
                    ? new String(data, 20, nameLen).replace("\0", "")
                    : "";
            return new OuterHeader(streamType, outerVersion, fileInstant, name, segbOffset);
        } catch (Exception e) {
            return null;
        }
    }

    // ── Magic-Suche ──────────────────────────────────────────────────────────

    /**
     * Sucht das SEGB-Magic in {@code data[0 .. MAX_OUTER_SCAN]}.
     *
     * @return Byte-Offset des Magic, oder 0 wenn es bei Offset 0 liegt
     * @throws SegbParseException wenn kein Magic gefunden wird
     */
    private static int locateMagic(byte[] data) throws SegbParseException {
        int limit = Math.min(MAX_OUTER_SCAN, data.length - 4);
        for (int i = 0; i <= limit; i++) {
            if (data[i]     == MAGIC[0] && data[i + 1] == MAGIC[1]
             && data[i + 2] == MAGIC[2] && data[i + 3] == MAGIC[3]) {
                return i;
            }
        }
        throw new SegbParseException(String.format(
                "SEGB-Magic (53 45 47 42) nicht in den ersten %d Bytes gefunden. "
                + "Erste Bytes: %02X %02X %02X %02X",
                MAX_OUTER_SCAN,
                data[0] & 0xFF, data[1] & 0xFF, data[2] & 0xFF, data[3] & 0xFF));
    }

    // ── SEGB v1 ───────────────────────────────────────────────────────────────

    private SegbFile parseV1(ByteBuffer buf, byte[] segb, String sourcePath,
                             OuterHeader outer) throws SegbParseException {
        int entryCount = buf.getInt(8);
        List<SegbRecord> records = new ArrayList<>(entryCount);
        int offset = 16; // Header-Größe v1

        for (int i = 0; i < entryCount; i++) {
            if (offset + 12 > segb.length) {
                throw new SegbParseException(String.format(
                        "Unerwartetes Dateiende bei v1-Eintrag #%d (Offset 0x%X).", i, offset));
            }

            buf.order(ByteOrder.LITTLE_ENDIAN);
            int payloadLength = buf.getInt(offset);

            buf.order(ByteOrder.BIG_ENDIAN);
            double appleTs = buf.getDouble(offset + 4);
            Instant timestamp = appleTimestampToInstant(appleTs);

            int payloadStart = offset + 12;
            if (payloadStart + payloadLength > segb.length) {
                throw new SegbParseException(String.format(
                        "Payload von v1-Eintrag #%d überschreitet Dateiende.", i));
            }

            byte[] payload = Arrays.copyOfRange(segb, payloadStart, payloadStart + payloadLength);
            records.add(new SegbRecord(i, offset, timestamp, payload));

            // Padding auf nächstes Vielfaches von 8
            int padded = ((12 + payloadLength) + 7) & ~7;
            offset += padded;
        }

        return new SegbFile(sourcePath, SegbFile.Version.V1, entryCount, records, outer);
    }

    // ── SEGB v2 ───────────────────────────────────────────────────────────────

    private SegbFile parseV2(ByteBuffer buf, byte[] segb, String sourcePath,
                             OuterHeader outer) throws SegbParseException {
        if (segb.length < 32) {
            throw new SegbParseException("Datei zu klein für einen SEGB-v2-Header (<32 Bytes).");
        }

        int entryCount      = buf.getInt(8);
        int entrySectionLen = buf.getInt(12);
        int trailerStart    = 32 + entrySectionLen;
        int trailerRecordSz = 24;

        List<SegbRecord> records = new ArrayList<>(entryCount);

        for (int i = 0; i < entryCount; i++) {
            int tOff = trailerStart + i * trailerRecordSz;
            if (tOff + trailerRecordSz > segb.length) {
                throw new SegbParseException(String.format(
                        "Trailer-Eintrag #%d außerhalb der Datei (Offset 0x%X).", i, tOff));
            }

            buf.order(ByteOrder.LITTLE_ENDIAN);
            long entryEndOffset = buf.getLong(tOff);

            buf.order(ByteOrder.BIG_ENDIAN);
            double appleTs = buf.getDouble(tOff + 8);
            Instant timestamp = appleTimestampToInstant(appleTs);

            long prevEnd = (i == 0) ? 0L
                    : buf.order(ByteOrder.LITTLE_ENDIAN).getLong(trailerStart + (i - 1) * trailerRecordSz);
            long entryStart = 32 + alignUp(prevEnd, 4);
            long entryEnd   = 32 + entryEndOffset;

            if (entryStart + 8 > segb.length || entryEnd > segb.length || entryStart >= entryEnd) {
                records.add(new SegbRecord(i, entryStart, timestamp, new byte[0]));
                continue;
            }

            int payloadStart = (int) (entryStart + 8);
            byte[] payload = Arrays.copyOfRange(segb, payloadStart, (int) entryEnd);
            records.add(new SegbRecord(i, entryStart, timestamp, payload));
        }

        return new SegbFile(sourcePath, SegbFile.Version.V2, entryCount, records, outer);
    }

    // ── SEGB Fixed-Size-Variant (flags = 0x21) ────────────────────────────────

    /**
     * Parst das Fixed-Size-Ring-Buffer-Format, das in Biome-Dateien mit
     * Flags-Wert {@code 0x21} beobachtet wurde. Jeder Record ist genau
     * {@value #FIXED_RECORD_SIZE} Bytes groß.
     *
     * <p>Der Zähler im SEGB-Header gibt die Anzahl der <em>aktiven</em> Records
     * an; der Ring-Buffer kann mehr Slots enthalten. Records werden gelesen,
     * bis ein Sentinel (Marker = {@value #MARKER_SENTINEL}) oder das Dateiende
     * erreicht wird.
     */
    private SegbFile parseFixed(ByteBuffer buf, byte[] segb, String sourcePath,
                                OuterHeader outer) {
        int declaredCount = buf.order(ByteOrder.LITTLE_ENDIAN).getInt(8);
        List<SegbRecord> records = new ArrayList<>();

        int offset = FIXED_HEADER_SIZE;
        int index  = 0;

        while (offset + FIXED_RECORD_SIZE <= segb.length) {

            // Zeitstempel als LE float64 lesen
            buf.order(ByteOrder.LITTLE_ENDIAN);
            double tsStart = buf.getDouble(offset);
            double tsEnd   = buf.getDouble(offset + 8);

            int marker = buf.getInt(offset + FIXED_MARKER_OFF);

            // Sentinel oder leerer Slot → Ring-Buffer-Ende
            if (marker == MARKER_SENTINEL && !isPlausibleTimestamp(tsStart)) {
                break;
            }

            Instant timestamp = isPlausibleTimestamp(tsStart)
                    ? appleTimestampToInstant(tsStart) : null;
            Instant timestampEnd = isPlausibleTimestamp(tsEnd)
                    ? appleTimestampToInstant(tsEnd) : null;

            byte[] payload = Arrays.copyOfRange(segb,
                    offset + FIXED_PAYLOAD_OFF,
                    offset + FIXED_PAYLOAD_OFF + FIXED_PAYLOAD_LEN);

            records.add(new SegbRecord(index, offset, timestamp, timestampEnd, payload, marker));

            index++;
            offset += FIXED_RECORD_SIZE;
        }

        return new SegbFile(sourcePath, SegbFile.Version.FIXED, declaredCount, records, outer);
    }

    // ── SEGB Fixed-Size-Variant, kompakt (flags = 2) ──────────────────────────

    /**
     * Parst die kompakte Fixed-Size-Ring-Buffer-Variante reiner
     * Zeitstempel-Streams (siehe Klassen-Javadoc). Im Unterschied zu
     * {@link #parseFixed} gibt es keinen separaten Payload-Bereich — die
     * 24 Bytes nach dem Zeitstempel-Paar (Reserviert + Marker) werden
     * vollständig als Payload übernommen, damit sie im Hex-Dump sichtbar
     * bleiben; der Marker wird zusätzlich separat ausgewertet, um das
     * Ring-Buffer-Ende zu erkennen.
     */
    private SegbFile parseFixedCompact(ByteBuffer buf, byte[] segb, String sourcePath,
                                       OuterHeader outer) {
        int declaredCount = buf.order(ByteOrder.LITTLE_ENDIAN).getInt(8);
        List<SegbRecord> records = new ArrayList<>();

        int offset = FIXED_HEADER_SIZE;
        int index  = 0;

        while (offset + FIXED_COMPACT_RECORD_SIZE <= segb.length) {

            buf.order(ByteOrder.LITTLE_ENDIAN);
            double tsStart = buf.getDouble(offset);
            double tsEnd   = buf.getDouble(offset + 8);
            int marker     = buf.getInt(offset + FIXED_COMPACT_MARKER_OFF);

            // Sentinel oder leerer Slot → Ring-Buffer-Ende
            if (marker == MARKER_SENTINEL && !isPlausibleTimestamp(tsStart)) {
                break;
            }

            Instant timestamp = isPlausibleTimestamp(tsStart)
                    ? appleTimestampToInstant(tsStart) : null;
            Instant timestampEnd = isPlausibleTimestamp(tsEnd)
                    ? appleTimestampToInstant(tsEnd) : null;

            byte[] payload = Arrays.copyOfRange(segb, offset + 16, offset + FIXED_COMPACT_RECORD_SIZE);

            records.add(new SegbRecord(index, offset, timestamp, timestampEnd, payload, marker));

            index++;
            offset += FIXED_COMPACT_RECORD_SIZE;
        }

        return new SegbFile(sourcePath, SegbFile.Version.FIXED, declaredCount, records, outer);
    }

    // ── SEGB Tagged-Entry-Variante (z. B. flags = 4) ──────────────────────────

    /**
     * Parst Streams mit getaggten, variabel langen Einträgen (siehe
     * Klassen-Javadoc). Jeder Eintrag beginnt mit einem 8-Byte-Tag
     * (Typ-Code + Format-Konstante), gefolgt von Start-/End-Zeitstempel
     * und einer Payload unbekannter Länge. Der nächste Eintrag wird per
     * Resynchronisation auf die Format-Konstante gefunden, da die
     * Payload-Länge nicht explizit codiert ist.
     *
     * <p>Der Typ-Code des jeweiligen Eintrags wird in
     * {@link SegbRecord#getMarker()} abgelegt (analog zu den
     * Fixed-Size-Varianten), auch wenn er hier keine Ring-Buffer-Markierung
     * im engeren Sinn ist, sondern den Eintragstyp kennzeichnet.
     */
    private SegbFile parseTagged(ByteBuffer buf, byte[] segb, String sourcePath,
                                 OuterHeader outer) {
        buf.order(ByteOrder.LITTLE_ENDIAN);
        int formatConst = buf.getInt(8);
        List<SegbRecord> records = new ArrayList<>();

        int headerOffset = 4; // SEGB-Header = Magic(4) + Tag des ersten Eintrags (8)
        int index = 0;

        while (true) {
            int tsOff = headerOffset + 8;
            if (tsOff + 16 > segb.length) break;

            int typeCode   = buf.getInt(headerOffset);
            double tsStart = buf.getDouble(tsOff);
            double tsEnd   = buf.getDouble(tsOff + 8);
            int payloadStart = tsOff + 16;

            int p = payloadStart;
            int nextHeader = -1;
            while (p + 8 <= segb.length) {
                if (buf.getInt(p + 4) == formatConst) {
                    nextHeader = p;
                    break;
                }
                p += 4;
            }
            if (nextHeader == -1) break;

            Instant timestamp = isPlausibleTimestamp(tsStart)
                    ? appleTimestampToInstant(tsStart) : null;
            Instant timestampEnd = isPlausibleTimestamp(tsEnd)
                    ? appleTimestampToInstant(tsEnd) : null;
            byte[] payload = Arrays.copyOfRange(segb, payloadStart, nextHeader);

            records.add(new SegbRecord(index, headerOffset, timestamp, timestampEnd, payload, typeCode));

            index++;
            headerOffset = nextHeader;
        }

        return new SegbFile(sourcePath, SegbFile.Version.FIXED, records.size(), records, outer);
    }

    // ── SEGB Activity-Log-Variante ────────────────────────────────────────────

    /** Erwartete Länge eines UUID-Strings in den Activity-Protobuf-Events. */
    private static final int ACTIVITY_UUID_LEN = 36;
    /** Mindestlänge eines durchgängigen Null-Laufs, der das Ende von Block 1 markiert. */
    private static final int ACTIVITY_ZERO_RUN = 32;
    /** Größe eines Zeitstempel-Index-Eintrags in Block 2. */
    private static final int ACTIVITY_INDEX_RECORD_SIZE = 16;

    /**
     * Parst die Activity-Log-Variante (siehe Klassen-Javadoc). Da diese
     * Variante kein eindeutiges Kennungs-Byte hat, wird sie nur als letzter
     * Fallback versucht und ist selbstvalidierend: Sie liefert nur dann
     * Records, wenn mindestens ein UUID-Event (Block 1) gefunden wird.
     *
     * <p>Block 2 (Zeitstempel-Index) wird, sofern vorhanden, separat als
     * Folge von {@code Marker=Typ-Code}-Records angehängt; Einträge vom
     * Typ-Code 1 werden stattdessen — sofern Anzahl und Reihenfolge zu
     * Block 1 passen — den Activity-Events als Zeitstempel zugeordnet
     * (siehe unten), um Duplikate zu vermeiden.
     */
    private SegbFile parseActivityEvents(ByteBuffer buf, byte[] segb, String sourcePath,
                                         OuterHeader outer) {
        buf.order(ByteOrder.LITTLE_ENDIAN);

        // ── Block 1: UUID + Aktivitätsname (Protobuf-artige Events) ─────────
        List<Integer> anchors = new ArrayList<>();
        int limit = segb.length - (2 + ACTIVITY_UUID_LEN + 1);
        int p = 8; // mind. 8 Bytes Platz für den Vorspann des ersten Events lassen
        while (p <= limit) {
            if ((segb[p] & 0xFF) == 0x0A && (segb[p + 1] & 0xFF) == 0x24
                    && looksLikeUuid(segb, p + 2)
                    && (segb[p + 2 + ACTIVITY_UUID_LEN] & 0xFF) == 0x12) {
                anchors.add(p);
                p += 2 + ACTIVITY_UUID_LEN + 1; // Einträge überlappen nicht
            } else {
                p++;
            }
        }

        if (anchors.isEmpty()) {
            return new SegbFile(sourcePath, SegbFile.Version.ACTIVITY, 0, new ArrayList<>(), outer);
        }

        // Jedem Event geht ein 8-Byte-Vorspann (Prüfsumme/Hash + 4 Bytes
        // Reserviert) voraus. Die Payload wird inklusive dieses Vorspanns
        // übernommen — analog zu v1/v2, deren Payload ebenfalls mit einem
        // 8-Byte "Biome-internen" Header beginnt (siehe buildSegbTree in
        // BinViewer, das diese 8 Bytes beim automatischen Aufklappen
        // überspringt).
        List<SegbRecord> activityRecords = new ArrayList<>(anchors.size());
        for (int i = 0; i < anchors.size(); i++) {
            int a        = anchors.get(i);
            int hdrStart = a - 8;
            int end      = (i + 1 < anchors.size()) ? anchors.get(i + 1) - 8 : findZeroRun(segb, a);

            byte[] payload = Arrays.copyOfRange(segb, hdrStart, Math.max(hdrStart, end));
            activityRecords.add(new SegbRecord(i, hdrStart, null, payload));
        }

        // ── Block 2: Zeitstempel-Index (16 Bytes/Eintrag) ────────────────────
        int chunk2Start = findActivityIndexStart(segb);
        List<SegbRecord> indexRecords = new ArrayList<>();
        List<Instant> type1Timestamps = new ArrayList<>();

        if (chunk2Start >= 0) {
            int idx = 0;
            int q = chunk2Start;
            while (isActivityIndexWindowValid(buf, segb, q)) {
                int typeCode = buf.getInt(q + 4);
                double tsRaw = buf.getDouble(q + 8);
                Instant ts   = appleTimestampToInstant(tsRaw);

                if (typeCode == 1) {
                    type1Timestamps.add(ts);
                } else {
                    byte[] raw = Arrays.copyOfRange(segb, q, q + ACTIVITY_INDEX_RECORD_SIZE);
                    indexRecords.add(new SegbRecord(idx, q, ts, null, raw, typeCode));
                }
                idx++;
                q += ACTIVITY_INDEX_RECORD_SIZE;
            }
        }

        // Zeitstempel den Activity-Events zuordnen, wenn Anzahl + Reihenfolge passen
        if (type1Timestamps.size() == activityRecords.size()) {
            for (int i = 0; i < activityRecords.size(); i++) {
                SegbRecord r = activityRecords.get(i);
                activityRecords.set(i, new SegbRecord(r.getIndex(), r.getOffset(),
                        type1Timestamps.get(i), r.getPayload()));
            }
        }

        List<SegbRecord> all = new ArrayList<>(activityRecords.size() + indexRecords.size());
        all.addAll(activityRecords);
        all.addAll(indexRecords);

        return new SegbFile(sourcePath, SegbFile.Version.ACTIVITY, all.size(), all, outer);
    }

    /** Prüft, ob 36 Bytes ab {@code p} einem UUID-String (8-4-4-4-12, Hex + Bindestriche) entsprechen. */
    private static boolean looksLikeUuid(byte[] data, int p) {
        if (p + ACTIVITY_UUID_LEN > data.length) return false;
        for (int i = 0; i < ACTIVITY_UUID_LEN; i++) {
            int c = data[p + i] & 0xFF;
            boolean dashExpected = (i == 8 || i == 13 || i == 18 || i == 23);
            if (dashExpected) {
                if (c != '-') return false;
            } else if (!isHexDigit(c)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isHexDigit(int c) {
        return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f');
    }

    /** Sucht ab {@code start} die erste Position mit mindestens {@value #ACTIVITY_ZERO_RUN} Null-Bytes. */
    private static int findZeroRun(byte[] data, int start) {
        int n = data.length;
        for (int i = start; i + ACTIVITY_ZERO_RUN <= n; i++) {
            boolean allZero = true;
            for (int j = 0; j < ACTIVITY_ZERO_RUN; j++) {
                if (data[i + j] != 0) { allZero = false; break; }
            }
            if (allZero) return i;
        }
        return n;
    }

    /**
     * Sucht den Beginn von Block 2 (Zeitstempel-Index): die erste 4-Byte-
     * ausgerichtete Position, an der vier aufeinanderfolgende 16-Byte-
     * Fenster jeweils einen plausiblen Typ-Code (1 oder 3) und Zeitstempel
     * ergeben — das schließt Zufallstreffer auf einzelnen Feldern aus.
     */
    private static int findActivityIndexStart(byte[] segb) {
        ByteBuffer buf = ByteBuffer.wrap(segb).order(ByteOrder.LITTLE_ENDIAN);
        int n = segb.length;
        for (int p = 0; p + ACTIVITY_INDEX_RECORD_SIZE * 4 <= n; p += 4) {
            if (isActivityIndexWindowValid(buf, segb, p)
                    && isActivityIndexWindowValid(buf, segb, p + ACTIVITY_INDEX_RECORD_SIZE)
                    && isActivityIndexWindowValid(buf, segb, p + ACTIVITY_INDEX_RECORD_SIZE * 2)
                    && isActivityIndexWindowValid(buf, segb, p + ACTIVITY_INDEX_RECORD_SIZE * 3)) {
                return p;
            }
        }
        return -1;
    }

    private static boolean isActivityIndexWindowValid(ByteBuffer buf, byte[] segb, int p) {
        if (p + ACTIVITY_INDEX_RECORD_SIZE > segb.length) return false;
        int typeCode = buf.getInt(p + 4);
        if (typeCode != 1 && typeCode != 3) return false;
        double ts = buf.getDouble(p + 8);
        return isPlausibleTimestamp(ts);
    }

    // ── Hilfsmethoden ────────────────────────────────────────────────────────

    private static Instant appleTimestampToInstant(double appleTime) {
        if (!isPlausibleTimestamp(appleTime)) return null;
        long seconds = (long) appleTime;
        long nanos   = (long) ((appleTime - seconds) * 1_000_000_000L);
        return Instant.ofEpochSecond(seconds + APPLE_EPOCH_OFFSET_SECONDS, Math.max(0, nanos));
    }

    private static boolean isPlausibleTimestamp(double v) {
        return !Double.isNaN(v) && !Double.isInfinite(v) && v > TS_MIN && v < TS_MAX;
    }

    private static long alignUp(long value, int alignment) {
        return (value + alignment - 1) & ~(alignment - 1L);
    }
}
