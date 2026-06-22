package fqlite.viewer.model;

import fqlite.viewer.parser.SegbParser;

import java.util.Collections;
import java.util.List;

/**
 * Repräsentiert eine geparste SEGB-Datei mit allen enthaltenen Einträgen.
 */
public class SegbFile {

    public enum Version {
        V1,
        V2,
        /** Fixed-Size-Ring-Buffer-Variant (Flags = 0x21, beobachtet in Biome-Dateien). */
        FIXED,
        /** Activity-Log-Variante: Protobuf-Events (UUID + Aktivitätsname) plus separater Zeitstempel-Index. */
        ACTIVITY,
        UNKNOWN
    }

    private final String                    filePath;
    private final Version                   version;
    /** Anzahl aktiver Einträge laut SEGB-Header (bei FIXED = deklarierte Kapazität). */
    private final int                       entryCount;
    private final List<SegbRecord>          records;
    /** Metadaten aus dem optionalen Biome Outer-Container; {@code null} wenn nicht vorhanden. */
    private final SegbParser.OuterHeader    outerHeader;

    public SegbFile(String filePath, Version version, int entryCount,
                    List<SegbRecord> records, SegbParser.OuterHeader outerHeader) {
        this.filePath    = filePath;
        this.version     = version;
        this.entryCount  = entryCount;
        this.records     = Collections.unmodifiableList(records);
        this.outerHeader = outerHeader;
    }

    /** Kompatibilitätskonstruktor ohne Outer-Header (für Tests). */
    public SegbFile(String filePath, Version version, int entryCount, List<SegbRecord> records) {
        this(filePath, version, entryCount, records, null);
    }

    public String                 getFilePath()    { return filePath;    }
    public Version                getVersion()     { return version;     }
    public int                    getEntryCount()  { return entryCount;  }
    public List<SegbRecord>       getRecords()     { return records;     }
    public SegbParser.OuterHeader getOuterHeader() { return outerHeader; }

    /** Gibt {@code true} zurück, wenn ein Biome Outer-Container vorhanden war. */
    public boolean hasOuterHeader() { return outerHeader != null; }

    @Override
    public String toString() {
        return String.format("SEGB %s | %d Einträge | %s", version, entryCount, filePath);
    }
}
