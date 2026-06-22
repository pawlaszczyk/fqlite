package fqlite.viewer.model;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Repräsentiert einen einzelnen Eintrag (Record) in einer SEGB-Datei.
 *
 * <p>Bei v1/v2 enthält {@link #getPayload()} typischerweise Protobuf-Daten.
 * Bei der Fixed-Size-Variant sind die Payloads häufig leer (reine Timestamp-Events);
 * {@link #getTimestampEnd()} und {@link #getMarker()} sind dann zusätzlich befüllt.
 */
public class SegbRecord {

    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    /** Marker-Wert für aktive Records in der Fixed-Size-Variant. */
    public static final int MARKER_ACTIVE   = 0x21;
    /** Marker-Wert für den letzten Record vor dem Sentinel. */
    public static final int MARKER_LAST     = 0x2D;
    /** Marker-Wert für leere / Sentinel-Slots. */
    public static final int MARKER_SENTINEL = 0x00;

    private final int     index;
    private final long    offset;
    private final Instant timestamp;
    /** Ende-Zeitstempel (nur Fixed-Size-Variant; sonst {@code null}). */
    private final Instant timestampEnd;
    private final byte[]  payload;
    /** Record-Marker (nur Fixed-Size-Variant; sonst -1). */
    private final int     marker;

    /** Konstruktor für v1/v2. */
    public SegbRecord(int index, long offset, Instant timestamp, byte[] payload) {
        this(index, offset, timestamp, null, payload, -1);
    }

    /** Vollständiger Konstruktor für die Fixed-Size-Variant. */
    public SegbRecord(int index, long offset, Instant timestamp, Instant timestampEnd,
                      byte[] payload, int marker) {
        this.index        = index;
        this.offset       = offset;
        this.timestamp    = timestamp;
        this.timestampEnd = timestampEnd;
        this.payload      = payload;
        this.marker       = marker;
    }

    public int     getIndex()        { return index;        }
    public long    getOffset()       { return offset;       }
    public Instant getTimestamp()    { return timestamp;    }
    public Instant getTimestampEnd() { return timestampEnd; }
    public byte[]  getPayload()      { return payload;      }
    public int     getMarker()       { return marker;       }

    /** {@code true} wenn Anfangs- und End-Zeitstempel gesetzt und verschieden sind. */
    public boolean hasDuration() {
        return timestampEnd != null && timestamp != null && !timestamp.equals(timestampEnd);
    }

    /** {@code true} wenn es sich um einen Fixed-Size-Record handelt (marker != -1). */
    public boolean isFixedSize() { return marker != -1; }

    /** Gibt die Nutzlast als klassischen Hex-Dump zurück. */
    public String getPayloadHex() {
        if (payload == null || payload.length == 0) return "(keine Payload-Daten)";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < payload.length; i++) {
            if (i > 0 && i % 16 == 0) sb.append('\n');
            else if (i > 0 && i % 8  == 0) sb.append(' ');
            sb.append(String.format("%02X ", payload[i] & 0xFF));
        }
        return sb.toString().trim();
    }

    @Override
    public String toString() {
        String ts = timestamp != null ? FMT.format(timestamp) : "–";
        return String.format("Record #%d | Offset: 0x%X | %s | %d Bytes Payload",
                index, offset, ts, payload.length);
    }
}
