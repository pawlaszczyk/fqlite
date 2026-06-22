package fqlite.viewer.parser;

/**
 * Wird geworfen, wenn eine Datei zwar das "bplist"-Magic trägt, aber eine
 * Versions-Variante verwendet, für die es keine öffentliche Spezifikation
 * gibt (z. B. "bplist15" oder "bplist16").
 *
 * <p>Im Gegensatz zu {@code bplist00} (Standardformat, dokumentiert über
 * Apples Open-Source-CFBinaryPList.c) sind diese Varianten intern bei Apple
 * und unterscheiden sich strukturell — "bplist16" hat z. B. keinen Trailer
 * und zusätzliche, undokumentierte Datentypen (UUID, URL, Sets, NULL).
 * Ein Parsing-Versuch nach dem bplist00-Schema (Trailer in den letzten
 * 32 Bytes, feste Objekt-/Offset-Tabelle) würde hier mit hoher
 * Wahrscheinlichkeit falsche Werte statt eines Fehlers liefern — für ein
 * forensisches Tool ist das schlimmer als ein klar erkennbarer Abbruch.</p>
 */
public class BPListUnsupportedVersionException extends BPListParseException {

    private final String version;

    public BPListUnsupportedVersionException(String version) {
        super("Nicht unterstützte BPList-Version: bplist" + version
                + " (keine öffentliche Spezifikation verfügbar)");
        this.version = version;
    }

    /** Die zwei Versionsziffern aus dem Magic, z. B. "15" oder "16". */
    public String getVersion() { return version; }
}
