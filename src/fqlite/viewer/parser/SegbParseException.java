package fqlite.viewer.parser;

/**
 * Wird geworfen, wenn eine SEGB-Datei nicht geparst werden kann.
 */
public class SegbParseException extends Exception {
    public SegbParseException(String message) {
        super(message);
    }
    public SegbParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
