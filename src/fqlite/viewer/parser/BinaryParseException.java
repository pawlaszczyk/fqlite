package fqlite.viewer.parser;

/** Thrown by BSON, MessagePack, Thrift and FlatBuffers parsers on malformed input. */
public class BinaryParseException extends Exception {
    private final String parserName;

    public BinaryParseException(String parserName, String message) {
        super("[" + parserName + "] " + message);
        this.parserName = parserName;
    }

    public BinaryParseException(String parserName, String message, Throwable cause) {
        super("[" + parserName + "] " + message, cause);
        this.parserName = parserName;
    }

    public String getParserName() { return parserName; }
}
