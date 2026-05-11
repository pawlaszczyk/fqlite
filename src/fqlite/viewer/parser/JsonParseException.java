package fqlite.viewer.parser;

public class JsonParseException extends Exception {
    public JsonParseException(String message)                  { super(message); }
    public JsonParseException(String message, Throwable cause) { super(message, cause); }
}
