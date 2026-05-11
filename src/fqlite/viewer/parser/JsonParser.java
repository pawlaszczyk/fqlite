package fqlite.viewer.parser;

import fqlite.viewer.parser.JsonNode;

/**
 * Hand-written, dependency-free, recursive-descent JSON parser.
 *
 * Compliant with RFC 8259 / ECMA-404.
 * Produces a tree of {@link JsonNode}s.
 *
 * Limitations (acceptable for a forensic viewer):
 *   – Duplicate object keys: last value wins (LinkedHashMap preserves insertion order up
 *     to the first duplicate; thereafter the duplicate replaces the earlier entry's value).
 *   – Maximum nesting depth: 512 (to guard against stack overflow on malicious input).
 */
public class JsonParser {

    private static final int MAX_DEPTH = 512;

    // ── Parse state ─────────────────────────────────────────────────────
    private String  src;
    private int     pos;
    private int     depth;

    // ── Public API ──────────────────────────────────────────────────────

    /**
     * Parse a JSON text.
     *
     * @param json  raw JSON string (must not be null)
     * @return      root JsonNode
     * @throws JsonParseException on any syntax error
     */
    public synchronized JsonNode parse(String json) throws JsonParseException {
        this.src   = json;
        this.pos   = 0;
        this.depth = 0;
        skipWhitespace();
        JsonNode root = parseValue();
        skipWhitespace();
        if (pos < src.length()) {
            throw new JsonParseException(
                    "Unexpected trailing content at position " + pos
                    + ": " + excerpt());
        }
        return root;
    }

    // ── Value dispatch ──────────────────────────────────────────────────

    private JsonNode parseValue() throws JsonParseException {
        if (depth > MAX_DEPTH) throw new JsonParseException("Nesting depth exceeds " + MAX_DEPTH);
        if (pos >= src.length()) throw new JsonParseException("Unexpected end of input");

        char c = src.charAt(pos);
        return switch (c) {
            case '{'       -> parseObject();
            case '['       -> parseArray();
            case '"'       -> parseString();
            case 't', 'f'  -> parseBoolean();
            case 'n'       -> parseNull();
            case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'
                           -> parseNumber();
            default        -> throw new JsonParseException(
                    "Unexpected character '" + c + "' at position " + pos);
        };
    }

    // ── Object ──────────────────────────────────────────────────────────

    private JsonNode parseObject() throws JsonParseException {
        expect('{');
        depth++;
        JsonNode node = new JsonNode(JsonNode.Type.OBJECT);
        skipWhitespace();
        if (peek() == '}') { advance(); depth--; return node; }

        while (true) {
            skipWhitespace();
            if (pos >= src.length()) throw new JsonParseException("Unterminated object");

            // Key must be a string
            if (src.charAt(pos) != '"')
                throw new JsonParseException("Expected string key at position " + pos);
            String key = readStringLiteral();

            skipWhitespace();
            expect(':');
            skipWhitespace();

            JsonNode value = parseValue();
            value.setKeyLabel(key);
            node.addObjectEntry(key, value);

            skipWhitespace();
            char next = peek();
            if (next == '}') { advance(); break; }
            if (next == ',') { advance(); continue; }
            throw new JsonParseException("Expected ',' or '}' at position " + pos);
        }
        depth--;
        return node;
    }

    // ── Array ───────────────────────────────────────────────────────────

    private JsonNode parseArray() throws JsonParseException {
        expect('[');
        depth++;
        JsonNode node = new JsonNode(JsonNode.Type.ARRAY);
        skipWhitespace();
        if (peek() == ']') { advance(); depth--; return node; }

        while (true) {
            skipWhitespace();
            if (pos >= src.length()) throw new JsonParseException("Unterminated array");

            JsonNode el = parseValue();
            node.addArrayElement(el);

            skipWhitespace();
            char next = peek();
            if (next == ']') { advance(); break; }
            if (next == ',') { advance(); continue; }
            throw new JsonParseException("Expected ',' or ']' at position " + pos);
        }
        depth--;
        return node;
    }

    // ── String ──────────────────────────────────────────────────────────

    private JsonNode parseString() throws JsonParseException {
        return new JsonNode(JsonNode.Type.STRING, readStringLiteral());
    }

    /** Reads a JSON string literal (with escape processing) and returns the *value* (no quotes). */
    private String readStringLiteral() throws JsonParseException {
        expect('"');
        StringBuilder sb = new StringBuilder();
        while (pos < src.length()) {
            char c = src.charAt(pos++);
            if (c == '"') return sb.toString();
            if (c == '\\') {
                if (pos >= src.length()) break;
                char esc = src.charAt(pos++);
                switch (esc) {
                    case '"'  -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case '/'  -> sb.append('/');
                    case 'b'  -> sb.append('\b');
                    case 'f'  -> sb.append('\f');
                    case 'n'  -> sb.append('\n');
                    case 'r'  -> sb.append('\r');
                    case 't'  -> sb.append('\t');
                    case 'u'  -> {
                        if (pos + 4 > src.length())
                            throw new JsonParseException("Incomplete \\u escape at position " + (pos - 2));
                        String hex = src.substring(pos, pos + 4);
                        try {
                            sb.append((char) Integer.parseInt(hex, 16));
                        } catch (NumberFormatException e) {
                            throw new JsonParseException("Invalid \\u escape: \\u" + hex);
                        }
                        pos += 4;
                    }
                    default -> sb.append(esc);   // lenient: pass through unknown escapes
                }
            } else {
                sb.append(c);
            }
        }
        throw new JsonParseException("Unterminated string starting before position " + pos);
    }

    // ── Boolean ─────────────────────────────────────────────────────────

    private JsonNode parseBoolean() throws JsonParseException {
        if (src.startsWith("true", pos)) {
            pos += 4;
            return new JsonNode(JsonNode.Type.BOOLEAN, "true");
        }
        if (src.startsWith("false", pos)) {
            pos += 5;
            return new JsonNode(JsonNode.Type.BOOLEAN, "false");
        }
        throw new JsonParseException("Expected 'true' or 'false' at position " + pos);
    }

    // ── Null ────────────────────────────────────────────────────────────

    private JsonNode parseNull() throws JsonParseException {
        if (src.startsWith("null", pos)) {
            pos += 4;
            return new JsonNode(JsonNode.Type.NULL, "null");
        }
        throw new JsonParseException("Expected 'null' at position " + pos);
    }

    // ── Number ──────────────────────────────────────────────────────────

    private JsonNode parseNumber() throws JsonParseException {
        int start = pos;
        if (pos < src.length() && src.charAt(pos) == '-') pos++;
        if (pos >= src.length()) throw new JsonParseException("Truncated number at position " + start);

        // Integer part
        if (src.charAt(pos) == '0') {
            pos++;
        } else {
            while (pos < src.length() && Character.isDigit(src.charAt(pos))) pos++;
        }
        // Fraction
        if (pos < src.length() && src.charAt(pos) == '.') {
            pos++;
            while (pos < src.length() && Character.isDigit(src.charAt(pos))) pos++;
        }
        // Exponent
        if (pos < src.length() && (src.charAt(pos) == 'e' || src.charAt(pos) == 'E')) {
            pos++;
            if (pos < src.length() && (src.charAt(pos) == '+' || src.charAt(pos) == '-')) pos++;
            while (pos < src.length() && Character.isDigit(src.charAt(pos))) pos++;
        }
        return new JsonNode(JsonNode.Type.NUMBER, src.substring(start, pos));
    }

    // ── Low-level helpers ───────────────────────────────────────────────

    private void skipWhitespace() {
        while (pos < src.length() && Character.isWhitespace(src.charAt(pos))) pos++;
    }

    private char peek() {
        return pos < src.length() ? src.charAt(pos) : '\0';
    }

    private void advance() { pos++; }

    private void expect(char c) throws JsonParseException {
        if (pos >= src.length() || src.charAt(pos) != c) {
            throw new JsonParseException(
                    "Expected '" + c + "' at position " + pos + " but found: " + excerpt());
        }
        pos++;
    }

    private String excerpt() {
        int end = Math.min(pos + 20, src.length());
        return "«" + src.substring(pos, end) + (end < src.length() ? "…" : "") + "»";
    }
}
