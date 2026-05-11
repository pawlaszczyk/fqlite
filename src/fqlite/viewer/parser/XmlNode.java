package fqlite.viewer.parser;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents one node in a parsed XML document.
 *
 * Node types:
 *   ELEMENT   – a tag, may contain attributes, children and/or text
 *   TEXT      – a text-only leaf (trimmed whitespace)
 *   ATTRIBUTE – synthetic node produced for the detail panel
 *   COMMENT   – XML comment (<!-- … -->)
 *   CDATA     – <![CDATA[ … ]]>
 */
public class XmlNode {

    public enum Type { ELEMENT, TEXT, ATTRIBUTE, COMMENT, CDATA }

    // ── Fields ──────────────────────────────────────────────────────────
    private final Type                   type;
    private final String                 name;       // tag name or "#text" / "#comment"
    private       String                 textValue;  // for TEXT / COMMENT / CDATA / ATTRIBUTE
    private final Map<String, String>    attributes  = new LinkedHashMap<>();
    private final List<XmlNode>          children    = new ArrayList<>();

    // ── Constructors ────────────────────────────────────────────────────
    public XmlNode(Type type, String name) {
        this.type = type;
        this.name = name;
    }

    public XmlNode(Type type, String name, String textValue) {
        this.type      = type;
        this.name      = name;
        this.textValue = textValue;
    }

    // ── Accessors ───────────────────────────────────────────────────────
    public Type              getType()       { return type; }
    public String            getName()       { return name; }
    public String            getTextValue()  { return textValue; }
    public Map<String,String>getAttributes() { return attributes; }
    public List<XmlNode>     getChildren()   { return children; }

    public void setTextValue(String v)            { this.textValue = v; }
    public void addAttribute(String k, String v)  { attributes.put(k, v); }
    public void addChild(XmlNode child)           { children.add(child); }

    // ── Display helpers ─────────────────────────────────────────────────
    public String typeIcon() {
        return switch (type) {
            case ELEMENT   -> "⊞";
            case TEXT      -> "\"";
            case ATTRIBUTE -> "@";
            case COMMENT   -> "//";
            case CDATA     -> "[ ]";
        };
    }

    public String colorHex() {
        return switch (type) {
            case ELEMENT   -> "#a0c8ff";   // light blue
            case TEXT      -> "#e0a0d0";   // pink
            case ATTRIBUTE -> "#f0c060";   // gold
            case COMMENT   -> "#7a9070";   // muted green
            case CDATA     -> "#c0b0d0";   // lavender
        };
    }

    /** Label shown in the tree view. */
    public String treeLabel() {
        return switch (type) {
            case ELEMENT -> {
                String attrHint = attributes.isEmpty()
                        ? ""
                        : "  [" + attributes.size() + " attr" + (attributes.size() > 1 ? "s" : "") + "]";
                String childHint = children.isEmpty()
                        ? ""
                        : "  (" + children.size() + " child" + (children.size() > 1 ? "ren" : "") + ")";
                yield "<" + name + ">" + attrHint + childHint;
            }
            case TEXT      -> {
                String v = textValue != null ? textValue : "";
                String p = v.length() > 90 ? v.substring(0, 90) + "…" : v;
                yield "\" " + p;
            }
            case ATTRIBUTE -> "@" + name + " = \"" + textValue + "\"";
            case COMMENT   -> "<!-- " + (textValue != null && textValue.length() > 60
                    ? textValue.substring(0, 60) + "…" : textValue) + " -->";
            case CDATA     -> "<![CDATA[…]]>  (" + (textValue != null ? textValue.length() : 0) + " chars)";
        };
    }

    @Override public String toString() { return treeLabel(); }
}
