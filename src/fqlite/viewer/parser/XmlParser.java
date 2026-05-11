package fqlite.viewer.parser;

import fqlite.viewer.parser.XmlNode;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Parses XML bytes into a tree of {@link XmlNode}s using Java's built-in DOM parser.
 *
 * Handles:
 *   – Elements with attributes and child elements
 *   – Text nodes (trimmed, whitespace-only nodes are dropped)
 *   – Comment nodes
 *   – CDATA sections
 *
 * Intentionally does NOT validate against a DTD / schema (secure, offline mode).
 */
public class XmlParser {

    // ── Public API ──────────────────────────────────────────────────────
    /**
     * Parse raw bytes as an XML document.
     *
     * @param data  UTF-8 (or declared charset) XML bytes
     * @return      root XmlNode representing the document element
     * @throws XmlParseException on any parsing error
     */
    public XmlNode parse(byte[] data) throws XmlParseException {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            // Security: disable external entity loading (XXE prevention)
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl",        false);
            factory.setFeature("http://xml.org/sax/features/external-general-entities",       false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities",     false);
            factory.setAttribute("http://javax.xml.XMLConstants/property/accessExternalDTD",  "");
            factory.setAttribute("http://javax.xml.XMLConstants/property/accessExternalSchema","");
            factory.setExpandEntityReferences(false);
            factory.setNamespaceAware(false);
            factory.setCoalescing(false);   // keep CDATA as CDATA_SECTION_NODE

            DocumentBuilder builder = factory.newDocumentBuilder();
            // Suppress SAX error output to stderr
            builder.setErrorHandler(null);

            Document doc = builder.parse(new ByteArrayInputStream(data));
            doc.normalizeDocument();

            Element root = doc.getDocumentElement();
            return convertElement(root);

        } catch (ParserConfigurationException e) {
            throw new XmlParseException("XML parser configuration error: " + e.getMessage(), e);
        } catch (SAXException e) {
            throw new XmlParseException("XML syntax error: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new XmlParseException("I/O error while reading XML: " + e.getMessage(), e);
        }
    }

    // ── Private conversion ──────────────────────────────────────────────

    private XmlNode convertElement(Element el) {
        XmlNode node = new XmlNode(XmlNode.Type.ELEMENT, el.getTagName());

        // Attributes (sorted alphabetically for stable display)
        NamedNodeMap attrs = el.getAttributes();
        if (attrs != null) {
            for (int i = 0; i < attrs.getLength(); i++) {
                Attr a = (Attr) attrs.item(i);
                node.addAttribute(a.getName(), a.getValue());
            }
        }

        // Children
        NodeList children = el.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            switch (child.getNodeType()) {

                case Node.ELEMENT_NODE ->
                    node.addChild(convertElement((Element) child));

                case Node.TEXT_NODE -> {
                    String text = child.getNodeValue();
                    if (text != null && !text.isBlank()) {
                        node.addChild(new XmlNode(XmlNode.Type.TEXT, "#text", text.strip()));
                    }
                }

                case Node.CDATA_SECTION_NODE -> {
                    String cdata = child.getNodeValue();
                    node.addChild(new XmlNode(XmlNode.Type.CDATA, "#cdata", cdata));
                }

                case Node.COMMENT_NODE -> {
                    String comment = child.getNodeValue();
                    if (comment != null && !comment.isBlank()) {
                        node.addChild(new XmlNode(XmlNode.Type.COMMENT, "#comment", comment.strip()));
                    }
                }

                default -> { /* processing instructions, etc. – skip */ }
            }
        }

        return node;
    }
}
