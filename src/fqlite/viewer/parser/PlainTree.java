package fqlite.viewer.parser;

import java.util.List;
import java.util.Map;

import fqlite.blob.BlobDecoder;

/** converts a decoded value tree (Map / List / scalars, see {@link BlobDecoder}) into BinaryNodes for the tree view */
public final class PlainTree {

	private PlainTree() {
	}

	public static BinaryNode toNode(Object v, String name, BinaryNode.Format fmt) {
		return node(v, name, fmt, 0);
	}

	private static BinaryNode node(Object v, String name, BinaryNode.Format fmt, int depth) {
		if (v instanceof Map<?, ?> m) {
			Object cls = m.get("$class");
			BinaryNode n = new BinaryNode(fmt, BinaryNode.Kind.DOCUMENT, name, cls != null ? String.valueOf(cls) : "Dict");
			if (depth < 64)
				for (Map.Entry<?, ?> e : m.entrySet())
					if (!"$class".equals(e.getKey()))
						n.addChild(node(e.getValue(), String.valueOf(e.getKey()), fmt, depth + 1));
			return n;
		}
		if (v instanceof List<?> l) {
			BinaryNode n = new BinaryNode(fmt, BinaryNode.Kind.ARRAY, name, "Array");
			if (depth < 64)
				for (int i = 0; i < l.size(); i++)
					n.addChild(node(l.get(i), "[" + i + "]", fmt, depth + 1));
			return n;
		}
		if (v instanceof byte[] b) {
			if (depth < 16 && b.length >= 8) {
				BlobDecoder.Result r = BlobDecoder.decode(b);
				if (r.structured() && !r.chain().isEmpty()) {
					BinaryNode inner = node(r.value(), name, fmt, depth + 1);
					BinaryNode n = new BinaryNode(fmt, BinaryNode.Kind.DOCUMENT, name, "Data → " + r.chain());
					for (BinaryNode c : inner.getChildren())
						n.addChild(c);
					n.setRawBytes(b);
					return n;
				}
			}
			BinaryNode n = new BinaryNode(fmt, BinaryNode.Kind.BINARY, name, "Data");
			n.setValue(BlobDecoder.display(b));
			n.setRawBytes(b);
			return n;
		}
		String type = v == null ? "Null" : v instanceof String ? "String" : v instanceof Long ? "Int" : v instanceof Double ? "Double"
				: v instanceof Boolean ? "Bool" : v.getClass().getSimpleName();
		BinaryNode n = new BinaryNode(fmt, BinaryNode.Kind.SCALAR, name, type);
		n.setValue(v == null ? "<null>" : v instanceof String s ? "\"" + s + "\"" : BlobDecoder.display(v));
		return n;
	}
}
