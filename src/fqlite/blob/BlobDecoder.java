package fqlite.blob;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import fqlite.base.Limits;
import fqlite.viewer.model.BPListNode;
import fqlite.viewer.parser.BPListParser;
import fqlite.viewer.parser.JsonNode;
import fqlite.viewer.parser.JsonParser;

/**
 * Turns the content of a cell into a tree of plain values (Map, List, String, Long, Double,
 * Boolean, byte[]): compression is removed first, then typedstream, binary/XML property lists
 * (NSKeyedArchiver archives resolved), JSON and protobuf are recognised. Embedded binary
 * values (e.g. a plist inside an NSData attribute) are decoded recursively. {@link #fields}
 * flattens the tree into path &rarr; value pairs for the "Decode column into fields" view.
 */
public final class BlobDecoder {

	private BlobDecoder() {
	}

	/** decoding result; {@code chain} names all layers, e.g. "zstd → bplist (NSKeyedArchiver)" */
	public record Result(String chain, String format, Object value, String error) {
		public boolean structured() {
			return value instanceof Map || value instanceof List;
		}
	}

	public static Result decode(byte[] raw) {
		if (raw == null)
			return new Result("", "NULL", null, null);
		List<String> chain = new ArrayList<>();
		String[] err = new String[1];
		Object v = decode(raw, chain, err, 0);
		String fmt = chain.isEmpty() ? "" : chain.get(chain.size() - 1);
		return new Result(String.join(" → ", chain), fmt, v, err[0]);
	}

	private static Object decode(byte[] d, List<String> chain, String[] err, int depth) {
		if (depth >= Limits.BLOB_DECODE_DEPTH + 2)
			return d;
		Codecs.Unwrapped u = Codecs.unwrap(d);
		for (Codecs.Codec c : u.layers())
			chain.add(c.label);
		if (u.error() != null && err[0] == null)
			err[0] = u.error();
		d = u.data();
		try {
			if (TypedStream.isTypedStream(d)) {
				Object v = TypedStream.decode(d);
				chain.add("typedstream");
				return v;
			}
			if (isBPlist(d)) {
				BPListNode root = new BPListParser().parse(d);
				Object v = fromBPlist(root, 0);
				Object k = keyedArchive(v);
				chain.add(k != NOT_KEYED ? "bplist (NSKeyedArchiver)" : "bplist");
				return k != NOT_KEYED ? k : v;
			}
			String text = utf8(d);
			if (text != null) {
				String t = text.strip();
				if (t.startsWith("<?xml") || t.startsWith("<plist") || t.startsWith("<!DOCTYPE plist")) {
					if (t.contains("<plist")) {
						Object v = xmlPlist(d);
						Object k = keyedArchive(v);
						chain.add(k != NOT_KEYED ? "XML plist (NSKeyedArchiver)" : "XML plist");
						return k != NOT_KEYED ? k : v;
					}
				}
				if ((t.startsWith("{") && t.endsWith("}")) || (t.startsWith("[") && t.endsWith("]"))) {
					try {
						Object v = fromJson(new JsonParser().parse(t), 0);
						chain.add("JSON");
						return v;
					} catch (Exception e) {
						// no JSON
					}
				}
				// Base64 wrapping a strongly recognisable format
				if (t.length() >= 16 && t.length() % 4 == 0 && t.matches("[A-Za-z0-9+/\\r\\n]+=*")) {
					try {
						byte[] b = Base64.getMimeDecoder().decode(t);
						if (isBPlist(b) || TypedStream.isTypedStream(b) || Codecs.detect(b) != null && Codecs.detect(b) != Codecs.Codec.ZLIB) {
							chain.add("Base64");
							return decode(b, chain, err, depth + 1);
						}
					} catch (IllegalArgumentException e) {
						// no Base64
					}
				}
				if (!u.compressed())
					return text;
				chain.add("text");
				return text;
			}
			Object pb = protobuf(d, 0, d.length, 0);
			if (pb != null) {
				chain.add("protobuf");
				return pb;
			}
		} catch (Exception | LinkageError e) {
			if (err[0] == null)
				err[0] = e.getMessage();
		}
		return d;
	}

	static boolean isBPlist(byte[] d) {
		return d.length >= 8 && d[0] == 'b' && d[1] == 'p' && d[2] == 'l' && d[3] == 'i' && d[4] == 's' && d[5] == 't';
	}

	/** the text if the bytes are printable UTF-8, otherwise null */
	static String utf8(byte[] d) {
		if (d.length == 0)
			return "";
		try {
			String s = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
					.onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(d)).toString();
			int ctrl = 0;
			for (int i = 0; i < s.length(); i++) {
				char c = s.charAt(i);
				if (c < 0x20 && c != '\n' && c != '\r' && c != '\t')
					ctrl++;
			}
			return ctrl == 0 || (ctrl <= 1 && s.endsWith("\0")) ? s.replace("\0", "") : null;
		} catch (CharacterCodingException e) {
			return null;
		}
	}

	// ------------------------------------------------------------------ property lists

	/** marker for "not an NSKeyedArchiver archive" */
	private static final Object NOT_KEYED = new Object();

	/** a UID of a binary plist (NSKeyedArchiver object reference) */
	public record Uid(long value) {
		@Override
		public String toString() {
			return "UID(" + value + ")";
		}
	}

	private static Object fromBPlist(BPListNode n, int depth) {
		if (n == null)
			return null;
		if (depth > 128)
			return "…";
		switch (n.getType()) {
		case DICT -> {
			Map<String, Object> m = new LinkedHashMap<>();
			for (Map.Entry<BPListNode, BPListNode> e : n.getDictEntries().entrySet())
				m.put(e.getKey().getScalarValue(), fromBPlist(e.getValue(), depth + 1));
			return m;
		}
		case ARRAY, SET -> {
			List<Object> l = new ArrayList<>();
			for (BPListNode c : n.getArrayElements())
				l.add(fromBPlist(c, depth + 1));
			return l;
		}
		case UID -> {
			String s = n.getScalarValue();
			return new Uid(Long.parseLong(s.substring(4, s.length() - 1)));
		}
		case DATA -> {
			byte[] r = n.getRawBytes(); // marker byte + data
			return java.util.Arrays.copyOfRange(r, Math.min(1, r.length), r.length);
		}
		case INT -> {
			try {
				return Long.parseLong(n.getScalarValue());
			} catch (NumberFormatException e) {
				return n.getScalarValue();
			}
		}
		case REAL -> {
			try {
				return Double.parseDouble(n.getScalarValue());
			} catch (NumberFormatException e) {
				return n.getScalarValue();
			}
		}
		case BOOL -> {
			return "true".equals(n.getScalarValue());
		}
		case NULL -> {
			return null;
		}
		default -> {
			return n.getScalarValue();
		}
		}
	}

	private static Object xmlPlist(byte[] d) throws Exception {
		DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
		f.setNamespaceAware(false);
		f.setExpandEntityReferences(false);
		f.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		f.setFeature("http://xml.org/sax/features/external-general-entities", false);
		f.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
		DocumentBuilder b = f.newDocumentBuilder();
		b.setEntityResolver((p, s) -> new org.xml.sax.InputSource(new java.io.StringReader("")));
		Element root = b.parse(new ByteArrayInputStream(d)).getDocumentElement();
		for (Node c = root.getFirstChild(); c != null; c = c.getNextSibling())
			if (c instanceof Element e)
				return fromXml(e, 0);
		return null;
	}

	private static Object fromXml(Element e, int depth) {
		if (depth > 128)
			return "…";
		String t = e.getTagName(), txt = e.getTextContent().strip();
		switch (t) {
		case "dict": {
			Map<String, Object> m = new LinkedHashMap<>();
			String key = null;
			for (Node c = e.getFirstChild(); c != null; c = c.getNextSibling()) {
				if (!(c instanceof Element ce))
					continue;
				if (ce.getTagName().equals("key"))
					key = ce.getTextContent();
				else {
					m.put(key == null ? "?" : key, fromXml(ce, depth + 1));
					key = null;
				}
			}
			if (m.size() == 1 && m.get("CF$UID") instanceof Long uid)
				return new Uid(uid);
			return m;
		}
		case "array": {
			List<Object> l = new ArrayList<>();
			for (Node c = e.getFirstChild(); c != null; c = c.getNextSibling())
				if (c instanceof Element ce)
					l.add(fromXml(ce, depth + 1));
			return l;
		}
		case "integer":
			try {
				return Long.parseLong(txt);
			} catch (NumberFormatException x) {
				return txt;
			}
		case "real":
			try {
				return Double.parseDouble(txt);
			} catch (NumberFormatException x) {
				return txt;
			}
		case "true":
			return true;
		case "false":
			return false;
		case "data":
			try {
				return Base64.getMimeDecoder().decode(txt.replaceAll("\\s", ""));
			} catch (IllegalArgumentException x) {
				return txt;
			}
		default:
			return e.getTextContent();
		}
	}

	/** resolves an NSKeyedArchiver archive; returns {@link #NOT_KEYED} if it is none */
	@SuppressWarnings("unchecked")
	private static Object keyedArchive(Object v) {
		if (!(v instanceof Map<?, ?> m) || !"NSKeyedArchiver".equals(m.get("$archiver")) || !(m.get("$objects") instanceof List)
				|| !(m.get("$top") instanceof Map))
			return NOT_KEYED;
		List<Object> objs = (List<Object>) m.get("$objects");
		Map<String, Object> top = (Map<String, Object>) m.get("$top");
		Map<String, Object> r = new LinkedHashMap<>();
		for (Map.Entry<String, Object> e : top.entrySet())
			r.put(e.getKey(), resolve(e.getValue(), objs, new ArrayList<>(), 0));
		if (r.size() == 1 && r.containsKey("root"))
			return r.get("root");
		return r;
	}

	@SuppressWarnings("unchecked")
	private static Object resolve(Object v, List<Object> objs, List<Long> path, int depth) {
		if (depth > 64)
			return "…";
		if (v instanceof Uid u) {
			if (u.value < 0 || u.value >= objs.size())
				return "<invalid " + u + ">";
			if (path.contains(u.value))
				return "<cyclic reference " + u + ">";
			path.add(u.value);
			try {
				Object o = objs.get((int) u.value);
				if ("$null".equals(o))
					return null;
				return resolve(o, objs, path, depth + 1);
			} finally {
				path.remove(path.size() - 1);
			}
		}
		if (v instanceof List<?> l) {
			List<Object> r = new ArrayList<>();
			for (Object o : l)
				r.add(resolve(o, objs, path, depth + 1));
			return r;
		}
		if (!(v instanceof Map<?, ?> mm))
			return v;
		Map<String, Object> m = (Map<String, Object>) mm;
		String cls = null;
		if (m.get("$class") instanceof Uid cu && cu.value >= 0 && cu.value < objs.size() && objs.get((int) cu.value) instanceof Map<?, ?> cm
				&& cm.get("$classname") instanceof String s)
			cls = s;
		if (cls != null) {
			switch (cls) {
			case "NSDictionary", "NSMutableDictionary": {
				if (m.get("NS.keys") instanceof List<?> ks && m.get("NS.objects") instanceof List<?> vs) {
					Map<String, Object> r = new LinkedHashMap<>();
					for (int i = 0; i < Math.min(ks.size(), vs.size()); i++)
						r.put(String.valueOf(resolve(ks.get(i), objs, path, depth + 1)), resolve(vs.get(i), objs, path, depth + 1));
					return r;
				}
				break;
			}
			case "NSArray", "NSMutableArray", "NSSet", "NSMutableSet", "NSOrderedSet", "NSMutableOrderedSet":
				if (m.get("NS.objects") instanceof List<?> l)
					return resolve(l, objs, path, depth + 1);
				break;
			case "NSString", "NSMutableString":
				if (m.containsKey("NS.string"))
					return resolve(m.get("NS.string"), objs, path, depth + 1);
				if (m.get("NS.bytes") instanceof byte[] b)
					return new String(b, StandardCharsets.UTF_8);
				break;
			case "NSData", "NSMutableData":
				if (m.containsKey("NS.data"))
					return resolve(m.get("NS.data"), objs, path, depth + 1);
				if (m.containsKey("NS.bytes"))
					return resolve(m.get("NS.bytes"), objs, path, depth + 1);
				break;
			case "NSDate":
				if (m.get("NS.time") instanceof Number t)
					return Instant.ofEpochMilli((long) ((t.doubleValue() + 978_307_200L) * 1000)).toString();
				break;
			case "NSUUID":
				if (m.get("NS.uuidbytes") instanceof byte[] b && b.length == 16) {
					ByteBuffer bb = ByteBuffer.wrap(b);
					return new UUID(bb.getLong(), bb.getLong()).toString();
				}
				break;
			case "NSURL":
				if (m.containsKey("NS.relative")) {
					Object base = resolve(m.get("NS.base"), objs, path, depth + 1), rel = resolve(m.get("NS.relative"), objs, path, depth + 1);
					return base == null ? rel : base + "" + rel;
				}
				break;
			case "NSAttributedString", "NSMutableAttributedString": {
				Map<String, Object> r = new LinkedHashMap<>();
				r.put("text", resolve(m.get("NSString"), objs, path, depth + 1));
				if (m.containsKey("NSAttributes"))
					r.put("attributes", resolve(m.get("NSAttributes"), objs, path, depth + 1));
				return r;
			}
			default:
				break;
			}
		}
		Map<String, Object> r = new LinkedHashMap<>();
		if (cls != null)
			r.put("$class", cls);
		for (Map.Entry<String, Object> e : m.entrySet())
			if (!e.getKey().equals("$class"))
				r.put(e.getKey(), resolve(e.getValue(), objs, path, depth + 1));
		return r;
	}

	// ------------------------------------------------------------------ JSON

	private static Object fromJson(JsonNode n, int depth) {
		if (n == null || depth > 128)
			return null;
		switch (n.getType()) {
		case OBJECT -> {
			Map<String, Object> m = new LinkedHashMap<>();
			for (Map.Entry<String, JsonNode> e : n.getObjectEntries().entrySet())
				m.put(e.getKey(), fromJson(e.getValue(), depth + 1));
			return m;
		}
		case ARRAY -> {
			List<Object> l = new ArrayList<>();
			for (JsonNode c : n.getArrayElements())
				l.add(fromJson(c, depth + 1));
			return l;
		}
		case NULL -> {
			return null;
		}
		case BOOLEAN -> {
			return "true".equals(n.getRawValue());
		}
		case NUMBER -> {
			String s = n.getRawValue();
			try {
				return s.contains(".") || s.contains("e") || s.contains("E") ? (Object) Double.parseDouble(s) : (Object) Long.parseLong(s);
			} catch (NumberFormatException e) {
				return s;
			}
		}
		default -> {
			return n.getRawValue();
		}
		}
	}

	// ------------------------------------------------------------------ protobuf

	/**
	 * Strict protobuf walk: the bytes must consist of complete fields with valid wire types
	 * (no groups) and end exactly at the end; otherwise null. Field numbers become keys,
	 * repeated fields lists; length-delimited fields are shown as nested message, text or
	 * bytes.
	 */
	static Object protobuf(byte[] d, int p, int end, int depth) {
		if (end - p < 2 || depth > 32)
			return null;
		Map<String, Object> m = new LinkedHashMap<>();
		int fields = 0;
		while (p < end) {
			long[] tag = varint(d, p, end);
			if (tag == null)
				return null;
			p = (int) tag[1];
			int wt = (int) (tag[0] & 7);
			long fn = tag[0] >>> 3;
			if (fn < 1 || fn > (1 << 29) - 1)
				return null;
			Object val;
			switch (wt) {
			case 0 -> {
				long[] v = varint(d, p, end);
				if (v == null)
					return null;
				p = (int) v[1];
				val = v[0];
			}
			case 1 -> {
				if (p + 8 > end)
					return null;
				long l = ByteBuffer.wrap(d, p, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
				double dv = Double.longBitsToDouble(l);
				val = plausibleDouble(dv) ? (Object) dv : (Object) l;
				p += 8;
			}
			case 2 -> {
				long[] v = varint(d, p, end);
				if (v == null || v[0] < 0 || v[1] + v[0] > end)
					return null;
				int s = (int) v[1], e = (int) (v[1] + v[0]);
				byte[] sub = java.util.Arrays.copyOfRange(d, s, e);
				String txt = sub.length > 0 ? utf8(sub) : "";
				Object nested = txt != null && txt.length() > 0 && printableRatio(txt) > 0.9 ? null : protobuf(d, s, e, depth + 1);
				val = nested != null ? nested : txt != null ? txt : sub;
				p = e;
			}
			case 5 -> {
				if (p + 4 > end)
					return null;
				int iv = ByteBuffer.wrap(d, p, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
				float fv = Float.intBitsToFloat(iv);
				val = plausibleDouble(fv) ? (Object) (double) fv : (Object) (long) iv;
				p += 4;
			}
			default -> {
				return null;
			}
			}
			fields++;
			String k = String.valueOf(fn);
			Object prev = m.get(k);
			if (prev == null && !m.containsKey(k))
				m.put(k, val);
			else if (prev instanceof Repeated r)
				r.add(val);
			else {
				Repeated r = new Repeated();
				r.add(prev);
				r.add(val);
				m.put(k, r);
			}
		}
		if (fields == 0 || p != end)
			return null;
		for (Map.Entry<String, Object> e : m.entrySet())
			if (e.getValue() instanceof Repeated r)
				e.setValue(new ArrayList<>(r));
		return m;
	}

	private static final class Repeated extends ArrayList<Object> {
		private static final long serialVersionUID = 1L;
	}

	private static boolean plausibleDouble(double v) {
		double a = Math.abs(v);
		return v == 0 || (!Double.isNaN(v) && a > 1e-6 && a < 1e15);
	}

	private static double printableRatio(String s) {
		int ok = 0;
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (c >= 0x20 || c == '\n' || c == '\r' || c == '\t')
				ok++;
		}
		return s.isEmpty() ? 0 : (double) ok / s.length();
	}

	private static long[] varint(byte[] d, int p, int end) {
		long v = 0;
		for (int i = 0; i < 10; i++) {
			if (p >= end)
				return null;
			int b = d[p++] & 0xFF;
			v |= (long) (b & 0x7F) << (7 * i);
			if ((b & 0x80) == 0)
				return new long[] { v, p };
		}
		return null;
	}

	// ------------------------------------------------------------------ flattening

	/** path → display value of every leaf (at most {@link Limits#FIELD_MAX_PATHS}) */
	public static LinkedHashMap<String, String> fields(Result r) {
		LinkedHashMap<String, String> out = new LinkedHashMap<>();
		if (r.value() instanceof Map || r.value() instanceof List)
			flatten("", r.value(), out, 0);
		else if (r.value() != null && !(r.value() instanceof byte[]))
			out.put("value", display(r.value()));
		return out;
	}

	private static void flatten(String path, Object v, Map<String, String> out, int depth) {
		if (out.size() >= Limits.FIELD_MAX_PATHS)
			return;
		if (v instanceof byte[] b && depth < 8 && b.length >= 8) {
			Result inner = decode(b);
			if (inner.structured() && !inner.chain().isEmpty()) {
				flatten(path, inner.value(), out, depth + 1);
				return;
			}
		}
		if (v instanceof Map<?, ?> m) {
			if (m.isEmpty()) {
				out.put(path.isEmpty() ? "value" : path, "{}");
				return;
			}
			for (Map.Entry<?, ?> e : m.entrySet())
				flatten(path.isEmpty() ? String.valueOf(e.getKey()) : path + "." + e.getKey(), e.getValue(), out, depth + 1);
		} else if (v instanceof List<?> l) {
			if (l.isEmpty()) {
				out.put(path.isEmpty() ? "value" : path, "[]");
				return;
			}
			for (int i = 0; i < l.size() && out.size() < Limits.FIELD_MAX_PATHS; i++)
				flatten(path + "[" + i + "]", l.get(i), out, depth + 1);
		} else
			out.put(path.isEmpty() ? "value" : path, display(v));
	}

	/** display form of a leaf value */
	public static String display(Object v) {
		if (v == null)
			return "NULL";
		if (v instanceof byte[] b) {
			String t = utf8(b);
			if (t != null && !t.isEmpty())
				return t;
			int n = Math.min(b.length, 64);
			return "x'" + HexFormat.of().formatHex(b, 0, n) + (b.length > n ? "…" : "") + "' (" + b.length + " bytes)";
		}
		if (v instanceof Double dv && dv == Math.rint(dv) && Math.abs(dv) < 1e15)
			return String.valueOf(dv.longValue());
		return String.valueOf(v);
	}

	/** a one-line summary, e.g. for a tooltip or a decoded-text column */
	public static String summary(Result r) {
		Object v = r.value();
		if (v instanceof Map<?, ?> m && m.get("text") instanceof String s)
			return s;
		if (v instanceof String s)
			return s;
		if (v instanceof byte[])
			return null;
		StringBuilder sb = new StringBuilder();
		compact(v, sb, 0);
		return sb.toString();
	}

	/** compact one-line rendering, cut after about 300 characters */
	private static void compact(Object v, StringBuilder sb, int depth) {
		if (sb.length() > 300) {
			if (!sb.toString().endsWith("\u2026"))
				sb.append('\u2026');
			return;
		}
		if (v instanceof Map<?, ?> m) {
			sb.append('{');
			boolean first = true;
			for (Map.Entry<?, ?> e : m.entrySet()) {
				if (!first)
					sb.append(", ");
				first = false;
				sb.append(e.getKey()).append(": ");
				compact(e.getValue(), sb, depth + 1);
				if (sb.length() > 300)
					break;
			}
			sb.append('}');
		} else if (v instanceof List<?> l) {
			sb.append('[');
			for (int i = 0; i < l.size(); i++) {
				if (i > 0)
					sb.append(", ");
				compact(l.get(i), sb, depth + 1);
				if (sb.length() > 300)
					break;
			}
			sb.append(']');
		} else if (v instanceof byte[] b && depth < 6) {
			Result inner = decode(b);
			if (inner.structured() && !inner.chain().isEmpty())
				compact(inner.value(), sb, depth + 1);
			else
				sb.append(display(v));
		} else
			sb.append(display(v));
	}
}
