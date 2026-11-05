package fqlite.blob;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reader for Apple's typedstream format (NSArchiver, "streamtyped"), used e.g. by iMessage for
 * {@code message.attributedBody}. The stream is read completely generically (it is
 * self-describing); afterwards the common Foundation classes (NSString, NSAttributedString,
 * NSDictionary, NSArray, NSNumber, NSData, NSDate) are turned into plain values. Format
 * details follow the python-typedstream reference by dgelessus.
 */
public final class TypedStream {

	private TypedStream() {
	}

	private static final int TAG_INT2 = -127, TAG_INT4 = -126, TAG_FLOAT = -125, TAG_NEW = -124, TAG_NIL = -123, TAG_END = -122;
	private static final int FIRST_TAG = -128, LAST_TAG = -111, FIRST_REF = LAST_TAG + 1;

	/** a class of the stream with its superclass chain */
	public record TsClass(String name, int version, TsClass superclass) {
		public boolean is(String n) {
			for (TsClass c = this; c != null; c = c.superclass)
				if (c.name.equals(n))
					return true;
			return false;
		}
	}

	/** an archived object: its class and the values it wrote, in stream order */
	public static final class TsObject {
		public TsClass cls;
		public final List<Object> values = new ArrayList<>();

		public String className() {
			return cls == null ? "?" : cls.name;
		}
	}

	public static boolean isTypedStream(byte[] d) {
		if (d == null || d.length < 16 || (d[0] != 3 && d[0] != 4) || d[1] != 11)
			return false;
		String sig = new String(d, 2, 11, StandardCharsets.US_ASCII);
		return sig.equals("streamtyped") || sig.equals("typedstream");
	}

	// ------------------------------------------------------------------ reader

	private final static class Reader {
		final byte[] d;
		int p;
		boolean little;
		final List<byte[]> strings = new ArrayList<>();
		final List<Object> objects = new ArrayList<>();

		Reader(byte[] d) {
			this.d = d;
		}

		int raw() {
			if (p >= d.length)
				throw new DecodeException("typedstream: unexpected end of data at offset " + p);
			return d[p++];
		}

		byte[] bytes(int n) {
			if (n < 0 || p + n > d.length)
				throw new DecodeException("typedstream: string/array beyond the end of the data");
			byte[] b = java.util.Arrays.copyOfRange(d, p, p + n);
			p += n;
			return b;
		}

		long fixed(int n) {
			byte[] b = bytes(n);
			long v = 0;
			for (int i = 0; i < n; i++) {
				int k = little ? i : n - 1 - i;
				v |= (b[k] & 0xFFL) << (8 * i);
			}
			return v;
		}

		long integer(int head, boolean signed) {
			if (head < FIRST_TAG || head > LAST_TAG)
				return signed ? head : head & 0xFF;
			if (head == TAG_INT2) {
				long v = fixed(2);
				return signed ? (short) v : v;
			}
			if (head == TAG_INT4) {
				long v = fixed(4);
				return signed ? (int) v : v;
			}
			throw new DecodeException("typedstream: unexpected tag 0x" + Integer.toHexString(head & 0xFF) + " at offset " + (p - 1));
		}

		int ref(int head) {
			long n = integer(head, true) - FIRST_REF;
			if (n < 0)
				throw new DecodeException("typedstream: invalid reference");
			return (int) n;
		}

		byte[] unshared(int head) {
			if (head == TAG_NIL)
				return null;
			long len = integer(head, false);
			return bytes((int) len);
		}

		byte[] shared(int head) {
			if (head == TAG_NIL)
				return null;
			if (head == TAG_NEW) {
				byte[] s = unshared(raw());
				if (s == null)
					throw new DecodeException("typedstream: nil shared string");
				strings.add(s);
				return s;
			}
			int r = ref(head);
			if (r >= strings.size())
				throw new DecodeException("typedstream: reference to unknown string " + r);
			return strings.get(r);
		}

		Object lookup(int head) {
			int r = ref(head);
			if (r >= objects.size())
				throw new DecodeException("typedstream: reference to unknown object " + r);
			return objects.get(r);
		}

		TsClass cls(int head) {
			List<String> names = new ArrayList<>();
			List<Integer> versions = new ArrayList<>();
			while (head == TAG_NEW) {
				byte[] n = shared(raw());
				if (n == null)
					throw new DecodeException("typedstream: nil class name");
				names.add(new String(n, StandardCharsets.UTF_8));
				versions.add((int) integer(raw(), true));
				head = raw();
			}
			TsClass sup = null;
			if (head != TAG_NIL) {
				Object o = lookup(head);
				if (!(o instanceof TsClass c))
					throw new DecodeException("typedstream: class reference to a non-class");
				sup = c;
			}
			TsClass[] chain = new TsClass[names.size()];
			for (int i = names.size() - 1; i >= 0; i--) {
				sup = new TsClass(names.get(i), versions.get(i), sup);
				chain[i] = sup;
			}
			for (TsClass c : chain)
				objects.add(c);
			return sup;
		}

		Object object(int head, int depth) {
			if (head == TAG_NIL)
				return null;
			if (head == TAG_NEW) {
				if (depth > 200)
					throw new DecodeException("typedstream: nesting too deep");
				TsObject o = new TsObject();
				objects.add(o);
				o.cls = cls(raw());
				int h = raw();
				while (h != TAG_END) {
					typedValues(h, o.values, depth + 1);
					h = raw();
				}
				return o;
			}
			return lookup(head);
		}

		Object cString(int head) {
			if (head == TAG_NIL)
				return null;
			if (head == TAG_NEW) {
				byte[] s = shared(raw());
				String v = s == null ? null : new String(s, StandardCharsets.UTF_8);
				objects.add(v);
				return v;
			}
			return lookup(head);
		}

		void typedValues(int head, List<Object> out, int depth) {
			byte[] enc = shared(head);
			if (enc == null || enc.length == 0)
				throw new DecodeException("typedstream: missing type encoding");
			String e = new String(enc, StandardCharsets.US_ASCII);
			for (String t : split(e))
				out.add(value(t, depth));
		}

		Object value(String t, int depth) {
			char c = t.charAt(0);
			switch (c) {
			case 'B':
				return raw() != 0;
			case 'C':
				return (long) (raw() & 0xFF);
			case 'c':
				return (long) raw();
			case 'S', 'I', 'L', 'Q':
				return integer(raw(), false);
			case 's', 'i', 'l', 'q':
				return integer(raw(), true);
			case 'f', 'd': {
				int h = raw();
				if (h == TAG_FLOAT)
					return c == 'f' ? (double) Float.intBitsToFloat((int) fixed(4)) : Double.longBitsToDouble(fixed(8));
				return (double) integer(h, true);
			}
			case '*':
				return cString(raw());
			case '%', ':': {
				byte[] s = shared(raw());
				return s == null ? null : new String(s, StandardCharsets.UTF_8);
			}
			case '+':
				return unshared(raw());
			case '#':
				return cls(raw());
			case '@':
				return object(raw(), depth);
			case '!':
				return null;
			case '[': {
				int i = 1;
				while (i < t.length() && Character.isDigit(t.charAt(i)))
					i++;
				int n = Integer.parseInt(t.substring(1, i));
				String et = t.substring(i, t.length() - 1);
				if (et.equals("c") || et.equals("C"))
					return bytes(n);
				List<Object> l = new ArrayList<>();
				for (int k = 0; k < n; k++)
					l.add(value(et, depth));
				return l;
			}
			case '{': {
				String body = t.substring(1, t.length() - 1);
				int eq = body.indexOf('=');
				int br = body.indexOf('{');
				if (eq >= 0 && (br < 0 || eq < br))
					body = body.substring(eq + 1);
				List<Object> l = new ArrayList<>();
				for (String f : split(body))
					l.add(value(f, depth));
				return l;
			}
			default:
				throw new DecodeException("typedstream: unknown type encoding '" + t + "'");
			}
		}
	}

	static List<String> split(String e) {
		List<String> r = new ArrayList<>();
		int i = 0;
		while (i < e.length()) {
			int start = i, depth = 0;
			do {
				char c = e.charAt(i);
				if (c == '[' || c == '{' || c == '(')
					depth++;
				else if (c == ']' || c == '}' || c == ')')
					depth--;
				i++;
			} while (depth > 0 && i < e.length());
			if (depth != 0)
				throw new DecodeException("typedstream: malformed type encoding " + e);
			r.add(e.substring(start, i));
		}
		return r;
	}

	/** reads all top-level values */
	public static List<Object> parse(byte[] d) {
		if (!isTypedStream(d))
			throw new DecodeException("not a typedstream");
		if (d[0] == 3)
			throw new DecodeException("typedstream: old NeXTSTEP version not supported");
		Reader r = new Reader(d);
		r.little = d[2] == 's';
		r.p = 13;
		r.integer(r.raw(), false); // system version
		List<Object> top = new ArrayList<>();
		while (r.p < d.length)
			r.typedValues(r.raw(), top, 0);
		return top;
	}

	// ------------------------------------------------------------------ plain values

	/** the decoded content: the first top-level value as plain value (see {@link #plain}) */
	public static Object decode(byte[] d) {
		List<Object> top = parse(d);
		if (top.size() == 1)
			return plain(top.get(0));
		List<Object> l = new ArrayList<>();
		for (Object o : top)
			l.add(plain(o));
		return l;
	}

	/** the text of an archived NSAttributedString / NSString, or null */
	public static String text(byte[] d) {
		try {
			Object v = decode(d);
			if (v instanceof String s)
				return s;
			if (v instanceof Map<?, ?> m && m.get("text") instanceof String s)
				return s;
		} catch (RuntimeException e) {
			// not decodable
		}
		return null;
	}

	public static Object plain(Object v) {
		return plain(v, new IdentityHashMap<>(), 0);
	}

	private static final long APPLE_EPOCH = 978_307_200L;

	private static Object plain(Object v, IdentityHashMap<Object, Boolean> busy, int depth) {
		if (v == null || v instanceof String || v instanceof Number || v instanceof Boolean || v instanceof byte[])
			return v;
		if (depth > 64)
			return "…";
		if (v instanceof TsClass c)
			return "class " + c.name;
		if (v instanceof List<?> l) {
			List<Object> r = new ArrayList<>();
			for (Object o : l)
				r.add(plain(o, busy, depth + 1));
			return r;
		}
		if (!(v instanceof TsObject o))
			return String.valueOf(v);
		if (busy.containsKey(o))
			return "<cyclic reference to " + o.className() + ">";
		busy.put(o, true);
		try {
			TsClass c = o.cls;
			List<Object> vals = o.values;
			if (c != null && (c.is("NSAttributedString"))) {
				Map<String, Object> m = new LinkedHashMap<>();
				Object s = vals.isEmpty() ? null : plain(vals.get(0), busy, depth + 1);
				m.put("text", s);
				List<Object> runs = new ArrayList<>();
				long len = -1;
				int pos = 0;
				for (int i = 1; i < vals.size(); i++) {
					Object x = vals.get(i);
					if (x instanceof Long n)
						len = n;
					else {
						Map<String, Object> run = new LinkedHashMap<>();
						run.put("location", (long) pos);
						run.put("length", len);
						run.put("attributes", plain(x, busy, depth + 1));
						runs.add(run);
						if (len > 0)
							pos += (int) len;
					}
				}
				if (!runs.isEmpty())
					m.put("runs", runs);
				return m;
			}
			if (c != null && c.is("NSString")) {
				for (Object x : vals)
					if (x instanceof byte[] b)
						return new String(b, StandardCharsets.UTF_8);
					else if (x instanceof String s)
						return s;
				return "";
			}
			if (c != null && c.is("NSDictionary") && !vals.isEmpty() && vals.get(0) instanceof Long n && vals.size() >= 1 + 2 * n) {
				Map<String, Object> m = new LinkedHashMap<>();
				for (int i = 0; i < n; i++) {
					Object k = plain(vals.get(1 + 2 * i), busy, depth + 1);
					m.put(String.valueOf(k), plain(vals.get(2 + 2 * i), busy, depth + 1));
				}
				return m;
			}
			if (c != null && (c.is("NSArray") || c.is("NSSet") || c.is("NSOrderedSet")) && !vals.isEmpty() && vals.get(0) instanceof Long) {
				List<Object> l = new ArrayList<>();
				for (int i = 1; i < vals.size(); i++)
					l.add(plain(vals.get(i), busy, depth + 1));
				return l;
			}
			if (c != null && c.is("NSNumber") && vals.size() >= 2)
				return vals.get(vals.size() - 1);
			if (c != null && c.is("NSData")) {
				for (Object x : vals)
					if (x instanceof byte[] b)
						return b;
			}
			if (c != null && c.is("NSDate") && !vals.isEmpty() && vals.get(0) instanceof Double t) {
				long ms = (long) ((t + APPLE_EPOCH) * 1000);
				return Instant.ofEpochMilli(ms).toString();
			}
			Map<String, Object> m = new LinkedHashMap<>();
			m.put("$class", o.className());
			if (vals.size() == 1)
				m.put("value", plain(vals.get(0), busy, depth + 1));
			else
				for (int i = 0; i < vals.size(); i++)
					m.put("v" + i, plain(vals.get(i), busy, depth + 1));
			return m;
		} finally {
			busy.remove(o);
		}
	}
}
