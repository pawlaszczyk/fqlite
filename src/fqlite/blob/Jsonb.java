package fqlite.blob;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Decoder for SQLite's binary JSON format JSONB (SQLite 3.45.0 and later, written by
 * {@code jsonb()}, {@code jsonb_set()}, {@code jsonb_insert()}, ...).
 * <p>
 * A JSONB value is a tree of elements. Each element starts with a header: the low nibble of
 * the first byte is the element type, the high nibble the payload size (0..11 directly; 12,
 * 13, 14, 15 = the size follows as a 1, 2, 4 or 8 byte big-endian integer). Scalars carry
 * their value as text in the payload; arrays and objects carry their child elements (objects
 * alternate key and value). See https://sqlite.org/jsonb.html.
 * <p>
 * JSONB has no magic number. {@link #isJsonb(byte[])} therefore accepts a BLOB only if it
 * parses completely and strictly (root element covers exactly the whole BLOB, every nested
 * element is valid, texts are valid UTF-8, numbers are well-formed); only arrays and objects
 * of at least two bytes are recognised automatically, because a scalar or a one-byte value
 * would match far too many unrelated BLOBs. {@link #toJson(byte[])} decodes any valid JSONB value, scalars included.
 * The output is the canonical JSON text that SQLite's {@code json()} returns
 * (JSON5 elements converted, e.g. {@code 0x1F} &rarr; {@code 31}, {@code .5} &rarr; {@code 0.5}).
 */
public final class Jsonb {

	private Jsonb() {
	}

	public static final int NULL = 0, TRUE = 1, FALSE = 2, INT = 3, INT5 = 4, FLOAT = 5, FLOAT5 = 6, TEXT = 7,
			TEXTJ = 8, TEXT5 = 9, TEXTRAW = 10, ARRAY = 11, OBJECT = 12;

	/** nesting limit (SQLite: JSON_MAX_DEPTH = 1000) */
	public static final int MAX_DEPTH = 1000;

	private static final String[] TYPE_NAMES = { "NULL", "TRUE", "FALSE", "INT", "INT5", "FLOAT", "FLOAT5", "TEXT",
			"TEXTJ", "TEXT5", "TEXTRAW", "ARRAY", "OBJECT" };

	public static String typeName(int type) {
		return type >= 0 && type < TYPE_NAMES.length ? TYPE_NAMES[type] : "RESERVED-" + type;
	}

	/**
	 * True if the bytes are a complete, valid JSONB array or object. Cheap for non-JSONB data:
	 * the size in the root header must match the BLOB length exactly, which random data
	 * rarely does.
	 */
	public static boolean isJsonb(byte[] b) {
		if (b == null || b.length < 2) // a single byte 0x0B/0x0C ("[]"/"{}") is too ambiguous
			return false;
		int type = b[0] & 0x0F;
		if (type != ARRAY && type != OBJECT)
			return false;
		long[] h = header(b, 0, b.length);
		if (h == null || h[0] + h[1] != b.length)
			return false;
		try {
			new Parser(b, null).element(0, b.length, 0);
			return true;
		} catch (DecodeException e) {
			return false;
		}
	}

	/** type of the root element, or -1 if the header is not readable */
	public static int rootType(byte[] b) {
		return b == null || b.length == 0 ? -1 : b[0] & 0x0F;
	}

	/**
	 * Decodes a JSONB value (any root type) into JSON text.
	 *
	 * @throws DecodeException if the bytes are not valid JSONB
	 */
	public static String toJson(byte[] b) {
		if (b == null || b.length == 0)
			throw new DecodeException("empty");
		StringBuilder sb = new StringBuilder(Math.min(b.length * 2, 1 << 20));
		long end = new Parser(b, sb).element(0, b.length, 0);
		if (end != b.length)
			throw new DecodeException("trailing bytes after the JSONB value at offset " + end);
		return sb.toString();
	}

	/** JSON text if the bytes are JSONB (array/object, as {@link #isJsonb}), otherwise null */
	public static String tryJson(byte[] b) {
		return isJsonb(b) ? toJson(b) : null;
	}

	/**
	 * Reads an element header at {@code pos}.
	 *
	 * @return {header length, payload length, type} or null if the header runs past {@code limit}
	 */
	static long[] header(byte[] b, int pos, int limit) {
		if (pos >= limit)
			return null;
		int h = b[pos] & 0xFF;
		int sz = h >>> 4;
		int type = h & 0x0F;
		if (sz <= 11)
			return new long[] { 1, sz, type };
		int n = switch (sz) {
		case 12 -> 1;
		case 13 -> 2;
		case 14 -> 4;
		default -> 8;
		};
		if (pos + 1 + n > limit)
			return null;
		long len = 0;
		for (int i = 0; i < n; i++)
			len = (len << 8) | (b[pos + 1 + i] & 0xFF);
		if (len < 0) // 8-byte size with the top bit set
			return null;
		return new long[] { 1 + n, len, type };
	}

	// ------------------------------------------------------------------ parser

	private static final class Parser {
		final byte[] b;
		final StringBuilder out; // null: validate only

		Parser(byte[] b, StringBuilder out) {
			this.b = b;
			this.out = out;
		}

		/** parses the element at pos (must end at or before limit); returns the end offset */
		long element(int pos, int limit, int depth) {
			if (depth > MAX_DEPTH)
				throw new DecodeException("JSONB nested deeper than " + MAX_DEPTH);
			long[] h = header(b, pos, limit);
			if (h == null)
				throw new DecodeException("truncated JSONB header at offset " + pos);
			long start = pos + h[0];
			long end = start + h[1];
			if (end > limit)
				throw new DecodeException("JSONB element at offset " + pos + " exceeds its container (" + end + " > " + limit + ")");
			int s = (int) start, e = (int) end;
			int type = (int) h[2];
			switch (type) {
			case NULL, TRUE, FALSE -> {
				if (e != s)
					throw new DecodeException(typeName(type) + " with payload at offset " + pos);
				emit(type == NULL ? "null" : type == TRUE ? "true" : "false");
			}
			case INT -> emit(intText(s, e, pos));
			case INT5 -> emit(int5Text(s, e, pos));
			case FLOAT -> emit(floatText(s, e, pos));
			case FLOAT5 -> emit(float5Text(s, e, pos));
			case TEXT, TEXTRAW -> {
				String t = utf8(s, e, pos);
				if (out != null)
					quote(t);
			}
			case TEXTJ -> {
				String t = utf8(s, e, pos);
				checkTextJ(t, pos);
				if (out != null)
					out.append('"').append(t).append('"');
			}
			case TEXT5 -> {
				String t = text5(utf8(s, e, pos), pos);
				if (out != null)
					out.append('"').append(t).append('"');
			}
			case ARRAY -> {
				emit("[");
				int p = s;
				boolean first = true;
				while (p < e) {
					if (!first)
						emit(",");
					first = false;
					p = (int) element(p, e, depth + 1);
				}
				emit("]");
			}
			case OBJECT -> {
				emit("{");
				int p = s;
				boolean first = true;
				while (p < e) {
					if (!first)
						emit(",");
					first = false;
					int kt = b[p] & 0x0F;
					if (kt < TEXT || kt > TEXTRAW)
						throw new DecodeException("object key at offset " + p + " is " + typeName(kt) + ", not text");
					p = (int) element(p, e, depth + 1);
					if (p >= e)
						throw new DecodeException("object key without value at offset " + p);
					emit(":");
					p = (int) element(p, e, depth + 1);
				}
				emit("}");
			}
			default -> throw new DecodeException("reserved JSONB element type " + type + " at offset " + pos);
			}
			return end;
		}

		private void emit(String s) {
			if (out != null)
				out.append(s);
		}

		private String ascii(int s, int e, int pos) {
			if (e == s)
				throw new DecodeException("empty number at offset " + pos);
			char[] c = new char[e - s];
			for (int i = s; i < e; i++) {
				int x = b[i] & 0xFF;
				if (x < 0x20 || x > 0x7E)
					throw new DecodeException("non-ASCII byte in number at offset " + i);
				c[i - s] = (char) x;
			}
			return new String(c);
		}

		private String intText(int s, int e, int pos) {
			String t = ascii(s, e, pos);
			if (!t.matches("-?[0-9]+"))
				throw new DecodeException("invalid INT '" + t + "' at offset " + pos);
			return t;
		}

		private String int5Text(int s, int e, int pos) {
			String t = ascii(s, e, pos);
			if (!t.matches("[+-]?(0[xX][0-9a-fA-F]+|[0-9]+)"))
				throw new DecodeException("invalid INT5 '" + t + "' at offset " + pos);
			boolean neg = t.startsWith("-");
			String u = t.startsWith("+") || neg ? t.substring(1) : t;
			BigInteger v = u.length() > 1 && (u.charAt(1) == 'x' || u.charAt(1) == 'X') ? new BigInteger(u.substring(2), 16)
					: new BigInteger(u);
			return (neg ? v.negate() : v).toString();
		}

		private String floatText(int s, int e, int pos) {
			String t = ascii(s, e, pos);
			if (!t.matches("-?[0-9]+(\\.[0-9]+)?([eE][+-]?[0-9]+)?"))
				throw new DecodeException("invalid FLOAT '" + t + "' at offset " + pos);
			return t;
		}

		private String float5Text(int s, int e, int pos) {
			String t = ascii(s, e, pos);
			String sign = "";
			String u = t;
			if (u.startsWith("+") || u.startsWith("-")) {
				sign = u.startsWith("-") ? "-" : "";
				u = u.substring(1);
			}
			if (u.equals("Infinity") || u.equals("Inf"))
				return sign + "9e999"; // as SQLite's json()
			if (u.equals("NaN"))
				return "null";
			if (!u.matches("([0-9]+\\.?[0-9]*|\\.[0-9]+)([eE][+-]?[0-9]+)?"))
				throw new DecodeException("invalid FLOAT5 '" + t + "' at offset " + pos);
			int ex = Math.max(u.indexOf('e'), u.indexOf('E'));
			String mant = ex < 0 ? u : u.substring(0, ex);
			String exp = ex < 0 ? "" : u.substring(ex);
			if (mant.startsWith("."))
				mant = "0" + mant;
			if (mant.endsWith("."))
				mant = mant + "0";
			return sign + mant + exp;
		}

		private String utf8(int s, int e, int pos) {
			try {
				return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
						.onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(b, s, e - s)).toString();
			} catch (CharacterCodingException ex) {
				throw new DecodeException("invalid UTF-8 in text at offset " + pos);
			}
		}

		/** TEXTJ: text with valid JSON escapes and no characters that must be escaped */
		private void checkTextJ(String t, int pos) {
			for (int i = 0; i < t.length(); i++) {
				char c = t.charAt(i);
				if (c < 0x20 || c == '"')
					throw new DecodeException("unescaped character in TEXTJ at offset " + pos);
				if (c == '\\') {
					if (++i >= t.length())
						throw new DecodeException("dangling escape in TEXTJ at offset " + pos);
					char n = t.charAt(i);
					if (n == 'u') {
						if (i + 4 >= t.length())
							throw new DecodeException("short \\u escape in TEXTJ at offset " + pos);
						for (int k = 1; k <= 4; k++)
							if (Character.digit(t.charAt(i + k), 16) < 0)
								throw new DecodeException("invalid \\u escape in TEXTJ at offset " + pos);
						i += 4;
					} else if ("\"\\/bfnrt".indexOf(n) < 0)
						throw new DecodeException("invalid escape \\" + n + " in TEXTJ at offset " + pos);
				}
			}
		}

		/**
		 * TEXT5: converts the JSON5 escapes into JSON escapes (as SQLite's json() does:
		 * {@code \x41} &rarr; {@code \u0041}, {@code \'} &rarr; {@code '}, line continuations removed)
		 * and escapes characters that JSON does not allow unescaped
		 */
		private String text5(String t, int pos) {
			StringBuilder r = new StringBuilder(t.length() + 8);
			for (int i = 0; i < t.length(); i++) {
				char c = t.charAt(i);
				if (c != '\\') {
					if (c == '"')
						r.append("\\\"");
					else if (c < 0x20)
						r.append(String.format("\\u%04x", (int) c));
					else
						r.append(c);
					continue;
				}
				if (++i >= t.length())
					throw new DecodeException("dangling escape in TEXT5 at offset " + pos);
				char n = t.charAt(i);
				switch (n) {
				case '"', '\\', '/', 'b', 'f', 'n', 'r', 't' -> r.append('\\').append(n);
				case '\'' -> r.append('\'');
				case 'v' -> r.append("\\u000b");
				case '0' -> r.append("\\u0000");
				case 'x' -> {
					r.append(String.format("\\u%04x", hex(t, i + 1, 2, pos)));
					i += 2;
				}
				case 'u' -> {
					hex(t, i + 1, 4, pos);
					r.append('\\').append(t, i, i + 5);
					i += 4;
				}
				case '\n', '\u2028', '\u2029' -> {
					// line continuation
				}
				case '\r' -> {
					if (i + 1 < t.length() && t.charAt(i + 1) == '\n')
						i++;
				}
				default -> throw new DecodeException("invalid escape \\" + n + " in TEXT5 at offset " + pos);
				}
			}
			return r.toString();
		}

		private static int hex(String t, int from, int n, int pos) {
			if (from + n > t.length())
				throw new DecodeException("short hex escape at offset " + pos);
			int v = 0;
			for (int k = 0; k < n; k++) {
				int d = Character.digit(t.charAt(from + k), 16);
				if (d < 0)
					throw new DecodeException("invalid hex escape at offset " + pos);
				v = v * 16 + d;
			}
			return v;
		}

		/** appends a JSON string literal for a plain string value */
		private void quote(String t) {
			out.append('"');
			for (int i = 0; i < t.length(); i++) {
				char c = t.charAt(i);
				switch (c) {
				case '"' -> out.append("\\\"");
				case '\\' -> out.append("\\\\");
				case '\b' -> out.append("\\b");
				case '\f' -> out.append("\\f");
				case '\n' -> out.append("\\n");
				case '\r' -> out.append("\\r");
				case '\t' -> out.append("\\t");
				default -> {
					if (c < 0x20)
						out.append(String.format("\\u%04x", (int) c));
					else
						out.append(c);
				}
				}
			}
			out.append('"');
		}
	}
}
