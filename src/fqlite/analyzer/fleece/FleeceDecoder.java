package fqlite.analyzer.fleece;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import fqlite.analyzer.Converter;
import fqlite.base.Job;

/**
 * This class supports the de-serialisation of JSON messages encoded using
 * Fleece.
 * 
 * @author pawlaszc
 *
 */
public class FleeceDecoder extends Converter {

	static ArrayList<String> values = new ArrayList<String>();
	static byte[] stream = null;
	static TreeMap<Integer, String> document = new TreeMap<Integer, String>();
	static int pos;
	static ByteBuffer bb = null;
	static StringBuffer json = null;
	static StringBuffer text = new StringBuffer();

	private static void init(){
		values = new ArrayList<String>();
		stream = null;
		document = new TreeMap<Integer, String>();
		bb = null;
		json = new StringBuffer();
		text = new StringBuffer();
		pos = 0;
	}
	
	/**
	 * Call this method to start the decoding process of a Fleece-serial stream
	 * 
	 * @param A byte array with the payload to decode.
	 * 
	 */
	public static String decode(byte[] stream) {
	    init();
		text = new StringBuffer();
		FleeceDecoder.stream = stream;
		bb = ByteBuffer.wrap(stream);
		boolean goon = false;

		do {
			goon = decodeSlice();

		} while (goon);

		System.out.println("in decode()" + text);
		if (text.length() == 0)
			return "invalid value";
		return text.toString();
	}

	/**
	 * The central decoding function. Note: The function is also used recursively to
	 * decode nested data structures.
	 * 
	 * @return Whether or not the decoding was successful
	 */
	private static boolean decodeSlice() {
		if (pos >= stream.length)
			return false; // do not go on - end of stream reached

		// read next byte from stream
		int next = stream[pos];

		// type identifier -> it's only 4 bits
		int b = next >> 4;

		// extract length (the lower half byte)
		int length = next & 0xF;

		// because of left shift for a neg. number the signed is shifted
		b = Math.abs(b);

		switch (b) {
		// 00 <empty> - ignore and go on with next byte in stream
		case 0:
			if (next == 0) {
				pos++;
				return true;
			} else {
				parseSmallInt(pos, (byte) length);
				pos++;
				return true;
			}
			// 10 int (32Bit)
		case 1:
			pos = parseInt32(pos, (byte) length + 1);
			return true;
		// float (64Bit IEEE floating point)
		case 2:
			pos = parseFloat(pos);
			return true;
		// boolean value 38 -> true 34 -> false 30 -> null
		case 3:
			switch (next) {
			case 0x30:
				text.append("null \n");
				document.put(pos, "null");
				break;
			case 0x34:
				text.append("false \n");
				document.put(pos, "false");
				break;
			case 0x38:
				text.append("true \n");
				document.put(pos, "true");
				break;
			}
			pos += 2;
			return true;
		// String type 4?x0
		case 4:
			pos = parseString(pos, length);
			return true;

		case 5:

			// Array
		case 6:
			int ae = stream[pos + 1];
			parseArray(ae);
			return true;
		// Dictionary
		case 7:
			int de = stream[pos + 1];
			parseDictionary(de);
			return true;

		// Pointer Reference
		case 8:
			printOffset(pos);
			byte[] value = new byte[] { stream[pos], stream[pos + 1] };
			printBytes(value, false);
			followReference(pos + 1, getBackwardOffset(value));
			pos += 2; // go on
			return true;

		// nothing above is matching? -> skip decoding
		default: {
			return false;
		}
		}
	}

	static int getBackwardOffset(byte[] pointer) {
		ByteBuffer wrapped = ByteBuffer.wrap(pointer); // big-endian by default
		short num = wrapped.getShort(); // create a short value out of the byte array
		// only the lower 12 bits are used for the offset
		int offset = num & 0x0FFF;
		return offset;
	}

	static void printOffset(int pos) {
		text.append(String.format("%04x: ", pos));
	}

	static void printType(int pos) {
		text.append(String.format("%02x ", stream[pos]));
	}

	static void printBytes(byte[] values, boolean nogap) {
		int max = 0;
		max = values.length < 4 ? values.length : 4;

		for (int i = 0; i < 4; i++) {
			if (i < max)
				text.append(String.format("%02x ", values[i]));
			else
				text.append("   ");

		}

		if (max > 4)
			text.append("...");
		else
			text.append("   ");

		if (!nogap)
			text.append("   ");
		text.append(" | ");
	}

	/**
	 * Parse a int out of the byte array.
	 * 
	 * @param pos
	 * @return
	 */
	static int parseInt32(int pos, int length) {
		printOffset(pos);
		printType(pos);
		pos++;
		byte[] bs = Arrays.copyOfRange(stream, pos, pos + length);
		printBytes(bs, true);
		bs = invert(bs);
		long value = new BigInteger(bs).intValue();
		text.append(value + "\n");
		document.put(pos - 1, String.valueOf(value));
		return pos + length; // next byte offset after number
	}

	static void parseSmallInt(int pos, byte value) {
		printOffset(pos - 1);
		printBytes(new byte[] { 0, value }, false);
		document.put(pos, String.valueOf(value));
		text.append("\t" + Integer.valueOf(value) + "\n");
	}

	static int parseFloat(int pos) {
		printOffset(pos);
		printType(pos);
		pos += 2;
		byte[] bs = Arrays.copyOfRange(stream, pos, pos + 8);
		printBytes(bs, true);
		bs = invert(bs);
		ByteBuffer buffer = ByteBuffer.wrap(bs);
		double db = buffer.getDouble();
		text.append(db + "\n");
		document.put(pos - 2, String.valueOf(db));
		return pos + 8; // next byte offset after number
	}

	/**
	 * Parse a string out of the byte array.
	 * 
	 * @param pos
	 * @param length
	 * @return
	 */
	static int parseString(int pos, int length) {
		printOffset(pos);
		printType(pos);
		pos++;

		byte[] bs;
		if (length == 0xF) {
			// string length exceeds 15 -> read length byte (a varint)

			length = readUnsignedVarInt();

			if (length <= 127)
				pos++;
			else if (length <= 16384)
				pos += 2;
			else if (length <= 2097152)
				pos += 3;
			else if (length <= 268435456)
				pos += 4;

			bs = Arrays.copyOfRange(stream, pos, pos + length);
			printBytes(bs, true);
			String str = new String(bs, StandardCharsets.UTF_8);
			str = "\"" + str + "\"";
			text.append(str + "\n");
			document.put(pos - 2, str);

		} else {
			// short string
			bs = Arrays.copyOfRange(stream, pos, pos + length);
			printBytes(bs, true);
			String str = new String(bs, StandardCharsets.UTF_8);
			str = "\"" + str + "\"";
			text.append(str + "\n");
			document.put(pos - 1, str);

		}

		return pos + length; // next byte offset after String

	}

	/**
	 * Changes from BigEndian to LittleEndian and vice versa.
	 */
	static byte[] invert(byte[] array) {
		for (int i = 0; i < array.length / 2; i++) {
			byte temp = array[i];
			array[i] = array[array.length - 1 - i];
			array[array.length - 1 - i] = temp;
		}
		return array;
	}

	/**
	 * Converts a given Hex-String into a byte array.
	 * 
	 * @param s must be an even-length string.
	 */
	public static byte[] hexStringToByteArray(String s) {
		int len = s.length();
		text.append("Length" + len + "\n");
		byte[] data = new byte[len / 2];
		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
		}
		return data;
	}

	/**
	 * Parse Array for all fields.
	 * 
	 */
	static int parseArray(int elements) {
		printOffset(pos);
		byte[] type = new byte[] { stream[pos], stream[++pos] };
		printBytes(type, false);
		text.append("Array[" + elements + "]:" + "\n");
		document.put(pos - 1, "Array[" + elements + "]@" + (pos - 1));

		pos++;
		for (int i = 0; i < elements; i++) {
			for (int j = 0; j < 2; j++)
				decodeSlice();
		}
		return -1;
	}

	/**
	 * Parse Dictionary for all key-value <k,v> pairs.
	 * 
	 * @param s must be an even-length string.
	 */
	static void parseDictionary(int elements) {
		int start = pos;

		printOffset(pos);
		byte[] type = new byte[] { stream[pos], stream[pos + 1] };
		printBytes(type, false);
		text.append("Dict[" + elements + "]:" + "\n");
		document.put(start, "Dict[" + elements + "]@" + start);
		pos += 2;
		for (int i = 0; i < elements; i++) {

			for (int j = 0; j < 2; j++) {
				printOffset(pos);
				byte b = stream[pos];

				// Pointers are always expressed as relative backwards offsets
				pos++;

				byte[] slice = new byte[] { b, stream[pos] };

				printBytes(slice, false);

				if (b == -128) // 0x80
				{
					int boff = getBackwardOffset(slice);
					pos = followReference(pos, boff);
				} else {
					// it is not //0x80 -> it is a real 2byte value;
					// go one byte back to catch the right type

					if (b == 00) {
						// within a dictionary we have 16bit(2Byte) <key,value>
						document.put(pos, String.valueOf(stream[pos]));
						text.append(Integer.valueOf(stream[pos]) + "\n");
						pos++;
					} else {
						// in a dictionary a slice start 00.. - small in 30<null>,34<true>,38<false>..
						// or 80<pointer>
						pos--;
						decodeSlice();
					}
				}

			}

		}

	}

	static int followReference(int pos, int boff) {
		// first the key
		text.append("&");

		int offset = boff * 2; // assuming a 2-Byte offset

		int goback = pos - 1 - offset;

		String value = document.get(goback);
		text.append(value);
		text.append(" (@" + String.format("%04x", goback) + ")" + "\n");
		document.put(pos, "@" + goback);

		pos++;

		return pos;
	}

	/**
	 * This method reads a Varint value startRegion the transferred buffer. A varint
	 * has a length between 1 and 9 bytes. The MSB displays whether further bytes
	 * follow. If it is set to 1, then at least one more byte can be read.
	 * 
	 */
	public static int readUnsignedVarInt() {

		bb.position(pos + 1);
		ByteBuffer buf = ByteBuffer.allocate(9);

		byte b = bb.get();
		while ((b & 0x80) != 0) {
			// MSB Bit is set
			buf.put(b);
			b = bb.get();
		}
		buf.put(b);
		buf.position(0);

		byte[] test = buf.array();
		invert(test);

		int j = 0;
		while (test[j] == 0) {
			j++;
		}

		// get first byte (MSB)
		b = test[j];

		// MSB (left bit) to 0
		int value = b & 0x7F;

		// second byte
		if (j >= (test.length - 1)) {
			return value;
		}

		b = test[++j];

		while ((b & 0x80) != 0) {
			value <<= 7;
			value |= (b & 0x7F);
			if (++j < test.length)
				/* 3th,4th..byte */
				b = test[j];
			else
				break;
		}

		return value;

	}

	/**
	 * 
	 * @return
	 */
	 public static String toJSON() {
		json = new StringBuffer();

		text.append(document.toString() + "\n");

		NavigableMap<Integer, String> tree = document.descendingMap();

		String until = tree.firstEntry().getValue();
		if (until.startsWith("@")) {
			until = document.get(Integer.valueOf(until.substring(1)));
		}

		// a JSON document always starts with an opening bracket
		json.append("JSON{ \n");

		
		ArrayList<String> list = new ArrayList<String>();

		for (String element : tree.values()) // iterate values.
		{
			list.add(element);
		}

		for (int i = 1; i < list.size(); i += 2) {

			String k, v;

			k = list.get(i + 1);
			v = list.get(i);

			if (k.equals(until) || v.equals(until)) {
				json.append("}");
				break;
			}

			if (v.startsWith("@")) {
				v = document.get(Integer.valueOf(v.substring(1)));
			}

			if (k.startsWith("@")) {
				k = document.get(Integer.valueOf(k.substring(1)));
			}

			if (v.startsWith("Dict[")) {
				// nested Dictionary
				v = parseDic(v);
			}

			if (v.startsWith("Array[")) {
				// nested Array
				v = parseArray(v);
			}

			json.append("\t " + k + ": " + v + ",");

			json.append("\r\n");

		}

		text.append(json.toString() + "\n");

		return json.toString();
	}

	/**
	 * Parse array with its fields and create json string.
	 * 
	 * @param name
	 * @return
	 */
	private static String parseArray(String name) {
		StringBuffer json = new StringBuffer();

		int key = Integer.valueOf(name.split("@")[1]);

		String ahead = document.get(key);
		int number = howMany(ahead);

		Set<Integer> keysGreaterThanEqToArrayKey = document.tailMap(key).keySet();

		Iterator<Integer> it = keysGreaterThanEqToArrayKey.iterator();

		// skip first element (the array header)
		it.next();

		json.append("[");
		// go on with follow up keys - the array elements
		for (int i = 0; i < number; i++) {
			Integer nextkey = it.next();
			String value = document.get(nextkey);

			/* nested reference */
			if (isDictRef(value))
				value = parseDic(value);
			else if (isArrayRef(value))
				value = parseArray(value);

			json.append((value));

			if (i < (number - 1))
				json.append(",");
		}
		json.append("]");

		return json.toString();
	}

	/**
	 * Parse dictionary with its elements and create string.
	 * 
	 * @param name
	 * @return
	 */
	private static String parseDic(String name) {
		StringBuffer json = new StringBuffer();
		json.append("{  \r\n");

		int key = Integer.valueOf(name.split("@")[1]);

		String ahead = document.get(key);
		int number = howMany(ahead);

		// the dictionary <v,k> values follow directly after the dict-header
		Set<Integer> keysGreaterThanEqToArrayKey = document.tailMap(key).keySet();

		Iterator<Integer> it = keysGreaterThanEqToArrayKey.iterator();

		// skip first element (the dictionary header)
		it.next();

		for (int i = 0; i < number * 2; i += 2) {

			Integer kk = it.next();
			Integer vv = it.next();

			String v = document.get(vv);
			String k = document.get(kk);

			if (v.startsWith("@")) {
				v = document.get(Integer.valueOf(v.substring(1)));
			}

			if (k.startsWith("@")) {
				k = document.get(Integer.valueOf(k.substring(1)));
			}

			if (v.startsWith("Dict[")) {
				// nested Dictionary
				v = parseDic(v);
			}

			if (v.startsWith("Array[")) {
				// nested Array
				v = parseArray(v);
			}

			json.append("\t \t" + k + ": " + v + "," + "\r\n");

		}

		json.append("\t " + "}");

		return json.toString();
	}

	private static int howMany(String s) {
		// how many elements are inside the this level ?
		Matcher m = Pattern.compile("\\[(\\d+)\\]").matcher(s);
		while (m.find()) {
			return Integer.valueOf(m.group(1));
		}

		return 0;
	}

	private static boolean isDictRef(String value) {
		return value.startsWith("Dict[") && value.contains("&");
	}

	private static boolean isArrayRef(String value) {
		return value.startsWith("Array[") && value.contains("&");
	}

	@Override
	public String decode(Job job, String offset) {

		try {
			String data = job.bincache.getHexString(offset);
			data = data.toUpperCase();
			byte[] stream = FleeceDecoder.hexStringToByteArray(data);
			return decode(stream);
		} catch (Exception err) {
			return "invalid stream";
		}
	}
}
