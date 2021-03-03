package fqlite.base;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.text.SimpleDateFormat;
import fqlite.types.SerialTypes;
import fqlite.types.StorageClasses;
import fqlite.util.Auxiliary;
import fqlite.util.DatetimeConverter;


/**
 * This class represents a concrete Table column. 
 * 
 * @author pawlaszc
 *
 */
public class SqliteElement {
	public SerialTypes type;
	public StorageClasses serial;
	public int length;

	public SqliteElement(SerialTypes type, StorageClasses serial, int length) {
		this.length = length;
		this.type = type;
		this.serial = serial;
	}

	public final String toString(byte[] value) {
		if (type == SerialTypes.INT0)
			return String.valueOf(0);
		else if (type == SerialTypes.INT1)
			return String.valueOf(1);
		else if (value.length == 0)
			return "";
		else if (type == SerialTypes.STRING)
			return decodeString(value).toString();
		else if (type == SerialTypes.INT8)
			return String.valueOf(decodeInt8(value[0]));
		else if (type == SerialTypes.INT16)
			return String.valueOf(decodeInt16(value));
		else if (type == SerialTypes.INT24)
			return String.valueOf(decodeInt24(value));
		else if (type == SerialTypes.INT32)
			return String.valueOf(decodeInt32(value));
		else if (type == SerialTypes.INT48)
			return String.valueOf(decodeInt48(value));
		else if (type == SerialTypes.INT64)
			return String.valueOf(decodeInt64(value));
		else if (type == SerialTypes.FLOAT64)
			return String.valueOf(decodeFloat64(value));
		else if (type == SerialTypes.BLOB)
			return String.valueOf(Auxiliary.bytesToHex(value));
		return null;

	}

	public final static int decodeInt8(byte v) {
		return v;
	}

	final static int decodeInt16(byte[] v) {
		ByteBuffer bf = ByteBuffer.wrap(v);
		return bf.getShort();
	}

	final int decodeInt24(byte[] v) {
		int result = int24bytesToUInt(v);
		return result;
	}

	private static int int24bytesToUInt(byte[] input) {

		if (input.length < 3)
			return (0 & 0xFF << 24) | (0 & 0xFF) << 16 | (input[0] & 0xFF) << 8 | (input[1] & 0xFF) << 0;

		return (0 & 0xFF << 24) | (input[0] & 0xFF) << 16 | (input[1] & 0xFF) << 8 | (input[2] & 0xFF) << 0;
	}

	final static int decodeInt32(byte[] v) {
		ByteBuffer bf = ByteBuffer.wrap(v);
		return bf.getInt();
	}

	final static String decodeInt48(byte[] v) {
		// we have to read 6 Bytes
		if (v.length < 6)
			return "00";
		ByteBuffer bf = ByteBuffer.wrap(v);
		byte[] value = bf.array();
		byte[] converted = new byte[8];

		for (int i = 0; i < 6; i++) {
			converted[i + 2] = value[i];
		}
		ByteBuffer result = ByteBuffer.wrap(converted);
 
       // bf.order(ByteOrder.BIG_ENDIAN);
			
		long z = result.getLong();
			
		//if (z > 1000000000000000L && z < 1800000000000000L)
		//	return convertToDate(z);
		//else
		//	return Long.toString(z);
		
		String int64 = DatetimeConverter.isUnixEpoch(z);
		if (null == int64)
		{
			Long.toString(z);
		}
		return int64;
	}

	final static String decodeInt64(byte[] v) {
		ByteBuffer bf = ByteBuffer.wrap(v);
		long z = bf.getLong();

		String int64 = DatetimeConverter.isUnixEpoch(z);
		if (null == int64)
		{
			Long.toString(z);
		}
		//if (z > 100000000000L)
		//	return convertToDate(z);
		return int64;
	}

	final static String convertToDate(long value) {
		Date d = new Date(value / 1000);
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
		return dateFormat.format(d);
	}

	final static String decodeFloat64(byte[] v) {
		ByteBuffer bf = ByteBuffer.wrap(v);
		
		double d = bf.getDouble();
		
		if(d > 600000000)
		{
			System.out.println("MAC-Time gefunden " + d);
		}
		String fp64 = DatetimeConverter.isMacAbsoluteTime(d);
		if (null == fp64)
			return String.format("%.8f", d);
		// return bf.getDouble();
		System.out.println("Rückgabe :: " + fp64);
		return fp64;
	}

	final static CharBuffer decodeString(byte[] v) {
		return Job.db_encoding.decode(ByteBuffer.wrap(v));
	}

	private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

	public static String bytesToHex(byte[] bytes) {
		char[] hexChars = new char[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = HEX_ARRAY[v >>> 4];
			hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
		}
		return new String(hexChars);
	}

	public static boolean isStringContent(byte[] value) {
		float threshold = 0.8f;
		int printable = 0;

		for (byte b : value) {
			if (b >= 32 && b < 127) {
				printable++;
			}
		}

		if (printable / value.length > threshold) {
			return true;
		}

		return false;
	}

}
