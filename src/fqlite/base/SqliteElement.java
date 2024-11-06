package fqlite.base;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Arrays;

import fqlite.types.SerialTypes;
import fqlite.types.StorageClass;
import fqlite.util.Auxiliary;


/**
 * This class represents a concrete Table column. 
 * 
 * @author pawlaszc
 *
 */
public class SqliteElement {
	public SerialTypes type;
	public StorageClass serial;
	public int length;
	
	public SqliteElement(SerialTypes type, StorageClass serial, int length) {
		this.length = length;
		this.type = type;
		this.serial = serial;
	}
	
	public final String getBLOB(byte[] value, boolean truncBLOB){
		
		/* we took only the first 32 characters of the byte array 
		 * to display if truncBLOB is true */
		String s;
		if (truncBLOB)
			s = toString(value,false,true);
		else
			s = toString(value,false,false);
		
		return parseBLOB(s)+s;
	}
	
	

	public final String toString(byte[] value, boolean withoutQuotes, boolean truncBLOB) {
		
		try {
		//System.out.println("type:::: " + type + " " + " Serial " + serial);
		if (type == SerialTypes.INT0)
			return String.valueOf(0);
		else if (type == SerialTypes.INT1)
			return String.valueOf(1);
		else if ((type == SerialTypes.PRIMARY_KEY) && (value.length == 0)) {
		   	return "";
		}
		else if (value.length == 0) {
			//System.out.println("Achtung!!! länge null type:: " + type);
			return "";
		}
		else if (type == SerialTypes.STRING)
		{
			//withoutQuotes=false;
			
			String result = decodeString(value).toString();
		    if (withoutQuotes)
		    	return result;
		    else
		    	return result; //"\""+ result + "\"";
		}
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
			return String.valueOf(decodeFloat64(value)).replace(",",".");
		else if (type == SerialTypes.BLOB)
			 {  
			  
			  if (truncBLOB && value.length > 32)
				  return String.valueOf(Auxiliary.bytesToHex(Arrays.copyOfRange(value, 0, 32)));
			  
			  return String.valueOf(Auxiliary.bytesToHex(Arrays.copyOfRange(value, 0, value.length)));   	
			  	  
	    }
		}catch(Exception err){
			System.out.println();
		}
		
		
		return null;

	}

	
	public static String parseBLOB(String blob)
	{
		
		if(blob.contains("ffd8"))
			return "<jpg>";
		if(blob.contains("89504e470d0a1a"))
		    return "<png>";
		if(blob.startsWith("003b"))
		    return "<gif>";
		if(blob.startsWith("424d"))
		    return "<bmp>";
		if(blob.contains("25504446"))
			return "<pdf>";			
		if(blob.startsWith("62706c697374"))
			return "<plist>";			
		if(blob.startsWith("49492a00") || blob.startsWith("4D4D002A"))
			return "<tiff>";			
		if(blob.contains("474946383761") || blob.startsWith("474946383961"))
			return "<gif>";			
		if(blob.startsWith("1f8b"))
			return "<gzip>";
		if(blob.contains("66747970686569") || blob.contains("667479706d"))
			return "<heic>";
		if(blob.startsWith("aced0005"))
			return "<java>";
		if(blob.startsWith("4f626a")) 
			return "<avro>";
		return "";
	}
	
	public final static int decodeInt8(byte v) {
		return v;
	}

	public final static int decodeInt16(byte[] v) {
		ByteBuffer bf = ByteBuffer.wrap(v);
		return bf.getShort();
	}

	public final static int decodeInt24(byte[] v) {
		int result = int24bytesToUInt(v);
		return result;
	}

	private static int int24bytesToUInt(byte[] input) {

		if (input.length < 3)
			return (0 & 0xFF << 24) | (0 & 0xFF) << 16 | (input[0] & 0xFF) << 8 | (input[1] & 0xFF) << 0;

		return (0 & 0xFF << 24) | (input[0] & 0xFF) << 16 | (input[1] & 0xFF) << 8 | (input[2] & 0xFF) << 0;
	}
	

	public final static int decodeInt32(byte[] v) {
		ByteBuffer bf = ByteBuffer.wrap(v);
		return bf.getInt();
	}

	
	public final static long decodeInt48ToLong(byte[] v){
				// we have to read 6 Bytes
				if (v.length < 6)
					return 0;
				ByteBuffer bf = ByteBuffer.wrap(v);
				byte[] value = bf.array();
				byte[] converted = new byte[8];

				for (int i = 0; i < 6; i++) {
					converted[i + 2] = value[i];
				}
				ByteBuffer result = ByteBuffer.wrap(converted);
		 
		       // bf.order(ByteOrder.BIG_ENDIAN);
					
				long z = result.getLong();
				return z;
	}
	
	public final static String decodeInt48(byte[] v) {
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
		
		//String int64 = DatetimeConverter.isUnixEpoch(z);
		//if (null == int64)
		//{
			return Long.toString(z);
		//}
		//return int64;
	}

	final static String decodeInt64(byte[] v) {
		ByteBuffer bf = ByteBuffer.wrap(v);
		long z = bf.getLong();

		//String int64 = DatetimeConverter.isUnixEpoch(z);
		//if (null == int64)
		//{
		String int64 = Long.toString(z);
		//}
		//if (z > 100000000000L)
		//	return convertToDate(z);
		return int64;
	}

//	final static String convertToDate(long value) {
		
//		ZonedDateTime utc = ZonedDateTime.ofInstant(Instant.ofEpochMilli(value),ZoneOffset.UTC);
//		Date d = new Date(value / 1000);
		//SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
//		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy - HH:mm:ss Z");

//		return utc.format(formatter);//dateFormat.format(d);
//	}

	final static String decodeFloat64(byte[] v) {
		ByteBuffer bf = ByteBuffer.wrap(v);
		
		double d = bf.getDouble();
		
		//if(d > 600000000)
		//{
			//System.out.println("MAC-Time gefunden " + d);
		//}
		//String fp64 = DatetimeConverter.isMacAbsoluteTime(d);
		//if (null == fp64)
		return String.format("%.8f", d);
		// return bf.getDouble();
		// System.out.println("Rückgabe :: " + fp64);
		//return fp64;
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
