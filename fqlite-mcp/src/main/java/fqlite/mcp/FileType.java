package fqlite.mcp;

/**
 *  This class determines the file type of a BLOB from its signature (magic bytes):
 *
 * @author pawlaszc
 *
 */
final class FileType {

	/**
	 *  static helper class.
	 */
	private FileType() {
	}

	/**
	 * Call this method to determine the type of BLOB.
	 *
	 * @param b
	 * @return
	 */
	static String detect(byte[] b) {
		if (starts(b, 0x89, 'P', 'N', 'G')) return "png";
		if (starts(b, 0xFF, 0xD8, 0xFF)) return "jpeg";
		if (starts(b, 'G', 'I', 'F', '8')) return "gif";
		if (starts(b, '%', 'P', 'D', 'F')) return "pdf";
		if (starts(b, 'B', 'M') && b.length > 14) return "bmp";
		if (starts(b, 'I', 'I', '*', 0) || starts(b, 'M', 'M', 0, '*')) return "tiff";
		if (starts(b, 'b', 'p', 'l', 'i', 's', 't')) return "bplist";
		if (starts(b, '<', '?', 'x', 'm', 'l')) return "xml";
		if (starts(b, 'P', 'K', 3, 4)) return "zip";
		if (starts(b, 0x1F, 0x8B)) return "gzip";
		if (starts(b, 'S', 'Q', 'L', 'i', 't', 'e', ' ', 'f')) return "sqlite";
		if (starts(b, 'O', 'b', 'j', 1)) return "avro";
		if (starts(b, 'R', 'I', 'F', 'F') && b.length > 12 && b[8] == 'W' && b[9] == 'E' && b[10] == 'B') return "webp";
		if (b.length > 12 && b[4] == 'f' && b[5] == 't' && b[6] == 'y' && b[7] == 'p') {
			String brand = new String(b, 8, 4, java.nio.charset.StandardCharsets.ISO_8859_1);
			if (brand.startsWith("hei") || brand.startsWith("mif") || brand.equals("heix")) return "heic";
			return "mp4";
		}
		return "bin";
	}

	static String extension(String type) {
		return switch (type) {
			case "jpeg" -> "jpg";
			case "bplist" -> "plist";
			case "gzip" -> "gz";
			default -> type;
		};
	}

	/**
	 * Check, whether the givn byte sequence begins with the particular magic number.
	 * @param b
	 * @param sig
	 * @return
	 */
	private static boolean starts(byte[] b, int... sig) {
		if (b.length < sig.length)
			return false;
		for (int i = 0; i < sig.length; i++)
			if ((b[i] & 0xFF) != (sig[i] & 0xFF))
				return false;
		return true;
	}
}
