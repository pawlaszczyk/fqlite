package fqlite.base;

import java.util.Properties;

/**
 * Adjustable limits and display sizes (Settings &rarr; Display &amp; limits), stored in
 * fqlite.conf. All values are clamped to a sensible range when they are loaded.
 */
public final class Limits {

	private Limits() {
	}

	/** font size of the user interface in px */
	public static int UI_FONT_SIZE = 13;
	/** font size of the hex dump and the code views in px */
	public static int MONO_FONT_SIZE = 12;
	/** bytes of a cell shown in the hex pane below the table */
	public static int HEX_PREVIEW_BYTES = 256;
	/** maximum size of decompressed BLOB content in MiB (protection against decompression bombs) */
	public static int BLOB_MAX_MB = 64;
	/** maximum number of nested layers (compression, embedded plists, ...) decoded in a BLOB */
	public static int BLOB_DECODE_DEPTH = 4;
	/** rows sampled to find the fields of a column ("Decode column into fields") */
	public static int FIELD_SCAN_ROWS = 2000;
	/** maximum number of fields listed per decoded value */
	public static int FIELD_MAX_PATHS = 500;
	/** maximum number of missing row numbers listed per table */
	public static int GAPS_MAX_LISTED = 100_000;
	/** maximum number of hits of a case search */
	public static int CASE_MAX_HITS = 100_000;

	public static long blobMaxBytes() {
		return (long) BLOB_MAX_MB << 20;
	}

	public static void load(Properties p) {
		UI_FONT_SIZE = get(p, "UI_FONT_SIZE", UI_FONT_SIZE, 9, 28);
		MONO_FONT_SIZE = get(p, "MONO_FONT_SIZE", MONO_FONT_SIZE, 8, 28);
		HEX_PREVIEW_BYTES = get(p, "HEX_PREVIEW_BYTES", HEX_PREVIEW_BYTES, 16, 1 << 20);
		BLOB_MAX_MB = get(p, "BLOB_MAX_MB", BLOB_MAX_MB, 1, 1024);
		BLOB_DECODE_DEPTH = get(p, "BLOB_DECODE_DEPTH", BLOB_DECODE_DEPTH, 1, 16);
		FIELD_SCAN_ROWS = get(p, "FIELD_SCAN_ROWS", FIELD_SCAN_ROWS, 10, 10_000_000);
		FIELD_MAX_PATHS = get(p, "FIELD_MAX_PATHS", FIELD_MAX_PATHS, 10, 100_000);
		GAPS_MAX_LISTED = get(p, "GAPS_MAX_LISTED", GAPS_MAX_LISTED, 100, 100_000_000);
		CASE_MAX_HITS = get(p, "CASE_MAX_HITS", CASE_MAX_HITS, 100, 100_000_000);
	}

	public static void store(Properties p) {
		p.setProperty("UI_FONT_SIZE", String.valueOf(UI_FONT_SIZE));
		p.setProperty("MONO_FONT_SIZE", String.valueOf(MONO_FONT_SIZE));
		p.setProperty("HEX_PREVIEW_BYTES", String.valueOf(HEX_PREVIEW_BYTES));
		p.setProperty("BLOB_MAX_MB", String.valueOf(BLOB_MAX_MB));
		p.setProperty("BLOB_DECODE_DEPTH", String.valueOf(BLOB_DECODE_DEPTH));
		p.setProperty("FIELD_SCAN_ROWS", String.valueOf(FIELD_SCAN_ROWS));
		p.setProperty("FIELD_MAX_PATHS", String.valueOf(FIELD_MAX_PATHS));
		p.setProperty("GAPS_MAX_LISTED", String.valueOf(GAPS_MAX_LISTED));
		p.setProperty("CASE_MAX_HITS", String.valueOf(CASE_MAX_HITS));
	}

	public static int get(Properties p, String key, int def, int min, int max) {
		String v = p.getProperty(key);
		if (v == null)
			return def;
		try {
			return Math.max(min, Math.min(max, Integer.parseInt(v.trim())));
		} catch (NumberFormatException e) {
			return def;
		}
	}
}
