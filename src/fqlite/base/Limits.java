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
	/**
	 * databases of at least this size (MiB) load the live entries of their index b-trees only
	 * when needed (table opened, export, search); 0 = always load them right away
	 */
	public static int INDEX_ON_DEMAND_MB = 256;
	/**
	 * 1 = also search for deleted records in Lighthouse (ETSI) databases written by FQLite's
	 * importer; 0 (default) = skip it, the evidence is the imported XML file
	 */
	public static int CARVE_LIGHTHOUSE_DB = 0;

	/**
	 * Lighthouse, database opened directly: from this number of records the map
	 * and timeline show an overview (counts per location and time, records read
	 * page by page) instead of loading every record; 0 = always the overview
	 */
	public static int LIGHTHOUSE_OVERVIEW_ROWS = 2_000_000;

	/**
	 * Lighthouse overview: filter and query results up to this number of
	 * records are loaded completely (all functions on single records); larger
	 * results are shown as an overview
	 */
	public static int LIGHTHOUSE_DETAIL_ROWS = 500_000;

	/**
	 * Lighthouse (database opened directly): ask before a query whose estimated
	 * duration is at least this many seconds; 0 = never ask
	 */
	public static int LIGHTHOUSE_CONFIRM_SECONDS = 30;

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
		INDEX_ON_DEMAND_MB = get(p, "INDEX_ON_DEMAND_MB", INDEX_ON_DEMAND_MB, 0, 1_000_000);
		CARVE_LIGHTHOUSE_DB = get(p, "CARVE_LIGHTHOUSE_DB", CARVE_LIGHTHOUSE_DB, 0, 1);
		LIGHTHOUSE_OVERVIEW_ROWS = get(p, "LIGHTHOUSE_OVERVIEW_ROWS", LIGHTHOUSE_OVERVIEW_ROWS, 0, Integer.MAX_VALUE);
		LIGHTHOUSE_DETAIL_ROWS = get(p, "LIGHTHOUSE_DETAIL_ROWS", LIGHTHOUSE_DETAIL_ROWS, 1_000, 100_000_000);
		LIGHTHOUSE_CONFIRM_SECONDS = get(p, "LIGHTHOUSE_CONFIRM_SECONDS", LIGHTHOUSE_CONFIRM_SECONDS, 0, 1_000_000);
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
		p.setProperty("INDEX_ON_DEMAND_MB", String.valueOf(INDEX_ON_DEMAND_MB));
		p.setProperty("CARVE_LIGHTHOUSE_DB", String.valueOf(CARVE_LIGHTHOUSE_DB));
		p.setProperty("LIGHTHOUSE_OVERVIEW_ROWS", String.valueOf(LIGHTHOUSE_OVERVIEW_ROWS));
		p.setProperty("LIGHTHOUSE_DETAIL_ROWS", String.valueOf(LIGHTHOUSE_DETAIL_ROWS));
		p.setProperty("LIGHTHOUSE_CONFIRM_SECONDS", String.valueOf(LIGHTHOUSE_CONFIRM_SECONDS));
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
