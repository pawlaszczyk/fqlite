package fqlite.blob;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;

import fqlite.base.Limits;

/**
 * Recognises compressed BLOB content by its signature and removes the compression - also
 * several nested layers (e.g. gzip inside zstd). Supported: gzip, zlib, zstd, LZ4 (frame,
 * legacy frame, Apple "bv41"), LZFSE/LZVN (Apple "bvx*").
 */
public final class Codecs {

	private Codecs() {
	}

	public enum Codec {
		GZIP("gzip"), ZLIB("zlib"), ZSTD("zstd"), LZ4("LZ4"), LZ4_APPLE("LZ4 (Apple)"), LZFSE("LZFSE/LZVN");

		public final String label;

		Codec(String label) {
			this.label = label;
		}
	}

	/** result of {@link #unwrap}: the layers removed (outermost first) and the content */
	public record Unwrapped(List<Codec> layers, byte[] data, String error) {
		public boolean compressed() {
			return !layers.isEmpty();
		}

		/** e.g. "zstd → gzip" */
		public String chain() {
			StringBuilder sb = new StringBuilder();
			for (Codec c : layers) {
				if (sb.length() > 0)
					sb.append(" → ");
				sb.append(c.label);
			}
			return sb.toString();
		}
	}

	/** the codec whose signature the data starts with, or null */
	public static Codec detect(byte[] d) {
		if (d == null || d.length < 4)
			return null;
		int b0 = d[0] & 0xFF, b1 = d[1] & 0xFF;
		if (b0 == 0x1F && b1 == 0x8B && (d[2] & 0xFF) == 8)
			return Codec.GZIP;
		if (Zstd.isZstd(d))
			return Codec.ZSTD;
		if (Lz4.isFrame(d) || Lz4.isLegacy(d))
			return Codec.LZ4;
		if (Lz4.isApple(d))
			return Codec.LZ4_APPLE;
		if (Lzfse.isLzfse(d) && d[3] != '$')
			return Codec.LZFSE;
		// zlib: CM = 8 (deflate), window <= 32K, no preset dictionary, header checksum
		if ((b0 & 0x0F) == 8 && (b0 >>> 4) <= 7 && (b1 & 0x20) == 0 && ((b0 << 8) | b1) % 31 == 0)
			return Codec.ZLIB;
		return null;
	}

	public static byte[] decompress(Codec c, byte[] d, long limit) {
		return switch (c) {
		case GZIP -> stream(new GzipSafe(d), limit);
		case ZLIB -> inflate(d, limit);
		case ZSTD -> Zstd.decompress(d, limit);
		case LZ4 -> Lz4.frame(d, limit);
		case LZ4_APPLE -> Lz4.apple(d, limit);
		case LZFSE -> Lzfse.decompress(d, limit);
		};
	}

	/** removes all compression layers (at most {@link Limits#BLOB_DECODE_DEPTH}) */
	public static Unwrapped unwrap(byte[] data) {
		List<Codec> layers = new ArrayList<>();
		byte[] cur = data;
		String error = null;
		for (int depth = 0; depth < Limits.BLOB_DECODE_DEPTH; depth++) {
			Codec c = detect(cur);
			if (c == null)
				break;
			try {
				byte[] next = decompress(c, cur, Limits.blobMaxBytes());
				if (next.length == 0 && c == Codec.ZLIB)
					break;
				layers.add(c);
				cur = next;
			} catch (RuntimeException e) {
				byte[] part = e instanceof DecodeException de ? de.partial : null;
				if (c == Codec.ZLIB && part != null && part.length < 32)
					part = null; // probably no zlib stream at all
				// a zlib header is a weak signature: only report errors for the strong ones
				if (c != Codec.ZLIB || (part != null && part.length > 0))
					error = c.label + ": " + e.getMessage();
				if (part != null && part.length > 0) { // damaged or truncated: keep what was decoded
					layers.add(c);
					cur = part;
					error += " - " + part.length + " bytes decoded before the error";
				}
				break;
			}
		}
		return new Unwrapped(layers, cur, error);
	}

	private static byte[] inflate(byte[] d, long limit) {
		Inflater inf = new Inflater();
		ByteSink out = new ByteSink(d.length * 4, limit);
		try {
			inf.setInput(d);
			byte[] buf = new byte[65536];
			while (!inf.finished()) {
				int n = inf.inflate(buf);
				if (n == 0) {
					if (inf.needsInput() || inf.needsDictionary())
						throw new DecodeException("zlib: truncated or needs a dictionary", out.toArray());
				}
				out.put(buf, 0, n);
			}
			return out.toArray();
		} catch (DataFormatException e) {
			throw new DecodeException("zlib: " + e.getMessage(), out.toArray());
		} finally {
			inf.end();
		}
	}

	private static byte[] stream(InputStream in, long limit) {
		ByteSink out = new ByteSink(65536, limit);
		byte[] buf = new byte[65536];
		try (in) {
			int n;
			while ((n = in.read(buf)) > 0)
				out.put(buf, 0, n);
		} catch (IOException e) {
			if (out.size == 0)
				throw new DecodeException(e.getMessage());
			// truncated member: keep what was decoded
		}
		return out.toArray();
	}

	/** gzip stream that wraps the IOException of the constructor */
	private static final class GzipSafe extends InputStream {
		private final InputStream in;

		GzipSafe(byte[] d) {
			try {
				in = new GZIPInputStream(new ByteArrayInputStream(d), 65536);
			} catch (IOException e) {
				throw new DecodeException("gzip: " + e.getMessage());
			}
		}

		@Override
		public int read() throws IOException {
			return in.read();
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			return in.read(b, off, len);
		}

		@Override
		public void close() throws IOException {
			in.close();
		}
	}
}
