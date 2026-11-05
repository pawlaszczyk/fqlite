package fqlite.blob;

/**
 * LZ4 decoder: raw blocks, the LZ4 frame format (magic 04 22 4D 18), the legacy frame
 * (02 21 4C 18) and Apple's block container of the Compression framework ("bv41", "bv4-").
 * Checksums are not verified - the content of a damaged frame is still shown as far as it
 * can be decoded.
 */
public final class Lz4 {

	private Lz4() {
	}

	/** decodes one LZ4 block {@code [off, off+len)} into {@code out} */
	static void block(byte[] in, int off, int len, ByteSink out) {
		int ip = off, end = off + len;
		while (ip < end) {
			int token = in[ip++] & 0xFF;
			int lit = token >>> 4;
			if (lit == 15) {
				int b;
				do {
					if (ip >= end)
						throw new DecodeException("LZ4: truncated literal length");
					b = in[ip++] & 0xFF;
					lit += b;
				} while (b == 255);
			}
			if (ip + lit > end)
				throw new DecodeException("LZ4: literals beyond the end of the block");
			out.put(in, ip, lit);
			ip += lit;
			if (ip >= end)
				break; // the last sequence has no match
			if (ip + 2 > end)
				throw new DecodeException("LZ4: truncated match offset");
			int dist = (in[ip] & 0xFF) | (in[ip + 1] & 0xFF) << 8;
			ip += 2;
			int ml = token & 15;
			if (ml == 15) {
				int b;
				do {
					if (ip >= end)
						throw new DecodeException("LZ4: truncated match length");
					b = in[ip++] & 0xFF;
					ml += b;
				} while (b == 255);
			}
			out.match(dist, ml + 4);
		}
	}

	static boolean isFrame(byte[] d) {
		return d.length >= 7 && u32(d, 0) == 0x184D2204L;
	}

	static boolean isLegacy(byte[] d) {
		return d.length >= 8 && u32(d, 0) == 0x184C2102L;
	}

	static boolean isApple(byte[] d) {
		return d.length >= 8 && d[0] == 'b' && d[1] == 'v' && d[2] == '4' && (d[3] == '1' || d[3] == '-');
	}

	static byte[] frame(byte[] in, long limit) {
		ByteSink out = new ByteSink(in.length * 3, limit);
		try {
			return frame0(in, out);
		} catch (DecodeException | IndexOutOfBoundsException e) {
			throw new DecodeException(e.getMessage(), out.toArray());
		}
	}

	private static byte[] frame0(byte[] in, ByteSink out) {
		int p = 0;
		while (p + 4 <= in.length) {
			long magic = u32(in, p);
			if ((magic & 0xFFFFFFF0L) == 0x184D2A50L) { // skippable frame
				p += 8 + (int) u32(in, p + 4);
				continue;
			}
			if (magic == 0x184C2102L) { // legacy: blocks of compressed size, no end mark
				p += 4;
				while (p + 4 <= in.length && u32(in, p) != 0x184D2204L && u32(in, p) != 0x184C2102L) {
					int sz = (int) u32(in, p);
					p += 4;
					if (sz <= 0 || p + sz > in.length)
						throw new DecodeException("LZ4 legacy: bad block size");
					block(in, p, sz, out);
					p += sz;
				}
				continue;
			}
			if (magic != 0x184D2204L)
				break;
			p += 4;
			int flg = in[p] & 0xFF;
			if ((flg >>> 6) != 1)
				throw new DecodeException("LZ4: unsupported frame version");
			boolean blockChecksum = (flg & 0x10) != 0, contentSize = (flg & 0x08) != 0, contentChecksum = (flg & 0x04) != 0,
					dict = (flg & 0x01) != 0;
			p += 2 + (contentSize ? 8 : 0) + (dict ? 4 : 0) + 1; // FLG, BD, [size], [dict], HC
			while (true) {
				if (p + 4 > in.length)
					throw new DecodeException("LZ4: truncated frame");
				long bs = u32(in, p);
				p += 4;
				if (bs == 0)
					break; // end mark
				boolean raw = (bs & 0x80000000L) != 0;
				int sz = (int) (bs & 0x7FFFFFFF);
				if (p + sz > in.length)
					throw new DecodeException("LZ4: block beyond the end of the data");
				if (raw)
					out.put(in, p, sz);
				else
					block(in, p, sz, out);
				p += sz + (blockChecksum ? 4 : 0);
			}
			if (contentChecksum)
				p += 4;
		}
		return out.toArray();
	}

	/** Apple Compression framework, algorithm COMPRESSION_LZ4 */
	static byte[] apple(byte[] in, long limit) {
		ByteSink out = new ByteSink(in.length * 3, limit);
		try {
			return apple0(in, out);
		} catch (DecodeException | IndexOutOfBoundsException e) {
			throw new DecodeException(e.getMessage(), out.toArray());
		}
	}

	private static byte[] apple0(byte[] in, ByteSink out) {
		int p = 0;
		while (p + 4 <= in.length) {
			if (in[p] != 'b' || in[p + 1] != 'v' || in[p + 2] != '4')
				throw new DecodeException("LZ4 (Apple): unknown block magic at offset " + p);
			char kind = (char) in[p + 3];
			if (kind == '$')
				break;
			if (kind == '-') {
				int n = (int) u32(in, p + 4);
				out.put(in, p + 8, n);
				p += 8 + n;
			} else if (kind == '1') {
				int raw = (int) u32(in, p + 4), comp = (int) u32(in, p + 8);
				int before = out.size;
				if (p + 12 + comp > in.length)
					throw new DecodeException("LZ4 (Apple): block beyond the end of the data");
				block(in, p + 12, comp, out);
				if (out.size - before != raw)
					throw new DecodeException("LZ4 (Apple): block size mismatch");
				p += 12 + comp;
			} else
				throw new DecodeException("LZ4 (Apple): unknown block type " + kind);
		}
		return out.toArray();
	}

	static long u32(byte[] d, int p) {
		if (p + 4 > d.length)
			throw new DecodeException("truncated data");
		return (d[p] & 0xFFL) | (d[p + 1] & 0xFFL) << 8 | (d[p + 2] & 0xFFL) << 16 | (d[p + 3] & 0xFFL) << 24;
	}
}
