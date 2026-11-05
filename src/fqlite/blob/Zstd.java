package fqlite.blob;

/**
 * Zstandard decoder (RFC 8878) in plain Java, without native code. Supports all block types,
 * Huffman-coded literals (1 and 4 streams, tree reuse), FSE/RLE/predefined/repeat sequence
 * tables, repeat offsets, several and skippable frames. Dictionaries are not supported;
 * checksums are not verified.
 */
public final class Zstd {

	private Zstd() {
	}

	static boolean isZstd(byte[] d) {
		return d.length >= 6 && (d[0] & 0xFF) == 0x28 && (d[1] & 0xFF) == 0xB5 && (d[2] & 0xFF) == 0x2F && (d[3] & 0xFF) == 0xFD;
	}

	// ------------------------------------------------------------------ constants (RFC 8878)

	private static final int[] LL_BASE = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 20, 22, 24, 28, 32, 40, 48,
			64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536 };
	private static final int[] LL_BITS = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 3, 3, 4, 6, 7, 8, 9, 10,
			11, 12, 13, 14, 15, 16 };
	private static final int[] ML_BASE = new int[53];
	private static final int[] ML_BITS = new int[53];
	static {
		for (int i = 0; i < 32; i++)
			ML_BASE[i] = i + 3;
		int[] base = { 35, 37, 39, 41, 43, 47, 51, 59, 67, 83, 99, 131, 259, 515, 1027, 2051, 4099, 8195, 16387, 32771, 65539 };
		int[] bits = { 1, 1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
		for (int i = 0; i < base.length; i++) {
			ML_BASE[32 + i] = base[i];
			ML_BITS[32 + i] = bits[i];
		}
	}
	private static final short[] LL_DEFAULT = { 4, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 2, 1, 1,
			1, 1, 1, -1, -1, -1, -1 };
	private static final short[] ML_DEFAULT = { 1, 4, 3, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
			1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1, -1, -1, -1, -1, -1, -1 };
	private static final short[] OF_DEFAULT = { 1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1, -1, -1,
			-1, -1 };

	// ------------------------------------------------------------------ bit readers

	/** backward bit stream as used for Huffman, FSE and sequence data */
	static final class BackBits {
		final byte[] d;
		final int start;
		final int len;
		long pos; // bits left (may become negative when reading past the beginning)

		BackBits(byte[] d, int start, int len) {
			if (len <= 0)
				throw new DecodeException("zstd: empty bit stream");
			this.d = d;
			this.start = start;
			this.len = len;
			int last = d[start + len - 1] & 0xFF;
			if (last == 0)
				throw new DecodeException("zstd: bit stream without end mark");
			pos = (long) (len - 1) * 8 + (31 - Integer.numberOfLeadingZeros(last));
		}

		int read(int n) {
			if (n == 0)
				return 0;
			pos -= n;
			return (int) bits(pos, n);
		}

		private long bits(long from, int n) {
			if (from < 0) {
				int neg = (int) -from;
				if (neg >= n)
					return 0;
				return bits(0, n - neg) << neg;
			}
			int bi = (int) (from >>> 3), off = (int) (from & 7);
			long w = 0;
			for (int k = 0; k < 8 && bi + k < len; k++)
				w |= (d[start + bi + k] & 0xFFL) << (8 * k);
			return (w >>> off) & ((1L << n) - 1);
		}
	}

	// ------------------------------------------------------------------ FSE

	static final class Fse {
		int log;
		int[] sym, bits, base;

		static Fse rle(int symbol) {
			Fse f = new Fse();
			f.log = 0;
			f.sym = new int[] { symbol };
			f.bits = new int[] { 0 };
			f.base = new int[] { 0 };
			return f;
		}

		static Fse build(short[] norm, int nsym, int log) {
			int size = 1 << log;
			Fse f = new Fse();
			f.log = log;
			f.sym = new int[size];
			f.bits = new int[size];
			f.base = new int[size];
			int high = size - 1;
			int[] next = new int[nsym];
			for (int s = 0; s < nsym; s++) {
				if (norm[s] == -1) {
					f.sym[high--] = s;
					next[s] = 1;
				} else
					next[s] = norm[s];
			}
			int step = (size >>> 1) + (size >>> 3) + 3, mask = size - 1, pos = 0;
			for (int s = 0; s < nsym; s++) {
				for (int i = 0; i < norm[s]; i++) {
					f.sym[pos] = s;
					do
						pos = (pos + step) & mask;
					while (pos > high);
				}
			}
			if (pos != 0)
				throw new DecodeException("zstd: corrupt FSE distribution");
			for (int u = 0; u < size; u++) {
				int s = f.sym[u];
				int ns = next[s]++;
				int nb = log - (31 - Integer.numberOfLeadingZeros(ns));
				f.bits[u] = nb;
				f.base[u] = (ns << nb) - size;
			}
			return f;
		}

		/** reads a table description; returns the number of bytes used */
		static int read(byte[] d, int p, int end, int maxSym, int maxLog, Fse[] out) {
			long bitPos = 0;
			int log = (int) (peek(d, p, end, bitPos) & 0xF) + 5;
			if (log > maxLog)
				throw new DecodeException("zstd: FSE accuracy log too large");
			bitPos += 4;
			int remaining = (1 << log) + 1, threshold = 1 << log, nbBits = log + 1, sym = 0;
			boolean prev0 = false;
			short[] norm = new short[maxSym + 1];
			while (remaining > 1 && sym <= maxSym) {
				if (prev0) {
					int n0 = sym;
					while ((peek(d, p, end, bitPos) & 0xFFFF) == 0xFFFF) {
						n0 += 24;
						bitPos += 16;
					}
					while ((peek(d, p, end, bitPos) & 3) == 3) {
						n0 += 3;
						bitPos += 2;
					}
					n0 += (int) (peek(d, p, end, bitPos) & 3);
					bitPos += 2;
					if (n0 > maxSym + 1)
						throw new DecodeException("zstd: FSE distribution too long");
					sym = n0;
					if (sym > maxSym)
						break;
				}
				long bs = peek(d, p, end, bitPos);
				int max = (2 * threshold - 1) - remaining;
				int count;
				if ((bs & (threshold - 1)) < max) {
					count = (int) (bs & (threshold - 1));
					bitPos += nbBits - 1;
				} else {
					count = (int) (bs & (2 * threshold - 1));
					if (count >= threshold)
						count -= max;
					bitPos += nbBits;
				}
				count--;
				remaining -= Math.abs(count);
				norm[sym++] = (short) count;
				prev0 = count == 0;
				while (remaining < threshold) {
					nbBits--;
					threshold >>>= 1;
				}
			}
			if (remaining != 1)
				throw new DecodeException("zstd: corrupt FSE distribution");
			out[0] = build(norm, sym, log);
			return (int) ((bitPos + 7) >>> 3);
		}

		private static long peek(byte[] d, int p, int end, long bitPos) {
			int bi = p + (int) (bitPos >>> 3), off = (int) (bitPos & 7);
			long w = 0;
			for (int k = 0; k < 5 && bi + k < end; k++)
				w |= (d[bi + k] & 0xFFL) << (8 * k);
			return (w >>> off) & 0xFFFFFFFFL;
		}
	}

	// ------------------------------------------------------------------ Huffman

	static final class Huf {
		int maxBits;
		byte[] sym;
		byte[] bits;

		/** reads a tree description; returns bytes used */
		static int read(byte[] d, int p, int end, Huf[] out) {
			if (p >= end)
				throw new DecodeException("zstd: missing Huffman tree");
			int header = d[p] & 0xFF;
			int[] w = new int[256];
			int n, used;
			if (header >= 128) {
				n = header - 127;
				used = 1 + (n + 1) / 2;
				if (p + used > end)
					throw new DecodeException("zstd: truncated Huffman weights");
				for (int i = 0; i < n; i++) {
					int b = d[p + 1 + i / 2] & 0xFF;
					w[i] = (i % 2 == 0) ? b >>> 4 : b & 15;
				}
			} else {
				used = 1 + header;
				if (p + used > end)
					throw new DecodeException("zstd: truncated Huffman weights");
				Fse[] t = new Fse[1];
				int tl = Fse.read(d, p + 1, p + 1 + header, 255, 6, t);
				Fse f = t[0];
				BackBits in = new BackBits(d, p + 1 + tl, header - tl);
				int s1 = in.read(f.log), s2 = in.read(f.log);
				n = 0;
				while (true) {
					if (n > 254)
						throw new DecodeException("zstd: too many Huffman weights");
					w[n++] = f.sym[s1];
					s1 = f.base[s1] + in.read(f.bits[s1]);
					if (in.pos < 0) {
						w[n++] = f.sym[s2];
						break;
					}
					w[n++] = f.sym[s2];
					s2 = f.base[s2] + in.read(f.bits[s2]);
					if (in.pos < 0) {
						w[n++] = f.sym[s1];
						break;
					}
				}
			}
			long total = 0;
			for (int i = 0; i < n; i++) {
				if (w[i] > 11)
					throw new DecodeException("zstd: Huffman weight too large");
				if (w[i] > 0)
					total += 1L << (w[i] - 1);
			}
			if (total == 0)
				throw new DecodeException("zstd: empty Huffman tree");
			int maxBits = 64 - Long.numberOfLeadingZeros(total); // highbit + 1
			long rest = (1L << maxBits) - total;
			if (Long.bitCount(rest) != 1)
				throw new DecodeException("zstd: corrupt Huffman weights");
			w[n++] = 64 - Long.numberOfLeadingZeros(rest); // log2(rest) + 1
			if (maxBits > 11)
				throw new DecodeException("zstd: Huffman code too long");
			Huf h = new Huf();
			h.maxBits = maxBits;
			int size = 1 << maxBits;
			h.sym = new byte[size];
			h.bits = new byte[size];
			int[] count = new int[maxBits + 2];
			int[] nb = new int[n];
			for (int i = 0; i < n; i++) {
				nb[i] = w[i] > 0 ? maxBits + 1 - w[i] : 0;
				count[nb[i]]++;
			}
			int[] idx = new int[maxBits + 2];
			idx[maxBits] = 0;
			for (int b = maxBits; b >= 1; b--)
				idx[b - 1] = idx[b] + count[b] * (1 << (maxBits - b));
			for (int i = 0; i < n; i++) {
				if (nb[i] == 0)
					continue;
				int code = idx[nb[i]], len = 1 << (maxBits - nb[i]);
				for (int k = 0; k < len; k++) {
					h.sym[code + k] = (byte) i;
					h.bits[code + k] = (byte) nb[i];
				}
				idx[nb[i]] += len;
			}
			out[0] = h;
			return used;
		}

		void stream(byte[] d, int p, int len, byte[] dst, int dp, int count) {
			BackBits in = new BackBits(d, p, len);
			int mask = (1 << maxBits) - 1;
			int state = in.read(maxBits);
			for (int i = 0; i < count; i++) {
				dst[dp + i] = sym[state];
				int b = bits[state];
				state = ((state << b) | in.read(b)) & mask;
			}
			if (in.pos < -maxBits)
				throw new DecodeException("zstd: Huffman stream overrun");
		}
	}

	// ------------------------------------------------------------------ frame state

	private static final class State {
		Huf huf;
		Fse ll, of, ml;
		final int[] rep = { 1, 4, 8 };
	}

	public static byte[] decompress(byte[] in, long limit) {
		ByteSink out = new ByteSink(in.length * 4, limit);
		int p = 0;
		boolean any = false;
		try {
		while (p + 4 <= in.length) {
			long magic = Lz4.u32(in, p);
			if ((magic & 0xFFFFFFF0L) == 0x184D2A50L) {
				p += 8 + (int) Lz4.u32(in, p + 4);
				continue;
			}
			if (magic != 0xFD2FB528L) {
				if (!any)
					throw new DecodeException("zstd: no frame magic");
				break; // trailing garbage after the last frame
			}
			p = frame(in, p + 4, out);
			any = true;
		}
		} catch (DecodeException | IndexOutOfBoundsException e) {
			throw new DecodeException(e.getMessage(), out.toArray());
		}
		return out.toArray();
	}

	private static int frame(byte[] in, int p, ByteSink out) {
		int fhd = u8(in, p++);
		int fcsFlag = fhd >>> 6;
		boolean single = (fhd & 0x20) != 0, checksum = (fhd & 0x04) != 0;
		int dictFlag = fhd & 3;
		if ((fhd & 0x08) != 0)
			throw new DecodeException("zstd: reserved bit set");
		if (!single)
			p++; // window descriptor - the whole output stays in memory anyway
		int dictLen = new int[] { 0, 1, 2, 4 }[dictFlag];
		long dictId = 0;
		for (int i = 0; i < dictLen; i++)
			dictId |= (long) u8(in, p + i) << (8 * i);
		p += dictLen;
		if (dictId != 0)
			throw new DecodeException("zstd: frame needs dictionary " + dictId + " (not available)");
		int fcsLen = fcsFlag == 0 ? (single ? 1 : 0) : new int[] { 0, 2, 4, 8 }[fcsFlag];
		p += fcsLen;
		State st = new State();
		int frameStart = out.size;
		while (true) {
			if (p + 3 > in.length)
				throw new DecodeException("zstd: truncated block header");
			int bh = u8(in, p) | u8(in, p + 1) << 8 | u8(in, p + 2) << 16;
			p += 3;
			boolean last = (bh & 1) != 0;
			int type = (bh >>> 1) & 3, size = bh >>> 3;
			switch (type) {
			case 0 -> {
				if (p + size > in.length)
					throw new DecodeException("zstd: truncated raw block");
				out.put(in, p, size);
				p += size;
			}
			case 1 -> {
				out.fill(u8(in, p), size);
				p += 1;
			}
			case 2 -> {
				if (p + size > in.length)
					throw new DecodeException("zstd: truncated compressed block");
				compressed(in, p, p + size, out, st, frameStart);
				p += size;
			}
			default -> throw new DecodeException("zstd: reserved block type");
			}
			if (last)
				break;
		}
		if (checksum)
			p += 4;
		return p;
	}

	private static void compressed(byte[] in, int p, int end, ByteSink out, State st, int frameStart) {
		// ---- literals section
		int b0 = u8(in, p);
		int ltype = b0 & 3, sf = (b0 >>> 2) & 3;
		byte[] lit;
		int regen;
		if (ltype == 0 || ltype == 1) {
			int hl;
			if ((sf & 1) == 0) {
				regen = b0 >>> 3;
				hl = 1;
			} else if (sf == 1) {
				regen = (b0 >>> 4) | u8(in, p + 1) << 4;
				hl = 2;
			} else {
				regen = (b0 >>> 4) | u8(in, p + 1) << 4 | u8(in, p + 2) << 12;
				hl = 3;
			}
			p += hl;
			lit = new byte[regen];
			if (ltype == 0) {
				if (p + regen > end)
					throw new DecodeException("zstd: truncated raw literals");
				System.arraycopy(in, p, lit, 0, regen);
				p += regen;
			} else {
				java.util.Arrays.fill(lit, (byte) u8(in, p));
				p += 1;
			}
		} else {
			int comp, hl;
			boolean four = sf != 0;
			if (sf <= 1) {
				long c = u8(in, p) | u8(in, p + 1) << 8 | (long) u8(in, p + 2) << 16;
				regen = (int) ((c >>> 4) & 0x3FF);
				comp = (int) ((c >>> 14) & 0x3FF);
				hl = 3;
			} else if (sf == 2) {
				long c = u8(in, p) | u8(in, p + 1) << 8 | (long) u8(in, p + 2) << 16 | (long) u8(in, p + 3) << 24;
				regen = (int) ((c >>> 4) & 0x3FFF);
				comp = (int) ((c >>> 18) & 0x3FFF);
				hl = 4;
			} else {
				long c = u8(in, p) | u8(in, p + 1) << 8 | (long) u8(in, p + 2) << 16 | (long) u8(in, p + 3) << 24
						| (long) u8(in, p + 4) << 32;
				regen = (int) ((c >>> 4) & 0x3FFFF);
				comp = (int) ((c >>> 22) & 0x3FFFF);
				hl = 5;
			}
			p += hl;
			if (p + comp > end)
				throw new DecodeException("zstd: truncated Huffman literals");
			int q = p, qend = p + comp;
			if (ltype == 2) {
				Huf[] h = new Huf[1];
				q += Huf.read(in, q, qend, h);
				st.huf = h[0];
			} else if (st.huf == null)
				throw new DecodeException("zstd: treeless literals without a previous Huffman tree");
			lit = new byte[regen];
			if (!four)
				st.huf.stream(in, q, qend - q, lit, 0, regen);
			else {
				int s1 = u8(in, q) | u8(in, q + 1) << 8, s2 = u8(in, q + 2) | u8(in, q + 3) << 8, s3 = u8(in, q + 4) | u8(in, q + 5) << 8;
				q += 6;
				int s4 = qend - q - s1 - s2 - s3;
				if (s4 <= 0)
					throw new DecodeException("zstd: corrupt jump table");
				int seg = (regen + 3) / 4;
				st.huf.stream(in, q, s1, lit, 0, seg);
				st.huf.stream(in, q + s1, s2, lit, seg, seg);
				st.huf.stream(in, q + s1 + s2, s3, lit, 2 * seg, seg);
				st.huf.stream(in, q + s1 + s2 + s3, s4, lit, 3 * seg, regen - 3 * seg);
			}
			p = qend;
		}

		// ---- sequences section
		if (p >= end) { // no sequences
			out.put(lit, 0, lit.length);
			return;
		}
		int nseq = u8(in, p++);
		if (nseq >= 128) {
			if (nseq < 255)
				nseq = ((nseq - 128) << 8) + u8(in, p++);
			else {
				nseq = u8(in, p) + (u8(in, p + 1) << 8) + 0x7F00;
				p += 2;
			}
		}
		if (nseq == 0) {
			out.put(lit, 0, lit.length);
			return;
		}
		int modes = u8(in, p++);
		Fse[] t = new Fse[1];
		p = table(in, p, end, (modes >>> 6) & 3, 35, 9, LL_DEFAULT, 6, t, st.ll);
		st.ll = t[0];
		p = table(in, p, end, (modes >>> 4) & 3, 31, 8, OF_DEFAULT, 5, t, st.of);
		st.of = t[0];
		p = table(in, p, end, (modes >>> 2) & 3, 52, 9, ML_DEFAULT, 6, t, st.ml);
		st.ml = t[0];

		BackBits bb = new BackBits(in, p, end - p);
		Fse ll = st.ll, of = st.of, ml = st.ml;
		int sl = bb.read(ll.log), so = bb.read(of.log), sm = bb.read(ml.log);
		int litPos = 0;
		int[] rep = st.rep;
		for (int i = 0; i < nseq; i++) {
			int ofCode = of.sym[so], mlCode = ml.sym[sm], llCode = ll.sym[sl];
			if (ofCode > 31 || mlCode > 52 || llCode > 35)
				throw new DecodeException("zstd: invalid sequence code");
			long ofv = (1L << ofCode) + bb.read(ofCode > 30 ? 0 : ofCode);
			if (ofCode > 30)
				throw new DecodeException("zstd: offset code too large");
			int mlen = ML_BASE[mlCode] + bb.read(ML_BITS[mlCode]);
			int llen = LL_BASE[llCode] + bb.read(LL_BITS[llCode]);
			int offset;
			if (ofv > 3) {
				offset = (int) (ofv - 3);
				rep[2] = rep[1];
				rep[1] = rep[0];
				rep[0] = offset;
			} else {
				int idx = (int) ofv - 1;
				if (llen == 0)
					idx++;
				if (idx == 0)
					offset = rep[0];
				else {
					offset = idx == 3 ? rep[0] - 1 : rep[idx];
					if (idx > 1)
						rep[2] = rep[1];
					rep[1] = rep[0];
					rep[0] = offset;
				}
			}
			if (litPos + llen > lit.length)
				throw new DecodeException("zstd: literal length beyond the literals");
			out.put(lit, litPos, llen);
			litPos += llen;
			if (offset > out.size - frameStart)
				throw new DecodeException("zstd: match offset before the start of the frame");
			out.match(offset, mlen);
			if (i + 1 < nseq) {
				sl = ll.base[sl] + bb.read(ll.bits[sl]);
				sm = ml.base[sm] + bb.read(ml.bits[sm]);
				so = of.base[so] + bb.read(of.bits[so]);
			}
		}
		if (bb.pos != 0)
			throw new DecodeException("zstd: sequence bit stream not fully consumed");
		out.put(lit, litPos, lit.length - litPos);
	}

	private static int table(byte[] in, int p, int end, int mode, int maxSym, int maxLog, short[] def, int defLog, Fse[] t, Fse prev) {
		switch (mode) {
		case 0 -> t[0] = Fse.build(def, def.length, defLog);
		case 1 -> {
			int s = u8(in, p++);
			if (s > maxSym)
				throw new DecodeException("zstd: RLE symbol out of range");
			t[0] = Fse.rle(s);
		}
		case 2 -> p += Fse.read(in, p, end, maxSym, maxLog, t);
		default -> {
			if (prev == null)
				throw new DecodeException("zstd: repeat mode without a previous table");
			t[0] = prev;
		}
		}
		return p;
	}

	private static int u8(byte[] d, int p) {
		if (p >= d.length)
			throw new DecodeException("zstd: truncated data");
		return d[p] & 0xFF;
	}
}
