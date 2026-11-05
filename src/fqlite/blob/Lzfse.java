package fqlite.blob;

/**
 * Decoder for Apple's LZFSE container (Compression framework, also used in APFS and many
 * iOS/macOS databases): blocks "bvx1" (LZFSE, uncompressed header), "bvx2" (LZFSE,
 * compressed header), "bvxn" (LZVN), "bvx-" (stored) and the end mark "bvx$". Ported from the
 * reference implementation (lzfse_decode_base.c, lzvn_decode_base.c, BSD licence).
 */
public final class Lzfse {

	private Lzfse() {
	}

	static boolean isLzfse(byte[] d) {
		return d.length >= 8 && d[0] == 'b' && d[1] == 'v' && d[2] == 'x'
				&& (d[3] == '1' || d[3] == '2' || d[3] == 'n' || d[3] == '-' || d[3] == '$');
	}

	private static final int L_SYMBOLS = 20, M_SYMBOLS = 20, D_SYMBOLS = 64, LIT_SYMBOLS = 256;
	private static final int L_STATES = 64, M_STATES = 64, D_STATES = 256, LIT_STATES = 1024;

	private static final int[] L_BITS = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 5, 8 };
	private static final int[] L_BASE = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 20, 28, 60 };
	private static final int[] M_BITS = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 5, 8, 11 };
	private static final int[] M_BASE = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 24, 56, 312 };
	private static final int[] D_BITS = new int[64];
	private static final int[] D_BASE = new int[64];
	static {
		int base = 0;
		for (int i = 0; i < 64; i++) {
			D_BITS[i] = i < 4 ? 0 : (i - 4) / 4 + 1;
			D_BASE[i] = base;
			base += 1 << D_BITS[i];
		}
	}

	private static final int[] FREQ_NBITS = { 2, 3, 2, 5, 2, 3, 2, 8, 2, 3, 2, 5, 2, 3, 2, 14, 2, 3, 2, 5, 2, 3, 2, 8, 2, 3, 2, 5, 2,
			3, 2, 14 };
	private static final int[] FREQ_VALUE = { 0, 2, 1, 4, 0, 3, 1, -1, 0, 2, 1, 5, 0, 3, 1, -1, 0, 2, 1, 6, 0, 3, 1, -1, 0, 2, 1, 7,
			0, 3, 1, -1 };

	public static byte[] decompress(byte[] in, long limit) {
		ByteSink out = new ByteSink(in.length * 4, limit);
		try {
			return run(in, out);
		} catch (DecodeException | IndexOutOfBoundsException e) {
			throw new DecodeException(e.getMessage(), out.toArray());
		}
	}

	private static byte[] run(byte[] in, ByteSink out) {
		int p = 0;
		while (true) {
			if (p + 4 > in.length)
				throw new DecodeException("LZFSE: missing end of stream");
			if (in[p] != 'b' || in[p + 1] != 'v' || in[p + 2] != 'x')
				throw new DecodeException("LZFSE: unknown block magic at offset " + p);
			char kind = (char) in[p + 3];
			switch (kind) {
			case '$' -> {
				return out.toArray();
			}
			case '-' -> {
				int n = (int) Lz4.u32(in, p + 4);
				out.put(in, p + 8, n);
				p += 8 + n;
			}
			case 'n' -> {
				int raw = (int) Lz4.u32(in, p + 4), pay = (int) Lz4.u32(in, p + 8);
				if (p + 12 + pay > in.length)
					throw new DecodeException("LZVN: block beyond the end of the data");
				int before = out.size;
				lzvn(in, p + 12, p + 12 + pay, out, before + raw);
				p += 12 + pay;
			}
			case '1', '2' -> p = lzfseBlock(in, p, kind == '2', out);
			default -> throw new DecodeException("LZFSE: unknown block type " + kind);
			}
		}
	}

	// ------------------------------------------------------------------ LZFSE block

	private static long u64(byte[] d, int p) {
		return Lz4.u32(d, p) | Lz4.u32(d, p + 4) << 32;
	}

	private static int u16(byte[] d, int p) {
		return (d[p] & 0xFF) | (d[p + 1] & 0xFF) << 8;
	}

	private static int field(long v, int off, int n) {
		return (int) ((v >>> off) & ((1L << n) - 1));
	}

	private static int lzfseBlock(byte[] in, int p, boolean v2, ByteSink out) {
		int nRaw, nLiterals, nMatches, nLitPayload, nLmdPayload, literalBits, lmdBits, lState, mState, dState;
		int[] litState = new int[4];
		int[] lFreq = new int[L_SYMBOLS], mFreq = new int[M_SYMBOLS], dFreq = new int[D_SYMBOLS], litFreq = new int[LIT_SYMBOLS];
		int payload;
		if (v2) {
			nRaw = (int) Lz4.u32(in, p + 4);
			long v0 = u64(in, p + 8), v1 = u64(in, p + 16), w2 = u64(in, p + 24);
			nLiterals = field(v0, 0, 20);
			nLitPayload = field(v0, 20, 20);
			nMatches = field(v0, 40, 20);
			literalBits = field(v0, 60, 3) - 7;
			for (int i = 0; i < 4; i++)
				litState[i] = field(v1, 10 * i, 10);
			nLmdPayload = field(v1, 40, 20);
			lmdBits = field(v1, 60, 3) - 7;
			int headerSize = field(w2, 0, 32);
			lState = field(w2, 32, 10);
			mState = field(w2, 42, 10);
			dState = field(w2, 52, 10);
			int src = p + 32, srcEnd = p + headerSize;
			if (srcEnd > in.length || srcEnd < src)
				throw new DecodeException("LZFSE: bad header size");
			if (srcEnd > src) {
				long accum = 0;
				int nbits = 0;
				int total = L_SYMBOLS + M_SYMBOLS + D_SYMBOLS + LIT_SYMBOLS;
				for (int i = 0; i < total; i++) {
					while (src < srcEnd && nbits + 8 <= 32) {
						accum |= (in[src] & 0xFFL) << nbits;
						nbits += 8;
						src++;
					}
					int b = (int) (accum & 31);
					int n = FREQ_NBITS[b];
					int value;
					if (n == 8)
						value = 8 + (int) ((accum >>> 4) & 0xF);
					else if (n == 14)
						value = 24 + (int) ((accum >>> 4) & 0x3FF);
					else
						value = FREQ_VALUE[b];
					if (n > nbits)
						throw new DecodeException("LZFSE: corrupt frequency tables");
					accum >>>= n;
					nbits -= n;
					if (i < 20)
						lFreq[i] = value;
					else if (i < 40)
						mFreq[i - 20] = value;
					else if (i < 104)
						dFreq[i - 40] = value;
					else
						litFreq[i - 104] = value;
				}
				if (nbits >= 8 || src != srcEnd)
					throw new DecodeException("LZFSE: corrupt frequency tables");
			}
			payload = p + headerSize;
		} else {
			nRaw = (int) Lz4.u32(in, p + 4);
			// n_payload_bytes at +8 is redundant
			nLiterals = (int) Lz4.u32(in, p + 12);
			nMatches = (int) Lz4.u32(in, p + 16);
			nLitPayload = (int) Lz4.u32(in, p + 20);
			nLmdPayload = (int) Lz4.u32(in, p + 24);
			literalBits = (int) Lz4.u32(in, p + 28);
			for (int i = 0; i < 4; i++)
				litState[i] = u16(in, p + 32 + 2 * i);
			lmdBits = (int) Lz4.u32(in, p + 40);
			lState = u16(in, p + 44);
			mState = u16(in, p + 46);
			dState = u16(in, p + 48);
			int q = p + 50;
			for (int i = 0; i < 20; i++)
				lFreq[i] = u16(in, q + 2 * i);
			q += 40;
			for (int i = 0; i < 20; i++)
				mFreq[i] = u16(in, q + 2 * i);
			q += 40;
			for (int i = 0; i < 64; i++)
				dFreq[i] = u16(in, q + 2 * i);
			q += 128;
			for (int i = 0; i < 256; i++)
				litFreq[i] = u16(in, q + 2 * i);
			payload = q + 512;
		}
		if (nLiterals > 4 * 1024 * 1024 || nMatches > 4 * 1024 * 1024 || payload + nLitPayload + nLmdPayload > in.length)
			throw new DecodeException("LZFSE: corrupt block header");

		// literals
		int[] litTable = decoderTable(LIT_STATES, litFreq);
		byte[] literals = new byte[nLiterals + 64];
		InStream lit = new InStream(in, payload, payload + nLitPayload, literalBits);
		int s0 = litState[0], s1 = litState[1], s2 = litState[2], s3 = litState[3];
		for (int i = 0; i < nLiterals; i += 4) {
			lit.flush();
			int e = litTable[check(s0, LIT_STATES)];
			literals[i] = (byte) (e >>> 8);
			s0 = (e >> 16) + (int) lit.pull(e & 0xFF);
			e = litTable[check(s1, LIT_STATES)];
			literals[i + 1] = (byte) (e >>> 8);
			s1 = (e >> 16) + (int) lit.pull(e & 0xFF);
			e = litTable[check(s2, LIT_STATES)];
			literals[i + 2] = (byte) (e >>> 8);
			s2 = (e >> 16) + (int) lit.pull(e & 0xFF);
			e = litTable[check(s3, LIT_STATES)];
			literals[i + 3] = (byte) (e >>> 8);
			s3 = (e >> 16) + (int) lit.pull(e & 0xFF);
		}

		// L, M, D
		long[] lt = valueTable(L_STATES, lFreq, L_BITS, L_BASE);
		long[] mt = valueTable(M_STATES, mFreq, M_BITS, M_BASE);
		long[] dt = valueTable(D_STATES, dFreq, D_BITS, D_BASE);
		int lmdStart = payload + nLitPayload;
		InStream lmd = new InStream(in, lmdStart, lmdStart + nLmdPayload, lmdBits);
		int start = out.size;
		int litPos = 0, d = -1;
		int[] st = { lState, mState, dState };
		for (int i = 0; i < nMatches; i++) {
			lmd.flush();
			int l = value(lt, st, 0, L_STATES, lmd);
			int m = value(mt, st, 1, M_STATES, lmd);
			int nd = value(dt, st, 2, D_STATES, lmd);
			if (nd != 0)
				d = nd;
			if (litPos + l > nLiterals + 64)
				throw new DecodeException("LZFSE: literal run beyond the literals");
			out.put(literals, litPos, l);
			litPos += l;
			if (m > 0)
				out.match(d, m);
		}
		if (out.size - start != nRaw)
			throw new DecodeException("LZFSE: block size mismatch (" + (out.size - start) + " instead of " + nRaw + ")");
		return payload + nLitPayload + nLmdPayload;
	}

	private static int check(int state, int n) {
		if (state < 0 || state >= n)
			throw new DecodeException("LZFSE: invalid FSE state");
		return state;
	}

	/** entry: nbits (bits 0-7), symbol (8-15), delta (16-31, signed) */
	private static int[] decoderTable(int nstates, int[] freq) {
		int[] t = new int[nstates];
		int nclz = Integer.numberOfLeadingZeros(nstates);
		int sum = 0, pos = 0;
		for (int i = 0; i < freq.length; i++) {
			int f = freq[i];
			if (f == 0)
				continue;
			sum += f;
			if (sum > nstates)
				throw new DecodeException("LZFSE: frequency table too large");
			int k = Integer.numberOfLeadingZeros(f) - nclz;
			int j0 = ((2 * nstates) >> k) - f;
			for (int j = 0; j < f; j++) {
				int nb, delta;
				if (j < j0) {
					nb = k;
					delta = ((f + j) << k) - nstates;
				} else {
					nb = k - 1;
					delta = (j - j0) << (k - 1);
				}
				t[pos++] = (nb & 0xFF) | (i & 0xFF) << 8 | delta << 16;
			}
		}
		return t;
	}

	/** entry: total_bits (0-7), value_bits (8-15), delta (16-31 signed), vbase (32-63) */
	private static long[] valueTable(int nstates, int[] freq, int[] vbits, int[] vbase) {
		long[] t = new long[nstates];
		int nclz = Integer.numberOfLeadingZeros(nstates);
		int sum = 0, pos = 0;
		for (int i = 0; i < freq.length; i++) {
			int f = freq[i];
			if (f == 0)
				continue;
			sum += f;
			if (sum > nstates)
				throw new DecodeException("LZFSE: frequency table too large");
			int k = Integer.numberOfLeadingZeros(f) - nclz;
			int j0 = ((2 * nstates) >> k) - f;
			for (int j = 0; j < f; j++) {
				int total, delta;
				if (j < j0) {
					total = k + vbits[i];
					delta = ((f + j) << k) - nstates;
				} else {
					total = k - 1 + vbits[i];
					delta = (j - j0) << (k - 1);
				}
				t[pos++] = (total & 0xFF) | (vbits[i] & 0xFF) << 8 | (long) (delta & 0xFFFF) << 16 | (long) vbase[i] << 32;
			}
		}
		return t;
	}

	private static int value(long[] t, int[] st, int which, int n, InStream in) {
		long e = t[check(st[which], n)];
		int total = (int) (e & 0xFF), vb = (int) ((e >>> 8) & 0xFF);
		int delta = (short) ((e >>> 16) & 0xFFFF);
		int vbase = (int) (e >>> 32);
		long sv = in.pull(total);
		st[which] = delta + (int) (sv >>> vb);
		return vbase + (int) (sv & ((1L << vb) - 1));
	}

	/** backward FSE bit stream (64-bit accumulator as in fse_in_checked_*64) */
	private static final class InStream {
		final byte[] d;
		final int start;
		int p; // current read position (moves towards start)
		long accum;
		int nbits;

		InStream(byte[] d, int start, int end, int n) {
			this.d = d;
			this.start = start;
			if (n != 0) {
				p = end - 8;
				if (p < start)
					throw new DecodeException("LZFSE: bit stream too short");
				accum = load(p, 8);
				nbits = n + 64;
			} else {
				p = end - 7;
				if (p < start)
					throw new DecodeException("LZFSE: bit stream too short");
				accum = load(p, 7);
				nbits = 56;
			}
			if (nbits < 56 || nbits > 63)
				throw new DecodeException("LZFSE: invalid bit stream start");
		}

		private long load(int at, int n) {
			long v = 0;
			for (int i = 0; i < n; i++)
				if (at + i >= start)
					v |= (d[at + i] & 0xFFL) << (8 * i);
			return v;
		}

		void flush() {
			int n = (63 - nbits) & ~7;
			if (n == 0)
				return;
			int q = p - (n >>> 3);
			// the reference decoder may read up to 8 bytes before the stream (bits that are
			// never used); these bytes are taken as zero here
			if (q < start - 8)
				throw new DecodeException("LZFSE: bit stream overrun");
			p = q;
			long incoming = load(q, n >>> 3);
			accum = (accum << n) | incoming;
			nbits += n;
		}

		long pull(int n) {
			if (n == 0)
				return 0;
			nbits -= n;
			if (nbits < 0)
				throw new DecodeException("LZFSE: bit stream underrun");
			long r = accum >>> nbits;
			accum &= (1L << nbits) - 1;
			return r;
		}
	}

	// ------------------------------------------------------------------ LZVN

	static void lzvn(byte[] in, int p, int end, ByteSink out, int expectedEnd) {
		int d = 0;
		while (p < end) {
			int opc = in[p] & 0xFF;
			int l = 0, m = 0;
			if (opc == 0x06)
				break; // end of stream
			if (opc == 0x0E || opc == 0x16) { // nop
				p++;
				continue;
			}
			if (opc >= 0xF0) { // match with previous distance
				if (opc == 0xF0) {
					m = (in[p + 1] & 0xFF) + 16;
					p += 2;
				} else {
					m = opc & 0x0F;
					p++;
				}
			} else if (opc >= 0xE0) { // literals only
				if (opc == 0xE0) {
					l = (in[p + 1] & 0xFF) + 16;
					p += 2;
				} else {
					l = opc & 0x0F;
					p++;
				}
			} else if (opc >= 0xA0 && opc < 0xC0) { // med_d
				int c = (in[p + 1] & 0xFF) | (in[p + 2] & 0xFF) << 8;
				l = (opc >>> 3) & 3;
				m = (((opc & 7) << 2) | (c & 3)) + 3;
				d = c >>> 2;
				p += 3;
			} else if (opc >= 0x70 && opc < 0x80) {
				throw new DecodeException("LZVN: undefined opcode 0x" + Integer.toHexString(opc));
			} else {
				int low = opc & 7;
				l = opc >>> 6;
				m = ((opc >>> 3) & 7) + 3;
				if (low == 7) { // lrg_d
					d = (in[p + 1] & 0xFF) | (in[p + 2] & 0xFF) << 8;
					p += 3;
				} else if (low == 6) { // pre_d
					if (opc < 0x40)
						throw new DecodeException("LZVN: undefined opcode 0x" + Integer.toHexString(opc));
					p += 1;
				} else { // sml_d
					d = (low << 8) | (in[p + 1] & 0xFF);
					p += 2;
				}
			}
			if (l > 0) {
				if (p + l > end)
					throw new DecodeException("LZVN: literals beyond the end of the block");
				out.put(in, p, l);
				p += l;
			}
			if (m > 0)
				out.match(d, m);
		}
		if (out.size != expectedEnd)
			throw new DecodeException("LZVN: block size mismatch");
	}
}
