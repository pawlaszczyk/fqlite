package fqlite.blob;

/** a BLOB could not be decoded (damaged, truncated or not in the assumed format) */
public class DecodeException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/** what could be decoded before the error (may be null) */
	public final byte[] partial;

	public DecodeException(String msg) {
		this(msg, null);
	}

	public DecodeException(String msg, byte[] partial) {
		super(msg);
		this.partial = partial;
	}
}
