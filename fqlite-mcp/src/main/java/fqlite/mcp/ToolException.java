package fqlite.mcp;

/**
 *  Error of a tool call - reported to the model as a tool result with isError=true
 *
 *  @author pawlaszc
 */
final class ToolException extends Exception {

	private static final long serialVersionUID = 1L;

	ToolException(String msg) {
		super(msg);
	}
}
