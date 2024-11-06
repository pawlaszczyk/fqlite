package fqlite.ui.hexviewer;

/**
 * This class is used to prepare the hex-offset printout at the beginning of each
 * hex-line.  
 * @author pawlaszc
 *
 */
public class OffsetFormatter extends goryachev.fxtexteditor.ALineNumberFormatter {

	@Override
	public String formatLineNumber(int lineNumber) {

		long offset = ((lineNumber-1)*16);
		return "" + offset;
	}

}
