package fqlite.base;

/**
 * Represents a not yet checked (slack or uncharted) area within a data page.  
 * Objects of this class are used as a marker for uncharted areas.
 * 
 * @author pawlaszc
 *
 */
public class Gap {
	int from;
	int to;

	Gap(int fromIdx, int toIdx) {
		from = fromIdx;
		to = toIdx;
	}

}
