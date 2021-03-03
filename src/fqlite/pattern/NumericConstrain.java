package fqlite.pattern;

/**
 * An numeric Type could be anything.
 * Accordingly this type always returns true.
 * 
 * @author pawlaszc
 *
 */
public class NumericConstrain implements Constraint{

	@Override
	public boolean match(int value) {
		
		return true;
	}
	
	@Override
	public String toString(){
		return "00..255";
	}
	
}
