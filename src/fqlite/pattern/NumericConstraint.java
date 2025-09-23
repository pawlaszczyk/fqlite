package fqlite.pattern;

/**
 * A numeric Type could be anything.
 * Accordingly, this type always returns true.
 * 
 * @author pawlaszc
 *
 */
public class NumericConstraint implements Constraint{

	@Override
	public boolean match(int value) {
		
		return true;
	}
	
	@Override
	public String toString(){
		return "00..255";
	}
	
}
