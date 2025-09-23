package fqlite.pattern;


/**
 * Represents an empty column type.
 * 
 * @author pawlaszc
 *
 */
public class ZeroConstraint implements Constraint{

	@Override
	public boolean match(int value) {
		
		if (value == 0)
			return true;
		else 
			return false;
	}
	
	@Override
	public String toString(){
		return "00";
	}


}
