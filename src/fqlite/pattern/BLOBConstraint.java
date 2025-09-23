package fqlite.pattern;

public class BLOBConstraint implements Constraint {

	@Override
	public boolean match(int value) {
		
		if ((value % 2 == 0) && (value > 12))  // OR 0 ?
			return true;
		return false;
	}
	
	@Override
	public String toString(){
		return ">=12";
	}

}
