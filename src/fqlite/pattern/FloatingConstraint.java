package fqlite.pattern;

public class FloatingConstraint implements Constraint {

	@Override
	public boolean match(int value) {
	
		if (value == 7)
			return true;
		else 
			return false;
	}
	
	@Override
	public String toString(){
		return "07";
	}

}
