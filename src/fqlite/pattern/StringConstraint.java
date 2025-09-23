package fqlite.pattern;

public class StringConstraint implements Constraint {

	int max = Integer.MAX_VALUE;
	
	public StringConstraint()
	{}
	
	public StringConstraint(int max)
	{
		this.max = max*2 + 13;
	}
	
	@Override
	public boolean match(int value) {

		/* Note: If the column is empty, then its value is set to 0. */
		if (value == 0)
				return true;
		if (value > 13 && value % 2 != 0 && value <= max)
				return true;
		return false;
	}
	
	@Override
	public String toString(){
		return ">=13";
	}
	

}
