package fqlite.pattern;

public class IntegerConstraint implements Constraint {

	int min = 0;  
	int max = 6;
	
	public IntegerConstraint(boolean notNull)
	{
		if (notNull)
			min = 1;
	}
	
	@Override
	public boolean match(int value) {
		
		if ((value <= max  && value >= min) || value == 8 || value == 9)
		{
			return true;
		}
		return false;
	}
	
	@Override
	public String toString(){
		return "0" + min + "..06" + " OR 08|09";
	}


}
