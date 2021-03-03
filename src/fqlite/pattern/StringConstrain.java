package fqlite.pattern;

public class StringConstrain implements Constraint {

	int max = Integer.MAX_VALUE;
	
	public StringConstrain()
	{}
	
	public StringConstrain(int max)
	{
		this.max = max*2 + 13;
	}
	
	@Override
	public boolean match(int value) {

		if (value > 13 && value % 2 != 0 && value <= max)
				return true;
		return false;
	}
	
	@Override
	public String toString(){
		return ">=13";
	}
	

}
