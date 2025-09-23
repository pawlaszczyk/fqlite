package fqlite.pattern;

public class HeaderConstraint implements Constraint {

	int min = 1;
	int max = Integer.MAX_VALUE;
	
	public HeaderConstraint(int length)
	{
		max = min = length;
	}
	
	public HeaderConstraint(int min, int max)
	{
		this.min = min;
		this.max = max;
	}
	
	@Override
	public boolean match(int value) {
		
		if (value >= min && value <=max)
			return true;
		
		return false;
	}
	
	@Override
	public String toString(){
		StringBuffer s = new StringBuffer();
		if (min < 10) s.append("0");
	    s.append(min+"..");
		if (max < 10) s.append("0");
		s.append(max);
	    
		return s.toString();
	}

}
