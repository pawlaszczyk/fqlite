package fqlite.pattern;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper-class representing a list of constraints.
 * This class is used for matching a byte sequence 
 * against a list of constraints.
 * 
 * Every serial type of record header, including the header length byte, is represented
 * with a Constraint object. 
 * 
 * @author pawlaszc
 *
 */
public class HeaderPattern {
	
	public List<Constraint> pattern = new ArrayList<Constraint>();

	/**
	 *  Do nothing. Default-Constructor.
	 */
	public HeaderPattern()
	{
		// nothing to do
	}
	
	public Constraint get(int idx)
	{
		return pattern.get(idx);
	}
	
	
	public void change2RowID(int idx)
	{
		pattern.set(idx, new ZeroConstraint());
	}
	
	public void add(Constraint c)
	{
		pattern.add(c);
	}
	
	public int size()
	{
		return pattern.size();
	}
	
	/**
	 *  add a "00" zero column constraint to the constraint list.
	 */
	public void addZeroConstraint() 
	{
		pattern.add(new ZeroConstraint());
	}

	/**
	 *  add a "mix...max" header constraint to the constraint list.
	 */
	public void addHeaderConstraint(int min, int max) 
	{
		pattern.add(new HeaderConstraint(min,max));
		
	}
	
	/**
	 *  add an integer column constraint to the constraint list.
	 *  A value between 00..06.
	 */
	public void addIntegerConstraint() 
	{
		pattern.add(new IntegerConstraint(false));
	}
	
	
	/**
	 *  add a string column constraint to the constraint list.
	 *  A value >= 13;  
	 **/
	public void addStringConstraint() 
	{
		pattern.add(new StringConstraint());
	}
	
	
	/**
	 *  add a string column constraint to the constraint list
	 *  with a max-value (i.e. varchar(32) -> max-value = 32*2+13)
	 **/
	public void addStringConstraint(int maxlength) 
	{
		pattern.add(new StringConstraint(maxlength));
	}
	
	
	public void addNumericConstraint() 
	{
		pattern.add(new NumericConstraint());
	}
	
	/**
	 *  add a BLOB constraint for a BLOB column.
	 */
	public void addBLOBConstraint() 
	{
		pattern.add(new BLOBConstraint());
	}
	
	/**
	 *  add a floating-point constraint.
	 */
	public void addFloatingConstraint()
	{
		pattern.add(new FloatingConstraint());
	}
	
	
	/**
	 * As the name says. 
	 */
	public String toString() 
	{
		String result = "[";
		
		for (Constraint c: pattern)
		{
			result += c.toString() + "|";
		}
		
		return result += "]";
		
	}
	
}
