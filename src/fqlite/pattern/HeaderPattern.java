package fqlite.pattern;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper-class representing a list of constraints.
 * This class is used for matching a byte sequence 
 * against a list of constraints.
 * 
 * Every serial type of a record header including 
 * the header length byte is represented
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
		pattern.set(idx, new ZeroConstrain());
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
	 *  add a "00" zero column constrain to the constrain list.
	 */
	public void addZeroConstraint() 
	{
		pattern.add(new ZeroConstrain());
	}

	/**
	 *  add a "mix..max" header constrain to the constrain list.
	 */
	public void addHeaderConstraint(int min, int max) 
	{
		pattern.add(new HeaderConstrain(min,max));
		
	}
	
	/**
	 *  add a integer column constrain to the constrain list.
	 *  A value between 00..06.
	 */
	public void addIntegerConstraint() 
	{
		pattern.add(new IntegerConstraint(false));
	}
	
	
	/**
	 *  add a string column constrain to the constrain list.
	 *  A value >= 13;  
	 **/
	public void addStringConstraint() 
	{
		pattern.add(new StringConstrain());
	}
	
	
	/**
	 *  add a string column constrain to the constrain list
	 *  with a max-value (i.e. varchar(32) -> max-value = 32*2+13)
	 **/
	public void addStringConstraint(int maxlength) 
	{
		pattern.add(new StringConstrain(maxlength));
	}
	
	
	public void addNumericConstraint() 
	{
		pattern.add(new NumericConstrain());
	}
	
	/**
	 *  add a BLOB constrain for a BLOB column.
	 */
	public void addBLOBConstraint() 
	{
		pattern.add(new BLOBConstrain());
	}
	
	/**
	 *  add a floating point constrain.
	 */
	public void addFloatingConstraint()
	{
		pattern.add(new FloatingConstrain());
	}
	
	
	/**
	 * As the name says. 
	 */
	public String toString() 
	{
		String result = "[";
		
		for (Constraint c : pattern)
		{
			result += c.toString() + "|";
		}
		
		return result += "]";
		
	}
	
}
