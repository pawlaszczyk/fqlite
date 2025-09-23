package fqlite.pattern;

/**
 * The base interface represents a generic constrain.
 * Such a constraint normally only needs to have a match() method.
 * Constrains are used to cover storage classes and serial types 
 * in SQLite. 
 * 
 * @author pawlaszc
 *
 */
public interface Constraint {

	public boolean match(int value);
	
}
