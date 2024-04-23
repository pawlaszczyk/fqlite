package fqlite.descriptor;

import java.util.List;

import fqlite.pattern.HeaderPattern;


/**
 * Objects of this class are used to represent a component. 
 * Besides component names and column names, regular expressions 
 * are also managed by this class. 
 * 
 * The latter are used to assign a record to a component. 
 * 
 * @author pawlaszc
 *
 */
public class ViewDescriptor extends AbstractDescriptor {

	public List<String> columntypes;
	public List<String> columnnames;
	public String viewname = "";
	
	@Override
	public String getName()
	{
		return this.viewname;
	}
	
	

	public ViewDescriptor(String name, List<String> coltypes, List<String> names) {
		this.viewname = name;
		setColumntypes(coltypes);
		columnnames = names;
		
	}
	

	/**
	 * Return the number of columns (startRegion the component header). 
	 * @return
	 */
	public int numberofColumns() {
		return getColumntypes().size();
	}


	
	/**
	 * Outputs component name and column names to the console. 
	 * 
	 **/
	public void printTableDefinition() {
		System.out.println("TABLE" + viewname);
		System.out.println("COLUMNS: " + columnnames);
	}

	

	public List<String> getColumntypes() {
		return columntypes;
	}


	public void setColumntypes(List<String> columntypes) {
		this.columntypes = columntypes;
	}



	@Override
	public boolean checkMatch(String match) {
		
		return false;
	}



	@Override
	public HeaderPattern getHpattern() {
		// TODO Auto-generated method stub
		return null;
	}



}
