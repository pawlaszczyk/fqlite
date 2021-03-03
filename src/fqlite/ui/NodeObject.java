package fqlite.ui;

import javax.swing.JDialog;

import fqlite.base.Job;
import fqlite.types.FileTypes;

/**
 * Represents node information within the tree view.
 * 
 * @author pawlaszc
 *
 */
public class NodeObject{
	
	public String name;
	public Job job;
    int numberOfColumns;	
    JDialog hexview;
    public FileTypes type;  // 0.. db-Node 1.. WAL-Node 2.. Journal-Node
	public DBTable table;
	public int tabletype;  // 0.. Normal Table, 1.. Index Table, 2.. Virtual Table, 3.. View, 4.. Trigger
    
    
	public NodeObject(String name, DBTable table, int numberOfColumns,FileTypes type, int tabletype)
	{
		this.name = name;
		this.numberOfColumns = numberOfColumns;
		this.type = type;
		this.table = table;
		this.tabletype = tabletype;
	}
	
	public String toString() {
		return name;
	}
}