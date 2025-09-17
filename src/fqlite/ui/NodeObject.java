package fqlite.ui;

import java.io.File;

import fqlite.base.Job;
import fqlite.types.FileTypes;
import javafx.scene.layout.VBox;

/**
 * Represents node information within the tree view.
 * 
 * @author pawlaszc
 *
 */
public class NodeObject{
	
	public boolean hasData = false;
	public String name;
	public File   filename;
	public Job job;
    int numberOfColumns;	
    public FileTypes type;  // 0.. db-Node 1.. WAL-Node 2.. Journal-Node
	public VBox tablePane; //TableView<List<Object>> table;
	public int tabletype;  // 0.. Normal Table, 1.. Index Table, 2.. Virtual Table, 3.. View, 4.. Trigger
    public boolean isRoot = false;
    public boolean isTable = false;

    public NodeObject(String name, boolean isRoot)
    {
    	this.name = name;
    	this.isRoot = isRoot;
    }
	
	
	public NodeObject(String name, VBox tablePane, int numberOfColumns,FileTypes type, int tabletype, boolean isTable)
	{
		this.name = name;
		this.numberOfColumns = numberOfColumns;
		this.type = type;
		this.tablePane = tablePane;
		this.tabletype = tabletype;
        this.isTable = isTable;
	}
	
	public String toString() {
		return name;
	}
}