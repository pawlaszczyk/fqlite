package fqlite.ui;

import java.util.List;

import fqlite.base.Job;
import javafx.collections.ObservableList;
import javafx.scene.control.TableView;

public class FQTableView<T> extends TableView<T>
{
	public String dbname;
    public Job job;
    public String tablename;
	public List<String> columns;
	public List<String> columntypes;
	
	
    public FQTableView(String tablename, String dbname, Job job, List<String> columns, List<String> columntypes){
	   super();
	   this.dbname = dbname;
	   this.job = job;
	   this.tablename = tablename;
	   this.columns = columns;
	   this.columntypes = columntypes;
	}
	
	
	public FQTableView(String tablename, ObservableList<T> items, String dbname, Job job){
	  super(items);	
	  this.dbname = dbname;
	  this.job = job;
	  this.tablename = tablename;
	}
	
	
	
}
