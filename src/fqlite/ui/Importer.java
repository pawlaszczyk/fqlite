package fqlite.ui;

import fqlite.base.Job;
import javafx.application.Application;
import javafx.scene.control.TreeItem;

public class Importer{
   
  public static void createAndShowGUI(Application parent, String path, Job job, TreeItem<NodeObject> dbNode) {
    ImportDBTask task1 = new ImportDBTask(job, dbNode);
    task1.start();
  }
}
