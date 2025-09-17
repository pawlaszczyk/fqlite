package fqlite.ui;

import fqlite.base.GUI;
import fqlite.base.Job;
import fqlite.base.RollbackJournalReader;
import fqlite.base.WALReader;
import fqlite.log.AppLog;
import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.scene.control.TreeItem;
import javafx.scene.image.ImageView;

import java.util.Objects;

public class ImportDBTask {
  Job job;
  
  TreeItem<NodeObject> dbNode;
  
  public ImportDBTask(Job job, TreeItem<NodeObject> dbNode) {
    this.job = job;
    this.dbNode = dbNode;
  }
  
  public void start() {
    Task<Integer> task = new Task<Integer>() {
        public Integer call() throws Exception {
          ImportDBTask.this.job.start();
          return null;
        }
        
        public void succeeded() {
          super.succeeded();
          AppLog.info("DB Import Done!");
          System.out.println("DB Import Done!");
          Platform.runLater(new Runnable() {
                public void run() {
                  // database-small-icon.png
                  String s = Objects.requireNonNull(GUI.class.getResource("/green_database24.png")).toExternalForm();
                  ImageView iv = new ImageView(s);
                  job.dbNode.setGraphic(iv);
                  job.updatePropertyPanel();
           //     }
           //   });
          if (GUI.doesRollbackJournalExist(ImportDBTask.this.job.path) > 0L) {
            String rjpath = String.valueOf(ImportDBTask.this.job.path) + "-journal";
            RollbackJournalReader rol = new RollbackJournalReader(rjpath, ImportDBTask.this.job);
            rol.ps = ImportDBTask.this.job.ps;
            rol.parse();
            rol.output();
            String s2 = Objects.requireNonNull(GUI.class.getResource("/green_journal24.png")).toExternalForm();
            ImageView iv2 = new ImageView(s2);
            if (ImportDBTask.this.job.rjNode != null)
              ImportDBTask.this.job.rjNode.setGraphic(iv2); 
            ImportDBTask.this.job.rol = rol;
            ImportDBTask.this.job.updateRollbackPanel();
          } 
          if (GUI.doesWALFileExist(ImportDBTask.this.job.path) > 0L) {
            String walpath = String.valueOf(ImportDBTask.this.job.path) + "-wal";
            WALReader wal = new WALReader(walpath, ImportDBTask.this.job);
            ImportDBTask.this.job.wal = wal;
            wal.parse();
            wal.output();
            String s3 = GUI.class.getResource("/green_archive24.png").toExternalForm();
            ImageView iv3 = new ImageView(s3);
            if (ImportDBTask.this.job.walNode != null)
              ImportDBTask.this.job.walNode.setGraphic(iv3); 
            ImportDBTask.this.job.updateWALPanel();
          } 
          
               }
          });
         
          //Toolkit.getDefaultToolkit().beep();
        }
        
        public void cancelled() {
          super.cancelled();
          updateMessage("Cancelled!");
        }
        
        public void failed() {
          super.failed();
          updateMessage("Failed!");
        }
      };
    Thread th = new Thread(task);
    th.setDaemon(true);
    th.start();
  }
}