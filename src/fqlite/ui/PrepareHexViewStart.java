package fqlite.ui;

import fqlite.base.Job;
import javafx.concurrent.Task;
import javafx.scene.control.TreeItem;

class PrepareHexViewTask {
    Job job;
    TreeItem<NodeObject> dbNode;

    public PrepareHexViewTask(Job job, TreeItem<NodeObject> dbNode) {
        this.job = job;
        this.dbNode = dbNode;
    }

    public void start() {
        Task<Integer> task = new Task<Integer>() { // from class: fqlite.ui.PrepareHexViewTask.1
            /* JADX INFO: Access modifiers changed from: protected */
            /* JADX WARN: Can't rename method to resolve collision */
            @Override // javafx.concurrent.Task
            public Integer call() throws Exception {
                System.out.println(" hex Dialog geladen ");
                return null;
            }

            /* JADX INFO: Access modifiers changed from: protected */
            @Override // javafx.concurrent.Task
            public void succeeded() {
                super.succeeded();
                System.out.println("Done HexViewImport!");
            }

            /* JADX INFO: Access modifiers changed from: protected */
            @Override // javafx.concurrent.Task
            public void cancelled() {
                super.cancelled();
                updateMessage("Creation of HexView Cancelled!");
            }

            /* JADX INFO: Access modifiers changed from: protected */
            @Override // javafx.concurrent.Task
            public void failed() {
                super.failed();
                updateMessage("Creation of HexView Failed!");
            }
        };
        Thread th = new Thread(task);
        th.setDaemon(true);
        th.start();
        System.out.println("Thread started");
    }
}
