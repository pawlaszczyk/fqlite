package fqlite.ui;

import fqlite.base.Job;
import java.awt.BorderLayout;
import java.awt.Insets;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import javafx.application.Application;
import javafx.scene.control.Dialog;
import javafx.scene.control.TreeItem;
import javax.swing.BorderFactory;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;


/* loaded from: Importer.class */
public class Importer extends JPanel implements PropertyChangeListener {
    static JProgressBar progressBar;
    JTextArea taskOutput;
    JFrame parent;
    static Dialog<String> dialog;

    public Importer() {
        super(new BorderLayout());
        progressBar = new JProgressBar(0, 100);
        progressBar.setStringPainted(true);
        this.taskOutput = new JTextArea(5, 20);
        this.taskOutput.setMargin(new Insets(5, 5, 5, 5));
        this.taskOutput.setEditable(false);
        JPanel panel = new JPanel();
        panel.add(progressBar);
        add(panel, "First");
        add(new JScrollPane(this.taskOutput), "Center");
        setBorder(BorderFactory.createEmptyBorder(20, 20, 20, 20));
    }

    public static void close() {
        dialog.close();
    }

    @Override // java.beans.PropertyChangeListener
    public void propertyChange(PropertyChangeEvent evt) {
        if ("progress" == evt.getPropertyName()) {
            progressBar.setIndeterminate(true);
            progressBar.setValue(10);
            this.taskOutput.append("Import is running...");
        }
    }

    public static void createAndShowGUI(Application parent, String path, Job job, TreeItem<NodeObject> dbNode) {
        ImportDBTask task1 = new ImportDBTask(job, dbNode);
        task1.start();
      //PrepareHexViewTask task2 = new PrepareHexViewTask(job, dbNode);
      //task2.start();
    }
}