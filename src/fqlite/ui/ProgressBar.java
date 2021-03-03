package fqlite.ui;
import java.awt.BorderLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Random;
import javax.swing.BorderFactory;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingWorker;

import fqlite.base.Job;

/*
---------------
ProgressBar.java
---------------
(C) Copyright 2015.

Original Author:  Dirk Pawlaszczyk
Contributor(s):   -;


Project Info:  http://www.hs-mittweida.de

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

Dieses Programm ist Freie Software: Sie können es unter den Bedingungen
der GNU General Public License, wie von der Free Software Foundation,
Version 3 der Lizenz oder (nach Ihrer Wahl) jeder neueren
veröffentlichten Version, weiterverbreiten und/oder modifizieren.

Dieses Programm wird in der Hoffnung, dass es nützlich sein wird, aber
OHNE JEDE GEWÄHRLEISTUNG, bereitgestellt; sogar ohne die implizite
Gewährleistung der MARKTFÄHIGKEIT oder EIGNUNG FÜR EINEN BESTIMMTEN ZWECK.
Siehe die GNU General Public License für weitere Details.

Sie sollten eine Kopie der GNU General Public License zusammen mit diesem
Programm erhalten haben. Wenn nicht, siehe <http://www.gnu.org/licenses/>.

*/


/**
 * Display a progress bar during import.
 * Only for internal use.
 * 
 * @author Dirk Pawlaszczyk
 *
 */
@SuppressWarnings("serial")
public class ProgressBar extends JPanel implements PropertyChangeListener {

	static JProgressBar progressBar;
	JTextArea taskOutput;
	JFrame parent;
	static JDialog dialog;

	/**
	 * Constructor. 
	 * Set up the ProgressBar.
	 */
	public ProgressBar() {
		super(new BorderLayout());

		progressBar = new JProgressBar(0, 100);
		progressBar.setStringPainted(true);

		taskOutput = new JTextArea(5, 20);
		taskOutput.setMargin(new Insets(5, 5, 5, 5));
		taskOutput.setEditable(false);

		JPanel panel = new JPanel();

		panel.add(progressBar);

		add(panel, BorderLayout.PAGE_START);
		add(new JScrollPane(taskOutput), BorderLayout.CENTER);
		setBorder(BorderFactory.createEmptyBorder(20, 20, 20, 20));
	}

	/**
	 * Hide dialog.
	 */
	public static void close() {
		dialog.setVisible(false);
	}

	/**
	 * Invoked when task's progress property changes.
	 */
	public void propertyChange(PropertyChangeEvent evt) {
		if ("progress" == evt.getPropertyName()) {
			// int progress = (Integer) evt.getNewValue();
			progressBar.setIndeterminate(true);
			progressBar.setValue(10);
			// taskOutput.append(String.format("Completed %d%% of task.\n",progress));
			taskOutput.append("Import is running...");
		}
	}

	/**
	 * Create a dialog component to show up progress.
	 * @param parent
	 * @param path
	 */
	public static void createAndShowGUI(JFrame parent, String path, Job job) {
		
		// Create and set up the window.
		dialog = new JDialog(parent);
		dialog.setAlwaysOnTop(true);

		// Create and set up the content pane.
		ProgressBar newContentPane = new ProgressBar();
		newContentPane.setOpaque(true); // content panes must be opaque
		dialog.setContentPane(newContentPane);
		Task task1 = new Task(job);
		task1.addPropertyChangeListener(newContentPane);
		task1.execute();
		// Display the window.
		dialog.pack();
		dialog.setLocationRelativeTo(parent);
		dialog.setVisible(true);

	}

}

class Task extends SwingWorker<Void, Void> {

	Job job;
	public Task(Job job)
	{
		super();
		this.job = job;
	}
	
	/*
	 * Main task. Executed in background thread.
	 */
	@Override
	public Void doInBackground() {

		Random random = new Random();
		int progress = 0;
		// Initialize progress property.
		setProgress(0);

		while (progress < 100) {
			progress += random.nextInt(10);
			setProgress(Math.min(progress, 100));
		}

		// start with import
		job.start();
	
		ProgressBar.progressBar.setValue(100);
		ProgressBar.close();
		return null;
	}

	/*
	 * Executed in event dispatch thread
	 */
	public void done() {
		
		Toolkit.getDefaultToolkit().beep();
		job.updatePropertyPanel();
		
		if (job.readWAL)
		{
			job.updateWALPanel();
		}
			
		
	}

}