package fqlite.ui;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.border.EtchedBorder;

/*
---------------
Statusbar.java
---------------
(C) Copyright 2020.

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
 * Used to show up a simple status bar on the bottom. 
 */
@SuppressWarnings("serial")
public class Statusbar extends JPanel implements Runnable {

	private JLabel msg = new JLabel();
	private JProgressBar progress = new JProgressBar();
	private String statusText;

	/**
	 * The constructor.
	 * 
	 */
	public Statusbar() {
		progress.setMinimum(0);
		progress.setMaximum(100);
		progress.setMinimumSize(new Dimension(100, 20));
		progress.setSize(new Dimension(100, 20));

		msg.setMinimumSize(new Dimension(300, 20));
		msg.setSize(new Dimension(300, 20));
		msg.setFont(new Font("Dialog", Font.PLAIN, 10));
		msg.setForeground(Color.black);

		setLayout(new BorderLayout());
		setBorder(new EtchedBorder(EtchedBorder.LOWERED));
		add(msg, BorderLayout.CENTER);
		add(progress, BorderLayout.EAST);
	}

	/**
	 * @param s
	 *            the status string to show
	 */
	public void showStatus(String s) {
		msg.setText(s);
		paintImmediately(getBounds());
	}

	/**
	 * @param percent
	 *            the percentage of the progress bar to be shown
	 */
	public void showProgress(int percent) {
		progress.setValue(percent);
	}

	/**
	 * @param delataPercent
	 *            an increment for the progrss bar
	 */
	public void incProgress(int delataPercent) {
		progress.setValue(progress.getValue() + delataPercent);
	}

	/**
	 * @param s
	 *            the status bar text
	 * @param work
	 *            the work that has to be done, i.e. the maximum value for the
	 *            progress
	 */
	public synchronized void doFakeProgress(String s, int work) {
		statusText = s;
		showStatus(statusText + "... not implemented yet ...");
		progress.setMaximum(work);
		progress.setValue(0);
		Thread t = new Thread(this);
		t.start();
	}

	/**
	 * @see java.lang.Runnable#run()
	 */
	public synchronized void run() {
		int work = progress.getMaximum();
		for (int i = 0; i < work; i++) {
			progress.setValue(i);
			repaint();
			try {
				wait(10);
			} catch (Exception ex) {
			}
		}
		showStatus(statusText + "... done.");
		repaint();
		try {
			wait(1000);
		} catch (Exception ex) {
		}
		progress.setValue(0);
		showStatus("");
		repaint();
	}

}
