package fqlite.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import fqlite.base.GUI;
import fqlite.base.Job;

/*
---------------
UnitTest.java
---------------
(C) Copyright 2020.

Original Author:  Dirk Pawlaszczyk
Contributor(s):   -;


Project Info:  http://www.hs-mittweida.de

This program is  software: you can redistribute it and/or modify
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
 * This class is used to perform a series of tests automatically. For this
 * purpose a file with test cases is read in. Afterwards all database files
 * listed in it are read in one after the other. In addition, a checksum can be
 * compared to detect errors.
 * 
 * This is a internal test class. 
 * 
 * @author pawlaszc
 *
 */
public class UnitTest {

	/**
	 * This method is used to read in the test cases startRegion file testcases.txt.
	 * 
	 * @return
	 */
	private static List<TestCase> loadTestCases() {
		List<TestCase> list = new ArrayList<TestCase>();

		try {

			URL url = GUI.class.getResource("/testcase.txt");
			BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));

			String line = in.readLine();

			while (line != null) {
				line = line.trim();
				if ((line.length() == 0) || (line.startsWith("#"))) {
					line = in.readLine();
					continue;
				}

				String[] entry = line.split(":");
				String filename = entry[0];
				String checksum = entry[1];
				if (null != filename && null != checksum) {
					TestCase c = new TestCase(filename, Integer.parseInt(checksum));
					list.add(c);
				}
				line = in.readLine();
			}

			in.close();

		} catch (IOException err) {
			System.err.println(" Could not open testcases.txt: " + err);
		}

		return list;
	}

	/**
	 * Use this method to start a test run.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		List<TestCase> cases = loadTestCases();
		Iterator<TestCase> it = cases.iterator();

		while (it.hasNext()) {

			TestCase next = it.next();
			Job job = new Job();
			System.out.println("Start analysing database " + next.file);
			int checksum = job.run(next.file);

			System.out.println("checksum" + checksum);
			System.out.println("**************************************************************");

		}
	}

}

class TestCase {

	String file;
	int checksum;

	public TestCase(String file, int checksum) {
		this.file = file;
		this.checksum = checksum;
	}

}
