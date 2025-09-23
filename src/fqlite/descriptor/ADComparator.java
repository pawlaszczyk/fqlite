package fqlite.descriptor;

import java.util.Comparator;
import java.util.List;

public class ADComparator implements Comparator<AbstractDescriptor> {

	/**
	 * Compares its two arguments for order. Returns a negative integer, 
	 * zero, or a positive integer as the first
	 * argument is less than, equal to, or greater than the second.
	 */
	@Override
	public int compare(AbstractDescriptor o1, AbstractDescriptor o2) {
		
		List<String> l1 = o1.serialtypes;
		List<String> l2 = o2.serialtypes;
		int first = 0;
		int second = 0;

		if(l1 == null || l2 == null) {
			System.out.println("NULL values");
			return 0;
		}	

		if (l1.isEmpty() || l2.isEmpty())
			return 0;
		
		if(l1.get(0).equals("BLOB") || l1.get(0).equals("TEXT"))
			first += 20;
	     
		if(l2.get(0).equals("BLOB") || l1.get(0).equals("TEXT"))
			first -= 20;
		
		/* compare number of columns -> the highest column number wins*/
		if(l1.size() < l2.size())
			first +=1;
		if(l1.size() > l2.size())
			second +=1;

        return Integer.compare(first, second);

    }

}
