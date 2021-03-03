package fqlite.ui;

import java.util.Collections;
import java.util.Comparator;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.MutableTreeNode;

public class FTreeNode extends DefaultMutableTreeNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9003951729811371555L;

	public FTreeNode(Object userObject) {
		super(userObject);
	}

	@Override
	public void add(MutableTreeNode newChild) {
		super.add(newChild);
		sort();// add to tree and sort immediately use in case the model is small if large
				// comment it and and call node.sort once you've added all the children
	}

	@SuppressWarnings("unchecked")
	public void sort() {
		Collections.sort(children, compare());
	}

	private Comparator<DefaultMutableTreeNode> compare() {
		return new Comparator<DefaultMutableTreeNode>() {
			@Override
			public int compare(DefaultMutableTreeNode o1, DefaultMutableTreeNode o2) {
				return o1.getUserObject().toString().compareTo(o2.getUserObject().toString());
			}

		};

	}
}