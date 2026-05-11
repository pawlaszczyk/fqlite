package fqlite.viewer.ui;

import fqlite.viewer.model.BPListNode;
import javafx.scene.control.TreeCell;

/**
 * TreeCell for BPListNode – color-codes by node type.
 */
public class BPListNodeCell extends TreeCell<BPListNode> {

    @Override
    protected void updateItem(BPListNode node, boolean empty) {
        super.updateItem(node, empty);

        if (empty || node == null) {
            setText(null);
            setStyle(null);
            return;
        }

        setText(node.treeLabel());
        setStyle("-fx-text-fill: " + node.colorHex() + ";");
    }
}
