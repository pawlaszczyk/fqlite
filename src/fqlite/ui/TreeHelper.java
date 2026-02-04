package fqlite.ui;

import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;
import java.util.ArrayList;
import java.util.List;

public class TreeHelper {

    /**
     * Extracts all Nodes from the first level.
     *
     * @param treeView the TreeView
     * @param <T> type of tree-elements
     * @return List with values for the first level nodes
     */
    public static <T> List<T> getFirstLevelNodes(TreeView<T> treeView) {
        List<T> firstLevelNodes = new ArrayList<>();

        TreeItem<T> root = treeView.getRoot();
        if (root != null) {
            for (TreeItem<T> child : root.getChildren()) {
                firstLevelNodes.add(child.getValue());
            }
        }

        return firstLevelNodes;
    }

    /**
     * Exctract all TreeItem-Objects for the database tree (first level only) and return a list.
     *
     * @param treeView the TreeView
     * @param <T> The type of tree elements
     * @return List with the tree items of the first level.
     */
    public static <T> List<TreeItem<T>> getFirstLevelTreeItems(TreeView<T> treeView) {
        List<TreeItem<T>> firstLevelItems = new ArrayList<>();

        TreeItem<T> root = treeView.getRoot();
        if (root != null) {
            firstLevelItems.addAll(root.getChildren());
        }

        return firstLevelItems;
    }


}