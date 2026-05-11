package fqlite.viewer.ui;

import fqlite.viewer.model.ProtoField;
import javafx.scene.control.TreeCell;

/**
 * Custom TreeCell that color-codes fields by wire type and
 * visually marks nested messages.
 */
public class ProtoFieldCell extends TreeCell<ProtoField> {

    @Override
    protected void updateItem(ProtoField field, boolean empty) {
        super.updateItem(field, empty);

        if (empty || field == null) {
            setText(null);
            setStyle(null);
            setGraphic(null);
            return;
        }

        setText(field.treeLabel());
        setStyle(styleFor(field));
    }

    private String styleFor(ProtoField f) {
        String color = switch (f.getWireType()) {
            case ProtoField.WIRE_VARINT -> "#7ec8e3";  // blue
            case ProtoField.WIRE_64BIT  -> "#f0c060";  // gold
            case ProtoField.WIRE_LEN    -> f.isNestedMessage()
                    ? "#a0e8a0"   // green for nested messages
                    : "#e0a0d0";  // pink for strings/bytes
            case ProtoField.WIRE_32BIT  -> "#f0a060";  // orange
            default                     -> "#c0c0c0";
        };
        return "-fx-text-fill: " + color + ";";
    }
}
