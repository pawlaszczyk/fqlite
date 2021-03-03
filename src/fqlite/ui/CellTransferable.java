package fqlite.ui;

import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.Transferable;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.io.IOException;

public class CellTransferable implements Transferable {

    public static final DataFlavor CELL_DATA_FLAVOR = new DataFlavor(Object.class, "FQLite/x-cell-value");

    private Object cellValue;

    public CellTransferable(Object cellValue) {
        this.cellValue = cellValue;
    }

    @Override
    public DataFlavor[] getTransferDataFlavors() {
        return new DataFlavor[]{CELL_DATA_FLAVOR};
    }

    @Override
    public boolean isDataFlavorSupported(DataFlavor flavor) {
        return CELL_DATA_FLAVOR.equals(flavor);
    }

    @Override
    public Object getTransferData(DataFlavor flavor) throws UnsupportedFlavorException, IOException {
        if (!isDataFlavorSupported(flavor)) {
            throw new UnsupportedFlavorException(flavor);
        }
        return cellValue;
    }

}
