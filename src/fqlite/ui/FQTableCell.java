package fqlite.ui;

import javafx.scene.control.TableCell;
import javafx.scene.image.ImageView;

public class FQTableCell <T> extends TableCell<T, Boolean>{
   
	private final ImageView imageView;
    
    public FQTableCell() {
        imageView = new ImageView();
        setGraphic(imageView);   
    }

    @Override
    protected void updateItem(Boolean item, boolean empty) {
        super.updateItem(item, empty);

        if (empty || item == null) {
            imageView.setImage(null);
        } 
        else {
            
        }
    }
}