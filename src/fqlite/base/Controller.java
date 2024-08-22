package fqlite.base;

import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.Button;

public class Controller {

 @FXML private Button meinButton;
	
	 
	 public void buttonAction(ActionEvent event){
		 
		 System.out.println(" Jetzt hat der Nutzer auf den Button geklickt");
	 };
	
}
