package fqlite.base;

import com.dlsc.pdfviewfx.PDFView;
import fr.brouillard.oss.cssfx.CSSFX;
import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import java.io.File;

public class PDFPreviewer extends Application {

	 	//private FileChooser chooser;
	    private String filename;
	    private PDFView pdfView = new PDFView();
	 	
	    public PDFPreviewer(String filename){
	    	this.filename = filename;
	 	    pdfView.load(new File(this.filename));
	    }
	 
	   
	    @Override
	    public void start(Stage primaryStage) {
	    
//	        MenuItem loadItem = new MenuItem("Load PDF...");
//	        loadItem.setAccelerator(KeyCombination.valueOf("SHORTCUT+o"));
//	        loadItem.setOnAction(evt -> {
//	            if (chooser == null) {
//	                chooser = new FileChooser();
//	                chooser.setTitle("Load PDF File");
//	                final ExtensionFilter filter = new ExtensionFilter("PDF Files", "*.pdf");
//	                chooser.getExtensionFilters().add(filter);
//	                chooser.setSelectedExtensionFilter(filter);
//	            }
//
//	            final File file = chooser.showOpenDialog(pdfView.getScene().getWindow());
//	            if (file != null) {
//	                pdfView.load(file);
//	            }
//	        });

	        //try {
	            //pdfView.load(getClass().getResourceAsStream("/tesla3-owners-manual-short.pdf"));
	        //} catch (Exception e) {
	        //    e.printStackTrace();
	        //}

//	        MenuItem closeItem = new MenuItem("Close PDF");
//	        closeItem.setAccelerator(KeyCombination.valueOf("SHORTCUT+c"));
//	        closeItem.setOnAction(evt -> pdfView.unload());
//	        closeItem.disableProperty().bind(Bindings.isNull(pdfView.documentProperty()));
//
//	        MenuItem printItem = new MenuItem("Print PDF...");
//	        printItem.setAccelerator(KeyCombination.valueOf("SHORTCUT+p"));
//	        printItem.setOnAction(evt -> {
//	            SwingUtilities.invokeLater(() -> {
//	                PDFView.Document pdfDoc = pdfView.getDocument();
//	                if (pdfDoc != null) {
//	                    PrinterJob job = PrinterJob.getPrinterJob();
//	                    job.setPageable(pdfDoc.getPageable());
//	                    if (job.printDialog()) {
//	                        try {
//	                            job.print();
//	                        } catch (PrinterException e) {
//	                            e.printStackTrace();
//	                        }
//	                    }
//	                }
//	            });
//	        });
//	        printItem.disableProperty().bind(Bindings.isNull(pdfView.documentProperty()));

//	        Menu fileMenu = new Menu("File");
//	        ObservableList<MenuItem> fileMenuItems = fileMenu.getItems();
//	        fileMenuItems.add(loadItem);
//	        fileMenuItems.add(closeItem);
//	        fileMenuItems.add(new SeparatorMenuItem());
//	        fileMenuItems.add(printItem);
//
//	        MenuBar menuBar = new MenuBar(fileMenu);
//	        menuBar.setUseSystemMenuBar(false);

	        VBox.setVgrow(pdfView, Priority.ALWAYS);
//	        VBox box = new VBox(menuBar, pdfView);
	        VBox box = new VBox(pdfView);
	    	
	        box.setFillWidth(true);

	        Scene scene = new Scene(box);

	        CSSFX.start(primaryStage);

	        primaryStage.setTitle("PDF Preview");
	        primaryStage.setWidth(800);
	        primaryStage.setHeight(600);
	        primaryStage.setScene(scene);
	        primaryStage.centerOnScreen();
	        primaryStage.show();
	    }
}
	
	

