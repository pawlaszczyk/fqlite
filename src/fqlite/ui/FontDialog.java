package fqlite.ui;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Properties;
import java.util.function.Predicate;
import fqlite.base.GUI;
import fqlite.base.Global;
import javafx.application.Platform;
import javafx.beans.binding.DoubleBinding;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.transformation.FilteredList;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.DialogPane;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Priority;
import javafx.scene.layout.RowConstraints;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.scene.shape.Rectangle;
import javafx.scene.text.Font;
import javafx.scene.text.FontPosture;
import javafx.scene.text.FontWeight;
import javafx.scene.text.Text;
import javafx.stage.Stage;
import javafx.util.Callback;

/**
 * This class realizes a font-selection dialog window.
 * 
 * You can change the application font with this class.
 * 
 */
public class FontDialog extends javafx.scene.control.Dialog<Font> {
    
    private FontPanel fontPanel;
    static  Font defaultFont;
    static  javafx.scene.Node root;
     
    public FontDialog(Font defaultFont, javafx.scene.Node rootelement) {
        
    	fontPanel = new FontPanel();
        root = rootelement;
        FontDialog.defaultFont = defaultFont;
                
        final DialogPane dialogPane = getDialogPane();
        
        setTitle("Select font");
        dialogPane.setHeaderText("Select font");

        String s = GUI.class.getResource("/icon_checked.png").toExternalForm();		
        dialogPane.setGraphic(new ImageView(new Image(s)));
       
        ButtonBar buttonBar = new ButtonBar();
        buttonBar.setPadding( new Insets(10) );

        Button applyButton = new Button("Apply");
        Button cancelButton = new Button("Cancel");
        Button defaultButton = new Button("Default");
        
        defaultButton.setOnAction(e -> {
        	fontPanel.setFont(Font.getDefault());
        });
        
        applyButton.setOnAction(e -> { 
        	fontPanel.applyFont();
        	Stage stage = (Stage) applyButton.getScene().getWindow();
            // do what you have to do
            stage.close();
        	
        });
        
        cancelButton.setOnAction( new EventHandler<ActionEvent>() {
            @Override
            public void handle(final ActionEvent e) {
            	// get a handle to the stage
                Stage stage = (Stage) applyButton.getScene().getWindow();
                // do what you have to do
                stage.close();
            }
        });

        
        buttonBar.getButtons().addAll(defaultButton, applyButton, cancelButton);

    	fontPanel.setFont(defaultFont);    
        VBox vb = new VBox();
        fontPanel.getChildren().add(buttonBar);
        vb.getChildren().add(fontPanel);
        vb.getChildren().add(buttonBar);
        dialogPane.setContent(vb);
    }
    

    
    /**************************************************************************
     * 
     * Support classes
     * 
     **************************************************************************/

  

    private static class FontPanel extends GridPane {
        private static final double HGAP = 10;
        private static final double VGAP = 5;

        private static final Predicate<Object> MATCH_ALL = new Predicate<Object>() {
            @Override public boolean test(Object t) {
                return true;
            }
        };

        private static final Double[] fontSizes = new Double[] {8d,9d,10d,11d,12d,13d,14d,15d,16d,18d,20d};
        private final FilteredList<String> filteredFontList = new FilteredList<>(FXCollections.observableArrayList(Font.getFamilies()), MATCH_ALL);
        private final FilteredList<Double> filteredSizeList = new FilteredList<>(FXCollections.observableArrayList(fontSizes), MATCH_ALL);
        private final ListView<String> fontListView = new ListView<String>(filteredFontList);
        private final ListView<Double> sizeListView = new ListView<Double>(filteredSizeList);
        private final Text sample = new Text("\ud83d\udc3b" + " What is bear + 1 ? " + "\ud83d\udc3c");

        public FontPanel() {
            setHgap(HGAP);
            setVgap(VGAP);
            setPrefSize(350, 300);
            setMinSize(350, 300);

            ColumnConstraints c0 = new ColumnConstraints();
            c0.setPercentWidth(60);
            ColumnConstraints c1 = new ColumnConstraints();
            c1.setPercentWidth(25);
            ColumnConstraints c2 = new ColumnConstraints();
            c2.setPercentWidth(15);
            getColumnConstraints().addAll(c0, c1);

            RowConstraints r0 = new RowConstraints();
            r0.setVgrow(Priority.NEVER);
            RowConstraints r1 = new RowConstraints();
            r1.setVgrow(Priority.NEVER);
            RowConstraints r2 = new RowConstraints();
            r2.setFillHeight(true);
            r2.setVgrow(Priority.NEVER);
            RowConstraints r3 = new RowConstraints();
            r3.setPrefHeight(250);
            r3.setVgrow(Priority.NEVER);
            getRowConstraints().addAll(r0, r1, r2, r3);

            add(new Label("Font"), 0, 0);
            add(fontListView, 0, 1);
            fontListView.setCellFactory(new Callback<ListView<String>, ListCell<String>>() {
                @Override public ListCell<String> call(ListView<String> listview) {
                    return new ListCell<String>() {
               
                    	@Override protected void updateItem(String family, boolean empty) {
                            super.updateItem(family, empty);

                            
                            if (!empty) {
                                setFont(Font.font(family));
                                setText(family);
                            } else {
                                setText(null);
                            }
                        }
                    };
                }
            });
            
          

            ChangeListener<Object> sampleRefreshListener = new ChangeListener<Object>() {
                @Override public void changed(ObservableValue<? extends Object> arg0, Object arg1, Object arg2) {
                    refreshSample();
                }
            };

            fontListView.selectionModelProperty().get().selectedItemProperty().addListener( new ChangeListener<String>() {

                @Override public void changed(ObservableValue<? extends String> arg0, String arg1, String arg2) {
                    refreshSample();
                }});

       
            add( new Label("Size"), 1, 0);
            add(sizeListView, 1, 1);
            sizeListView.selectionModelProperty().get().selectedItemProperty().addListener(sampleRefreshListener);

            final double height = 45;
            final DoubleBinding sampleWidth = new DoubleBinding() {
                {
                    bind(fontListView.widthProperty(), sizeListView.widthProperty());    
                }

                @Override protected double computeValue() {
                    return fontListView.getWidth() + sizeListView.getWidth() + 1 * HGAP;
                    
                }
            };
            StackPane sampleStack = new StackPane(sample);
            sampleStack.setAlignment(Pos.CENTER);
            sampleStack.setMinHeight(height);
            sampleStack.setPrefHeight(height);
            sampleStack.setMaxHeight(height);
            sampleStack.prefWidthProperty().bind(sampleWidth);
            Rectangle clip = new Rectangle(0, height);
            clip.widthProperty().bind(sampleWidth);
            sampleStack.setClip(clip);
            add(sampleStack, 0, 2, 1, 3);
        }
        
        
        public void applyFont(){
        	
        	
        	Font f = Font.font(
                     listSelection(fontListView),
                     listSelection(sizeListView));
          
        	
     	    Global.font_name = listSelection(fontListView);
            Global.font_size = String.valueOf(listSelection(sizeListView));
            Global.font_style = f.getStyle();
            System.out.println(">>" + Global.font_name);
            System.out.println(">>" + Global.font_size);
          
            File baseDir = new File(System.getProperty("user.home"), ".fqlite");
        	String path = baseDir.getAbsolutePath()+ File.separator + "fqlite.conf";
            
        	Properties appProps = new Properties();
    		try {
    			
    			appProps.load(new FileInputStream(path));
    	        appProps.setProperty("font_name",Global.font_name);
    	        appProps.setProperty("font_size",Global.font_size);
    	        
    	        appProps.store(new FileOutputStream(path), null);
    	        
    	        Alert alert = new Alert(AlertType.CONFIRMATION);
    			alert.setTitle("Information");
    			alert.setContentText("Restart of FQLite is require for changes to take effect ");
    			alert.showAndWait();		

    		} catch (Exception err) {
    		
    		}
    		
    		
        }

        public void setFont(final Font font) {
        	
            final Font _font = font == null ? Font.getDefault() : font;
            if (_font != null) {
                selectInList(fontListView,  _font.getFamily());
                selectInList(sizeListView,  _font.getSize());
                
                System.out.println(_font.getSize());
            }
        }

        

        private void refreshSample() {
            sample.setStyle("-fx-font: "+ listSelection(sizeListView)  +" \""+ listSelection(fontListView) + "\"; ");
 
        }

        @SuppressWarnings("unused")
		private <T> void selectInList( final ListView<T> listView, final T selection ) {
            Platform.runLater(new Runnable() {
                @Override public void run() {
                    listView.scrollTo(selection);
                    listView.getSelectionModel().select(selection);
                }
            });
        }

        private <T> T listSelection(final ListView<T> listView) {
            return listView.selectionModelProperty().get().getSelectedItem();
        }
    }  
}