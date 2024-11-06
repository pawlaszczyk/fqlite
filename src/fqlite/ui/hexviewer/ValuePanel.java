package fqlite.ui.hexviewer;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import goryachev.fxtexteditor.FxTextEditor;
import goryachev.fxtexteditor.Marker;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ContentDisplay;
import javafx.scene.control.Label;
import javafx.scene.control.RadioButton;
import javafx.scene.control.Separator;
import javafx.scene.control.TextField;
import javafx.scene.control.ToggleGroup;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.HBox;

public class ValuePanel extends FlowPane {

	public static final int UBYTE_MAX_VALUE = 255;
	public static final int SWORD_MIN_VALUE = -32768;
	public static final int SWORD_MAX_VALUE = 32767;
	public static final int UWORD_MAX_VALUE = 65535;
	public static final long UINT_MAX_VALUE = 4294967295l;
	public static final BigInteger ULONG_MAX_VALUE = new BigInteger("4294967295");
	public static final BigInteger BIG_INTEGER_BYTE_MASK = BigInteger.valueOf(255);
	public static final String VALUE_OUT_OF_RANGE = "Value is out of range";
	public static int CACHE_SIZE = 16;
    private int dataPosition;
	
	private byte[] valuesCache = new byte[CACHE_SIZE];
	private final ByteBuffer byteBuffer = ByteBuffer.wrap(valuesCache);
	private final ValuesUpdater valuesUpdater = new ValuesUpdater();

	// Variables declaration - do not modify//GEN-BEGIN:variables
	private RadioButton bigEndianRadioButton;
	private CheckBox binaryCheckBox0;
	private CheckBox binaryCheckBox1;
	private CheckBox binaryCheckBox2;
	private CheckBox binaryCheckBox3;
	private CheckBox binaryCheckBox4;
	private CheckBox binaryCheckBox5;
	private CheckBox binaryCheckBox6;
	private CheckBox binaryCheckBox7;
	private Label binaryLabel;
	private Label byteLabel;
	private TextField byteTextField;
	private Label characterLabel;
	private TextField characterTextField;
	private Label doubleLabel;
	private TextField doubleTextField;
	private ToggleGroup endianButtonGroup;
	private Label floatLabel;
	private TextField floatTextField;
	private Label intLabel;
	private TextField intTextField;
	private ToggleGroup integerSignButtonGroup;
	private Separator jSeparator1;
	private RadioButton littleEndianRadioButton;
	private Label longLabel;
	private TextField longTextField;
	private RadioButton signedRadioButton;
	private Label stringLabel;
	private TextField stringTextField;
	private RadioButton unsignedRadioButton;
	private Label wordLabel;
	private TextField wordTextField;
    private FxTextEditor ed;
	
	public ValuePanel(FxTextEditor ed) {
		initComponents();
		this.ed = ed;
		
		ed.selectionProperty().addListener(e ->  { 
		     updateValues();
		     
	    });    
		
		
	}

	private void initComponents() {

		endianButtonGroup = new ToggleGroup();
		integerSignButtonGroup = new ToggleGroup();
		binaryLabel = new Label();
		binaryCheckBox0 = new CheckBox();
		binaryCheckBox1 = new CheckBox();
		binaryCheckBox2 = new CheckBox();
		binaryCheckBox3 = new CheckBox();
		binaryCheckBox4 = new CheckBox();
		binaryCheckBox5 = new CheckBox();
		binaryCheckBox6 = new CheckBox();
		binaryCheckBox7 = new CheckBox();
		byteLabel = new Label();
		byteTextField = new TextField();
		wordLabel = new Label();
		wordTextField = new TextField();
		intLabel = new Label();
		intTextField = new TextField();
		longLabel = new Label();
		longTextField = new TextField();
		floatLabel = new Label();
		floatTextField = new TextField();
		doubleLabel = new Label();
		doubleTextField = new TextField();
		characterLabel = new Label();
		characterTextField = new TextField();
		stringLabel = new Label();
		stringTextField = new TextField();
		jSeparator1 = new Separator();
		bigEndianRadioButton = new RadioButton();
		littleEndianRadioButton = new RadioButton();
		signedRadioButton = new RadioButton();
		unsignedRadioButton = new RadioButton();

		
		binaryLabel.setText("Binary ");
		binaryCheckBox0.setOnAction(e -> {
			binaryCheckBox0ActionPerformed();
		});
		binaryCheckBox1.setOnAction(e -> {
			binaryCheckBox1ActionPerformed();
		});
		binaryCheckBox2.setOnAction(e -> {
			binaryCheckBox2ActionPerformed();
		});
		binaryCheckBox3.setOnAction(e -> {
			binaryCheckBox3ActionPerformed();
		});
		binaryCheckBox4.setOnAction(e -> {
			binaryCheckBox4ActionPerformed();
		});
		binaryCheckBox5.setOnAction(e -> {
			binaryCheckBox5ActionPerformed();
		});
		binaryCheckBox6.setOnAction(e -> {
			binaryCheckBox6ActionPerformed();
		});
		binaryCheckBox7.setOnAction(e -> {
			binaryCheckBox7ActionPerformed();
		});

		byteLabel.setText("Byte ");

		byteTextField.setEditable(false);
		longTextField.setEditable(false);
		floatTextField.setEditable(false);
		doubleTextField.setEditable(false);
		characterTextField.setEditable(false);
		stringTextField.setEditable(false);

		wordLabel.setText("Word ");
		intLabel.setText("Integer ");
		longLabel.setText("Long ");
		longLabel.setContentDisplay(ContentDisplay.CENTER);
	
		floatLabel.setText("Float  ");
		doubleLabel.setText("Double ");
		characterLabel.setText("Char  ");
		stringLabel.setText("String ");

		jSeparator1.setOrientation(Orientation.VERTICAL);

		bigEndianRadioButton.setToggleGroup(endianButtonGroup);
		bigEndianRadioButton.setSelected(true);
		bigEndianRadioButton.setText("Big Endian");
		bigEndianRadioButton.setTooltip(new Tooltip("Big Endian"));
		//bigEndianRadioButton.setOnAction(e -> updateValues());

		littleEndianRadioButton.setToggleGroup(endianButtonGroup);
		littleEndianRadioButton.setText("Little Endian");
		littleEndianRadioButton.setTooltip(new Tooltip("Little Endian"));
		//littleEndianRadioButton.setOnAction(e -> updateValues());

		signedRadioButton.setToggleGroup(integerSignButtonGroup);
		signedRadioButton.setText("Signed");
		signedRadioButton.setTooltip(new Tooltip("Signed Integers"));
		//signedRadioButton.setOnAction(e -> updateValues());

		unsignedRadioButton.setToggleGroup(integerSignButtonGroup);
		unsignedRadioButton.setText("Unsigned");
		unsignedRadioButton.setSelected(true);
		unsignedRadioButton.setTooltip(new Tooltip("Unsigned Integers"));
		//unsignedRadioButton.setOnAction(e -> updateValues());

		setPadding(new Insets(10, 10, 10, 10));
		setVgap(10);
		setHgap(10);

		// finally arrange Values on Pane

//		this.add(bigEndianRadioButton,1,0);
//		this.add(littleEndianRadioButton,2,0);
//		this.add(signedRadioButton,3,0);
//		this.add(unsignedRadioButton,4,0);
//
//		this.add(binaryLabel,1,1);
//		FlowPane fl = new FlowPane();
//		fl.getChildren().addAll(binaryCheckBox0, binaryCheckBox1, binaryCheckBox2, binaryCheckBox3, binaryCheckBox4,
//				binaryCheckBox5, binaryCheckBox6, binaryCheckBox7);
//		this.add(fl,2,1);
//		
//		this.add(byteLabel,1,2);
//		this.add(byteTextField,2,2);
//
//		this.add(wordLabel,3,2);
//		this.add(wordTextField,4,2);
//		
//		this.add(intLabel,5,2);
//		this.add(intTextField,6,2);
//		
		
		
		HBox firstrow = new HBox();
		firstrow.setSpacing(10);
		firstrow.getChildren().addAll(bigEndianRadioButton, littleEndianRadioButton, signedRadioButton, unsignedRadioButton);
		
		HBox secondrow = new HBox();
		secondrow.setSpacing(10);
		secondrow.getChildren().addAll(binaryLabel, binaryCheckBox0, binaryCheckBox1, binaryCheckBox2, binaryCheckBox3, binaryCheckBox4,
				binaryCheckBox5, binaryCheckBox6, binaryCheckBox7);
		
		
		HBox vierrow = new HBox();
		vierrow.setSpacing(10);
		vierrow.getChildren().addAll(byteLabel, byteTextField);
		vierrow.getChildren().addAll(wordLabel, wordTextField);
		vierrow.getChildren().addAll(intLabel, intTextField);
		
		
		HBox thirdrow = new HBox();
		thirdrow.setSpacing(10);
		thirdrow.getChildren().addAll(longLabel, longTextField);
		thirdrow.getChildren().addAll(floatLabel, floatTextField);
		thirdrow.getChildren().addAll(doubleLabel, doubleTextField);

	
		getChildren().addAll(
				firstrow, secondrow, vierrow, thirdrow);
		//characterLabel, characterTextField, stringLabel, stringTextField);

	}

	
	public void updateValues() {

	    goryachev.fxtexteditor.SelectionSegment seg = ed.getSelection().getSegment();
		if(seg == null)
		{
			return ;
		}
						
						
		Marker m = seg.getCaret();
						
		int posinline = m.getCharIndex();
		int line = seg.getCaret().getLine(); //getCaretLine();
		String value = ed.getModel().getPlainText(line);
		if (null == value)
			return;
		
		value = value.substring(0, 32);
		
		 
    	  // as long as we are not at the end of the field
    	  if (posinline < 32) {
            
    		  // determine how many chars(bytes) are left from current position to the end of the area
    		  // int availableData = dataSize - dataPosition >= CACHE_SIZE ? CACHE_SIZE : (int) (dataSize - dataPosition);
              
    		  // get the remaining data
              String bf = value.substring(posinline, posinline + (posinline<24?8:(32-posinline)));
              
              int availableData = 32 - posinline;
              
              //System.out.println(" String " + bf);
              if (bf.length()%2 == 1)
            	  bf =  bf.concat("0");
              byte[] values = hexStringToByteArray(bf);
              
              int cc = 0;
              valuesCache = new byte[CACHE_SIZE];
              for(byte b : values)
              {
            	  valuesCache[cc] = b;
            	  cc++;
              }
              
              // if there are some bytes missing fill the cache buffer with zeros bytes
              if (availableData < CACHE_SIZE) {
                 Arrays.fill(valuesCache, availableData, CACHE_SIZE, (byte) 0);
            }
        }

    	 ByteBuffer.wrap(valuesCache);
         valuesUpdater.schedule();
	}

	private boolean isSigned() {
		return signedRadioButton.isSelected();
	}

	private void binaryCheckBox0ActionPerformed() {
		if (!valuesUpdater.isUpdateInProgress() && ((valuesCache[0] & 0x80) > 0 != binaryCheckBox0.isSelected())) {
			valuesCache[0] = (byte) (valuesCache[0] ^ 0x80);
			modifyValues(1);
		}
	}

	private void binaryCheckBox1ActionPerformed() {

		if (!valuesUpdater.isUpdateInProgress() && ((valuesCache[0] & 0x40) > 0 != binaryCheckBox1.isSelected())) {
			valuesCache[0] = (byte) (valuesCache[0] ^ 0x40);
			modifyValues(1);
		}
	}

	private void binaryCheckBox2ActionPerformed() {

		if (!valuesUpdater.isUpdateInProgress() && ((valuesCache[0] & 0x20) > 0 != binaryCheckBox2.isSelected())) {
			valuesCache[0] = (byte) (valuesCache[0] ^ 0x20);
			modifyValues(1);
		}
	}

	private void binaryCheckBox3ActionPerformed() {

		if (!valuesUpdater.isUpdateInProgress() && ((valuesCache[0] & 0x10) > 0 != binaryCheckBox3.isSelected())) {
			valuesCache[0] = (byte) (valuesCache[0] ^ 0x10);
			modifyValues(1);
		}
	}

	private void binaryCheckBox4ActionPerformed() {

		if (!valuesUpdater.isUpdateInProgress() && ((valuesCache[0] & 0x8) > 0 != binaryCheckBox4.isSelected())) {
			valuesCache[0] = (byte) (valuesCache[0] ^ 0x8);
			modifyValues(1);
		}
	}

	private void binaryCheckBox5ActionPerformed() {

		if (!valuesUpdater.isUpdateInProgress() && ((valuesCache[0] & 0x4) > 0 != binaryCheckBox5.isSelected())) {
			valuesCache[0] = (byte) (valuesCache[0] ^ 0x4);
			modifyValues(1);
		}
	}

	private void binaryCheckBox6ActionPerformed() {

		if (!valuesUpdater.isUpdateInProgress() && ((valuesCache[0] & 0x2) > 0 != binaryCheckBox6.isSelected())) {
			valuesCache[0] = (byte) (valuesCache[0] ^ 0x2);
			modifyValues(1);
		}

	}

	private void binaryCheckBox7ActionPerformed() {

		if (!valuesUpdater.isUpdateInProgress() && ((valuesCache[0] & 0x1) > 0 != binaryCheckBox7.isSelected())) {
			valuesCache[0] = (byte) (valuesCache[0] ^ 0x1);
			modifyValues(1);
		}

	}

	private void modifyValues(int bytesCount) {

	}

	private ByteOrder getByteOrder() {
		return littleEndianRadioButton.isSelected() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
	}

	private class ValuesUpdater {

		private boolean updateInProgress = false;
		private boolean updateTerminated = false;
		private boolean scheduleUpdate = false;
		private boolean clearFields = true;

		boolean signed;
		ByteOrder byteOrder;
		byte[] values;

		public boolean isUpdateInProgress() {
			return updateInProgress;
		}

		private synchronized void schedule() {
			if (updateInProgress) {
				updateTerminated = true;
			}
			if (!scheduleUpdate) {
				scheduleUpdate = true;
				scheduleNextStep(ValuesPanelField.values()[0]);
			}
		}

		private void scheduleNextStep(final ValuesPanelField valuesPanelField) {
			Platform.runLater(new Runnable() {
				@Override
				public void run() {
					updateValue(valuesPanelField);
				}
			});
		}

		private void updateValue(final ValuesPanelField valuesPanelField) {
		
			if (valuesPanelField.ordinal() == 0) {
				long dataSize = 16;
				clearFields = dataPosition >= dataSize;
				byteOrder = littleEndianRadioButton.isSelected() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
				byteOrder = getByteOrder();
				signed = isSigned();
  			    values = valuesCache;
				if (clearFields) {
					values[0] = 0;
				}
				updateStarted();
			}

			if (updateTerminated) {
				stopUpdate();
				return;
			}

			if (clearFields) {
				clearField(valuesPanelField);
			}
			else {
				updateField(valuesPanelField);
			}

			final ValuesPanelField[] panelFields = ValuesPanelField.values();
			ValuesPanelField lastValue = panelFields[panelFields.length - 1];
			if (valuesPanelField == lastValue) {
				stopUpdate();
			} else {

				Platform.runLater(new Runnable() {
					@Override
					public void run() {
						ValuesPanelField nextValue = panelFields[valuesPanelField.ordinal() + 1];
						updateValue(nextValue);
					}
				});
			}

		}

		private synchronized void updateStarted() {
			updateInProgress = true;
			scheduleUpdate = false;
		}
		
		private synchronized void stopUpdate() {
			updateInProgress = false;
			updateTerminated = false;
		}
	

		private void updateField(ValuesPanelField valuesPanelField) {
         
		 switch (valuesPanelField) {
             case BINARY0: {
                 binaryCheckBox0.setSelected((values[0] & 0x80) > 0);
                 break;
             }
             case BINARY1: {
                 binaryCheckBox1.setSelected((values[0] & 0x40) > 0);
                 break;
             }
             case BINARY2: {
                 binaryCheckBox2.setSelected((values[0] & 0x20) > 0);
                 break;
             }
             case BINARY3: {
                 binaryCheckBox3.setSelected((values[0] & 0x10) > 0);
                 break;
             }
             case BINARY4: {
                 binaryCheckBox4.setSelected((values[0] & 0x8) > 0);
                 break;
             }
             case BINARY5: {
                 binaryCheckBox5.setSelected((values[0] & 0x4) > 0);
                 break;
             }
             case BINARY6: {
                 binaryCheckBox6.setSelected((values[0] & 0x2) > 0);
                 break;
             }
             case BINARY7: {
                 binaryCheckBox7.setSelected((values[0] & 0x1) > 0);
                 break;
             }
             case BYTE: {
                 byteTextField.setText(String.valueOf(signed ? values[0] : values[0] & 0xff));
                 break;
             }
             case WORD: {
                 int wordValue = signed
                         ? (byteOrder == ByteOrder.LITTLE_ENDIAN
                                 ? (values[0] & 0xff) | (values[1] << 8)
                                 : (values[1] & 0xff) | (values[0] << 8))
                         : (byteOrder == ByteOrder.LITTLE_ENDIAN
                                 ? (values[0] & 0xff) | ((values[1] & 0xff) << 8)
                                 : (values[1] & 0xff) | ((values[0] & 0xff) << 8));
                 wordTextField.setText(String.valueOf(wordValue));
                 break;
             }
             case INTEGER: {
                 long intValue = signed
                         ? (byteOrder == ByteOrder.LITTLE_ENDIAN
                                 ? (values[0] & 0xffl) | ((values[1] & 0xffl) << 8) | ((values[2] & 0xffl) << 16) | (values[3] << 24)
                                 : (values[3] & 0xffl) | ((values[2] & 0xffl) << 8) | ((values[1] & 0xffl) << 16) | (values[0] << 24))
                         : (byteOrder == ByteOrder.LITTLE_ENDIAN
                                 ? (values[0] & 0xffl) | ((values[1] & 0xffl) << 8) | ((values[2] & 0xffl) << 16) | ((values[3] & 0xffl) << 24)
                                 : (values[3] & 0xffl) | ((values[2] & 0xffl) << 8) | ((values[1] & 0xffl) << 16) | ((values[0] & 0xffl) << 24));
                 intTextField.setText(String.valueOf(intValue));
                 break;
             }
             case LONG: {
                 if (signed) {
                     byteBuffer.rewind();
                     if (byteBuffer.order() != byteOrder) {
                         byteBuffer.order(byteOrder);
                     }

                     longTextField.setText(String.valueOf(byteBuffer.getLong()));
                 } else {
                     long longValue = byteOrder == ByteOrder.LITTLE_ENDIAN
                             ? (values[0] & 0xffl) | ((values[1] & 0xffl) << 8) | ((values[2] & 0xffl) << 16) | ((values[3] & 0xffl) << 24)
                             | ((values[4] & 0xffl) << 32) | ((values[5] & 0xffl) << 40) | ((values[6] & 0xffl) << 48)
                             : (values[7] & 0xffl) | ((values[6] & 0xffl) << 8) | ((values[5] & 0xffl) << 16) | ((values[4] & 0xffl) << 24)
                             | ((values[3] & 0xffl) << 32) | ((values[2] & 0xffl) << 40) | ((values[1] & 0xffl) << 48);
                     BigInteger bigInt1 = BigInteger.valueOf(values[byteOrder == ByteOrder.LITTLE_ENDIAN ? 7 : 0] & 0xffl);
                     BigInteger bigInt2 = bigInt1.shiftLeft(56);
                     BigInteger bigInt3 = bigInt2.add(BigInteger.valueOf(longValue));
                     longTextField.setText(bigInt3.toString());
                 }
                 break;
             }
             case FLOAT: {
                 ByteBuffer buffer = ByteBuffer.wrap(values);
                 buffer = buffer.rewind();
                 if (buffer.order() != byteOrder) {
                     buffer.order(byteOrder);
                 }
                 floatTextField.setText(String.valueOf(buffer.getFloat()));
                 break;
             }
             case DOUBLE: {
                 byteBuffer.position(0);
                 ByteBuffer buffer = ByteBuffer.wrap(values);
                 buffer = buffer.rewind();
                 if (buffer.order() != byteOrder) {
                     buffer.order(byteOrder);
                 }
                 doubleTextField.setText(String.valueOf(buffer.getDouble()));
                 break;
             }
             case CHARACTER: {
                 String strValue = "";
  				 strValue = ""; //area.getSelectedText();
				
                 if (strValue.length() > 0) {
                     characterTextField.setText(strValue.substring(0, 1));
                 } else {
                     characterTextField.setText("");
                 }
                 break;
             }
//             case STRING: {
//                 String strValue = "";
//				try {
//					strValue = new String(values, area.getSelectedText());
//				} catch (UnsupportedEncodingException e) {
//					e.printStackTrace();
//				}
//                 for (int i = 0; i < strValue.length(); i++) {
//                     char charAt = strValue.charAt(i);
//                     if (charAt == '\r' || charAt == '\n' || charAt == 0) {
//                         strValue = strValue.substring(0, i);
//                         break;
//                     }
//                 }
//                 stringTextField.setText(strValue);
//                 stringTextField.positionCaret(0);
//                 break;
//             }
           }
         }
     
	
		private void clearField(ValuesPanelField valuesPanelField) {
	
		switch (valuesPanelField) {
		
		case BINARY0: {
			binaryCheckBox0.setSelected(false);
			break;
		}
		case BINARY1: {
			binaryCheckBox1.setSelected(false);
			break;
		}
		case BINARY2: {
			binaryCheckBox2.setSelected(false);
			break;
		}
		case BINARY3: {
			binaryCheckBox3.setSelected(false);
			break;
		}
		case BINARY4: {
			binaryCheckBox4.setSelected(false);
			break;
		}
		case BINARY5: {
			binaryCheckBox5.setSelected(false);
			break;
		}
		case BINARY6: {
			binaryCheckBox6.setSelected(false);
			break;
		}
		case BINARY7: {
			binaryCheckBox7.setSelected(false);
			break;
		}
		case BYTE: {
			byteTextField.setText("");
			break;
		}
		case WORD: {
			wordTextField.setText("");
			break;
		}
		case INTEGER: {
			intTextField.setText("");
			break;
		}
		case LONG: {
			longTextField.setText("");
			break;
		}
		case FLOAT: {
			floatTextField.setText("");
			break;
		}
		case DOUBLE: {
			doubleTextField.setText("");
			break;
		}
		case CHARACTER: {
			characterTextField.setText("");
			break;
		}
		case STRING: {
			stringTextField.setText("");
			break;
		}
		}
	}
	}

	/* s must be an even-length string. */
    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                                 + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

}

enum ValuesPanelField {
	BINARY0, BINARY1, BINARY2, BINARY3, BINARY4, BINARY5, BINARY6, BINARY7, BYTE, WORD, INTEGER, LONG, FLOAT, DOUBLE,
	CHARACTER, STRING
}
