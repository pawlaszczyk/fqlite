package fqlite.ui;

import javax.swing.text.AttributeSet;
import javax.swing.text.BadLocationException;
import javax.swing.text.PlainDocument;


/*
* this class is a document for text fields to limit the maximum count of
* inserted characters with the additional ability to ignore a subset of them.
* <p>
* to use: <code>JTextfield.setDocument(new Validation(int limit))</code> or
* <code>JTextfield.setDocument(new Validation(int limit, String regEx))</code>
* </p>
* 
*/
public class LimitlessPlainDocument extends PlainDocument {

	 
		private static final long serialVersionUID = 1L;
		
		/**
	     * empty string for replacing the characters identified by the regular
	     * expression in order to ignore them when displaying the inserted values.
	     */
	    private static final String EMPTY_STRING = "";
	    /**
	     * maximum count of characters in the object using this document.
	     */
	    private final int limit;
	    /**
	     * regular expression for characters to ignore when inserted in the object
	     * using this document.
	     */
	    private final String regExToIgnore;

	    /**
	     * constructor for the validation document.
	     * 
	     * @param maximum
	     *        maximum count of characters insertable
	     */
	    public LimitlessPlainDocument(final int maximum) {
	        this(maximum, null);
	    }

	    /**
	     * constructor for the validation document.
	     * 
	     * @param maximum
	     *        maximum count of characters insertable
	     * @param regEx
	     *        regular expression to ignore in input
	     */
	    public LimitlessPlainDocument(final int maximum, final String regEx) {
	        super();
	        if (maximum < 0) {
	            this.limit = 0;
	        } else {
	            this.limit = maximum;
	        }
	        if ((regEx != null) && (regEx.length() > 0)) {
	            this.regExToIgnore = regEx;
	        } else {
	            this.regExToIgnore = null;
	        }
	    }

	    /**
	     * overrides insertString() method of PlainDocument regarding the maximum
	     * limit and ignoring all parts matching the stored regular expression.
	     * 
	     * @param offset
	     *        position to start the insertion
	     * @param str
	     *        string to insert
	     * @param attr
	     *        Attributset of the string
	     * @throws BadLocationException
	     *         if offset got an invalid value
	     */
	    @Override
	    public final void insertString(final int offset, final String str,
	            final AttributeSet attr) throws BadLocationException {
	        if (str == null) {
	            return;
	        }
	        String concat;
	        if (this.regExToIgnore == null) {
	            concat = str;
	        } else {
	            concat = str.replaceAll(this.regExToIgnore, EMPTY_STRING);
	        }
	        if ((getLength() + concat.length()) <= this.limit) {
	            super.insertString(offset, concat, attr);
	        } else {
	            super.insertString(offset, concat.substring(0, this.limit
	                    - getLength()), attr);
	        }
	    }
	}
	

