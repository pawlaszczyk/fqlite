// Copyright © 2020-2023 Andy Goryachev <andy@goryachev.com>
package fqlite.ui.hexviewer;
import goryachev.fx.CssStyle;
import goryachev.fx.FX;
import goryachev.fx.Formatters;
import goryachev.fx.FxFormatter;
import goryachev.fx.HPane;
import goryachev.fxtexteditor.FxTextEditor;
import goryachev.fxtexteditor.Marker;
import javafx.beans.binding.Bindings;
import javafx.scene.control.Label;


/**
 * Status bar of the hex viewer window.
 */
public class StatusBar
	extends HPane
{
	public static final CssStyle PANE = new CssStyle("StatusBar_PANE");
	public static final CssStyle LABEL = new CssStyle("StatusBar_LABEL");
	public final Label caret;
	private int totalbytes = 0;
	
	public StatusBar()
	{
		FX.style(this, PANE);
		
		caret = FX.label(LABEL);
		
		add(caret);
		fill();
	}

    public void setTotal(int total)
    {
    	totalbytes = total;
    }
	
	public void attach(FxTextEditor ed)
	{
		caret.textProperty().bind(Bindings.createStringBinding
		(
			() ->
			{
				goryachev.fxtexteditor.SelectionSegment seg =  ed.getSelection().getSegment(); // ed.getSelectedSegment();
				if(seg == null)
				{
					return null;
				}
				
				FxFormatter fmt = Formatters.integerFormatter();
				
				Marker m = seg.getCaret();
				
				int posinline = 0;
				
				int position = m.getCharIndex();
				
				if(position<33)
				  posinline = position/2;
				else if(position == 33)
				  posinline = 16;
				else
				  posinline = position - 33;
				
				return 
					"  Offset " + (m.getLine()*16 + posinline) + " out of " + totalbytes + " bytes " +
					" | " + 	
					"line:" +
					fmt.format(m.getLine() + 1) +
					"  column:" +
					//fmt.format(ed.getCogetColumnAt(m) + 1) +
					"  char:" + 
					fmt.format(m.getCharIndex());
			},
			ed.selectionProperty() 
		));
	}
}
