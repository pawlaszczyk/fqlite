package fqlite.ui.hexviewer;

import java.io.File;

import goryachev.common.util.text.IBreakIterator;
import goryachev.fxtexteditor.Edit;
import goryachev.fxtexteditor.FileCachePlainTextEditorModel;
import goryachev.fxtexteditor.FxTextEditorModel;
import goryachev.fxtexteditor.ITextLine;

public class HexEditorFileCacheEditorModel extends FxTextEditorModel{

	private FileCachePlainTextEditorModel internal;
	
	public HexEditorFileCacheEditorModel(File f){
		
		internal = new FileCachePlainTextEditorModel(f);
	}
	
	@Override
	public int getLineCount() {

		return internal.getLineCount();
	}

	
	@Override
	public ITextLine getTextLine(int line) {

		return internal.getTextLine(line);
		
	}

	@Override
	public Edit edit(Edit ed) throws Exception {

		return internal.edit(ed);
	}

	@Override
	public IBreakIterator getBreakIterator() {
		
		return internal.getBreakIterator();
	}
	
	
	public long length(){
		return internal.length();
	}

}
