package fqlite.ui.hexviewer;

import goryachev.common.util.Parsers;
import goryachev.fx.CPane;
import goryachev.fx.FxComboBox;
import goryachev.fx.FxMenuBar;
import goryachev.fx.FxToolBar;
import goryachev.fx.FxWindow;
import goryachev.fx.settings.LocalSettings;
import goryachev.fxtexteditor.Actions;
import goryachev.fxtexteditor.FxTextEditor;
import goryachev.fxtexteditor.FxTextEditorModel;
import goryachev.fxtexteditor.internal.Markers;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Hashtable;
import demo.fxtexteditor.AnItem;
import demo.fxtexteditor.MainPane;
import javafx.scene.Node;
import javafx.scene.control.Label;
import javafx.scene.layout.VBox;

public class HexFXWindow extends FxWindow {
  
  public final MainPane mainPane;
  
  public final CPane content;
  
  public final StatusBar statusBar;
  
  protected final FxComboBox<Object> modelSelector = new FxComboBox<Object>();
  
  protected final FxComboBox<Object> fontSelector = new FxComboBox<Object>();
  
  public Hashtable<Object,FileCachePlainTextEditorModel> files = new Hashtable<>();
  
  Markers myselection;
  
  
  public void loadNewHexFile(String path) {
    Path pp = Paths.get(path, new String[0]);
    AnItem newhex = new AnItem(path, pp.getFileName().toString());
    if (this.files.containsKey(path)) {
      this.modelSelector.select(newhex);
      return;
    } 
    FileCachePlainTextEditorModel m = new FileCachePlainTextEditorModel(new File(path));
    try {
      Thread.currentThread();
      Thread.sleep(100L);
    } catch (InterruptedException interruptedException) {}
    this.modelSelector.addItem(newhex);
    this.files.put(path, m);
    this.modelSelector.select(this.modelSelector.getItems().size() - 1);

  }
  
  public void loadLines(String path, long offset){
	  
	  FileCachePlainTextEditorModel m = this.files.get(path);
	  long line = offset / 16;
	  line-=2;
	  m.loadLines((int)line, 50);
  }
	  
  
public HexFXWindow() {
    super("HexViewer");
    this.myselection = new Markers(100);
    AnItem EMPTY = new AnItem("EMTPY", "<EMPTY>");
    this.modelSelector.addItem(EMPTY);
    this.modelSelector.select(0);
    this.modelSelector.valueProperty().addListener((s, p, c) -> onModelSelectionChange(c));
    this.fontSelector.setItems(new Object[] { "9", "12", "18", "24" });
    this.fontSelector.valueProperty().addListener((s, p, c) -> onFontChange(c));
    this.mainPane = new MainPane();
    this.content = new CPane();
    this.content.setTop(createToolbar());
    this.content.setCenter((Node)this.mainPane);
    this.statusBar = new StatusBar();
    setTitle("HexView");
    setTop(createMenu());
    setCenter((Node)this.content);
    VBox vb = new VBox();
    vb.getChildren().addAll(new Node[] { new ValuePanel(editor()), (Node)this.statusBar });
    setBottom(vb);
    this.fontSelector.setEditable(false);
    this.fontSelector.select("18");
    this.editor().setLineNumberFormatter(new OffsetFormatter());
    this.editor().setShowLineNumbers(true);
    this.statusBar.attach(editor());
    
    LocalSettings.get(this).
	add("LINE_WRAP", editor().wrapLinesProperty());
    
    setSize(780.0D, 800.0D);
 
  }
  
  protected FxTextEditor editor() {
    return this.mainPane.editor;
  }
  
  protected Node createMenu() {
    FxMenuBar m = new FxMenuBar();
    Actions a = (editor()).actions;
    m.menu("Action");
    m.item("Copy", a.copy());
    return (Node)m;
  }
  
  protected Node createToolbar() {
    FxToolBar t = new FxToolBar();
    t.add(new Label("Font:"));
    t.add((Node)this.fontSelector);
    t.space();
    t.add(new Label("Model:"));
    t.space(2);
    t.add((Node)this.modelSelector);
    return (Node)t;
  }
  
  protected void preferences() {}
  
  protected void newWindow() {
    HexFXWindow w = new HexFXWindow();
    w.mainPane.setModel(this.mainPane.getModel());
    w.open();
  }
  
  protected void onModelSelectionChange(Object x) {
    if (x instanceof AnItem) {
      AnItem s = (AnItem)x;
      FileCachePlainTextEditorModel m = this.files.get(s.getCode());
      this.mainPane.setModel((FxTextEditorModel)m);
      if (m == null)
        return; 
      this.statusBar.setTotal((int)m.length());
    } 
  }
  
  protected void onFontChange(Object x) {
    int sz = Parsers.parseInt(x, 18);
    this.mainPane.editor.setFontSize(sz);
  }
  
  
  
  /**
   * Go to a certain line with a given offset.
   * 
   * @param offset
   */
  public void goTo(long offset) {
	int line = (int)(offset/16);
	goToLine(line);	
	editor().scrollCaretToView();
	editor().reloadVisibleArea();
  }


	/** navigates to the line if row is between [0...lineCount-1], otherwise does nothing */
	public void goToLine(int row)
	{
		System.out.println("WrapLines :: " + editor().isWrapLines());
		editor().wrapLinesProperty().set(true);
		
		// TODO smarter algorithm near the end of file
		if((row >= 0) && (row < editor().getLineCount()))
		{
			editor().setOrigin(Math.max(0, row-3));
			editor().setCaret(row, 0);
		}
	}
  
  public void clearAll() {
    this.modelSelector.getSelectionModel().clearSelection();
    this.modelSelector.setValue(null);
    this.modelSelector.getItems().clear();
    this.files.clear();
    AnItem EMPTY = new AnItem("EMTPY", "<EMPTY>");
    this.modelSelector.addItem(EMPTY);
    this.modelSelector.select(0);
  }
}

