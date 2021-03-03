package fqlite.parser;
// Generated from SQLite.g4 by ANTLR 4.8
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class SQLiteParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.8", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		SCOL=1, DOT=2, OPEN_PAR=3, CLOSE_PAR=4, COMMA=5, ASSIGN=6, STAR=7, PLUS=8, 
		MINUS=9, TILDE=10, PIPE2=11, DIV=12, MOD=13, LT2=14, GT2=15, AMP=16, PIPE=17, 
		LT=18, LT_EQ=19, GT=20, GT_EQ=21, EQ=22, NOT_EQ1=23, NOT_EQ2=24, K_ABORT=25, 
		K_ACTION=26, K_ADD=27, K_AFTER=28, K_ALL=29, K_ALTER=30, K_ANALYZE=31, 
		K_AND=32, K_AS=33, K_ASC=34, K_ATTACH=35, K_AUTOINCREMENT=36, K_BEFORE=37, 
		K_BEGIN=38, K_BETWEEN=39, K_BY=40, K_CASCADE=41, K_CASE=42, K_CAST=43, 
		K_CHECK=44, K_COLLATE=45, K_COLUMN=46, K_COMMIT=47, K_CONFLICT=48, K_CONSTRAINT=49, 
		K_CREATE=50, K_CROSS=51, K_CURRENT_DATE=52, K_CURRENT_TIME=53, K_CURRENT_TIMESTAMP=54, 
		K_DATABASE=55, K_DEFAULT=56, K_DEFERRABLE=57, K_DEFERRED=58, K_DELETE=59, 
		K_DESC=60, K_DETACH=61, K_DISTINCT=62, K_DROP=63, K_EACH=64, K_ELSE=65, 
		K_END=66, K_ESCAPE=67, K_EXCEPT=68, K_EXCLUSIVE=69, K_EXISTS=70, K_EXPLAIN=71, 
		K_FAIL=72, K_FOR=73, K_FOREIGN=74, K_FROM=75, K_FULL=76, K_GLOB=77, K_GROUP=78, 
		K_HAVING=79, K_IF=80, K_IGNORE=81, K_IMMEDIATE=82, K_IN=83, K_INDEX=84, 
		K_INDEXED=85, K_INITIALLY=86, K_INNER=87, K_INSERT=88, K_INSTEAD=89, K_INTERSECT=90, 
		K_INTO=91, K_IS=92, K_ISNULL=93, K_JOIN=94, K_KEY=95, K_LEFT=96, K_LIKE=97, 
		K_LIMIT=98, K_MATCH=99, K_NATURAL=100, K_NO=101, K_NOT=102, K_NOTNULL=103, 
		K_NULL=104, K_OF=105, K_OFFSET=106, K_ON=107, K_OR=108, K_ORDER=109, K_OUTER=110, 
		K_PLAN=111, K_PRAGMA=112, K_PRIMARY=113, K_QUERY=114, K_RAISE=115, K_RECURSIVE=116, 
		K_REFERENCES=117, K_REGEXP=118, K_REINDEX=119, K_RELEASE=120, K_RENAME=121, 
		K_REPLACE=122, K_RESTRICT=123, K_RIGHT=124, K_ROLLBACK=125, K_ROW=126, 
		K_SAVEPOINT=127, K_SELECT=128, K_SET=129, K_TABLE=130, K_TEMP=131, K_TEMPORARY=132, 
		K_THEN=133, K_TO=134, K_TRANSACTION=135, K_TRIGGER=136, K_UNION=137, K_UNIQUE=138, 
		K_UPDATE=139, K_USING=140, K_VACUUM=141, K_VALUES=142, K_VIEW=143, K_VIRTUAL=144, 
		K_WHEN=145, K_WHERE=146, K_WITH=147, K_WITHOUT=148, IDENTIFIER=149, NUMERIC_LITERAL=150, 
		BIND_PARAMETER=151, STRING_LITERAL=152, BLOB_LITERAL=153, SINGLE_LINE_COMMENT=154, 
		MULTILINE_COMMENT=155, SPACES=156, UNEXPECTED_CHAR=157;
	public static final int
		RULE_parse = 0, RULE_error = 1, RULE_sql_stmt_list = 2, RULE_sql_stmt = 3, 
		RULE_alter_table_stmt = 4, RULE_analyze_stmt = 5, RULE_attach_stmt = 6, 
		RULE_begin_stmt = 7, RULE_commit_stmt = 8, RULE_compound_select_stmt = 9, 
		RULE_create_index_stmt = 10, RULE_create_table_stmt = 11, RULE_create_trigger_stmt = 12, 
		RULE_create_view_stmt = 13, RULE_create_virtual_table_stmt = 14, RULE_delete_stmt = 15, 
		RULE_delete_stmt_limited = 16, RULE_detach_stmt = 17, RULE_drop_index_stmt = 18, 
		RULE_drop_table_stmt = 19, RULE_drop_trigger_stmt = 20, RULE_drop_view_stmt = 21, 
		RULE_factored_select_stmt = 22, RULE_insert_stmt = 23, RULE_pragma_stmt = 24, 
		RULE_reindex_stmt = 25, RULE_release_stmt = 26, RULE_rollback_stmt = 27, 
		RULE_savepoint_stmt = 28, RULE_simple_select_stmt = 29, RULE_select_stmt = 30, 
		RULE_select_or_values = 31, RULE_update_stmt = 32, RULE_update_stmt_limited = 33, 
		RULE_vacuum_stmt = 34, RULE_column_def = 35, RULE_type_name = 36, RULE_column_constraint = 37, 
		RULE_conflict_clause = 38, RULE_expr = 39, RULE_foreign_key_clause = 40, 
		RULE_raise_function = 41, RULE_indexed_column = 42, RULE_table_constraint = 43, 
		RULE_with_clause = 44, RULE_qualified_table_name = 45, RULE_ordering_term = 46, 
		RULE_pragma_value = 47, RULE_common_table_expression = 48, RULE_result_column = 49, 
		RULE_table_or_subquery = 50, RULE_join_clause = 51, RULE_join_operator = 52, 
		RULE_join_constraint = 53, RULE_select_core = 54, RULE_compound_operator = 55, 
		RULE_signed_number = 56, RULE_literal_value = 57, RULE_unary_operator = 58, 
		RULE_error_message = 59, RULE_module_argument = 60, RULE_column_alias = 61, 
		RULE_keyword = 62, RULE_name = 63, RULE_function_name = 64, RULE_database_name = 65, 
		RULE_schema_name = 66, RULE_table_function_name = 67, RULE_table_name = 68, 
		RULE_table_or_index_name = 69, RULE_new_table_name = 70, RULE_column_name = 71, 
		RULE_collation_name = 72, RULE_foreign_table = 73, RULE_index_name = 74, 
		RULE_trigger_name = 75, RULE_view_name = 76, RULE_module_name = 77, RULE_pragma_name = 78, 
		RULE_savepoint_name = 79, RULE_table_alias = 80, RULE_transaction_name = 81, 
		RULE_any_name = 82;
	private static String[] makeRuleNames() {
		return new String[] {
			"parse", "error", "sql_stmt_list", "sql_stmt", "alter_table_stmt", "analyze_stmt", 
			"attach_stmt", "begin_stmt", "commit_stmt", "compound_select_stmt", "create_index_stmt", 
			"create_table_stmt", "create_trigger_stmt", "create_view_stmt", "create_virtual_table_stmt", 
			"delete_stmt", "delete_stmt_limited", "detach_stmt", "drop_index_stmt", 
			"drop_table_stmt", "drop_trigger_stmt", "drop_view_stmt", "factored_select_stmt", 
			"insert_stmt", "pragma_stmt", "reindex_stmt", "release_stmt", "rollback_stmt", 
			"savepoint_stmt", "simple_select_stmt", "select_stmt", "select_or_values", 
			"update_stmt", "update_stmt_limited", "vacuum_stmt", "column_def", "type_name", 
			"column_constraint", "conflict_clause", "expr", "foreign_key_clause", 
			"raise_function", "indexed_column", "table_constraint", "with_clause", 
			"qualified_table_name", "ordering_term", "pragma_value", "common_table_expression", 
			"result_column", "table_or_subquery", "join_clause", "join_operator", 
			"join_constraint", "select_core", "compound_operator", "signed_number", 
			"literal_value", "unary_operator", "error_message", "module_argument", 
			"column_alias", "keyword", "name", "function_name", "database_name", 
			"schema_name", "table_function_name", "table_name", "table_or_index_name", 
			"new_table_name", "column_name", "collation_name", "foreign_table", "index_name", 
			"trigger_name", "view_name", "module_name", "pragma_name", "savepoint_name", 
			"table_alias", "transaction_name", "any_name"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "';'", "'.'", "'('", "')'", "','", "'='", "'*'", "'+'", "'-'", 
			"'~'", "'||'", "'/'", "'%'", "'<<'", "'>>'", "'&'", "'|'", "'<'", "'<='", 
			"'>'", "'>='", "'=='", "'!='", "'<>'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "SCOL", "DOT", "OPEN_PAR", "CLOSE_PAR", "COMMA", "ASSIGN", "STAR", 
			"PLUS", "MINUS", "TILDE", "PIPE2", "DIV", "MOD", "LT2", "GT2", "AMP", 
			"PIPE", "LT", "LT_EQ", "GT", "GT_EQ", "EQ", "NOT_EQ1", "NOT_EQ2", "K_ABORT", 
			"K_ACTION", "K_ADD", "K_AFTER", "K_ALL", "K_ALTER", "K_ANALYZE", "K_AND", 
			"K_AS", "K_ASC", "K_ATTACH", "K_AUTOINCREMENT", "K_BEFORE", "K_BEGIN", 
			"K_BETWEEN", "K_BY", "K_CASCADE", "K_CASE", "K_CAST", "K_CHECK", "K_COLLATE", 
			"K_COLUMN", "K_COMMIT", "K_CONFLICT", "K_CONSTRAINT", "K_CREATE", "K_CROSS", 
			"K_CURRENT_DATE", "K_CURRENT_TIME", "K_CURRENT_TIMESTAMP", "K_DATABASE", 
			"K_DEFAULT", "K_DEFERRABLE", "K_DEFERRED", "K_DELETE", "K_DESC", "K_DETACH", 
			"K_DISTINCT", "K_DROP", "K_EACH", "K_ELSE", "K_END", "K_ESCAPE", "K_EXCEPT", 
			"K_EXCLUSIVE", "K_EXISTS", "K_EXPLAIN", "K_FAIL", "K_FOR", "K_FOREIGN", 
			"K_FROM", "K_FULL", "K_GLOB", "K_GROUP", "K_HAVING", "K_IF", "K_IGNORE", 
			"K_IMMEDIATE", "K_IN", "K_INDEX", "K_INDEXED", "K_INITIALLY", "K_INNER", 
			"K_INSERT", "K_INSTEAD", "K_INTERSECT", "K_INTO", "K_IS", "K_ISNULL", 
			"K_JOIN", "K_KEY", "K_LEFT", "K_LIKE", "K_LIMIT", "K_MATCH", "K_NATURAL", 
			"K_NO", "K_NOT", "K_NOTNULL", "K_NULL", "K_OF", "K_OFFSET", "K_ON", "K_OR", 
			"K_ORDER", "K_OUTER", "K_PLAN", "K_PRAGMA", "K_PRIMARY", "K_QUERY", "K_RAISE", 
			"K_RECURSIVE", "K_REFERENCES", "K_REGEXP", "K_REINDEX", "K_RELEASE", 
			"K_RENAME", "K_REPLACE", "K_RESTRICT", "K_RIGHT", "K_ROLLBACK", "K_ROW", 
			"K_SAVEPOINT", "K_SELECT", "K_SET", "K_TABLE", "K_TEMP", "K_TEMPORARY", 
			"K_THEN", "K_TO", "K_TRANSACTION", "K_TRIGGER", "K_UNION", "K_UNIQUE", 
			"K_UPDATE", "K_USING", "K_VACUUM", "K_VALUES", "K_VIEW", "K_VIRTUAL", 
			"K_WHEN", "K_WHERE", "K_WITH", "K_WITHOUT", "IDENTIFIER", "NUMERIC_LITERAL", 
			"BIND_PARAMETER", "STRING_LITERAL", "BLOB_LITERAL", "SINGLE_LINE_COMMENT", 
			"MULTILINE_COMMENT", "SPACES", "UNEXPECTED_CHAR"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "SQLite.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public SQLiteParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	public static class ParseContext extends ParserRuleContext {
		public TerminalNode EOF() { return getToken(SQLiteParser.EOF, 0); }
		public List<Sql_stmt_listContext> sql_stmt_list() {
			return getRuleContexts(Sql_stmt_listContext.class);
		}
		public Sql_stmt_listContext sql_stmt_list(int i) {
			return getRuleContext(Sql_stmt_listContext.class,i);
		}
		public List<ErrorContext> error() {
			return getRuleContexts(ErrorContext.class);
		}
		public ErrorContext error(int i) {
			return getRuleContext(ErrorContext.class,i);
		}
		public ParseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parse; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterParse(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitParse(this);
		}
	}

	public final ParseContext parse() throws RecognitionException {
		ParseContext _localctx = new ParseContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_parse);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(170);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << SCOL) | (1L << K_ALTER) | (1L << K_ANALYZE) | (1L << K_ATTACH) | (1L << K_BEGIN) | (1L << K_COMMIT) | (1L << K_CREATE) | (1L << K_DELETE) | (1L << K_DETACH) | (1L << K_DROP))) != 0) || ((((_la - 66)) & ~0x3f) == 0 && ((1L << (_la - 66)) & ((1L << (K_END - 66)) | (1L << (K_EXPLAIN - 66)) | (1L << (K_INSERT - 66)) | (1L << (K_PRAGMA - 66)) | (1L << (K_REINDEX - 66)) | (1L << (K_RELEASE - 66)) | (1L << (K_REPLACE - 66)) | (1L << (K_ROLLBACK - 66)) | (1L << (K_SAVEPOINT - 66)) | (1L << (K_SELECT - 66)))) != 0) || ((((_la - 139)) & ~0x3f) == 0 && ((1L << (_la - 139)) & ((1L << (K_UPDATE - 139)) | (1L << (K_VACUUM - 139)) | (1L << (K_VALUES - 139)) | (1L << (K_WITH - 139)) | (1L << (UNEXPECTED_CHAR - 139)))) != 0)) {
				{
				setState(168);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case SCOL:
				case K_ALTER:
				case K_ANALYZE:
				case K_ATTACH:
				case K_BEGIN:
				case K_COMMIT:
				case K_CREATE:
				case K_DELETE:
				case K_DETACH:
				case K_DROP:
				case K_END:
				case K_EXPLAIN:
				case K_INSERT:
				case K_PRAGMA:
				case K_REINDEX:
				case K_RELEASE:
				case K_REPLACE:
				case K_ROLLBACK:
				case K_SAVEPOINT:
				case K_SELECT:
				case K_UPDATE:
				case K_VACUUM:
				case K_VALUES:
				case K_WITH:
					{
					setState(166);
					sql_stmt_list();
					}
					break;
				case UNEXPECTED_CHAR:
					{
					setState(167);
					error();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				setState(172);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(173);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ErrorContext extends ParserRuleContext {
		public Token UNEXPECTED_CHAR;
		public TerminalNode UNEXPECTED_CHAR() { return getToken(SQLiteParser.UNEXPECTED_CHAR, 0); }
		public ErrorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_error; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterError(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitError(this);
		}
	}

	public final ErrorContext error() throws RecognitionException {
		ErrorContext _localctx = new ErrorContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_error);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(175);
			((ErrorContext)_localctx).UNEXPECTED_CHAR = match(UNEXPECTED_CHAR);
			 
			     throw new RuntimeException("UNEXPECTED_CHAR=" + (((ErrorContext)_localctx).UNEXPECTED_CHAR!=null?((ErrorContext)_localctx).UNEXPECTED_CHAR.getText():null)); 
			   
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Sql_stmt_listContext extends ParserRuleContext {
		public List<Sql_stmtContext> sql_stmt() {
			return getRuleContexts(Sql_stmtContext.class);
		}
		public Sql_stmtContext sql_stmt(int i) {
			return getRuleContext(Sql_stmtContext.class,i);
		}
		public List<TerminalNode> SCOL() { return getTokens(SQLiteParser.SCOL); }
		public TerminalNode SCOL(int i) {
			return getToken(SQLiteParser.SCOL, i);
		}
		public Sql_stmt_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sql_stmt_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSql_stmt_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSql_stmt_list(this);
		}
	}

	public final Sql_stmt_listContext sql_stmt_list() throws RecognitionException {
		Sql_stmt_listContext _localctx = new Sql_stmt_listContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_sql_stmt_list);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(181);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==SCOL) {
				{
				{
				setState(178);
				match(SCOL);
				}
				}
				setState(183);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(184);
			sql_stmt();
			setState(193);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,4,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(186); 
					_errHandler.sync(this);
					_la = _input.LA(1);
					do {
						{
						{
						setState(185);
						match(SCOL);
						}
						}
						setState(188); 
						_errHandler.sync(this);
						_la = _input.LA(1);
					} while ( _la==SCOL );
					setState(190);
					sql_stmt();
					}
					} 
				}
				setState(195);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,4,_ctx);
			}
			setState(199);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,5,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(196);
					match(SCOL);
					}
					} 
				}
				setState(201);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,5,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Sql_stmtContext extends ParserRuleContext {
		public Alter_table_stmtContext alter_table_stmt() {
			return getRuleContext(Alter_table_stmtContext.class,0);
		}
		public Analyze_stmtContext analyze_stmt() {
			return getRuleContext(Analyze_stmtContext.class,0);
		}
		public Attach_stmtContext attach_stmt() {
			return getRuleContext(Attach_stmtContext.class,0);
		}
		public Begin_stmtContext begin_stmt() {
			return getRuleContext(Begin_stmtContext.class,0);
		}
		public Commit_stmtContext commit_stmt() {
			return getRuleContext(Commit_stmtContext.class,0);
		}
		public Compound_select_stmtContext compound_select_stmt() {
			return getRuleContext(Compound_select_stmtContext.class,0);
		}
		public Create_index_stmtContext create_index_stmt() {
			return getRuleContext(Create_index_stmtContext.class,0);
		}
		public Create_table_stmtContext create_table_stmt() {
			return getRuleContext(Create_table_stmtContext.class,0);
		}
		public Create_trigger_stmtContext create_trigger_stmt() {
			return getRuleContext(Create_trigger_stmtContext.class,0);
		}
		public Create_view_stmtContext create_view_stmt() {
			return getRuleContext(Create_view_stmtContext.class,0);
		}
		public Create_virtual_table_stmtContext create_virtual_table_stmt() {
			return getRuleContext(Create_virtual_table_stmtContext.class,0);
		}
		public Delete_stmtContext delete_stmt() {
			return getRuleContext(Delete_stmtContext.class,0);
		}
		public Delete_stmt_limitedContext delete_stmt_limited() {
			return getRuleContext(Delete_stmt_limitedContext.class,0);
		}
		public Detach_stmtContext detach_stmt() {
			return getRuleContext(Detach_stmtContext.class,0);
		}
		public Drop_index_stmtContext drop_index_stmt() {
			return getRuleContext(Drop_index_stmtContext.class,0);
		}
		public Drop_table_stmtContext drop_table_stmt() {
			return getRuleContext(Drop_table_stmtContext.class,0);
		}
		public Drop_trigger_stmtContext drop_trigger_stmt() {
			return getRuleContext(Drop_trigger_stmtContext.class,0);
		}
		public Drop_view_stmtContext drop_view_stmt() {
			return getRuleContext(Drop_view_stmtContext.class,0);
		}
		public Factored_select_stmtContext factored_select_stmt() {
			return getRuleContext(Factored_select_stmtContext.class,0);
		}
		public Insert_stmtContext insert_stmt() {
			return getRuleContext(Insert_stmtContext.class,0);
		}
		public Pragma_stmtContext pragma_stmt() {
			return getRuleContext(Pragma_stmtContext.class,0);
		}
		public Reindex_stmtContext reindex_stmt() {
			return getRuleContext(Reindex_stmtContext.class,0);
		}
		public Release_stmtContext release_stmt() {
			return getRuleContext(Release_stmtContext.class,0);
		}
		public Rollback_stmtContext rollback_stmt() {
			return getRuleContext(Rollback_stmtContext.class,0);
		}
		public Savepoint_stmtContext savepoint_stmt() {
			return getRuleContext(Savepoint_stmtContext.class,0);
		}
		public Simple_select_stmtContext simple_select_stmt() {
			return getRuleContext(Simple_select_stmtContext.class,0);
		}
		public Select_stmtContext select_stmt() {
			return getRuleContext(Select_stmtContext.class,0);
		}
		public Update_stmtContext update_stmt() {
			return getRuleContext(Update_stmtContext.class,0);
		}
		public Update_stmt_limitedContext update_stmt_limited() {
			return getRuleContext(Update_stmt_limitedContext.class,0);
		}
		public Vacuum_stmtContext vacuum_stmt() {
			return getRuleContext(Vacuum_stmtContext.class,0);
		}
		public TerminalNode K_EXPLAIN() { return getToken(SQLiteParser.K_EXPLAIN, 0); }
		public TerminalNode K_QUERY() { return getToken(SQLiteParser.K_QUERY, 0); }
		public TerminalNode K_PLAN() { return getToken(SQLiteParser.K_PLAN, 0); }
		public Sql_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sql_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSql_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSql_stmt(this);
		}
	}

	public final Sql_stmtContext sql_stmt() throws RecognitionException {
		Sql_stmtContext _localctx = new Sql_stmtContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_sql_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(207);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_EXPLAIN) {
				{
				setState(202);
				match(K_EXPLAIN);
				setState(205);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_QUERY) {
					{
					setState(203);
					match(K_QUERY);
					setState(204);
					match(K_PLAN);
					}
				}

				}
			}

			setState(239);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
			case 1:
				{
				setState(209);
				alter_table_stmt();
				}
				break;
			case 2:
				{
				setState(210);
				analyze_stmt();
				}
				break;
			case 3:
				{
				setState(211);
				attach_stmt();
				}
				break;
			case 4:
				{
				setState(212);
				begin_stmt();
				}
				break;
			case 5:
				{
				setState(213);
				commit_stmt();
				}
				break;
			case 6:
				{
				setState(214);
				compound_select_stmt();
				}
				break;
			case 7:
				{
				setState(215);
				create_index_stmt();
				}
				break;
			case 8:
				{
				setState(216);
				create_table_stmt();
				}
				break;
			case 9:
				{
				setState(217);
				create_trigger_stmt();
				}
				break;
			case 10:
				{
				setState(218);
				create_view_stmt();
				}
				break;
			case 11:
				{
				setState(219);
				create_virtual_table_stmt();
				}
				break;
			case 12:
				{
				setState(220);
				delete_stmt();
				}
				break;
			case 13:
				{
				setState(221);
				delete_stmt_limited();
				}
				break;
			case 14:
				{
				setState(222);
				detach_stmt();
				}
				break;
			case 15:
				{
				setState(223);
				drop_index_stmt();
				}
				break;
			case 16:
				{
				setState(224);
				drop_table_stmt();
				}
				break;
			case 17:
				{
				setState(225);
				drop_trigger_stmt();
				}
				break;
			case 18:
				{
				setState(226);
				drop_view_stmt();
				}
				break;
			case 19:
				{
				setState(227);
				factored_select_stmt();
				}
				break;
			case 20:
				{
				setState(228);
				insert_stmt();
				}
				break;
			case 21:
				{
				setState(229);
				pragma_stmt();
				}
				break;
			case 22:
				{
				setState(230);
				reindex_stmt();
				}
				break;
			case 23:
				{
				setState(231);
				release_stmt();
				}
				break;
			case 24:
				{
				setState(232);
				rollback_stmt();
				}
				break;
			case 25:
				{
				setState(233);
				savepoint_stmt();
				}
				break;
			case 26:
				{
				setState(234);
				simple_select_stmt();
				}
				break;
			case 27:
				{
				setState(235);
				select_stmt();
				}
				break;
			case 28:
				{
				setState(236);
				update_stmt();
				}
				break;
			case 29:
				{
				setState(237);
				update_stmt_limited();
				}
				break;
			case 30:
				{
				setState(238);
				vacuum_stmt();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Alter_table_stmtContext extends ParserRuleContext {
		public TerminalNode K_ALTER() { return getToken(SQLiteParser.K_ALTER, 0); }
		public TerminalNode K_TABLE() { return getToken(SQLiteParser.K_TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode K_RENAME() { return getToken(SQLiteParser.K_RENAME, 0); }
		public TerminalNode K_TO() { return getToken(SQLiteParser.K_TO, 0); }
		public New_table_nameContext new_table_name() {
			return getRuleContext(New_table_nameContext.class,0);
		}
		public TerminalNode K_ADD() { return getToken(SQLiteParser.K_ADD, 0); }
		public Column_defContext column_def() {
			return getRuleContext(Column_defContext.class,0);
		}
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode DOT() { return getToken(SQLiteParser.DOT, 0); }
		public TerminalNode K_COLUMN() { return getToken(SQLiteParser.K_COLUMN, 0); }
		public Alter_table_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alter_table_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterAlter_table_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitAlter_table_stmt(this);
		}
	}

	public final Alter_table_stmtContext alter_table_stmt() throws RecognitionException {
		Alter_table_stmtContext _localctx = new Alter_table_stmtContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_alter_table_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(241);
			match(K_ALTER);
			setState(242);
			match(K_TABLE);
			setState(246);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
			case 1:
				{
				setState(243);
				database_name();
				setState(244);
				match(DOT);
				}
				break;
			}
			setState(248);
			table_name();
			setState(257);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case K_RENAME:
				{
				setState(249);
				match(K_RENAME);
				setState(250);
				match(K_TO);
				setState(251);
				new_table_name();
				}
				break;
			case K_ADD:
				{
				setState(252);
				match(K_ADD);
				setState(254);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,10,_ctx) ) {
				case 1:
					{
					setState(253);
					match(K_COLUMN);
					}
					break;
				}
				setState(256);
				column_def();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Analyze_stmtContext extends ParserRuleContext {
		public TerminalNode K_ANALYZE() { return getToken(SQLiteParser.K_ANALYZE, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public Table_or_index_nameContext table_or_index_name() {
			return getRuleContext(Table_or_index_nameContext.class,0);
		}
		public TerminalNode DOT() { return getToken(SQLiteParser.DOT, 0); }
		public Analyze_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_analyze_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterAnalyze_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitAnalyze_stmt(this);
		}
	}

	public final Analyze_stmtContext analyze_stmt() throws RecognitionException {
		Analyze_stmtContext _localctx = new Analyze_stmtContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_analyze_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(259);
			match(K_ANALYZE);
			setState(266);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
			case 1:
				{
				setState(260);
				database_name();
				}
				break;
			case 2:
				{
				setState(261);
				table_or_index_name();
				}
				break;
			case 3:
				{
				setState(262);
				database_name();
				setState(263);
				match(DOT);
				setState(264);
				table_or_index_name();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Attach_stmtContext extends ParserRuleContext {
		public TerminalNode K_ATTACH() { return getToken(SQLiteParser.K_ATTACH, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode K_AS() { return getToken(SQLiteParser.K_AS, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode K_DATABASE() { return getToken(SQLiteParser.K_DATABASE, 0); }
		public Attach_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_attach_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterAttach_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitAttach_stmt(this);
		}
	}

	public final Attach_stmtContext attach_stmt() throws RecognitionException {
		Attach_stmtContext _localctx = new Attach_stmtContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_attach_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(268);
			match(K_ATTACH);
			setState(270);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
			case 1:
				{
				setState(269);
				match(K_DATABASE);
				}
				break;
			}
			setState(272);
			expr(0);
			setState(273);
			match(K_AS);
			setState(274);
			database_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Begin_stmtContext extends ParserRuleContext {
		public TerminalNode K_BEGIN() { return getToken(SQLiteParser.K_BEGIN, 0); }
		public TerminalNode K_TRANSACTION() { return getToken(SQLiteParser.K_TRANSACTION, 0); }
		public TerminalNode K_DEFERRED() { return getToken(SQLiteParser.K_DEFERRED, 0); }
		public TerminalNode K_IMMEDIATE() { return getToken(SQLiteParser.K_IMMEDIATE, 0); }
		public TerminalNode K_EXCLUSIVE() { return getToken(SQLiteParser.K_EXCLUSIVE, 0); }
		public Transaction_nameContext transaction_name() {
			return getRuleContext(Transaction_nameContext.class,0);
		}
		public Begin_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_begin_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterBegin_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitBegin_stmt(this);
		}
	}

	public final Begin_stmtContext begin_stmt() throws RecognitionException {
		Begin_stmtContext _localctx = new Begin_stmtContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_begin_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(276);
			match(K_BEGIN);
			setState(278);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 58)) & ~0x3f) == 0 && ((1L << (_la - 58)) & ((1L << (K_DEFERRED - 58)) | (1L << (K_EXCLUSIVE - 58)) | (1L << (K_IMMEDIATE - 58)))) != 0)) {
				{
				setState(277);
				_la = _input.LA(1);
				if ( !(((((_la - 58)) & ~0x3f) == 0 && ((1L << (_la - 58)) & ((1L << (K_DEFERRED - 58)) | (1L << (K_EXCLUSIVE - 58)) | (1L << (K_IMMEDIATE - 58)))) != 0)) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(284);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_TRANSACTION) {
				{
				setState(280);
				match(K_TRANSACTION);
				setState(282);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
				case 1:
					{
					setState(281);
					transaction_name();
					}
					break;
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Commit_stmtContext extends ParserRuleContext {
		public TerminalNode K_COMMIT() { return getToken(SQLiteParser.K_COMMIT, 0); }
		public TerminalNode K_END() { return getToken(SQLiteParser.K_END, 0); }
		public TerminalNode K_TRANSACTION() { return getToken(SQLiteParser.K_TRANSACTION, 0); }
		public Transaction_nameContext transaction_name() {
			return getRuleContext(Transaction_nameContext.class,0);
		}
		public Commit_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_commit_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCommit_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCommit_stmt(this);
		}
	}

	public final Commit_stmtContext commit_stmt() throws RecognitionException {
		Commit_stmtContext _localctx = new Commit_stmtContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_commit_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(286);
			_la = _input.LA(1);
			if ( !(_la==K_COMMIT || _la==K_END) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(291);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_TRANSACTION) {
				{
				setState(287);
				match(K_TRANSACTION);
				setState(289);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
				case 1:
					{
					setState(288);
					transaction_name();
					}
					break;
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Compound_select_stmtContext extends ParserRuleContext {
		public List<Select_coreContext> select_core() {
			return getRuleContexts(Select_coreContext.class);
		}
		public Select_coreContext select_core(int i) {
			return getRuleContext(Select_coreContext.class,i);
		}
		public With_clauseContext with_clause() {
			return getRuleContext(With_clauseContext.class,0);
		}
		public TerminalNode K_ORDER() { return getToken(SQLiteParser.K_ORDER, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public List<Ordering_termContext> ordering_term() {
			return getRuleContexts(Ordering_termContext.class);
		}
		public Ordering_termContext ordering_term(int i) {
			return getRuleContext(Ordering_termContext.class,i);
		}
		public TerminalNode K_LIMIT() { return getToken(SQLiteParser.K_LIMIT, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> K_UNION() { return getTokens(SQLiteParser.K_UNION); }
		public TerminalNode K_UNION(int i) {
			return getToken(SQLiteParser.K_UNION, i);
		}
		public List<TerminalNode> K_INTERSECT() { return getTokens(SQLiteParser.K_INTERSECT); }
		public TerminalNode K_INTERSECT(int i) {
			return getToken(SQLiteParser.K_INTERSECT, i);
		}
		public List<TerminalNode> K_EXCEPT() { return getTokens(SQLiteParser.K_EXCEPT); }
		public TerminalNode K_EXCEPT(int i) {
			return getToken(SQLiteParser.K_EXCEPT, i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public TerminalNode K_OFFSET() { return getToken(SQLiteParser.K_OFFSET, 0); }
		public List<TerminalNode> K_ALL() { return getTokens(SQLiteParser.K_ALL); }
		public TerminalNode K_ALL(int i) {
			return getToken(SQLiteParser.K_ALL, i);
		}
		public Compound_select_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_compound_select_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCompound_select_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCompound_select_stmt(this);
		}
	}

	public final Compound_select_stmtContext compound_select_stmt() throws RecognitionException {
		Compound_select_stmtContext _localctx = new Compound_select_stmtContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_compound_select_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(294);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WITH) {
				{
				setState(293);
				with_clause();
				}
			}

			setState(296);
			select_core();
			setState(306); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(303);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case K_UNION:
					{
					setState(297);
					match(K_UNION);
					setState(299);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==K_ALL) {
						{
						setState(298);
						match(K_ALL);
						}
					}

					}
					break;
				case K_INTERSECT:
					{
					setState(301);
					match(K_INTERSECT);
					}
					break;
				case K_EXCEPT:
					{
					setState(302);
					match(K_EXCEPT);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(305);
				select_core();
				}
				}
				setState(308); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==K_EXCEPT || _la==K_INTERSECT || _la==K_UNION );
			setState(320);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_ORDER) {
				{
				setState(310);
				match(K_ORDER);
				setState(311);
				match(K_BY);
				setState(312);
				ordering_term();
				setState(317);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(313);
					match(COMMA);
					setState(314);
					ordering_term();
					}
					}
					setState(319);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(328);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_LIMIT) {
				{
				setState(322);
				match(K_LIMIT);
				setState(323);
				expr(0);
				setState(326);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA || _la==K_OFFSET) {
					{
					setState(324);
					_la = _input.LA(1);
					if ( !(_la==COMMA || _la==K_OFFSET) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(325);
					expr(0);
					}
				}

				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Create_index_stmtContext extends ParserRuleContext {
		public TerminalNode K_CREATE() { return getToken(SQLiteParser.K_CREATE, 0); }
		public TerminalNode K_INDEX() { return getToken(SQLiteParser.K_INDEX, 0); }
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public TerminalNode K_ON() { return getToken(SQLiteParser.K_ON, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode OPEN_PAR() { return getToken(SQLiteParser.OPEN_PAR, 0); }
		public List<Indexed_columnContext> indexed_column() {
			return getRuleContexts(Indexed_columnContext.class);
		}
		public Indexed_columnContext indexed_column(int i) {
			return getRuleContext(Indexed_columnContext.class,i);
		}
		public TerminalNode CLOSE_PAR() { return getToken(SQLiteParser.CLOSE_PAR, 0); }
		public TerminalNode K_UNIQUE() { return getToken(SQLiteParser.K_UNIQUE, 0); }
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode DOT() { return getToken(SQLiteParser.DOT, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public TerminalNode K_WHERE() { return getToken(SQLiteParser.K_WHERE, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Create_index_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_index_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCreate_index_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCreate_index_stmt(this);
		}
	}

	public final Create_index_stmtContext create_index_stmt() throws RecognitionException {
		Create_index_stmtContext _localctx = new Create_index_stmtContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_create_index_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(330);
			match(K_CREATE);
			setState(332);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_UNIQUE) {
				{
				setState(331);
				match(K_UNIQUE);
				}
			}

			setState(334);
			match(K_INDEX);
			setState(338);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
			case 1:
				{
				setState(335);
				match(K_IF);
				setState(336);
				match(K_NOT);
				setState(337);
				match(K_EXISTS);
				}
				break;
			}
			setState(343);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
			case 1:
				{
				setState(340);
				database_name();
				setState(341);
				match(DOT);
				}
				break;
			}
			setState(345);
			index_name();
			setState(346);
			match(K_ON);
			setState(347);
			table_name();
			setState(348);
			match(OPEN_PAR);
			setState(349);
			indexed_column();
			setState(354);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(350);
				match(COMMA);
				setState(351);
				indexed_column();
				}
				}
				setState(356);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(357);
			match(CLOSE_PAR);
			setState(360);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WHERE) {
				{
				setState(358);
				match(K_WHERE);
				setState(359);
				expr(0);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Create_table_stmtContext extends ParserRuleContext {
		public TerminalNode K_CREATE() { return getToken(SQLiteParser.K_CREATE, 0); }
		public TerminalNode K_TABLE() { return getToken(SQLiteParser.K_TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode OPEN_PAR() { return getToken(SQLiteParser.OPEN_PAR, 0); }
		public List<Column_defContext> column_def() {
			return getRuleContexts(Column_defContext.class);
		}
		public Column_defContext column_def(int i) {
			return getRuleContext(Column_defContext.class,i);
		}
		public TerminalNode CLOSE_PAR() { return getToken(SQLiteParser.CLOSE_PAR, 0); }
		public TerminalNode K_AS() { return getToken(SQLiteParser.K_AS, 0); }
		public Select_stmtContext select_stmt() {
			return getRuleContext(Select_stmtContext.class,0);
		}
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode DOT() { return getToken(SQLiteParser.DOT, 0); }
		public TerminalNode K_TEMP() { return getToken(SQLiteParser.K_TEMP, 0); }
		public TerminalNode K_TEMPORARY() { return getToken(SQLiteParser.K_TEMPORARY, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public List<Table_constraintContext> table_constraint() {
			return getRuleContexts(Table_constraintContext.class);
		}
		public Table_constraintContext table_constraint(int i) {
			return getRuleContext(Table_constraintContext.class,i);
		}
		public TerminalNode K_WITHOUT() { return getToken(SQLiteParser.K_WITHOUT, 0); }
		public TerminalNode IDENTIFIER() { return getToken(SQLiteParser.IDENTIFIER, 0); }
		public Create_table_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_table_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCreate_table_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCreate_table_stmt(this);
		}
	}

	public final Create_table_stmtContext create_table_stmt() throws RecognitionException {
		Create_table_stmtContext _localctx = new Create_table_stmtContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_create_table_stmt);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(362);
			match(K_CREATE);
			setState(364);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_TEMP || _la==K_TEMPORARY) {
				{
				setState(363);
				_la = _input.LA(1);
				if ( !(_la==K_TEMP || _la==K_TEMPORARY) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(366);
			match(K_TABLE);
			setState(370);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
			case 1:
				{
				setState(367);
				match(K_IF);
				setState(368);
				match(K_NOT);
				setState(369);
				match(K_EXISTS);
				}
				break;
			}
			setState(375);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
			case 1:
				{
				setState(372);
				database_name();
				setState(373);
				match(DOT);
				}
				break;
			}
			setState(377);
			table_name();
			setState(401);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case OPEN_PAR:
				{
				setState(378);
				match(OPEN_PAR);
				setState(379);
				column_def();
				setState(384);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,35,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(380);
						match(COMMA);
						setState(381);
						column_def();
						}
						} 
					}
					setState(386);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,35,_ctx);
				}
				setState(391);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(387);
					match(COMMA);
					setState(388);
					table_constraint();
					}
					}
					setState(393);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(394);
				match(CLOSE_PAR);
				setState(397);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WITHOUT) {
					{
					setState(395);
					match(K_WITHOUT);
					setState(396);
					match(IDENTIFIER);
					}
				}

				}
				break;
			case K_AS:
				{
				setState(399);
				match(K_AS);
				setState(400);
				select_stmt();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Create_trigger_stmtContext extends ParserRuleContext {
		public TerminalNode K_CREATE() { return getToken(SQLiteParser.K_CREATE, 0); }
		public TerminalNode K_TRIGGER() { return getToken(SQLiteParser.K_TRIGGER, 0); }
		public Trigger_nameContext trigger_name() {
			return getRuleContext(Trigger_nameContext.class,0);
		}
		public TerminalNode K_ON() { return getToken(SQLiteParser.K_ON, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode K_BEGIN() { return getToken(SQLiteParser.K_BEGIN, 0); }
		public TerminalNode K_END() { return getToken(SQLiteParser.K_END, 0); }
		public TerminalNode K_DELETE() { return getToken(SQLiteParser.K_DELETE, 0); }
		public TerminalNode K_INSERT() { return getToken(SQLiteParser.K_INSERT, 0); }
		public TerminalNode K_UPDATE() { return getToken(SQLiteParser.K_UPDATE, 0); }
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public List<Database_nameContext> database_name() {
			return getRuleContexts(Database_nameContext.class);
		}
		public Database_nameContext database_name(int i) {
			return getRuleContext(Database_nameContext.class,i);
		}
		public List<TerminalNode> DOT() { return getTokens(SQLiteParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(SQLiteParser.DOT, i);
		}
		public TerminalNode K_BEFORE() { return getToken(SQLiteParser.K_BEFORE, 0); }
		public TerminalNode K_AFTER() { return getToken(SQLiteParser.K_AFTER, 0); }
		public TerminalNode K_INSTEAD() { return getToken(SQLiteParser.K_INSTEAD, 0); }
		public List<TerminalNode> K_OF() { return getTokens(SQLiteParser.K_OF); }
		public TerminalNode K_OF(int i) {
			return getToken(SQLiteParser.K_OF, i);
		}
		public TerminalNode K_FOR() { return getToken(SQLiteParser.K_FOR, 0); }
		public TerminalNode K_EACH() { return getToken(SQLiteParser.K_EACH, 0); }
		public TerminalNode K_ROW() { return getToken(SQLiteParser.K_ROW, 0); }
		public TerminalNode K_WHEN() { return getToken(SQLiteParser.K_WHEN, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public List<TerminalNode> SCOL() { return getTokens(SQLiteParser.SCOL); }
		public TerminalNode SCOL(int i) {
			return getToken(SQLiteParser.SCOL, i);
		}
		public TerminalNode K_TEMP() { return getToken(SQLiteParser.K_TEMP, 0); }
		public TerminalNode K_TEMPORARY() { return getToken(SQLiteParser.K_TEMPORARY, 0); }
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public List<Update_stmtContext> update_stmt() {
			return getRuleContexts(Update_stmtContext.class);
		}
		public Update_stmtContext update_stmt(int i) {
			return getRuleContext(Update_stmtContext.class,i);
		}
		public List<Insert_stmtContext> insert_stmt() {
			return getRuleContexts(Insert_stmtContext.class);
		}
		public Insert_stmtContext insert_stmt(int i) {
			return getRuleContext(Insert_stmtContext.class,i);
		}
		public List<Delete_stmtContext> delete_stmt() {
			return getRuleContexts(Delete_stmtContext.class);
		}
		public Delete_stmtContext delete_stmt(int i) {
			return getRuleContext(Delete_stmtContext.class,i);
		}
		public List<Select_stmtContext> select_stmt() {
			return getRuleContexts(Select_stmtContext.class);
		}
		public Select_stmtContext select_stmt(int i) {
			return getRuleContext(Select_stmtContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public Create_trigger_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_trigger_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCreate_trigger_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCreate_trigger_stmt(this);
		}
	}

	public final Create_trigger_stmtContext create_trigger_stmt() throws RecognitionException {
		Create_trigger_stmtContext _localctx = new Create_trigger_stmtContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_create_trigger_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(403);
			match(K_CREATE);
			setState(405);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_TEMP || _la==K_TEMPORARY) {
				{
				setState(404);
				_la = _input.LA(1);
				if ( !(_la==K_TEMP || _la==K_TEMPORARY) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(407);
			match(K_TRIGGER);
			setState(411);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,40,_ctx) ) {
			case 1:
				{
				setState(408);
				match(K_IF);
				setState(409);
				match(K_NOT);
				setState(410);
				match(K_EXISTS);
				}
				break;
			}
			setState(416);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,41,_ctx) ) {
			case 1:
				{
				setState(413);
				database_name();
				setState(414);
				match(DOT);
				}
				break;
			}
			setState(418);
			trigger_name();
			setState(423);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case K_BEFORE:
				{
				setState(419);
				match(K_BEFORE);
				}
				break;
			case K_AFTER:
				{
				setState(420);
				match(K_AFTER);
				}
				break;
			case K_INSTEAD:
				{
				setState(421);
				match(K_INSTEAD);
				setState(422);
				match(K_OF);
				}
				break;
			case K_DELETE:
			case K_INSERT:
			case K_UPDATE:
				break;
			default:
				break;
			}
			setState(439);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case K_DELETE:
				{
				setState(425);
				match(K_DELETE);
				}
				break;
			case K_INSERT:
				{
				setState(426);
				match(K_INSERT);
				}
				break;
			case K_UPDATE:
				{
				setState(427);
				match(K_UPDATE);
				setState(437);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_OF) {
					{
					setState(428);
					match(K_OF);
					setState(429);
					column_name();
					setState(434);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(430);
						match(COMMA);
						setState(431);
						column_name();
						}
						}
						setState(436);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(441);
			match(K_ON);
			setState(445);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,46,_ctx) ) {
			case 1:
				{
				setState(442);
				database_name();
				setState(443);
				match(DOT);
				}
				break;
			}
			setState(447);
			table_name();
			setState(451);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_FOR) {
				{
				setState(448);
				match(K_FOR);
				setState(449);
				match(K_EACH);
				setState(450);
				match(K_ROW);
				}
			}

			setState(455);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WHEN) {
				{
				setState(453);
				match(K_WHEN);
				setState(454);
				expr(0);
				}
			}

			setState(457);
			match(K_BEGIN);
			setState(466); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(462);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,49,_ctx) ) {
				case 1:
					{
					setState(458);
					update_stmt();
					}
					break;
				case 2:
					{
					setState(459);
					insert_stmt();
					}
					break;
				case 3:
					{
					setState(460);
					delete_stmt();
					}
					break;
				case 4:
					{
					setState(461);
					select_stmt();
					}
					break;
				}
				setState(464);
				match(SCOL);
				}
				}
				setState(468); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==K_DELETE || ((((_la - 88)) & ~0x3f) == 0 && ((1L << (_la - 88)) & ((1L << (K_INSERT - 88)) | (1L << (K_REPLACE - 88)) | (1L << (K_SELECT - 88)) | (1L << (K_UPDATE - 88)) | (1L << (K_VALUES - 88)) | (1L << (K_WITH - 88)))) != 0) );
			setState(470);
			match(K_END);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Create_view_stmtContext extends ParserRuleContext {
		public TerminalNode K_CREATE() { return getToken(SQLiteParser.K_CREATE, 0); }
		public TerminalNode K_VIEW() { return getToken(SQLiteParser.K_VIEW, 0); }
		public View_nameContext view_name() {
			return getRuleContext(View_nameContext.class,0);
		}
		public TerminalNode K_AS() { return getToken(SQLiteParser.K_AS, 0); }
		public Select_stmtContext select_stmt() {
			return getRuleContext(Select_stmtContext.class,0);
		}
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode DOT() { return getToken(SQLiteParser.DOT, 0); }
		public TerminalNode K_TEMP() { return getToken(SQLiteParser.K_TEMP, 0); }
		public TerminalNode K_TEMPORARY() { return getToken(SQLiteParser.K_TEMPORARY, 0); }
		public Create_view_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_view_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCreate_view_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCreate_view_stmt(this);
		}
	}

	public final Create_view_stmtContext create_view_stmt() throws RecognitionException {
		Create_view_stmtContext _localctx = new Create_view_stmtContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_create_view_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(472);
			match(K_CREATE);
			setState(474);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_TEMP || _la==K_TEMPORARY) {
				{
				setState(473);
				_la = _input.LA(1);
				if ( !(_la==K_TEMP || _la==K_TEMPORARY) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(476);
			match(K_VIEW);
			setState(480);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,52,_ctx) ) {
			case 1:
				{
				setState(477);
				match(K_IF);
				setState(478);
				match(K_NOT);
				setState(479);
				match(K_EXISTS);
				}
				break;
			}
			setState(485);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,53,_ctx) ) {
			case 1:
				{
				setState(482);
				database_name();
				setState(483);
				match(DOT);
				}
				break;
			}
			setState(487);
			view_name();
			setState(488);
			match(K_AS);
			setState(489);
			select_stmt();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Create_virtual_table_stmtContext extends ParserRuleContext {
		public TerminalNode K_CREATE() { return getToken(SQLiteParser.K_CREATE, 0); }
		public TerminalNode K_VIRTUAL() { return getToken(SQLiteParser.K_VIRTUAL, 0); }
		public TerminalNode K_TABLE() { return getToken(SQLiteParser.K_TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode K_USING() { return getToken(SQLiteParser.K_USING, 0); }
		public Module_nameContext module_name() {
			return getRuleContext(Module_nameContext.class,0);
		}
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode DOT() { return getToken(SQLiteParser.DOT, 0); }
		public TerminalNode OPEN_PAR() { return getToken(SQLiteParser.OPEN_PAR, 0); }
		public List<Module_argumentContext> module_argument() {
			return getRuleContexts(Module_argumentContext.class);
		}
		public Module_argumentContext module_argument(int i) {
			return getRuleContext(Module_argumentContext.class,i);
		}
		public TerminalNode CLOSE_PAR() { return getToken(SQLiteParser.CLOSE_PAR, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public Create_virtual_table_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_virtual_table_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCreate_virtual_table_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCreate_virtual_table_stmt(this);
		}
	}

	public final Create_virtual_table_stmtContext create_virtual_table_stmt() throws RecognitionException {
		Create_virtual_table_stmtContext _localctx = new Create_virtual_table_stmtContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_create_virtual_table_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(491);
			match(K_CREATE);
			setState(492);
			match(K_VIRTUAL);
			setState(493);
			match(K_TABLE);
			setState(497);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,54,_ctx) ) {
			case 1:
				{
				setState(494);
				match(K_IF);
				setState(495);
				match(K_NOT);
				setState(496);
				match(K_EXISTS);
				}
				break;
			}
			setState(502);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,55,_ctx) ) {
			case 1:
				{
				setState(499);
				database_name();
				setState(500);
				match(DOT);
				}
				break;
			}
			setState(504);
			table_name();
			setState(505);
			match(K_USING);
			setState(506);
			module_name();
			setState(518);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OPEN_PAR) {
				{
				setState(507);
				match(OPEN_PAR);
				setState(508);
				module_argument();
				setState(513);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(509);
					match(COMMA);
					setState(510);
					module_argument();
					}
					}
					setState(515);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(516);
				match(CLOSE_PAR);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Delete_stmtContext extends ParserRuleContext {
		public TerminalNode K_DELETE() { return getToken(SQLiteParser.K_DELETE, 0); }
		public TerminalNode K_FROM() { return getToken(SQLiteParser.K_FROM, 0); }
		public Qualified_table_nameContext qualified_table_name() {
			return getRuleContext(Qualified_table_nameContext.class,0);
		}
		public With_clauseContext with_clause() {
			return getRuleContext(With_clauseContext.class,0);
		}
		public TerminalNode K_WHERE() { return getToken(SQLiteParser.K_WHERE, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Delete_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_delete_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterDelete_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitDelete_stmt(this);
		}
	}

	public final Delete_stmtContext delete_stmt() throws RecognitionException {
		Delete_stmtContext _localctx = new Delete_stmtContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_delete_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(521);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WITH) {
				{
				setState(520);
				with_clause();
				}
			}

			setState(523);
			match(K_DELETE);
			setState(524);
			match(K_FROM);
			setState(525);
			qualified_table_name();
			setState(528);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WHERE) {
				{
				setState(526);
				match(K_WHERE);
				setState(527);
				expr(0);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Delete_stmt_limitedContext extends ParserRuleContext {
		public TerminalNode K_DELETE() { return getToken(SQLiteParser.K_DELETE, 0); }
		public TerminalNode K_FROM() { return getToken(SQLiteParser.K_FROM, 0); }
		public Qualified_table_nameContext qualified_table_name() {
			return getRuleContext(Qualified_table_nameContext.class,0);
		}
		public With_clauseContext with_clause() {
			return getRuleContext(With_clauseContext.class,0);
		}
		public TerminalNode K_WHERE() { return getToken(SQLiteParser.K_WHERE, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode K_LIMIT() { return getToken(SQLiteParser.K_LIMIT, 0); }
		public TerminalNode K_ORDER() { return getToken(SQLiteParser.K_ORDER, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public List<Ordering_termContext> ordering_term() {
			return getRuleContexts(Ordering_termContext.class);
		}
		public Ordering_termContext ordering_term(int i) {
			return getRuleContext(Ordering_termContext.class,i);
		}
		public TerminalNode K_OFFSET() { return getToken(SQLiteParser.K_OFFSET, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public Delete_stmt_limitedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_delete_stmt_limited; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterDelete_stmt_limited(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitDelete_stmt_limited(this);
		}
	}

	public final Delete_stmt_limitedContext delete_stmt_limited() throws RecognitionException {
		Delete_stmt_limitedContext _localctx = new Delete_stmt_limitedContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_delete_stmt_limited);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(531);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WITH) {
				{
				setState(530);
				with_clause();
				}
			}

			setState(533);
			match(K_DELETE);
			setState(534);
			match(K_FROM);
			setState(535);
			qualified_table_name();
			setState(538);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WHERE) {
				{
				setState(536);
				match(K_WHERE);
				setState(537);
				expr(0);
				}
			}

			setState(558);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_LIMIT || _la==K_ORDER) {
				{
				setState(550);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_ORDER) {
					{
					setState(540);
					match(K_ORDER);
					setState(541);
					match(K_BY);
					setState(542);
					ordering_term();
					setState(547);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(543);
						match(COMMA);
						setState(544);
						ordering_term();
						}
						}
						setState(549);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(552);
				match(K_LIMIT);
				setState(553);
				expr(0);
				setState(556);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA || _la==K_OFFSET) {
					{
					setState(554);
					_la = _input.LA(1);
					if ( !(_la==COMMA || _la==K_OFFSET) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(555);
					expr(0);
					}
				}

				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Detach_stmtContext extends ParserRuleContext {
		public TerminalNode K_DETACH() { return getToken(SQLiteParser.K_DETACH, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode K_DATABASE() { return getToken(SQLiteParser.K_DATABASE, 0); }
		public Detach_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_detach_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterDetach_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitDetach_stmt(this);
		}
	}

	public final Detach_stmtContext detach_stmt() throws RecognitionException {
		Detach_stmtContext _localctx = new Detach_stmtContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_detach_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(560);
			match(K_DETACH);
			setState(562);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,66,_ctx) ) {
			case 1:
				{
				setState(561);
				match(K_DATABASE);
				}
				break;
			}
			setState(564);
			database_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Drop_index_stmtContext extends ParserRuleContext {
		public TerminalNode K_DROP() { return getToken(SQLiteParser.K_DROP, 0); }
		public TerminalNode K_INDEX() { return getToken(SQLiteParser.K_INDEX, 0); }
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode DOT() { return getToken(SQLiteParser.DOT, 0); }
		public Drop_index_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_index_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterDrop_index_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitDrop_index_stmt(this);
		}
	}

	public final Drop_index_stmtContext drop_index_stmt() throws RecognitionException {
		Drop_index_stmtContext _localctx = new Drop_index_stmtContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_drop_index_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(566);
			match(K_DROP);
			setState(567);
			match(K_INDEX);
			setState(570);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,67,_ctx) ) {
			case 1:
				{
				setState(568);
				match(K_IF);
				setState(569);
				match(K_EXISTS);
				}
				break;
			}
			setState(575);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,68,_ctx) ) {
			case 1:
				{
				setState(572);
				database_name();
				setState(573);
				match(DOT);
				}
				break;
			}
			setState(577);
			index_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Drop_table_stmtContext extends ParserRuleContext {
		public TerminalNode K_DROP() { return getToken(SQLiteParser.K_DROP, 0); }
		public TerminalNode K_TABLE() { return getToken(SQLiteParser.K_TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode DOT() { return getToken(SQLiteParser.DOT, 0); }
		public Drop_table_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_table_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterDrop_table_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitDrop_table_stmt(this);
		}
	}

	public final Drop_table_stmtContext drop_table_stmt() throws RecognitionException {
		Drop_table_stmtContext _localctx = new Drop_table_stmtContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_drop_table_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(579);
			match(K_DROP);
			setState(580);
			match(K_TABLE);
			setState(583);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,69,_ctx) ) {
			case 1:
				{
				setState(581);
				match(K_IF);
				setState(582);
				match(K_EXISTS);
				}
				break;
			}
			setState(588);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
			case 1:
				{
				setState(585);
				database_name();
				setState(586);
				match(DOT);
				}
				break;
			}
			setState(590);
			table_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Drop_trigger_stmtContext extends ParserRuleContext {
		public TerminalNode K_DROP() { return getToken(SQLiteParser.K_DROP, 0); }
		public TerminalNode K_TRIGGER() { return getToken(SQLiteParser.K_TRIGGER, 0); }
		public Trigger_nameContext trigger_name() {
			return getRuleContext(Trigger_nameContext.class,0);
		}
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode DOT() { return getToken(SQLiteParser.DOT, 0); }
		public Drop_trigger_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_trigger_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterDrop_trigger_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitDrop_trigger_stmt(this);
		}
	}

	public final Drop_trigger_stmtContext drop_trigger_stmt() throws RecognitionException {
		Drop_trigger_stmtContext _localctx = new Drop_trigger_stmtContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_drop_trigger_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(592);
			match(K_DROP);
			setState(593);
			match(K_TRIGGER);
			setState(596);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,71,_ctx) ) {
			case 1:
				{
				setState(594);
				match(K_IF);
				setState(595);
				match(K_EXISTS);
				}
				break;
			}
			setState(601);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
			case 1:
				{
				setState(598);
				database_name();
				setState(599);
				match(DOT);
				}
				break;
			}
			setState(603);
			trigger_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Drop_view_stmtContext extends ParserRuleContext {
		public TerminalNode K_DROP() { return getToken(SQLiteParser.K_DROP, 0); }
		public TerminalNode K_VIEW() { return getToken(SQLiteParser.K_VIEW, 0); }
		public View_nameContext view_name() {
			return getRuleContext(View_nameContext.class,0);
		}
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode DOT() { return getToken(SQLiteParser.DOT, 0); }
		public Drop_view_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_view_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterDrop_view_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitDrop_view_stmt(this);
		}
	}

	public final Drop_view_stmtContext drop_view_stmt() throws RecognitionException {
		Drop_view_stmtContext _localctx = new Drop_view_stmtContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_drop_view_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(605);
			match(K_DROP);
			setState(606);
			match(K_VIEW);
			setState(609);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,73,_ctx) ) {
			case 1:
				{
				setState(607);
				match(K_IF);
				setState(608);
				match(K_EXISTS);
				}
				break;
			}
			setState(614);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,74,_ctx) ) {
			case 1:
				{
				setState(611);
				database_name();
				setState(612);
				match(DOT);
				}
				break;
			}
			setState(616);
			view_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Factored_select_stmtContext extends ParserRuleContext {
		public List<Select_coreContext> select_core() {
			return getRuleContexts(Select_coreContext.class);
		}
		public Select_coreContext select_core(int i) {
			return getRuleContext(Select_coreContext.class,i);
		}
		public With_clauseContext with_clause() {
			return getRuleContext(With_clauseContext.class,0);
		}
		public List<Compound_operatorContext> compound_operator() {
			return getRuleContexts(Compound_operatorContext.class);
		}
		public Compound_operatorContext compound_operator(int i) {
			return getRuleContext(Compound_operatorContext.class,i);
		}
		public TerminalNode K_ORDER() { return getToken(SQLiteParser.K_ORDER, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public List<Ordering_termContext> ordering_term() {
			return getRuleContexts(Ordering_termContext.class);
		}
		public Ordering_termContext ordering_term(int i) {
			return getRuleContext(Ordering_termContext.class,i);
		}
		public TerminalNode K_LIMIT() { return getToken(SQLiteParser.K_LIMIT, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public TerminalNode K_OFFSET() { return getToken(SQLiteParser.K_OFFSET, 0); }
		public Factored_select_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_factored_select_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterFactored_select_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitFactored_select_stmt(this);
		}
	}

	public final Factored_select_stmtContext factored_select_stmt() throws RecognitionException {
		Factored_select_stmtContext _localctx = new Factored_select_stmtContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_factored_select_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(619);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WITH) {
				{
				setState(618);
				with_clause();
				}
			}

			setState(621);
			select_core();
			setState(627);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==K_EXCEPT || _la==K_INTERSECT || _la==K_UNION) {
				{
				{
				setState(622);
				compound_operator();
				setState(623);
				select_core();
				}
				}
				setState(629);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(640);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_ORDER) {
				{
				setState(630);
				match(K_ORDER);
				setState(631);
				match(K_BY);
				setState(632);
				ordering_term();
				setState(637);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(633);
					match(COMMA);
					setState(634);
					ordering_term();
					}
					}
					setState(639);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(648);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_LIMIT) {
				{
				setState(642);
				match(K_LIMIT);
				setState(643);
				expr(0);
				setState(646);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA || _la==K_OFFSET) {
					{
					setState(644);
					_la = _input.LA(1);
					if ( !(_la==COMMA || _la==K_OFFSET) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(645);
					expr(0);
					}
				}

				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Insert_stmtContext extends ParserRuleContext {
		public TerminalNode K_INTO() { return getToken(SQLiteParser.K_INTO, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode K_INSERT() { return getToken(SQLiteParser.K_INSERT, 0); }
		public TerminalNode K_REPLACE() { return getToken(SQLiteParser.K_REPLACE, 0); }
		public TerminalNode K_OR() { return getToken(SQLiteParser.K_OR, 0); }
		public TerminalNode K_ROLLBACK() { return getToken(SQLiteParser.K_ROLLBACK, 0); }
		public TerminalNode K_ABORT() { return getToken(SQLiteParser.K_ABORT, 0); }
		public TerminalNode K_FAIL() { return getToken(SQLiteParser.K_FAIL, 0); }
		public TerminalNode K_IGNORE() { return getToken(SQLiteParser.K_IGNORE, 0); }
		public TerminalNode K_VALUES() { return getToken(SQLiteParser.K_VALUES, 0); }
		public List<TerminalNode> OPEN_PAR() { return getTokens(SQLiteParser.OPEN_PAR); }
		public TerminalNode OPEN_PAR(int i) {
			return getToken(SQLiteParser.OPEN_PAR, i);
		}
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> CLOSE_PAR() { return getTokens(SQLiteParser.CLOSE_PAR); }
		public TerminalNode CLOSE_PAR(int i) {
			return getToken(SQLiteParser.CLOSE_PAR, i);
		}
		public Select_stmtContext select_stmt() {
			return getRuleContext(Select_stmtContext.class,0);
		}
		public TerminalNode K_DEFAULT() { return getToken(SQLiteParser.K_DEFAULT, 0); }
		public With_clauseContext with_clause() {
			return getRuleContext(With_clauseContext.class,0);
		}
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode DOT() { return getToken(SQLiteParser.DOT, 0); }
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public Insert_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insert_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterInsert_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitInsert_stmt(this);
		}
	}

	public final Insert_stmtContext insert_stmt() throws RecognitionException {
		Insert_stmtContext _localctx = new Insert_stmtContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_insert_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(651);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WITH) {
				{
				setState(650);
				with_clause();
				}
			}

			setState(670);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,82,_ctx) ) {
			case 1:
				{
				setState(653);
				match(K_INSERT);
				}
				break;
			case 2:
				{
				setState(654);
				match(K_REPLACE);
				}
				break;
			case 3:
				{
				setState(655);
				match(K_INSERT);
				setState(656);
				match(K_OR);
				setState(657);
				match(K_REPLACE);
				}
				break;
			case 4:
				{
				setState(658);
				match(K_INSERT);
				setState(659);
				match(K_OR);
				setState(660);
				match(K_ROLLBACK);
				}
				break;
			case 5:
				{
				setState(661);
				match(K_INSERT);
				setState(662);
				match(K_OR);
				setState(663);
				match(K_ABORT);
				}
				break;
			case 6:
				{
				setState(664);
				match(K_INSERT);
				setState(665);
				match(K_OR);
				setState(666);
				match(K_FAIL);
				}
				break;
			case 7:
				{
				setState(667);
				match(K_INSERT);
				setState(668);
				match(K_OR);
				setState(669);
				match(K_IGNORE);
				}
				break;
			}
			setState(672);
			match(K_INTO);
			setState(676);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,83,_ctx) ) {
			case 1:
				{
				setState(673);
				database_name();
				setState(674);
				match(DOT);
				}
				break;
			}
			setState(678);
			table_name();
			setState(690);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OPEN_PAR) {
				{
				setState(679);
				match(OPEN_PAR);
				setState(680);
				column_name();
				setState(685);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(681);
					match(COMMA);
					setState(682);
					column_name();
					}
					}
					setState(687);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(688);
				match(CLOSE_PAR);
				}
			}

			setState(723);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,89,_ctx) ) {
			case 1:
				{
				setState(692);
				match(K_VALUES);
				setState(693);
				match(OPEN_PAR);
				setState(694);
				expr(0);
				setState(699);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(695);
					match(COMMA);
					setState(696);
					expr(0);
					}
					}
					setState(701);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(702);
				match(CLOSE_PAR);
				setState(717);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(703);
					match(COMMA);
					setState(704);
					match(OPEN_PAR);
					setState(705);
					expr(0);
					setState(710);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(706);
						match(COMMA);
						setState(707);
						expr(0);
						}
						}
						setState(712);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(713);
					match(CLOSE_PAR);
					}
					}
					setState(719);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 2:
				{
				setState(720);
				select_stmt();
				}
				break;
			case 3:
				{
				setState(721);
				match(K_DEFAULT);
				setState(722);
				match(K_VALUES);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Pragma_stmtContext extends ParserRuleContext {
		public TerminalNode K_PRAGMA() { return getToken(SQLiteParser.K_PRAGMA, 0); }
		public Pragma_nameContext pragma_name() {
			return getRuleContext(Pragma_nameContext.class,0);
		}
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode DOT() { return getToken(SQLiteParser.DOT, 0); }
		public TerminalNode ASSIGN() { return getToken(SQLiteParser.ASSIGN, 0); }
		public Pragma_valueContext pragma_value() {
			return getRuleContext(Pragma_valueContext.class,0);
		}
		public TerminalNode OPEN_PAR() { return getToken(SQLiteParser.OPEN_PAR, 0); }
		public TerminalNode CLOSE_PAR() { return getToken(SQLiteParser.CLOSE_PAR, 0); }
		public Pragma_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pragma_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterPragma_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitPragma_stmt(this);
		}
	}

	public final Pragma_stmtContext pragma_stmt() throws RecognitionException {
		Pragma_stmtContext _localctx = new Pragma_stmtContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_pragma_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(725);
			match(K_PRAGMA);
			setState(729);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,90,_ctx) ) {
			case 1:
				{
				setState(726);
				database_name();
				setState(727);
				match(DOT);
				}
				break;
			}
			setState(731);
			pragma_name();
			setState(738);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ASSIGN:
				{
				setState(732);
				match(ASSIGN);
				setState(733);
				pragma_value();
				}
				break;
			case OPEN_PAR:
				{
				setState(734);
				match(OPEN_PAR);
				setState(735);
				pragma_value();
				setState(736);
				match(CLOSE_PAR);
				}
				break;
			case EOF:
			case SCOL:
			case K_ALTER:
			case K_ANALYZE:
			case K_ATTACH:
			case K_BEGIN:
			case K_COMMIT:
			case K_CREATE:
			case K_DELETE:
			case K_DETACH:
			case K_DROP:
			case K_END:
			case K_EXPLAIN:
			case K_INSERT:
			case K_PRAGMA:
			case K_REINDEX:
			case K_RELEASE:
			case K_REPLACE:
			case K_ROLLBACK:
			case K_SAVEPOINT:
			case K_SELECT:
			case K_UPDATE:
			case K_VACUUM:
			case K_VALUES:
			case K_WITH:
			case UNEXPECTED_CHAR:
				break;
			default:
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Reindex_stmtContext extends ParserRuleContext {
		public TerminalNode K_REINDEX() { return getToken(SQLiteParser.K_REINDEX, 0); }
		public Collation_nameContext collation_name() {
			return getRuleContext(Collation_nameContext.class,0);
		}
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode DOT() { return getToken(SQLiteParser.DOT, 0); }
		public Reindex_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_reindex_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterReindex_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitReindex_stmt(this);
		}
	}

	public final Reindex_stmtContext reindex_stmt() throws RecognitionException {
		Reindex_stmtContext _localctx = new Reindex_stmtContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_reindex_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(740);
			match(K_REINDEX);
			setState(751);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,94,_ctx) ) {
			case 1:
				{
				setState(741);
				collation_name();
				}
				break;
			case 2:
				{
				setState(745);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,92,_ctx) ) {
				case 1:
					{
					setState(742);
					database_name();
					setState(743);
					match(DOT);
					}
					break;
				}
				setState(749);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,93,_ctx) ) {
				case 1:
					{
					setState(747);
					table_name();
					}
					break;
				case 2:
					{
					setState(748);
					index_name();
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Release_stmtContext extends ParserRuleContext {
		public TerminalNode K_RELEASE() { return getToken(SQLiteParser.K_RELEASE, 0); }
		public Savepoint_nameContext savepoint_name() {
			return getRuleContext(Savepoint_nameContext.class,0);
		}
		public TerminalNode K_SAVEPOINT() { return getToken(SQLiteParser.K_SAVEPOINT, 0); }
		public Release_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_release_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterRelease_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitRelease_stmt(this);
		}
	}

	public final Release_stmtContext release_stmt() throws RecognitionException {
		Release_stmtContext _localctx = new Release_stmtContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_release_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(753);
			match(K_RELEASE);
			setState(755);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,95,_ctx) ) {
			case 1:
				{
				setState(754);
				match(K_SAVEPOINT);
				}
				break;
			}
			setState(757);
			savepoint_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Rollback_stmtContext extends ParserRuleContext {
		public TerminalNode K_ROLLBACK() { return getToken(SQLiteParser.K_ROLLBACK, 0); }
		public TerminalNode K_TRANSACTION() { return getToken(SQLiteParser.K_TRANSACTION, 0); }
		public TerminalNode K_TO() { return getToken(SQLiteParser.K_TO, 0); }
		public Savepoint_nameContext savepoint_name() {
			return getRuleContext(Savepoint_nameContext.class,0);
		}
		public Transaction_nameContext transaction_name() {
			return getRuleContext(Transaction_nameContext.class,0);
		}
		public TerminalNode K_SAVEPOINT() { return getToken(SQLiteParser.K_SAVEPOINT, 0); }
		public Rollback_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rollback_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterRollback_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitRollback_stmt(this);
		}
	}

	public final Rollback_stmtContext rollback_stmt() throws RecognitionException {
		Rollback_stmtContext _localctx = new Rollback_stmtContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_rollback_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(759);
			match(K_ROLLBACK);
			setState(764);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_TRANSACTION) {
				{
				setState(760);
				match(K_TRANSACTION);
				setState(762);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,96,_ctx) ) {
				case 1:
					{
					setState(761);
					transaction_name();
					}
					break;
				}
				}
			}

			setState(771);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_TO) {
				{
				setState(766);
				match(K_TO);
				setState(768);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,98,_ctx) ) {
				case 1:
					{
					setState(767);
					match(K_SAVEPOINT);
					}
					break;
				}
				setState(770);
				savepoint_name();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Savepoint_stmtContext extends ParserRuleContext {
		public TerminalNode K_SAVEPOINT() { return getToken(SQLiteParser.K_SAVEPOINT, 0); }
		public Savepoint_nameContext savepoint_name() {
			return getRuleContext(Savepoint_nameContext.class,0);
		}
		public Savepoint_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_savepoint_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSavepoint_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSavepoint_stmt(this);
		}
	}

	public final Savepoint_stmtContext savepoint_stmt() throws RecognitionException {
		Savepoint_stmtContext _localctx = new Savepoint_stmtContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_savepoint_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(773);
			match(K_SAVEPOINT);
			setState(774);
			savepoint_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Simple_select_stmtContext extends ParserRuleContext {
		public Select_coreContext select_core() {
			return getRuleContext(Select_coreContext.class,0);
		}
		public With_clauseContext with_clause() {
			return getRuleContext(With_clauseContext.class,0);
		}
		public TerminalNode K_ORDER() { return getToken(SQLiteParser.K_ORDER, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public List<Ordering_termContext> ordering_term() {
			return getRuleContexts(Ordering_termContext.class);
		}
		public Ordering_termContext ordering_term(int i) {
			return getRuleContext(Ordering_termContext.class,i);
		}
		public TerminalNode K_LIMIT() { return getToken(SQLiteParser.K_LIMIT, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public TerminalNode K_OFFSET() { return getToken(SQLiteParser.K_OFFSET, 0); }
		public Simple_select_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_simple_select_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSimple_select_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSimple_select_stmt(this);
		}
	}

	public final Simple_select_stmtContext simple_select_stmt() throws RecognitionException {
		Simple_select_stmtContext _localctx = new Simple_select_stmtContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_simple_select_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(777);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WITH) {
				{
				setState(776);
				with_clause();
				}
			}

			setState(779);
			select_core();
			setState(790);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_ORDER) {
				{
				setState(780);
				match(K_ORDER);
				setState(781);
				match(K_BY);
				setState(782);
				ordering_term();
				setState(787);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(783);
					match(COMMA);
					setState(784);
					ordering_term();
					}
					}
					setState(789);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(798);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_LIMIT) {
				{
				setState(792);
				match(K_LIMIT);
				setState(793);
				expr(0);
				setState(796);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA || _la==K_OFFSET) {
					{
					setState(794);
					_la = _input.LA(1);
					if ( !(_la==COMMA || _la==K_OFFSET) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(795);
					expr(0);
					}
				}

				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Select_stmtContext extends ParserRuleContext {
		public List<Select_or_valuesContext> select_or_values() {
			return getRuleContexts(Select_or_valuesContext.class);
		}
		public Select_or_valuesContext select_or_values(int i) {
			return getRuleContext(Select_or_valuesContext.class,i);
		}
		public With_clauseContext with_clause() {
			return getRuleContext(With_clauseContext.class,0);
		}
		public List<Compound_operatorContext> compound_operator() {
			return getRuleContexts(Compound_operatorContext.class);
		}
		public Compound_operatorContext compound_operator(int i) {
			return getRuleContext(Compound_operatorContext.class,i);
		}
		public TerminalNode K_ORDER() { return getToken(SQLiteParser.K_ORDER, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public List<Ordering_termContext> ordering_term() {
			return getRuleContexts(Ordering_termContext.class);
		}
		public Ordering_termContext ordering_term(int i) {
			return getRuleContext(Ordering_termContext.class,i);
		}
		public TerminalNode K_LIMIT() { return getToken(SQLiteParser.K_LIMIT, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public TerminalNode K_OFFSET() { return getToken(SQLiteParser.K_OFFSET, 0); }
		public Select_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSelect_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSelect_stmt(this);
		}
	}

	public final Select_stmtContext select_stmt() throws RecognitionException {
		Select_stmtContext _localctx = new Select_stmtContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_select_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(801);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WITH) {
				{
				setState(800);
				with_clause();
				}
			}

			setState(803);
			select_or_values();
			setState(809);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==K_EXCEPT || _la==K_INTERSECT || _la==K_UNION) {
				{
				{
				setState(804);
				compound_operator();
				setState(805);
				select_or_values();
				}
				}
				setState(811);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(822);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_ORDER) {
				{
				setState(812);
				match(K_ORDER);
				setState(813);
				match(K_BY);
				setState(814);
				ordering_term();
				setState(819);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(815);
					match(COMMA);
					setState(816);
					ordering_term();
					}
					}
					setState(821);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(830);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_LIMIT) {
				{
				setState(824);
				match(K_LIMIT);
				setState(825);
				expr(0);
				setState(828);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA || _la==K_OFFSET) {
					{
					setState(826);
					_la = _input.LA(1);
					if ( !(_la==COMMA || _la==K_OFFSET) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(827);
					expr(0);
					}
				}

				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Select_or_valuesContext extends ParserRuleContext {
		public TerminalNode K_SELECT() { return getToken(SQLiteParser.K_SELECT, 0); }
		public List<Result_columnContext> result_column() {
			return getRuleContexts(Result_columnContext.class);
		}
		public Result_columnContext result_column(int i) {
			return getRuleContext(Result_columnContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public TerminalNode K_FROM() { return getToken(SQLiteParser.K_FROM, 0); }
		public TerminalNode K_WHERE() { return getToken(SQLiteParser.K_WHERE, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode K_GROUP() { return getToken(SQLiteParser.K_GROUP, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public TerminalNode K_DISTINCT() { return getToken(SQLiteParser.K_DISTINCT, 0); }
		public TerminalNode K_ALL() { return getToken(SQLiteParser.K_ALL, 0); }
		public List<Table_or_subqueryContext> table_or_subquery() {
			return getRuleContexts(Table_or_subqueryContext.class);
		}
		public Table_or_subqueryContext table_or_subquery(int i) {
			return getRuleContext(Table_or_subqueryContext.class,i);
		}
		public Join_clauseContext join_clause() {
			return getRuleContext(Join_clauseContext.class,0);
		}
		public TerminalNode K_HAVING() { return getToken(SQLiteParser.K_HAVING, 0); }
		public TerminalNode K_VALUES() { return getToken(SQLiteParser.K_VALUES, 0); }
		public List<TerminalNode> OPEN_PAR() { return getTokens(SQLiteParser.OPEN_PAR); }
		public TerminalNode OPEN_PAR(int i) {
			return getToken(SQLiteParser.OPEN_PAR, i);
		}
		public List<TerminalNode> CLOSE_PAR() { return getTokens(SQLiteParser.CLOSE_PAR); }
		public TerminalNode CLOSE_PAR(int i) {
			return getToken(SQLiteParser.CLOSE_PAR, i);
		}
		public Select_or_valuesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_or_values; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSelect_or_values(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSelect_or_values(this);
		}
	}

	public final Select_or_valuesContext select_or_values() throws RecognitionException {
		Select_or_valuesContext _localctx = new Select_or_valuesContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_select_or_values);
		int _la;
		try {
			setState(906);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case K_SELECT:
				enterOuterAlt(_localctx, 1);
				{
				setState(832);
				match(K_SELECT);
				setState(834);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,111,_ctx) ) {
				case 1:
					{
					setState(833);
					_la = _input.LA(1);
					if ( !(_la==K_ALL || _la==K_DISTINCT) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				setState(836);
				result_column();
				setState(841);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(837);
					match(COMMA);
					setState(838);
					result_column();
					}
					}
					setState(843);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(856);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_FROM) {
					{
					setState(844);
					match(K_FROM);
					setState(854);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,114,_ctx) ) {
					case 1:
						{
						setState(845);
						table_or_subquery();
						setState(850);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
							{
							setState(846);
							match(COMMA);
							setState(847);
							table_or_subquery();
							}
							}
							setState(852);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
						break;
					case 2:
						{
						setState(853);
						join_clause();
						}
						break;
					}
					}
				}

				setState(860);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WHERE) {
					{
					setState(858);
					match(K_WHERE);
					setState(859);
					expr(0);
					}
				}

				setState(876);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_GROUP) {
					{
					setState(862);
					match(K_GROUP);
					setState(863);
					match(K_BY);
					setState(864);
					expr(0);
					setState(869);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(865);
						match(COMMA);
						setState(866);
						expr(0);
						}
						}
						setState(871);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(874);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==K_HAVING) {
						{
						setState(872);
						match(K_HAVING);
						setState(873);
						expr(0);
						}
					}

					}
				}

				}
				break;
			case K_VALUES:
				enterOuterAlt(_localctx, 2);
				{
				setState(878);
				match(K_VALUES);
				setState(879);
				match(OPEN_PAR);
				setState(880);
				expr(0);
				setState(885);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(881);
					match(COMMA);
					setState(882);
					expr(0);
					}
					}
					setState(887);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(888);
				match(CLOSE_PAR);
				setState(903);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(889);
					match(COMMA);
					setState(890);
					match(OPEN_PAR);
					setState(891);
					expr(0);
					setState(896);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(892);
						match(COMMA);
						setState(893);
						expr(0);
						}
						}
						setState(898);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(899);
					match(CLOSE_PAR);
					}
					}
					setState(905);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Update_stmtContext extends ParserRuleContext {
		public TerminalNode K_UPDATE() { return getToken(SQLiteParser.K_UPDATE, 0); }
		public Qualified_table_nameContext qualified_table_name() {
			return getRuleContext(Qualified_table_nameContext.class,0);
		}
		public TerminalNode K_SET() { return getToken(SQLiteParser.K_SET, 0); }
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public List<TerminalNode> ASSIGN() { return getTokens(SQLiteParser.ASSIGN); }
		public TerminalNode ASSIGN(int i) {
			return getToken(SQLiteParser.ASSIGN, i);
		}
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public With_clauseContext with_clause() {
			return getRuleContext(With_clauseContext.class,0);
		}
		public TerminalNode K_OR() { return getToken(SQLiteParser.K_OR, 0); }
		public TerminalNode K_ROLLBACK() { return getToken(SQLiteParser.K_ROLLBACK, 0); }
		public TerminalNode K_ABORT() { return getToken(SQLiteParser.K_ABORT, 0); }
		public TerminalNode K_REPLACE() { return getToken(SQLiteParser.K_REPLACE, 0); }
		public TerminalNode K_FAIL() { return getToken(SQLiteParser.K_FAIL, 0); }
		public TerminalNode K_IGNORE() { return getToken(SQLiteParser.K_IGNORE, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public TerminalNode K_WHERE() { return getToken(SQLiteParser.K_WHERE, 0); }
		public Update_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_update_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterUpdate_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitUpdate_stmt(this);
		}
	}

	public final Update_stmtContext update_stmt() throws RecognitionException {
		Update_stmtContext _localctx = new Update_stmtContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_update_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(909);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WITH) {
				{
				setState(908);
				with_clause();
				}
			}

			setState(911);
			match(K_UPDATE);
			setState(922);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,125,_ctx) ) {
			case 1:
				{
				setState(912);
				match(K_OR);
				setState(913);
				match(K_ROLLBACK);
				}
				break;
			case 2:
				{
				setState(914);
				match(K_OR);
				setState(915);
				match(K_ABORT);
				}
				break;
			case 3:
				{
				setState(916);
				match(K_OR);
				setState(917);
				match(K_REPLACE);
				}
				break;
			case 4:
				{
				setState(918);
				match(K_OR);
				setState(919);
				match(K_FAIL);
				}
				break;
			case 5:
				{
				setState(920);
				match(K_OR);
				setState(921);
				match(K_IGNORE);
				}
				break;
			}
			setState(924);
			qualified_table_name();
			setState(925);
			match(K_SET);
			setState(926);
			column_name();
			setState(927);
			match(ASSIGN);
			setState(928);
			expr(0);
			setState(936);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(929);
				match(COMMA);
				setState(930);
				column_name();
				setState(931);
				match(ASSIGN);
				setState(932);
				expr(0);
				}
				}
				setState(938);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(941);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WHERE) {
				{
				setState(939);
				match(K_WHERE);
				setState(940);
				expr(0);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Update_stmt_limitedContext extends ParserRuleContext {
		public TerminalNode K_UPDATE() { return getToken(SQLiteParser.K_UPDATE, 0); }
		public Qualified_table_nameContext qualified_table_name() {
			return getRuleContext(Qualified_table_nameContext.class,0);
		}
		public TerminalNode K_SET() { return getToken(SQLiteParser.K_SET, 0); }
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public List<TerminalNode> ASSIGN() { return getTokens(SQLiteParser.ASSIGN); }
		public TerminalNode ASSIGN(int i) {
			return getToken(SQLiteParser.ASSIGN, i);
		}
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public With_clauseContext with_clause() {
			return getRuleContext(With_clauseContext.class,0);
		}
		public TerminalNode K_OR() { return getToken(SQLiteParser.K_OR, 0); }
		public TerminalNode K_ROLLBACK() { return getToken(SQLiteParser.K_ROLLBACK, 0); }
		public TerminalNode K_ABORT() { return getToken(SQLiteParser.K_ABORT, 0); }
		public TerminalNode K_REPLACE() { return getToken(SQLiteParser.K_REPLACE, 0); }
		public TerminalNode K_FAIL() { return getToken(SQLiteParser.K_FAIL, 0); }
		public TerminalNode K_IGNORE() { return getToken(SQLiteParser.K_IGNORE, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public TerminalNode K_WHERE() { return getToken(SQLiteParser.K_WHERE, 0); }
		public TerminalNode K_LIMIT() { return getToken(SQLiteParser.K_LIMIT, 0); }
		public TerminalNode K_ORDER() { return getToken(SQLiteParser.K_ORDER, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public List<Ordering_termContext> ordering_term() {
			return getRuleContexts(Ordering_termContext.class);
		}
		public Ordering_termContext ordering_term(int i) {
			return getRuleContext(Ordering_termContext.class,i);
		}
		public TerminalNode K_OFFSET() { return getToken(SQLiteParser.K_OFFSET, 0); }
		public Update_stmt_limitedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_update_stmt_limited; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterUpdate_stmt_limited(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitUpdate_stmt_limited(this);
		}
	}

	public final Update_stmt_limitedContext update_stmt_limited() throws RecognitionException {
		Update_stmt_limitedContext _localctx = new Update_stmt_limitedContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_update_stmt_limited);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(944);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WITH) {
				{
				setState(943);
				with_clause();
				}
			}

			setState(946);
			match(K_UPDATE);
			setState(957);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,129,_ctx) ) {
			case 1:
				{
				setState(947);
				match(K_OR);
				setState(948);
				match(K_ROLLBACK);
				}
				break;
			case 2:
				{
				setState(949);
				match(K_OR);
				setState(950);
				match(K_ABORT);
				}
				break;
			case 3:
				{
				setState(951);
				match(K_OR);
				setState(952);
				match(K_REPLACE);
				}
				break;
			case 4:
				{
				setState(953);
				match(K_OR);
				setState(954);
				match(K_FAIL);
				}
				break;
			case 5:
				{
				setState(955);
				match(K_OR);
				setState(956);
				match(K_IGNORE);
				}
				break;
			}
			setState(959);
			qualified_table_name();
			setState(960);
			match(K_SET);
			setState(961);
			column_name();
			setState(962);
			match(ASSIGN);
			setState(963);
			expr(0);
			setState(971);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(964);
				match(COMMA);
				setState(965);
				column_name();
				setState(966);
				match(ASSIGN);
				setState(967);
				expr(0);
				}
				}
				setState(973);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(976);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WHERE) {
				{
				setState(974);
				match(K_WHERE);
				setState(975);
				expr(0);
				}
			}

			setState(996);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_LIMIT || _la==K_ORDER) {
				{
				setState(988);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_ORDER) {
					{
					setState(978);
					match(K_ORDER);
					setState(979);
					match(K_BY);
					setState(980);
					ordering_term();
					setState(985);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(981);
						match(COMMA);
						setState(982);
						ordering_term();
						}
						}
						setState(987);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(990);
				match(K_LIMIT);
				setState(991);
				expr(0);
				setState(994);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA || _la==K_OFFSET) {
					{
					setState(992);
					_la = _input.LA(1);
					if ( !(_la==COMMA || _la==K_OFFSET) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(993);
					expr(0);
					}
				}

				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Vacuum_stmtContext extends ParserRuleContext {
		public TerminalNode K_VACUUM() { return getToken(SQLiteParser.K_VACUUM, 0); }
		public Vacuum_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_vacuum_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterVacuum_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitVacuum_stmt(this);
		}
	}

	public final Vacuum_stmtContext vacuum_stmt() throws RecognitionException {
		Vacuum_stmtContext _localctx = new Vacuum_stmtContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_vacuum_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(998);
			match(K_VACUUM);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_defContext extends ParserRuleContext {
		public Column_nameContext column_name() {
			return getRuleContext(Column_nameContext.class,0);
		}
		public Type_nameContext type_name() {
			return getRuleContext(Type_nameContext.class,0);
		}
		public List<Column_constraintContext> column_constraint() {
			return getRuleContexts(Column_constraintContext.class);
		}
		public Column_constraintContext column_constraint(int i) {
			return getRuleContext(Column_constraintContext.class,i);
		}
		public Column_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterColumn_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitColumn_def(this);
		}
	}

	public final Column_defContext column_def() throws RecognitionException {
		Column_defContext _localctx = new Column_defContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_column_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1000);
			column_name();
			setState(1002);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,136,_ctx) ) {
			case 1:
				{
				setState(1001);
				type_name();
				}
				break;
			}
			setState(1007);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << K_CHECK) | (1L << K_COLLATE) | (1L << K_CONSTRAINT) | (1L << K_DEFAULT))) != 0) || ((((_la - 102)) & ~0x3f) == 0 && ((1L << (_la - 102)) & ((1L << (K_NOT - 102)) | (1L << (K_NULL - 102)) | (1L << (K_PRIMARY - 102)) | (1L << (K_REFERENCES - 102)) | (1L << (K_UNIQUE - 102)))) != 0)) {
				{
				{
				setState(1004);
				column_constraint();
				}
				}
				setState(1009);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Type_nameContext extends ParserRuleContext {
		public List<NameContext> name() {
			return getRuleContexts(NameContext.class);
		}
		public NameContext name(int i) {
			return getRuleContext(NameContext.class,i);
		}
		public TerminalNode OPEN_PAR() { return getToken(SQLiteParser.OPEN_PAR, 0); }
		public List<Signed_numberContext> signed_number() {
			return getRuleContexts(Signed_numberContext.class);
		}
		public Signed_numberContext signed_number(int i) {
			return getRuleContext(Signed_numberContext.class,i);
		}
		public TerminalNode CLOSE_PAR() { return getToken(SQLiteParser.CLOSE_PAR, 0); }
		public TerminalNode COMMA() { return getToken(SQLiteParser.COMMA, 0); }
		public Type_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_type_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterType_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitType_name(this);
		}
	}

	public final Type_nameContext type_name() throws RecognitionException {
		Type_nameContext _localctx = new Type_nameContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_type_name);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1011); 
			_errHandler.sync(this);
			_alt = 1+1;
			do {
				switch (_alt) {
				case 1+1:
					{
					{
					setState(1010);
					name();
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1013); 
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,138,_ctx);
			} while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
			setState(1025);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,139,_ctx) ) {
			case 1:
				{
				setState(1015);
				match(OPEN_PAR);
				setState(1016);
				signed_number();
				setState(1017);
				match(CLOSE_PAR);
				}
				break;
			case 2:
				{
				setState(1019);
				match(OPEN_PAR);
				setState(1020);
				signed_number();
				setState(1021);
				match(COMMA);
				setState(1022);
				signed_number();
				setState(1023);
				match(CLOSE_PAR);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_constraintContext extends ParserRuleContext {
		public TerminalNode K_PRIMARY() { return getToken(SQLiteParser.K_PRIMARY, 0); }
		public TerminalNode K_KEY() { return getToken(SQLiteParser.K_KEY, 0); }
		public Conflict_clauseContext conflict_clause() {
			return getRuleContext(Conflict_clauseContext.class,0);
		}
		public TerminalNode K_NULL() { return getToken(SQLiteParser.K_NULL, 0); }
		public TerminalNode K_UNIQUE() { return getToken(SQLiteParser.K_UNIQUE, 0); }
		public TerminalNode K_CHECK() { return getToken(SQLiteParser.K_CHECK, 0); }
		public TerminalNode OPEN_PAR() { return getToken(SQLiteParser.OPEN_PAR, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode CLOSE_PAR() { return getToken(SQLiteParser.CLOSE_PAR, 0); }
		public TerminalNode K_DEFAULT() { return getToken(SQLiteParser.K_DEFAULT, 0); }
		public TerminalNode K_COLLATE() { return getToken(SQLiteParser.K_COLLATE, 0); }
		public Collation_nameContext collation_name() {
			return getRuleContext(Collation_nameContext.class,0);
		}
		public Foreign_key_clauseContext foreign_key_clause() {
			return getRuleContext(Foreign_key_clauseContext.class,0);
		}
		public TerminalNode K_CONSTRAINT() { return getToken(SQLiteParser.K_CONSTRAINT, 0); }
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public Signed_numberContext signed_number() {
			return getRuleContext(Signed_numberContext.class,0);
		}
		public Literal_valueContext literal_value() {
			return getRuleContext(Literal_valueContext.class,0);
		}
		public TerminalNode K_AUTOINCREMENT() { return getToken(SQLiteParser.K_AUTOINCREMENT, 0); }
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_ASC() { return getToken(SQLiteParser.K_ASC, 0); }
		public TerminalNode K_DESC() { return getToken(SQLiteParser.K_DESC, 0); }
		public Column_constraintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_constraint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterColumn_constraint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitColumn_constraint(this);
		}
	}

	public final Column_constraintContext column_constraint() throws RecognitionException {
		Column_constraintContext _localctx = new Column_constraintContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_column_constraint);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1029);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_CONSTRAINT) {
				{
				setState(1027);
				match(K_CONSTRAINT);
				setState(1028);
				name();
				}
			}

			setState(1064);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case K_PRIMARY:
				{
				setState(1031);
				match(K_PRIMARY);
				setState(1032);
				match(K_KEY);
				setState(1034);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_ASC || _la==K_DESC) {
					{
					setState(1033);
					_la = _input.LA(1);
					if ( !(_la==K_ASC || _la==K_DESC) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(1036);
				conflict_clause();
				setState(1038);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_AUTOINCREMENT) {
					{
					setState(1037);
					match(K_AUTOINCREMENT);
					}
				}

				}
				break;
			case K_NOT:
			case K_NULL:
				{
				setState(1041);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_NOT) {
					{
					setState(1040);
					match(K_NOT);
					}
				}

				setState(1043);
				match(K_NULL);
				setState(1044);
				conflict_clause();
				}
				break;
			case K_UNIQUE:
				{
				setState(1045);
				match(K_UNIQUE);
				setState(1046);
				conflict_clause();
				}
				break;
			case K_CHECK:
				{
				setState(1047);
				match(K_CHECK);
				setState(1048);
				match(OPEN_PAR);
				setState(1049);
				expr(0);
				setState(1050);
				match(CLOSE_PAR);
				}
				break;
			case K_DEFAULT:
				{
				setState(1052);
				match(K_DEFAULT);
				setState(1059);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,144,_ctx) ) {
				case 1:
					{
					setState(1053);
					signed_number();
					}
					break;
				case 2:
					{
					setState(1054);
					literal_value();
					}
					break;
				case 3:
					{
					setState(1055);
					match(OPEN_PAR);
					setState(1056);
					expr(0);
					setState(1057);
					match(CLOSE_PAR);
					}
					break;
				}
				}
				break;
			case K_COLLATE:
				{
				setState(1061);
				match(K_COLLATE);
				setState(1062);
				collation_name();
				}
				break;
			case K_REFERENCES:
				{
				setState(1063);
				foreign_key_clause();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Conflict_clauseContext extends ParserRuleContext {
		public TerminalNode K_ON() { return getToken(SQLiteParser.K_ON, 0); }
		public TerminalNode K_CONFLICT() { return getToken(SQLiteParser.K_CONFLICT, 0); }
		public TerminalNode K_ROLLBACK() { return getToken(SQLiteParser.K_ROLLBACK, 0); }
		public TerminalNode K_ABORT() { return getToken(SQLiteParser.K_ABORT, 0); }
		public TerminalNode K_FAIL() { return getToken(SQLiteParser.K_FAIL, 0); }
		public TerminalNode K_IGNORE() { return getToken(SQLiteParser.K_IGNORE, 0); }
		public TerminalNode K_REPLACE() { return getToken(SQLiteParser.K_REPLACE, 0); }
		public Conflict_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_conflict_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterConflict_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitConflict_clause(this);
		}
	}

	public final Conflict_clauseContext conflict_clause() throws RecognitionException {
		Conflict_clauseContext _localctx = new Conflict_clauseContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_conflict_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1069);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_ON) {
				{
				setState(1066);
				match(K_ON);
				setState(1067);
				match(K_CONFLICT);
				setState(1068);
				_la = _input.LA(1);
				if ( !(_la==K_ABORT || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (K_FAIL - 72)) | (1L << (K_IGNORE - 72)) | (1L << (K_REPLACE - 72)) | (1L << (K_ROLLBACK - 72)))) != 0)) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExprContext extends ParserRuleContext {
		public Literal_valueContext literal_value() {
			return getRuleContext(Literal_valueContext.class,0);
		}
		public TerminalNode BIND_PARAMETER() { return getToken(SQLiteParser.BIND_PARAMETER, 0); }
		public Column_nameContext column_name() {
			return getRuleContext(Column_nameContext.class,0);
		}
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public List<TerminalNode> DOT() { return getTokens(SQLiteParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(SQLiteParser.DOT, i);
		}
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public Unary_operatorContext unary_operator() {
			return getRuleContext(Unary_operatorContext.class,0);
		}
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public Function_nameContext function_name() {
			return getRuleContext(Function_nameContext.class,0);
		}
		public TerminalNode OPEN_PAR() { return getToken(SQLiteParser.OPEN_PAR, 0); }
		public TerminalNode CLOSE_PAR() { return getToken(SQLiteParser.CLOSE_PAR, 0); }
		public TerminalNode STAR() { return getToken(SQLiteParser.STAR, 0); }
		public TerminalNode K_DISTINCT() { return getToken(SQLiteParser.K_DISTINCT, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public TerminalNode K_CAST() { return getToken(SQLiteParser.K_CAST, 0); }
		public TerminalNode K_AS() { return getToken(SQLiteParser.K_AS, 0); }
		public Type_nameContext type_name() {
			return getRuleContext(Type_nameContext.class,0);
		}
		public Select_stmtContext select_stmt() {
			return getRuleContext(Select_stmtContext.class,0);
		}
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_CASE() { return getToken(SQLiteParser.K_CASE, 0); }
		public TerminalNode K_END() { return getToken(SQLiteParser.K_END, 0); }
		public List<TerminalNode> K_WHEN() { return getTokens(SQLiteParser.K_WHEN); }
		public TerminalNode K_WHEN(int i) {
			return getToken(SQLiteParser.K_WHEN, i);
		}
		public List<TerminalNode> K_THEN() { return getTokens(SQLiteParser.K_THEN); }
		public TerminalNode K_THEN(int i) {
			return getToken(SQLiteParser.K_THEN, i);
		}
		public TerminalNode K_ELSE() { return getToken(SQLiteParser.K_ELSE, 0); }
		public Raise_functionContext raise_function() {
			return getRuleContext(Raise_functionContext.class,0);
		}
		public TerminalNode PIPE2() { return getToken(SQLiteParser.PIPE2, 0); }
		public TerminalNode DIV() { return getToken(SQLiteParser.DIV, 0); }
		public TerminalNode MOD() { return getToken(SQLiteParser.MOD, 0); }
		public TerminalNode PLUS() { return getToken(SQLiteParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SQLiteParser.MINUS, 0); }
		public TerminalNode LT2() { return getToken(SQLiteParser.LT2, 0); }
		public TerminalNode GT2() { return getToken(SQLiteParser.GT2, 0); }
		public TerminalNode AMP() { return getToken(SQLiteParser.AMP, 0); }
		public TerminalNode PIPE() { return getToken(SQLiteParser.PIPE, 0); }
		public TerminalNode LT() { return getToken(SQLiteParser.LT, 0); }
		public TerminalNode LT_EQ() { return getToken(SQLiteParser.LT_EQ, 0); }
		public TerminalNode GT() { return getToken(SQLiteParser.GT, 0); }
		public TerminalNode GT_EQ() { return getToken(SQLiteParser.GT_EQ, 0); }
		public TerminalNode ASSIGN() { return getToken(SQLiteParser.ASSIGN, 0); }
		public TerminalNode EQ() { return getToken(SQLiteParser.EQ, 0); }
		public TerminalNode NOT_EQ1() { return getToken(SQLiteParser.NOT_EQ1, 0); }
		public TerminalNode NOT_EQ2() { return getToken(SQLiteParser.NOT_EQ2, 0); }
		public TerminalNode K_AND() { return getToken(SQLiteParser.K_AND, 0); }
		public TerminalNode K_OR() { return getToken(SQLiteParser.K_OR, 0); }
		public TerminalNode K_IS() { return getToken(SQLiteParser.K_IS, 0); }
		public TerminalNode K_BETWEEN() { return getToken(SQLiteParser.K_BETWEEN, 0); }
		public TerminalNode K_IN() { return getToken(SQLiteParser.K_IN, 0); }
		public TerminalNode K_COLLATE() { return getToken(SQLiteParser.K_COLLATE, 0); }
		public Collation_nameContext collation_name() {
			return getRuleContext(Collation_nameContext.class,0);
		}
		public TerminalNode K_LIKE() { return getToken(SQLiteParser.K_LIKE, 0); }
		public TerminalNode K_GLOB() { return getToken(SQLiteParser.K_GLOB, 0); }
		public TerminalNode K_REGEXP() { return getToken(SQLiteParser.K_REGEXP, 0); }
		public TerminalNode K_MATCH() { return getToken(SQLiteParser.K_MATCH, 0); }
		public TerminalNode K_ESCAPE() { return getToken(SQLiteParser.K_ESCAPE, 0); }
		public TerminalNode K_ISNULL() { return getToken(SQLiteParser.K_ISNULL, 0); }
		public TerminalNode K_NOTNULL() { return getToken(SQLiteParser.K_NOTNULL, 0); }
		public TerminalNode K_NULL() { return getToken(SQLiteParser.K_NULL, 0); }
		public ExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitExpr(this);
		}
	}

	public final ExprContext expr() throws RecognitionException {
		return expr(0);
	}

	private ExprContext expr(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ExprContext _localctx = new ExprContext(_ctx, _parentState);
		ExprContext _prevctx = _localctx;
		int _startState = 78;
		enterRecursionRule(_localctx, 78, RULE_expr, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1147);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,157,_ctx) ) {
			case 1:
				{
				setState(1072);
				literal_value();
				}
				break;
			case 2:
				{
				setState(1073);
				match(BIND_PARAMETER);
				}
				break;
			case 3:
				{
				setState(1082);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,148,_ctx) ) {
				case 1:
					{
					setState(1077);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,147,_ctx) ) {
					case 1:
						{
						setState(1074);
						database_name();
						setState(1075);
						match(DOT);
						}
						break;
					}
					setState(1079);
					table_name();
					setState(1080);
					match(DOT);
					}
					break;
				}
				setState(1084);
				column_name();
				}
				break;
			case 4:
				{
				setState(1085);
				unary_operator();
				setState(1086);
				expr(21);
				}
				break;
			case 5:
				{
				setState(1088);
				function_name();
				setState(1089);
				match(OPEN_PAR);
				setState(1102);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case OPEN_PAR:
				case PLUS:
				case MINUS:
				case TILDE:
				case K_ABORT:
				case K_ACTION:
				case K_ADD:
				case K_AFTER:
				case K_ALL:
				case K_ALTER:
				case K_ANALYZE:
				case K_AND:
				case K_AS:
				case K_ASC:
				case K_ATTACH:
				case K_AUTOINCREMENT:
				case K_BEFORE:
				case K_BEGIN:
				case K_BETWEEN:
				case K_BY:
				case K_CASCADE:
				case K_CASE:
				case K_CAST:
				case K_CHECK:
				case K_COLLATE:
				case K_COLUMN:
				case K_COMMIT:
				case K_CONFLICT:
				case K_CONSTRAINT:
				case K_CREATE:
				case K_CROSS:
				case K_CURRENT_DATE:
				case K_CURRENT_TIME:
				case K_CURRENT_TIMESTAMP:
				case K_DATABASE:
				case K_DEFAULT:
				case K_DEFERRABLE:
				case K_DEFERRED:
				case K_DELETE:
				case K_DESC:
				case K_DETACH:
				case K_DISTINCT:
				case K_DROP:
				case K_EACH:
				case K_ELSE:
				case K_END:
				case K_ESCAPE:
				case K_EXCEPT:
				case K_EXCLUSIVE:
				case K_EXISTS:
				case K_EXPLAIN:
				case K_FAIL:
				case K_FOR:
				case K_FOREIGN:
				case K_FROM:
				case K_FULL:
				case K_GLOB:
				case K_GROUP:
				case K_HAVING:
				case K_IF:
				case K_IGNORE:
				case K_IMMEDIATE:
				case K_IN:
				case K_INDEX:
				case K_INDEXED:
				case K_INITIALLY:
				case K_INNER:
				case K_INSERT:
				case K_INSTEAD:
				case K_INTERSECT:
				case K_INTO:
				case K_IS:
				case K_ISNULL:
				case K_JOIN:
				case K_KEY:
				case K_LEFT:
				case K_LIKE:
				case K_LIMIT:
				case K_MATCH:
				case K_NATURAL:
				case K_NO:
				case K_NOT:
				case K_NOTNULL:
				case K_NULL:
				case K_OF:
				case K_OFFSET:
				case K_ON:
				case K_OR:
				case K_ORDER:
				case K_OUTER:
				case K_PLAN:
				case K_PRAGMA:
				case K_PRIMARY:
				case K_QUERY:
				case K_RAISE:
				case K_RECURSIVE:
				case K_REFERENCES:
				case K_REGEXP:
				case K_REINDEX:
				case K_RELEASE:
				case K_RENAME:
				case K_REPLACE:
				case K_RESTRICT:
				case K_RIGHT:
				case K_ROLLBACK:
				case K_ROW:
				case K_SAVEPOINT:
				case K_SELECT:
				case K_SET:
				case K_TABLE:
				case K_TEMP:
				case K_TEMPORARY:
				case K_THEN:
				case K_TO:
				case K_TRANSACTION:
				case K_TRIGGER:
				case K_UNION:
				case K_UNIQUE:
				case K_UPDATE:
				case K_USING:
				case K_VACUUM:
				case K_VALUES:
				case K_VIEW:
				case K_VIRTUAL:
				case K_WHEN:
				case K_WHERE:
				case K_WITH:
				case K_WITHOUT:
				case IDENTIFIER:
				case NUMERIC_LITERAL:
				case BIND_PARAMETER:
				case STRING_LITERAL:
				case BLOB_LITERAL:
					{
					setState(1091);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,149,_ctx) ) {
					case 1:
						{
						setState(1090);
						match(K_DISTINCT);
						}
						break;
					}
					setState(1093);
					expr(0);
					setState(1098);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(1094);
						match(COMMA);
						setState(1095);
						expr(0);
						}
						}
						setState(1100);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				case STAR:
					{
					setState(1101);
					match(STAR);
					}
					break;
				case CLOSE_PAR:
					break;
				default:
					break;
				}
				setState(1104);
				match(CLOSE_PAR);
				}
				break;
			case 6:
				{
				setState(1106);
				match(OPEN_PAR);
				setState(1107);
				expr(0);
				setState(1108);
				match(CLOSE_PAR);
				}
				break;
			case 7:
				{
				setState(1110);
				match(K_CAST);
				setState(1111);
				match(OPEN_PAR);
				setState(1112);
				expr(0);
				setState(1113);
				match(K_AS);
				setState(1114);
				type_name();
				setState(1115);
				match(CLOSE_PAR);
				}
				break;
			case 8:
				{
				setState(1121);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_EXISTS || _la==K_NOT) {
					{
					setState(1118);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==K_NOT) {
						{
						setState(1117);
						match(K_NOT);
						}
					}

					setState(1120);
					match(K_EXISTS);
					}
				}

				setState(1123);
				match(OPEN_PAR);
				setState(1124);
				select_stmt();
				setState(1125);
				match(CLOSE_PAR);
				}
				break;
			case 9:
				{
				setState(1127);
				match(K_CASE);
				setState(1129);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,154,_ctx) ) {
				case 1:
					{
					setState(1128);
					expr(0);
					}
					break;
				}
				setState(1136); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1131);
					match(K_WHEN);
					setState(1132);
					expr(0);
					setState(1133);
					match(K_THEN);
					setState(1134);
					expr(0);
					}
					}
					setState(1138); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==K_WHEN );
				setState(1142);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_ELSE) {
					{
					setState(1140);
					match(K_ELSE);
					setState(1141);
					expr(0);
					}
				}

				setState(1144);
				match(K_END);
				}
				break;
			case 10:
				{
				setState(1146);
				raise_function();
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(1236);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,169,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1234);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,168,_ctx) ) {
					case 1:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1149);
						if (!(precpred(_ctx, 20))) throw new FailedPredicateException(this, "precpred(_ctx, 20)");
						setState(1150);
						match(PIPE2);
						setState(1151);
						expr(21);
						}
						break;
					case 2:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1152);
						if (!(precpred(_ctx, 19))) throw new FailedPredicateException(this, "precpred(_ctx, 19)");
						setState(1153);
						_la = _input.LA(1);
						if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << STAR) | (1L << DIV) | (1L << MOD))) != 0)) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1154);
						expr(20);
						}
						break;
					case 3:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1155);
						if (!(precpred(_ctx, 18))) throw new FailedPredicateException(this, "precpred(_ctx, 18)");
						setState(1156);
						_la = _input.LA(1);
						if ( !(_la==PLUS || _la==MINUS) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1157);
						expr(19);
						}
						break;
					case 4:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1158);
						if (!(precpred(_ctx, 17))) throw new FailedPredicateException(this, "precpred(_ctx, 17)");
						setState(1159);
						_la = _input.LA(1);
						if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT2) | (1L << GT2) | (1L << AMP) | (1L << PIPE))) != 0)) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1160);
						expr(18);
						}
						break;
					case 5:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1161);
						if (!(precpred(_ctx, 16))) throw new FailedPredicateException(this, "precpred(_ctx, 16)");
						setState(1162);
						_la = _input.LA(1);
						if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT) | (1L << LT_EQ) | (1L << GT) | (1L << GT_EQ))) != 0)) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1163);
						expr(17);
						}
						break;
					case 6:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1164);
						if (!(precpred(_ctx, 15))) throw new FailedPredicateException(this, "precpred(_ctx, 15)");
						setState(1165);
						_la = _input.LA(1);
						if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ASSIGN) | (1L << EQ) | (1L << NOT_EQ1) | (1L << NOT_EQ2))) != 0)) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1166);
						expr(16);
						}
						break;
					case 7:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1167);
						if (!(precpred(_ctx, 13))) throw new FailedPredicateException(this, "precpred(_ctx, 13)");
						setState(1168);
						match(K_AND);
						setState(1169);
						expr(14);
						}
						break;
					case 8:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1170);
						if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
						setState(1171);
						match(K_OR);
						setState(1172);
						expr(13);
						}
						break;
					case 9:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1173);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(1174);
						match(K_IS);
						setState(1176);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,158,_ctx) ) {
						case 1:
							{
							setState(1175);
							match(K_NOT);
							}
							break;
						}
						setState(1178);
						expr(6);
						}
						break;
					case 10:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1179);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(1181);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==K_NOT) {
							{
							setState(1180);
							match(K_NOT);
							}
						}

						setState(1183);
						match(K_BETWEEN);
						setState(1184);
						expr(0);
						setState(1185);
						match(K_AND);
						setState(1186);
						expr(5);
						}
						break;
					case 11:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1188);
						if (!(precpred(_ctx, 14))) throw new FailedPredicateException(this, "precpred(_ctx, 14)");
						setState(1190);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==K_NOT) {
							{
							setState(1189);
							match(K_NOT);
							}
						}

						setState(1192);
						match(K_IN);
						setState(1212);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,164,_ctx) ) {
						case 1:
							{
							setState(1193);
							match(OPEN_PAR);
							setState(1203);
							_errHandler.sync(this);
							switch ( getInterpreter().adaptivePredict(_input,162,_ctx) ) {
							case 1:
								{
								setState(1194);
								select_stmt();
								}
								break;
							case 2:
								{
								setState(1195);
								expr(0);
								setState(1200);
								_errHandler.sync(this);
								_la = _input.LA(1);
								while (_la==COMMA) {
									{
									{
									setState(1196);
									match(COMMA);
									setState(1197);
									expr(0);
									}
									}
									setState(1202);
									_errHandler.sync(this);
									_la = _input.LA(1);
								}
								}
								break;
							}
							setState(1205);
							match(CLOSE_PAR);
							}
							break;
						case 2:
							{
							setState(1209);
							_errHandler.sync(this);
							switch ( getInterpreter().adaptivePredict(_input,163,_ctx) ) {
							case 1:
								{
								setState(1206);
								database_name();
								setState(1207);
								match(DOT);
								}
								break;
							}
							setState(1211);
							table_name();
							}
							break;
						}
						}
						break;
					case 12:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1214);
						if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
						setState(1215);
						match(K_COLLATE);
						setState(1216);
						collation_name();
						}
						break;
					case 13:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1217);
						if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
						setState(1219);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==K_NOT) {
							{
							setState(1218);
							match(K_NOT);
							}
						}

						setState(1221);
						_la = _input.LA(1);
						if ( !(((((_la - 77)) & ~0x3f) == 0 && ((1L << (_la - 77)) & ((1L << (K_GLOB - 77)) | (1L << (K_LIKE - 77)) | (1L << (K_MATCH - 77)) | (1L << (K_REGEXP - 77)))) != 0)) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1222);
						expr(0);
						setState(1225);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,166,_ctx) ) {
						case 1:
							{
							setState(1223);
							match(K_ESCAPE);
							setState(1224);
							expr(0);
							}
							break;
						}
						}
						break;
					case 14:
						{
						_localctx = new ExprContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(1227);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(1232);
						_errHandler.sync(this);
						switch (_input.LA(1)) {
						case K_ISNULL:
							{
							setState(1228);
							match(K_ISNULL);
							}
							break;
						case K_NOTNULL:
							{
							setState(1229);
							match(K_NOTNULL);
							}
							break;
						case K_NOT:
							{
							setState(1230);
							match(K_NOT);
							setState(1231);
							match(K_NULL);
							}
							break;
						default:
							throw new NoViableAltException(this);
						}
						}
						break;
					}
					} 
				}
				setState(1238);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,169,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class Foreign_key_clauseContext extends ParserRuleContext {
		public TerminalNode K_REFERENCES() { return getToken(SQLiteParser.K_REFERENCES, 0); }
		public Foreign_tableContext foreign_table() {
			return getRuleContext(Foreign_tableContext.class,0);
		}
		public TerminalNode OPEN_PAR() { return getToken(SQLiteParser.OPEN_PAR, 0); }
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public TerminalNode CLOSE_PAR() { return getToken(SQLiteParser.CLOSE_PAR, 0); }
		public TerminalNode K_DEFERRABLE() { return getToken(SQLiteParser.K_DEFERRABLE, 0); }
		public List<TerminalNode> K_ON() { return getTokens(SQLiteParser.K_ON); }
		public TerminalNode K_ON(int i) {
			return getToken(SQLiteParser.K_ON, i);
		}
		public List<TerminalNode> K_MATCH() { return getTokens(SQLiteParser.K_MATCH); }
		public TerminalNode K_MATCH(int i) {
			return getToken(SQLiteParser.K_MATCH, i);
		}
		public List<NameContext> name() {
			return getRuleContexts(NameContext.class);
		}
		public NameContext name(int i) {
			return getRuleContext(NameContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public List<TerminalNode> K_DELETE() { return getTokens(SQLiteParser.K_DELETE); }
		public TerminalNode K_DELETE(int i) {
			return getToken(SQLiteParser.K_DELETE, i);
		}
		public List<TerminalNode> K_UPDATE() { return getTokens(SQLiteParser.K_UPDATE); }
		public TerminalNode K_UPDATE(int i) {
			return getToken(SQLiteParser.K_UPDATE, i);
		}
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_INITIALLY() { return getToken(SQLiteParser.K_INITIALLY, 0); }
		public TerminalNode K_DEFERRED() { return getToken(SQLiteParser.K_DEFERRED, 0); }
		public TerminalNode K_IMMEDIATE() { return getToken(SQLiteParser.K_IMMEDIATE, 0); }
		public List<TerminalNode> K_SET() { return getTokens(SQLiteParser.K_SET); }
		public TerminalNode K_SET(int i) {
			return getToken(SQLiteParser.K_SET, i);
		}
		public List<TerminalNode> K_NULL() { return getTokens(SQLiteParser.K_NULL); }
		public TerminalNode K_NULL(int i) {
			return getToken(SQLiteParser.K_NULL, i);
		}
		public List<TerminalNode> K_DEFAULT() { return getTokens(SQLiteParser.K_DEFAULT); }
		public TerminalNode K_DEFAULT(int i) {
			return getToken(SQLiteParser.K_DEFAULT, i);
		}
		public List<TerminalNode> K_CASCADE() { return getTokens(SQLiteParser.K_CASCADE); }
		public TerminalNode K_CASCADE(int i) {
			return getToken(SQLiteParser.K_CASCADE, i);
		}
		public List<TerminalNode> K_RESTRICT() { return getTokens(SQLiteParser.K_RESTRICT); }
		public TerminalNode K_RESTRICT(int i) {
			return getToken(SQLiteParser.K_RESTRICT, i);
		}
		public List<TerminalNode> K_NO() { return getTokens(SQLiteParser.K_NO); }
		public TerminalNode K_NO(int i) {
			return getToken(SQLiteParser.K_NO, i);
		}
		public List<TerminalNode> K_ACTION() { return getTokens(SQLiteParser.K_ACTION); }
		public TerminalNode K_ACTION(int i) {
			return getToken(SQLiteParser.K_ACTION, i);
		}
		public Foreign_key_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_foreign_key_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterForeign_key_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitForeign_key_clause(this);
		}
	}

	public final Foreign_key_clauseContext foreign_key_clause() throws RecognitionException {
		Foreign_key_clauseContext _localctx = new Foreign_key_clauseContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_foreign_key_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1239);
			match(K_REFERENCES);
			setState(1240);
			foreign_table();
			setState(1252);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OPEN_PAR) {
				{
				setState(1241);
				match(OPEN_PAR);
				setState(1242);
				column_name();
				setState(1247);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1243);
					match(COMMA);
					setState(1244);
					column_name();
					}
					}
					setState(1249);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1250);
				match(CLOSE_PAR);
				}
			}

			setState(1272);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==K_MATCH || _la==K_ON) {
				{
				{
				setState(1268);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case K_ON:
					{
					setState(1254);
					match(K_ON);
					setState(1255);
					_la = _input.LA(1);
					if ( !(_la==K_DELETE || _la==K_UPDATE) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(1264);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,172,_ctx) ) {
					case 1:
						{
						setState(1256);
						match(K_SET);
						setState(1257);
						match(K_NULL);
						}
						break;
					case 2:
						{
						setState(1258);
						match(K_SET);
						setState(1259);
						match(K_DEFAULT);
						}
						break;
					case 3:
						{
						setState(1260);
						match(K_CASCADE);
						}
						break;
					case 4:
						{
						setState(1261);
						match(K_RESTRICT);
						}
						break;
					case 5:
						{
						setState(1262);
						match(K_NO);
						setState(1263);
						match(K_ACTION);
						}
						break;
					}
					}
					break;
				case K_MATCH:
					{
					setState(1266);
					match(K_MATCH);
					setState(1267);
					name();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				}
				setState(1274);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1285);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,177,_ctx) ) {
			case 1:
				{
				setState(1276);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_NOT) {
					{
					setState(1275);
					match(K_NOT);
					}
				}

				setState(1278);
				match(K_DEFERRABLE);
				setState(1283);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,176,_ctx) ) {
				case 1:
					{
					setState(1279);
					match(K_INITIALLY);
					setState(1280);
					match(K_DEFERRED);
					}
					break;
				case 2:
					{
					setState(1281);
					match(K_INITIALLY);
					setState(1282);
					match(K_IMMEDIATE);
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Raise_functionContext extends ParserRuleContext {
		public TerminalNode K_RAISE() { return getToken(SQLiteParser.K_RAISE, 0); }
		public TerminalNode OPEN_PAR() { return getToken(SQLiteParser.OPEN_PAR, 0); }
		public TerminalNode CLOSE_PAR() { return getToken(SQLiteParser.CLOSE_PAR, 0); }
		public TerminalNode K_IGNORE() { return getToken(SQLiteParser.K_IGNORE, 0); }
		public TerminalNode COMMA() { return getToken(SQLiteParser.COMMA, 0); }
		public Error_messageContext error_message() {
			return getRuleContext(Error_messageContext.class,0);
		}
		public TerminalNode K_ROLLBACK() { return getToken(SQLiteParser.K_ROLLBACK, 0); }
		public TerminalNode K_ABORT() { return getToken(SQLiteParser.K_ABORT, 0); }
		public TerminalNode K_FAIL() { return getToken(SQLiteParser.K_FAIL, 0); }
		public Raise_functionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_raise_function; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterRaise_function(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitRaise_function(this);
		}
	}

	public final Raise_functionContext raise_function() throws RecognitionException {
		Raise_functionContext _localctx = new Raise_functionContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_raise_function);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1287);
			match(K_RAISE);
			setState(1288);
			match(OPEN_PAR);
			setState(1293);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case K_IGNORE:
				{
				setState(1289);
				match(K_IGNORE);
				}
				break;
			case K_ABORT:
			case K_FAIL:
			case K_ROLLBACK:
				{
				setState(1290);
				_la = _input.LA(1);
				if ( !(_la==K_ABORT || _la==K_FAIL || _la==K_ROLLBACK) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1291);
				match(COMMA);
				setState(1292);
				error_message();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(1295);
			match(CLOSE_PAR);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Indexed_columnContext extends ParserRuleContext {
		public Column_nameContext column_name() {
			return getRuleContext(Column_nameContext.class,0);
		}
		public TerminalNode K_COLLATE() { return getToken(SQLiteParser.K_COLLATE, 0); }
		public Collation_nameContext collation_name() {
			return getRuleContext(Collation_nameContext.class,0);
		}
		public TerminalNode K_ASC() { return getToken(SQLiteParser.K_ASC, 0); }
		public TerminalNode K_DESC() { return getToken(SQLiteParser.K_DESC, 0); }
		public Indexed_columnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_indexed_column; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterIndexed_column(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitIndexed_column(this);
		}
	}

	public final Indexed_columnContext indexed_column() throws RecognitionException {
		Indexed_columnContext _localctx = new Indexed_columnContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_indexed_column);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1297);
			column_name();
			setState(1300);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_COLLATE) {
				{
				setState(1298);
				match(K_COLLATE);
				setState(1299);
				collation_name();
				}
			}

			setState(1303);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_ASC || _la==K_DESC) {
				{
				setState(1302);
				_la = _input.LA(1);
				if ( !(_la==K_ASC || _la==K_DESC) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_constraintContext extends ParserRuleContext {
		public TerminalNode OPEN_PAR() { return getToken(SQLiteParser.OPEN_PAR, 0); }
		public List<Indexed_columnContext> indexed_column() {
			return getRuleContexts(Indexed_columnContext.class);
		}
		public Indexed_columnContext indexed_column(int i) {
			return getRuleContext(Indexed_columnContext.class,i);
		}
		public TerminalNode CLOSE_PAR() { return getToken(SQLiteParser.CLOSE_PAR, 0); }
		public Conflict_clauseContext conflict_clause() {
			return getRuleContext(Conflict_clauseContext.class,0);
		}
		public TerminalNode K_CHECK() { return getToken(SQLiteParser.K_CHECK, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode K_FOREIGN() { return getToken(SQLiteParser.K_FOREIGN, 0); }
		public TerminalNode K_KEY() { return getToken(SQLiteParser.K_KEY, 0); }
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public Foreign_key_clauseContext foreign_key_clause() {
			return getRuleContext(Foreign_key_clauseContext.class,0);
		}
		public TerminalNode K_CONSTRAINT() { return getToken(SQLiteParser.K_CONSTRAINT, 0); }
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public TerminalNode K_PRIMARY() { return getToken(SQLiteParser.K_PRIMARY, 0); }
		public TerminalNode K_UNIQUE() { return getToken(SQLiteParser.K_UNIQUE, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public Table_constraintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_constraint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterTable_constraint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitTable_constraint(this);
		}
	}

	public final Table_constraintContext table_constraint() throws RecognitionException {
		Table_constraintContext _localctx = new Table_constraintContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_table_constraint);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1307);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_CONSTRAINT) {
				{
				setState(1305);
				match(K_CONSTRAINT);
				setState(1306);
				name();
				}
			}

			setState(1345);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case K_PRIMARY:
			case K_UNIQUE:
				{
				setState(1312);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case K_PRIMARY:
					{
					setState(1309);
					match(K_PRIMARY);
					setState(1310);
					match(K_KEY);
					}
					break;
				case K_UNIQUE:
					{
					setState(1311);
					match(K_UNIQUE);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1314);
				match(OPEN_PAR);
				setState(1315);
				indexed_column();
				setState(1320);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1316);
					match(COMMA);
					setState(1317);
					indexed_column();
					}
					}
					setState(1322);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1323);
				match(CLOSE_PAR);
				setState(1324);
				conflict_clause();
				}
				break;
			case K_CHECK:
				{
				setState(1326);
				match(K_CHECK);
				setState(1327);
				match(OPEN_PAR);
				setState(1328);
				expr(0);
				setState(1329);
				match(CLOSE_PAR);
				}
				break;
			case K_FOREIGN:
				{
				setState(1331);
				match(K_FOREIGN);
				setState(1332);
				match(K_KEY);
				setState(1333);
				match(OPEN_PAR);
				setState(1334);
				column_name();
				setState(1339);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1335);
					match(COMMA);
					setState(1336);
					column_name();
					}
					}
					setState(1341);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1342);
				match(CLOSE_PAR);
				setState(1343);
				foreign_key_clause();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class With_clauseContext extends ParserRuleContext {
		public TerminalNode K_WITH() { return getToken(SQLiteParser.K_WITH, 0); }
		public List<Common_table_expressionContext> common_table_expression() {
			return getRuleContexts(Common_table_expressionContext.class);
		}
		public Common_table_expressionContext common_table_expression(int i) {
			return getRuleContext(Common_table_expressionContext.class,i);
		}
		public TerminalNode K_RECURSIVE() { return getToken(SQLiteParser.K_RECURSIVE, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public With_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterWith_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitWith_clause(this);
		}
	}

	public final With_clauseContext with_clause() throws RecognitionException {
		With_clauseContext _localctx = new With_clauseContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_with_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1347);
			match(K_WITH);
			setState(1349);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,186,_ctx) ) {
			case 1:
				{
				setState(1348);
				match(K_RECURSIVE);
				}
				break;
			}
			setState(1351);
			common_table_expression();
			setState(1356);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1352);
				match(COMMA);
				setState(1353);
				common_table_expression();
				}
				}
				setState(1358);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Qualified_table_nameContext extends ParserRuleContext {
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode DOT() { return getToken(SQLiteParser.DOT, 0); }
		public TerminalNode K_INDEXED() { return getToken(SQLiteParser.K_INDEXED, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public Qualified_table_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualified_table_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterQualified_table_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitQualified_table_name(this);
		}
	}

	public final Qualified_table_nameContext qualified_table_name() throws RecognitionException {
		Qualified_table_nameContext _localctx = new Qualified_table_nameContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_qualified_table_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1362);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,188,_ctx) ) {
			case 1:
				{
				setState(1359);
				database_name();
				setState(1360);
				match(DOT);
				}
				break;
			}
			setState(1364);
			table_name();
			setState(1370);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case K_INDEXED:
				{
				setState(1365);
				match(K_INDEXED);
				setState(1366);
				match(K_BY);
				setState(1367);
				index_name();
				}
				break;
			case K_NOT:
				{
				setState(1368);
				match(K_NOT);
				setState(1369);
				match(K_INDEXED);
				}
				break;
			case EOF:
			case SCOL:
			case K_ALTER:
			case K_ANALYZE:
			case K_ATTACH:
			case K_BEGIN:
			case K_COMMIT:
			case K_CREATE:
			case K_DELETE:
			case K_DETACH:
			case K_DROP:
			case K_END:
			case K_EXPLAIN:
			case K_INSERT:
			case K_LIMIT:
			case K_ORDER:
			case K_PRAGMA:
			case K_REINDEX:
			case K_RELEASE:
			case K_REPLACE:
			case K_ROLLBACK:
			case K_SAVEPOINT:
			case K_SELECT:
			case K_SET:
			case K_UPDATE:
			case K_VACUUM:
			case K_VALUES:
			case K_WHERE:
			case K_WITH:
			case UNEXPECTED_CHAR:
				break;
			default:
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Ordering_termContext extends ParserRuleContext {
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode K_COLLATE() { return getToken(SQLiteParser.K_COLLATE, 0); }
		public Collation_nameContext collation_name() {
			return getRuleContext(Collation_nameContext.class,0);
		}
		public TerminalNode K_ASC() { return getToken(SQLiteParser.K_ASC, 0); }
		public TerminalNode K_DESC() { return getToken(SQLiteParser.K_DESC, 0); }
		public Ordering_termContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ordering_term; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterOrdering_term(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitOrdering_term(this);
		}
	}

	public final Ordering_termContext ordering_term() throws RecognitionException {
		Ordering_termContext _localctx = new Ordering_termContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_ordering_term);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1372);
			expr(0);
			setState(1375);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_COLLATE) {
				{
				setState(1373);
				match(K_COLLATE);
				setState(1374);
				collation_name();
				}
			}

			setState(1378);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_ASC || _la==K_DESC) {
				{
				setState(1377);
				_la = _input.LA(1);
				if ( !(_la==K_ASC || _la==K_DESC) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Pragma_valueContext extends ParserRuleContext {
		public Signed_numberContext signed_number() {
			return getRuleContext(Signed_numberContext.class,0);
		}
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public TerminalNode STRING_LITERAL() { return getToken(SQLiteParser.STRING_LITERAL, 0); }
		public Pragma_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pragma_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterPragma_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitPragma_value(this);
		}
	}

	public final Pragma_valueContext pragma_value() throws RecognitionException {
		Pragma_valueContext _localctx = new Pragma_valueContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_pragma_value);
		try {
			setState(1383);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,192,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1380);
				signed_number();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1381);
				name();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1382);
				match(STRING_LITERAL);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Common_table_expressionContext extends ParserRuleContext {
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode K_AS() { return getToken(SQLiteParser.K_AS, 0); }
		public List<TerminalNode> OPEN_PAR() { return getTokens(SQLiteParser.OPEN_PAR); }
		public TerminalNode OPEN_PAR(int i) {
			return getToken(SQLiteParser.OPEN_PAR, i);
		}
		public Select_stmtContext select_stmt() {
			return getRuleContext(Select_stmtContext.class,0);
		}
		public List<TerminalNode> CLOSE_PAR() { return getTokens(SQLiteParser.CLOSE_PAR); }
		public TerminalNode CLOSE_PAR(int i) {
			return getToken(SQLiteParser.CLOSE_PAR, i);
		}
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public Common_table_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_common_table_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCommon_table_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCommon_table_expression(this);
		}
	}

	public final Common_table_expressionContext common_table_expression() throws RecognitionException {
		Common_table_expressionContext _localctx = new Common_table_expressionContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_common_table_expression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1385);
			table_name();
			setState(1397);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OPEN_PAR) {
				{
				setState(1386);
				match(OPEN_PAR);
				setState(1387);
				column_name();
				setState(1392);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1388);
					match(COMMA);
					setState(1389);
					column_name();
					}
					}
					setState(1394);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1395);
				match(CLOSE_PAR);
				}
			}

			setState(1399);
			match(K_AS);
			setState(1400);
			match(OPEN_PAR);
			setState(1401);
			select_stmt();
			setState(1402);
			match(CLOSE_PAR);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Result_columnContext extends ParserRuleContext {
		public TerminalNode STAR() { return getToken(SQLiteParser.STAR, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode DOT() { return getToken(SQLiteParser.DOT, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Column_aliasContext column_alias() {
			return getRuleContext(Column_aliasContext.class,0);
		}
		public TerminalNode K_AS() { return getToken(SQLiteParser.K_AS, 0); }
		public Result_columnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_result_column; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterResult_column(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitResult_column(this);
		}
	}

	public final Result_columnContext result_column() throws RecognitionException {
		Result_columnContext _localctx = new Result_columnContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_result_column);
		int _la;
		try {
			setState(1416);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,197,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1404);
				match(STAR);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1405);
				table_name();
				setState(1406);
				match(DOT);
				setState(1407);
				match(STAR);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1409);
				expr(0);
				setState(1414);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_AS || _la==IDENTIFIER || _la==STRING_LITERAL) {
					{
					setState(1411);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==K_AS) {
						{
						setState(1410);
						match(K_AS);
						}
					}

					setState(1413);
					column_alias();
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_or_subqueryContext extends ParserRuleContext {
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Schema_nameContext schema_name() {
			return getRuleContext(Schema_nameContext.class,0);
		}
		public TerminalNode DOT() { return getToken(SQLiteParser.DOT, 0); }
		public Table_aliasContext table_alias() {
			return getRuleContext(Table_aliasContext.class,0);
		}
		public TerminalNode K_INDEXED() { return getToken(SQLiteParser.K_INDEXED, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_AS() { return getToken(SQLiteParser.K_AS, 0); }
		public Table_function_nameContext table_function_name() {
			return getRuleContext(Table_function_nameContext.class,0);
		}
		public TerminalNode OPEN_PAR() { return getToken(SQLiteParser.OPEN_PAR, 0); }
		public TerminalNode CLOSE_PAR() { return getToken(SQLiteParser.CLOSE_PAR, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public List<Table_or_subqueryContext> table_or_subquery() {
			return getRuleContexts(Table_or_subqueryContext.class);
		}
		public Table_or_subqueryContext table_or_subquery(int i) {
			return getRuleContext(Table_or_subqueryContext.class,i);
		}
		public Join_clauseContext join_clause() {
			return getRuleContext(Join_clauseContext.class,0);
		}
		public Select_stmtContext select_stmt() {
			return getRuleContext(Select_stmtContext.class,0);
		}
		public Table_or_subqueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_or_subquery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterTable_or_subquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitTable_or_subquery(this);
		}
	}

	public final Table_or_subqueryContext table_or_subquery() throws RecognitionException {
		Table_or_subqueryContext _localctx = new Table_or_subqueryContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_table_or_subquery);
		int _la;
		try {
			setState(1484);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,211,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1421);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,198,_ctx) ) {
				case 1:
					{
					setState(1418);
					schema_name();
					setState(1419);
					match(DOT);
					}
					break;
				}
				setState(1423);
				table_name();
				setState(1428);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPEN_PAR || _la==K_AS || _la==IDENTIFIER || _la==STRING_LITERAL) {
					{
					setState(1425);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==K_AS) {
						{
						setState(1424);
						match(K_AS);
						}
					}

					setState(1427);
					table_alias();
					}
				}

				setState(1435);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case K_INDEXED:
					{
					setState(1430);
					match(K_INDEXED);
					setState(1431);
					match(K_BY);
					setState(1432);
					index_name();
					}
					break;
				case K_NOT:
					{
					setState(1433);
					match(K_NOT);
					setState(1434);
					match(K_INDEXED);
					}
					break;
				case EOF:
				case SCOL:
				case CLOSE_PAR:
				case COMMA:
				case K_ALTER:
				case K_ANALYZE:
				case K_ATTACH:
				case K_BEGIN:
				case K_COMMIT:
				case K_CREATE:
				case K_CROSS:
				case K_DELETE:
				case K_DETACH:
				case K_DROP:
				case K_END:
				case K_EXCEPT:
				case K_EXPLAIN:
				case K_GROUP:
				case K_INNER:
				case K_INSERT:
				case K_INTERSECT:
				case K_JOIN:
				case K_LEFT:
				case K_LIMIT:
				case K_NATURAL:
				case K_ON:
				case K_ORDER:
				case K_PRAGMA:
				case K_REINDEX:
				case K_RELEASE:
				case K_REPLACE:
				case K_ROLLBACK:
				case K_SAVEPOINT:
				case K_SELECT:
				case K_UNION:
				case K_UPDATE:
				case K_USING:
				case K_VACUUM:
				case K_VALUES:
				case K_WHERE:
				case K_WITH:
				case UNEXPECTED_CHAR:
					break;
				default:
					break;
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1440);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,202,_ctx) ) {
				case 1:
					{
					setState(1437);
					schema_name();
					setState(1438);
					match(DOT);
					}
					break;
				}
				setState(1442);
				table_function_name();
				setState(1443);
				match(OPEN_PAR);
				setState(1452);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << OPEN_PAR) | (1L << PLUS) | (1L << MINUS) | (1L << TILDE) | (1L << K_ABORT) | (1L << K_ACTION) | (1L << K_ADD) | (1L << K_AFTER) | (1L << K_ALL) | (1L << K_ALTER) | (1L << K_ANALYZE) | (1L << K_AND) | (1L << K_AS) | (1L << K_ASC) | (1L << K_ATTACH) | (1L << K_AUTOINCREMENT) | (1L << K_BEFORE) | (1L << K_BEGIN) | (1L << K_BETWEEN) | (1L << K_BY) | (1L << K_CASCADE) | (1L << K_CASE) | (1L << K_CAST) | (1L << K_CHECK) | (1L << K_COLLATE) | (1L << K_COLUMN) | (1L << K_COMMIT) | (1L << K_CONFLICT) | (1L << K_CONSTRAINT) | (1L << K_CREATE) | (1L << K_CROSS) | (1L << K_CURRENT_DATE) | (1L << K_CURRENT_TIME) | (1L << K_CURRENT_TIMESTAMP) | (1L << K_DATABASE) | (1L << K_DEFAULT) | (1L << K_DEFERRABLE) | (1L << K_DEFERRED) | (1L << K_DELETE) | (1L << K_DESC) | (1L << K_DETACH) | (1L << K_DISTINCT) | (1L << K_DROP))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (K_EACH - 64)) | (1L << (K_ELSE - 64)) | (1L << (K_END - 64)) | (1L << (K_ESCAPE - 64)) | (1L << (K_EXCEPT - 64)) | (1L << (K_EXCLUSIVE - 64)) | (1L << (K_EXISTS - 64)) | (1L << (K_EXPLAIN - 64)) | (1L << (K_FAIL - 64)) | (1L << (K_FOR - 64)) | (1L << (K_FOREIGN - 64)) | (1L << (K_FROM - 64)) | (1L << (K_FULL - 64)) | (1L << (K_GLOB - 64)) | (1L << (K_GROUP - 64)) | (1L << (K_HAVING - 64)) | (1L << (K_IF - 64)) | (1L << (K_IGNORE - 64)) | (1L << (K_IMMEDIATE - 64)) | (1L << (K_IN - 64)) | (1L << (K_INDEX - 64)) | (1L << (K_INDEXED - 64)) | (1L << (K_INITIALLY - 64)) | (1L << (K_INNER - 64)) | (1L << (K_INSERT - 64)) | (1L << (K_INSTEAD - 64)) | (1L << (K_INTERSECT - 64)) | (1L << (K_INTO - 64)) | (1L << (K_IS - 64)) | (1L << (K_ISNULL - 64)) | (1L << (K_JOIN - 64)) | (1L << (K_KEY - 64)) | (1L << (K_LEFT - 64)) | (1L << (K_LIKE - 64)) | (1L << (K_LIMIT - 64)) | (1L << (K_MATCH - 64)) | (1L << (K_NATURAL - 64)) | (1L << (K_NO - 64)) | (1L << (K_NOT - 64)) | (1L << (K_NOTNULL - 64)) | (1L << (K_NULL - 64)) | (1L << (K_OF - 64)) | (1L << (K_OFFSET - 64)) | (1L << (K_ON - 64)) | (1L << (K_OR - 64)) | (1L << (K_ORDER - 64)) | (1L << (K_OUTER - 64)) | (1L << (K_PLAN - 64)) | (1L << (K_PRAGMA - 64)) | (1L << (K_PRIMARY - 64)) | (1L << (K_QUERY - 64)) | (1L << (K_RAISE - 64)) | (1L << (K_RECURSIVE - 64)) | (1L << (K_REFERENCES - 64)) | (1L << (K_REGEXP - 64)) | (1L << (K_REINDEX - 64)) | (1L << (K_RELEASE - 64)) | (1L << (K_RENAME - 64)) | (1L << (K_REPLACE - 64)) | (1L << (K_RESTRICT - 64)) | (1L << (K_RIGHT - 64)) | (1L << (K_ROLLBACK - 64)) | (1L << (K_ROW - 64)) | (1L << (K_SAVEPOINT - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (K_SELECT - 128)) | (1L << (K_SET - 128)) | (1L << (K_TABLE - 128)) | (1L << (K_TEMP - 128)) | (1L << (K_TEMPORARY - 128)) | (1L << (K_THEN - 128)) | (1L << (K_TO - 128)) | (1L << (K_TRANSACTION - 128)) | (1L << (K_TRIGGER - 128)) | (1L << (K_UNION - 128)) | (1L << (K_UNIQUE - 128)) | (1L << (K_UPDATE - 128)) | (1L << (K_USING - 128)) | (1L << (K_VACUUM - 128)) | (1L << (K_VALUES - 128)) | (1L << (K_VIEW - 128)) | (1L << (K_VIRTUAL - 128)) | (1L << (K_WHEN - 128)) | (1L << (K_WHERE - 128)) | (1L << (K_WITH - 128)) | (1L << (K_WITHOUT - 128)) | (1L << (IDENTIFIER - 128)) | (1L << (NUMERIC_LITERAL - 128)) | (1L << (BIND_PARAMETER - 128)) | (1L << (STRING_LITERAL - 128)) | (1L << (BLOB_LITERAL - 128)))) != 0)) {
					{
					setState(1444);
					expr(0);
					setState(1449);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(1445);
						match(COMMA);
						setState(1446);
						expr(0);
						}
						}
						setState(1451);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1454);
				match(CLOSE_PAR);
				setState(1459);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPEN_PAR || _la==K_AS || _la==IDENTIFIER || _la==STRING_LITERAL) {
					{
					setState(1456);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==K_AS) {
						{
						setState(1455);
						match(K_AS);
						}
					}

					setState(1458);
					table_alias();
					}
				}

				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1461);
				match(OPEN_PAR);
				setState(1471);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,208,_ctx) ) {
				case 1:
					{
					setState(1462);
					table_or_subquery();
					setState(1467);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(1463);
						match(COMMA);
						setState(1464);
						table_or_subquery();
						}
						}
						setState(1469);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				case 2:
					{
					setState(1470);
					join_clause();
					}
					break;
				}
				setState(1473);
				match(CLOSE_PAR);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1475);
				match(OPEN_PAR);
				setState(1476);
				select_stmt();
				setState(1477);
				match(CLOSE_PAR);
				setState(1482);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPEN_PAR || _la==K_AS || _la==IDENTIFIER || _la==STRING_LITERAL) {
					{
					setState(1479);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==K_AS) {
						{
						setState(1478);
						match(K_AS);
						}
					}

					setState(1481);
					table_alias();
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Join_clauseContext extends ParserRuleContext {
		public List<Table_or_subqueryContext> table_or_subquery() {
			return getRuleContexts(Table_or_subqueryContext.class);
		}
		public Table_or_subqueryContext table_or_subquery(int i) {
			return getRuleContext(Table_or_subqueryContext.class,i);
		}
		public List<Join_operatorContext> join_operator() {
			return getRuleContexts(Join_operatorContext.class);
		}
		public Join_operatorContext join_operator(int i) {
			return getRuleContext(Join_operatorContext.class,i);
		}
		public List<Join_constraintContext> join_constraint() {
			return getRuleContexts(Join_constraintContext.class);
		}
		public Join_constraintContext join_constraint(int i) {
			return getRuleContext(Join_constraintContext.class,i);
		}
		public Join_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_join_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterJoin_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitJoin_clause(this);
		}
	}

	public final Join_clauseContext join_clause() throws RecognitionException {
		Join_clauseContext _localctx = new Join_clauseContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_join_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1486);
			table_or_subquery();
			setState(1493);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA || _la==K_CROSS || ((((_la - 87)) & ~0x3f) == 0 && ((1L << (_la - 87)) & ((1L << (K_INNER - 87)) | (1L << (K_JOIN - 87)) | (1L << (K_LEFT - 87)) | (1L << (K_NATURAL - 87)))) != 0)) {
				{
				{
				setState(1487);
				join_operator();
				setState(1488);
				table_or_subquery();
				setState(1489);
				join_constraint();
				}
				}
				setState(1495);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Join_operatorContext extends ParserRuleContext {
		public TerminalNode COMMA() { return getToken(SQLiteParser.COMMA, 0); }
		public TerminalNode K_JOIN() { return getToken(SQLiteParser.K_JOIN, 0); }
		public TerminalNode K_NATURAL() { return getToken(SQLiteParser.K_NATURAL, 0); }
		public TerminalNode K_LEFT() { return getToken(SQLiteParser.K_LEFT, 0); }
		public TerminalNode K_INNER() { return getToken(SQLiteParser.K_INNER, 0); }
		public TerminalNode K_CROSS() { return getToken(SQLiteParser.K_CROSS, 0); }
		public TerminalNode K_OUTER() { return getToken(SQLiteParser.K_OUTER, 0); }
		public Join_operatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_join_operator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterJoin_operator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitJoin_operator(this);
		}
	}

	public final Join_operatorContext join_operator() throws RecognitionException {
		Join_operatorContext _localctx = new Join_operatorContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_join_operator);
		int _la;
		try {
			setState(1509);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case COMMA:
				enterOuterAlt(_localctx, 1);
				{
				setState(1496);
				match(COMMA);
				}
				break;
			case K_CROSS:
			case K_INNER:
			case K_JOIN:
			case K_LEFT:
			case K_NATURAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(1498);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_NATURAL) {
					{
					setState(1497);
					match(K_NATURAL);
					}
				}

				setState(1506);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case K_LEFT:
					{
					setState(1500);
					match(K_LEFT);
					setState(1502);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==K_OUTER) {
						{
						setState(1501);
						match(K_OUTER);
						}
					}

					}
					break;
				case K_INNER:
					{
					setState(1504);
					match(K_INNER);
					}
					break;
				case K_CROSS:
					{
					setState(1505);
					match(K_CROSS);
					}
					break;
				case K_JOIN:
					break;
				default:
					break;
				}
				setState(1508);
				match(K_JOIN);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Join_constraintContext extends ParserRuleContext {
		public TerminalNode K_ON() { return getToken(SQLiteParser.K_ON, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode K_USING() { return getToken(SQLiteParser.K_USING, 0); }
		public TerminalNode OPEN_PAR() { return getToken(SQLiteParser.OPEN_PAR, 0); }
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public TerminalNode CLOSE_PAR() { return getToken(SQLiteParser.CLOSE_PAR, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public Join_constraintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_join_constraint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterJoin_constraint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitJoin_constraint(this);
		}
	}

	public final Join_constraintContext join_constraint() throws RecognitionException {
		Join_constraintContext _localctx = new Join_constraintContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_join_constraint);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1525);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case K_ON:
				{
				setState(1511);
				match(K_ON);
				setState(1512);
				expr(0);
				}
				break;
			case K_USING:
				{
				setState(1513);
				match(K_USING);
				setState(1514);
				match(OPEN_PAR);
				setState(1515);
				column_name();
				setState(1520);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1516);
					match(COMMA);
					setState(1517);
					column_name();
					}
					}
					setState(1522);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1523);
				match(CLOSE_PAR);
				}
				break;
			case EOF:
			case SCOL:
			case CLOSE_PAR:
			case COMMA:
			case K_ALTER:
			case K_ANALYZE:
			case K_ATTACH:
			case K_BEGIN:
			case K_COMMIT:
			case K_CREATE:
			case K_CROSS:
			case K_DELETE:
			case K_DETACH:
			case K_DROP:
			case K_END:
			case K_EXCEPT:
			case K_EXPLAIN:
			case K_GROUP:
			case K_INNER:
			case K_INSERT:
			case K_INTERSECT:
			case K_JOIN:
			case K_LEFT:
			case K_LIMIT:
			case K_NATURAL:
			case K_ORDER:
			case K_PRAGMA:
			case K_REINDEX:
			case K_RELEASE:
			case K_REPLACE:
			case K_ROLLBACK:
			case K_SAVEPOINT:
			case K_SELECT:
			case K_UNION:
			case K_UPDATE:
			case K_VACUUM:
			case K_VALUES:
			case K_WHERE:
			case K_WITH:
			case UNEXPECTED_CHAR:
				break;
			default:
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Select_coreContext extends ParserRuleContext {
		public TerminalNode K_SELECT() { return getToken(SQLiteParser.K_SELECT, 0); }
		public List<Result_columnContext> result_column() {
			return getRuleContexts(Result_columnContext.class);
		}
		public Result_columnContext result_column(int i) {
			return getRuleContext(Result_columnContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SQLiteParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLiteParser.COMMA, i);
		}
		public TerminalNode K_FROM() { return getToken(SQLiteParser.K_FROM, 0); }
		public TerminalNode K_WHERE() { return getToken(SQLiteParser.K_WHERE, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode K_GROUP() { return getToken(SQLiteParser.K_GROUP, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public TerminalNode K_DISTINCT() { return getToken(SQLiteParser.K_DISTINCT, 0); }
		public TerminalNode K_ALL() { return getToken(SQLiteParser.K_ALL, 0); }
		public List<Table_or_subqueryContext> table_or_subquery() {
			return getRuleContexts(Table_or_subqueryContext.class);
		}
		public Table_or_subqueryContext table_or_subquery(int i) {
			return getRuleContext(Table_or_subqueryContext.class,i);
		}
		public Join_clauseContext join_clause() {
			return getRuleContext(Join_clauseContext.class,0);
		}
		public TerminalNode K_HAVING() { return getToken(SQLiteParser.K_HAVING, 0); }
		public TerminalNode K_VALUES() { return getToken(SQLiteParser.K_VALUES, 0); }
		public List<TerminalNode> OPEN_PAR() { return getTokens(SQLiteParser.OPEN_PAR); }
		public TerminalNode OPEN_PAR(int i) {
			return getToken(SQLiteParser.OPEN_PAR, i);
		}
		public List<TerminalNode> CLOSE_PAR() { return getTokens(SQLiteParser.CLOSE_PAR); }
		public TerminalNode CLOSE_PAR(int i) {
			return getToken(SQLiteParser.CLOSE_PAR, i);
		}
		public Select_coreContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_core; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSelect_core(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSelect_core(this);
		}
	}

	public final Select_coreContext select_core() throws RecognitionException {
		Select_coreContext _localctx = new Select_coreContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_select_core);
		int _la;
		try {
			setState(1601);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case K_SELECT:
				enterOuterAlt(_localctx, 1);
				{
				setState(1527);
				match(K_SELECT);
				setState(1529);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,219,_ctx) ) {
				case 1:
					{
					setState(1528);
					_la = _input.LA(1);
					if ( !(_la==K_ALL || _la==K_DISTINCT) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				setState(1531);
				result_column();
				setState(1536);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1532);
					match(COMMA);
					setState(1533);
					result_column();
					}
					}
					setState(1538);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1551);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_FROM) {
					{
					setState(1539);
					match(K_FROM);
					setState(1549);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,222,_ctx) ) {
					case 1:
						{
						setState(1540);
						table_or_subquery();
						setState(1545);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
							{
							setState(1541);
							match(COMMA);
							setState(1542);
							table_or_subquery();
							}
							}
							setState(1547);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
						break;
					case 2:
						{
						setState(1548);
						join_clause();
						}
						break;
					}
					}
				}

				setState(1555);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WHERE) {
					{
					setState(1553);
					match(K_WHERE);
					setState(1554);
					expr(0);
					}
				}

				setState(1571);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_GROUP) {
					{
					setState(1557);
					match(K_GROUP);
					setState(1558);
					match(K_BY);
					setState(1559);
					expr(0);
					setState(1564);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(1560);
						match(COMMA);
						setState(1561);
						expr(0);
						}
						}
						setState(1566);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(1569);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==K_HAVING) {
						{
						setState(1567);
						match(K_HAVING);
						setState(1568);
						expr(0);
						}
					}

					}
				}

				}
				break;
			case K_VALUES:
				enterOuterAlt(_localctx, 2);
				{
				setState(1573);
				match(K_VALUES);
				setState(1574);
				match(OPEN_PAR);
				setState(1575);
				expr(0);
				setState(1580);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1576);
					match(COMMA);
					setState(1577);
					expr(0);
					}
					}
					setState(1582);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1583);
				match(CLOSE_PAR);
				setState(1598);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1584);
					match(COMMA);
					setState(1585);
					match(OPEN_PAR);
					setState(1586);
					expr(0);
					setState(1591);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(1587);
						match(COMMA);
						setState(1588);
						expr(0);
						}
						}
						setState(1593);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(1594);
					match(CLOSE_PAR);
					}
					}
					setState(1600);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Compound_operatorContext extends ParserRuleContext {
		public TerminalNode K_UNION() { return getToken(SQLiteParser.K_UNION, 0); }
		public TerminalNode K_ALL() { return getToken(SQLiteParser.K_ALL, 0); }
		public TerminalNode K_INTERSECT() { return getToken(SQLiteParser.K_INTERSECT, 0); }
		public TerminalNode K_EXCEPT() { return getToken(SQLiteParser.K_EXCEPT, 0); }
		public Compound_operatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_compound_operator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCompound_operator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCompound_operator(this);
		}
	}

	public final Compound_operatorContext compound_operator() throws RecognitionException {
		Compound_operatorContext _localctx = new Compound_operatorContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_compound_operator);
		try {
			setState(1608);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,232,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1603);
				match(K_UNION);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1604);
				match(K_UNION);
				setState(1605);
				match(K_ALL);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1606);
				match(K_INTERSECT);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1607);
				match(K_EXCEPT);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Signed_numberContext extends ParserRuleContext {
		public TerminalNode NUMERIC_LITERAL() { return getToken(SQLiteParser.NUMERIC_LITERAL, 0); }
		public TerminalNode PLUS() { return getToken(SQLiteParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SQLiteParser.MINUS, 0); }
		public Signed_numberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_signed_number; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSigned_number(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSigned_number(this);
		}
	}

	public final Signed_numberContext signed_number() throws RecognitionException {
		Signed_numberContext _localctx = new Signed_numberContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_signed_number);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1611);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PLUS || _la==MINUS) {
				{
				setState(1610);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(1613);
			match(NUMERIC_LITERAL);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Literal_valueContext extends ParserRuleContext {
		public TerminalNode NUMERIC_LITERAL() { return getToken(SQLiteParser.NUMERIC_LITERAL, 0); }
		public TerminalNode STRING_LITERAL() { return getToken(SQLiteParser.STRING_LITERAL, 0); }
		public TerminalNode BLOB_LITERAL() { return getToken(SQLiteParser.BLOB_LITERAL, 0); }
		public TerminalNode K_NULL() { return getToken(SQLiteParser.K_NULL, 0); }
		public TerminalNode K_CURRENT_TIME() { return getToken(SQLiteParser.K_CURRENT_TIME, 0); }
		public TerminalNode K_CURRENT_DATE() { return getToken(SQLiteParser.K_CURRENT_DATE, 0); }
		public TerminalNode K_CURRENT_TIMESTAMP() { return getToken(SQLiteParser.K_CURRENT_TIMESTAMP, 0); }
		public Literal_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_literal_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterLiteral_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitLiteral_value(this);
		}
	}

	public final Literal_valueContext literal_value() throws RecognitionException {
		Literal_valueContext _localctx = new Literal_valueContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_literal_value);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1615);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << K_CURRENT_DATE) | (1L << K_CURRENT_TIME) | (1L << K_CURRENT_TIMESTAMP))) != 0) || ((((_la - 104)) & ~0x3f) == 0 && ((1L << (_la - 104)) & ((1L << (K_NULL - 104)) | (1L << (NUMERIC_LITERAL - 104)) | (1L << (STRING_LITERAL - 104)) | (1L << (BLOB_LITERAL - 104)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Unary_operatorContext extends ParserRuleContext {
		public TerminalNode MINUS() { return getToken(SQLiteParser.MINUS, 0); }
		public TerminalNode PLUS() { return getToken(SQLiteParser.PLUS, 0); }
		public TerminalNode TILDE() { return getToken(SQLiteParser.TILDE, 0); }
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public Unary_operatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unary_operator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterUnary_operator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitUnary_operator(this);
		}
	}

	public final Unary_operatorContext unary_operator() throws RecognitionException {
		Unary_operatorContext _localctx = new Unary_operatorContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_unary_operator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1617);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << PLUS) | (1L << MINUS) | (1L << TILDE))) != 0) || _la==K_NOT) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Error_messageContext extends ParserRuleContext {
		public TerminalNode STRING_LITERAL() { return getToken(SQLiteParser.STRING_LITERAL, 0); }
		public Error_messageContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_error_message; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterError_message(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitError_message(this);
		}
	}

	public final Error_messageContext error_message() throws RecognitionException {
		Error_messageContext _localctx = new Error_messageContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_error_message);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1619);
			match(STRING_LITERAL);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Module_argumentContext extends ParserRuleContext {
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Column_defContext column_def() {
			return getRuleContext(Column_defContext.class,0);
		}
		public Module_argumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_module_argument; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterModule_argument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitModule_argument(this);
		}
	}

	public final Module_argumentContext module_argument() throws RecognitionException {
		Module_argumentContext _localctx = new Module_argumentContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_module_argument);
		try {
			setState(1623);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,234,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1621);
				expr(0);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1622);
				column_def();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_aliasContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(SQLiteParser.IDENTIFIER, 0); }
		public TerminalNode STRING_LITERAL() { return getToken(SQLiteParser.STRING_LITERAL, 0); }
		public Column_aliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_alias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterColumn_alias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitColumn_alias(this);
		}
	}

	public final Column_aliasContext column_alias() throws RecognitionException {
		Column_aliasContext _localctx = new Column_aliasContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_column_alias);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1625);
			_la = _input.LA(1);
			if ( !(_la==IDENTIFIER || _la==STRING_LITERAL) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class KeywordContext extends ParserRuleContext {
		public TerminalNode K_ABORT() { return getToken(SQLiteParser.K_ABORT, 0); }
		public TerminalNode K_ACTION() { return getToken(SQLiteParser.K_ACTION, 0); }
		public TerminalNode K_ADD() { return getToken(SQLiteParser.K_ADD, 0); }
		public TerminalNode K_AFTER() { return getToken(SQLiteParser.K_AFTER, 0); }
		public TerminalNode K_ALL() { return getToken(SQLiteParser.K_ALL, 0); }
		public TerminalNode K_ALTER() { return getToken(SQLiteParser.K_ALTER, 0); }
		public TerminalNode K_ANALYZE() { return getToken(SQLiteParser.K_ANALYZE, 0); }
		public TerminalNode K_AND() { return getToken(SQLiteParser.K_AND, 0); }
		public TerminalNode K_AS() { return getToken(SQLiteParser.K_AS, 0); }
		public TerminalNode K_ASC() { return getToken(SQLiteParser.K_ASC, 0); }
		public TerminalNode K_ATTACH() { return getToken(SQLiteParser.K_ATTACH, 0); }
		public TerminalNode K_AUTOINCREMENT() { return getToken(SQLiteParser.K_AUTOINCREMENT, 0); }
		public TerminalNode K_BEFORE() { return getToken(SQLiteParser.K_BEFORE, 0); }
		public TerminalNode K_BEGIN() { return getToken(SQLiteParser.K_BEGIN, 0); }
		public TerminalNode K_BETWEEN() { return getToken(SQLiteParser.K_BETWEEN, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public TerminalNode K_CASCADE() { return getToken(SQLiteParser.K_CASCADE, 0); }
		public TerminalNode K_CASE() { return getToken(SQLiteParser.K_CASE, 0); }
		public TerminalNode K_CAST() { return getToken(SQLiteParser.K_CAST, 0); }
		public TerminalNode K_CHECK() { return getToken(SQLiteParser.K_CHECK, 0); }
		public TerminalNode K_COLLATE() { return getToken(SQLiteParser.K_COLLATE, 0); }
		public TerminalNode K_COLUMN() { return getToken(SQLiteParser.K_COLUMN, 0); }
		public TerminalNode K_COMMIT() { return getToken(SQLiteParser.K_COMMIT, 0); }
		public TerminalNode K_CONFLICT() { return getToken(SQLiteParser.K_CONFLICT, 0); }
		public TerminalNode K_CONSTRAINT() { return getToken(SQLiteParser.K_CONSTRAINT, 0); }
		public TerminalNode K_CREATE() { return getToken(SQLiteParser.K_CREATE, 0); }
		public TerminalNode K_CROSS() { return getToken(SQLiteParser.K_CROSS, 0); }
		public TerminalNode K_CURRENT_DATE() { return getToken(SQLiteParser.K_CURRENT_DATE, 0); }
		public TerminalNode K_CURRENT_TIME() { return getToken(SQLiteParser.K_CURRENT_TIME, 0); }
		public TerminalNode K_CURRENT_TIMESTAMP() { return getToken(SQLiteParser.K_CURRENT_TIMESTAMP, 0); }
		public TerminalNode K_DATABASE() { return getToken(SQLiteParser.K_DATABASE, 0); }
		public TerminalNode K_DEFAULT() { return getToken(SQLiteParser.K_DEFAULT, 0); }
		public TerminalNode K_DEFERRABLE() { return getToken(SQLiteParser.K_DEFERRABLE, 0); }
		public TerminalNode K_DEFERRED() { return getToken(SQLiteParser.K_DEFERRED, 0); }
		public TerminalNode K_DELETE() { return getToken(SQLiteParser.K_DELETE, 0); }
		public TerminalNode K_DESC() { return getToken(SQLiteParser.K_DESC, 0); }
		public TerminalNode K_DETACH() { return getToken(SQLiteParser.K_DETACH, 0); }
		public TerminalNode K_DISTINCT() { return getToken(SQLiteParser.K_DISTINCT, 0); }
		public TerminalNode K_DROP() { return getToken(SQLiteParser.K_DROP, 0); }
		public TerminalNode K_EACH() { return getToken(SQLiteParser.K_EACH, 0); }
		public TerminalNode K_ELSE() { return getToken(SQLiteParser.K_ELSE, 0); }
		public TerminalNode K_END() { return getToken(SQLiteParser.K_END, 0); }
		public TerminalNode K_ESCAPE() { return getToken(SQLiteParser.K_ESCAPE, 0); }
		public TerminalNode K_EXCEPT() { return getToken(SQLiteParser.K_EXCEPT, 0); }
		public TerminalNode K_EXCLUSIVE() { return getToken(SQLiteParser.K_EXCLUSIVE, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public TerminalNode K_EXPLAIN() { return getToken(SQLiteParser.K_EXPLAIN, 0); }
		public TerminalNode K_FAIL() { return getToken(SQLiteParser.K_FAIL, 0); }
		public TerminalNode K_FOR() { return getToken(SQLiteParser.K_FOR, 0); }
		public TerminalNode K_FOREIGN() { return getToken(SQLiteParser.K_FOREIGN, 0); }
		public TerminalNode K_FROM() { return getToken(SQLiteParser.K_FROM, 0); }
		public TerminalNode K_FULL() { return getToken(SQLiteParser.K_FULL, 0); }
		public TerminalNode K_GLOB() { return getToken(SQLiteParser.K_GLOB, 0); }
		public TerminalNode K_GROUP() { return getToken(SQLiteParser.K_GROUP, 0); }
		public TerminalNode K_HAVING() { return getToken(SQLiteParser.K_HAVING, 0); }
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_IGNORE() { return getToken(SQLiteParser.K_IGNORE, 0); }
		public TerminalNode K_IMMEDIATE() { return getToken(SQLiteParser.K_IMMEDIATE, 0); }
		public TerminalNode K_IN() { return getToken(SQLiteParser.K_IN, 0); }
		public TerminalNode K_INDEX() { return getToken(SQLiteParser.K_INDEX, 0); }
		public TerminalNode K_INDEXED() { return getToken(SQLiteParser.K_INDEXED, 0); }
		public TerminalNode K_INITIALLY() { return getToken(SQLiteParser.K_INITIALLY, 0); }
		public TerminalNode K_INNER() { return getToken(SQLiteParser.K_INNER, 0); }
		public TerminalNode K_INSERT() { return getToken(SQLiteParser.K_INSERT, 0); }
		public TerminalNode K_INSTEAD() { return getToken(SQLiteParser.K_INSTEAD, 0); }
		public TerminalNode K_INTERSECT() { return getToken(SQLiteParser.K_INTERSECT, 0); }
		public TerminalNode K_INTO() { return getToken(SQLiteParser.K_INTO, 0); }
		public TerminalNode K_IS() { return getToken(SQLiteParser.K_IS, 0); }
		public TerminalNode K_ISNULL() { return getToken(SQLiteParser.K_ISNULL, 0); }
		public TerminalNode K_JOIN() { return getToken(SQLiteParser.K_JOIN, 0); }
		public TerminalNode K_KEY() { return getToken(SQLiteParser.K_KEY, 0); }
		public TerminalNode K_LEFT() { return getToken(SQLiteParser.K_LEFT, 0); }
		public TerminalNode K_LIKE() { return getToken(SQLiteParser.K_LIKE, 0); }
		public TerminalNode K_LIMIT() { return getToken(SQLiteParser.K_LIMIT, 0); }
		public TerminalNode K_MATCH() { return getToken(SQLiteParser.K_MATCH, 0); }
		public TerminalNode K_NATURAL() { return getToken(SQLiteParser.K_NATURAL, 0); }
		public TerminalNode K_NO() { return getToken(SQLiteParser.K_NO, 0); }
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_NOTNULL() { return getToken(SQLiteParser.K_NOTNULL, 0); }
		public TerminalNode K_NULL() { return getToken(SQLiteParser.K_NULL, 0); }
		public TerminalNode K_OF() { return getToken(SQLiteParser.K_OF, 0); }
		public TerminalNode K_OFFSET() { return getToken(SQLiteParser.K_OFFSET, 0); }
		public TerminalNode K_ON() { return getToken(SQLiteParser.K_ON, 0); }
		public TerminalNode K_OR() { return getToken(SQLiteParser.K_OR, 0); }
		public TerminalNode K_ORDER() { return getToken(SQLiteParser.K_ORDER, 0); }
		public TerminalNode K_OUTER() { return getToken(SQLiteParser.K_OUTER, 0); }
		public TerminalNode K_PLAN() { return getToken(SQLiteParser.K_PLAN, 0); }
		public TerminalNode K_PRAGMA() { return getToken(SQLiteParser.K_PRAGMA, 0); }
		public TerminalNode K_PRIMARY() { return getToken(SQLiteParser.K_PRIMARY, 0); }
		public TerminalNode K_QUERY() { return getToken(SQLiteParser.K_QUERY, 0); }
		public TerminalNode K_RAISE() { return getToken(SQLiteParser.K_RAISE, 0); }
		public TerminalNode K_RECURSIVE() { return getToken(SQLiteParser.K_RECURSIVE, 0); }
		public TerminalNode K_REFERENCES() { return getToken(SQLiteParser.K_REFERENCES, 0); }
		public TerminalNode K_REGEXP() { return getToken(SQLiteParser.K_REGEXP, 0); }
		public TerminalNode K_REINDEX() { return getToken(SQLiteParser.K_REINDEX, 0); }
		public TerminalNode K_RELEASE() { return getToken(SQLiteParser.K_RELEASE, 0); }
		public TerminalNode K_RENAME() { return getToken(SQLiteParser.K_RENAME, 0); }
		public TerminalNode K_REPLACE() { return getToken(SQLiteParser.K_REPLACE, 0); }
		public TerminalNode K_RESTRICT() { return getToken(SQLiteParser.K_RESTRICT, 0); }
		public TerminalNode K_RIGHT() { return getToken(SQLiteParser.K_RIGHT, 0); }
		public TerminalNode K_ROLLBACK() { return getToken(SQLiteParser.K_ROLLBACK, 0); }
		public TerminalNode K_ROW() { return getToken(SQLiteParser.K_ROW, 0); }
		public TerminalNode K_SAVEPOINT() { return getToken(SQLiteParser.K_SAVEPOINT, 0); }
		public TerminalNode K_SELECT() { return getToken(SQLiteParser.K_SELECT, 0); }
		public TerminalNode K_SET() { return getToken(SQLiteParser.K_SET, 0); }
		public TerminalNode K_TABLE() { return getToken(SQLiteParser.K_TABLE, 0); }
		public TerminalNode K_TEMP() { return getToken(SQLiteParser.K_TEMP, 0); }
		public TerminalNode K_TEMPORARY() { return getToken(SQLiteParser.K_TEMPORARY, 0); }
		public TerminalNode K_THEN() { return getToken(SQLiteParser.K_THEN, 0); }
		public TerminalNode K_TO() { return getToken(SQLiteParser.K_TO, 0); }
		public TerminalNode K_TRANSACTION() { return getToken(SQLiteParser.K_TRANSACTION, 0); }
		public TerminalNode K_TRIGGER() { return getToken(SQLiteParser.K_TRIGGER, 0); }
		public TerminalNode K_UNION() { return getToken(SQLiteParser.K_UNION, 0); }
		public TerminalNode K_UNIQUE() { return getToken(SQLiteParser.K_UNIQUE, 0); }
		public TerminalNode K_UPDATE() { return getToken(SQLiteParser.K_UPDATE, 0); }
		public TerminalNode K_USING() { return getToken(SQLiteParser.K_USING, 0); }
		public TerminalNode K_VACUUM() { return getToken(SQLiteParser.K_VACUUM, 0); }
		public TerminalNode K_VALUES() { return getToken(SQLiteParser.K_VALUES, 0); }
		public TerminalNode K_VIEW() { return getToken(SQLiteParser.K_VIEW, 0); }
		public TerminalNode K_VIRTUAL() { return getToken(SQLiteParser.K_VIRTUAL, 0); }
		public TerminalNode K_WHEN() { return getToken(SQLiteParser.K_WHEN, 0); }
		public TerminalNode K_WHERE() { return getToken(SQLiteParser.K_WHERE, 0); }
		public TerminalNode K_WITH() { return getToken(SQLiteParser.K_WITH, 0); }
		public TerminalNode K_WITHOUT() { return getToken(SQLiteParser.K_WITHOUT, 0); }
		public KeywordContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_keyword; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterKeyword(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitKeyword(this);
		}
	}

	public final KeywordContext keyword() throws RecognitionException {
		KeywordContext _localctx = new KeywordContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_keyword);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1627);
			_la = _input.LA(1);
			if ( !(((((_la - 25)) & ~0x3f) == 0 && ((1L << (_la - 25)) & ((1L << (K_ABORT - 25)) | (1L << (K_ACTION - 25)) | (1L << (K_ADD - 25)) | (1L << (K_AFTER - 25)) | (1L << (K_ALL - 25)) | (1L << (K_ALTER - 25)) | (1L << (K_ANALYZE - 25)) | (1L << (K_AND - 25)) | (1L << (K_AS - 25)) | (1L << (K_ASC - 25)) | (1L << (K_ATTACH - 25)) | (1L << (K_AUTOINCREMENT - 25)) | (1L << (K_BEFORE - 25)) | (1L << (K_BEGIN - 25)) | (1L << (K_BETWEEN - 25)) | (1L << (K_BY - 25)) | (1L << (K_CASCADE - 25)) | (1L << (K_CASE - 25)) | (1L << (K_CAST - 25)) | (1L << (K_CHECK - 25)) | (1L << (K_COLLATE - 25)) | (1L << (K_COLUMN - 25)) | (1L << (K_COMMIT - 25)) | (1L << (K_CONFLICT - 25)) | (1L << (K_CONSTRAINT - 25)) | (1L << (K_CREATE - 25)) | (1L << (K_CROSS - 25)) | (1L << (K_CURRENT_DATE - 25)) | (1L << (K_CURRENT_TIME - 25)) | (1L << (K_CURRENT_TIMESTAMP - 25)) | (1L << (K_DATABASE - 25)) | (1L << (K_DEFAULT - 25)) | (1L << (K_DEFERRABLE - 25)) | (1L << (K_DEFERRED - 25)) | (1L << (K_DELETE - 25)) | (1L << (K_DESC - 25)) | (1L << (K_DETACH - 25)) | (1L << (K_DISTINCT - 25)) | (1L << (K_DROP - 25)) | (1L << (K_EACH - 25)) | (1L << (K_ELSE - 25)) | (1L << (K_END - 25)) | (1L << (K_ESCAPE - 25)) | (1L << (K_EXCEPT - 25)) | (1L << (K_EXCLUSIVE - 25)) | (1L << (K_EXISTS - 25)) | (1L << (K_EXPLAIN - 25)) | (1L << (K_FAIL - 25)) | (1L << (K_FOR - 25)) | (1L << (K_FOREIGN - 25)) | (1L << (K_FROM - 25)) | (1L << (K_FULL - 25)) | (1L << (K_GLOB - 25)) | (1L << (K_GROUP - 25)) | (1L << (K_HAVING - 25)) | (1L << (K_IF - 25)) | (1L << (K_IGNORE - 25)) | (1L << (K_IMMEDIATE - 25)) | (1L << (K_IN - 25)) | (1L << (K_INDEX - 25)) | (1L << (K_INDEXED - 25)) | (1L << (K_INITIALLY - 25)) | (1L << (K_INNER - 25)) | (1L << (K_INSERT - 25)))) != 0) || ((((_la - 89)) & ~0x3f) == 0 && ((1L << (_la - 89)) & ((1L << (K_INSTEAD - 89)) | (1L << (K_INTERSECT - 89)) | (1L << (K_INTO - 89)) | (1L << (K_IS - 89)) | (1L << (K_ISNULL - 89)) | (1L << (K_JOIN - 89)) | (1L << (K_KEY - 89)) | (1L << (K_LEFT - 89)) | (1L << (K_LIKE - 89)) | (1L << (K_LIMIT - 89)) | (1L << (K_MATCH - 89)) | (1L << (K_NATURAL - 89)) | (1L << (K_NO - 89)) | (1L << (K_NOT - 89)) | (1L << (K_NOTNULL - 89)) | (1L << (K_NULL - 89)) | (1L << (K_OF - 89)) | (1L << (K_OFFSET - 89)) | (1L << (K_ON - 89)) | (1L << (K_OR - 89)) | (1L << (K_ORDER - 89)) | (1L << (K_OUTER - 89)) | (1L << (K_PLAN - 89)) | (1L << (K_PRAGMA - 89)) | (1L << (K_PRIMARY - 89)) | (1L << (K_QUERY - 89)) | (1L << (K_RAISE - 89)) | (1L << (K_RECURSIVE - 89)) | (1L << (K_REFERENCES - 89)) | (1L << (K_REGEXP - 89)) | (1L << (K_REINDEX - 89)) | (1L << (K_RELEASE - 89)) | (1L << (K_RENAME - 89)) | (1L << (K_REPLACE - 89)) | (1L << (K_RESTRICT - 89)) | (1L << (K_RIGHT - 89)) | (1L << (K_ROLLBACK - 89)) | (1L << (K_ROW - 89)) | (1L << (K_SAVEPOINT - 89)) | (1L << (K_SELECT - 89)) | (1L << (K_SET - 89)) | (1L << (K_TABLE - 89)) | (1L << (K_TEMP - 89)) | (1L << (K_TEMPORARY - 89)) | (1L << (K_THEN - 89)) | (1L << (K_TO - 89)) | (1L << (K_TRANSACTION - 89)) | (1L << (K_TRIGGER - 89)) | (1L << (K_UNION - 89)) | (1L << (K_UNIQUE - 89)) | (1L << (K_UPDATE - 89)) | (1L << (K_USING - 89)) | (1L << (K_VACUUM - 89)) | (1L << (K_VALUES - 89)) | (1L << (K_VIEW - 89)) | (1L << (K_VIRTUAL - 89)) | (1L << (K_WHEN - 89)) | (1L << (K_WHERE - 89)) | (1L << (K_WITH - 89)) | (1L << (K_WITHOUT - 89)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public NameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitName(this);
		}
	}

	public final NameContext name() throws RecognitionException {
		NameContext _localctx = new NameContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1629);
			any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Function_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Function_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_function_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterFunction_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitFunction_name(this);
		}
	}

	public final Function_nameContext function_name() throws RecognitionException {
		Function_nameContext _localctx = new Function_nameContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_function_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1631);
			any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Database_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Database_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_database_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterDatabase_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitDatabase_name(this);
		}
	}

	public final Database_nameContext database_name() throws RecognitionException {
		Database_nameContext _localctx = new Database_nameContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_database_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1633);
			any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Schema_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Schema_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_schema_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSchema_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSchema_name(this);
		}
	}

	public final Schema_nameContext schema_name() throws RecognitionException {
		Schema_nameContext _localctx = new Schema_nameContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_schema_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1635);
			any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_function_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Table_function_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_function_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterTable_function_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitTable_function_name(this);
		}
	}

	public final Table_function_nameContext table_function_name() throws RecognitionException {
		Table_function_nameContext _localctx = new Table_function_nameContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_table_function_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1637);
			any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Table_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterTable_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitTable_name(this);
		}
	}

	public final Table_nameContext table_name() throws RecognitionException {
		Table_nameContext _localctx = new Table_nameContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_table_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1639);
			any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_or_index_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Table_or_index_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_or_index_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterTable_or_index_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitTable_or_index_name(this);
		}
	}

	public final Table_or_index_nameContext table_or_index_name() throws RecognitionException {
		Table_or_index_nameContext _localctx = new Table_or_index_nameContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_table_or_index_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1641);
			any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class New_table_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public New_table_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_new_table_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterNew_table_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitNew_table_name(this);
		}
	}

	public final New_table_nameContext new_table_name() throws RecognitionException {
		New_table_nameContext _localctx = new New_table_nameContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_new_table_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1643);
			any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Column_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterColumn_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitColumn_name(this);
		}
	}

	public final Column_nameContext column_name() throws RecognitionException {
		Column_nameContext _localctx = new Column_nameContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_column_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1645);
			any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Collation_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Collation_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_collation_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCollation_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCollation_name(this);
		}
	}

	public final Collation_nameContext collation_name() throws RecognitionException {
		Collation_nameContext _localctx = new Collation_nameContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_collation_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1647);
			any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Foreign_tableContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Foreign_tableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_foreign_table; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterForeign_table(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitForeign_table(this);
		}
	}

	public final Foreign_tableContext foreign_table() throws RecognitionException {
		Foreign_tableContext _localctx = new Foreign_tableContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_foreign_table);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1649);
			any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Index_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Index_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_index_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterIndex_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitIndex_name(this);
		}
	}

	public final Index_nameContext index_name() throws RecognitionException {
		Index_nameContext _localctx = new Index_nameContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_index_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1651);
			any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Trigger_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Trigger_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_trigger_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterTrigger_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitTrigger_name(this);
		}
	}

	public final Trigger_nameContext trigger_name() throws RecognitionException {
		Trigger_nameContext _localctx = new Trigger_nameContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_trigger_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1653);
			any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class View_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public View_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_view_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterView_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitView_name(this);
		}
	}

	public final View_nameContext view_name() throws RecognitionException {
		View_nameContext _localctx = new View_nameContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_view_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1655);
			any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Module_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Module_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_module_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterModule_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitModule_name(this);
		}
	}

	public final Module_nameContext module_name() throws RecognitionException {
		Module_nameContext _localctx = new Module_nameContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_module_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1657);
			any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Pragma_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Pragma_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pragma_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterPragma_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitPragma_name(this);
		}
	}

	public final Pragma_nameContext pragma_name() throws RecognitionException {
		Pragma_nameContext _localctx = new Pragma_nameContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_pragma_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1659);
			any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Savepoint_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Savepoint_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_savepoint_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSavepoint_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSavepoint_name(this);
		}
	}

	public final Savepoint_nameContext savepoint_name() throws RecognitionException {
		Savepoint_nameContext _localctx = new Savepoint_nameContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_savepoint_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1661);
			any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_aliasContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(SQLiteParser.IDENTIFIER, 0); }
		public TerminalNode STRING_LITERAL() { return getToken(SQLiteParser.STRING_LITERAL, 0); }
		public TerminalNode OPEN_PAR() { return getToken(SQLiteParser.OPEN_PAR, 0); }
		public Table_aliasContext table_alias() {
			return getRuleContext(Table_aliasContext.class,0);
		}
		public TerminalNode CLOSE_PAR() { return getToken(SQLiteParser.CLOSE_PAR, 0); }
		public Table_aliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_alias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterTable_alias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitTable_alias(this);
		}
	}

	public final Table_aliasContext table_alias() throws RecognitionException {
		Table_aliasContext _localctx = new Table_aliasContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_table_alias);
		try {
			setState(1669);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(1663);
				match(IDENTIFIER);
				}
				break;
			case STRING_LITERAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(1664);
				match(STRING_LITERAL);
				}
				break;
			case OPEN_PAR:
				enterOuterAlt(_localctx, 3);
				{
				setState(1665);
				match(OPEN_PAR);
				setState(1666);
				table_alias();
				setState(1667);
				match(CLOSE_PAR);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Transaction_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Transaction_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transaction_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterTransaction_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitTransaction_name(this);
		}
	}

	public final Transaction_nameContext transaction_name() throws RecognitionException {
		Transaction_nameContext _localctx = new Transaction_nameContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_transaction_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1671);
			any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Any_nameContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(SQLiteParser.IDENTIFIER, 0); }
		public KeywordContext keyword() {
			return getRuleContext(KeywordContext.class,0);
		}
		public TerminalNode STRING_LITERAL() { return getToken(SQLiteParser.STRING_LITERAL, 0); }
		public TerminalNode OPEN_PAR() { return getToken(SQLiteParser.OPEN_PAR, 0); }
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public TerminalNode CLOSE_PAR() { return getToken(SQLiteParser.CLOSE_PAR, 0); }
		public Any_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_any_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterAny_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitAny_name(this);
		}
	}

	public final Any_nameContext any_name() throws RecognitionException {
		Any_nameContext _localctx = new Any_nameContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_any_name);
		try {
			setState(1680);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(1673);
				match(IDENTIFIER);
				}
				break;
			case K_ABORT:
			case K_ACTION:
			case K_ADD:
			case K_AFTER:
			case K_ALL:
			case K_ALTER:
			case K_ANALYZE:
			case K_AND:
			case K_AS:
			case K_ASC:
			case K_ATTACH:
			case K_AUTOINCREMENT:
			case K_BEFORE:
			case K_BEGIN:
			case K_BETWEEN:
			case K_BY:
			case K_CASCADE:
			case K_CASE:
			case K_CAST:
			case K_CHECK:
			case K_COLLATE:
			case K_COLUMN:
			case K_COMMIT:
			case K_CONFLICT:
			case K_CONSTRAINT:
			case K_CREATE:
			case K_CROSS:
			case K_CURRENT_DATE:
			case K_CURRENT_TIME:
			case K_CURRENT_TIMESTAMP:
			case K_DATABASE:
			case K_DEFAULT:
			case K_DEFERRABLE:
			case K_DEFERRED:
			case K_DELETE:
			case K_DESC:
			case K_DETACH:
			case K_DISTINCT:
			case K_DROP:
			case K_EACH:
			case K_ELSE:
			case K_END:
			case K_ESCAPE:
			case K_EXCEPT:
			case K_EXCLUSIVE:
			case K_EXISTS:
			case K_EXPLAIN:
			case K_FAIL:
			case K_FOR:
			case K_FOREIGN:
			case K_FROM:
			case K_FULL:
			case K_GLOB:
			case K_GROUP:
			case K_HAVING:
			case K_IF:
			case K_IGNORE:
			case K_IMMEDIATE:
			case K_IN:
			case K_INDEX:
			case K_INDEXED:
			case K_INITIALLY:
			case K_INNER:
			case K_INSERT:
			case K_INSTEAD:
			case K_INTERSECT:
			case K_INTO:
			case K_IS:
			case K_ISNULL:
			case K_JOIN:
			case K_KEY:
			case K_LEFT:
			case K_LIKE:
			case K_LIMIT:
			case K_MATCH:
			case K_NATURAL:
			case K_NO:
			case K_NOT:
			case K_NOTNULL:
			case K_NULL:
			case K_OF:
			case K_OFFSET:
			case K_ON:
			case K_OR:
			case K_ORDER:
			case K_OUTER:
			case K_PLAN:
			case K_PRAGMA:
			case K_PRIMARY:
			case K_QUERY:
			case K_RAISE:
			case K_RECURSIVE:
			case K_REFERENCES:
			case K_REGEXP:
			case K_REINDEX:
			case K_RELEASE:
			case K_RENAME:
			case K_REPLACE:
			case K_RESTRICT:
			case K_RIGHT:
			case K_ROLLBACK:
			case K_ROW:
			case K_SAVEPOINT:
			case K_SELECT:
			case K_SET:
			case K_TABLE:
			case K_TEMP:
			case K_TEMPORARY:
			case K_THEN:
			case K_TO:
			case K_TRANSACTION:
			case K_TRIGGER:
			case K_UNION:
			case K_UNIQUE:
			case K_UPDATE:
			case K_USING:
			case K_VACUUM:
			case K_VALUES:
			case K_VIEW:
			case K_VIRTUAL:
			case K_WHEN:
			case K_WHERE:
			case K_WITH:
			case K_WITHOUT:
				enterOuterAlt(_localctx, 2);
				{
				setState(1674);
				keyword();
				}
				break;
			case STRING_LITERAL:
				enterOuterAlt(_localctx, 3);
				{
				setState(1675);
				match(STRING_LITERAL);
				}
				break;
			case OPEN_PAR:
				enterOuterAlt(_localctx, 4);
				{
				setState(1676);
				match(OPEN_PAR);
				setState(1677);
				any_name();
				setState(1678);
				match(CLOSE_PAR);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 39:
			return expr_sempred((ExprContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean expr_sempred(ExprContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 20);
		case 1:
			return precpred(_ctx, 19);
		case 2:
			return precpred(_ctx, 18);
		case 3:
			return precpred(_ctx, 17);
		case 4:
			return precpred(_ctx, 16);
		case 5:
			return precpred(_ctx, 15);
		case 6:
			return precpred(_ctx, 13);
		case 7:
			return precpred(_ctx, 12);
		case 8:
			return precpred(_ctx, 5);
		case 9:
			return precpred(_ctx, 4);
		case 10:
			return precpred(_ctx, 14);
		case 11:
			return precpred(_ctx, 8);
		case 12:
			return precpred(_ctx, 7);
		case 13:
			return precpred(_ctx, 6);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\u009f\u0695\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
		"\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\tT"+
		"\3\2\3\2\7\2\u00ab\n\2\f\2\16\2\u00ae\13\2\3\2\3\2\3\3\3\3\3\3\3\4\7\4"+
		"\u00b6\n\4\f\4\16\4\u00b9\13\4\3\4\3\4\6\4\u00bd\n\4\r\4\16\4\u00be\3"+
		"\4\7\4\u00c2\n\4\f\4\16\4\u00c5\13\4\3\4\7\4\u00c8\n\4\f\4\16\4\u00cb"+
		"\13\4\3\5\3\5\3\5\5\5\u00d0\n\5\5\5\u00d2\n\5\3\5\3\5\3\5\3\5\3\5\3\5"+
		"\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3"+
		"\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5\u00f2\n\5\3\6\3\6\3\6\3\6\3\6\5\6\u00f9"+
		"\n\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0101\n\6\3\6\5\6\u0104\n\6\3\7\3\7\3"+
		"\7\3\7\3\7\3\7\3\7\5\7\u010d\n\7\3\b\3\b\5\b\u0111\n\b\3\b\3\b\3\b\3\b"+
		"\3\t\3\t\5\t\u0119\n\t\3\t\3\t\5\t\u011d\n\t\5\t\u011f\n\t\3\n\3\n\3\n"+
		"\5\n\u0124\n\n\5\n\u0126\n\n\3\13\5\13\u0129\n\13\3\13\3\13\3\13\5\13"+
		"\u012e\n\13\3\13\3\13\5\13\u0132\n\13\3\13\6\13\u0135\n\13\r\13\16\13"+
		"\u0136\3\13\3\13\3\13\3\13\3\13\7\13\u013e\n\13\f\13\16\13\u0141\13\13"+
		"\5\13\u0143\n\13\3\13\3\13\3\13\3\13\5\13\u0149\n\13\5\13\u014b\n\13\3"+
		"\f\3\f\5\f\u014f\n\f\3\f\3\f\3\f\3\f\5\f\u0155\n\f\3\f\3\f\3\f\5\f\u015a"+
		"\n\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\7\f\u0163\n\f\f\f\16\f\u0166\13\f\3\f"+
		"\3\f\3\f\5\f\u016b\n\f\3\r\3\r\5\r\u016f\n\r\3\r\3\r\3\r\3\r\5\r\u0175"+
		"\n\r\3\r\3\r\3\r\5\r\u017a\n\r\3\r\3\r\3\r\3\r\3\r\7\r\u0181\n\r\f\r\16"+
		"\r\u0184\13\r\3\r\3\r\7\r\u0188\n\r\f\r\16\r\u018b\13\r\3\r\3\r\3\r\5"+
		"\r\u0190\n\r\3\r\3\r\5\r\u0194\n\r\3\16\3\16\5\16\u0198\n\16\3\16\3\16"+
		"\3\16\3\16\5\16\u019e\n\16\3\16\3\16\3\16\5\16\u01a3\n\16\3\16\3\16\3"+
		"\16\3\16\3\16\5\16\u01aa\n\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\7\16"+
		"\u01b3\n\16\f\16\16\16\u01b6\13\16\5\16\u01b8\n\16\5\16\u01ba\n\16\3\16"+
		"\3\16\3\16\3\16\5\16\u01c0\n\16\3\16\3\16\3\16\3\16\5\16\u01c6\n\16\3"+
		"\16\3\16\5\16\u01ca\n\16\3\16\3\16\3\16\3\16\3\16\5\16\u01d1\n\16\3\16"+
		"\3\16\6\16\u01d5\n\16\r\16\16\16\u01d6\3\16\3\16\3\17\3\17\5\17\u01dd"+
		"\n\17\3\17\3\17\3\17\3\17\5\17\u01e3\n\17\3\17\3\17\3\17\5\17\u01e8\n"+
		"\17\3\17\3\17\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\20\5\20\u01f4\n\20"+
		"\3\20\3\20\3\20\5\20\u01f9\n\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\7\20"+
		"\u0202\n\20\f\20\16\20\u0205\13\20\3\20\3\20\5\20\u0209\n\20\3\21\5\21"+
		"\u020c\n\21\3\21\3\21\3\21\3\21\3\21\5\21\u0213\n\21\3\22\5\22\u0216\n"+
		"\22\3\22\3\22\3\22\3\22\3\22\5\22\u021d\n\22\3\22\3\22\3\22\3\22\3\22"+
		"\7\22\u0224\n\22\f\22\16\22\u0227\13\22\5\22\u0229\n\22\3\22\3\22\3\22"+
		"\3\22\5\22\u022f\n\22\5\22\u0231\n\22\3\23\3\23\5\23\u0235\n\23\3\23\3"+
		"\23\3\24\3\24\3\24\3\24\5\24\u023d\n\24\3\24\3\24\3\24\5\24\u0242\n\24"+
		"\3\24\3\24\3\25\3\25\3\25\3\25\5\25\u024a\n\25\3\25\3\25\3\25\5\25\u024f"+
		"\n\25\3\25\3\25\3\26\3\26\3\26\3\26\5\26\u0257\n\26\3\26\3\26\3\26\5\26"+
		"\u025c\n\26\3\26\3\26\3\27\3\27\3\27\3\27\5\27\u0264\n\27\3\27\3\27\3"+
		"\27\5\27\u0269\n\27\3\27\3\27\3\30\5\30\u026e\n\30\3\30\3\30\3\30\3\30"+
		"\7\30\u0274\n\30\f\30\16\30\u0277\13\30\3\30\3\30\3\30\3\30\3\30\7\30"+
		"\u027e\n\30\f\30\16\30\u0281\13\30\5\30\u0283\n\30\3\30\3\30\3\30\3\30"+
		"\5\30\u0289\n\30\5\30\u028b\n\30\3\31\5\31\u028e\n\31\3\31\3\31\3\31\3"+
		"\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\5"+
		"\31\u02a1\n\31\3\31\3\31\3\31\3\31\5\31\u02a7\n\31\3\31\3\31\3\31\3\31"+
		"\3\31\7\31\u02ae\n\31\f\31\16\31\u02b1\13\31\3\31\3\31\5\31\u02b5\n\31"+
		"\3\31\3\31\3\31\3\31\3\31\7\31\u02bc\n\31\f\31\16\31\u02bf\13\31\3\31"+
		"\3\31\3\31\3\31\3\31\3\31\7\31\u02c7\n\31\f\31\16\31\u02ca\13\31\3\31"+
		"\3\31\7\31\u02ce\n\31\f\31\16\31\u02d1\13\31\3\31\3\31\3\31\5\31\u02d6"+
		"\n\31\3\32\3\32\3\32\3\32\5\32\u02dc\n\32\3\32\3\32\3\32\3\32\3\32\3\32"+
		"\3\32\5\32\u02e5\n\32\3\33\3\33\3\33\3\33\3\33\5\33\u02ec\n\33\3\33\3"+
		"\33\5\33\u02f0\n\33\5\33\u02f2\n\33\3\34\3\34\5\34\u02f6\n\34\3\34\3\34"+
		"\3\35\3\35\3\35\5\35\u02fd\n\35\5\35\u02ff\n\35\3\35\3\35\5\35\u0303\n"+
		"\35\3\35\5\35\u0306\n\35\3\36\3\36\3\36\3\37\5\37\u030c\n\37\3\37\3\37"+
		"\3\37\3\37\3\37\3\37\7\37\u0314\n\37\f\37\16\37\u0317\13\37\5\37\u0319"+
		"\n\37\3\37\3\37\3\37\3\37\5\37\u031f\n\37\5\37\u0321\n\37\3 \5 \u0324"+
		"\n \3 \3 \3 \3 \7 \u032a\n \f \16 \u032d\13 \3 \3 \3 \3 \3 \7 \u0334\n"+
		" \f \16 \u0337\13 \5 \u0339\n \3 \3 \3 \3 \5 \u033f\n \5 \u0341\n \3!"+
		"\3!\5!\u0345\n!\3!\3!\3!\7!\u034a\n!\f!\16!\u034d\13!\3!\3!\3!\3!\7!\u0353"+
		"\n!\f!\16!\u0356\13!\3!\5!\u0359\n!\5!\u035b\n!\3!\3!\5!\u035f\n!\3!\3"+
		"!\3!\3!\3!\7!\u0366\n!\f!\16!\u0369\13!\3!\3!\5!\u036d\n!\5!\u036f\n!"+
		"\3!\3!\3!\3!\3!\7!\u0376\n!\f!\16!\u0379\13!\3!\3!\3!\3!\3!\3!\7!\u0381"+
		"\n!\f!\16!\u0384\13!\3!\3!\7!\u0388\n!\f!\16!\u038b\13!\5!\u038d\n!\3"+
		"\"\5\"\u0390\n\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\5\"\u039d"+
		"\n\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\7\"\u03a9\n\"\f\"\16\"\u03ac"+
		"\13\"\3\"\3\"\5\"\u03b0\n\"\3#\5#\u03b3\n#\3#\3#\3#\3#\3#\3#\3#\3#\3#"+
		"\3#\3#\5#\u03c0\n#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\7#\u03cc\n#\f#\16#\u03cf"+
		"\13#\3#\3#\5#\u03d3\n#\3#\3#\3#\3#\3#\7#\u03da\n#\f#\16#\u03dd\13#\5#"+
		"\u03df\n#\3#\3#\3#\3#\5#\u03e5\n#\5#\u03e7\n#\3$\3$\3%\3%\5%\u03ed\n%"+
		"\3%\7%\u03f0\n%\f%\16%\u03f3\13%\3&\6&\u03f6\n&\r&\16&\u03f7\3&\3&\3&"+
		"\3&\3&\3&\3&\3&\3&\3&\5&\u0404\n&\3\'\3\'\5\'\u0408\n\'\3\'\3\'\3\'\5"+
		"\'\u040d\n\'\3\'\3\'\5\'\u0411\n\'\3\'\5\'\u0414\n\'\3\'\3\'\3\'\3\'\3"+
		"\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\5\'\u0426\n\'\3\'\3\'\3"+
		"\'\5\'\u042b\n\'\3(\3(\3(\5(\u0430\n(\3)\3)\3)\3)\3)\3)\5)\u0438\n)\3"+
		")\3)\3)\5)\u043d\n)\3)\3)\3)\3)\3)\3)\3)\5)\u0446\n)\3)\3)\3)\7)\u044b"+
		"\n)\f)\16)\u044e\13)\3)\5)\u0451\n)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3"+
		")\3)\3)\5)\u0461\n)\3)\5)\u0464\n)\3)\3)\3)\3)\3)\3)\5)\u046c\n)\3)\3"+
		")\3)\3)\3)\6)\u0473\n)\r)\16)\u0474\3)\3)\5)\u0479\n)\3)\3)\3)\5)\u047e"+
		"\n)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)"+
		"\3)\3)\3)\3)\3)\5)\u049b\n)\3)\3)\3)\5)\u04a0\n)\3)\3)\3)\3)\3)\3)\3)"+
		"\5)\u04a9\n)\3)\3)\3)\3)\3)\3)\7)\u04b1\n)\f)\16)\u04b4\13)\5)\u04b6\n"+
		")\3)\3)\3)\3)\5)\u04bc\n)\3)\5)\u04bf\n)\3)\3)\3)\3)\3)\5)\u04c6\n)\3"+
		")\3)\3)\3)\5)\u04cc\n)\3)\3)\3)\3)\3)\5)\u04d3\n)\7)\u04d5\n)\f)\16)\u04d8"+
		"\13)\3*\3*\3*\3*\3*\3*\7*\u04e0\n*\f*\16*\u04e3\13*\3*\3*\5*\u04e7\n*"+
		"\3*\3*\3*\3*\3*\3*\3*\3*\3*\3*\5*\u04f3\n*\3*\3*\5*\u04f7\n*\7*\u04f9"+
		"\n*\f*\16*\u04fc\13*\3*\5*\u04ff\n*\3*\3*\3*\3*\3*\5*\u0506\n*\5*\u0508"+
		"\n*\3+\3+\3+\3+\3+\3+\5+\u0510\n+\3+\3+\3,\3,\3,\5,\u0517\n,\3,\5,\u051a"+
		"\n,\3-\3-\5-\u051e\n-\3-\3-\3-\5-\u0523\n-\3-\3-\3-\3-\7-\u0529\n-\f-"+
		"\16-\u052c\13-\3-\3-\3-\3-\3-\3-\3-\3-\3-\3-\3-\3-\3-\3-\7-\u053c\n-\f"+
		"-\16-\u053f\13-\3-\3-\3-\5-\u0544\n-\3.\3.\5.\u0548\n.\3.\3.\3.\7.\u054d"+
		"\n.\f.\16.\u0550\13.\3/\3/\3/\5/\u0555\n/\3/\3/\3/\3/\3/\3/\5/\u055d\n"+
		"/\3\60\3\60\3\60\5\60\u0562\n\60\3\60\5\60\u0565\n\60\3\61\3\61\3\61\5"+
		"\61\u056a\n\61\3\62\3\62\3\62\3\62\3\62\7\62\u0571\n\62\f\62\16\62\u0574"+
		"\13\62\3\62\3\62\5\62\u0578\n\62\3\62\3\62\3\62\3\62\3\62\3\63\3\63\3"+
		"\63\3\63\3\63\3\63\3\63\5\63\u0586\n\63\3\63\5\63\u0589\n\63\5\63\u058b"+
		"\n\63\3\64\3\64\3\64\5\64\u0590\n\64\3\64\3\64\5\64\u0594\n\64\3\64\5"+
		"\64\u0597\n\64\3\64\3\64\3\64\3\64\3\64\5\64\u059e\n\64\3\64\3\64\3\64"+
		"\5\64\u05a3\n\64\3\64\3\64\3\64\3\64\3\64\7\64\u05aa\n\64\f\64\16\64\u05ad"+
		"\13\64\5\64\u05af\n\64\3\64\3\64\5\64\u05b3\n\64\3\64\5\64\u05b6\n\64"+
		"\3\64\3\64\3\64\3\64\7\64\u05bc\n\64\f\64\16\64\u05bf\13\64\3\64\5\64"+
		"\u05c2\n\64\3\64\3\64\3\64\3\64\3\64\3\64\5\64\u05ca\n\64\3\64\5\64\u05cd"+
		"\n\64\5\64\u05cf\n\64\3\65\3\65\3\65\3\65\3\65\7\65\u05d6\n\65\f\65\16"+
		"\65\u05d9\13\65\3\66\3\66\5\66\u05dd\n\66\3\66\3\66\5\66\u05e1\n\66\3"+
		"\66\3\66\5\66\u05e5\n\66\3\66\5\66\u05e8\n\66\3\67\3\67\3\67\3\67\3\67"+
		"\3\67\3\67\7\67\u05f1\n\67\f\67\16\67\u05f4\13\67\3\67\3\67\5\67\u05f8"+
		"\n\67\38\38\58\u05fc\n8\38\38\38\78\u0601\n8\f8\168\u0604\138\38\38\3"+
		"8\38\78\u060a\n8\f8\168\u060d\138\38\58\u0610\n8\58\u0612\n8\38\38\58"+
		"\u0616\n8\38\38\38\38\38\78\u061d\n8\f8\168\u0620\138\38\38\58\u0624\n"+
		"8\58\u0626\n8\38\38\38\38\38\78\u062d\n8\f8\168\u0630\138\38\38\38\38"+
		"\38\38\78\u0638\n8\f8\168\u063b\138\38\38\78\u063f\n8\f8\168\u0642\13"+
		"8\58\u0644\n8\39\39\39\39\39\59\u064b\n9\3:\5:\u064e\n:\3:\3:\3;\3;\3"+
		"<\3<\3=\3=\3>\3>\5>\u065a\n>\3?\3?\3@\3@\3A\3A\3B\3B\3C\3C\3D\3D\3E\3"+
		"E\3F\3F\3G\3G\3H\3H\3I\3I\3J\3J\3K\3K\3L\3L\3M\3M\3N\3N\3O\3O\3P\3P\3"+
		"Q\3Q\3R\3R\3R\3R\3R\3R\5R\u0688\nR\3S\3S\3T\3T\3T\3T\3T\3T\3T\5T\u0693"+
		"\nT\3T\4\u0182\u03f7\3PU\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&("+
		"*,.\60\62\64\668:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084"+
		"\u0086\u0088\u008a\u008c\u008e\u0090\u0092\u0094\u0096\u0098\u009a\u009c"+
		"\u009e\u00a0\u00a2\u00a4\u00a6\2\25\5\2<<GGTT\4\2\61\61DD\4\2\7\7ll\3"+
		"\2\u0085\u0086\4\2\37\37@@\4\2$$>>\7\2\33\33JJSS||\177\177\4\2\t\t\16"+
		"\17\3\2\n\13\3\2\20\23\3\2\24\27\4\2\b\b\30\32\6\2OOcceexx\4\2==\u008d"+
		"\u008d\5\2\33\33JJ\177\177\6\2\668jj\u0098\u0098\u009a\u009b\4\2\n\fh"+
		"h\4\2\u0097\u0097\u009a\u009a\3\2\33\u0096\2\u0793\2\u00ac\3\2\2\2\4\u00b1"+
		"\3\2\2\2\6\u00b7\3\2\2\2\b\u00d1\3\2\2\2\n\u00f3\3\2\2\2\f\u0105\3\2\2"+
		"\2\16\u010e\3\2\2\2\20\u0116\3\2\2\2\22\u0120\3\2\2\2\24\u0128\3\2\2\2"+
		"\26\u014c\3\2\2\2\30\u016c\3\2\2\2\32\u0195\3\2\2\2\34\u01da\3\2\2\2\36"+
		"\u01ed\3\2\2\2 \u020b\3\2\2\2\"\u0215\3\2\2\2$\u0232\3\2\2\2&\u0238\3"+
		"\2\2\2(\u0245\3\2\2\2*\u0252\3\2\2\2,\u025f\3\2\2\2.\u026d\3\2\2\2\60"+
		"\u028d\3\2\2\2\62\u02d7\3\2\2\2\64\u02e6\3\2\2\2\66\u02f3\3\2\2\28\u02f9"+
		"\3\2\2\2:\u0307\3\2\2\2<\u030b\3\2\2\2>\u0323\3\2\2\2@\u038c\3\2\2\2B"+
		"\u038f\3\2\2\2D\u03b2\3\2\2\2F\u03e8\3\2\2\2H\u03ea\3\2\2\2J\u03f5\3\2"+
		"\2\2L\u0407\3\2\2\2N\u042f\3\2\2\2P\u047d\3\2\2\2R\u04d9\3\2\2\2T\u0509"+
		"\3\2\2\2V\u0513\3\2\2\2X\u051d\3\2\2\2Z\u0545\3\2\2\2\\\u0554\3\2\2\2"+
		"^\u055e\3\2\2\2`\u0569\3\2\2\2b\u056b\3\2\2\2d\u058a\3\2\2\2f\u05ce\3"+
		"\2\2\2h\u05d0\3\2\2\2j\u05e7\3\2\2\2l\u05f7\3\2\2\2n\u0643\3\2\2\2p\u064a"+
		"\3\2\2\2r\u064d\3\2\2\2t\u0651\3\2\2\2v\u0653\3\2\2\2x\u0655\3\2\2\2z"+
		"\u0659\3\2\2\2|\u065b\3\2\2\2~\u065d\3\2\2\2\u0080\u065f\3\2\2\2\u0082"+
		"\u0661\3\2\2\2\u0084\u0663\3\2\2\2\u0086\u0665\3\2\2\2\u0088\u0667\3\2"+
		"\2\2\u008a\u0669\3\2\2\2\u008c\u066b\3\2\2\2\u008e\u066d\3\2\2\2\u0090"+
		"\u066f\3\2\2\2\u0092\u0671\3\2\2\2\u0094\u0673\3\2\2\2\u0096\u0675\3\2"+
		"\2\2\u0098\u0677\3\2\2\2\u009a\u0679\3\2\2\2\u009c\u067b\3\2\2\2\u009e"+
		"\u067d\3\2\2\2\u00a0\u067f\3\2\2\2\u00a2\u0687\3\2\2\2\u00a4\u0689\3\2"+
		"\2\2\u00a6\u0692\3\2\2\2\u00a8\u00ab\5\6\4\2\u00a9\u00ab\5\4\3\2\u00aa"+
		"\u00a8\3\2\2\2\u00aa\u00a9\3\2\2\2\u00ab\u00ae\3\2\2\2\u00ac\u00aa\3\2"+
		"\2\2\u00ac\u00ad\3\2\2\2\u00ad\u00af\3\2\2\2\u00ae\u00ac\3\2\2\2\u00af"+
		"\u00b0\7\2\2\3\u00b0\3\3\2\2\2\u00b1\u00b2\7\u009f\2\2\u00b2\u00b3\b\3"+
		"\1\2\u00b3\5\3\2\2\2\u00b4\u00b6\7\3\2\2\u00b5\u00b4\3\2\2\2\u00b6\u00b9"+
		"\3\2\2\2\u00b7\u00b5\3\2\2\2\u00b7\u00b8\3\2\2\2\u00b8\u00ba\3\2\2\2\u00b9"+
		"\u00b7\3\2\2\2\u00ba\u00c3\5\b\5\2\u00bb\u00bd\7\3\2\2\u00bc\u00bb\3\2"+
		"\2\2\u00bd\u00be\3\2\2\2\u00be\u00bc\3\2\2\2\u00be\u00bf\3\2\2\2\u00bf"+
		"\u00c0\3\2\2\2\u00c0\u00c2\5\b\5\2\u00c1\u00bc\3\2\2\2\u00c2\u00c5\3\2"+
		"\2\2\u00c3\u00c1\3\2\2\2\u00c3\u00c4\3\2\2\2\u00c4\u00c9\3\2\2\2\u00c5"+
		"\u00c3\3\2\2\2\u00c6\u00c8\7\3\2\2\u00c7\u00c6\3\2\2\2\u00c8\u00cb\3\2"+
		"\2\2\u00c9\u00c7\3\2\2\2\u00c9\u00ca\3\2\2\2\u00ca\7\3\2\2\2\u00cb\u00c9"+
		"\3\2\2\2\u00cc\u00cf\7I\2\2\u00cd\u00ce\7t\2\2\u00ce\u00d0\7q\2\2\u00cf"+
		"\u00cd\3\2\2\2\u00cf\u00d0\3\2\2\2\u00d0\u00d2\3\2\2\2\u00d1\u00cc\3\2"+
		"\2\2\u00d1\u00d2\3\2\2\2\u00d2\u00f1\3\2\2\2\u00d3\u00f2\5\n\6\2\u00d4"+
		"\u00f2\5\f\7\2\u00d5\u00f2\5\16\b\2\u00d6\u00f2\5\20\t\2\u00d7\u00f2\5"+
		"\22\n\2\u00d8\u00f2\5\24\13\2\u00d9\u00f2\5\26\f\2\u00da\u00f2\5\30\r"+
		"\2\u00db\u00f2\5\32\16\2\u00dc\u00f2\5\34\17\2\u00dd\u00f2\5\36\20\2\u00de"+
		"\u00f2\5 \21\2\u00df\u00f2\5\"\22\2\u00e0\u00f2\5$\23\2\u00e1\u00f2\5"+
		"&\24\2\u00e2\u00f2\5(\25\2\u00e3\u00f2\5*\26\2\u00e4\u00f2\5,\27\2\u00e5"+
		"\u00f2\5.\30\2\u00e6\u00f2\5\60\31\2\u00e7\u00f2\5\62\32\2\u00e8\u00f2"+
		"\5\64\33\2\u00e9\u00f2\5\66\34\2\u00ea\u00f2\58\35\2\u00eb\u00f2\5:\36"+
		"\2\u00ec\u00f2\5<\37\2\u00ed\u00f2\5> \2\u00ee\u00f2\5B\"\2\u00ef\u00f2"+
		"\5D#\2\u00f0\u00f2\5F$\2\u00f1\u00d3\3\2\2\2\u00f1\u00d4\3\2\2\2\u00f1"+
		"\u00d5\3\2\2\2\u00f1\u00d6\3\2\2\2\u00f1\u00d7\3\2\2\2\u00f1\u00d8\3\2"+
		"\2\2\u00f1\u00d9\3\2\2\2\u00f1\u00da\3\2\2\2\u00f1\u00db\3\2\2\2\u00f1"+
		"\u00dc\3\2\2\2\u00f1\u00dd\3\2\2\2\u00f1\u00de\3\2\2\2\u00f1\u00df\3\2"+
		"\2\2\u00f1\u00e0\3\2\2\2\u00f1\u00e1\3\2\2\2\u00f1\u00e2\3\2\2\2\u00f1"+
		"\u00e3\3\2\2\2\u00f1\u00e4\3\2\2\2\u00f1\u00e5\3\2\2\2\u00f1\u00e6\3\2"+
		"\2\2\u00f1\u00e7\3\2\2\2\u00f1\u00e8\3\2\2\2\u00f1\u00e9\3\2\2\2\u00f1"+
		"\u00ea\3\2\2\2\u00f1\u00eb\3\2\2\2\u00f1\u00ec\3\2\2\2\u00f1\u00ed\3\2"+
		"\2\2\u00f1\u00ee\3\2\2\2\u00f1\u00ef\3\2\2\2\u00f1\u00f0\3\2\2\2\u00f2"+
		"\t\3\2\2\2\u00f3\u00f4\7 \2\2\u00f4\u00f8\7\u0084\2\2\u00f5\u00f6\5\u0084"+
		"C\2\u00f6\u00f7\7\4\2\2\u00f7\u00f9\3\2\2\2\u00f8\u00f5\3\2\2\2\u00f8"+
		"\u00f9\3\2\2\2\u00f9\u00fa\3\2\2\2\u00fa\u0103\5\u008aF\2\u00fb\u00fc"+
		"\7{\2\2\u00fc\u00fd\7\u0088\2\2\u00fd\u0104\5\u008eH\2\u00fe\u0100\7\35"+
		"\2\2\u00ff\u0101\7\60\2\2\u0100\u00ff\3\2\2\2\u0100\u0101\3\2\2\2\u0101"+
		"\u0102\3\2\2\2\u0102\u0104\5H%\2\u0103\u00fb\3\2\2\2\u0103\u00fe\3\2\2"+
		"\2\u0104\13\3\2\2\2\u0105\u010c\7!\2\2\u0106\u010d\5\u0084C\2\u0107\u010d"+
		"\5\u008cG\2\u0108\u0109\5\u0084C\2\u0109\u010a\7\4\2\2\u010a\u010b\5\u008c"+
		"G\2\u010b\u010d\3\2\2\2\u010c\u0106\3\2\2\2\u010c\u0107\3\2\2\2\u010c"+
		"\u0108\3\2\2\2\u010c\u010d\3\2\2\2\u010d\r\3\2\2\2\u010e\u0110\7%\2\2"+
		"\u010f\u0111\79\2\2\u0110\u010f\3\2\2\2\u0110\u0111\3\2\2\2\u0111\u0112"+
		"\3\2\2\2\u0112\u0113\5P)\2\u0113\u0114\7#\2\2\u0114\u0115\5\u0084C\2\u0115"+
		"\17\3\2\2\2\u0116\u0118\7(\2\2\u0117\u0119\t\2\2\2\u0118\u0117\3\2\2\2"+
		"\u0118\u0119\3\2\2\2\u0119\u011e\3\2\2\2\u011a\u011c\7\u0089\2\2\u011b"+
		"\u011d\5\u00a4S\2\u011c\u011b\3\2\2\2\u011c\u011d\3\2\2\2\u011d\u011f"+
		"\3\2\2\2\u011e\u011a\3\2\2\2\u011e\u011f\3\2\2\2\u011f\21\3\2\2\2\u0120"+
		"\u0125\t\3\2\2\u0121\u0123\7\u0089\2\2\u0122\u0124\5\u00a4S\2\u0123\u0122"+
		"\3\2\2\2\u0123\u0124\3\2\2\2\u0124\u0126\3\2\2\2\u0125\u0121\3\2\2\2\u0125"+
		"\u0126\3\2\2\2\u0126\23\3\2\2\2\u0127\u0129\5Z.\2\u0128\u0127\3\2\2\2"+
		"\u0128\u0129\3\2\2\2\u0129\u012a\3\2\2\2\u012a\u0134\5n8\2\u012b\u012d"+
		"\7\u008b\2\2\u012c\u012e\7\37\2\2\u012d\u012c\3\2\2\2\u012d\u012e\3\2"+
		"\2\2\u012e\u0132\3\2\2\2\u012f\u0132\7\\\2\2\u0130\u0132\7F\2\2\u0131"+
		"\u012b\3\2\2\2\u0131\u012f\3\2\2\2\u0131\u0130\3\2\2\2\u0132\u0133\3\2"+
		"\2\2\u0133\u0135\5n8\2\u0134\u0131\3\2\2\2\u0135\u0136\3\2\2\2\u0136\u0134"+
		"\3\2\2\2\u0136\u0137\3\2\2\2\u0137\u0142\3\2\2\2\u0138\u0139\7o\2\2\u0139"+
		"\u013a\7*\2\2\u013a\u013f\5^\60\2\u013b\u013c\7\7\2\2\u013c\u013e\5^\60"+
		"\2\u013d\u013b\3\2\2\2\u013e\u0141\3\2\2\2\u013f\u013d\3\2\2\2\u013f\u0140"+
		"\3\2\2\2\u0140\u0143\3\2\2\2\u0141\u013f\3\2\2\2\u0142\u0138\3\2\2\2\u0142"+
		"\u0143\3\2\2\2\u0143\u014a\3\2\2\2\u0144\u0145\7d\2\2\u0145\u0148\5P)"+
		"\2\u0146\u0147\t\4\2\2\u0147\u0149\5P)\2\u0148\u0146\3\2\2\2\u0148\u0149"+
		"\3\2\2\2\u0149\u014b\3\2\2\2\u014a\u0144\3\2\2\2\u014a\u014b\3\2\2\2\u014b"+
		"\25\3\2\2\2\u014c\u014e\7\64\2\2\u014d\u014f\7\u008c\2\2\u014e\u014d\3"+
		"\2\2\2\u014e\u014f\3\2\2\2\u014f\u0150\3\2\2\2\u0150\u0154\7V\2\2\u0151"+
		"\u0152\7R\2\2\u0152\u0153\7h\2\2\u0153\u0155\7H\2\2\u0154\u0151\3\2\2"+
		"\2\u0154\u0155\3\2\2\2\u0155\u0159\3\2\2\2\u0156\u0157\5\u0084C\2\u0157"+
		"\u0158\7\4\2\2\u0158\u015a\3\2\2\2\u0159\u0156\3\2\2\2\u0159\u015a\3\2"+
		"\2\2\u015a\u015b\3\2\2\2\u015b\u015c\5\u0096L\2\u015c\u015d\7m\2\2\u015d"+
		"\u015e\5\u008aF\2\u015e\u015f\7\5\2\2\u015f\u0164\5V,\2\u0160\u0161\7"+
		"\7\2\2\u0161\u0163\5V,\2\u0162\u0160\3\2\2\2\u0163\u0166\3\2\2\2\u0164"+
		"\u0162\3\2\2\2\u0164\u0165\3\2\2\2\u0165\u0167\3\2\2\2\u0166\u0164\3\2"+
		"\2\2\u0167\u016a\7\6\2\2\u0168\u0169\7\u0094\2\2\u0169\u016b\5P)\2\u016a"+
		"\u0168\3\2\2\2\u016a\u016b\3\2\2\2\u016b\27\3\2\2\2\u016c\u016e\7\64\2"+
		"\2\u016d\u016f\t\5\2\2\u016e\u016d\3\2\2\2\u016e\u016f\3\2\2\2\u016f\u0170"+
		"\3\2\2\2\u0170\u0174\7\u0084\2\2\u0171\u0172\7R\2\2\u0172\u0173\7h\2\2"+
		"\u0173\u0175\7H\2\2\u0174\u0171\3\2\2\2\u0174\u0175\3\2\2\2\u0175\u0179"+
		"\3\2\2\2\u0176\u0177\5\u0084C\2\u0177\u0178\7\4\2\2\u0178\u017a\3\2\2"+
		"\2\u0179\u0176\3\2\2\2\u0179\u017a\3\2\2\2\u017a\u017b\3\2\2\2\u017b\u0193"+
		"\5\u008aF\2\u017c\u017d\7\5\2\2\u017d\u0182\5H%\2\u017e\u017f\7\7\2\2"+
		"\u017f\u0181\5H%\2\u0180\u017e\3\2\2\2\u0181\u0184\3\2\2\2\u0182\u0183"+
		"\3\2\2\2\u0182\u0180\3\2\2\2\u0183\u0189\3\2\2\2\u0184\u0182\3\2\2\2\u0185"+
		"\u0186\7\7\2\2\u0186\u0188\5X-\2\u0187\u0185\3\2\2\2\u0188\u018b\3\2\2"+
		"\2\u0189\u0187\3\2\2\2\u0189\u018a\3\2\2\2\u018a\u018c\3\2\2\2\u018b\u0189"+
		"\3\2\2\2\u018c\u018f\7\6\2\2\u018d\u018e\7\u0096\2\2\u018e\u0190\7\u0097"+
		"\2\2\u018f\u018d\3\2\2\2\u018f\u0190\3\2\2\2\u0190\u0194\3\2\2\2\u0191"+
		"\u0192\7#\2\2\u0192\u0194\5> \2\u0193\u017c\3\2\2\2\u0193\u0191\3\2\2"+
		"\2\u0194\31\3\2\2\2\u0195\u0197\7\64\2\2\u0196\u0198\t\5\2\2\u0197\u0196"+
		"\3\2\2\2\u0197\u0198\3\2\2\2\u0198\u0199\3\2\2\2\u0199\u019d\7\u008a\2"+
		"\2\u019a\u019b\7R\2\2\u019b\u019c\7h\2\2\u019c\u019e\7H\2\2\u019d\u019a"+
		"\3\2\2\2\u019d\u019e\3\2\2\2\u019e\u01a2\3\2\2\2\u019f\u01a0\5\u0084C"+
		"\2\u01a0\u01a1\7\4\2\2\u01a1\u01a3\3\2\2\2\u01a2\u019f\3\2\2\2\u01a2\u01a3"+
		"\3\2\2\2\u01a3\u01a4\3\2\2\2\u01a4\u01a9\5\u0098M\2\u01a5\u01aa\7\'\2"+
		"\2\u01a6\u01aa\7\36\2\2\u01a7\u01a8\7[\2\2\u01a8\u01aa\7k\2\2\u01a9\u01a5"+
		"\3\2\2\2\u01a9\u01a6\3\2\2\2\u01a9\u01a7\3\2\2\2\u01a9\u01aa\3\2\2\2\u01aa"+
		"\u01b9\3\2\2\2\u01ab\u01ba\7=\2\2\u01ac\u01ba\7Z\2\2\u01ad\u01b7\7\u008d"+
		"\2\2\u01ae\u01af\7k\2\2\u01af\u01b4\5\u0090I\2\u01b0\u01b1\7\7\2\2\u01b1"+
		"\u01b3\5\u0090I\2\u01b2\u01b0\3\2\2\2\u01b3\u01b6\3\2\2\2\u01b4\u01b2"+
		"\3\2\2\2\u01b4\u01b5\3\2\2\2\u01b5\u01b8\3\2\2\2\u01b6\u01b4\3\2\2\2\u01b7"+
		"\u01ae\3\2\2\2\u01b7\u01b8\3\2\2\2\u01b8\u01ba\3\2\2\2\u01b9\u01ab\3\2"+
		"\2\2\u01b9\u01ac\3\2\2\2\u01b9\u01ad\3\2\2\2\u01ba\u01bb\3\2\2\2\u01bb"+
		"\u01bf\7m\2\2\u01bc\u01bd\5\u0084C\2\u01bd\u01be\7\4\2\2\u01be\u01c0\3"+
		"\2\2\2\u01bf\u01bc\3\2\2\2\u01bf\u01c0\3\2\2\2\u01c0\u01c1\3\2\2\2\u01c1"+
		"\u01c5\5\u008aF\2\u01c2\u01c3\7K\2\2\u01c3\u01c4\7B\2\2\u01c4\u01c6\7"+
		"\u0080\2\2\u01c5\u01c2\3\2\2\2\u01c5\u01c6\3\2\2\2\u01c6\u01c9\3\2\2\2"+
		"\u01c7\u01c8\7\u0093\2\2\u01c8\u01ca\5P)\2\u01c9\u01c7\3\2\2\2\u01c9\u01ca"+
		"\3\2\2\2\u01ca\u01cb\3\2\2\2\u01cb\u01d4\7(\2\2\u01cc\u01d1\5B\"\2\u01cd"+
		"\u01d1\5\60\31\2\u01ce\u01d1\5 \21\2\u01cf\u01d1\5> \2\u01d0\u01cc\3\2"+
		"\2\2\u01d0\u01cd\3\2\2\2\u01d0\u01ce\3\2\2\2\u01d0\u01cf\3\2\2\2\u01d1"+
		"\u01d2\3\2\2\2\u01d2\u01d3\7\3\2\2\u01d3\u01d5\3\2\2\2\u01d4\u01d0\3\2"+
		"\2\2\u01d5\u01d6\3\2\2\2\u01d6\u01d4\3\2\2\2\u01d6\u01d7\3\2\2\2\u01d7"+
		"\u01d8\3\2\2\2\u01d8\u01d9\7D\2\2\u01d9\33\3\2\2\2\u01da\u01dc\7\64\2"+
		"\2\u01db\u01dd\t\5\2\2\u01dc\u01db\3\2\2\2\u01dc\u01dd\3\2\2\2\u01dd\u01de"+
		"\3\2\2\2\u01de\u01e2\7\u0091\2\2\u01df\u01e0\7R\2\2\u01e0\u01e1\7h\2\2"+
		"\u01e1\u01e3\7H\2\2\u01e2\u01df\3\2\2\2\u01e2\u01e3\3\2\2\2\u01e3\u01e7"+
		"\3\2\2\2\u01e4\u01e5\5\u0084C\2\u01e5\u01e6\7\4\2\2\u01e6\u01e8\3\2\2"+
		"\2\u01e7\u01e4\3\2\2\2\u01e7\u01e8\3\2\2\2\u01e8\u01e9\3\2\2\2\u01e9\u01ea"+
		"\5\u009aN\2\u01ea\u01eb\7#\2\2\u01eb\u01ec\5> \2\u01ec\35\3\2\2\2\u01ed"+
		"\u01ee\7\64\2\2\u01ee\u01ef\7\u0092\2\2\u01ef\u01f3\7\u0084\2\2\u01f0"+
		"\u01f1\7R\2\2\u01f1\u01f2\7h\2\2\u01f2\u01f4\7H\2\2\u01f3\u01f0\3\2\2"+
		"\2\u01f3\u01f4\3\2\2\2\u01f4\u01f8\3\2\2\2\u01f5\u01f6\5\u0084C\2\u01f6"+
		"\u01f7\7\4\2\2\u01f7\u01f9\3\2\2\2\u01f8\u01f5\3\2\2\2\u01f8\u01f9\3\2"+
		"\2\2\u01f9\u01fa\3\2\2\2\u01fa\u01fb\5\u008aF\2\u01fb\u01fc\7\u008e\2"+
		"\2\u01fc\u0208\5\u009cO\2\u01fd\u01fe\7\5\2\2\u01fe\u0203\5z>\2\u01ff"+
		"\u0200\7\7\2\2\u0200\u0202\5z>\2\u0201\u01ff\3\2\2\2\u0202\u0205\3\2\2"+
		"\2\u0203\u0201\3\2\2\2\u0203\u0204\3\2\2\2\u0204\u0206\3\2\2\2\u0205\u0203"+
		"\3\2\2\2\u0206\u0207\7\6\2\2\u0207\u0209\3\2\2\2\u0208\u01fd\3\2\2\2\u0208"+
		"\u0209\3\2\2\2\u0209\37\3\2\2\2\u020a\u020c\5Z.\2\u020b\u020a\3\2\2\2"+
		"\u020b\u020c\3\2\2\2\u020c\u020d\3\2\2\2\u020d\u020e\7=\2\2\u020e\u020f"+
		"\7M\2\2\u020f\u0212\5\\/\2\u0210\u0211\7\u0094\2\2\u0211\u0213\5P)\2\u0212"+
		"\u0210\3\2\2\2\u0212\u0213\3\2\2\2\u0213!\3\2\2\2\u0214\u0216\5Z.\2\u0215"+
		"\u0214\3\2\2\2\u0215\u0216\3\2\2\2\u0216\u0217\3\2\2\2\u0217\u0218\7="+
		"\2\2\u0218\u0219\7M\2\2\u0219\u021c\5\\/\2\u021a\u021b\7\u0094\2\2\u021b"+
		"\u021d\5P)\2\u021c\u021a\3\2\2\2\u021c\u021d\3\2\2\2\u021d\u0230\3\2\2"+
		"\2\u021e\u021f\7o\2\2\u021f\u0220\7*\2\2\u0220\u0225\5^\60\2\u0221\u0222"+
		"\7\7\2\2\u0222\u0224\5^\60\2\u0223\u0221\3\2\2\2\u0224\u0227\3\2\2\2\u0225"+
		"\u0223\3\2\2\2\u0225\u0226\3\2\2\2\u0226\u0229\3\2\2\2\u0227\u0225\3\2"+
		"\2\2\u0228\u021e\3\2\2\2\u0228\u0229\3\2\2\2\u0229\u022a\3\2\2\2\u022a"+
		"\u022b\7d\2\2\u022b\u022e\5P)\2\u022c\u022d\t\4\2\2\u022d\u022f\5P)\2"+
		"\u022e\u022c\3\2\2\2\u022e\u022f\3\2\2\2\u022f\u0231\3\2\2\2\u0230\u0228"+
		"\3\2\2\2\u0230\u0231\3\2\2\2\u0231#\3\2\2\2\u0232\u0234\7?\2\2\u0233\u0235"+
		"\79\2\2\u0234\u0233\3\2\2\2\u0234\u0235\3\2\2\2\u0235\u0236\3\2\2\2\u0236"+
		"\u0237\5\u0084C\2\u0237%\3\2\2\2\u0238\u0239\7A\2\2\u0239\u023c\7V\2\2"+
		"\u023a\u023b\7R\2\2\u023b\u023d\7H\2\2\u023c\u023a\3\2\2\2\u023c\u023d"+
		"\3\2\2\2\u023d\u0241\3\2\2\2\u023e\u023f\5\u0084C\2\u023f\u0240\7\4\2"+
		"\2\u0240\u0242\3\2\2\2\u0241\u023e\3\2\2\2\u0241\u0242\3\2\2\2\u0242\u0243"+
		"\3\2\2\2\u0243\u0244\5\u0096L\2\u0244\'\3\2\2\2\u0245\u0246\7A\2\2\u0246"+
		"\u0249\7\u0084\2\2\u0247\u0248\7R\2\2\u0248\u024a\7H\2\2\u0249\u0247\3"+
		"\2\2\2\u0249\u024a\3\2\2\2\u024a\u024e\3\2\2\2\u024b\u024c\5\u0084C\2"+
		"\u024c\u024d\7\4\2\2\u024d\u024f\3\2\2\2\u024e\u024b\3\2\2\2\u024e\u024f"+
		"\3\2\2\2\u024f\u0250\3\2\2\2\u0250\u0251\5\u008aF\2\u0251)\3\2\2\2\u0252"+
		"\u0253\7A\2\2\u0253\u0256\7\u008a\2\2\u0254\u0255\7R\2\2\u0255\u0257\7"+
		"H\2\2\u0256\u0254\3\2\2\2\u0256\u0257\3\2\2\2\u0257\u025b\3\2\2\2\u0258"+
		"\u0259\5\u0084C\2\u0259\u025a\7\4\2\2\u025a\u025c\3\2\2\2\u025b\u0258"+
		"\3\2\2\2\u025b\u025c\3\2\2\2\u025c\u025d\3\2\2\2\u025d\u025e\5\u0098M"+
		"\2\u025e+\3\2\2\2\u025f\u0260\7A\2\2\u0260\u0263\7\u0091\2\2\u0261\u0262"+
		"\7R\2\2\u0262\u0264\7H\2\2\u0263\u0261\3\2\2\2\u0263\u0264\3\2\2\2\u0264"+
		"\u0268\3\2\2\2\u0265\u0266\5\u0084C\2\u0266\u0267\7\4\2\2\u0267\u0269"+
		"\3\2\2\2\u0268\u0265\3\2\2\2\u0268\u0269\3\2\2\2\u0269\u026a\3\2\2\2\u026a"+
		"\u026b\5\u009aN\2\u026b-\3\2\2\2\u026c\u026e\5Z.\2\u026d\u026c\3\2\2\2"+
		"\u026d\u026e\3\2\2\2\u026e\u026f\3\2\2\2\u026f\u0275\5n8\2\u0270\u0271"+
		"\5p9\2\u0271\u0272\5n8\2\u0272\u0274\3\2\2\2\u0273\u0270\3\2\2\2\u0274"+
		"\u0277\3\2\2\2\u0275\u0273\3\2\2\2\u0275\u0276\3\2\2\2\u0276\u0282\3\2"+
		"\2\2\u0277\u0275\3\2\2\2\u0278\u0279\7o\2\2\u0279\u027a\7*\2\2\u027a\u027f"+
		"\5^\60\2\u027b\u027c\7\7\2\2\u027c\u027e\5^\60\2\u027d\u027b\3\2\2\2\u027e"+
		"\u0281\3\2\2\2\u027f\u027d\3\2\2\2\u027f\u0280\3\2\2\2\u0280\u0283\3\2"+
		"\2\2\u0281\u027f\3\2\2\2\u0282\u0278\3\2\2\2\u0282\u0283\3\2\2\2\u0283"+
		"\u028a\3\2\2\2\u0284\u0285\7d\2\2\u0285\u0288\5P)\2\u0286\u0287\t\4\2"+
		"\2\u0287\u0289\5P)\2\u0288\u0286\3\2\2\2\u0288\u0289\3\2\2\2\u0289\u028b"+
		"\3\2\2\2\u028a\u0284\3\2\2\2\u028a\u028b\3\2\2\2\u028b/\3\2\2\2\u028c"+
		"\u028e\5Z.\2\u028d\u028c\3\2\2\2\u028d\u028e\3\2\2\2\u028e\u02a0\3\2\2"+
		"\2\u028f\u02a1\7Z\2\2\u0290\u02a1\7|\2\2\u0291\u0292\7Z\2\2\u0292\u0293"+
		"\7n\2\2\u0293\u02a1\7|\2\2\u0294\u0295\7Z\2\2\u0295\u0296\7n\2\2\u0296"+
		"\u02a1\7\177\2\2\u0297\u0298\7Z\2\2\u0298\u0299\7n\2\2\u0299\u02a1\7\33"+
		"\2\2\u029a\u029b\7Z\2\2\u029b\u029c\7n\2\2\u029c\u02a1\7J\2\2\u029d\u029e"+
		"\7Z\2\2\u029e\u029f\7n\2\2\u029f\u02a1\7S\2\2\u02a0\u028f\3\2\2\2\u02a0"+
		"\u0290\3\2\2\2\u02a0\u0291\3\2\2\2\u02a0\u0294\3\2\2\2\u02a0\u0297\3\2"+
		"\2\2\u02a0\u029a\3\2\2\2\u02a0\u029d\3\2\2\2\u02a1\u02a2\3\2\2\2\u02a2"+
		"\u02a6\7]\2\2\u02a3\u02a4\5\u0084C\2\u02a4\u02a5\7\4\2\2\u02a5\u02a7\3"+
		"\2\2\2\u02a6\u02a3\3\2\2\2\u02a6\u02a7\3\2\2\2\u02a7\u02a8\3\2\2\2\u02a8"+
		"\u02b4\5\u008aF\2\u02a9\u02aa\7\5\2\2\u02aa\u02af\5\u0090I\2\u02ab\u02ac"+
		"\7\7\2\2\u02ac\u02ae\5\u0090I\2\u02ad\u02ab\3\2\2\2\u02ae\u02b1\3\2\2"+
		"\2\u02af\u02ad\3\2\2\2\u02af\u02b0\3\2\2\2\u02b0\u02b2\3\2\2\2\u02b1\u02af"+
		"\3\2\2\2\u02b2\u02b3\7\6\2\2\u02b3\u02b5\3\2\2\2\u02b4\u02a9\3\2\2\2\u02b4"+
		"\u02b5\3\2\2\2\u02b5\u02d5\3\2\2\2\u02b6\u02b7\7\u0090\2\2\u02b7\u02b8"+
		"\7\5\2\2\u02b8\u02bd\5P)\2\u02b9\u02ba\7\7\2\2\u02ba\u02bc\5P)\2\u02bb"+
		"\u02b9\3\2\2\2\u02bc\u02bf\3\2\2\2\u02bd\u02bb\3\2\2\2\u02bd\u02be\3\2"+
		"\2\2\u02be\u02c0\3\2\2\2\u02bf\u02bd\3\2\2\2\u02c0\u02cf\7\6\2\2\u02c1"+
		"\u02c2\7\7\2\2\u02c2\u02c3\7\5\2\2\u02c3\u02c8\5P)\2\u02c4\u02c5\7\7\2"+
		"\2\u02c5\u02c7\5P)\2\u02c6\u02c4\3\2\2\2\u02c7\u02ca\3\2\2\2\u02c8\u02c6"+
		"\3\2\2\2\u02c8\u02c9\3\2\2\2\u02c9\u02cb\3\2\2\2\u02ca\u02c8\3\2\2\2\u02cb"+
		"\u02cc\7\6\2\2\u02cc\u02ce\3\2\2\2\u02cd\u02c1\3\2\2\2\u02ce\u02d1\3\2"+
		"\2\2\u02cf\u02cd\3\2\2\2\u02cf\u02d0\3\2\2\2\u02d0\u02d6\3\2\2\2\u02d1"+
		"\u02cf\3\2\2\2\u02d2\u02d6\5> \2\u02d3\u02d4\7:\2\2\u02d4\u02d6\7\u0090"+
		"\2\2\u02d5\u02b6\3\2\2\2\u02d5\u02d2\3\2\2\2\u02d5\u02d3\3\2\2\2\u02d6"+
		"\61\3\2\2\2\u02d7\u02db\7r\2\2\u02d8\u02d9\5\u0084C\2\u02d9\u02da\7\4"+
		"\2\2\u02da\u02dc\3\2\2\2\u02db\u02d8\3\2\2\2\u02db\u02dc\3\2\2\2\u02dc"+
		"\u02dd\3\2\2\2\u02dd\u02e4\5\u009eP\2\u02de\u02df\7\b\2\2\u02df\u02e5"+
		"\5`\61\2\u02e0\u02e1\7\5\2\2\u02e1\u02e2\5`\61\2\u02e2\u02e3\7\6\2\2\u02e3"+
		"\u02e5\3\2\2\2\u02e4\u02de\3\2\2\2\u02e4\u02e0\3\2\2\2\u02e4\u02e5\3\2"+
		"\2\2\u02e5\63\3\2\2\2\u02e6\u02f1\7y\2\2\u02e7\u02f2\5\u0092J\2\u02e8"+
		"\u02e9\5\u0084C\2\u02e9\u02ea\7\4\2\2\u02ea\u02ec\3\2\2\2\u02eb\u02e8"+
		"\3\2\2\2\u02eb\u02ec\3\2\2\2\u02ec\u02ef\3\2\2\2\u02ed\u02f0\5\u008aF"+
		"\2\u02ee\u02f0\5\u0096L\2\u02ef\u02ed\3\2\2\2\u02ef\u02ee\3\2\2\2\u02f0"+
		"\u02f2\3\2\2\2\u02f1\u02e7\3\2\2\2\u02f1\u02eb\3\2\2\2\u02f1\u02f2\3\2"+
		"\2\2\u02f2\65\3\2\2\2\u02f3\u02f5\7z\2\2\u02f4\u02f6\7\u0081\2\2\u02f5"+
		"\u02f4\3\2\2\2\u02f5\u02f6\3\2\2\2\u02f6\u02f7\3\2\2\2\u02f7\u02f8\5\u00a0"+
		"Q\2\u02f8\67\3\2\2\2\u02f9\u02fe\7\177\2\2\u02fa\u02fc\7\u0089\2\2\u02fb"+
		"\u02fd\5\u00a4S\2\u02fc\u02fb\3\2\2\2\u02fc\u02fd\3\2\2\2\u02fd\u02ff"+
		"\3\2\2\2\u02fe\u02fa\3\2\2\2\u02fe\u02ff\3\2\2\2\u02ff\u0305\3\2\2\2\u0300"+
		"\u0302\7\u0088\2\2\u0301\u0303\7\u0081\2\2\u0302\u0301\3\2\2\2\u0302\u0303"+
		"\3\2\2\2\u0303\u0304\3\2\2\2\u0304\u0306\5\u00a0Q\2\u0305\u0300\3\2\2"+
		"\2\u0305\u0306\3\2\2\2\u03069\3\2\2\2\u0307\u0308\7\u0081\2\2\u0308\u0309"+
		"\5\u00a0Q\2\u0309;\3\2\2\2\u030a\u030c\5Z.\2\u030b\u030a\3\2\2\2\u030b"+
		"\u030c\3\2\2\2\u030c\u030d\3\2\2\2\u030d\u0318\5n8\2\u030e\u030f\7o\2"+
		"\2\u030f\u0310\7*\2\2\u0310\u0315\5^\60\2\u0311\u0312\7\7\2\2\u0312\u0314"+
		"\5^\60\2\u0313\u0311\3\2\2\2\u0314\u0317\3\2\2\2\u0315\u0313\3\2\2\2\u0315"+
		"\u0316\3\2\2\2\u0316\u0319\3\2\2\2\u0317\u0315\3\2\2\2\u0318\u030e\3\2"+
		"\2\2\u0318\u0319\3\2\2\2\u0319\u0320\3\2\2\2\u031a\u031b\7d\2\2\u031b"+
		"\u031e\5P)\2\u031c\u031d\t\4\2\2\u031d\u031f\5P)\2\u031e\u031c\3\2\2\2"+
		"\u031e\u031f\3\2\2\2\u031f\u0321\3\2\2\2\u0320\u031a\3\2\2\2\u0320\u0321"+
		"\3\2\2\2\u0321=\3\2\2\2\u0322\u0324\5Z.\2\u0323\u0322\3\2\2\2\u0323\u0324"+
		"\3\2\2\2\u0324\u0325\3\2\2\2\u0325\u032b\5@!\2\u0326\u0327\5p9\2\u0327"+
		"\u0328\5@!\2\u0328\u032a\3\2\2\2\u0329\u0326\3\2\2\2\u032a\u032d\3\2\2"+
		"\2\u032b\u0329\3\2\2\2\u032b\u032c\3\2\2\2\u032c\u0338\3\2\2\2\u032d\u032b"+
		"\3\2\2\2\u032e\u032f\7o\2\2\u032f\u0330\7*\2\2\u0330\u0335\5^\60\2\u0331"+
		"\u0332\7\7\2\2\u0332\u0334\5^\60\2\u0333\u0331\3\2\2\2\u0334\u0337\3\2"+
		"\2\2\u0335\u0333\3\2\2\2\u0335\u0336\3\2\2\2\u0336\u0339\3\2\2\2\u0337"+
		"\u0335\3\2\2\2\u0338\u032e\3\2\2\2\u0338\u0339\3\2\2\2\u0339\u0340\3\2"+
		"\2\2\u033a\u033b\7d\2\2\u033b\u033e\5P)\2\u033c\u033d\t\4\2\2\u033d\u033f"+
		"\5P)\2\u033e\u033c\3\2\2\2\u033e\u033f\3\2\2\2\u033f\u0341\3\2\2\2\u0340"+
		"\u033a\3\2\2\2\u0340\u0341\3\2\2\2\u0341?\3\2\2\2\u0342\u0344\7\u0082"+
		"\2\2\u0343\u0345\t\6\2\2\u0344\u0343\3\2\2\2\u0344\u0345\3\2\2\2\u0345"+
		"\u0346\3\2\2\2\u0346\u034b\5d\63\2\u0347\u0348\7\7\2\2\u0348\u034a\5d"+
		"\63\2\u0349\u0347\3\2\2\2\u034a\u034d\3\2\2\2\u034b\u0349\3\2\2\2\u034b"+
		"\u034c\3\2\2\2\u034c\u035a\3\2\2\2\u034d\u034b\3\2\2\2\u034e\u0358\7M"+
		"\2\2\u034f\u0354\5f\64\2\u0350\u0351\7\7\2\2\u0351\u0353\5f\64\2\u0352"+
		"\u0350\3\2\2\2\u0353\u0356\3\2\2\2\u0354\u0352\3\2\2\2\u0354\u0355\3\2"+
		"\2\2\u0355\u0359\3\2\2\2\u0356\u0354\3\2\2\2\u0357\u0359\5h\65\2\u0358"+
		"\u034f\3\2\2\2\u0358\u0357\3\2\2\2\u0359\u035b\3\2\2\2\u035a\u034e\3\2"+
		"\2\2\u035a\u035b\3\2\2\2\u035b\u035e\3\2\2\2\u035c\u035d\7\u0094\2\2\u035d"+
		"\u035f\5P)\2\u035e\u035c\3\2\2\2\u035e\u035f\3\2\2\2\u035f\u036e\3\2\2"+
		"\2\u0360\u0361\7P\2\2\u0361\u0362\7*\2\2\u0362\u0367\5P)\2\u0363\u0364"+
		"\7\7\2\2\u0364\u0366\5P)\2\u0365\u0363\3\2\2\2\u0366\u0369\3\2\2\2\u0367"+
		"\u0365\3\2\2\2\u0367\u0368\3\2\2\2\u0368\u036c\3\2\2\2\u0369\u0367\3\2"+
		"\2\2\u036a\u036b\7Q\2\2\u036b\u036d\5P)\2\u036c\u036a\3\2\2\2\u036c\u036d"+
		"\3\2\2\2\u036d\u036f\3\2\2\2\u036e\u0360\3\2\2\2\u036e\u036f\3\2\2\2\u036f"+
		"\u038d\3\2\2\2\u0370\u0371\7\u0090\2\2\u0371\u0372\7\5\2\2\u0372\u0377"+
		"\5P)\2\u0373\u0374\7\7\2\2\u0374\u0376\5P)\2\u0375\u0373\3\2\2\2\u0376"+
		"\u0379\3\2\2\2\u0377\u0375\3\2\2\2\u0377\u0378\3\2\2\2\u0378\u037a\3\2"+
		"\2\2\u0379\u0377\3\2\2\2\u037a\u0389\7\6\2\2\u037b\u037c\7\7\2\2\u037c"+
		"\u037d\7\5\2\2\u037d\u0382\5P)\2\u037e\u037f\7\7\2\2\u037f\u0381\5P)\2"+
		"\u0380\u037e\3\2\2\2\u0381\u0384\3\2\2\2\u0382\u0380\3\2\2\2\u0382\u0383"+
		"\3\2\2\2\u0383\u0385\3\2\2\2\u0384\u0382\3\2\2\2\u0385\u0386\7\6\2\2\u0386"+
		"\u0388\3\2\2\2\u0387\u037b\3\2\2\2\u0388\u038b\3\2\2\2\u0389\u0387\3\2"+
		"\2\2\u0389\u038a\3\2\2\2\u038a\u038d\3\2\2\2\u038b\u0389\3\2\2\2\u038c"+
		"\u0342\3\2\2\2\u038c\u0370\3\2\2\2\u038dA\3\2\2\2\u038e\u0390\5Z.\2\u038f"+
		"\u038e\3\2\2\2\u038f\u0390\3\2\2\2\u0390\u0391\3\2\2\2\u0391\u039c\7\u008d"+
		"\2\2\u0392\u0393\7n\2\2\u0393\u039d\7\177\2\2\u0394\u0395\7n\2\2\u0395"+
		"\u039d\7\33\2\2\u0396\u0397\7n\2\2\u0397\u039d\7|\2\2\u0398\u0399\7n\2"+
		"\2\u0399\u039d\7J\2\2\u039a\u039b\7n\2\2\u039b\u039d\7S\2\2\u039c\u0392"+
		"\3\2\2\2\u039c\u0394\3\2\2\2\u039c\u0396\3\2\2\2\u039c\u0398\3\2\2\2\u039c"+
		"\u039a\3\2\2\2\u039c\u039d\3\2\2\2\u039d\u039e\3\2\2\2\u039e\u039f\5\\"+
		"/\2\u039f\u03a0\7\u0083\2\2\u03a0\u03a1\5\u0090I\2\u03a1\u03a2\7\b\2\2"+
		"\u03a2\u03aa\5P)\2\u03a3\u03a4\7\7\2\2\u03a4\u03a5\5\u0090I\2\u03a5\u03a6"+
		"\7\b\2\2\u03a6\u03a7\5P)\2\u03a7\u03a9\3\2\2\2\u03a8\u03a3\3\2\2\2\u03a9"+
		"\u03ac\3\2\2\2\u03aa\u03a8\3\2\2\2\u03aa\u03ab\3\2\2\2\u03ab\u03af\3\2"+
		"\2\2\u03ac\u03aa\3\2\2\2\u03ad\u03ae\7\u0094\2\2\u03ae\u03b0\5P)\2\u03af"+
		"\u03ad\3\2\2\2\u03af\u03b0\3\2\2\2\u03b0C\3\2\2\2\u03b1\u03b3\5Z.\2\u03b2"+
		"\u03b1\3\2\2\2\u03b2\u03b3\3\2\2\2\u03b3\u03b4\3\2\2\2\u03b4\u03bf\7\u008d"+
		"\2\2\u03b5\u03b6\7n\2\2\u03b6\u03c0\7\177\2\2\u03b7\u03b8\7n\2\2\u03b8"+
		"\u03c0\7\33\2\2\u03b9\u03ba\7n\2\2\u03ba\u03c0\7|\2\2\u03bb\u03bc\7n\2"+
		"\2\u03bc\u03c0\7J\2\2\u03bd\u03be\7n\2\2\u03be\u03c0\7S\2\2\u03bf\u03b5"+
		"\3\2\2\2\u03bf\u03b7\3\2\2\2\u03bf\u03b9\3\2\2\2\u03bf\u03bb\3\2\2\2\u03bf"+
		"\u03bd\3\2\2\2\u03bf\u03c0\3\2\2\2\u03c0\u03c1\3\2\2\2\u03c1\u03c2\5\\"+
		"/\2\u03c2\u03c3\7\u0083\2\2\u03c3\u03c4\5\u0090I\2\u03c4\u03c5\7\b\2\2"+
		"\u03c5\u03cd\5P)\2\u03c6\u03c7\7\7\2\2\u03c7\u03c8\5\u0090I\2\u03c8\u03c9"+
		"\7\b\2\2\u03c9\u03ca\5P)\2\u03ca\u03cc\3\2\2\2\u03cb\u03c6\3\2\2\2\u03cc"+
		"\u03cf\3\2\2\2\u03cd\u03cb\3\2\2\2\u03cd\u03ce\3\2\2\2\u03ce\u03d2\3\2"+
		"\2\2\u03cf\u03cd\3\2\2\2\u03d0\u03d1\7\u0094\2\2\u03d1\u03d3\5P)\2\u03d2"+
		"\u03d0\3\2\2\2\u03d2\u03d3\3\2\2\2\u03d3\u03e6\3\2\2\2\u03d4\u03d5\7o"+
		"\2\2\u03d5\u03d6\7*\2\2\u03d6\u03db\5^\60\2\u03d7\u03d8\7\7\2\2\u03d8"+
		"\u03da\5^\60\2\u03d9\u03d7\3\2\2\2\u03da\u03dd\3\2\2\2\u03db\u03d9\3\2"+
		"\2\2\u03db\u03dc\3\2\2\2\u03dc\u03df\3\2\2\2\u03dd\u03db\3\2\2\2\u03de"+
		"\u03d4\3\2\2\2\u03de\u03df\3\2\2\2\u03df\u03e0\3\2\2\2\u03e0\u03e1\7d"+
		"\2\2\u03e1\u03e4\5P)\2\u03e2\u03e3\t\4\2\2\u03e3\u03e5\5P)\2\u03e4\u03e2"+
		"\3\2\2\2\u03e4\u03e5\3\2\2\2\u03e5\u03e7\3\2\2\2\u03e6\u03de\3\2\2\2\u03e6"+
		"\u03e7\3\2\2\2\u03e7E\3\2\2\2\u03e8\u03e9\7\u008f\2\2\u03e9G\3\2\2\2\u03ea"+
		"\u03ec\5\u0090I\2\u03eb\u03ed\5J&\2\u03ec\u03eb\3\2\2\2\u03ec\u03ed\3"+
		"\2\2\2\u03ed\u03f1\3\2\2\2\u03ee\u03f0\5L\'\2\u03ef\u03ee\3\2\2\2\u03f0"+
		"\u03f3\3\2\2\2\u03f1\u03ef\3\2\2\2\u03f1\u03f2\3\2\2\2\u03f2I\3\2\2\2"+
		"\u03f3\u03f1\3\2\2\2\u03f4\u03f6\5\u0080A\2\u03f5\u03f4\3\2\2\2\u03f6"+
		"\u03f7\3\2\2\2\u03f7\u03f8\3\2\2\2\u03f7\u03f5\3\2\2\2\u03f8\u0403\3\2"+
		"\2\2\u03f9\u03fa\7\5\2\2\u03fa\u03fb\5r:\2\u03fb\u03fc\7\6\2\2\u03fc\u0404"+
		"\3\2\2\2\u03fd\u03fe\7\5\2\2\u03fe\u03ff\5r:\2\u03ff\u0400\7\7\2\2\u0400"+
		"\u0401\5r:\2\u0401\u0402\7\6\2\2\u0402\u0404\3\2\2\2\u0403\u03f9\3\2\2"+
		"\2\u0403\u03fd\3\2\2\2\u0403\u0404\3\2\2\2\u0404K\3\2\2\2\u0405\u0406"+
		"\7\63\2\2\u0406\u0408\5\u0080A\2\u0407\u0405\3\2\2\2\u0407\u0408\3\2\2"+
		"\2\u0408\u042a\3\2\2\2\u0409\u040a\7s\2\2\u040a\u040c\7a\2\2\u040b\u040d"+
		"\t\7\2\2\u040c\u040b\3\2\2\2\u040c\u040d\3\2\2\2\u040d\u040e\3\2\2\2\u040e"+
		"\u0410\5N(\2\u040f\u0411\7&\2\2\u0410\u040f\3\2\2\2\u0410\u0411\3\2\2"+
		"\2\u0411\u042b\3\2\2\2\u0412\u0414\7h\2\2\u0413\u0412\3\2\2\2\u0413\u0414"+
		"\3\2\2\2\u0414\u0415\3\2\2\2\u0415\u0416\7j\2\2\u0416\u042b\5N(\2\u0417"+
		"\u0418\7\u008c\2\2\u0418\u042b\5N(\2\u0419\u041a\7.\2\2\u041a\u041b\7"+
		"\5\2\2\u041b\u041c\5P)\2\u041c\u041d\7\6\2\2\u041d\u042b\3\2\2\2\u041e"+
		"\u0425\7:\2\2\u041f\u0426\5r:\2\u0420\u0426\5t;\2\u0421\u0422\7\5\2\2"+
		"\u0422\u0423\5P)\2\u0423\u0424\7\6\2\2\u0424\u0426\3\2\2\2\u0425\u041f"+
		"\3\2\2\2\u0425\u0420\3\2\2\2\u0425\u0421\3\2\2\2\u0426\u042b\3\2\2\2\u0427"+
		"\u0428\7/\2\2\u0428\u042b\5\u0092J\2\u0429\u042b\5R*\2\u042a\u0409\3\2"+
		"\2\2\u042a\u0413\3\2\2\2\u042a\u0417\3\2\2\2\u042a\u0419\3\2\2\2\u042a"+
		"\u041e\3\2\2\2\u042a\u0427\3\2\2\2\u042a\u0429\3\2\2\2\u042bM\3\2\2\2"+
		"\u042c\u042d\7m\2\2\u042d\u042e\7\62\2\2\u042e\u0430\t\b\2\2\u042f\u042c"+
		"\3\2\2\2\u042f\u0430\3\2\2\2\u0430O\3\2\2\2\u0431\u0432\b)\1\2\u0432\u047e"+
		"\5t;\2\u0433\u047e\7\u0099\2\2\u0434\u0435\5\u0084C\2\u0435\u0436\7\4"+
		"\2\2\u0436\u0438\3\2\2\2\u0437\u0434\3\2\2\2\u0437\u0438\3\2\2\2\u0438"+
		"\u0439\3\2\2\2\u0439\u043a\5\u008aF\2\u043a\u043b\7\4\2\2\u043b\u043d"+
		"\3\2\2\2\u043c\u0437\3\2\2\2\u043c\u043d\3\2\2\2\u043d\u043e\3\2\2\2\u043e"+
		"\u047e\5\u0090I\2\u043f\u0440\5v<\2\u0440\u0441\5P)\27\u0441\u047e\3\2"+
		"\2\2\u0442\u0443\5\u0082B\2\u0443\u0450\7\5\2\2\u0444\u0446\7@\2\2\u0445"+
		"\u0444\3\2\2\2\u0445\u0446\3\2\2\2\u0446\u0447\3\2\2\2\u0447\u044c\5P"+
		")\2\u0448\u0449\7\7\2\2\u0449\u044b\5P)\2\u044a\u0448\3\2\2\2\u044b\u044e"+
		"\3\2\2\2\u044c\u044a\3\2\2\2\u044c\u044d\3\2\2\2\u044d\u0451\3\2\2\2\u044e"+
		"\u044c\3\2\2\2\u044f\u0451\7\t\2\2\u0450\u0445\3\2\2\2\u0450\u044f\3\2"+
		"\2\2\u0450\u0451\3\2\2\2\u0451\u0452\3\2\2\2\u0452\u0453\7\6\2\2\u0453"+
		"\u047e\3\2\2\2\u0454\u0455\7\5\2\2\u0455\u0456\5P)\2\u0456\u0457\7\6\2"+
		"\2\u0457\u047e\3\2\2\2\u0458\u0459\7-\2\2\u0459\u045a\7\5\2\2\u045a\u045b"+
		"\5P)\2\u045b\u045c\7#\2\2\u045c\u045d\5J&\2\u045d\u045e\7\6\2\2\u045e"+
		"\u047e\3\2\2\2\u045f\u0461\7h\2\2\u0460\u045f\3\2\2\2\u0460\u0461\3\2"+
		"\2\2\u0461\u0462\3\2\2\2\u0462\u0464\7H\2\2\u0463\u0460\3\2\2\2\u0463"+
		"\u0464\3\2\2\2\u0464\u0465\3\2\2\2\u0465\u0466\7\5\2\2\u0466\u0467\5>"+
		" \2\u0467\u0468\7\6\2\2\u0468\u047e\3\2\2\2\u0469\u046b\7,\2\2\u046a\u046c"+
		"\5P)\2\u046b\u046a\3\2\2\2\u046b\u046c\3\2\2\2\u046c\u0472\3\2\2\2\u046d"+
		"\u046e\7\u0093\2\2\u046e\u046f\5P)\2\u046f\u0470\7\u0087\2\2\u0470\u0471"+
		"\5P)\2\u0471\u0473\3\2\2\2\u0472\u046d\3\2\2\2\u0473\u0474\3\2\2\2\u0474"+
		"\u0472\3\2\2\2\u0474\u0475\3\2\2\2\u0475\u0478\3\2\2\2\u0476\u0477\7C"+
		"\2\2\u0477\u0479\5P)\2\u0478\u0476\3\2\2\2\u0478\u0479\3\2\2\2\u0479\u047a"+
		"\3\2\2\2\u047a\u047b\7D\2\2\u047b\u047e\3\2\2\2\u047c\u047e\5T+\2\u047d"+
		"\u0431\3\2\2\2\u047d\u0433\3\2\2\2\u047d\u043c\3\2\2\2\u047d\u043f\3\2"+
		"\2\2\u047d\u0442\3\2\2\2\u047d\u0454\3\2\2\2\u047d\u0458\3\2\2\2\u047d"+
		"\u0463\3\2\2\2\u047d\u0469\3\2\2\2\u047d\u047c\3\2\2\2\u047e\u04d6\3\2"+
		"\2\2\u047f\u0480\f\26\2\2\u0480\u0481\7\r\2\2\u0481\u04d5\5P)\27\u0482"+
		"\u0483\f\25\2\2\u0483\u0484\t\t\2\2\u0484\u04d5\5P)\26\u0485\u0486\f\24"+
		"\2\2\u0486\u0487\t\n\2\2\u0487\u04d5\5P)\25\u0488\u0489\f\23\2\2\u0489"+
		"\u048a\t\13\2\2\u048a\u04d5\5P)\24\u048b\u048c\f\22\2\2\u048c\u048d\t"+
		"\f\2\2\u048d\u04d5\5P)\23\u048e\u048f\f\21\2\2\u048f\u0490\t\r\2\2\u0490"+
		"\u04d5\5P)\22\u0491\u0492\f\17\2\2\u0492\u0493\7\"\2\2\u0493\u04d5\5P"+
		")\20\u0494\u0495\f\16\2\2\u0495\u0496\7n\2\2\u0496\u04d5\5P)\17\u0497"+
		"\u0498\f\7\2\2\u0498\u049a\7^\2\2\u0499\u049b\7h\2\2\u049a\u0499\3\2\2"+
		"\2\u049a\u049b\3\2\2\2\u049b\u049c\3\2\2\2\u049c\u04d5\5P)\b\u049d\u049f"+
		"\f\6\2\2\u049e\u04a0\7h\2\2\u049f\u049e\3\2\2\2\u049f\u04a0\3\2\2\2\u04a0"+
		"\u04a1\3\2\2\2\u04a1\u04a2\7)\2\2\u04a2\u04a3\5P)\2\u04a3\u04a4\7\"\2"+
		"\2\u04a4\u04a5\5P)\7\u04a5\u04d5\3\2\2\2\u04a6\u04a8\f\20\2\2\u04a7\u04a9"+
		"\7h\2\2\u04a8\u04a7\3\2\2\2\u04a8\u04a9\3\2\2\2\u04a9\u04aa\3\2\2\2\u04aa"+
		"\u04be\7U\2\2\u04ab\u04b5\7\5\2\2\u04ac\u04b6\5> \2\u04ad\u04b2\5P)\2"+
		"\u04ae\u04af\7\7\2\2\u04af\u04b1\5P)\2\u04b0\u04ae\3\2\2\2\u04b1\u04b4"+
		"\3\2\2\2\u04b2\u04b0\3\2\2\2\u04b2\u04b3\3\2\2\2\u04b3\u04b6\3\2\2\2\u04b4"+
		"\u04b2\3\2\2\2\u04b5\u04ac\3\2\2\2\u04b5\u04ad\3\2\2\2\u04b5\u04b6\3\2"+
		"\2\2\u04b6\u04b7\3\2\2\2\u04b7\u04bf\7\6\2\2\u04b8\u04b9\5\u0084C\2\u04b9"+
		"\u04ba\7\4\2\2\u04ba\u04bc\3\2\2\2\u04bb\u04b8\3\2\2\2\u04bb\u04bc\3\2"+
		"\2\2\u04bc\u04bd\3\2\2\2\u04bd\u04bf\5\u008aF\2\u04be\u04ab\3\2\2\2\u04be"+
		"\u04bb\3\2\2\2\u04bf\u04d5\3\2\2\2\u04c0\u04c1\f\n\2\2\u04c1\u04c2\7/"+
		"\2\2\u04c2\u04d5\5\u0092J\2\u04c3\u04c5\f\t\2\2\u04c4\u04c6\7h\2\2\u04c5"+
		"\u04c4\3\2\2\2\u04c5\u04c6\3\2\2\2\u04c6\u04c7\3\2\2\2\u04c7\u04c8\t\16"+
		"\2\2\u04c8\u04cb\5P)\2\u04c9\u04ca\7E\2\2\u04ca\u04cc\5P)\2\u04cb\u04c9"+
		"\3\2\2\2\u04cb\u04cc\3\2\2\2\u04cc\u04d5\3\2\2\2\u04cd\u04d2\f\b\2\2\u04ce"+
		"\u04d3\7_\2\2\u04cf\u04d3\7i\2\2\u04d0\u04d1\7h\2\2\u04d1\u04d3\7j\2\2"+
		"\u04d2\u04ce\3\2\2\2\u04d2\u04cf\3\2\2\2\u04d2\u04d0\3\2\2\2\u04d3\u04d5"+
		"\3\2\2\2\u04d4\u047f\3\2\2\2\u04d4\u0482\3\2\2\2\u04d4\u0485\3\2\2\2\u04d4"+
		"\u0488\3\2\2\2\u04d4\u048b\3\2\2\2\u04d4\u048e\3\2\2\2\u04d4\u0491\3\2"+
		"\2\2\u04d4\u0494\3\2\2\2\u04d4\u0497\3\2\2\2\u04d4\u049d\3\2\2\2\u04d4"+
		"\u04a6\3\2\2\2\u04d4\u04c0\3\2\2\2\u04d4\u04c3\3\2\2\2\u04d4\u04cd\3\2"+
		"\2\2\u04d5\u04d8\3\2\2\2\u04d6\u04d4\3\2\2\2\u04d6\u04d7\3\2\2\2\u04d7"+
		"Q\3\2\2\2\u04d8\u04d6\3\2\2\2\u04d9\u04da\7w\2\2\u04da\u04e6\5\u0094K"+
		"\2\u04db\u04dc\7\5\2\2\u04dc\u04e1\5\u0090I\2\u04dd\u04de\7\7\2\2\u04de"+
		"\u04e0\5\u0090I\2\u04df\u04dd\3\2\2\2\u04e0\u04e3\3\2\2\2\u04e1\u04df"+
		"\3\2\2\2\u04e1\u04e2\3\2\2\2\u04e2\u04e4\3\2\2\2\u04e3\u04e1\3\2\2\2\u04e4"+
		"\u04e5\7\6\2\2\u04e5\u04e7\3\2\2\2\u04e6\u04db\3\2\2\2\u04e6\u04e7\3\2"+
		"\2\2\u04e7\u04fa\3\2\2\2\u04e8\u04e9\7m\2\2\u04e9\u04f2\t\17\2\2\u04ea"+
		"\u04eb\7\u0083\2\2\u04eb\u04f3\7j\2\2\u04ec\u04ed\7\u0083\2\2\u04ed\u04f3"+
		"\7:\2\2\u04ee\u04f3\7+\2\2\u04ef\u04f3\7}\2\2\u04f0\u04f1\7g\2\2\u04f1"+
		"\u04f3\7\34\2\2\u04f2\u04ea\3\2\2\2\u04f2\u04ec\3\2\2\2\u04f2\u04ee\3"+
		"\2\2\2\u04f2\u04ef\3\2\2\2\u04f2\u04f0\3\2\2\2\u04f3\u04f7\3\2\2\2\u04f4"+
		"\u04f5\7e\2\2\u04f5\u04f7\5\u0080A\2\u04f6\u04e8\3\2\2\2\u04f6\u04f4\3"+
		"\2\2\2\u04f7\u04f9\3\2\2\2\u04f8\u04f6\3\2\2\2\u04f9\u04fc\3\2\2\2\u04fa"+
		"\u04f8\3\2\2\2\u04fa\u04fb\3\2\2\2\u04fb\u0507\3\2\2\2\u04fc\u04fa\3\2"+
		"\2\2\u04fd\u04ff\7h\2\2\u04fe\u04fd\3\2\2\2\u04fe\u04ff\3\2\2\2\u04ff"+
		"\u0500\3\2\2\2\u0500\u0505\7;\2\2\u0501\u0502\7X\2\2\u0502\u0506\7<\2"+
		"\2\u0503\u0504\7X\2\2\u0504\u0506\7T\2\2\u0505\u0501\3\2\2\2\u0505\u0503"+
		"\3\2\2\2\u0505\u0506\3\2\2\2\u0506\u0508\3\2\2\2\u0507\u04fe\3\2\2\2\u0507"+
		"\u0508\3\2\2\2\u0508S\3\2\2\2\u0509\u050a\7u\2\2\u050a\u050f\7\5\2\2\u050b"+
		"\u0510\7S\2\2\u050c\u050d\t\20\2\2\u050d\u050e\7\7\2\2\u050e\u0510\5x"+
		"=\2\u050f\u050b\3\2\2\2\u050f\u050c\3\2\2\2\u0510\u0511\3\2\2\2\u0511"+
		"\u0512\7\6\2\2\u0512U\3\2\2\2\u0513\u0516\5\u0090I\2\u0514\u0515\7/\2"+
		"\2\u0515\u0517\5\u0092J\2\u0516\u0514\3\2\2\2\u0516\u0517\3\2\2\2\u0517"+
		"\u0519\3\2\2\2\u0518\u051a\t\7\2\2\u0519\u0518\3\2\2\2\u0519\u051a\3\2"+
		"\2\2\u051aW\3\2\2\2\u051b\u051c\7\63\2\2\u051c\u051e\5\u0080A\2\u051d"+
		"\u051b\3\2\2\2\u051d\u051e\3\2\2\2\u051e\u0543\3\2\2\2\u051f\u0520\7s"+
		"\2\2\u0520\u0523\7a\2\2\u0521\u0523\7\u008c\2\2\u0522\u051f\3\2\2\2\u0522"+
		"\u0521\3\2\2\2\u0523\u0524\3\2\2\2\u0524\u0525\7\5\2\2\u0525\u052a\5V"+
		",\2\u0526\u0527\7\7\2\2\u0527\u0529\5V,\2\u0528\u0526\3\2\2\2\u0529\u052c"+
		"\3\2\2\2\u052a\u0528\3\2\2\2\u052a\u052b\3\2\2\2\u052b\u052d\3\2\2\2\u052c"+
		"\u052a\3\2\2\2\u052d\u052e\7\6\2\2\u052e\u052f\5N(\2\u052f\u0544\3\2\2"+
		"\2\u0530\u0531\7.\2\2\u0531\u0532\7\5\2\2\u0532\u0533\5P)\2\u0533\u0534"+
		"\7\6\2\2\u0534\u0544\3\2\2\2\u0535\u0536\7L\2\2\u0536\u0537\7a\2\2\u0537"+
		"\u0538\7\5\2\2\u0538\u053d\5\u0090I\2\u0539\u053a\7\7\2\2\u053a\u053c"+
		"\5\u0090I\2\u053b\u0539\3\2\2\2\u053c\u053f\3\2\2\2\u053d\u053b\3\2\2"+
		"\2\u053d\u053e\3\2\2\2\u053e\u0540\3\2\2\2\u053f\u053d\3\2\2\2\u0540\u0541"+
		"\7\6\2\2\u0541\u0542\5R*\2\u0542\u0544\3\2\2\2\u0543\u0522\3\2\2\2\u0543"+
		"\u0530\3\2\2\2\u0543\u0535\3\2\2\2\u0544Y\3\2\2\2\u0545\u0547\7\u0095"+
		"\2\2\u0546\u0548\7v\2\2\u0547\u0546\3\2\2\2\u0547\u0548\3\2\2\2\u0548"+
		"\u0549\3\2\2\2\u0549\u054e\5b\62\2\u054a\u054b\7\7\2\2\u054b\u054d\5b"+
		"\62\2\u054c\u054a\3\2\2\2\u054d\u0550\3\2\2\2\u054e\u054c\3\2\2\2\u054e"+
		"\u054f\3\2\2\2\u054f[\3\2\2\2\u0550\u054e\3\2\2\2\u0551\u0552\5\u0084"+
		"C\2\u0552\u0553\7\4\2\2\u0553\u0555\3\2\2\2\u0554\u0551\3\2\2\2\u0554"+
		"\u0555\3\2\2\2\u0555\u0556\3\2\2\2\u0556\u055c\5\u008aF\2\u0557\u0558"+
		"\7W\2\2\u0558\u0559\7*\2\2\u0559\u055d\5\u0096L\2\u055a\u055b\7h\2\2\u055b"+
		"\u055d\7W\2\2\u055c\u0557\3\2\2\2\u055c\u055a\3\2\2\2\u055c\u055d\3\2"+
		"\2\2\u055d]\3\2\2\2\u055e\u0561\5P)\2\u055f\u0560\7/\2\2\u0560\u0562\5"+
		"\u0092J\2\u0561\u055f\3\2\2\2\u0561\u0562\3\2\2\2\u0562\u0564\3\2\2\2"+
		"\u0563\u0565\t\7\2\2\u0564\u0563\3\2\2\2\u0564\u0565\3\2\2\2\u0565_\3"+
		"\2\2\2\u0566\u056a\5r:\2\u0567\u056a\5\u0080A\2\u0568\u056a\7\u009a\2"+
		"\2\u0569\u0566\3\2\2\2\u0569\u0567\3\2\2\2\u0569\u0568\3\2\2\2\u056aa"+
		"\3\2\2\2\u056b\u0577\5\u008aF\2\u056c\u056d\7\5\2\2\u056d\u0572\5\u0090"+
		"I\2\u056e\u056f\7\7\2\2\u056f\u0571\5\u0090I\2\u0570\u056e\3\2\2\2\u0571"+
		"\u0574\3\2\2\2\u0572\u0570\3\2\2\2\u0572\u0573\3\2\2\2\u0573\u0575\3\2"+
		"\2\2\u0574\u0572\3\2\2\2\u0575\u0576\7\6\2\2\u0576\u0578\3\2\2\2\u0577"+
		"\u056c\3\2\2\2\u0577\u0578\3\2\2\2\u0578\u0579\3\2\2\2\u0579\u057a\7#"+
		"\2\2\u057a\u057b\7\5\2\2\u057b\u057c\5> \2\u057c\u057d\7\6\2\2\u057dc"+
		"\3\2\2\2\u057e\u058b\7\t\2\2\u057f\u0580\5\u008aF\2\u0580\u0581\7\4\2"+
		"\2\u0581\u0582\7\t\2\2\u0582\u058b\3\2\2\2\u0583\u0588\5P)\2\u0584\u0586"+
		"\7#\2\2\u0585\u0584\3\2\2\2\u0585\u0586\3\2\2\2\u0586\u0587\3\2\2\2\u0587"+
		"\u0589\5|?\2\u0588\u0585\3\2\2\2\u0588\u0589\3\2\2\2\u0589\u058b\3\2\2"+
		"\2\u058a\u057e\3\2\2\2\u058a\u057f\3\2\2\2\u058a\u0583\3\2\2\2\u058be"+
		"\3\2\2\2\u058c\u058d\5\u0086D\2\u058d\u058e\7\4\2\2\u058e\u0590\3\2\2"+
		"\2\u058f\u058c\3\2\2\2\u058f\u0590\3\2\2\2\u0590\u0591\3\2\2\2\u0591\u0596"+
		"\5\u008aF\2\u0592\u0594\7#\2\2\u0593\u0592\3\2\2\2\u0593\u0594\3\2\2\2"+
		"\u0594\u0595\3\2\2\2\u0595\u0597\5\u00a2R\2\u0596\u0593\3\2\2\2\u0596"+
		"\u0597\3\2\2\2\u0597\u059d\3\2\2\2\u0598\u0599\7W\2\2\u0599\u059a\7*\2"+
		"\2\u059a\u059e\5\u0096L\2\u059b\u059c\7h\2\2\u059c\u059e\7W\2\2\u059d"+
		"\u0598\3\2\2\2\u059d\u059b\3\2\2\2\u059d\u059e\3\2\2\2\u059e\u05cf\3\2"+
		"\2\2\u059f\u05a0\5\u0086D\2\u05a0\u05a1\7\4\2\2\u05a1\u05a3\3\2\2\2\u05a2"+
		"\u059f\3\2\2\2\u05a2\u05a3\3\2\2\2\u05a3\u05a4\3\2\2\2\u05a4\u05a5\5\u0088"+
		"E\2\u05a5\u05ae\7\5\2\2\u05a6\u05ab\5P)\2\u05a7\u05a8\7\7\2\2\u05a8\u05aa"+
		"\5P)\2\u05a9\u05a7\3\2\2\2\u05aa\u05ad\3\2\2\2\u05ab\u05a9\3\2\2\2\u05ab"+
		"\u05ac\3\2\2\2\u05ac\u05af\3\2\2\2\u05ad\u05ab\3\2\2\2\u05ae\u05a6\3\2"+
		"\2\2\u05ae\u05af\3\2\2\2\u05af\u05b0\3\2\2\2\u05b0\u05b5\7\6\2\2\u05b1"+
		"\u05b3\7#\2\2\u05b2\u05b1\3\2\2\2\u05b2\u05b3\3\2\2\2\u05b3\u05b4\3\2"+
		"\2\2\u05b4\u05b6\5\u00a2R\2\u05b5\u05b2\3\2\2\2\u05b5\u05b6\3\2\2\2\u05b6"+
		"\u05cf\3\2\2\2\u05b7\u05c1\7\5\2\2\u05b8\u05bd\5f\64\2\u05b9\u05ba\7\7"+
		"\2\2\u05ba\u05bc\5f\64\2\u05bb\u05b9\3\2\2\2\u05bc\u05bf\3\2\2\2\u05bd"+
		"\u05bb\3\2\2\2\u05bd\u05be\3\2\2\2\u05be\u05c2\3\2\2\2\u05bf\u05bd\3\2"+
		"\2\2\u05c0\u05c2\5h\65\2\u05c1\u05b8\3\2\2\2\u05c1\u05c0\3\2\2\2\u05c2"+
		"\u05c3\3\2\2\2\u05c3\u05c4\7\6\2\2\u05c4\u05cf\3\2\2\2\u05c5\u05c6\7\5"+
		"\2\2\u05c6\u05c7\5> \2\u05c7\u05cc\7\6\2\2\u05c8\u05ca\7#\2\2\u05c9\u05c8"+
		"\3\2\2\2\u05c9\u05ca\3\2\2\2\u05ca\u05cb\3\2\2\2\u05cb\u05cd\5\u00a2R"+
		"\2\u05cc\u05c9\3\2\2\2\u05cc\u05cd\3\2\2\2\u05cd\u05cf\3\2\2\2\u05ce\u058f"+
		"\3\2\2\2\u05ce\u05a2\3\2\2\2\u05ce\u05b7\3\2\2\2\u05ce\u05c5\3\2\2\2\u05cf"+
		"g\3\2\2\2\u05d0\u05d7\5f\64\2\u05d1\u05d2\5j\66\2\u05d2\u05d3\5f\64\2"+
		"\u05d3\u05d4\5l\67\2\u05d4\u05d6\3\2\2\2\u05d5\u05d1\3\2\2\2\u05d6\u05d9"+
		"\3\2\2\2\u05d7\u05d5\3\2\2\2\u05d7\u05d8\3\2\2\2\u05d8i\3\2\2\2\u05d9"+
		"\u05d7\3\2\2\2\u05da\u05e8\7\7\2\2\u05db\u05dd\7f\2\2\u05dc\u05db\3\2"+
		"\2\2\u05dc\u05dd\3\2\2\2\u05dd\u05e4\3\2\2\2\u05de\u05e0\7b\2\2\u05df"+
		"\u05e1\7p\2\2\u05e0\u05df\3\2\2\2\u05e0\u05e1\3\2\2\2\u05e1\u05e5\3\2"+
		"\2\2\u05e2\u05e5\7Y\2\2\u05e3\u05e5\7\65\2\2\u05e4\u05de\3\2\2\2\u05e4"+
		"\u05e2\3\2\2\2\u05e4\u05e3\3\2\2\2\u05e4\u05e5\3\2\2\2\u05e5\u05e6\3\2"+
		"\2\2\u05e6\u05e8\7`\2\2\u05e7\u05da\3\2\2\2\u05e7\u05dc\3\2\2\2\u05e8"+
		"k\3\2\2\2\u05e9\u05ea\7m\2\2\u05ea\u05f8\5P)\2\u05eb\u05ec\7\u008e\2\2"+
		"\u05ec\u05ed\7\5\2\2\u05ed\u05f2\5\u0090I\2\u05ee\u05ef\7\7\2\2\u05ef"+
		"\u05f1\5\u0090I\2\u05f0\u05ee\3\2\2\2\u05f1\u05f4\3\2\2\2\u05f2\u05f0"+
		"\3\2\2\2\u05f2\u05f3\3\2\2\2\u05f3\u05f5\3\2\2\2\u05f4\u05f2\3\2\2\2\u05f5"+
		"\u05f6\7\6\2\2\u05f6\u05f8\3\2\2\2\u05f7\u05e9\3\2\2\2\u05f7\u05eb\3\2"+
		"\2\2\u05f7\u05f8\3\2\2\2\u05f8m\3\2\2\2\u05f9\u05fb\7\u0082\2\2\u05fa"+
		"\u05fc\t\6\2\2\u05fb\u05fa\3\2\2\2\u05fb\u05fc\3\2\2\2\u05fc\u05fd\3\2"+
		"\2\2\u05fd\u0602\5d\63\2\u05fe\u05ff\7\7\2\2\u05ff\u0601\5d\63\2\u0600"+
		"\u05fe\3\2\2\2\u0601\u0604\3\2\2\2\u0602\u0600\3\2\2\2\u0602\u0603\3\2"+
		"\2\2\u0603\u0611\3\2\2\2\u0604\u0602\3\2\2\2\u0605\u060f\7M\2\2\u0606"+
		"\u060b\5f\64\2\u0607\u0608\7\7\2\2\u0608\u060a\5f\64\2\u0609\u0607\3\2"+
		"\2\2\u060a\u060d\3\2\2\2\u060b\u0609\3\2\2\2\u060b\u060c\3\2\2\2\u060c"+
		"\u0610\3\2\2\2\u060d\u060b\3\2\2\2\u060e\u0610\5h\65\2\u060f\u0606\3\2"+
		"\2\2\u060f\u060e\3\2\2\2\u0610\u0612\3\2\2\2\u0611\u0605\3\2\2\2\u0611"+
		"\u0612\3\2\2\2\u0612\u0615\3\2\2\2\u0613\u0614\7\u0094\2\2\u0614\u0616"+
		"\5P)\2\u0615\u0613\3\2\2\2\u0615\u0616\3\2\2\2\u0616\u0625\3\2\2\2\u0617"+
		"\u0618\7P\2\2\u0618\u0619\7*\2\2\u0619\u061e\5P)\2\u061a\u061b\7\7\2\2"+
		"\u061b\u061d\5P)\2\u061c\u061a\3\2\2\2\u061d\u0620\3\2\2\2\u061e\u061c"+
		"\3\2\2\2\u061e\u061f\3\2\2\2\u061f\u0623\3\2\2\2\u0620\u061e\3\2\2\2\u0621"+
		"\u0622\7Q\2\2\u0622\u0624\5P)\2\u0623\u0621\3\2\2\2\u0623\u0624\3\2\2"+
		"\2\u0624\u0626\3\2\2\2\u0625\u0617\3\2\2\2\u0625\u0626\3\2\2\2\u0626\u0644"+
		"\3\2\2\2\u0627\u0628\7\u0090\2\2\u0628\u0629\7\5\2\2\u0629\u062e\5P)\2"+
		"\u062a\u062b\7\7\2\2\u062b\u062d\5P)\2\u062c\u062a\3\2\2\2\u062d\u0630"+
		"\3\2\2\2\u062e\u062c\3\2\2\2\u062e\u062f\3\2\2\2\u062f\u0631\3\2\2\2\u0630"+
		"\u062e\3\2\2\2\u0631\u0640\7\6\2\2\u0632\u0633\7\7\2\2\u0633\u0634\7\5"+
		"\2\2\u0634\u0639\5P)\2\u0635\u0636\7\7\2\2\u0636\u0638\5P)\2\u0637\u0635"+
		"\3\2\2\2\u0638\u063b\3\2\2\2\u0639\u0637\3\2\2\2\u0639\u063a\3\2\2\2\u063a"+
		"\u063c\3\2\2\2\u063b\u0639\3\2\2\2\u063c\u063d\7\6\2\2\u063d\u063f\3\2"+
		"\2\2\u063e\u0632\3\2\2\2\u063f\u0642\3\2\2\2\u0640\u063e\3\2\2\2\u0640"+
		"\u0641\3\2\2\2\u0641\u0644\3\2\2\2\u0642\u0640\3\2\2\2\u0643\u05f9\3\2"+
		"\2\2\u0643\u0627\3\2\2\2\u0644o\3\2\2\2\u0645\u064b\7\u008b\2\2\u0646"+
		"\u0647\7\u008b\2\2\u0647\u064b\7\37\2\2\u0648\u064b\7\\\2\2\u0649\u064b"+
		"\7F\2\2\u064a\u0645\3\2\2\2\u064a\u0646\3\2\2\2\u064a\u0648\3\2\2\2\u064a"+
		"\u0649\3\2\2\2\u064bq\3\2\2\2\u064c\u064e\t\n\2\2\u064d\u064c\3\2\2\2"+
		"\u064d\u064e\3\2\2\2\u064e\u064f\3\2\2\2\u064f\u0650\7\u0098\2\2\u0650"+
		"s\3\2\2\2\u0651\u0652\t\21\2\2\u0652u\3\2\2\2\u0653\u0654\t\22\2\2\u0654"+
		"w\3\2\2\2\u0655\u0656\7\u009a\2\2\u0656y\3\2\2\2\u0657\u065a\5P)\2\u0658"+
		"\u065a\5H%\2\u0659\u0657\3\2\2\2\u0659\u0658\3\2\2\2\u065a{\3\2\2\2\u065b"+
		"\u065c\t\23\2\2\u065c}\3\2\2\2\u065d\u065e\t\24\2\2\u065e\177\3\2\2\2"+
		"\u065f\u0660\5\u00a6T\2\u0660\u0081\3\2\2\2\u0661\u0662\5\u00a6T\2\u0662"+
		"\u0083\3\2\2\2\u0663\u0664\5\u00a6T\2\u0664\u0085\3\2\2\2\u0665\u0666"+
		"\5\u00a6T\2\u0666\u0087\3\2\2\2\u0667\u0668\5\u00a6T\2\u0668\u0089\3\2"+
		"\2\2\u0669\u066a\5\u00a6T\2\u066a\u008b\3\2\2\2\u066b\u066c\5\u00a6T\2"+
		"\u066c\u008d\3\2\2\2\u066d\u066e\5\u00a6T\2\u066e\u008f\3\2\2\2\u066f"+
		"\u0670\5\u00a6T\2\u0670\u0091\3\2\2\2\u0671\u0672\5\u00a6T\2\u0672\u0093"+
		"\3\2\2\2\u0673\u0674\5\u00a6T\2\u0674\u0095\3\2\2\2\u0675\u0676\5\u00a6"+
		"T\2\u0676\u0097\3\2\2\2\u0677\u0678\5\u00a6T\2\u0678\u0099\3\2\2\2\u0679"+
		"\u067a\5\u00a6T\2\u067a\u009b\3\2\2\2\u067b\u067c\5\u00a6T\2\u067c\u009d"+
		"\3\2\2\2\u067d\u067e\5\u00a6T\2\u067e\u009f\3\2\2\2\u067f\u0680\5\u00a6"+
		"T\2\u0680\u00a1\3\2\2\2\u0681\u0688\7\u0097\2\2\u0682\u0688\7\u009a\2"+
		"\2\u0683\u0684\7\5\2\2\u0684\u0685\5\u00a2R\2\u0685\u0686\7\6\2\2\u0686"+
		"\u0688\3\2\2\2\u0687\u0681\3\2\2\2\u0687\u0682\3\2\2\2\u0687\u0683\3\2"+
		"\2\2\u0688\u00a3\3\2\2\2\u0689\u068a\5\u00a6T\2\u068a\u00a5\3\2\2\2\u068b"+
		"\u0693\7\u0097\2\2\u068c\u0693\5~@\2\u068d\u0693\7\u009a\2\2\u068e\u068f"+
		"\7\5\2\2\u068f\u0690\5\u00a6T\2\u0690\u0691\7\6\2\2\u0691\u0693\3\2\2"+
		"\2\u0692\u068b\3\2\2\2\u0692\u068c\3\2\2\2\u0692\u068d\3\2\2\2\u0692\u068e"+
		"\3\2\2\2\u0693\u00a7\3\2\2\2\u00ef\u00aa\u00ac\u00b7\u00be\u00c3\u00c9"+
		"\u00cf\u00d1\u00f1\u00f8\u0100\u0103\u010c\u0110\u0118\u011c\u011e\u0123"+
		"\u0125\u0128\u012d\u0131\u0136\u013f\u0142\u0148\u014a\u014e\u0154\u0159"+
		"\u0164\u016a\u016e\u0174\u0179\u0182\u0189\u018f\u0193\u0197\u019d\u01a2"+
		"\u01a9\u01b4\u01b7\u01b9\u01bf\u01c5\u01c9\u01d0\u01d6\u01dc\u01e2\u01e7"+
		"\u01f3\u01f8\u0203\u0208\u020b\u0212\u0215\u021c\u0225\u0228\u022e\u0230"+
		"\u0234\u023c\u0241\u0249\u024e\u0256\u025b\u0263\u0268\u026d\u0275\u027f"+
		"\u0282\u0288\u028a\u028d\u02a0\u02a6\u02af\u02b4\u02bd\u02c8\u02cf\u02d5"+
		"\u02db\u02e4\u02eb\u02ef\u02f1\u02f5\u02fc\u02fe\u0302\u0305\u030b\u0315"+
		"\u0318\u031e\u0320\u0323\u032b\u0335\u0338\u033e\u0340\u0344\u034b\u0354"+
		"\u0358\u035a\u035e\u0367\u036c\u036e\u0377\u0382\u0389\u038c\u038f\u039c"+
		"\u03aa\u03af\u03b2\u03bf\u03cd\u03d2\u03db\u03de\u03e4\u03e6\u03ec\u03f1"+
		"\u03f7\u0403\u0407\u040c\u0410\u0413\u0425\u042a\u042f\u0437\u043c\u0445"+
		"\u044c\u0450\u0460\u0463\u046b\u0474\u0478\u047d\u049a\u049f\u04a8\u04b2"+
		"\u04b5\u04bb\u04be\u04c5\u04cb\u04d2\u04d4\u04d6\u04e1\u04e6\u04f2\u04f6"+
		"\u04fa\u04fe\u0505\u0507\u050f\u0516\u0519\u051d\u0522\u052a\u053d\u0543"+
		"\u0547\u054e\u0554\u055c\u0561\u0564\u0569\u0572\u0577\u0585\u0588\u058a"+
		"\u058f\u0593\u0596\u059d\u05a2\u05ab\u05ae\u05b2\u05b5\u05bd\u05c1\u05c9"+
		"\u05cc\u05ce\u05d7\u05dc\u05e0\u05e4\u05e7\u05f2\u05f7\u05fb\u0602\u060b"+
		"\u060f\u0611\u0615\u061e\u0623\u0625\u062e\u0639\u0640\u0643\u064a\u064d"+
		"\u0659\u0687\u0692";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}