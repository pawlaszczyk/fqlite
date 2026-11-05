package fqlite.forensics;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Explains a {@code CREATE TRIGGER} statement in plain English and points out what it means
 * for an examination (automatic deletions, history tables, blocked changes). Rule-based; the
 * statement need not be complete (carved schema entries), unknown parts are quoted.
 */
public final class TriggerExplainer {

	private TriggerExplainer() {
	}

	public record Explanation(String name, String table, String timing, String event, String condition, List<String> actions,
			List<String> notes, String text) {
	}

	private static final String ID = "(\"(?:[^\"]|\"\")+\"|`[^`]+`|\\[[^\\]]+\\]|'(?:[^']|'')+'|[\\w$.]+)";
	private static final Pattern HEAD = Pattern.compile("(?is)^\\s*CREATE\\s+(?:TEMP(?:ORARY)?\\s+)?TRIGGER\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?" + ID
			+ "\\s+(BEFORE|AFTER|INSTEAD\\s+OF)?\\s*(DELETE|INSERT|UPDATE(?:\\s+OF\\s+(.+?))?)\\s+ON\\s+" + ID
			+ "(?:\\s+FOR\\s+EACH\\s+ROW)?\\s*(?:WHEN\\s+(.+?))?\\s*BEGIN\\b(.*)$");

	public static Explanation explain(String sql) {
		if (sql == null)
			sql = "";
		String s = sql.strip();
		Matcher m = HEAD.matcher(s);
		List<String> actions = new ArrayList<>(), notes = new ArrayList<>();
		if (!m.find())
			return new Explanation("?", "?", "", "", "", actions, notes,
					"The statement could not be analysed (incomplete or unusual syntax):\n" + s);
		String name = unquote(m.group(1));
		String timing = m.group(2) == null ? "BEFORE" : m.group(2).toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
		String evt = m.group(3).toUpperCase(Locale.ROOT);
		String event = evt.startsWith("UPDATE") ? "UPDATE" : evt;
		String cols = m.group(4) == null ? null : m.group(4).strip();
		String table = unquote(m.group(5));
		String when = m.group(6) == null ? null : m.group(6).strip();
		String body = m.group(7);
		int end = lastEnd(body);
		boolean complete = end >= 0;
		if (complete)
			body = body.substring(0, end);

		StringBuilder sb = new StringBuilder();
		String what = switch (event) {
		case "DELETE" -> "a row is deleted from " + table;
		case "INSERT" -> "a row is inserted into " + table;
		default -> (cols != null ? "column " + joinOr(cols) + " of a row in " + table
				: "a row in " + table) + " is updated";
		};
		String when1 = switch (timing) {
		case "AFTER" -> "After ";
		case "INSTEAD OF" -> "Instead of executing the change when ";
		default -> "Before ";
		};
		sb.append(when1).append(what);
		if (when != null)
			sb.append(", but only if ").append(human(when));
		sb.append(", SQLite automatically does the following");
		sb.append(timing.equals("INSTEAD OF") ? " (" + table + " is a view; the change itself is not executed):" : ":");

		for (String st : statements(body)) {
			String a = action(st, table, event, timing, notes);
			if (a != null)
				actions.add(a);
		}
		for (int i = 0; i < actions.size(); i++)
			sb.append("\n  ").append(i + 1).append(". ").append(actions.get(i));
		if (actions.isEmpty())
			sb.append("\n  (no statements)");
		if (!complete)
			notes.add("The statement is incomplete (END is missing), e.g. because it was recovered from free space; "
					+ "further actions may be lost.");
		if (!notes.isEmpty()) {
			sb.append("\n\nForensic notes:");
			for (String n : dedupe(notes))
				sb.append("\n  • ").append(n);
		}
		return new Explanation(name, table, timing, event + (cols != null ? " OF " + cols : ""), when == null ? "" : when, actions,
				dedupe(notes), sb.toString());
	}

	private static List<String> dedupe(List<String> l) {
		List<String> r = new ArrayList<>();
		for (String s : l)
			if (!r.contains(s))
				r.add(s);
		return r;
	}

	private static final Pattern INSERT = Pattern.compile("(?is)^(?:INSERT|REPLACE)(?:\\s+OR\\s+(\\w+))?\\s+INTO\\s+" + ID
			+ "\\s*(?:\\(([^)]*)\\))?\\s*(VALUES\\s*\\((.*)\\)|SELECT\\s+.*|DEFAULT\\s+VALUES)\\s*$");
	private static final Pattern UPDATE = Pattern.compile("(?is)^UPDATE(?:\\s+OR\\s+(\\w+))?\\s+" + ID + "\\s+SET\\s+(.+?)(?:\\s+WHERE\\s+(.+))?$");
	private static final Pattern DELETE = Pattern.compile("(?is)^DELETE\\s+FROM\\s+" + ID + "(?:\\s+WHERE\\s+(.+))?$");
	private static final Pattern RAISE = Pattern.compile("(?is)RAISE\\s*\\(\\s*(ABORT|FAIL|ROLLBACK|IGNORE)\\s*(?:,\\s*('(?:[^']|'')*'|\"[^\"]*\"))?\\s*\\)");

	private static String action(String st, String table, String event, String timing, List<String> notes) {
		String s = st.strip();
		if (s.isEmpty())
			return null;
		Matcher m;
		if ((m = INSERT.matcher(s)).find()) {
			String t = unquote(m.group(2));
			String cols = m.group(3);
			String src = m.group(4);
			StringBuilder a = new StringBuilder("inserts a row into " + t);
			if (src.toUpperCase(Locale.ROOT).startsWith("SELECT"))
				a = new StringBuilder("inserts into " + t + (cols != null ? " (" + cols.strip() + ")" : "") + " the rows returned by "
						+ human(src));
			else if (cols != null && m.group(5) != null) {
				String[] c = splitTop(cols, ',');
				String[] v = splitTop(m.group(5), ',');
				a.append(" with ");
				for (int i = 0; i < c.length; i++) {
					if (i > 0)
						a.append(", ");
					a.append(c[i].strip()).append(" = ").append(i < v.length ? human(v[i].strip()) : "?");
				}
			} else if (m.group(5) != null)
				a.append(" with the values ").append(human(m.group(5)));
			if (m.group(1) != null)
				a.append(" (on conflict: ").append(m.group(1).toUpperCase(Locale.ROOT)).append(")");
			if (timing.equals("INSTEAD OF"))
				notes.add("Changes to the view \"" + table + "\" are redirected to \"" + t + "\".");
			else if (!t.equalsIgnoreCase(table) && (event.equals("DELETE") || event.equals("UPDATE")))
				notes.add("Old values are copied into \"" + t + "\" when rows of \"" + table + "\" are "
						+ (event.equals("DELETE") ? "deleted" : "changed") + " - check " + t
						+ " for content that no longer exists in " + table + ".");
			else if (!t.equalsIgnoreCase(table))
				notes.add("Every insert into \"" + table + "\" is logged in \"" + t + "\" - entries there may outlive the original rows.");
			return a.toString();
		}
		if ((m = UPDATE.matcher(s)).find()) {
			String t = unquote(m.group(2));
			StringBuilder a = new StringBuilder("in " + t + " sets ");
			String[] sets = splitTop(m.group(3), ',');
			for (int i = 0; i < sets.length; i++) {
				if (i > 0)
					a.append(", ");
				String[] kv = sets[i].split("=", 2);
				a.append(kv[0].strip()).append(" to ").append(kv.length > 1 ? human(kv[1].strip()) : "?");
			}
			a.append(m.group(4) != null ? " where " + human(m.group(4)) : " in all rows");
			if (m.group(1) != null)
				a.append(" (on conflict: ").append(m.group(1).toUpperCase(Locale.ROOT)).append(")");
			notes.add("Values in \"" + t + "\" are changed automatically - a changed value need not result from a user action.");
			if (event.equals("DELETE") && t.equalsIgnoreCase(table))
				notes.add("Possibly a \"soft delete\": rows are only marked, the content remains in the table.");
			return a.toString();
		}
		if ((m = DELETE.matcher(s)).find()) {
			String t = unquote(m.group(1));
			String a = "deletes " + (m.group(2) != null ? "the rows of " + t + " where " + human(m.group(2)) : "all rows of " + t);
			notes.add("Rows of \"" + t + "\" are deleted automatically - missing rows or deleted records in " + t
					+ " can be caused by this trigger and need not be a user's action; the deleted records may still be "
					+ "recoverable from free space, the WAL or the journal.");
			if (m.group(2) == null)
				notes.add("The trigger empties \"" + t + "\" completely each time it fires.");
			return a;
		}
		if ((m = RAISE.matcher(s)).find()) {
			String kind = m.group(1).toUpperCase(Locale.ROOT);
			String msg = m.group(2) == null ? "" : " with the message " + m.group(2);
			String cond = "";
			Matcher w = Pattern.compile("(?is)\\bWHERE\\s+(.+)$").matcher(s);
			if (w.find())
				cond = " if " + human(w.group(1));
			String what = switch (kind) {
			case "IGNORE" -> "silently skips the triggering change";
			case "ROLLBACK" -> "rolls back the whole transaction";
			case "FAIL" -> "stops the statement (changes made so far remain)";
			default -> "rejects the change (the statement is undone)";
			};
			notes.add("The trigger can block changes - attempts to " + event.toLowerCase(Locale.ROOT)
					+ " rows of \"" + table + "\" may have failed without leaving data.");
			return what + msg + cond;
		}
		if (s.toUpperCase(Locale.ROOT).startsWith("SELECT"))
			return "evaluates " + human(s) + " (typically to call a function)";
		return "executes: " + s;
	}

	/** NEW.x / OLD.x in words */
	static String human(String expr) {
		String e = expr.strip().replaceAll("\\s+", " ");
		e = e.replaceAll("(?i)\\bNEW\\.(\"[^\"]+\"|`[^`]+`|\\[[^\\]]+\\]|\\w+)", "the new $1");
		e = e.replaceAll("(?i)\\bOLD\\.(\"[^\"]+\"|`[^`]+`|\\[[^\\]]+\\]|\\w+)", "the old $1");
		e = e.replaceAll("(?i)\\bIS NOT NULL\\b", "is set").replaceAll("(?i)\\bIS NULL\\b", "is empty (NULL)");
		e = e.replaceAll("(?i)\\b(datetime|date|time)\\('now'(?:\\s*,\\s*'localtime')?\\)", "the current $1");
		e = e.replaceAll("(?i)\\bstrftime\\('%s'\\s*,\\s*'now'\\)", "the current Unix time");
		e = e.replace("<>", "≠").replace("!=", "≠").replace(">=", "≥").replace("<=", "≤");
		if (e.length() > 300)
			e = e.substring(0, 300) + "…";
		return e;
	}

	private static String joinOr(String cols) {
		String[] c = splitTop(cols, ',');
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < c.length; i++) {
			if (i > 0)
				sb.append(i == c.length - 1 ? " or " : ", ");
			sb.append(unquote(c[i].strip()));
		}
		return sb.toString();
	}

	static String unquote(String s) {
		if (s == null)
			return "";
		s = s.strip();
		if (s.length() >= 2) {
			char a = s.charAt(0), b = s.charAt(s.length() - 1);
			if ((a == '"' && b == '"') || (a == '`' && b == '`') || (a == '[' && b == ']') || (a == '\'' && b == '\''))
				return s.substring(1, s.length() - 1).replace("\"\"", "\"").replace("''", "'");
		}
		return s;
	}

	/** index of the END keyword that closes the trigger body (ignores CASE ... END) */
	private static int lastEnd(String body) {
		int depth = 0, last = -1;
		Matcher m = Pattern.compile("(?i)'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|\\bCASE\\b|\\bEND\\b").matcher(body);
		while (m.find()) {
			String t = m.group().toUpperCase(Locale.ROOT);
			if (t.equals("CASE"))
				depth++;
			else if (t.equals("END")) {
				if (depth > 0)
					depth--;
				else
					last = m.start();
			}
		}
		return last;
	}

	/** splits at {@code sep} outside quotes and parentheses */
	static String[] splitTop(String s, char sep) {
		List<String> r = new ArrayList<>();
		int depth = 0, start = 0;
		char q = 0;
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (q != 0) {
				if (c == q)
					q = 0;
			} else if (c == '\'' || c == '"' || c == '`')
				q = c;
			else if (c == '[')
				q = ']';
			else if (c == '(')
				depth++;
			else if (c == ')')
				depth--;
			else if (c == sep && depth == 0) {
				r.add(s.substring(start, i));
				start = i + 1;
			}
		}
		r.add(s.substring(start));
		return r.toArray(new String[0]);
	}

	private static List<String> statements(String body) {
		List<String> r = new ArrayList<>();
		for (String st : splitTop(body, ';'))
			if (!st.isBlank())
				r.add(st.strip());
		return r;
	}
}
