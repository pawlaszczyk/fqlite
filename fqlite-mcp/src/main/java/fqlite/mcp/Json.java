package fqlite.mcp;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 *  This is a small helper class around Gson.
 *
 *  @author pawlaszc
 *
 */
final class Json {

	static final Gson GSON = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();
	static final Gson PRETTY = new GsonBuilder().disableHtmlEscaping().serializeNulls().setPrettyPrinting().create();

	private Json() {
	}

	static JsonObject obj(Object... kv) {
		JsonObject o = new JsonObject();
		for (int i = 0; i + 1 < kv.length; i += 2)
			o.add((String) kv[i], el(kv[i + 1]));
		return o;
	}

	static JsonArray arr(Iterable<?> items) {
		JsonArray a = new JsonArray();
		for (Object x : items)
			a.add(el(x));
		return a;
	}

	static JsonElement el(Object v) {
		if (v instanceof JsonElement e)
			return e;
		return GSON.toJsonTree(v);
	}

	static String str(JsonObject o, String key, String def) {
		return o != null && o.has(key) && !o.get(key).isJsonNull() ? o.get(key).getAsString() : def;
	}

	static int integer(JsonObject o, String key, int def) {
		return o != null && o.has(key) && !o.get(key).isJsonNull() ? o.get(key).getAsInt() : def;
	}

	static Boolean bool(JsonObject o, String key, Boolean def) {
		return o != null && o.has(key) && !o.get(key).isJsonNull() ? o.get(key).getAsBoolean() : def;
	}
}
