package fqlite.timemap;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Optional online lookup of a phone's TAC (Type Allocation Code — the first
 * 8 digits of an IMEI) against the HiCellTek IMEI/TAC API, to answer
 * "Um was für ein Gerät handelt es sich bei der IMEI x?" with a manufacturer/
 * model guess when the local dataset alone can't tell.
 *
 * <p><b>Status: code stub, not a verified integration.</b> HiCellTek's full
 * API reference page ({@code https://hicelltek.com/en/api-documentation/})
 * renders via JavaScript and could not be fetched as plain text while this
 * class was written — the endpoint URL, header name, request body shape and
 * response field names below come from third-party search-result summaries
 * of that page, not from the primary documentation itself. Treat every
 * detail here (the endpoint path, the {@code X-Api-Key} header, the
 * {@code "tac"} request field, and the {@code brand}/{@code model}/
 * {@code chipsetFamily} response fields) as a best-effort guess that may
 * need correcting once someone has an actual API key and can compare a real
 * response against this code.</p>
 *
 * <p>{@link TacLocalDatabase} (Settings → Location → "Lokale TAC-
 * Datenbank") is always tried first when configured — no network call,
 * no dependence on the unverified HiCellTek contract below. The online API
 * is only used as a fallback when the local file has no entry for a TAC.</p>
 *
 * <p>Off by default — must be explicitly enabled (Settings → Location →
 * "IMEI/TAC-Gerätelookup") via {@link #setEnabled(boolean)}, mirroring the
 * beaconDB online-fallback toggle, since this also sends data (the TAC) to
 * a third party. Requires an API key, configurable either in that same
 * Settings panel via {@link #setApiKey(String)} or via the system property
 * {@code fqlite.hicelltek.apikey} (e.g. {@code -Dfqlite.hicelltek.apikey=...}
 * on the JVM command line) as a fallback. Without an enabled flag and a key,
 * {@link #lookup(String)} returns a {@link Result} with {@link Result#error}
 * set and makes no network call. No key is bundled with this project — the
 * analyst must obtain their own from HiCellTek and accepts that queries
 * leave the local machine when this feature is used.</p>
 */
public final class TacLookupService {

    private static final Logger LOG = Logger.getLogger(TacLookupService.class.getName());

    private static final String API_KEY_PROPERTY = "fqlite.hicelltek.apikey";
    private static final String ENDPOINT = "https://imei.hicelltek.com/api/v1/tac/lookup";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    /** Off by default — see class Javadoc. Set from SettingsDialog/startup config. */
    private static volatile boolean enabled = false;

    /** API key configured via SettingsDialog; falls back to {@link #API_KEY_PROPERTY} if unset. */
    private static volatile String apiKey;

    public static void setEnabled(boolean value) {
        enabled = value;
        LOG.info("TacLookupService (IMEI/TAC Online-Gerätelookup): " + (value ? "ENABLED" : "DISABLED"));
    }

    public static boolean isEnabled() { return enabled; }

    /** Sets the HiCellTek API key (overrides the {@code fqlite.hicelltek.apikey} system property). */
    public static void setApiKey(String key) { apiKey = (key == null || key.isBlank()) ? null : key.trim(); }

    public static String getApiKey() { return apiKey; }

    private TacLookupService() { }

    /** Outcome of a TAC lookup — either populated brand/model/chipset fields, or {@link #error} explaining why not. */
    public static final class Result {
        public final String tac;
        public String brand;
        public String model;
        public String chipsetFamily;
        public String rawResponse;
        public String error;
        /** "lokal" or "online" — which path produced {@link #brand}/{@link #model}; {@code null} on error. */
        public String source;

        Result(String tac) {
            this.tac = tac;
        }

        public boolean hasDeviceInfo() {
            return error == null && (brand != null || model != null || chipsetFamily != null);
        }
    }

    /**
     * Looks up device info for the given IMEI or bare TAC. Only the first 8
     * digits (the TAC) are sent to the API — the remaining IMEI digits
     * (serial number + check digit) identify the individual handset, not the
     * model, and are neither needed for nor sent to a manufacturer/model
     * lookup.
     *
     * @param imeiOrTac a full IMEI or just its 8-digit TAC prefix; any
     *                  non-digit characters are stripped first
     */
    public static Result lookup(String imeiOrTac) {
        String digits = imeiOrTac == null ? "" : imeiOrTac.replaceAll("[^0-9]", "");
        if (digits.length() < 8) {
            Result r = new Result(digits);
            r.error = "IMEI/TAC zu kurz (mindestens 8 Ziffern nötig): '" + imeiOrTac + "'";
            return r;
        }
        String tac = digits.substring(0, 8);
        Result result = new Result(tac);

        // Local TAC database (Einstellungen → Location → "Lokale TAC-
        // Datenbank") takes priority over the online API: no network call,
        // no unverified-endpoint risk, and it's what the analyst explicitly
        // asked to use instead of HiCellTek once they had a file for it.
        boolean localConfigured = TacLocalDatabase.getInstance().isConfigured();
        if (localConfigured) {
            TacLocalDatabase.Entry localHit = TacLocalDatabase.getInstance().lookup(tac);
            if (localHit != null) {
                result.brand = localHit.brand;
                result.model = localHit.specs;
                result.source = "lokal";
                return result;
            }
        }

        if (!enabled) {
            result.error = (localConfigured
                    ? "TAC " + tac + " nicht in der lokalen TAC-Datenbank gefunden. "
                    : "")
                    + "Online-Gerätelookup (HiCellTek) ist deaktiviert (Einstellungen → Location) "
                    + "— es wird nur der lokale Datensatzbefund angezeigt.";
            return result;
        }

        String key = apiKey != null ? apiKey : System.getProperty(API_KEY_PROPERTY);
        if (key == null || key.isBlank()) {
            result.error = (localConfigured
                    ? "TAC " + tac + " nicht in der lokalen TAC-Datenbank gefunden. "
                    : "")
                    + "Kein API-Key konfiguriert (Einstellungen → Location, oder System-Property '"
                    + API_KEY_PROPERTY + "') — es wird nur der lokale Datensatzbefund angezeigt.";
            return result;
        }

        try {
            HttpClient client = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
            String body = "{\"tac\":\"" + tac + "\"}";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(ENDPOINT))
                    .timeout(TIMEOUT)
                    .header("Content-Type", "application/json")
                    .header("X-Api-Key", key)
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            result.rawResponse = response.body();

            if (response.statusCode() != 200) {
                result.error = "HiCellTek-API antwortete mit Status " + response.statusCode()
                        + (result.rawResponse != null ? (": " + truncate(result.rawResponse, 300)) : "");
                return result;
            }

            result.brand = extractJsonField(result.rawResponse, "brand");
            result.model = extractJsonField(result.rawResponse, "model");
            result.chipsetFamily = extractJsonField(result.rawResponse, "chipsetFamily");

            if (!result.hasDeviceInfo()) {
                result.error = "Antwort der HiCellTek-API enthielt keines der erwarteten Felder "
                        + "(brand/model/chipsetFamily) — Rohantwort: " + truncate(result.rawResponse, 300);
            } else {
                result.source = "online";
            }
        } catch (IOException | InterruptedException ex) {
            result.error = "Netzwerkfehler beim Abruf der HiCellTek-API: " + ex.getMessage();
            LOG.warning("TacLookupService: lookup failed for TAC " + tac + ": " + ex.getMessage());
        } catch (RuntimeException ex) {
            result.error = "Unerwarteter Fehler beim Abruf der HiCellTek-API: " + ex.getMessage();
            LOG.warning("TacLookupService: lookup failed for TAC " + tac + ": " + ex.getMessage());
        }
        return result;
    }

    /**
     * Hand-written regex extraction of a flat string field from a JSON
     * response. The project has no JSON library (the same regex-based
     * approach is used elsewhere for parsing LLM responses), and the exact
     * response shape here is unverified (see class Javadoc), so this
     * deliberately tolerates the field appearing anywhere in the response
     * rather than assuming a specific nesting/object structure.
     */
    private static String extractJsonField(String json, String field) {
        if (json == null) return null;
        Matcher m = Pattern.compile("\"" + Pattern.quote(field) + "\"\\s*:\\s*\"([^\"]*)\"").matcher(json);
        return m.find() ? m.group(1) : null;
    }

    private static String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max) + "…";
    }
}
