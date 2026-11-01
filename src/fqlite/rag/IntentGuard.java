package fqlite.rag;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Deterministische Absicherung der LLM-Intent-Klassifikation (Router) in
 * {@link RAGPipeline#classifyIntent}. Java-Port von {@code intent_guard.py}
 * (SQLiteForensicsModel, dort Paritaetstest und Messung mit test_classify_intent.py).
 *
 * <p>Hintergrund: Der Router entscheidet, ob eine Anfrage ueber eine feste
 * SQL-Vorlage oder ueber den freien SQL-Generator beantwortet wird. Das kleine
 * Modell verwechselt dabei typische Muster (Filter → Kolokation, Auswertungsfragen
 * → Anruf-/SMS-Vorlage, Wechsler-Anfragen ohne gueltiges JSON). Die Regeln
 * greifen nur auf eindeutige Schluesselwoerter zurueck; im Zweifel bleibt die
 * Entscheidung des Modells bestehen.</p>
 *
 * <p>Regeln (Reihenfolge = Prioritaet):</p>
 * <ol>
 *   <li>R0_PARSE     call_sms_type-Wert im Feld intent → calls_sms; unbekannter intent → filter</li>
 *   <li>R1_ANALYTIC  Auswertungsfrage (wie oft/lange/viele, insgesamt, mit wem, am haeufigsten,
 *                    ausschliesslich, nie …)
 *                    → filter + Zeitfenster leeren (erzwingt freien SQL-Generator; keine Vorlage
 *                    kann zaehlen, summieren oder Partner ermitteln)</li>
 *   <li>R2_WECHSLER  Geraete-/SIM-Wechsel-Stichworte → wechsler</li>
 *   <li>R3_ROAMERS   Wanderer / mehrere Funkzellen/Standorte / bewegt → roamers</li>
 *   <li>R4_COLO      intent=colocation ohne Kolokations-Stichwort (gemeinsam, gleichzeitig,
 *                    derselben, zusammen, same cell …) → filter</li>
 *   <li>R5_IDENT     konkrete Kennung (≥ 8 Ziffern) bei calls_sms oder filter, sowie colocation mit
 *                    ≥ 2 Kennungen oder Kennung + Kontakt-/Kommunikationswort → filter +
 *                    Zeitfenster leeren (die Vorlagen koennen nicht nach einer Kennung filtern;
 *                    ein reiner Zeitfilter wuerde die Kennung stillschweigend ignorieren)</li>
 *   <li>R6_CALLSMS   intent=filter (vom Modell), Anruf-/SMS-Stichwort, keine Kennung
 *                    → calls_sms mit abgeleiteter Richtung</li>
 *   <li>R8_ANYDAY    "egal an welchem Tag", "taeglich", "any day" … → filter, Zeitfenster leer,
 *                    keine Datums-Rueckfrage (wird nach R1, vor R2 geprueft)</li>
 *   <li>R7_TIME_DIR  genau eine Angabe "vor/bis HH:MM" bzw. "nach/ab HH:MM" (ohne "zwischen")
 *                    → Zeitfenster Tagesbeginn–HH:MM bzw. HH:MM–Tagesende</li>
 * </ol>
 * <p>Wechselt eine Regel den intent auf wechsler/roamers/calls_sms und fehlt ein
 * Zeitfenster, wird es aus den ISO-Daten der normalisierten Anfrage ergaenzt
 * (nur ohne Uhrzeit-Angabe: ein Datum → ganzer Tag, zwei → erster bis letzter Tag).</p>
 */
public final class IntentGuard {

    private IntentGuard() { }

    public static final class Result {
        public final String intent, startTime, endTime, callSmsType;
        public final List<String> rules;
        Result(String intent, String startTime, String endTime, String callSmsType, List<String> rules) {
            this.intent = intent; this.startTime = startTime; this.endTime = endTime;
            this.callSmsType = callSmsType; this.rules = rules;
        }
        public boolean changed() { return !rules.isEmpty(); }
    }

    private static final int F = Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
    private static final List<String> KNOWN = List.of(
            "colocation", "calls_sms", "country_filter", "roamers", "wechsler", "filter");
    private static final List<String> CALL_SMS = List.of(
            "eingehender_anruf", "ausgehender_anruf", "gesendete_sms", "empfangene_sms", "alle_anrufe", "alle_sms");

    static final Pattern ANALYTIC = Pattern.compile(
            "\\b(wie\\s+oft|wie\\s+lange|wie\\s*viele|insgesamt|gesamtdauer|anzahl|mit\\s+wem|am\\s+meisten"
            + "|am\\s+h[äa]ufigsten|h[äa]ufigste\\w*|meisten|sowohl|how\\s+many|how\\s+often|how\\s+long"
            + "|in\\s+total|with\\s+whom|the\\s+most|most\\s+frequent\\w*"
            + "|ausschlie(ss|ß)lich\\w*|exklusiv\\w*|exclusively|nie|niemals|never|nicht\\s+mehr"
            + "|am\\s+[öo]e?ftesten|[öo]e?fteste\\w*)\\b", F);
    static final Pattern WECHSLER = Pattern.compile(
            "\\b(wechsler\\w*|ger[äa]te-?wechsel\\w*|sim-?wechsel\\w*|sim-?karten-?wechsel\\w*"
            + "|mehrere[n]?\\s+(ger[äa]te\\w*|sim\\w*|handys?|imeis?|imsis?)"
            + "|(sim|device|phone|handset)\\s+(swap|change)\\w*"
            + "|changed\\s+(their\\s+|the\\s+|his\\s+|her\\s+)?(phone|device|sim|handset)\\w*)\\b", F);
    static final Pattern GEWECHSELT = Pattern.compile("\\bgewechselt\\b", F);
    static final Pattern WECHSEL_OBJ = Pattern.compile("\\b(sim\\w*|handy\\w*|ger[äa]t\\w*|telefon\\w*|karte\\w*|imsi\\w*|imei\\w*|smartphone\\w*)", F);
    static final Pattern ROAMERS = Pattern.compile(
            "\\b(wanderer\\w*|roam\\w*|beweg(t|en|te|ten)"
            + "|mehrere[n]?\\s+(funkzellen|standorten?|zellen|orten)|multiple\\s+(cells|locations|sites))\\b", F);
    static final Pattern COLO = Pattern.compile(
            "\\b(gemeinsam\\w*|gleichzeitig\\w*|zusammen|derselben|dieselbe[n]?|selben|zur\\s+selben\\s+zeit"
            + "|getroffen|treffen|kolokation\\w*|co-?locat\\w*|same\\s+(cell|location|place|site|time)"
            + "|together|simultaneous\\w*)\\b", F);
    static final Pattern IDENT = Pattern.compile("\\b\\d{8,}\\b");
    static final Pattern ANYDAY = Pattern.compile(
            "\\b(egal\\s+(an\\s+)?welche[mn]?\\s+(tag|datum)|egal\\s+wann|an\\s+(jedem|allen|beliebigen)\\s+tag\\w*"
            + "|jeden\\s+tag|t[äa]glich\\w*|unabh[äa]ngig\\s+vo[mn]\\s+(tag|datum)|tagesunabh[äa]ngig\\w*|ohne\\s+datum"
            + "|(on\\s+)?any\\s+(day|date)|every\\s+day|daily|regardless\\s+of\\s+(the\\s+)?(day|date))\\b", F);
    static final Pattern CONTACT = Pattern.compile(
            "\\b(kontakt\\w*|kommuni\\w*|telefoniert\\w*|angerufen|anruf\\w*|sms|nachricht\\w*"
            + "|contacts?|communicat\\w*|called|texted|in\\s+common)\\b", F);
    static final Pattern SMS = Pattern.compile("\\b(sms|nachricht\\w*|text\\s+messages?|texts)\\b", F);
    static final Pattern CALL = Pattern.compile("\\b(anruf\\w*|telefonat\\w*|calls?|phone\\s+calls?)\\b", F);
    static final Pattern OUT = Pattern.compile(
            "\\b(ausgehend\\w*|gesendet\\w*|verschickt\\w*|versendet\\w*|versandt\\w*|get[äa]tigt\\w*|abgehend\\w*|outgoing|sent)\\b", F);
    static final Pattern IN = Pattern.compile(
            "\\b(eingehend\\w*|empfangen\\w*|erhalten\\w*|ankommend\\w*|incoming|received)\\b", F);
    static final Pattern TIME_OF_DAY = Pattern.compile(
            "\\b\\d{1,2}\\s*[:.]\\s*\\d{2}\\b|\\b\\d{1,2}\\s*uhr\\b|\\buhr\\b", F);
    static final Pattern ISO_DATE = Pattern.compile("\\b(\\d{4}-\\d{2}-\\d{2})\\b");
    /** Uhrzeit nur als HH:MM[:SS] oder "HH Uhr" (Datumsangaben mit Punkt werden so nie erfasst). */
    private static final String TIME = "(\\d{1,2})(?::(\\d{2})(?::(\\d{2}))?(?:\\s*uhr)?|\\s*uhr)\\b";
    static final Pattern BEFORE_T = Pattern.compile("\\b(vor|before|bis|until|till)\\s+" + TIME, F);
    static final Pattern AFTER_T  = Pattern.compile("\\b(nach|after|ab|since|seit)\\s+" + TIME, F);
    static final Pattern BETWEEN_W = Pattern.compile("\\b(zwischen|between|von|from)\\b", F);

    private static String hms(Matcher m) {
        int h = Integer.parseInt(m.group(2));
        int mi = m.group(3) == null ? 0 : Integer.parseInt(m.group(3));
        int se = m.group(4) == null ? 0 : Integer.parseInt(m.group(4));
        return (h > 23 || mi > 59 || se > 59) ? null : String.format("%02d:%02d:%02d", h, mi, se);
    }

    /** Eigenstaendiger Matcher auf genau diesem Treffer (damit Gruppen spaeter lesbar bleiben). */
    private static Matcher copyOf(Matcher m, Pattern p, String s) {
        Matcher c = p.matcher(s);
        c.find(m.start());
        return c;
    }

    /**
     * @param request    Originaltext der Anfrage
     * @param normalized datums-normalisierte Anfrage (ISO-Daten), darf null sein
     */
    public static Result apply(String request, String normalized, String intent,
                               String startTime, String endTime, String callSmsType) {
        List<String> rules = new ArrayList<>();
        String req = request == null ? "" : request.toLowerCase(Locale.ROOT);   // Muster sind klein geschrieben (Paritaet mit Python re.ASCII)
        String in = intent == null ? "filter" : intent.trim().toLowerCase(Locale.ROOT);
        String cst = callSmsType;
        String st = startTime, et = endTime;
        String modelIntent;

        // R0
        if (CALL_SMS.contains(in)) { cst = in; in = "calls_sms"; rules.add("R0_PARSE"); }
        else if (!KNOWN.contains(in)) { in = "filter"; rules.add("R0_PARSE"); }
        modelIntent = in;
        int nIdent = 0;
        for (Matcher mi = IDENT.matcher(req); mi.find(); ) nIdent++;
        boolean hasIdent = nIdent > 0;

        // R1
        if (ANALYTIC.matcher(req).find()) {
            if (!"filter".equals(in) || st != null || et != null) rules.add("R1_ANALYTIC");
            return new Result("filter", null, null, null, rules);
        }
        // R8: ausdruecklich "egal an welchem Tag" -> Uhrzeit gilt fuer alle Tage; freier Pfad
        // (time(start_time) ...), keine Rueckfrage nach einem konkreten Datum.
        if (ANYDAY.matcher(req).find()) {
            rules.add("R8_ANYDAY");
            return new Result("filter", null, null, null, rules);
        }
        // R2
        boolean wechsler = WECHSLER.matcher(req).find()
                || (GEWECHSELT.matcher(req).find() && WECHSEL_OBJ.matcher(req).find());
        if (wechsler) {
            if (!"wechsler".equals(in)) { in = "wechsler"; cst = null; rules.add("R2_WECHSLER"); }
        } else if (ROAMERS.matcher(req).find()) {                                   // R3
            if (!"roamers".equals(in)) { in = "roamers"; cst = null; rules.add("R3_ROAMERS"); }
        }
        // R4
        if ("colocation".equals(in) && !COLO.matcher(req).find()) { in = "filter"; rules.add("R4_COLO"); }
        // R5
        // R5 (erweitert): Kolokation mit >= 2 Kennungen oder mit Kontakt-/Kommunikationswort
        // ist eine Kommunikationsanalyse ("gemeinsame Kontakte") -> freier Pfad. Kolokation mit
        // EINER Kennung bleibt bei der Vorlage (Obermenge statt fehleranfaelligem Self-Join).
        boolean coloComm = "colocation".equals(in)
                && (nIdent >= 2 || (hasIdent && CONTACT.matcher(req).find()));
        if (coloComm || (hasIdent && ("calls_sms".equals(in) || "filter".equals(in)))) {
            if (!"filter".equals(in) || st != null || et != null) rules.add("R5_IDENT");
            return new Result("filter", null, null, null, rules);
        }
        // R6
        if ("filter".equals(in) && "filter".equals(modelIntent) && rules.isEmpty()) {
            boolean sms = SMS.matcher(req).find(), call = CALL.matcher(req).find();
            if (sms || call) {
                boolean out = OUT.matcher(req).find(), inc = IN.matcher(req).find();
                String t;
                if (sms) t = out && !inc ? "gesendete_sms" : inc && !out ? "empfangene_sms" : "alle_sms";
                else     t = out && !inc ? "ausgehender_anruf" : inc && !out ? "eingehender_anruf" : "alle_anrufe";
                in = "calls_sms"; cst = t; rules.add("R6_CALLSMS");
            }
        }
        if ("wechsler".equals(in) || "roamers".equals(in) || "colocation".equals(in) || "country_filter".equals(in)) cst = null;
        // R7: Richtung "vor/bis X Uhr" bzw. "nach/ab X Uhr" deterministisch pruefen — das
        // Modell liefert bei "vor 16:30" teils das Fenster 16:30–23:59:59 (umgekehrt).
        if (st != null && et != null && !BETWEEN_W.matcher(req).find()) {
            List<Matcher> b = new ArrayList<>(), af = new ArrayList<>();
            for (Matcher m = BEFORE_T.matcher(req); m.find(); ) b.add(copyOf(m, BEFORE_T, req));
            for (Matcher m = AFTER_T.matcher(req); m.find(); ) af.add(copyOf(m, AFTER_T, req));
            if (b.size() + af.size() == 1) {
                Matcher m = b.isEmpty() ? af.get(0) : b.get(0);
                String t = hms(m);
                List<String> ds = new ArrayList<>();
                if (normalized != null) for (Matcher dm = ISO_DATE.matcher(normalized); dm.find(); ) ds.add(dm.group(1));
                String day = ds.size() == 1 ? ds.get(0)
                        : (st.length() >= 10 && et.length() >= 10 && st.substring(0, 10).equals(et.substring(0, 10))
                           ? st.substring(0, 10) : null);
                if (t != null && day != null) {
                    String nst = !b.isEmpty() ? day + " 00:00:00" : day + " " + t;
                    String net = !b.isEmpty() ? day + " " + t     : day + " 23:59:59";
                    if (!(nst.equals(st) && net.equals(et))) { st = nst; et = net; rules.add("R7_TIME_DIR"); }
                }
            }
        }
        // Zeitfenster ergaenzen, wenn eine Regel den Intent umgestellt hat
        if (!rules.isEmpty() && (st == null || et == null)
                && ("wechsler".equals(in) || "roamers".equals(in) || "calls_sms".equals(in))
                && !TIME_OF_DAY.matcher(req).find() && normalized != null) {
            Matcher m = ISO_DATE.matcher(normalized);
            List<String> ds = new ArrayList<>();
            while (m.find()) ds.add(m.group(1));
            if (!ds.isEmpty()) {
                st = ds.get(0) + " 00:00:00";
                et = ds.get(ds.size() - 1) + " 23:59:59";
            }
        }
        return new Result(in, st, et, cst, rules);
    }
}
