package fqlite.timemap;

import java.util.HashMap;
import java.util.Map;

/**
 * Static lookup table mapping MCC+MNC pairs to operator names and countries.
 *
 * <p>Contains a representative subset focussed on German-speaking markets
 * (DE/AT/CH) plus the most common international operators. For a complete list
 * the application can load the full ITU E.212 data or use the OpenCelliD
 * CSV export which already includes operator names.</p>
 */
public final class MncLookup {

    private MncLookup() {}

    // ── MCC → ISO country code ────────────────────────────────────────────────
    private static final Map<Integer, String> MCC_COUNTRY = new HashMap<>();

    // ── MCC*1000+MNC → operator name ─────────────────────────────────────────
    private static final Map<Integer, String> MCC_MNC_OPERATOR = new HashMap<>();

    static {
        // Germany (262)
        MCC_COUNTRY.put(262, "DE");
        add(262,  1, "Telekom Deutschland");
        add(262,  2, "Vodafone DE");
        add(262,  3, "Telekom Deutschland");  // E-Plus (merged)
        add(262,  4, "Vodafone DE");
        add(262,  5, "E-Plus");
        add(262,  6, "T-Mobile DE");
        add(262,  7, "O2 DE");
        add(262,  8, "O2 DE");
        add(262,  9, "Vodafone DE");
        add(262, 10, "Deutsche Telekom");
        add(262, 11, "O2 DE");
        add(262, 12, "Dolphin Telecom");
        add(262, 13, "Mobilcom");
        add(262, 14, "Group 3G UMTS");
        add(262, 15, "Airdata");
        add(262, 16, "Telogic");
        add(262, 17, "E-Plus");
        add(262, 20, "Shared");
        add(262, 43, "Lycamobile DE");
        add(262, 77, "E-Plus");

        // Austria (232)
        MCC_COUNTRY.put(232, "AT");
        add(232,  1, "A1 Telekom Austria");
        add(232,  3, "Magenta AT");
        add(232,  5, "Hutchison 3G Austria");
        add(232,  6, "Orange AT");
        add(232,  7, "T-Mobile AT");
        add(232, 10, "3 AT");
        add(232, 11, "A1 AT");
        add(232, 12, "Yesss!");
        add(232, 15, "Tele2 AT");

        // Switzerland (228)
        MCC_COUNTRY.put(228, "CH");
        add(228,  1, "Swisscom");
        add(228,  2, "Sunrise");
        add(228,  3, "Salt Mobile");
        add(228,  7, "IN&Phone");
        add(228, 54, "Lycamobile CH");

        // UK (234/235)
        MCC_COUNTRY.put(234, "GB");
        MCC_COUNTRY.put(235, "GB");
        add(234, 10, "O2 UK");
        add(234, 20, "3 UK");
        add(234, 30, "EE");
        add(234, 15, "Vodafone UK");
        add(235, 30, "EE UK");

        // France (208)
        MCC_COUNTRY.put(208, "FR");
        add(208,  1, "Orange FR");
        add(208, 10, "SFR FR");
        add(208, 20, "Bouygues");
        add(208, 15, "Free Mobile");

        // Netherlands (204)
        MCC_COUNTRY.put(204, "NL");
        add(204,  4, "Vodafone NL");
        add(204, 16, "T-Mobile NL");
        add(204, 20, "T-Mobile NL");

        // USA (310–316)
        for (int m = 310; m <= 316; m++) MCC_COUNTRY.put(m, "US");
        add(310, 260, "T-Mobile US");
        add(310, 410, "AT&T");
        add(311, 480, "Verizon");
        add(316,  10, "T-Mobile US");

        // China (460)
        MCC_COUNTRY.put(460, "CN");
        add(460,  0, "China Mobile");
        add(460,  1, "China Unicom");
        add(460,  3, "China Telecom");

        // Poland (260)
        MCC_COUNTRY.put(260, "PL");
        add(260,  1, "Plus PL");
        add(260,  2, "T-Mobile PL");
        add(260,  3, "Orange PL");
        add(260,  6, "Play PL");

        // Belgium (206)
        MCC_COUNTRY.put(206, "BE");
        add(206,  1, "Proximus");
        add(206, 10, "Orange BE");
        add(206, 20, "Base BE");
    }

    private static void add(int mcc, int mnc, String name) {
        MCC_MNC_OPERATOR.put(mcc * 1000 + mnc, name);
    }

    /**
     * Returns the operator name for the given MCC+MNC, or {@code null}.
     */
    public static String getOperator(int mcc, int mnc) {
        return MCC_MNC_OPERATOR.get(mcc * 1000 + mnc);
    }

    /**
     * Returns the ISO 3166-1 alpha-2 country code for the given MCC, or {@code null}.
     */
    public static String getCountry(int mcc) {
        return MCC_COUNTRY.get(mcc);
    }
}
