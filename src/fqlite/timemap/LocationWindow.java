package fqlite.timemap;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Scene;
import javafx.stage.Stage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stand-alone demo application that populates {@link MapViewPane} with
 * synthetic data covering both the fixed {@code response_records} schema and
 * the legacy heuristic-detected tables.
 *
 * <p>Run this class to verify the UI without a real SQLite database.
 * The built-in theme toggle button (top-right) lets you switch between
 * dark and light mode at runtime.</p>
 */
public class LocationWindow extends Application {

    ObservableList<ObservableList<String>> rows = FXCollections.observableArrayList();
    Map<String, List<String>> headers = new HashMap<>();
    ConcurrentHashMap<String, ObservableList<ObservableList<String>>> data =
            new ConcurrentHashMap<>();

    /**
     * Constructor used by the main application when passing real table data.
     *
     * @param tblname name of the table to analyse
     * @param rows    table row data set
     * @param header  column names
     */
    public LocationWindow(String tblname,
                          ObservableList<ObservableList<String>> rows,
                          List<String> header) {
        this.headers.put(tblname, header);
        data.put(tblname, rows);
    }

    /** No-arg constructor required by {@link Application#launch}. */
    public LocationWindow() {}

    // -------------------------------------------------------------------------
    // Sample data
    // -------------------------------------------------------------------------

    /**
     * Populates {@link #data} and {@link #headers} with synthetic rows that
     * exactly match the fqlite serialisation format observed in real data.
     *
     * <p>Each row has <b>40 semicolon-delimited fields</b>:</p>
     * <ul>
     *   <li>Fields 0–2: fqlite meta (row-counter, table-name, record-ref)</li>
     *   <li>Fields 3–9: id … nw_access_type</li>
     *   <li>Fields 10–12: three undocumented extra fields
     *       (carrier_country_code, carrier_header_id, connection_type)</li>
     *   <li>Fields 13–14: start_time, end_time</li>
     *   <li>Fields 15–28: duration_seconds … apn, followed by 5 "Key"="Value" pairs
     *       for other_information</li>
     *   <li>Fields 29–36: call_indicator…type_of_data (plain repeats + nat columns)</li>
     * </ul>
     */
    private void buildSampleData() {

        // The column header list matches the CREATE TABLE definition.
        // It is used only for the detail-panel display labels; extraction
        // uses fixed positional indices (RR_IDX_* in DataAnalyzer).
        List<String> rrHeader = List.of(
                "id", "request_id", "record_number",
                "party_type",
                "national_country_code", "national_header_id",
                "nw_access_type",
                "start_time", "end_time", "duration_seconds",
                "na_device_id",
                "lt_raw", "lo_raw", "latitude", "longitude", "map_datum", "azimuth",
                "user_location_info",
                "imsi", "msisdn", "apn",
                "other_information",
                "call_indicator", "call_action_code", "call_subtype",
                "session_id", "type_of_data_extra",
                "nat_country_code", "nat_header_id", "type_of_data"
        );
        headers.put("response_records", rrHeader);

        // Each String[] represents one semicolon-split row (40 elements).
        // Format mirrors the real fqlite export exactly.
        ObservableList<ObservableList<String>> rrRows = FXCollections.observableArrayList();
        for (String[] r : new String[][] {
            // meta[0]  meta[1]              meta[2]     id   req_id  rec_nr  party  ncc  nhid  nw_access_type                 xtra_cc  xtra_hdr             xtra_conn         start_time             end_time               dur    device_id          lt_raw    lo_raw    latitude     longitude    datum   az  user_loc_info              imsi               msisdn           apn                       other1                        other2                         other3                   other4                                      other5                    ci    ca  cs  session                  type_x  natCC  natHdr               typeData
            {"5","response_records","[444|31]","5"," ","47048","5","1","5","Carrier ID: 20404. GPRS","DE","01.28.01.08.1.01","mobilePacketData","2024-09-27 00:06:32","2024-09-27 01:06:32","3600","35628610088612","N515715","E0144130","51.95416700","14.69166700","wGS84","30","560008011062f2200077fc01","02047401799929f7","882397109799927","open.internet.de.ott","\"Call Indikator\"=\"SGP\"","\"Call Action Code\"=\"2\"","\"Call Subtype\"=\"I\"","\"Session Id\"=\"44c04a19_3e8c80ab\"","\"TypeOfData\"=\"tKG96\"","SGP","2","I","44c04a19_3e8c80ab","tKG96","DE","01.28.01.08.1.01","betrieblicheVerkehrsdaten"},
            {"6","response_records","[417|31]","6"," ","46628","6","1","6","GPRS","DE","01.28.01.08.1.01","mobilePacketData","2024-09-27 00:06:51","2024-09-27 01:06:51","3600","35685911523758","N515715","E0144130","51.95416700","14.69166700","wGS84","30","560008011062f2200077fc01","62022208269448f7","491729752628","web.vodafone.de","\"Call Indikator\"=\"SGP\"","\"Call Action Code\"=\"2\"","\"Call Subtype\"=\"I\"","\"Session Id\"=\"0496ab2e_8b0784a8\"","\"TypeOfData\"=\"tKG96\"","SGP","2","I","0496ab2e_8b0784a8","tKG96","DE","01.28.01.08.1.01","betrieblicheVerkehrsdaten"},
            // Row without coordinates to verify timestamp-only handling:
            {"7","response_records","[418|31]","7"," ","46629","6","1","6","GSM","DE","01.28.01.08.1.01","mobileVoice","2024-09-27 08:15:00","2024-09-27 08:17:30","150","35685911523759","","","","","","","","62022208269448f8","491729752629","","\"Call Indikator\"=\"MOC\"","\"Call Action Code\"=\"1\"","\"Call Subtype\"=\"V\"","\"Session Id\"=\"aabbccdd_11223344\"","\"TypeOfData\"=\"tKG10\"","MOC","1","V","aabbccdd_11223344","tKG10","DE","01.28.01.08.1.01","betrieblicheVerkehrsdaten"},
        }) rrRows.add(FXCollections.observableArrayList(r));
        data.put("response_records", rrRows);
    }

    // -------------------------------------------------------------------------
    // JavaFX entry point
    // -------------------------------------------------------------------------

    @Override
    public void start(Stage primaryStage) {
        // If no data was injected via the constructor, use synthetic samples.
        if (data.isEmpty()) buildSampleData();

        MapViewPane analyzerPane = new MapViewPane();
        analyzerPane.setData(data, headers);

        Scene scene = new Scene(analyzerPane, 1280, 780);
        primaryStage.setTitle("FQLite – MapView");
        primaryStage.setScene(scene);
        primaryStage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }
}
