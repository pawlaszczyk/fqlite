package fqlite.timemap;

import fqlite.rag.RAGPipeline;
import fqlite.sql.DBManager;
import fqlite.sql.InMemoryDatabase;
import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Scene;
import javafx.stage.Stage;
import javafx.stage.Screen;


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

    /**
     * Extended constructor that also wires an LLM pipeline to the map view.
     *
     * <p>Pass the same {@link RAGPipeline} instance that is used by
     * {@link fqlite.rag.LLMWindow} so the schemas are already loaded.
     * The {@link InMemoryDatabase} must match the database shown in the map.</p>
     *
     * @param tblname  name of the table to analyse
     * @param rows     table row data set
     * @param header   column names
     * @param pipeline an initialised {@link RAGPipeline} (may be {@code null}
     *                 to skip LLM integration)
     * @param mdb      the backing in-memory DB (may be {@code null})
     */
    public LocationWindow(String tblname,
                          ObservableList<ObservableList<String>> rows,
                          List<String> header,
                          RAGPipeline pipeline,
                          InMemoryDatabase mdb) {
        this.headers.put(tblname, header);
        data.put(tblname, rows);
        this.ragPipeline = pipeline;
        this.inMemoryDB  = mdb;
    }

    // Optional LLM components – null when not configured
    private RAGPipeline    ragPipeline = null;
    private InMemoryDatabase inMemoryDB = null;

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
        // DataAnalyzer resolves field positions by matching these names
        // against the raw row (offset by its fqlite meta-field prefix),
        // so this list must name every response_records column correctly,
        // in the table's real order — see DataAnalyzer#colIndex.
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
        livePane = analyzerPane;   // ermöglicht live-attach via setRagPipeline()
        activePane = analyzerPane; // ermöglicht Zugriff von außen, siehe getActivePane()

        // Use setBaseData so the "↺ Reset" button in the LLM bar can restore
        // the full original dataset after an LLM-filtered view.
        analyzerPane.setBaseData(data, headers);

        // Wire LLM if a pipeline was supplied (either via the extended
        // constructor or set after construction via setRagPipeline()).
        if (ragPipeline != null) {
            analyzerPane.attachLLM(ragPipeline, inMemoryDB);
        }

        Scene scene = new Scene(analyzerPane, Screen.getPrimary().getVisualBounds().getWidth() * 0.8, Screen.getPrimary().getVisualBounds().getHeight() * 0.8);
        primaryStage.setTitle("FQLite – MapView");
        primaryStage.setScene(scene);
        primaryStage.setOnCloseRequest(e -> {
            livePane = null;    // aufräumen
            if (activePane == analyzerPane) activePane = null;
        });
        primaryStage.show();
    }

    /**
     * Wires the LLM after construction — safe to call from any thread,
     * including a background loader thread.
     *
     * <p>If {@link #start} has already been called and the {@link MapViewPane}
     * is visible, {@link MapViewPane#attachLLM} is invoked immediately on the
     * FX thread so the status label and prompt field update live.  If the window
     * is not yet open the pipeline is stored and wired inside {@link #start}.</p>
     *
     * @param pipeline an initialised pipeline
     * @param mdb      the backing in-memory database
     */
    public void setRagPipeline(RAGPipeline pipeline, InMemoryDatabase mdb) {
        this.ragPipeline = pipeline;
        this.inMemoryDB  = mdb;
        // Falls das Fenster bereits offen ist, sofort einhängen
        if (livePane != null) {
            javafx.application.Platform.runLater(() -> livePane.attachLLM(pipeline, mdb));
        }
    }

    /** Reference to the pane once the window has been opened. */
    private MapViewPane livePane = null;

    /**
     * The {@link MapViewPane} of whichever {@link LocationWindow} instance
     * was opened most recently and hasn't been closed yet, or {@code null}
     * if no map/timeline window is currently open.
     *
     * <p>Lets {@link fqlite.rag.LLMWindow}'s chat assistant push a query
     * result onto an already-open map/timeline view (see
     * {@link MapViewPane#runQueryAndDisplay}) in addition to showing it in
     * the chat's own SQL-Analyzer result list — without the chat window
     * needing a direct reference to the map window it didn't create.
     * Only one map window is tracked at a time, matching this class's
     * existing single-instance assumption (see {@link #livePane}).</p>
     */
    private static MapViewPane activePane = null;

    /** @return the currently open map/timeline pane, or {@code null} if none is open. */
    public static MapViewPane getActivePane() { return activePane; }

    public static void main(String[] args) {
        launch(args);
    }
}
