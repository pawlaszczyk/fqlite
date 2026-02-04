package fqlite.rag;

import de.kherud.llama.InferenceParameters;
import de.kherud.llama.LlamaModel;
import de.kherud.llama.LlamaOutput;
import de.kherud.llama.ModelParameters;
import de.kherud.llama.*;
import fqlite.erm.SchemaRetriever;

import java.sql.*;
import java.util.*;

/**
 * This class contains RAG + SQL Generator + DB Integration
 *  author: @pawlaszc
 */
public class RAGPipeline {

    private SchemaRetriever schemaRetriever = null;
    private final SQLGenerator sqlGenerator;

    /**
     * Constructor for the pipeline.
     *
     * @param modelPath  we need the path to the .guff file
     * @throws Exception thrown if something goes wrong during initialization.
     */
    public RAGPipeline(String modelPath) {

        // make sure, that modell is loaded and ready
        this.sqlGenerator = new SQLGenerator(modelPath);

    }

        /**
         * Constructor for the pipeline.
         *
         * @param modelPath  we need the path to the .guff file
         * @param schema all CREATE TABLE statement from the schema definition as String
         * @throws Exception thrown if something goes wrong during initialization.
         */
    public RAGPipeline(String modelPath, String schema) {

        System.out.println("Initializing ForensicSQL with RAG...");

        String fullSchema = "";

        if (schema.isEmpty()) {

            // just for testing
            fullSchema = """
                    CREATE TABLE messages (
                        _id INTEGER PRIMARY KEY,
                        thread_id INTEGER,
                        address TEXT,
                        body TEXT,
                        date INTEGER,
                        read INTEGER,
                        FOREIGN KEY (thread_id) REFERENCES thread(_id)
                    );
                    
                    CREATE TABLE thread (
                        _id INTEGER PRIMARY KEY,
                        recipient_id INTEGER,
                        snippet TEXT
                    );
                    
                    CREATE TABLE contacts (
                        _id INTEGER PRIMARY KEY,
                        name TEXT,
                        phone TEXT
                    );
                    
                    CREATE TABLE call_log (
                        _id INTEGER PRIMARY KEY,
                        number TEXT,
                        duration INTEGER
                    );
                    """;
        }

        System.out.println("✅ Extracted schema: " + countTables(fullSchema) + " tables");

        // 3. Initialize RAG retriever
       // this.schemaRetriever = new RAGSchemaRetriever(fullSchema);

        // 4. Initialize SQL generator
        this.sqlGenerator = new SQLGenerator(modelPath);

        System.out.println("✅ ForensicSQL with RAG ready!");
    }

    public void initializeRetriever(Connection dbConnection) {
         this.schemaRetriever = new SchemaRetriever(dbConnection);
    }


    /**
     * Generate SQL for prompt
     */
    public String generateSQL(String request) {
        return generateSQL(request, 200);
    }

    /**
     * This method is used to combine schema and SQL statement
     * @param request the SQL statement
     * @param maxTables how many table should be involved
     * @return the result of the query
     */
    public String generateSQL(String request, int maxTables) {
        System.out.println("\n📝 Request: " + request);

        // Step 1: RAG - Retrieve relevant schema
        String relevantSchema = schemaRetriever.retrieveRelevantSchema(request, maxTables);

        // Step 2: Generate SQL
        String sql = sqlGenerator.generateSQL(relevantSchema, request);

        System.out.println("✅ Generated SQL: " + sql);

        return sql;
    }

    /**
     * Generate SQL and analyze validity.
     */
    public QueryResult analyzeRequest(String request) {

        try {
            String sql = generateSQL(request);

            if (!isValidSQL(sql)) {
                return new QueryResult(false, "Invalid SQL generated", null, sql);
            }
            else
                return null;

        } catch (Exception e) {
            return new QueryResult(false, "SQL Error: " + e.getMessage(), null, null);
        }
    }


    private int countTables(String schema) {
        return (int) Arrays.stream(schema.split(";"))
                .filter(s -> s.trim().toUpperCase().startsWith("CREATE TABLE"))
                .count();
    }

    private boolean isValidSQL(String sql) {
        if (sql == null || sql.isEmpty()) return false;
        String upper = sql.toUpperCase().trim();
        return upper.startsWith("SELECT") && upper.contains("FROM");
    }




    // ========================================================================
    // INNER CLASS: SQLGenerator (using llama.cpp)
    // ========================================================================

    static class SQLGenerator {
        private static LlamaModel model;

        SQLGenerator(String modelPath) {
            if (null == model) {
                ModelParameters params = new ModelParameters().
                        setGpuLayers(0).setCtxSize(2048).setModel(modelPath);
                model = new LlamaModel(params);
                System.out.println("✅ Modell has been loaded");
            }
            else
                System.out.println("✅ Modell is already loaded. ");

        }

        String generateSQL(String schema, String request) {
            String prompt = String.format(
                    """
                            Generate a valid SQLite query for this forensic database request.
                            
                            Database Schema:
                            %s
                            
                            Request: %s
                            
                            SQLite Query:
                            """,
                    schema, request
            );

            InferenceParameters inferParams = new InferenceParameters(prompt)
                    .setTemperature(0.0f)
                    .setTopP(0.9f)
                    .setTopK(40)
                    .setPresencePenalty(1.1f)
                    .setNPredict(1024)
                    .setStopStrings("\n", ";");

            LlamaIterable outputs = model.generate(inferParams);
            StringBuilder sql = new StringBuilder();
            for (LlamaOutput output : outputs) {
                sql.append(output.text);
            }

            return cleanSQL(sql.toString());
        }

        private String cleanSQL(String sql) {
            sql = sql.trim().replace("```sql", "").replace("```", "");
            sql = sql.split("\n")[0].trim();
            if (!sql.endsWith(";")) sql += ";";
            return sql;
        }


    }

    // ========================================================================
    // RESULT CLASS
    // ========================================================================

    public static class QueryResult {
        public final boolean success;
        public final String message;
        public final List<Map<String, Object>> data;
        public final String sql;

        public QueryResult(boolean success, String message,
                           List<Map<String, Object>> data, String sql) {
            this.success = success;
            this.message = message;
            this.data = data;
            this.sql = sql;
        }

        public void print() {
            System.out.println("\n" + "=".repeat(80));
            System.out.println("QUERY RESULT");
            System.out.println("=".repeat(80));
            System.out.println("Success: " + success);
            System.out.println("SQL: " + sql);
            System.out.println("Message: " + message);

            if (data != null && !data.isEmpty()) {
                System.out.println("\nResults: " + data.size() + " rows");
                for (int i = 0; i < Math.min(5, data.size()); i++) {
                    System.out.println("  Row " + (i+1) + ": " + data.get(i));
                }
                if (data.size() > 5) {
                    System.out.println("  ... (" + (data.size() - 5) + " more)");
                }
            }
            System.out.println("=".repeat(80));
        }
    }



    // ========================================================================
    // MAIN - Example Usage
    // ========================================================================

    public static void main(String[] args) {
        String modelPath = "/Users/pawel/llm_models/forensic-sqlite-llama-3.2-3b-Q4_K_M.gguf";
        String schema = "";

        try {
            RAGPipeline analyzer = new RAGPipeline(modelPath, schema);

            String[] requests = {
                    "Find all unread messages from yesterday",
                    "Show me messages with media attachments",
                    "List all group conversations",
                    "Find deleted messages"
            };

            System.out.println("\n" + "=".repeat(80));
            System.out.println("FORENSIC SQL ANALYSIS WITH RAG");
            System.out.println("=".repeat(80));

            for (String request : requests) {
                analyzer.analyzeRequest(request);
                Arrays.stream(requests).toList().stream().toString();
            }

        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
