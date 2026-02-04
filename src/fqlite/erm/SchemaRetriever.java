package fqlite.erm;

import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.embedding.onnx.allminilml6v2.AllMiniLmL6V2EmbeddingModel;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.inmemory.InMemoryEmbeddingStore;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

///
///  This class is used to retrieve the SQL database schema.
///  from the internal memory table.
///
///  All schema related data - especially the CREATE TABLE... statements -
///  are than converted and stored to a embedding store.
///
///   @author pawlaszc
///
public class SchemaRetriever {
    private final EmbeddingModel embeddingModel;
    private final EmbeddingStore<TextSegment> embeddingStore;
    private final Map<String, TableSchema> tableSchemas;

    public SchemaRetriever(Connection connection) {
        this.embeddingModel = new AllMiniLmL6V2EmbeddingModel();
        this.embeddingStore = new InMemoryEmbeddingStore<>();
        this.tableSchemas = new HashMap<>();

        String fullSchema = null;
        try {
            fullSchema = extractFullSchema(connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        List<TableSchema> tables = parseSchema(fullSchema);

        for (TableSchema table : tables) {
            String description = createTableDescription(table);

            Metadata metadata = Metadata.from("table_name", table.name)
                    .put("column_count", String.valueOf(table.columns.size()))
                    .put("has_fks", String.valueOf(!table.foreignKeyReferences.isEmpty()));

            TextSegment segment = TextSegment.from(description, metadata);
            Embedding embedding = embeddingModel.embed(segment).content();

            embeddingStore.add(embedding, segment);
            tableSchemas.put(table.name, table);
        }
    }

    /**
     * Extract full schema from database
     */
    public String extractFullSchema(Connection dbConnection) throws SQLException {
        StringBuilder schema = new StringBuilder();

        DatabaseMetaData metaData = dbConnection.getMetaData();
        ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"});

        while (tables.next()) {
            String tableName = tables.getString("TABLE_NAME");

            if (tableName.startsWith("sqlite_") || tableName.startsWith("android_")) {
                continue;
            }

            Statement stmt = dbConnection.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='" + tableName + "'"
            );

            if (rs.next()) {
                schema.append(rs.getString("sql")).append(";\n\n");
            }

            rs.close();
            stmt.close();
        }

        tables.close();
        return schema.toString();
    }

    public String retrieveRelevantSchema(String request, int maxResults) {
        Embedding queryEmbedding = embeddingModel.embed(request).content();

        EmbeddingSearchRequest searchRequest = EmbeddingSearchRequest.builder()
                .queryEmbedding(queryEmbedding)
                .maxResults(maxResults)
                .minScore(0.3)
                .build();

        EmbeddingSearchResult<TextSegment> searchResult = embeddingStore.search(searchRequest);
        List<EmbeddingMatch<TextSegment>> matches = searchResult.matches();

        List<String> relevantTableNames = matches.stream()
                .map(match -> match.embedded().metadata().getString("table_name"))
                .collect(Collectors.toList());

        List<TableSchema> relevantTables = relevantTableNames.stream()
                .map(tableSchemas::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        relevantTables = addReferencedTables(relevantTables);

        return buildSchema(relevantTables);
    }

    public String createTableDescription(TableSchema table) {
        return "Table: " + table.name + ". Columns: " +
               String.join(", ", table.columns) + ".";
    }

    public List<TableSchema> parseSchema(String schema) {
        List<TableSchema> tables = new ArrayList<>();
        for (String stmt : schema.split(";")) {
            stmt = stmt.trim();
            if (stmt.toUpperCase().startsWith("CREATE TABLE")) {
                tables.add(new TableSchema(stmt));
            }
        }
        return tables;
    }

    private List<TableSchema> addReferencedTables(List<TableSchema> tables) {
        Set<String> included = tables.stream().map(t -> t.name).collect(Collectors.toSet());
        Set<String> referenced = tables.stream()
                .flatMap(t -> t.foreignKeyReferences.stream())
                .filter(n -> !included.contains(n))
                .collect(Collectors.toSet());

        List<TableSchema> expanded = new ArrayList<>(tables);
        for (String ref : referenced) {
            TableSchema refTable = tableSchemas.get(ref);
            if (refTable != null) expanded.add(refTable);
        }
        return expanded;
    }

    public String buildSchema(List<TableSchema> tables) {
        return tables.stream()
                       .map(t -> t.createStatement)
                       .collect(Collectors.joining(";\n\n")) + ";";
    }
}

