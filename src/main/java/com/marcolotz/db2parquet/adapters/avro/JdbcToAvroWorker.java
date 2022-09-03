package com.marcolotz.db2parquet.adapters.avro;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

@Log4j2
public class JdbcToAvroWorker {

    private final int numberOfRowsToFetch;
    private final ResultSet resultSet;
    @Getter
    private final ParsedAvroSchema avroSchema;
    private boolean isFinished = false;

    public JdbcToAvroWorker(final Connection dbConnection, final String query, final int numberOfRowsToFetch, final String schemaName, final String namespace) throws SQLException {
        this.numberOfRowsToFetch = numberOfRowsToFetch;
        PreparedStatement stmt = dbConnection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(numberOfRowsToFetch);

        log.debug(() -> "Evaluated query is: " + query);
        this.resultSet = stmt.executeQuery();

        avroSchema = new ResultSetSchemaGenerator().generateSchema(resultSet, schemaName, namespace);
    }

    public GenericRecord[] produceAvroRecords() throws SQLException {
        GenericRecord[] genericRecordsBatch = new GenericRecord[numberOfRowsToFetch];
        for (int count = 0; count < numberOfRowsToFetch; count++)
        {
            if (resultSet.next())
            {
                GenericRecord generatedRecord = convertToGenericRecord(resultSet);
                genericRecordsBatch[count] = generatedRecord;
            }
            else {
                isFinished = true;
                break;
            }
        }
        return genericRecordsBatch;
    }

    private GenericRecord convertToGenericRecord(ResultSet resultSet) throws SQLException {
        GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema.getParsedSchema());

        for (SchemaSqlMapping mapping : avroSchema.getMappings()) {
            builder.set(
                    avroSchema.getParsedSchema().getField(mapping.getSchemaName()),
                    ResultSetTransformer.extractResult(mapping, resultSet));
        }

        return builder.build();
    }

    public boolean hasFinishedWork() {
        return isFinished;
    }
}
