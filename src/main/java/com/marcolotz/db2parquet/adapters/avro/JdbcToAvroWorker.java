package com.marcolotz.db2parquet.adapters.avro;

import com.marcolotz.db2parquet.core.interfaces.JdbcWorker;
import lombok.extern.log4j.Log4j2;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Log4j2
public class SimpleJdbcWorker implements JdbcWorker {

    private final Connection dbConnection;

    private final int numberOfRowsToFetch;
    private final Statement stmt;
    private final ResultSet resultSet;
    private final ParsedAvroSchema avroSchema;
    private boolean isFinished = false;

    public SimpleJdbcWorker(final Connection dbConnection, final String query, final int numberOfRowsToFetch, final String schemaName, final String namespace) throws SQLException {
        this.numberOfRowsToFetch = numberOfRowsToFetch;
        this.dbConnection = dbConnection;
        stmt = this.dbConnection.prepareStatement(query,ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(numberOfRowsToFetch);

        log.debug(() -> "Evaluated query is: " + query);
        this.resultSet = stmt.executeQuery(query);

        avroSchema = new ResultSetSchemaGenerator().generateSchema(resultSet, schemaName, namespace);
    }

    @Override
    public GenericRecord[] produceAvroRecords() throws SQLException {
        GenericRecord[] genericRecordsBatch = new GenericRecord[numberOfRowsToFetch];
        for (int count = 0; count < numberOfRowsToFetch; count++)
        {
            if (resultSet.next())
            {
                isFinished = true;
                resultSet.close();
                return null;
            }
            else {
                GenericRecord generatedRecord = convertToGenericRecord(resultSet);
                genericRecordsBatch[count] = generatedRecord;
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

    @Override
    public boolean hasFinishedWork() {
        return isFinished;
    }
}
