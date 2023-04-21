package com.marcolotz.db2parquet.adapter.avro;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.log4j.Log4j2;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

@Log4j2
public class JdbcToAvroWorker {

  private final int numberOfRowsToFetch;
  private final PreparedStatement stmt;
  private final String schemaName;
  private final String namespace;
  @Getter
  private ParsedAvroSchema avroSchema;
  private ResultSet resultSet;
  private boolean isFinished = false;
  private boolean isIngestionRunning = false;

  public JdbcToAvroWorker(final Connection dbConnection, final String query,
    final int numberOfRowsToFetch, final String schemaName, final String namespace)
    throws SQLException {
    this.numberOfRowsToFetch = numberOfRowsToFetch;
    this.schemaName = schemaName;
    this.namespace = namespace;
    this.stmt = dbConnection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    stmt.setFetchSize(numberOfRowsToFetch);
    log.debug(() -> "Evaluated query is: {}" + query);
  }

  // TODO: maybe add synchronization here?
  @Synchronized
  public GenericRecord[] produceAvroRecords() throws SQLException {
    if (!isIngestionRunning) {
      log.info("Executing query and creating AVRO schema");
      this.resultSet = stmt.executeQuery();
      avroSchema = new ResultSetSchemaGenerator().generateSchema(resultSet, schemaName, namespace);
      isIngestionRunning = true;
    }
    long numberOfIngestedRows = 0;
    GenericRecord[] genericRecordsBatch = new GenericRecord[numberOfRowsToFetch];
    for (int count = 0; count < numberOfRowsToFetch; count++) {
      if (resultSet.next()) {
        GenericRecord generatedRecord = convertToGenericRecord(resultSet);
        genericRecordsBatch[count] = generatedRecord;
        numberOfIngestedRows++;
      } else {
        isFinished = true;
        isIngestionRunning = false;
        break;
      }
    }
    log.info("JDBC Producer finished ingesting {} rows from the database", numberOfIngestedRows);
    return genericRecordsBatch;
  }

  @Synchronized
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
