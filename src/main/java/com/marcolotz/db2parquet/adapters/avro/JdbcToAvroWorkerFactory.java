package com.marcolotz.db2parquet.adapters.avro;

import java.sql.SQLException;
import javax.sql.DataSource;
import lombok.Value;

@Value
public class JdbcToAvroWorkerFactory {

  String templatedQuery;
  DataSource dataSource;
  int numberOfRowsToFetch;
  String schemaName;
  String namespace;

  public JdbcToAvroWorker build(int parallelWorkerNumber) throws SQLException {
    final String workerQuery = replaceQueryWithParallelWorkerNumber(parallelWorkerNumber);
    return new JdbcToAvroWorker(dataSource.getConnection(), workerQuery, numberOfRowsToFetch, schemaName, namespace);
  }

  private String replaceQueryWithParallelWorkerNumber(int parallelWorkerNumber) {
    // TODO: implement
    return null;
  }

}
