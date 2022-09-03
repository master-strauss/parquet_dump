package com.marcolotz.db2parquet.core;

import com.marcolotz.db2parquet.adapters.avro.JdbcToAvroWorker;
import com.marcolotz.db2parquet.config.Db2ParquetConfigurationProperties;
import com.marcolotz.db2parquet.port.DiskWriter;
import com.marcolotz.db2parquet.port.Encryptor;
import com.marcolotz.db2parquet.port.ParquetSerializer;
import java.util.LinkedList;
import java.util.List;
import javax.sql.DataSource;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

@Value
@Log4j2
public class IngestionCoordinator {

  Db2ParquetConfigurationProperties configurationProperties;
  DataSource dataSource;
  // TODO: Change to factories
  JdbcToAvroWorker jdbcToAvroWorker;
  ParquetSerializer parquetSerializer;
  Encryptor encryptor;
  DiskWriter diskWriter;

  public void startIngestion() {
    log.info(
      () -> "Starting Ingestion: setting up " + configurationProperties.getNumberOfConcurrentSyncs()
        + " parallel ingestions");
    List<TaskSequence> taskSequences = new LinkedList<>();
    for (int i = 0; i < configurationProperties.getNumberOfConcurrentSyncs(); i++) {
      // Note all the based configuration of the builder is already provided on the creation of
      // the bean
//      final JdbcToAvroWorker jdbcToAvroWorker = jdbcToAvroWorkerBuilder.build();
      final TaskSequence taskSequence = new TaskSequence(jdbcToAvroWorker, parquetSerializer,
        encryptor, diskWriter);
      taskSequences.add(taskSequence);
    }
    taskSequences.forEach(TaskSequence::run);
  }

}
