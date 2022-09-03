package com.marcolotz.db2parquet.core;

import com.marcolotz.db2parquet.adapters.avro.JdbcToAvroWorker;
import com.marcolotz.db2parquet.adapters.avro.JdbcToAvroWorkerFactory;
import com.marcolotz.db2parquet.config.Db2ParquetConfigurationProperties;
import com.marcolotz.db2parquet.port.DiskWriter;
import com.marcolotz.db2parquet.port.Encryptor;
import com.marcolotz.db2parquet.port.ParquetSerializer;
import java.util.LinkedList;
import java.util.List;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

@Value
@Log4j2
public class IngestionCoordinator {

  Db2ParquetConfigurationProperties configurationProperties;
  JdbcToAvroWorkerFactory jdbcToAvroWorkerFactory;
  ParquetSerializer parquetSerializer;
  Encryptor encryptor;
  DiskWriter diskWriter;

  @SneakyThrows
  public void startIngestion() {
    log.info(
      () -> "Starting Ingestion: setting up " + configurationProperties.getNumberOfConcurrentSyncs()
        + " parallel ingestions");
    List<TaskSequence> taskSequences = new LinkedList<>();
    for (int parallelWorkerNumber = 0; parallelWorkerNumber < configurationProperties.getNumberOfConcurrentSyncs();
      parallelWorkerNumber++) {
      final JdbcToAvroWorker jdbcToAvroWorker = jdbcToAvroWorkerFactory.build(parallelWorkerNumber);
      final TaskSequence taskSequence = new TaskSequence(jdbcToAvroWorker, parquetSerializer,
        encryptor, diskWriter);
      taskSequences.add(taskSequence);
    }
    taskSequences.forEach(TaskSequence::run);
  }

}
