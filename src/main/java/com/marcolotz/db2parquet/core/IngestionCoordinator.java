package com.marcolotz.db2parquet.core;

import com.marcolotz.db2parquet.adapter.JdbcProducer;
import com.marcolotz.db2parquet.adapter.avro.JdbcToAvroWorker;
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
  JdbcToAvroWorker jdbcToAvroWorker;
  ParquetSerializer parquetSerializer;
  Encryptor encryptor;
  DiskWriter diskWriter;

  @SneakyThrows
  public void ingest() {
    log.info(
      () -> "Starting Ingestion: setting up " + configurationProperties.getNumberOfConcurrentSyncs()
        + " parallel ingestions");
    List<TaskSequence> taskSequences = new LinkedList<>();
    final JdbcProducer jdbcProducer = new JdbcProducer(jdbcToAvroWorker);
    // Parallelize to the number of concurrent syncs
    for (int parallelWorkerNumber = 0; parallelWorkerNumber < configurationProperties.getNumberOfConcurrentSyncs();
      parallelWorkerNumber++) {
      final TaskSequence taskSequence = new TaskSequence(jdbcProducer, parquetSerializer, encryptor, diskWriter);
      taskSequences.add(taskSequence);
    }
    // The jdbc producer will start all other tasks in the sequence
    jdbcProducer.run();
    log.info(() -> "All ingestion threads are running");

    log.info(() -> "Waiting ingestion threads to complete..");
    taskSequences.forEach(TaskSequence::waitForCompletion);
    log.info(() -> "Ingestion complete");
  }

}
