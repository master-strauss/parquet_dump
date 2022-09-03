package com.marcolotz.db2parquet.adapters;

import static java.lang.Thread.State.TERMINATED;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.marcolotz.db2parquet.adapters.avro.JdbcToAvroWorker;
import com.marcolotz.db2parquet.adapters.avro.ParsedAvroSchema;
import com.marcolotz.db2parquet.core.events.AvroResultSetEvent;
import com.marcolotz.db2parquet.port.EventProducer;
import java.sql.SQLException;
import lombok.SneakyThrows;
import lombok.Synchronized;
import lombok.extern.log4j.Log4j2;
import org.apache.avro.generic.GenericRecord;

@Log4j2
public class JdbcProducer implements EventProducer<GenericRecord[]> {

  // TODO: Maybe this should be a configuration?
  private static final String SCHEMA_NAME = "db2parquet";
  private static final String NAMESPACE = "com.marcolotz";
  private final JdbcToAvroWorker jdbcWorker;
  private final Disruptor<AvroResultSetEvent> disruptor;
  private Thread runningThread;

  public JdbcProducer(JdbcToAvroWorker jdbcWorker, Disruptor<AvroResultSetEvent> disruptor) {
    this.jdbcWorker = jdbcWorker;
    this.disruptor = disruptor;
  }

  @SneakyThrows
  // Just to avoid race conditions while creating the thread, probably better implementations
  // can be done here, but the overall overhead is low.
  @Synchronized
  public void run() {
    log.info(() -> "Starting JDBC producer");
    // TODO: Fix this syntax
    final Runnable producer = () -> {
      try {
        produce(jdbcWorker.produceAvroRecords());
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    };
    Thread thread = new Thread(producer);
    thread.start();
    log.info(() -> "JDBC producer running");
    runningThread = thread;
  }

  @Override
  @SneakyThrows
  public void produce(GenericRecord[] event) {
    final RingBuffer<AvroResultSetEvent> ringBuffer = disruptor.getRingBuffer();
    while (!jdbcWorker.hasFinishedWork()) {
      // Loads Data into a chunk
      GenericRecord[] records = jdbcWorker.produceAvroRecords();
      ParsedAvroSchema parsedAvroSchema = jdbcWorker.getAvroSchema();
      // Write to ring buffer
      final long seq = ringBuffer.next();
      final AvroResultSetEvent resultSetEvent = ringBuffer.get(seq);
      resultSetEvent.setAvroSchema(parsedAvroSchema.getParsedSchema());
      resultSetEvent.setAvroRecords(records);
      ringBuffer.publish(seq);
    }
    log.info(() -> "JDBC worker finished consuming data");
  }

  public boolean hasFinished() {
    return runningThread.getState().equals(TERMINATED);
  }
}
