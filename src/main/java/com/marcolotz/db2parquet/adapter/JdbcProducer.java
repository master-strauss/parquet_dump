package com.marcolotz.db2parquet.adapter;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.marcolotz.db2parquet.adapter.avro.JdbcToAvroWorker;
import com.marcolotz.db2parquet.adapter.avro.ParsedAvroSchema;
import com.marcolotz.db2parquet.core.events.AvroResultSetEvent;
import com.marcolotz.db2parquet.port.EventProducer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import lombok.Synchronized;
import lombok.extern.log4j.Log4j2;
import org.apache.avro.generic.GenericRecord;

@Log4j2
public class JdbcProducer implements EventProducer<GenericRecord[]> {

  private final JdbcToAvroWorker jdbcWorker;
  private final List<Disruptor<AvroResultSetEvent>> disruptorList;
  private CompletableFuture<Void> runningCompletableFuture;
  private int currentDisruptorListIndex = 0;

  public JdbcProducer(JdbcToAvroWorker jdbcWorker) {
    this.jdbcWorker = jdbcWorker;
    disruptorList = new ArrayList<>();
  }

  public void registerDisruptor(Disruptor<AvroResultSetEvent> disruptor) {
    disruptorList.add(disruptor);
  }

  @SneakyThrows
  // Just to avoid race conditions while creating the thread, probably better implementations
  // can be done here, but the overall overhead is low.
  @Synchronized
  public CompletableFuture<Void> run() {
    log.info(() -> "Starting JDBC producer");
    runningCompletableFuture = CompletableFuture.runAsync(() -> {
      try {
        produce(jdbcWorker.produceAvroRecords());
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      log.info(() -> "JDBC producer running");
    });
    return runningCompletableFuture;
  }

  @Override
  @SneakyThrows
  public void produce(GenericRecord[] event) {
    while (!jdbcWorker.hasFinishedWork()) {
      // Loads Data into a chunk
      GenericRecord[] records = jdbcWorker.produceAvroRecords();
      ParsedAvroSchema parsedAvroSchema = jdbcWorker.getAvroSchema();
      // Chooses next disruptor from round robin and write to ring buffer
      final Disruptor<AvroResultSetEvent> currentDisruptor = disruptorList.get(
        currentDisruptorListIndex % disruptorList.size());
      final RingBuffer<AvroResultSetEvent> ringBuffer = currentDisruptor.getRingBuffer();
      final long seq = ringBuffer.next();
      final AvroResultSetEvent resultSetEvent = ringBuffer.get(seq);
      resultSetEvent.setAvroSchema(parsedAvroSchema.getParsedSchema());
      resultSetEvent.setAvroRecords(records);
      ringBuffer.publish(seq);
      currentDisruptorListIndex++;
    }
    log.info(() -> "JDBC worker finished consuming data");
  }

  public boolean hasFinished() {
    return run().isDone();
  }
}
