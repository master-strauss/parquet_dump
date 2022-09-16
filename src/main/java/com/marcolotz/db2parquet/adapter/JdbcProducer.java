package com.marcolotz.db2parquet.adapter;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.marcolotz.db2parquet.adapter.avro.JdbcToAvroWorker;
import com.marcolotz.db2parquet.adapter.avro.ParsedAvroSchema;
import com.marcolotz.db2parquet.core.events.AvroResultSetEvent;
import com.marcolotz.db2parquet.port.EventProducer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
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
  public CompletableFuture<Void> run() {
    log.info(() -> "Starting JDBC producer");
    // TODO: Look into this, I am not keen on this argument null here
    runningCompletableFuture = CompletableFuture.runAsync(() -> produce(null));
    return runningCompletableFuture;
  }

  @Override
  @SneakyThrows
  public void produce(GenericRecord[] event) {
    while (!jdbcWorker.hasFinishedWork()) {
      // Loads Data into a chunk
      GenericRecord[] records = jdbcWorker.produceAvroRecords();
      // TODO: Look into this null test here
      if (records[0] != null) {
        ParsedAvroSchema parsedAvroSchema = jdbcWorker.getAvroSchema();
        // Chooses next disruptor from round-robin and write to ring buffer
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
    }
    log.info(() -> "JDBC worker finished consuming data");
  }

  public boolean hasFinished() {
    return runningCompletableFuture.isDone();
  }
}
