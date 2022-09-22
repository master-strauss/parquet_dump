package com.marcolotz.db2parquet.core;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import com.marcolotz.db2parquet.adapter.DiskConsumer;
import com.marcolotz.db2parquet.adapter.EncryptionTransformer;
import com.marcolotz.db2parquet.adapter.JdbcProducer;
import com.marcolotz.db2parquet.adapter.ParquetTransformer;
import com.marcolotz.db2parquet.core.events.AvroResultSetEvent;
import com.marcolotz.db2parquet.core.events.EncryptedByteSequenceEvent;
import com.marcolotz.db2parquet.core.events.ParquetByteSequenceEvent;
import com.marcolotz.db2parquet.port.DiskWriter;
import com.marcolotz.db2parquet.port.Encryptor;
import com.marcolotz.db2parquet.port.ParquetSerializer;
import java.util.concurrent.ThreadFactory;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class TaskSequence {

  private final JdbcProducer jdbcProducer;
  private final ParquetTransformer parquetTransformer;
  private final EncryptionTransformer encryptorTransformer;
  private final DiskConsumer diskConsumer;

  private final int RING_BUFFER_CAPACITY = 64;

  @SneakyThrows
  public TaskSequence(JdbcProducer jdbcProducer,
    ParquetSerializer parquetSerializer,
    Encryptor encryptor,
    DiskWriter diskWriter) {
    // Create Disruptor Ring Buffers
    this.jdbcProducer = jdbcProducer;
    final Disruptor<AvroResultSetEvent> avroResultSetDisruptor = createRingBuffer(RING_BUFFER_CAPACITY,
      AvroResultSetEvent.EVENT_FACTORY);
    final Disruptor<ParquetByteSequenceEvent> parquetToEncryptionDisruptor = createRingBuffer(RING_BUFFER_CAPACITY,
      ParquetByteSequenceEvent.EVENT_FACTORY);
    final Disruptor<EncryptedByteSequenceEvent> encryptionToDiskDisruptor = createRingBuffer(RING_BUFFER_CAPACITY,
      EncryptedByteSequenceEvent.EVENT_FACTORY);

    // Create producers and consumers
    this.jdbcProducer.registerDisruptor(avroResultSetDisruptor);
    parquetTransformer = new ParquetTransformer(parquetSerializer, avroResultSetDisruptor,
      parquetToEncryptionDisruptor);
    encryptorTransformer = new EncryptionTransformer(encryptor, parquetToEncryptionDisruptor,
      encryptionToDiskDisruptor);
    diskConsumer = new DiskConsumer(diskWriter, encryptionToDiskDisruptor);

    // Startup disruptors
    avroResultSetDisruptor.start();
    parquetToEncryptionDisruptor.start();
    encryptionToDiskDisruptor.start();
  }

  private <T> Disruptor<T> createRingBuffer(final int capacity,
    final EventFactory<T> eventFactory) {
    ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;

    // 1 MS instead of 100 ns.
    WaitStrategy waitStrategy = new SleepingWaitStrategy(200, 1_000_000);
    return new Disruptor<>(
      eventFactory,
      capacity,
      threadFactory,
      ProducerType.SINGLE,
      waitStrategy);
  }

  public boolean isFinished() {
    return jdbcProducer.hasFinished() && parquetTransformer.finishedProcessingAllMessages()
      && encryptorTransformer.finishedProcessingAllMessages() && diskConsumer.finishedProcessingAllMessages();
  }

  @SneakyThrows
  public void waitForCompletion() {
    // TODO: Probably a completable future here would be better
    while (!isFinished()) {
      Thread.sleep(1000);
    }
    log.info("Task Sequence finished");
  }
}
