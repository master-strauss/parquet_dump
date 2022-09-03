package com.marcolotz.db2parquet.core;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import com.marcolotz.db2parquet.adapters.DiskConsumer;
import com.marcolotz.db2parquet.adapters.EncryptionTransformer;
import com.marcolotz.db2parquet.adapters.JdbcProducer;
import com.marcolotz.db2parquet.adapters.ParquetTransformer;
import com.marcolotz.db2parquet.adapters.avro.JdbcToAvroWorker;
import com.marcolotz.db2parquet.core.events.AvroResultSetEvent;
import com.marcolotz.db2parquet.core.events.EncryptedByteSequenceEvent;
import com.marcolotz.db2parquet.core.events.ParquetByteSequenceEvent;
import com.marcolotz.db2parquet.port.DiskWriter;
import com.marcolotz.db2parquet.port.Encryptor;
import com.marcolotz.db2parquet.port.ParquetSerializer;
import java.util.concurrent.ThreadFactory;
import lombok.SneakyThrows;

public class TaskSequence {
  private static final String SCHEMA_NAME = "db2parquet";
  private static final String NAMESPACE = "com.marcolotz";
  private JdbcProducer jdbcProducer;
  private ParquetTransformer parquetTransformer;
  private EncryptionTransformer encryptorTransformer;
  private DiskConsumer diskConsumer;

  @SneakyThrows
  public TaskSequence(JdbcToAvroWorker jdbcToAvroWorker,
  ParquetSerializer parquetSerializer,
  Encryptor encryptor,
  DiskWriter diskWriter)
  {
    // Create Disruptor Ring Buffers
    final Disruptor<AvroResultSetEvent> avroResultSetDisruptor = createRingBuffer(64, AvroResultSetEvent.EVENT_FACTORY);
    final Disruptor<ParquetByteSequenceEvent> parquetToEncryptionDisruptor = createRingBuffer(64, ParquetByteSequenceEvent.EVENT_FACTORY);
    final Disruptor<EncryptedByteSequenceEvent> encryptionToDiskDisruptor = createRingBuffer(64, EncryptedByteSequenceEvent.EVENT_FACTORY);

    // Create producers and consumers
    jdbcProducer= new JdbcProducer(jdbcToAvroWorker, avroResultSetDisruptor);
    parquetTransformer = new ParquetTransformer(parquetSerializer, avroResultSetDisruptor, parquetToEncryptionDisruptor);
    encryptorTransformer = new EncryptionTransformer(encryptor, parquetToEncryptionDisruptor, encryptionToDiskDisruptor);
    diskConsumer = new DiskConsumer(diskWriter, encryptionToDiskDisruptor);

    // Startup disruptors
    avroResultSetDisruptor.start();
    parquetToEncryptionDisruptor.start();
    encryptionToDiskDisruptor.start();
  }

  public void run()
  {
    jdbcProducer.run();
  }

  private <T> Disruptor<T> createRingBuffer(final int capacity, final EventFactory<T> eventFactory){
    ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;

    // TODO: Analyse if this is the best strategy
    WaitStrategy waitStrategy = new BusySpinWaitStrategy();
    return new Disruptor<T>(
      eventFactory,
      capacity,
      threadFactory,
      ProducerType.SINGLE,
      waitStrategy);
  }

  public boolean isFinished()
  {
    // First all ingestions need to be finished
    if (!jdbcProducer.hasFinished())
    {
      return false;
    }

    // Then all transformers
    if (!(parquetTransformer.finishedProcessingAllMessages() && encryptorTransformer.finishedProcessingAllMessages()))
    {
      return false;
    }

    // Then the disk writer
    return diskConsumer.finishedProcessingAllMessages();
  }

}
