package com.marcolotz.db2parquet.adapter;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.marcolotz.db2parquet.core.events.EncryptedByteSequenceEvent;
import com.marcolotz.db2parquet.core.events.FileData;
import com.marcolotz.db2parquet.core.events.ParquetByteSequenceEvent;
import com.marcolotz.db2parquet.port.Encryptor;
import com.marcolotz.db2parquet.port.EventConsumer;
import com.marcolotz.db2parquet.port.EventProducer;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class EncryptionTransformer implements EventConsumer<ParquetByteSequenceEvent>,
  EventProducer<FileData> {

  final Disruptor<ParquetByteSequenceEvent> inputboundDisruptor;
  private final Encryptor encryptor;
  private final Disruptor<EncryptedByteSequenceEvent> outputDisruptor;

  public EncryptionTransformer(final Encryptor encryptor,
    final Disruptor<ParquetByteSequenceEvent> inputboundDisruptor,
    final Disruptor<EncryptedByteSequenceEvent> outputDisruptor) {
    this.encryptor = encryptor;
    this.outputDisruptor = outputDisruptor;
    this.inputboundDisruptor = inputboundDisruptor;
    inputboundDisruptor.handleEventsWith(getEventHandler());
  }

  private FileData toEncryptedFile(final FileData notEncryptedFile) {
    return new FileData(notEncryptedFile.getFileName(),
      encryptor.encrypt(notEncryptedFile.getContents()));
  }

  public EventHandler<ParquetByteSequenceEvent>[] getEventHandler() {
    EventHandler<ParquetByteSequenceEvent> eventHandler
      = (event, sequence, endOfBatch)
      -> processEvent(event, sequence);
    return new EventHandler[]{eventHandler};
  }

  private void processEvent(ParquetByteSequenceEvent event, long sequence) {
    log.debug(() -> "Starting encryption of message with sequence number: " + sequence);
    FileData encryptedFileData = encrypt(event.getParquetFile());
    produce(encryptedFileData);
    log.debug(() -> "Finished encryption of message with sequence number: " + sequence);
  }

  private FileData encrypt(FileData fileDataToEncrypt) {
    return toEncryptedFile((fileDataToEncrypt));
  }

  @Override
  public void produce(FileData encryptedFileData) {
    final RingBuffer<EncryptedByteSequenceEvent> ringBuffer = outputDisruptor.getRingBuffer();
    final long seq = ringBuffer.next();
    final EncryptedByteSequenceEvent encryptEvent = ringBuffer.get(seq);
    encryptEvent.setEncryptedData(encryptedFileData);
    ringBuffer.publish(seq);
  }

  public boolean finishedProcessingAllMessages() {
    return inputboundDisruptor.getRingBuffer().getCursor() == outputDisruptor.getRingBuffer()
      .getCursor();
  }
}
