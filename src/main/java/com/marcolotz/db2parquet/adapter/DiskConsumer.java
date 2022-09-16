package com.marcolotz.db2parquet.adapter;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.marcolotz.db2parquet.core.events.EncryptedByteSequenceEvent;
import com.marcolotz.db2parquet.core.events.FileData;
import com.marcolotz.db2parquet.port.DiskWriter;
import com.marcolotz.db2parquet.port.EventConsumer;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class DiskConsumer implements EventConsumer<EncryptedByteSequenceEvent> {

  private final DiskWriter diskWriter;
  private final Disruptor<EncryptedByteSequenceEvent> inputDisruptor;
  private int numberOfProcessedMessages = 0;

  public DiskConsumer(final DiskWriter diskWriter,
    final Disruptor<EncryptedByteSequenceEvent> input) {
    this.diskWriter = diskWriter;
    this.inputDisruptor = input;
    inputDisruptor.handleEventsWith(getEventHandler());
  }

  public void processEvent(final FileData fileData) {
    log.debug(() -> "starting writing " + fileData.getContents().length + " bytes to disk");
    diskWriter.write(fileData);
    log.debug(() -> "completed writing " + fileData.getContents().length + " bytes to disk");
    numberOfProcessedMessages++;
  }

  @Override
  public EventHandler<EncryptedByteSequenceEvent>[] getEventHandler() {
    EventHandler<EncryptedByteSequenceEvent> eventHandler
      = (event, sequence, endOfBatch)
      -> processEvent(event.getEncryptedData());
    return new EventHandler[]{eventHandler};
  }

  public boolean finishedProcessingAllMessages() {
    // All messages in the inbound have already been processed.
    return inputDisruptor.getCursor() + 1 == numberOfProcessedMessages;
  }
}
