package com.marcolotz.db2parquet.adapter;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.marcolotz.db2parquet.port.EventTransformer;
import lombok.extern.log4j.Log4j2;

@Log4j2
public abstract class BaseTransformer<INPUT_EVENT, OUTPUT_EVENT> implements
  EventTransformer<INPUT_EVENT> {

  Disruptor<INPUT_EVENT> inboundDisruptor;
  Disruptor<OUTPUT_EVENT> outputDisruptor;

  public BaseTransformer(final Disruptor<INPUT_EVENT> inputboundDisruptor,
    final Disruptor<OUTPUT_EVENT> outputDisruptor) {
    this.outputDisruptor = outputDisruptor;
    this.inboundDisruptor = inputboundDisruptor;
    inputboundDisruptor.handleEventsWith(getEventHandler());
  }

  public EventHandler<INPUT_EVENT>[] getEventHandler() {
    EventHandler<INPUT_EVENT> eventHandler = (event, sequence, endOfBatch) -> processEvent(event);
    return new EventHandler[]{eventHandler};
  }

  private void processEvent(INPUT_EVENT event) {
    log.debug(() -> "Processing event:");
    transform(event);
    log.trace(() -> "Finished event for:");
  }


  public boolean finishedProcessingAllMessages() {
    return inboundDisruptor.getRingBuffer().getCursor() == outputDisruptor.getRingBuffer()
      .getCursor();
  }

}
