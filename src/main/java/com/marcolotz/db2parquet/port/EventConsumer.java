package com.marcolotz.db2parquet.port;

import com.lmax.disruptor.EventHandler;

/**
 * Consumer that consumes event from ring buffer.
 */
public interface EventConsumer<EVENTTYPE> {

  /**
   * One or more event handler to handle event from ring buffer.
   */
  EventHandler<EVENTTYPE>[] getEventHandler();
}
