package com.marcolotz.db2parquet.port;

/**
 * Producer of events
 */
public interface EventProducer {

  /***
   * Produces an event
   */
  void produce();
}
