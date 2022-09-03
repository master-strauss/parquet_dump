package com.marcolotz.db2parquet.port;

/**
 * Producer that produces event for ring buffer.
 */
public interface EventProducer<EVENTTYPE> {

    /***
     * Produces an event
     * @param event event to be produced
     */
    void produce(final EVENTTYPE event);
}
