package com.marcolotz.db2parquet;

public class EncryptionEventProducer {
    private final JsonObject m_config;
    private final Disruptor<MessageEvent> m_disruptor;
    public EventProducer(JsonObject config
            , final Disruptor<MessageEvent> disruptor) {
        m_config = config;
        m_disruptor = disruptor;
        ...
    }

    public void stop() {
        ...
    }

    public void start() {
        final RingBuffer<MessageEvent> ringBuffer
                = m_disruptor.getRingBuffer();
        final int events_per_microsecond
                = m_config.getInteger("events-per-microsecond", 100);
        m_subscription = Observable.interval(1000/events_per_microsecond
                        , TimeUnit.NANOSECONDS)
                .onBackpressureBuffer(events_per_microsecond * 100000000
                        , () -> { ... }
                        , BackpressureOverflow.ON_OVERFLOW_DROP_LATEST)
                .observeOn(Schedulers.io())
                .subscribe(delay -> {
                            final long seq = ringBuffer.next();
                            final MessageEvent mEvent = ringBuffer.get(seq);
                            mEvent.setValue(100);
                            ringBuffer.publish(seq);
                        },
                        error -> {},
                        () -> {});
    }
}
