package com.marcolotz.db2parquet;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import com.marcolotz.db2parquet.core.events.EncryptedByteSequenceEvent;
import lombok.Value;

import java.util.Observable;

@Value
public class EncryptionToDiskDispatcher {
    ArrayList<EncryptionEventProducer> eventProducers;
    DiskWriterConsumer diskWriterConsumer;
    Disruptor<EncryptedByteSequenceEvent> disruptor;

    public void start() throws Exception {
        startup();
    }

    public void stop() throws Exception {
        Observable.from(eventProducers)
                .subscribe(producer -> {
                    producer.stop();
                });
        disruptor.halt();
        disruptor.shutdown();
        diskWriterConsumer.stop();
    }

    private void startup() {
        disruptor = new Disruptor<>(EncryptedByteSequenceEvent.EVENT_FACTORY
                , (int)Math.pow(2, 12)
                , DaemonThreadFactory.INSTANCE
                , ProducerType.MULTI
                , new BusySpinWaitStrategy());

        Observable.range(1, eventProducers.size())
                .subscribe(idx -> {
                    m_producers.add(new EventProducer(m_config, m_disruptor));
                });
        m_consumer = new MessageConsumer(m_disruptor);
        m_consumer.start();
        m_disruptor.start();
        Observable.from(m_producers)
                .subscribe(producer -> {
                    producer.start();
                });
    }
}
