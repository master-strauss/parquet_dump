//package com.marcolotz.db2parquet.core;
//
//import com.lmax.disruptor.dsl.Disruptor;
//import com.marcolotz.db2parquet.core.interfaces.IngestionService;
//
//
//public class BaseIngestionService implements IngestionService {
//
//    private ArrayList<EventProducer> m_producers
//            = new ArrayList<EventProducer>();
//
//    private MessageConsumer m_consumer = null;
//    private Disruptor<MessageEvent> m_disruptor = null;
//
//
//    @Override
//    public void startIngestion() {
//        startup();
//    }
//
//    public void stop() throws Exception {
//        Observable.from(m_producers)
//                .subscribe(producer -> {
//                    producer.stop();
//                });
//        m_disruptor.halt();
//        m_disruptor.shutdown();
//        m_consumer.stop();
//    }
//
//    private void startup() {
//        m_disruptor = new Disruptor<>(MessageEvent.EVENT_FACTORY
//                , (int)Math.pow(2, 12)
//                , DaemonThreadFactory.INSTANCE
//                , ProducerType.MULTI
//                , new BusySpinWaitStrategy());
//        Observable.range(1, m_producer_count)
//                .subscribe(idx -> {
//                    m_producers.add(new EventProducer(m_config, m_disruptor));
//                });
//        m_consumer = new MessageConsumer(m_disruptor);
//        m_consumer.start();
//        m_disruptor.start();
//        Observable.from(m_producers)
//                .subscribe(producer -> {
//                    producer.start();
//                });
//    }
//}
