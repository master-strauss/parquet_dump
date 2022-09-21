package com.marcolotz.db2parquet.port;

public interface EventTransformer<INPUT_EVENT, OUTPUT_EVENT_PAYLOAD> extends EventConsumer<INPUT_EVENT> {

  void transform(OUTPUT_EVENT_PAYLOAD outputEventPayload);

}
