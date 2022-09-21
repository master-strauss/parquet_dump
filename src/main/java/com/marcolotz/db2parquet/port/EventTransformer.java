package com.marcolotz.db2parquet.port;

public interface EventTransformer<INPUT_EVENT> extends EventConsumer<INPUT_EVENT> {

  void transform(INPUT_EVENT inputEvent);

}
