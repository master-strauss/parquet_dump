package com.marcolotz.db2parquet.port;

public interface IngestionService {

  void triggerIngestion();

  boolean isBusy();

}
