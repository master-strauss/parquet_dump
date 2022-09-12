package com.marcolotz.db2parquet.core;

import com.marcolotz.db2parquet.port.IngestionService;
import lombok.Synchronized;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

@Value
@Log4j2
public class BaseIngestionService implements IngestionService {

  IngestionCoordinator ingestionCoordinator;

  @Override
  @Synchronized
  public void triggerIngestion() {
    log.info("Starting ingestion");
    ingestionCoordinator.ingest();
  }
}
