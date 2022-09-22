package com.marcolotz.db2parquet.core;

import com.marcolotz.db2parquet.port.IngestionService;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.extern.log4j.Log4j2;

@Log4j2
@RequiredArgsConstructor
public class BaseIngestionService implements IngestionService {

  final IngestionCoordinator ingestionCoordinator;
  private boolean isBusy = false;

  @Override
  @Synchronized
  public void triggerIngestion() {
    isBusy = true;
    log.info("Starting ingestion");
    long startTimeInMilliseconds = System.currentTimeMillis();
    ingestionCoordinator.ingest();
    long numberOfSeconds = ((System.currentTimeMillis() - startTimeInMilliseconds) / 1000);
    log.info("Ingestion took " + numberOfSeconds + " seconds to complete");
    isBusy = false;
  }

  @Override
  public boolean isBusy() {
    return isBusy;
  }

}
