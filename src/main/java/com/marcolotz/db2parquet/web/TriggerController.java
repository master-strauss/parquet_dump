package com.marcolotz.db2parquet.web;

import com.marcolotz.db2parquet.api.TriggerApi;
import com.marcolotz.db2parquet.port.IngestionService;
import java.util.concurrent.CompletableFuture;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Log4j2
public class TriggerController implements TriggerApi {

  @Autowired
  IngestionService ingestionService;

  public ResponseEntity<Void> ingestData() {
    if (ingestionService.isBusy()) {
      return ResponseEntity.status(503).build();
    }
    // Start ingestion - only error feedback to users is on the logs
    CompletableFuture.runAsync(ingestionService::triggerIngestion).exceptionally(this::logException);
    return ResponseEntity.accepted().build();
  }

  private Void logException(Throwable throwable) {
    log.error("The following error happened while ingesting the data: " + throwable.getMessage());
    return null;
  }

}
