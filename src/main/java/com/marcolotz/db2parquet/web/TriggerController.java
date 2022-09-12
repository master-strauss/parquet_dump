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

  public CompletableFuture<ResponseEntity<Void>> ingestData() {
    // No controller advice due to quick and dirty implementation.
    // I want to have exceptions and 500 being returned here.
    // TODO: Make service async and avoid multiple parallel triggers
    // Right now I am using @Syncronized for avoid by, but there are likely
    // more elegant ways.
    // TODO: This will cause timeout error for long ingestions, that's one of the
    // reasons async should be used here.
    return CompletableFuture.supplyAsync(() ->
    {
      try {
        ingestionService.triggerIngestion();
        return ResponseEntity.ok().build();
      } catch (Exception e) {
        log.error("Exception raised while processing ingestion: {}", e.toString());
        return ResponseEntity.internalServerError().build();
      }
    });
  }
}
