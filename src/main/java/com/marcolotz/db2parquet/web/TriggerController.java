package com.marcolotz.db2parquet.web;

import com.marcolotz.db2parquet.api.TriggerApi;
import com.marcolotz.db2parquet.port.IngestionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TriggerController implements TriggerApi {

  @Autowired
  IngestionService ingestionService;

  public ResponseEntity<Void> ingestData() {
    return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
  }
}
