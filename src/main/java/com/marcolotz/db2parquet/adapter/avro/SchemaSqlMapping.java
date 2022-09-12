package com.marcolotz.db2parquet.adapter.avro;

import lombok.Getter;
import lombok.Value;
import org.apache.avro.Schema;

/**
 * Mapping between a sql column and schema.
 */
@Value
@Getter
public class SchemaSqlMapping {

  String schemaName;
  String sqlColumnName;
  int sqlType;
  Schema.Type schemaType;

}
