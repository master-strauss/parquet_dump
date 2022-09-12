package com.marcolotz.db2parquet.adapter.avro;

import java.util.List;
import lombok.Value;
import org.apache.avro.Schema;

/**
 * Contains both the {@link org.apache.avro.Schema } and the mapping between the sql column name and type, and
 * associated schema type.
 */
@Value
public class ParsedAvroSchema {

  Schema parsedSchema;

  List<SchemaSqlMapping> mappings;
}
