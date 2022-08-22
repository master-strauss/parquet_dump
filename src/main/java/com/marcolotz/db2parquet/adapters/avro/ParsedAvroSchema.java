package com.marcolotz.db2parquet.adapters.parquet;

import lombok.Data;
import org.apache.avro.Schema;
import java.util.List;

/**
 * Contains both the {@link org.apache.avro.Schema } and the mapping between the sql column name and type,
 * and associated schema type.
 */
@Data
public class ParsedAvroSchema {

    private Schema parsedSchema;

    private List<SchemaSqlMapping> mappings;
}
