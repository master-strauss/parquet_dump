package com.marcolotz.db2parquet.adapters.parquet;

import org.apache.avro.generic.GenericRecord;

/**
 * Called when a {@link ResultSetTransformer } parses a ResultSet.
 *
 */
public interface TransformerListener {

    /**
     * Called whenever a record parsed.
     *
     * @param record The record that was parsed.
     */
    void onRecordParsed(GenericRecord record);

    /**
     * Called when the schema is parsed from a {@link java.sql.ResultSet }.
     *
     * @param schemaResults The parsed Schema.
     */
    void onSchemaParsed(SchemaResults schemaResults);
}
