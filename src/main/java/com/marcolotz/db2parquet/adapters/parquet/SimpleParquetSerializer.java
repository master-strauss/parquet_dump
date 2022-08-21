package com.marcolotz.db2parquet.adapters.parquet;

import com.marcolotz.db2parquet.port.ParquetSerializer;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/***
 * Implementation based on:
 * https://github.com/benleov/parquet-resultset/tree/master/src/main/java/parquet/resultset/impl
 *
 */
@Value
public class SimpleParquetSerializer implements ParquetSerializer {

    String schemaName;
    String namespace;

    @Override
    @SneakyThrows
    public byte[] convertToParquet(ResultSet resultSet) {

        // TODO: This can be processed only once
        // ===
        SchemaResults schemaResults = new ResultSetSchemaGenerator().generateSchema(resultSet,
                schemaName, namespace);

        InMemoryOutputFile inMemoryOutputFile = new InMemoryOutputFile();

        ParquetWriter parquetWriter = AvroParquetWriter.builder(inMemoryOutputFile)
                .withSchema(schemaResults.getParsedSchema())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();

        List<GenericRecord> records = new ArrayList<>();

        // ===
        while (resultSet.next()) {

            GenericRecordBuilder builder = new GenericRecordBuilder(schemaResults.getParsedSchema());

            for (SchemaSqlMapping mapping : schemaResults.getMappings()) {

                builder.set(
                        schemaResults.getParsedSchema().getField(mapping.getSchemaName()),
                        ResultSetTransformer.extractResult(mapping, resultSet));
            }

            GenericRecord record = builder.build();

            records.add(record);
        }

        for (GenericRecord record : records) {
            parquetWriter.write(record);
        }

        parquetWriter.close();
        return inMemoryOutputFile.toArray();
    }
}
