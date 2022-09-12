package com.marcolotz.db2parquet.adapter.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName( "When converting a SQL table to Parquet" )
class SimpleParquetSerializerTest {

  protected static final String ID_FIELD_NAME = "id";
  protected static final String TEMP_FILE_NAME = "unit_test.tmp";
  protected static final String SCHEMA_NAME = "test_schema";
  protected static final String SCHEMA_NAMESPACE = "com.marcolotz";
  protected static final Integer[] ID_VALUES = {0, 1, 2, 3, 4, 5, 6};
  protected final SimpleParquetSerializer parquetSerializer = new SimpleParquetSerializer(1024 * 1024 * 64);

  protected Schema schema;
  protected GenericRecord[] records;

  @BeforeEach
  public void before() {

    schema = Schema.createRecord(SCHEMA_NAME, null, SCHEMA_NAMESPACE, false);
    List<Schema.Field> fields = new LinkedList<>();

    // Prepare Avro Schema
    Schema.Type columnType = Schema.Type.INT;
    fields.add(createNullableField(schema, ID_FIELD_NAME, columnType));
    schema.setFields(fields);

    // Translate into records
    records = convertToGenericRecordOfIds(ID_VALUES, schema);

  }

  @Test
  @DisplayName( "Then avro should be correctly serialized" )
  void writeToFile(@TempDir java.nio.file.Path directory) throws IOException {
    File tmpFile = new File(directory.toString() + "/" + TEMP_FILE_NAME);
    testToResultFile(tmpFile);
  }

  void testToResultFile(File tempFile) throws IOException {

    byte[] serializedParquet = parquetSerializer.convertToParquet(schema, records);

    // dump serialization to fs:
    try (FileOutputStream outputStream = new FileOutputStream(tempFile)) {
      outputStream.write(serializedParquet);
    }

    validate(tempFile, ID_FIELD_NAME, ID_VALUES);
  }

  void validate(File file, String fieldName, Integer... expectedValues) throws IOException {
    Path path = new Path(file.toString());
    ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(path).build();

    int x = 0;
    boolean recordsRead = false;
    for (GenericRecord record = reader.read(); record != null; record = reader.read()) {

      recordsRead = true;
      assertEquals(expectedValues[x++], record.get(fieldName));
    }

    assertTrue(recordsRead);

    reader.close();
  }

  private Schema.Field createNullableField(Schema recordSchema, String columnName,
    Schema.Type type) {

    Schema intSchema = Schema.create(type);
    Schema nullSchema = Schema.create(Schema.Type.NULL);

    List<Schema> fieldSchemas = new LinkedList<>();
    fieldSchemas.add(intSchema);
    fieldSchemas.add(nullSchema);

    Schema fieldSchema = Schema.createUnion(fieldSchemas);

    return new Schema.Field(columnName, fieldSchema, null, null);
  }

  private GenericRecord[] convertToGenericRecordOfIds(Integer[] idValue, Schema schema) {
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);

    return Arrays.stream(idValue).map(id -> builder.set(schema.getField(ID_FIELD_NAME), id).build())
      .toArray(GenericRecord[]::new);
  }

}