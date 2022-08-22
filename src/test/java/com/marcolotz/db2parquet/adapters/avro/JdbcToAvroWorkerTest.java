package com.marcolotz.db2parquet.adapters.avro;

import com.marcolotz.db2parquet.adapters.parquet.SimpleParquetSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("When loading data from JDBC")
class JdbcToAvroWorkerTest {
//
//    protected static final String ID_FIELD_NAME = "id";
//    protected static final String TEMP_FILE_NAME = "unit_test.tmp";
//    protected static final String SCHEMA_NAME = "SchemaName";
//    protected static final String NAMESPACE = "org.NAMESPACE";
//    protected static final Integer[] ID_VALUES = {0, 1, 2, 3, 4, 5, 6};
//
//    protected ResultSet resultSet = mock(ResultSet.class);
//    protected ResultSetMetaData metaData = mock(ResultSetMetaData.class);
//
//    SimpleParquetSerializer parquetSerializer;
//
//    @BeforeEach
//    public void before() throws Exception {
//
//        Boolean[] nextReturns = new Boolean[ID_VALUES.length + 1];
//        Arrays.fill(nextReturns, Boolean.TRUE);
//        nextReturns[nextReturns.length - 1] = false; // set last value to false
//
//        when(resultSet.next()).thenReturn(nextReturns[0], Arrays.copyOfRange(nextReturns, 1, nextReturns.length));
//        when(resultSet.getInt(ID_FIELD_NAME)).thenReturn(ID_VALUES[0], Arrays.copyOfRange(ID_VALUES, 1, ID_VALUES.length));
//        when(resultSet.getMetaData()).thenReturn(metaData);
//
//        // mock metadata so schema is created
//        when(metaData.getColumnCount()).thenReturn(1);
//        when(metaData.getColumnName(1)).thenReturn(ID_FIELD_NAME);
//        when(metaData.getColumnType(1)).thenReturn(Types.INTEGER);
//
//        parquetSerializer = new SimpleParquetSerializer(SCHEMA_NAME, NAMESPACE);
//    }
//
//    @Test
//    @DisplayName("Then avro should be correctly serialized")
//    void writeToFile(@TempDir java.nio.file.Path directory) throws IOException {
//        File tmpFile = new File(directory.toString() + "/" + TEMP_FILE_NAME);
//        testToResultFile(tmpFile);
//    }
//
//    void testToResultFile(File tempFile) throws IOException {
//
//        byte[] serializedParquet = parquetSerializer.convertToParquet(resultSet);
//
//        // dump serialization to fs:
//        try (FileOutputStream outputStream = new FileOutputStream(tempFile)) {
//            outputStream.write(serializedParquet);
//        }
//
//        validate(tempFile, ID_FIELD_NAME, ID_VALUES);
//    }
//
//    void validate(File file, String fieldName, Integer... expectedValues) throws IOException {
//        Path path = new Path(file.toString());
//        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(path).build();
//
//        int x = 0;
//        boolean recordsRead = false;
//        for(GenericRecord record = reader.read(); record != null; record = reader.read()) {
//
//            recordsRead = true;
//            assertEquals(expectedValues[x++], record.get(fieldName));
//        }
//
//
//        assertTrue(recordsRead);
//
//        reader.close();
//    }

}