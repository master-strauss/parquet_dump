package com.marcolotz.db2parquet.adapter.avro;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;

@SpringBootTest
@DisplayName( "When converting JDBC information to Avro..." )
class JdbcToAvroWorkerIT {

  private static final String QUERY = "SELECT * FROM owners";
  private static final int NUMBER_OF_ROWS_TO_FETCH = 100;
  private static final String SCHEMA_NAME = "testSchema";
  private static final String NAMESPACE = "com.marcolotz";

  private final DataSource dataSource;
  private JdbcToAvroWorker jdbcToAvroWorker;

  @Autowired
  JdbcToAvroWorkerIT(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  private static String convertResourceToString(final String resourceFilePath) throws IOException {
    ClassPathResource resource = new ClassPathResource(resourceFilePath);
    InputStream inputStream = resource.getInputStream();
    return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
  }

  @BeforeEach
  void before() throws SQLException, IOException {
    prepareDatabase(dataSource);
  }

  void prepareDatabase(DataSource dataSource) throws SQLException, IOException {
    // loads data into the database
    Connection con = dataSource.getConnection();

    final String schema = convertResourceToString("/jdbc/schema.sql");
    final String data = convertResourceToString("/jdbc/import_test_entries.sql");
    con.prepareStatement(schema).execute();
    con.prepareStatement(data).execute();
  }

  @Test
  @DisplayName( "then Sql schema is correctly mapped to avro schemas" )
  void whenRetrievingJdbcSchema_thenItIsCorrectlyMappedToAvro() throws SQLException {
    // Given
    final int numberOfRowsToFetch = 2;
    jdbcToAvroWorker = new JdbcToAvroWorker(dataSource.getConnection(), QUERY,
      NUMBER_OF_ROWS_TO_FETCH, SCHEMA_NAME, NAMESPACE);
    Set<String> fieldNames = Set.of("id", "first_name", "last_name", "address", "city",
      "telephone");
    jdbcToAvroWorker.produceAvroRecords();

    // When
    var avroSchema = jdbcToAvroWorker.getAvroSchema();

    // Expect
    List<Field> fields = avroSchema.getParsedSchema().getFields();
    assertThat(fields).allMatch(field -> fieldNames.contains(field.name()));
  }

  @Test
  @DisplayName( "Then SQL rows are correctly translated to SQL entries" )
  void whenRetrievingJdbcRecords_thenTheyAreCorrectlyMappedToAvro() throws SQLException {
    // Given
    int expectedCount = 10;  // From test_entries script
    jdbcToAvroWorker = new JdbcToAvroWorker(dataSource.getConnection(), QUERY,
      NUMBER_OF_ROWS_TO_FETCH, SCHEMA_NAME, NAMESPACE);
    Set<String> firstNames = Set.of("George", "Betty", "Eduardo", "Harold", "Peter", "Jean", "Jeff",
      "Maria", "David", "Carlos");
    final String testColumName = "first_name";

    // When
    GenericRecord[] avroRecords = jdbcToAvroWorker.produceAvroRecords();

    // Then
    var nonNullRecords = Arrays.stream(avroRecords).filter(Objects::nonNull)
      .collect(Collectors.toList());
    assertEquals(nonNullRecords.size(), expectedCount);

    assertThat(nonNullRecords).allMatch(
      record -> firstNames.contains((String) record.get(testColumName)));

  }

  @Test
  @DisplayName( "Then it will try fetching only the specified number of rows" )
    // Note: Not all DBs accept fetchSize parameters the same way. It seems that MySQL ignores it.
  void whenRetrievingJdbcRecords_thenItTriesToLimitFetchSize() throws SQLException {
    // Given
    int expectedCount = 2;
    jdbcToAvroWorker = new JdbcToAvroWorker(dataSource.getConnection(), QUERY, expectedCount,
      SCHEMA_NAME, NAMESPACE);

    // When
    GenericRecord[] avroRecords = jdbcToAvroWorker.produceAvroRecords();

    // Then
    assertEquals(avroRecords.length, expectedCount);
  }

}