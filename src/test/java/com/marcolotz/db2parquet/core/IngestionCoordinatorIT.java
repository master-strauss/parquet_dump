package com.marcolotz.db2parquet.core;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.marcolotz.db2parquet.adapter.NioDiskWriter;
import com.marcolotz.db2parquet.adapter.aes128.Aes128Encryptor;
import com.marcolotz.db2parquet.adapter.avro.JdbcToAvroWorker;
import com.marcolotz.db2parquet.adapter.parquet.SimpleParquetSerializer;
import com.marcolotz.db2parquet.config.Db2ParquetConfigurationProperties;
import com.marcolotz.db2parquet.core.IngestionCoordinatorIT.IngestionCoordinatorITConfiguration;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;

@SpringBootTest( properties = {
  "db2parquet.jdbc.fetch-size-in-rows=1"
}, webEnvironment = WebEnvironment.NONE, classes = ApplicationIT.class )
@Import( IngestionCoordinatorITConfiguration.class )
@DisplayName( "When triggering an ingestion" )
@DirtiesContext( classMode = ClassMode.AFTER_CLASS )
class IngestionCoordinatorIT {

  private final Db2ParquetConfigurationProperties configurationProperties;
  private final JdbcToAvroWorker jdbcToAvroWorker;
  private final DataSource dataSource;

  @TempDir
  Path directory;

  IngestionCoordinator coordinator;

  @Autowired
  public IngestionCoordinatorIT(Db2ParquetConfigurationProperties configurationProperties,
    JdbcToAvroWorker jdbcToAvroWorker, DataSource dataSource) {
    this.configurationProperties = configurationProperties;
    this.jdbcToAvroWorker = jdbcToAvroWorker;
    this.dataSource = dataSource;
  }

  private static String convertResourceToString(final String resourceFilePath) throws IOException {
    ClassPathResource resource = new ClassPathResource(resourceFilePath);
    InputStream inputStream = resource.getInputStream();
    return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
  }

  @BeforeEach
  void before() throws SQLException, IOException {
    coordinator = new IngestionCoordinator(configurationProperties, jdbcToAvroWorker,
      new SimpleParquetSerializer(1024 * 1024 * 100), new Aes128Encryptor("AnyEncryptionKey".getBytes()),
      new NioDiskWriter(directory.toAbsolutePath().toString()));
    prepareDatabase(dataSource);
  }

  @Test
  @DisplayName( "Then ingestion should trigger avro record consumption" )
  @Timeout( value = 5 )
  void whenIngestionIsTriggered_thenItShouldProduceAvroRecords() throws SQLException {
    // Given
    doReturn(1).when(configurationProperties).getNumberOfConcurrentSyncs();
    doReturn(directory.toAbsolutePath().toString()).when(configurationProperties).getOutputPath();
    doReturn("SELECT * FROM owners").when(configurationProperties).getQuery();

    // When
    coordinator.ingest();

    // Then
    verify(jdbcToAvroWorker, atLeast(1)).produceAvroRecords();
  }

  @Test
  @DisplayName( "Then all records should be consumed" )
  @Timeout( value = 5 )
  void whenIngestionIsTriggered_thenItWillConsumerAllDatabaseRows() throws SQLException {
    // Given
    doReturn(1).when(configurationProperties).getNumberOfConcurrentSyncs();
    doReturn(directory.toAbsolutePath().toString()).when(configurationProperties).getOutputPath();
    doReturn("SELECT * FROM owners").when(configurationProperties).getQuery();

    // When
    coordinator.ingest();

    // Then
    // There are exactly 10 owners in the test data
    verify(jdbcToAvroWorker, atLeast(10)).produceAvroRecords();
    verify(jdbcToAvroWorker, atMost(11)).produceAvroRecords();
  }

  @Test
  @DisplayName( "Then ingestion should verify conclusion on all tasks" )
  @Timeout( value = 5 )
  void whenIngestionIsTriggered_thenItShouldProduceVerifyConclusionOnAllTasks() {
    // Given
    final int parallelism_factor = 4;
    doReturn(parallelism_factor).when(configurationProperties).getNumberOfConcurrentSyncs();
    doReturn(directory.toAbsolutePath().toString()).when(configurationProperties).getOutputPath();
    doReturn("SELECT * FROM owners").when(configurationProperties).getQuery();

    // When
    coordinator.ingest();

    // Then
    verify(jdbcToAvroWorker, atLeast(parallelism_factor)).hasFinishedWork();
  }

  private void prepareDatabase(DataSource dataSource) throws SQLException, IOException {
    // loads data into the database
    Connection con = dataSource.getConnection();

    final String schema = convertResourceToString("/jdbc/schema.sql");
    final String data = convertResourceToString("/jdbc/import_test_entries.sql");
    con.prepareStatement(schema).execute();
    con.prepareStatement(data).execute();
  }

  @TestConfiguration
  @EnableConfigurationProperties
  public static class IngestionCoordinatorITConfiguration {

    @Bean
    JdbcToAvroWorker jdbcToAvroWorker(final DataSource dataSource,
      final Db2ParquetConfigurationProperties db2ParquetConfigurationProperties) throws SQLException {
      return spy(new JdbcToAvroWorker(
        dataSource.getConnection(),
        db2ParquetConfigurationProperties().getQuery(),
        db2ParquetConfigurationProperties.getJdbc().getFetchSizeInRows(),
        db2ParquetConfigurationProperties.getSchemaName(),
        db2ParquetConfigurationProperties.getNamespace()));
    }

    @Bean
    @ConfigurationProperties( prefix = "db2parquet" )
    Db2ParquetConfigurationProperties db2ParquetConfigurationProperties() {
      return spy(new Db2ParquetConfigurationProperties());
    }

    @Bean
    DriverManagerDataSource driverManagerDataSource(
      final Db2ParquetConfigurationProperties configurationProperties) {
      DriverManagerDataSource dataSource = new DriverManagerDataSource();
      dataSource.setDriverClassName(configurationProperties.getJdbc().getDriverClass());
      dataSource.setUrl(configurationProperties.getJdbc().getUrl());
      dataSource.setUsername(configurationProperties.getJdbc().getUserName());
      dataSource.setPassword(configurationProperties.getJdbc().getPassword());
      return dataSource;
    }

  }
}