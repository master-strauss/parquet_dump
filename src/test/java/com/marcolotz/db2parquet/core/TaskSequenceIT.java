package com.marcolotz.db2parquet.core;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.marcolotz.db2parquet.adapter.JdbcProducer;
import com.marcolotz.db2parquet.adapter.NioDiskWriter;
import com.marcolotz.db2parquet.adapter.aes128.Aes128Encryptor;
import com.marcolotz.db2parquet.adapter.avro.JdbcToAvroWorker;
import com.marcolotz.db2parquet.adapter.parquet.SimpleParquetSerializer;
import com.marcolotz.db2parquet.config.Db2ParquetConfigurationProperties;
import com.marcolotz.db2parquet.core.TaskSequenceIT.TaskSequenceITConfiguration;
import com.marcolotz.db2parquet.port.DiskWriter;
import com.marcolotz.db2parquet.port.Encryptor;
import com.marcolotz.db2parquet.port.ParquetSerializer;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  "db2parquet.jdbc.fetch-size-in-rows=1",
}, webEnvironment = WebEnvironment.NONE, classes = ApplicationIT.class )
@Import( TaskSequenceITConfiguration.class )
@DisplayName( "When deploying a processing task" )
@DirtiesContext( classMode = ClassMode.AFTER_CLASS )
class TaskSequenceIT {

  private final Db2ParquetConfigurationProperties configurationProperties;
  private final Encryptor encryptor;
  private final ParquetSerializer parquetSerializer;
  private final DataSource dataSource;
  @TempDir
  Path directory;
  private JdbcToAvroWorker jdbcToAvroWorker;
  private TaskSequence taskSequence;
  private DiskWriter diskWriter;
  private JdbcProducer jdbcProducer;

  @Autowired
  public TaskSequenceIT(Db2ParquetConfigurationProperties configurationProperties, DataSource dataSource) {
    this.configurationProperties = configurationProperties;
    this.parquetSerializer = spy(new SimpleParquetSerializer(1024 * 1024 * 64));
    this.encryptor = spy(new Aes128Encryptor("AnyEncryptionKey".getBytes()));
    this.dataSource = dataSource;
  }

  private static String convertResourceToString(final String resourceFilePath) throws IOException {
    ClassPathResource resource = new ClassPathResource(resourceFilePath);
    InputStream inputStream = resource.getInputStream();
    return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
  }

  @BeforeEach
  void setup() throws SQLException, IOException {
    prepareDatabase(dataSource);
    this.jdbcToAvroWorker = spy(new JdbcToAvroWorker(
      dataSource.getConnection(),
      configurationProperties.getQuery(),
      configurationProperties.getJdbc().getFetchSizeInRows(),
      configurationProperties.getSchemaName(),
      configurationProperties.getNamespace()));

    this.jdbcProducer = spy(new JdbcProducer(jdbcToAvroWorker));
    this.diskWriter = spy(new NioDiskWriter(directory.toAbsolutePath().toString()));
    reset(parquetSerializer, encryptor, diskWriter, jdbcProducer);
    this.taskSequence = new TaskSequence(jdbcProducer, parquetSerializer, encryptor, diskWriter);
  }

  private void prepareDatabase(DataSource dataSource) throws SQLException, IOException {
    // loads data into the database
    Connection con = dataSource.getConnection();

    final String schema = convertResourceToString("/jdbc/schema.sql");
    final String data = convertResourceToString("/jdbc/import_test_entries.sql");
    con.prepareStatement(schema).execute();
    con.prepareStatement(data).execute();
  }

  @DisplayName( "Then jdbc records will be converted to Parquet, encrypted and written to disk" )
  @Test
  @Timeout( 10 )
  void whenIngestingData_thenItWillBePersistedToDisk()
    throws SQLException {
    // Given
    doReturn(directory.toAbsolutePath().toString()).when(configurationProperties).getOutputPath();
    doReturn("SELECT * FROM owners").when(configurationProperties).getQuery();

    // When
    jdbcProducer.run();

    // Then
    taskSequence.waitForCompletion();

    verify(jdbcToAvroWorker, atLeast(1)).produceAvroRecords();
    verify(parquetSerializer, atLeast(1)).convertToParquet(any(), any());
    verify(encryptor, atLeast(1)).encrypt(any());
    verify(diskWriter, atLeast(1)).write(any());

    var outputFiles = Stream.of(Objects.requireNonNull(new File(directory.toAbsolutePath().toString()).listFiles()))
      .filter(file -> !file.isDirectory())
      .map(File::toPath)
      .collect(Collectors.toSet());
    assertThat(outputFiles).isNotEmpty();
    assertThat(outputFiles.stream()).allMatch(this::filesAreNotEmpty);
  }

  @DisplayName( "Then the task finishes only after run is triggered" )
  @Test
  @Timeout( 10 )
  void whenIngestingData_canOnlyFinishAfterExecution() {
    // Given
    doReturn(directory.toAbsolutePath().toString()).when(configurationProperties).getOutputPath();
    doReturn("SELECT * FROM owners").when(configurationProperties).getQuery();

    // Expect
    assertFalse(jdbcProducer.hasFinished());

    // When
    jdbcProducer.run();

    // Then
    taskSequence.waitForCompletion();
    assertTrue(taskSequence.isFinished());
  }

  @DisplayName( "Then tasks are independent" )
  @Test
  @Timeout( 10 )
  void whenIngestingData_thenTwoTasksAreIndependent() throws ExecutionException, InterruptedException {

    // Given
    ParquetSerializer idleSerializer = mock(ParquetSerializer.class);
    doAnswer(a -> {
      while (true) {
      }
    }).when(idleSerializer).convertToParquet(any(), any());
    TaskSequence taskSequence1 = new TaskSequence(jdbcProducer, parquetSerializer, encryptor, diskWriter);
    TaskSequence taskSequence2 = new TaskSequence(jdbcProducer, idleSerializer, encryptor, diskWriter);

    // When
    CompletableFuture<Void> future = jdbcProducer.run();

    // Then
    future.get();
    taskSequence1.waitForCompletion();

    assertTrue(taskSequence1.isFinished());
    assertFalse(taskSequence2.isFinished());
  }


  // TODO: Change to Sneakythrows from lombok
  private boolean filesAreNotEmpty(final Path path) {
    try {
      byte[] data = Files.readAllBytes(path);
      return data.length != 0;
    } catch (Exception e) {
      throw new RuntimeException("file not found");
    }
  }

  @TestConfiguration
  @EnableConfigurationProperties
  public static class TaskSequenceITConfiguration {

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