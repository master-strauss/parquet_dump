//package com.marcolotz.db2parquet.core;
//
//import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
//import static org.junit.jupiter.api.Assertions.assertFalse;
//import static org.junit.jupiter.api.Assertions.assertTrue;
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.Mockito.atLeast;
//import static org.mockito.Mockito.doAnswer;
//import static org.mockito.Mockito.doReturn;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.spy;
//import static org.mockito.Mockito.verify;
//
//import com.marcolotz.db2parquet.adapter.JdbcProducer;
//import com.marcolotz.db2parquet.adapter.avro.JdbcToAvroWorker;
//import com.marcolotz.db2parquet.config.Db2ParquetConfigurationProperties;
//import com.marcolotz.db2parquet.port.DiskWriter;
//import com.marcolotz.db2parquet.port.Encryptor;
//import com.marcolotz.db2parquet.port.ParquetSerializer;
//import java.io.File;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.sql.SQLException;
//import java.util.Objects;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.TimeUnit;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//import org.assertj.core.api.AssertionsForInterfaceTypes;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.DisplayName;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.io.TempDir;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.context.SpringBootTest;
//
//@SpringBootTest
//@DisplayName( "When handling tasks" )
//class TaskSequenceIT {
//
//  public static final int TIMEOUT_IN_SECONDS = 5;
//  JdbcProducer jdbcProducer;
//  Db2ParquetConfigurationProperties configurationProperties;
//  JdbcToAvroWorker jdbcToAvroWorker;
//  ParquetSerializer parquetSerializer;
//  Encryptor encryptor;
//  DiskWriter diskWriter;
//
//  TaskSequence taskSequence;
//
//  @TempDir
//  Path directory;
//
//  @Autowired
//  TaskSequenceIT(Db2ParquetConfigurationProperties configurationProperties, JdbcToAvroWorker jdbcToAvroWorker,
//    ParquetSerializer parquetSerializer, Encryptor encryptor, DiskWriter diskWriter) {
//    configurationProperties.setJdbc(spy(configurationProperties.getJdbc()));
//    this.configurationProperties = spy(configurationProperties);
//    this.jdbcToAvroWorker = spy(jdbcToAvroWorker);
//    this.parquetSerializer = parquetSerializer;
//    this.encryptor = encryptor;
//    this.diskWriter = diskWriter;
//    this.jdbcProducer = new JdbcProducer(jdbcToAvroWorker);
//
//  }
//
//  @BeforeEach
//  void setup() {
//    taskSequence = new TaskSequence(jdbcProducer, parquetSerializer, encryptor, diskWriter);
//  }
//
//  @DisplayName( "Then jdbc records will be converted to Parquet, encrypted and written to disk" )
//  @Test
//  void whenIngestingData_thenItWillBePersistedToDisk() throws SQLException {
//    // Given
//    doReturn(directory.toAbsolutePath().toString()).when(configurationProperties).getOutputPath();
//    doReturn(1).when(configurationProperties.getJdbc()).getFetchSizeInRows();
//    doReturn("SELECT * FROM owners").when(configurationProperties).getQuery();
//
//    // When
//    CompletableFuture<Void> runningIngestion = jdbcProducer.run();
//
//    // Then
//    assertThat(runningIngestion.orTimeout(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS))
//      .isCompleted();
//
//    // TODO: Change this to the number of messages
//    verify(jdbcToAvroWorker, atLeast(1)).produceAvroRecords();
//    verify(parquetSerializer, atLeast(1)).convertToParquet(any(), any());
//    verify(encryptor, atLeast(1)).encrypt(any());
//    verify(diskWriter, atLeast(1)).write(any());
//
//    var outputFiles = Stream.of(Objects.requireNonNull(new File(directory.toAbsolutePath().toString()).listFiles()))
//      .filter(file -> !file.isDirectory())
//      .map(File::toPath)
//      .collect(Collectors.toSet());
//    assertThat(outputFiles.stream()).allMatch(this::filesAreNotEmpty);
//  }
//
//  @DisplayName( "Then the task finishes only after run is triggered" )
//  @Test
//  void whenIngestingData_canOnlyFinishAfterExecution() {
//    // Given
//    doReturn(directory.toAbsolutePath().toString()).when(configurationProperties).getOutputPath();
//    doReturn(1).when(configurationProperties.getJdbc()).getFetchSizeInRows();
//    doReturn("SELECT * FROM owners").when(configurationProperties).getQuery();
//
//    // Expect
//    assertFalse(jdbcProducer.hasFinished());
//
//    // When
//    CompletableFuture<Void> runningIngestion = jdbcProducer.run();
//
//    // Then
//    assertThat(runningIngestion.orTimeout(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS))
//      .isCompleted();
//    assertTrue(taskSequence.isFinished());
//  }
//
//  @DisplayName( "Then tasks are independent" )
//  @Test
//  void whenIngestingData_thenTwoTasksAreIndependent() {
//
//    // Given
//    ParquetSerializer idleSerializer = mock(ParquetSerializer.class);
//    CompletableFuture blockedFuture = CompletableFuture.runAsync(() -> {
//      while (true) {
//      }
//    });
//    doAnswer((a) -> blockedFuture.get()).when(idleSerializer).convertToParquet(any(), any());
//    TaskSequence taskSequence1 = new TaskSequence(jdbcProducer, parquetSerializer, encryptor, diskWriter);
//    TaskSequence taskSequence2 = new TaskSequence(jdbcProducer, mock(ParquetSerializer.class), encryptor, diskWriter);
//
//    // When
//    jdbcProducer.run();
//
//    // When
//    CompletableFuture<Void> runningIngestion = jdbcProducer.run();
//
//    // Then
//    assertThat(runningIngestion.orTimeout(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS))
//      .isCompleted();
//    // TODO: This will be a race condition, better to setup a timeout here
//    //assertThat(CompletableFuture.supplyAsync(taskSequence1::isFinished).orTimeout(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS));
//    assertTrue(taskSequence1.isFinished());
//    assertFalse(taskSequence2.isFinished());
//
//    // Terminate the completable future
//    blockedFuture.cancel(true);
//  }
//
//
//  // TODO: Change to Sneakythrows from lombok
//  private boolean filesAreNotEmpty(final Path path) {
//    try {
//      byte[] data = Files.readAllBytes(path);
//      return data.length != 0;
//    } catch (Exception e) {
//      throw new RuntimeException("file not found");
//    }
//  }
//
//}