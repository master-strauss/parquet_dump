package com.marcolotz.db2parquet.adapter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.marcolotz.db2parquet.core.events.FileData;
import com.marcolotz.db2parquet.port.DiskWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName( "When writing to a file" )
class NioDiskWriterTest {

  @TempDir
  Path directory;

  @Test
  @DisplayName( "then the data should be written to a file" )
  void whenWritingToAFile_thenContentsShouldBePersisted() throws IOException {
    // Given
    DiskWriter diskWriter = new NioDiskWriter(directory.toString());
    final String test = "thisIs_aTest_String";
    final String fileName = "test.dump";
    final byte[] dump = test.getBytes();
    final String outputPath = directory.toAbsolutePath() + FileSystems.getDefault().getSeparator() + fileName;
    final FileData fileData = new FileData(fileName, dump);
    Path outputFilePath = Paths.get(outputPath);

    // When
    diskWriter.write(fileData);

    // Then
    byte[] data = Files.readAllBytes(outputFilePath);
    assertEquals(test, new String(data));
  }

}