package com.marcolotz.db2parquet.adapter.parquet;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import org.apache.parquet.io.DelegatingPositionOutputStream;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/***
 * Avro to Parquet is designed to write to FS instead of in-memory - this implementation forces a serialization
 * into memory instead. Similar to
 * https://stackoverflow.com/questions/39631812/q-converting-avro-to-parquet-in-memory
 */
public class InMemoryOutputFile implements OutputFile {

  // Ideally this should be 1GB, but this puts stress in the machine loading tests
  private static final int BUFFER_512_MB = 1024 * 1024 * 1024;

  private final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(
    BUFFER_512_MB);

  @Override
  public PositionOutputStream create(long blockSizeHint) { // Mode.CREATE calls this method
    return new InMemoryPositionOutputStream(byteArrayOutputStream);
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) {
    return null;
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return 0;
  }

  public byte[] toArray() {
    return byteArrayOutputStream.toByteArray();
  }

  private static class InMemoryPositionOutputStream extends DelegatingPositionOutputStream {

    public InMemoryPositionOutputStream(OutputStream outputStream) {
      super(outputStream);
    }

    @Override
    public long getPos() {
      return ((ByteArrayOutputStream) this.getStream()).size();
    }
  }
}