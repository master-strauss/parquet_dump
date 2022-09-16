package com.marcolotz.db2parquet.config;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import lombok.Data;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
public class Db2ParquetConfigurationProperties {

  @Min( 1 )
  private int numberOfConcurrentSyncs;
  private JdbcConfigurationProperties jdbc;
  @NotEmpty
  private String outputPath;
  @NotEmpty
  private String encryptionKey;

  @NotEmpty
  private String query;
  @NotEmpty
  private String schemaName;
  @NotEmpty
  private String namespace;
  // Minimum 100 MB - currently not being used
  @Min( 104857600 )
  private long maxFileSizeInBytes;

  @Data
  public static class JdbcConfigurationProperties {

    private String url;
    private String driverClass;
    private String userName;
    private String password;
    private String outputType;

    @Min( 1 )
    private int fetchSizeInRows;
  }
}
