package com.marcolotz.db2parquet.config;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import lombok.Data;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
public class Db2ParquetConfigurationProperties {

  @Min(1)
  private int numberOfConcurrentSyncs;
  private JdbcConfigurationProperties   jdbc;
  @NotEmpty
  private String outputPath;

  @Data
  public static class JdbcConfigurationProperties{
    private String url;
    private String driverClass;
    private String userName;
    private String password;
    private String outputType;

    @Min(10)
    private int fetchSizeInRows;
    // Minimum 100 MB - currently not being used
    @Min(104857600)
    private long maxFileSizeInBytes;
  }
}
