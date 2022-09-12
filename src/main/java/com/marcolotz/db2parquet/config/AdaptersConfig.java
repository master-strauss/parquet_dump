package com.marcolotz.db2parquet.config;

import com.marcolotz.db2parquet.adapter.NioDiskWriter;
import com.marcolotz.db2parquet.adapter.aes128.Aes128Encryptor;
import com.marcolotz.db2parquet.adapter.avro.JdbcToAvroWorker;
import com.marcolotz.db2parquet.adapter.parquet.SimpleParquetSerializer;
import com.marcolotz.db2parquet.core.BaseIngestionService;
import com.marcolotz.db2parquet.core.IngestionCoordinator;
import com.marcolotz.db2parquet.port.DiskWriter;
import com.marcolotz.db2parquet.port.Encryptor;
import com.marcolotz.db2parquet.port.IngestionService;
import com.marcolotz.db2parquet.port.ParquetSerializer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

@Configuration
@EnableConfigurationProperties
public class AdaptersConfig {

  @Bean
  public IngestionService ingestionService(final IngestionCoordinator ingestionCoordinator) {
    return new BaseIngestionService(ingestionCoordinator);
  }

  @Bean
  ParquetSerializer parquetSerializer() {
    // Using 1 GB in memory pages for parquet serializer. This can be changed to a configuration instead if there's
    // need. This is just used to avoid dynamic reallocation of memory
    return new SimpleParquetSerializer(1024 * 1024 * 1024);
  }

  @Bean
  Encryptor encryptor(final Db2ParquetConfigurationProperties db2ParquetConfigurationProperties) {
    return new Aes128Encryptor(db2ParquetConfigurationProperties.getEncryptionKey().getBytes(StandardCharsets.UTF_8));
  }

  @Bean
  DiskWriter diskWriter(final Db2ParquetConfigurationProperties db2ParquetConfigurationProperties) {
    return new NioDiskWriter(db2ParquetConfigurationProperties.getOutputPath());
  }

  @Bean
  JdbcToAvroWorker jdbcToAvroWorker(final DataSource dataSource,
    final Db2ParquetConfigurationProperties db2ParquetConfigurationProperties) throws SQLException {
    return new JdbcToAvroWorker(
      dataSource.getConnection(),
      db2ParquetConfigurationProperties().getQuery(),
      db2ParquetConfigurationProperties.getJdbc().getFetchSizeInRows(),
      db2ParquetConfigurationProperties.getSchemaName(),
      db2ParquetConfigurationProperties.getNamespace());
  }

  @Bean
  IngestionCoordinator ingestionCoordinator(
    final Db2ParquetConfigurationProperties configurationProperties,
    final JdbcToAvroWorker jdbcToAvroWorker,
    final ParquetSerializer parquetSerializer,
    final Encryptor encryptor,
    final DiskWriter diskWriter) {
    return new IngestionCoordinator(configurationProperties, jdbcToAvroWorker, parquetSerializer, encryptor,
      diskWriter);
  }

  @Bean
  @ConfigurationProperties( prefix = "db2parquet" )
  public Db2ParquetConfigurationProperties db2ParquetConfigurationProperties() {
    return new Db2ParquetConfigurationProperties();
  }

  // Design decision:
  // In order to keep it generic on db technology, table schema and to clearly control heap-pollution
  // I decided to implement this directly instead of abstracting (e.g. JdbcTemplate).
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
