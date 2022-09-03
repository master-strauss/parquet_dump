package com.marcolotz.db2parquet.config;

import com.marcolotz.db2parquet.adapters.avro.JdbcToAvroWorker;
import com.marcolotz.db2parquet.core.BaseIngestionService;
import com.marcolotz.db2parquet.core.IngestionCoordinator;
import com.marcolotz.db2parquet.port.DiskWriter;
import com.marcolotz.db2parquet.port.Encryptor;
import com.marcolotz.db2parquet.port.IngestionService;
import com.marcolotz.db2parquet.port.ParquetSerializer;
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
  public IngestionService ingestionService(){//final IngestionCoordinator ingestionCoordinator)
  {
    // TODO: Fix
  }
    return new BaseIngestionService(null);//ingestionCoordinator);
  }

  // TODO: Create Beans
//  @Bean IngestionCoordinator ingestionCoordinator( final Db2ParquetConfigurationProperties configurationProperties,
//  final DataSource dataSource, final JdbcToAvroWorker jdbcToAvroWorker, final ParquetSerializer parquetSerializer, final Encryptor encryptor, final DiskWriter diskWriter)
//  {
//    return new IngestionCoordinator(configurationProperties, dataSource, jdbcToAvroWorker, parquetSerializer,encryptor, diskWriter);
//  }

  @Bean
  @ConfigurationProperties(prefix = "db2parquet")
  public Db2ParquetConfigurationProperties db2ParquetConfigurationProperties() {
    return new Db2ParquetConfigurationProperties();
  }

  // Design decision:
  // In order to keep it generic on db technology, table schema and to clearly control heap-pollution
  // I decided to implement this directly instead of abstracting (e.g. JdbcTemplate).
  @Bean
  DriverManagerDataSource driverManagerDataSource(final Db2ParquetConfigurationProperties configurationProperties)
  {
    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setDriverClassName(configurationProperties.getJdbc().getDriverClass());
    dataSource.setUrl(configurationProperties.getJdbc().getUrl());
    dataSource.setUsername(configurationProperties.getJdbc().getUserName());
    dataSource.setPassword(configurationProperties.getJdbc().getPassword());
    return dataSource;
  }

}
