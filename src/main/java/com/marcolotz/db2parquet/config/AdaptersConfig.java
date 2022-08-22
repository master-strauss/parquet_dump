package com.marcolotz.db2parquet.config;

import com.marcolotz.db2parquet.core.BaseIngestionService;
import com.marcolotz.db2parquet.core.IngestionCoordinator;
import com.marcolotz.db2parquet.port.IngestionService;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

@Configuration
@EnableConfigurationProperties
public class AdaptersConfig {

  @Bean
  public IngestionService ingestionService(final IngestionCoordinator ingestionCoordinator)
  {
    return new BaseIngestionService(ingestionCoordinator);
  }

  @Bean IngestionCoordinator ingestionCoordinator(final Db2ParquetConfigurationProperties configurationProperties)
  {
    return new IngestionCoordinator(null, configurationProperties);
  }

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
