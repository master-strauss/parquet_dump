package com.marcolotz.db2parquet.port;

import java.sql.ResultSet;

public interface ParquetSerializer {

    byte[] convertToParquet(ResultSet resultSet);

}
