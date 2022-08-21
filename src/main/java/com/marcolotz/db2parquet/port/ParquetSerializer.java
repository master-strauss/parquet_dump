package com.marcolotz.db2parquet.port;

import javax.sql.RowSet;
import java.sql.ResultSet;

public interface ParquetSerializer {

    void convertToParquet(ResultSet resultSet);

}
