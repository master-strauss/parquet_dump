package com.marcolotz.db2parquet.port;

import javax.sql.RowSet;

public interface JdbcProducer  {

    RowSet produce();
}
