package com.marcolotz.db2parquet.core.interfaces;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface JdbcWorker {
    ResultSet produceResultSet() throws SQLException;

    boolean hasFinishedWork();

}
