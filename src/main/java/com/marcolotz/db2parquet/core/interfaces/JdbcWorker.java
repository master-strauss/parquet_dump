package com.marcolotz.db2parquet.core.interfaces;

public interface JdbcWorker {

    void consumerFromJdbc();

    void retrieveSchema();

    void hasFinishedWork();

}
