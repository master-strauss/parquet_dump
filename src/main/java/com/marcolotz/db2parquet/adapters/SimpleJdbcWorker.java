package com.marcolotz.db2parquet.adapters;

import com.marcolotz.db2parquet.core.interfaces.JdbcWorker;
import lombok.extern.log4j.Log4j2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Log4j2
public class SimpleJdbcWorker implements JdbcWorker {

    // Query template - note that they query needs to be provided already in a way to enable pagination.
    // For reference check: https://www.javamadesoeasy.com/2015/11/jdbc-pagination-how-can-we-readfetch.html
    String queryTemplate;
    Connection dbConnection;

    int numberOfRowsToFetch;
    int currentOffset = 0;

    boolean isFinished = false;

    public SimpleJdbcWorker(final Connection dbConnection, final String query, final int numberOfRowsToFetch) throws SQLException {
        currentOffset = 0;
        this.numberOfRowsToFetch = numberOfRowsToFetch;
        this.dbConnection = dbConnection;
        Statement stmt = this.dbConnection.prepareStatement(query,ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public ResultSet produceResultSet() throws SQLException {
        final String query = prepareQueryForNextInterval(queryTemplate, currentOffset, currentOffset + numberOfRowsToFetch);
        log.debug(() -> "Evaluated query is " + query);
        final Statement stmt = dbConnection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        // I am adding +100 to avoid any "smart" driver logic of sending another fetch when the rows are about to end
        stmt.setFetchSize(numberOfRowsToFetch + 100);
        ResultSet rs = stmt.executeQuery(query);
        // Bump offset
        currentOffset = currentOffset + numberOfRowsToFetch;
        if (rs.next())
        {
            isFinished = true;
            rs.close();
            return null;
        }
        else {
            return rs;
        }
    }

    @Override
    public boolean hasFinishedWork() {
        return isFinished;
    }

    private String prepareQueryForNextInterval(String queryTemplate, long startOffset, long endOffset)
    {
        // Poor-men version of templating
        return queryTemplate.replace("BEGIN_OFFSET", Long.toString(startOffset)).replace("END_OFFSET", Long.toString(endOffset));
    }
}
