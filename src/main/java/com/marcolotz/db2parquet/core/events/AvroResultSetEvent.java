package com.marcolotz.db2parquet.core.events;

import com.lmax.disruptor.EventFactory;
import lombok.Data;

import javax.sql.RowSet;
import java.sql.ResultSet;

@Data
public class ResultSetEvent {
    public final static EventFactory EVENT_FACTORY = ResultSetEvent::new;
    private ResultSet resultSet;

}
