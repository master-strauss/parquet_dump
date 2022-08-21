package com.marcolotz.db2parquet.core.events;

import com.lmax.disruptor.EventFactory;
import lombok.Data;

import javax.sql.RowSet;

@Data
public class RowGroupEvent {
    public final static EventFactory EVENT_FACTORY = RowGroupEvent::new;
    private RowSet jdbc_row_set;

}
