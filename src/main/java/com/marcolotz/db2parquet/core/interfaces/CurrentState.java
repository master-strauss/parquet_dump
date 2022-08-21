package com.marcolotz.db2parquet.core.interfaces;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum CurrentState {
    READY_FOR_WORK(0),
    WORKING(1),
    FINISHED(2);

    @Getter private int stageValue;
}
