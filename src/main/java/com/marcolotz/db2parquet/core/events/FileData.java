package com.marcolotz.db2parquet.core.events;

import lombok.Value;

@Value
public
class FileData {

        String fileName;
        byte[] contents;
}
