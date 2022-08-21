package com.marcolotz.db2parquet.adapters;

import com.marcolotz.db2parquet.port.DiskWriter;
import lombok.SneakyThrows;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class NioDiskWriter implements DiskWriter {

    @Override
    @SneakyThrows
    public void write(byte[] content, String path) {
        // Create a new path to your file on the default file system.
        Path filePath = Paths.get(path);
        Files.createFile(filePath);

        // Open a channel in write mode on your file.
        try (WritableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.WRITE)) {
            // Allocate a new buffer - the buffer has the same size as the data for a single dump
            // allocateDirect(), creates the buffer in native memory, i.e., outside the heap.
            // Native memory has the advantage that read and write operations are executed faster.
            // The reason is that the corresponding operating system operations can access this memory area directly,
            // and data does not have to be exchanged between the Java heap and the operating system first.
            // The disadvantage of this method is higher allocation and deallocation costs.
            // This allocation and dealocation costs are amortized by the size of our data and the expected
            // small amount of files.
            ByteBuffer buf = ByteBuffer.allocateDirect(content.length);

            // dumps to buffer
            buf.put(content);

            // Flip from write mode to read mode.
            buf.flip();

            // Write your buffer's data.
            channel.write(buf);
        }
    }
}
