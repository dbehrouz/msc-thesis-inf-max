package com.bd.propagation.function.ic.allpropagation;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Behrouz Derakhshan
 */
public class VertexMessage implements Writable {
    private long vertexId;
    private long vertexValue;

    public VertexMessage(long vertexId, long vertexValue) {
        this.vertexId = vertexId;
        this.vertexValue = vertexValue;
    }

    public long getVertexId() {
        return vertexId;
    }

    public long getVertexValue() {
        return vertexValue;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(vertexId);
        dataOutput.writeLong(vertexValue);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.vertexId = dataInput.readLong();
        this.vertexValue = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "vertexId: " + vertexId + ", vertexValue: " + vertexValue;
    }

    public static VertexMessage max(VertexMessage first, VertexMessage second) {
        return first.vertexValue > second.vertexValue ? first : second;
    }
}
