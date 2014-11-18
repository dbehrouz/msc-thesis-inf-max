package com.bd.datatypes;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Behrouz Derakhshan
 */
public class SingleAttemptVertexValue implements Writable {

    private Long influenceSize;
    private List<Long> vertexIds;

    public SingleAttemptVertexValue() {
        this(0L);
    }

    public SingleAttemptVertexValue(Long influenceSize) {
        this(influenceSize, new LinkedList<Long>());
    }

    public SingleAttemptVertexValue(Long influenceSize, List<Long> vertexIds) {
        this.influenceSize = influenceSize;
        this.vertexIds = vertexIds;
    }

    public void increment() {
        influenceSize++;
    }

    public void setVertexIds(List<Long> vertexIds) {
        this.vertexIds = vertexIds;
    }

    public void setInfluenceSize(Long influenceSize) {
        this.influenceSize = influenceSize;
    }

    public Long getInfluenceSize() {
        return influenceSize;
    }

    public List<Long> getVertexIds() {
        return vertexIds;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(influenceSize);
        dataOutput.writeInt(vertexIds.size());
        for (Long l : vertexIds) {
            dataOutput.writeLong(l);
        }

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        influenceSize = dataInput.readLong();
        int size = dataInput.readInt();
        vertexIds = new LinkedList<>();
        for (int i = 0; i < size; i++) {
            vertexIds.add(dataInput.readLong());
        }
    }

    @Override
    public String toString() {
        return "Size: " + getInfluenceSize() + ", influencedBy: " + StringUtils.join(vertexIds, ':');
    }
}
