package com.bd.datatypes;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Behrouz Derakhshan
 */
public class MultiAttemptVertexValue implements Writable {
    private Long[] influenceSize;
    private List<Text> vertexIds;

    public MultiAttemptVertexValue() {
    }

    public MultiAttemptVertexValue(int attempt) {
        this(new Long[attempt], new LinkedList<Text>());
    }

    public MultiAttemptVertexValue(Long[] influenceSize, List<Text> vertexIds) {
        this.influenceSize = influenceSize;
        this.vertexIds = vertexIds;
    }

    public void increment(int index) {
        influenceSize[index]++;
    }

    public void setVertexIds(List<Text> vertexIds) {
        this.vertexIds = vertexIds;
    }

    public void setInfluenceSize(Long[] influenceSize) {
        this.influenceSize = influenceSize;
    }

    public Long[] getInfluenceSize() {
        return influenceSize;
    }

    public List<Text> getVertexIds() {
        return vertexIds;
    }

    public float getAverageSpread() {
        Long sum = 0L;
        for (Long l : influenceSize) {
            sum += l;
        }

        return (float) sum / (float) influenceSize.length;

    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(influenceSize.length);
        for (Long l : influenceSize) {
            dataOutput.writeLong(l);
        }
        dataOutput.writeInt(vertexIds.size());
        for (Text l : vertexIds) {
            l.write(dataOutput);
        }

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        for (int i = 0; i < size; i++) {
            influenceSize[i] = dataInput.readLong();
        }
        size = dataInput.readInt();
        vertexIds = new LinkedList<>();
        for (int i = 0; i < size; i++) {
            Text t = new Text();
            t.readFields(dataInput);
            vertexIds.add(t);
        }
    }

    @Override
    public String toString() {
        return "Average Spread: " + getAverageSpread() + ", influencedBy: " + StringUtils.join(vertexIds, ':');
    }
}
