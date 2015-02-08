package com.bd.datatypes;

import com.bd.propagation.function.ic.singlecycle.MultiAttemptSingleCycle;
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
    private Long[] influenceSize = new Long[MultiAttemptSingleCycle.ATTEMPTS];
    private List<Text> vertexIds = new LinkedList<>();

    public MultiAttemptVertexValue() {
    }

    public MultiAttemptVertexValue(int attempt) {
        this(new Long[attempt], new LinkedList<Text>());
    }

    public MultiAttemptVertexValue(Long[] influenceSize, List<Text> vertexIds) {
        this.influenceSize = influenceSize;
        java.util.Arrays.fill(influenceSize, 0L);
        this.vertexIds = vertexIds;
    }

    public void increment(int index) {
        influenceSize[index] = influenceSize[index] + 1;
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

    public Float getAverageSpread() {
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
            //System.out.println("WRITE: "  + l);
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
            Long val = dataInput.readLong();
            //System.out.println("READ: "  + val);
            influenceSize[i] = val == null ? 0 : val;
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
        return getAverageSpread().toString();
    }

    public String toFullString() {
        return "Average Spread: " + getAverageSpread() + ", influencedBy: " + StringUtils.join(vertexIds, ':');

    }
}
