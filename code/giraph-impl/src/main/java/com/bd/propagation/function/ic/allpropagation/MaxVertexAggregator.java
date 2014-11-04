package com.bd.propagation.function.ic.allpropagation;

import org.apache.giraph.aggregators.BasicAggregator;

/**
 * @author Behrouz Derakhshan
 */
public class MaxVertexAggregator extends BasicAggregator<VertexMessage> {
    @Override
    public void aggregate(VertexMessage vertexMessage) {
        setAggregatedValue(VertexMessage.max(vertexMessage, getAggregatedValue()));
    }

    @Override
    public VertexMessage createInitialValue() {
        return new VertexMessage(-1000L, Long.MIN_VALUE);
    }
}
