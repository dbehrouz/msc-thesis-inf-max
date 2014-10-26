package com.bd.propagation.ic;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

/**
 * One iteration of IC algorithm.
 * High level description :
 * 1. Given an initial graph with some nodes having status as ACTIVE
 * 2. In each superstep :
 * a. If a node is ACTIVE :
 * i. Based on the outgoing edge probability try to activate the next node
 * b. Mark the node as COMPLETE so it cannot activate in the next round (by setting the value to a special number)
 * c. Continue until all nodes votes to halts
 *
 * everything with label ACTIVE or DONE_COMPUTING is the final result
 *
 * @author Behrouz Derakhshan
 */
public class OneIterationIndependentCascade extends
        BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
    private static final DoubleWritable DONE_COMPUTING = new DoubleWritable(2.0);
    private static final DoubleWritable ACTIVE = new DoubleWritable(1.0);

    @Override
    public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
                        Iterable<DoubleWritable> messages) throws IOException {
        if (vertex.getValue().equals(DONE_COMPUTING)) {
            vertex.voteToHalt();
        } else if (vertex.getValue().equals(ACTIVE)) {
            for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
                float weight = edge.getValue().get();
                if (Math.random() < weight) {
                    sendMessage(edge.getTargetVertexId(), ACTIVE);
                }
            }
            vertex.setValue(DONE_COMPUTING);
            vertex.voteToHalt();
            getContext().getCounter(NodeType.ACTIVE).increment(1);
        } else {
            boolean isActivated = false;
            for (DoubleWritable message : messages) {
                if (message.equals(ACTIVE)) {
                    vertex.setValue(ACTIVE);
                    isActivated = true;
                }
            }
            if (!isActivated) {
                vertex.voteToHalt();
            }
        }
    }
}
