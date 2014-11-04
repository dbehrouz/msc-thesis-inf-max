package com.bd.propagation.simulation;

import com.bd.propagation.Constants;
import com.bd.propagation.function.ic.NodeType;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

/**
 * @author Behrouz Derakhshan
 *
 *         Given a graph with initial seeds, return spread (number of nodes activated)
 */
public class ICSimulation extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
    public static final String SPREAD = "expected_spread";

    @Override
    public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex, Iterable<DoubleWritable> messages) throws IOException {
        if (vertex.getValue().equals(Constants.DONE_COMPUTING)) {
            vertex.voteToHalt();
        } else if (vertex.getValue().equals(Constants.ACTIVE)) {
            for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
                float weight = edge.getValue().get();
                if (Math.random() < weight) {
                    sendMessage(edge.getTargetVertexId(), Constants.ACTIVE);
                }
            }
            vertex.setValue(Constants.DONE_COMPUTING);
            vertex.voteToHalt();
            getContext().getCounter(NodeType.ACTIVE).increment(1);
            aggregate(SPREAD, new IntWritable(1));
        } else {
            boolean isActivated = false;
            for (DoubleWritable message : messages) {
                if (message.equals(Constants.ACTIVE)) {
                    vertex.setValue(Constants.ACTIVE);
                    isActivated = true;
                }
            }
            if (!isActivated) {
                vertex.voteToHalt();
            }
        }

    }
}
