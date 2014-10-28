package com.bd.propagation.ic.singlecycle;

import com.bd.datatypes.ComplexVertexValue;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

/**
 * @author Behrouz Derakhshan
 */
public class AllPropagationIC extends BasicComputation<LongWritable, ComplexVertexValue, FloatWritable, LongWritable> {
    public static final LongWritable INFLUENCED = new LongWritable(-10000);
    public static final LongWritable CONSTANT = new LongWritable(-2000);

    @Override
    public void compute(Vertex<LongWritable, ComplexVertexValue, FloatWritable> vertex, Iterable<LongWritable> messages) throws IOException {
        if (getSuperstep() == 0) {
            // adding my self to the list of influencedBy
            vertex.getValue().getVertexIds().add(vertex.getId().get());
            vertex.setValue(new ComplexVertexValue(1L));
            // at the start try to activate all around you with your label
            activate(vertex, vertex.getId());

        } else {
            for (LongWritable message : messages) {
                // if message is of type INFLUENCED it means
                // another vertex was influenced by this vertex
                // so increment its counter
                if (message.equals(INFLUENCED)) {
                    vertex.getValue().increment();
                } else if (!vertex.getValue().getVertexIds().contains(message.get())) {
                    vertex.getValue().getVertexIds().add(message.get());
                    // activate neighbours using this the message
                    activate(vertex, message);
                    // message is vertex id
                    // here we are informing the initial vertex that we have received your label
                    // so that it can update it's count
                    sendMessage(message, INFLUENCED);
                }
            }
            vertex.voteToHalt();
        }
    }

    private void activate(Vertex<LongWritable, ComplexVertexValue, FloatWritable> vertex, LongWritable message) {
        for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
            LongWritable targetVertex = edge.getTargetVertexId();
            if (targetVertex.get() != message.get()) {
                float weight = edge.getValue().get();
                if (Math.random() < weight) {
                    sendMessage(edge.getTargetVertexId(), message);
                }
            }
        }
    }
}
