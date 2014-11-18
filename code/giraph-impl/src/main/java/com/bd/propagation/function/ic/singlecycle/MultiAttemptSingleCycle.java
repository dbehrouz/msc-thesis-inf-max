package com.bd.propagation.function.ic.singlecycle;

import com.bd.datatypes.MultiAttemptVertexValue;
import com.bd.propagation.Constants;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Similar to
 *
 * @author Behrouz Derakhshan
 * @see com.bd.propagation.function.ic.singlecycle.SimpleSingleCycle
 * With the exception that in each activition trial multiple attempts are made and the result
 * is averaged for all the attemps
 */
public class MultiAttemptSingleCycle extends BasicComputation<LongWritable, MultiAttemptVertexValue, FloatWritable, Text> {
    public static final int attempt = 100;
    public static final Text INFLUENCED = new Text("INFLUENCED");

    @Override
    public void compute(Vertex<LongWritable, MultiAttemptVertexValue, FloatWritable> vertex,
                        Iterable<Text> messages) throws IOException {
        if (getSuperstep() == 0) {

            // adding my self to the list of influencedBy
            vertex.setValue(new MultiAttemptVertexValue(attempt));

            // at the start try to activate all around you with your label
            for (int i = 0; i < attempt; i++) {
                vertex.getValue().getVertexIds().add(new Text(vertex.getId().get() + "_" + i));
                activate(vertex, vertex.getId().get() + "_" + i);
            }

        } else {
            for (Text message : messages) {
                Long vertexId = getVertex(message.toString());
                int index = getIndex(message.toString());
                // if message is of type INFLUENCED it means
                // another vertex was influenced by this vertex
                // so increment its counter
                if (message.equals(INFLUENCED)) {
                    vertex.getValue().increment(index);
                } else if (!vertex.getValue().getVertexIds().contains(message)) {
                    vertex.getValue().getVertexIds().add(message);
                    // activate neighbours using this the message
                    activate(vertex, message.toString());
                    // message is vertex id
                    // here we are informing the initial vertex that we have received your label
                    // so that it can update it's count
                    sendMessage(new LongWritable(vertexId), new Text(INFLUENCED.toString() + "_" + index));
                }
            }
            vertex.voteToHalt();
        }
    }

    private Long getVertex(String message) {
        return Long.parseLong(message.split("_")[0]);
    }

    private int getIndex(String message) {
        return Integer.parseInt(message.split("_")[1]);
    }

    private void activate(Vertex<LongWritable, MultiAttemptVertexValue, FloatWritable> vertex, String message) {
        for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
            LongWritable targetVertex = edge.getTargetVertexId();
            if (targetVertex.toString().equals(message)) {
                float weight = edge.getValue().get();
                if (Math.random() < weight) {
                    sendMessage(edge.getTargetVertexId(), new Text(message));
                }
            }
        }
    }


}
