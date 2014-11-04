package com.bd.propagation.function;

import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Behrouz Derakhshan
 */
public class SimpleLabelPropagation extends
        BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    public static final LongConfOption SOURCE_ID =
            new LongConfOption("SimpleLabelPropagation.sourceId", 0,
                    "The shortest paths id");
    /**
     * Class logger
     */
    private static final Logger LOG =
            Logger.getLogger(SimpleLabelPropagation.class);

    /**
     * Is this vertex the source id?
     *
     * @param vertex Vertex
     * @return True if the source id
     */
    private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
        return vertex.getId().get() == SOURCE_ID.get(getConf());
    }

    @Override
    public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
                        Iterable<DoubleWritable> messages) throws IOException {
        if (getSuperstep() == 0) {
            if (vertex.getValue().get() == 1) {
                for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
                    sendMessage(edge.getTargetVertexId(), new DoubleWritable(vertex.getValue().get()));
                }
            }
        } else if (vertex.getValue().get() == 0) {
            for (DoubleWritable message : messages) {
                if (message.get() != 0) {
                    vertex.setValue(message);
                    for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
                        sendMessage(edge.getTargetVertexId(), new DoubleWritable(vertex.getValue().get()));
                    }
                    break;
                }
            }

        }
        vertex.voteToHalt();
    }

    private boolean isValue(Vertex<LongWritable, Text, LongWritable> vertex, String value) {
        return vertex.getValue().toString().equals(value);
    }
}
