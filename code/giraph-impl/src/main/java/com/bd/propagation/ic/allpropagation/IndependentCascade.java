package com.bd.propagation.ic.allpropagation;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

/**
 * @author Behrouz Derakhshan
 */
public class IndependentCascade extends
        BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
    @Override
    public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex, Iterable<DoubleWritable> messages) throws IOException {


    }
}
