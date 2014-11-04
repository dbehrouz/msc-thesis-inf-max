package com.bd.propagation.function.heuristic;

import com.bd.propagation.Constants;
import com.bd.propagation.function.ic.allpropagation.IndependentCascade;
import com.bd.propagation.function.ic.allpropagation.MaxVertexAggregator;
import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Behrouz Derakhshan
 */
public class Random extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
    private static final String LIST_AGGREGATOR = "random list";

    @Override
    public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex, Iterable<DoubleWritable> messages) throws IOException {
        ArrayWritable seeds = getAggregatedValue(LIST_AGGREGATOR);

    }

    class RandomMasterCompute extends DefaultMasterCompute {
        @Override
        public void initialize() throws IllegalAccessException, InstantiationException {
            registerAggregator(LIST_AGGREGATOR, ListAggregator.class);
        }
    }

    class ListAggregator extends BasicAggregator<ArrayWritable> {
        public final LongConfOption SEED_SIZE =
                new LongConfOption("IC.seedSize", Constants.SEED_SIZE,
                        "Seed size for IC");

        @Override
        public void aggregate(ArrayWritable arrayListWritable) {
            throw new IllegalStateException("This method should not be called");

        }

        @Override
        public ArrayWritable createInitialValue() {
            Long seedSize = SEED_SIZE.get(getConf());
            long totalNumVertices = getTotalNumVertices();
            List<LongWritable> randoms = new LinkedList<>();
            java.util.Random random = new java.util.Random();
            for (int i = 0; i < seedSize; i++) {
                long next = random.nextLong() % totalNumVertices;
                if (randoms.contains(new LongWritable(next))) {
                    i--;
                } else {
                    randoms.add(new LongWritable(next));
                }
            }
            return new ArrayWritable(LongWritable.class, randoms.toArray(new LongWritable[]{}));
        }
    }

}
