package com.bd.propagation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * @author Behrouz Derakhshan
 */
public class Constants {
    public static final DoubleWritable DONE_COMPUTING = new DoubleWritable(2.0);
    public static final DoubleWritable ACTIVE = new DoubleWritable(1.0);
    public static final Long SEED_SIZE = 50L;
    public static final LongWritable INFLUENCED = new LongWritable(-10000L);
}
