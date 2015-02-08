package com.bd.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Behrouz Derakhshan
 */

public class TopN {

    static class TopNMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        public TopNMapper() {

        }

        Long N;
        List<AbstractMap.SimpleEntry> pairList = new LinkedList<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            N = context.getConfiguration().getLong("seedSize", 40);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Long vertexId = Long.parseLong(value.toString().split("\\t")[0]);
            Float reach = Float.parseFloat(value.toString().split("\\t")[1]);
            pairList = insert(pairList, new AbstractMap.SimpleEntry(vertexId, reach));
            if (pairList.size() > N) {
                pairList.remove(pairList.size() - 1);
            }
        }

        protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
            for (AbstractMap.SimpleEntry p : pairList) {
                context.write(NullWritable.get(), new Text(p.getKey() + "," + p.getValue()));
            }
        }
    }

    static private List<AbstractMap.SimpleEntry> insert(List<AbstractMap.SimpleEntry> pairList, AbstractMap.SimpleEntry pair) {
        for (int i = 0; i < pairList.size(); i++) {
            if (smaller(pairList.get(i), pair)) {
                pairList.add(i, pair);
                return pairList;
            }
        }
        pairList.add(pair);
        return pairList;
    }

    static private Boolean smaller(AbstractMap.SimpleEntry<Long, Float> p1, AbstractMap.SimpleEntry<Long, Float> p2) {
        if (p1.getValue() < p2.getValue()) {
            return true;
        }
        return false;
    }

    static class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        Long N;
        List<AbstractMap.SimpleEntry> pairList = new LinkedList<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            N = context.getConfiguration().getLong("seedSize", 40);
        }

        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                System.out.println(value);
                Long vertexId = Long.parseLong(value.toString().split(",")[0]);
                Float reach = Float.parseFloat(value.toString().split(",")[1]);

                pairList = insert(pairList, new AbstractMap.SimpleEntry<>(vertexId, reach));
                if (pairList.size() > N) {
                    pairList.remove(pairList.size() - 1);
                }

            }
            for (AbstractMap.SimpleEntry p : pairList) {
                context.write(NullWritable.get(), new Text(p.getKey() + "," + p.getValue()));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: TopN <in> <out> <seedSize>");
            System.exit(2);
        }
        conf.setLong("seedSize", Long.parseLong(otherArgs[2]));
        System.out.println("Input Folder : " + otherArgs[0]);
        System.out.println("Output Folder : " + otherArgs[1]);
        System.out.println("SeedSize : " + conf.getLong("seedSize", -1));
        Job job = new Job(conf, "Top N users");
        job.setJarByClass(TopN.class);
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

