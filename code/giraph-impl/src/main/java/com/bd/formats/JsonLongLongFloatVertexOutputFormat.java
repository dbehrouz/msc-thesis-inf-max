package com.bd.formats;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;

/**
 * Exactly same format as @see com.bd.formats.JsonLongLongFloatVertexInputFormat
 *
 * @author Behrouz Derakhshan
 */
public class JsonLongLongFloatVertexOutputFormat extends
        TextVertexOutputFormat<LongWritable, LongWritable,
                FloatWritable> {

    @Override
    public TextVertexWriter createVertexWriter(
            TaskAttemptContext context) {
        return new JsonLongLongFloatVertexWriter();
    }


    private class JsonLongLongFloatVertexWriter extends
            TextVertexWriterToEachLine {
        @Override
        public Text convertVertexToLine(
                Vertex<LongWritable, LongWritable, FloatWritable> vertex
        ) throws IOException {
            JSONArray jsonVertex = new JSONArray();
            try {
                jsonVertex.put(vertex.getId().get());
                jsonVertex.put(vertex.getValue().get());
                JSONArray jsonEdgeArray = new JSONArray();
                for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
                    JSONArray jsonEdge = new JSONArray();
                    jsonEdge.put(edge.getTargetVertexId().get());
                    jsonEdge.put(edge.getValue().get());
                    jsonEdgeArray.put(jsonEdge);
                }
                jsonVertex.put(jsonEdgeArray);
            } catch (JSONException e) {
                throw new IllegalArgumentException(
                        "writeVertex: Couldn't write vertex " + vertex);
            }
            return new Text(jsonVertex.toString());
        }
    }
}
