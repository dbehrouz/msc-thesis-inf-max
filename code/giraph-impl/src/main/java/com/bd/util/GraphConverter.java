package com.bd.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Converts the graph downloaded from http://research.microsoft.com/en-us/people/weic/graphdata.zip
 * to Giraph friendly format.
 * The data in the original file is of the format :
 * v1 v2
 * Repeated lines means existence of multiple edges between two nodes,
 * Instead of using multiple edges between two nodes, we can define an integer weight for them
 * and increment it each time and edge is defined
 *
 * @author Behrouz Derakhshan
 */
public class GraphConverter {
    public static void main(String[] args) throws IOException {
        String file = args[0];
        String outputFile = args[1];
        List<String> graph = FileUtils.readLines(new File(file));
        graph.remove(0);
        Map<Long, Vertex> vertices = new HashMap<>();
        for (String line : graph) {
            String[] split = StringUtils.split(line);
            Long v1 = Long.parseLong(split[0]);
            Long v2 = Long.parseLong(split[1]);

            // first vertex
            Vertex vertex = vertices.get(v1);
            if (vertex == null) {
                vertex = new Vertex(v1);
                vertex.addEdge(v2);
                vertices.put(v1, vertex);
            } else {
                vertex.addEdge(v2);

            }

            // second vertex
            Vertex vertex2 = vertices.get(v2);
            if (vertex2 == null) {
                vertex2 = new Vertex(v2);
                vertex2.addEdge(v1);
                vertices.put(v2, vertex2);
            } else {
                vertex2.addEdge(v1);

            }
        }
        writeVertices(vertices, outputFile);
    }

    private static void writeVertices(Map<Long, Vertex> vertices, String output) throws IOException {
        for (Map.Entry<Long, Vertex> entry : vertices.entrySet()) {
            String data = "[" + entry.getKey() + ",0,[";
            for (Edge edge : entry.getValue().edges) {
                data += "[" + edge.v2 + "," + edge.weight + "],";
            }
            data = data.substring(0, data.length() - 1);
            data += "]\n";
            FileUtils.write(new File(output), data, true);
        }
    }

    static class Vertex {
        Long v1;
        List<Edge> edges;

        Vertex(Long v1) {
            this.v1 = v1;
            edges = new LinkedList<>();
        }

        void addEdge(Long v2) {
            boolean added = false;
            for (Edge edge : edges) {
                if (edge.v2.equals(v2)) {
                    if (added) {
                        throw new IllegalStateException("Multiple Edge to the same node");
                    }
                    edge.addWeight();
                    added = true;

                }
            }
            if (!added) {
                edges.add(new Edge(v2));
            }
        }

    }

    static class Edge {
        Long weight;
        Long v2;

        Edge(Long v2, Long weight) {
            this.v2 = v2;
            this.weight = weight;
        }

        Edge(Long v2) {
            this(v2, 1L);
        }

        void addWeight() {
            weight = weight + 1;
        }

        public Edge copy() {
            return new Edge(v2, weight);
        }
    }


}
