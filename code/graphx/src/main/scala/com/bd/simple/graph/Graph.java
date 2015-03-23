package com.bd.simple.graph;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Behrouz Derakhshan
 */
public class Graph {
    private Map<Long, Vertex> vertices = new HashMap<Long, Vertex>();

    public Map<Long, Vertex> getVertices() {
        return vertices;
    }

    public void loadGraph(String file) throws IOException {
        List<String> lines = FileUtils.readLines(new File(file));
        for (String line : lines) {
            String[] splits = line.split(" ");
            Long src = Long.parseLong(splits[0]);
            Long dst = Long.parseLong(splits[1]);
            addVertex(src, dst);
            addVertex(dst, src);
        }

    }

    private void addVertex(Long src, Long dst) {
        Vertex v = vertices.get(src);
        if (v == null) {
            v = new Vertex(src);
            v.addNeighbor(dst);
            vertices.put(src, v);
        } else {
            v.addNeighbor(dst);
            vertices.put(src, v);
        }
    }
}
