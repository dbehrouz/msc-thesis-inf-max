package com.bd.graph;

import java.io.IOException;
import java.util.List;

/**
 * @author Behrouz Derakhshan
 */
public class SingleNodeApplication {
    public static void main(String[] args) throws IOException {
        Graph g = new Graph();
        g.loadGraph(args[0]);
        IndependentCascade ic = new IndependentCascade(g, 0.01, 10000);
        List<Long> seed = ic.greedyMethod(Integer.parseInt(args[1]));

        System.out.println(seed);
    }
}
