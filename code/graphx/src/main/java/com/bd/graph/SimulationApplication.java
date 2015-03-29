package com.bd.graph;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Behrouz Derakhshan
 */
public class SimulationApplication {
    public static void main(String[] args) throws IOException {
        Graph g = new Graph();
        g.loadGraph(args[0]);
        IndependentCascade ic = new IndependentCascade(g, 0.01, 10000);
        List<Long> seeds = getSeedFromFile(args[1]);
        Double result = ic.simulate(seeds);

        System.out.println("Average spread: " + result);

    }

    private static List<Long> getSeedFromFile(String file) throws IOException {
        List<Long> seeds = new ArrayList<Long>();
        for (String line : FileUtils.readLines(new File(file))) {
            seeds.add(Long.parseLong(line));
        }
        return seeds;
    }
}
