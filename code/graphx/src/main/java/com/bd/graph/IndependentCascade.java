package com.bd.graph;


import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Stack;

/**
 * @author Behrouz Derakhshan
 */
public class IndependentCascade {
    private final Graph graph;
    private final Double probability;
    private final int simulations;

    public IndependentCascade(Graph graph, Double probability, int simulations) {
        this.graph = graph;
        this.probability = probability;
        this.simulations = simulations;
    }

    // simulate IC process
    public int singleDiffusion(List<Long> seed) {
        Set<Vertex> active = new HashSet(); //will store the active nodes
        Stack<Vertex> target = new Stack<Vertex>(); //will store unprocessed nodes during intermediate time
        Map<Long, Vertex> vertices = graph.getVertices();
        for (Long val : seed) {
            target.push(vertices.get(val));
            while (target.size() > 0) {
                Vertex node = target.pop();
                active.add(node);
                for (Map.Entry<Long, Double> follower : node.neighbors.entrySet()) {
                    Random random = new Random();
                    double ran = random.nextDouble();
                    if (ran < follower.getValue() * probability) {
                        if (!active.contains(vertices.get(follower.getKey()))) {
                            target.push(vertices.get(follower.getKey()));
                        }
                    }
                }
            }
        }

        return active.size();

    }

    public Double simulate(List<Long> seed) {
        Double total = 0.0;
        for (int i = 0; i < simulations; i++) {
            total += singleDiffusion(seed);
        }

        return total / (double) simulations;
    }

    public List<Long> greedyMethod(int seedSize) throws IOException {
        List<Long> seed = new ArrayList<Long>();
        Long maxVertex = -1l;
        Double previousSpread = 0d;
        Map<Long, Vertex> vertices = graph.getVertices();
        List<Vertex> intermediateSpread = initSpread(vertices);
        long start = System.currentTimeMillis();
        for (int i = 0; i < seedSize; i++) {

            int counter = 0;
            for (int j = 0; j < intermediateSpread.size(); j++) {
                Vertex v = intermediateSpread.get(j);
                if (!seed.contains((v.getId()))) {
                    List<Long> seedCopy = new ArrayList<Long>(seed);
                    seedCopy.add(v.getId());
                    Double totalSpread = simulate(seedCopy);
                    v.setSpread(totalSpread - previousSpread);
                    if (i > 0) {
                        if (v.getSpread() > intermediateSpread.get(j + 1).getSpread() ||
                                j == intermediateSpread.size()) {
                            System.out.println("Breaking after " + counter + " steps");
                            System.out.println("My Spread : " + v.getSpread());
                            System.out.println("His Spread : " + intermediateSpread.get(j + 1).getSpread());
                            previousSpread = totalSpread;
                            break;
                        }
                    }
                }
                counter++;
                if (counter % 100 == 0)
                    System.out.print(".");
            }
            Collections.sort(intermediateSpread, comparator());
            Vertex max = intermediateSpread.get(0);
            seed.add(max.getId());
            long end = System.currentTimeMillis();
            Long time = end - start;
            FileUtils.write(new File("greedytimes.txt"), time.toString() + "\n", true);
            if (i == 0) {
                previousSpread = max.getSpread();
            }
            System.out.println();
            System.out.println("Item " + (i + 1) + ": " + max.getId());
            intermediateSpread.remove(0);

        }
        return seed;
    }

    private List<Vertex> initSpread(Map<Long, Vertex> vertices) {
        List<Vertex> vertexSpread = new ArrayList<Vertex>();
        for (Map.Entry<Long, Vertex> entry : vertices.entrySet()) {
            Vertex v = entry.getValue();
            v.setSpread(0.0);
            vertexSpread.add(v);
        }

        return vertexSpread;
    }

    public static Comparator<Vertex> comparator() {
        return new Comparator<Vertex>() {
            @Override
            public int compare(Vertex o1, Vertex o2) {
                if (o1.getSpread() < o2.getSpread()) {
                    return 1;
                } else if (o1.getSpread() > o2.getSpread()) {
                    return -1;
                } else {
                    return 0;
                }
            }
        };
    }
}
