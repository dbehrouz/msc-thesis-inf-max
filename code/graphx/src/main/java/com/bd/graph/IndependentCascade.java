package com.bd.graph;


import java.util.ArrayList;
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

    public List<Long> greedyMethod(int seedSize) {
        List<Long> seed = new ArrayList<Long>();
        Long maxVertex = -1l;
        Double maxSpread = -1d;
        Map<Long, Vertex> vertices = graph.getVertices();
        for (int i = 0; i < seedSize; i++) {
            int counter = 0;
            for (Map.Entry<Long, Vertex> entry : vertices.entrySet()) {
                if (!seed.contains(entry.getKey())) {
                    List<Long> seedCopy = new ArrayList<Long>(seed);
                    seedCopy.add(entry.getKey());
                    Double totalSpread = simulate(seedCopy);
                    if (totalSpread > maxSpread) {
                        maxVertex = entry.getKey();
                        maxSpread = totalSpread;
                    }
                }
                counter++;
                if (counter % 100 == 0)
                    System.out.print(".");
            }
            seed.add(maxVertex);
            System.out.println();
            System.out.println("Item " + (i + 1) + ": " + maxVertex);

            maxSpread = -1d;
            maxVertex = -1l;


        }

        return seed;
    }


}
