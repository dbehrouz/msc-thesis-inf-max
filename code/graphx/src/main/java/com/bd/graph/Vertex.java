package com.bd.graph;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Behrouz Derakhshan
 */
public class Vertex {
    private final Long id;
    Map<Long, Double> neighbors = new HashMap<Long, Double>();
    private int label = 0;
    private double spread = 0;

    public Long getId() {
        return id;
    }

    public void setSpread(double spread) {
        this.spread = spread;
    }

    public double getSpread() {
        return spread;
    }

    public void activate() {
        this.label = 1;
    }

    public Boolean isActive() {
        return label == 1;
    }

    public Vertex(Long id) {
        this.id = id;
        neighbors = new HashMap<Long, Double>();
        this.spread = 0;
    }

    public void addNeighbor(Long val) {
        Double value = neighbors.get(val);
        if (value == null) {
            neighbors.put(val, 1.0);
        } else {
            neighbors.put(val, value + 1);
        }
    }

    @Override
    public String toString(){
        return id + ":" + spread;
    }
}
