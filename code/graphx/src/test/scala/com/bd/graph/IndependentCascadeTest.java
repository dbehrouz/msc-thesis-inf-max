package com.bd.graph;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class IndependentCascadeTest {

    @Test
    public void test() {
        List<Vertex> vertices = new ArrayList<Vertex>();
        for (int i = 0; i < 5; i++) {
            Vertex v = new Vertex((long) i);
            v.setSpread(new Random().nextDouble());
            vertices.add(v);
        }

        System.out.println(vertices);

        Vertex vertex = vertices.get(0);
        vertex.setSpread(100.0);

        System.out.println(vertices);

        Collections.sort(vertices, IndependentCascade.comparator());
        System.out.println(vertices);
    }

}