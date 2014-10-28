package com.bd.propagation.ic.allpropagation;

import org.apache.giraph.master.DefaultMasterCompute;

/**
 * @author Behrouz Derakhshan
 */
public class ICMasterCompute extends DefaultMasterCompute {

    @Override
    public void initialize() throws IllegalAccessException, InstantiationException {
        registerAggregator(IndependentCascade.MAX_AGG, MaxVertexAggregator.class);
    }
}
