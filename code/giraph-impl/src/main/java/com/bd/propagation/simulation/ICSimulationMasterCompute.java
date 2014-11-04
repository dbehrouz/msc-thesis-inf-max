package com.bd.propagation.simulation;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;

/**
 * @author Behrouz Derakhshan
 */
public class ICSimulationMasterCompute extends DefaultMasterCompute {
    @Override
    public void initialize() throws IllegalAccessException, InstantiationException {
        registerPersistentAggregator(ICSimulation.SPREAD, IntSumAggregator.class);
    }
}
