/*
 * Some header...
 */
package usal.spark.functions;

import org.apache.spark.api.java.function.Function2;

/**
 * Aggregates the value of two NGSI tuples having the same key.
 * 
 * @author frbattid
 */
public class NGSITuplesAggregatorByKey implements Function2<
        Float,
        Float,
        Float> {

    @Override
    public Float call(Float t1, Float t2) {
        return t1 + t2;
    } // call

} // NGSITuplesAggregatorByKey
