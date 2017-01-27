/*
 * Some header...
 */
package usal.spark.functions;

import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

/**
 * Aggregates the value of two NGSI tuples.
 * 
 * @author frbattid
 */

public class NGSITuplesAggregatorAll implements Function2<
        Tuple2<NGSITuple, Float>,
        Tuple2<NGSITuple, Float>,
        Tuple2<NGSITuple, Float>> {
    
    private final String aggrEntityId;
    private final String aggrEntityType;
    private final String aggrAttrName;
    private final String aggrAttrType;
    
    /**
     * Constructor.
     * @param aggrEntityId
     * @param aggrEntityType
     * @param aggrAttrName
     * @param aggrAttrType
     */
    public NGSITuplesAggregatorAll(String aggrEntityId, String aggrEntityType,
            String aggrAttrName, String aggrAttrType) {
        this.aggrEntityId = aggrEntityId;
        this.aggrEntityType = aggrEntityType;
        this.aggrAttrName = aggrAttrName;
        this.aggrAttrType = aggrAttrType;
    } // NGSITuplesAggregatorAll

    @Override
    public Tuple2<NGSITuple, Float> call(Tuple2<NGSITuple, Float> t1, Tuple2<NGSITuple, Float> t2) {
        return new Tuple2(new NGSITuple(aggrEntityId, aggrEntityType, aggrAttrName, aggrAttrType), t1._2() + t2._2());
    } // call

} // NGSITuplesAggregatorAll
