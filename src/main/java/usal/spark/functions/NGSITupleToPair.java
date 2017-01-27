/*
 * Some header..
 */
package usal.spark.functions;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * @author frb
 */
public class NGSITupleToPair implements PairFunction<
        Tuple2<NGSITuple, Float>,
        String,
        Float> {
    
    private final boolean onlyEntity;

    /**
     * Constructor.
     * @param onlyEntity
     */
    public NGSITupleToPair(boolean onlyEntity) {
        this.onlyEntity = onlyEntity;
    } // NGSITupleToPair
    
    @Override
    public Tuple2<String, Float> call(Tuple2<NGSITuple, Float> tuple) {
        return new Tuple2(tuple._1().getKey(onlyEntity), tuple._2());
    } // call
    
} // NGSITupleToPair
