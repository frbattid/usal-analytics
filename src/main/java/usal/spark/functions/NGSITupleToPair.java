/**
 * Copyright 2017 Francisco Romero Bueno
 *
 * usal-analytics is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * 
 * usal-analytics is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with usal-analytics. If not, see
 * http://www.gnu.org/licenses/.
 *
 * For those usages not covered by the GNU Affero General Public License please raise an issue at
 * https://github.com/frbattid/usal-analytics
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
