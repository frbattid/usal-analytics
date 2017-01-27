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
