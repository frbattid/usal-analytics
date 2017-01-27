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
