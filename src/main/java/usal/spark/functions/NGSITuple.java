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

import java.io.Serializable;

/**
 *
 * @author frbattid
 */
public class NGSITuple implements Serializable {
        
    private final String entityId;
    private final String entityType;
    private final String attrName;
    private final String attrType;

    /**
     * Constructor.
     * @param entityId
     * @param entityType
     * @param attrName
     * @param attrType
     */
    public NGSITuple(String entityId, String entityType, String attrName, String attrType) {
        this.entityId = entityId;
        this.entityType = entityType;
        this.attrName = attrName;
        this.attrType = attrType;
    } // NGSITuple

    @Override
    public String toString() {
        return entityId + "," + entityType + "," + attrName + "," + attrType;
    } // toString
    
    /**
     * Gets a key for this tuple.
     * @param onlyEntity
     * @return
     */
    public String getKey(boolean onlyEntity) {
        if (onlyEntity) {
            return entityId + "." + entityType;
        } else {
            return entityId + "." + entityType + "." + attrName + ",." + attrType;
        } // if else
    } // getKey

} // NGSITuple
