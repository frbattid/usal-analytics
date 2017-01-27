/*
 * Some header...
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
