package core.index.robusttree;

import java.util.List;

import core.index.MDIndex;
import core.index.MDIndex.Bucket;
import core.index.key.MDIndexKey;
import core.utils.RangeUtils.SimpleDateRange.SimpleDate;
import core.utils.SchemaUtils.TYPE;

/**
 * Internal node in robust tree datastructure
 * @author anil
 */
public class RNode {

    public int dimension;
    public TYPE type;
    public Object value;
    public float quantile;

    public RNode parent;
    public RNode leftChild;
    public RNode rightChild;

    public Bucket bucket;

    public RNode() {

    }

    public void setValues(int dimension, TYPE type, MDIndexKey key) {
        this.dimension = dimension;
        this.type = type;
        this.value = getValue(dimension, type, key);
    }

    private Object getValue(int dimension, TYPE type, MDIndexKey key) {
        switch (type) {
            case INT:
                return key.getIntAttribute(dimension);
            case LONG:
                return key.getLongAttribute(dimension);
            case FLOAT:
                return key.getFloatAttribute(dimension);
            case DATE:
                return key.getDateAttribute(dimension);
            case STRING:
                return key.getStringAttribute(dimension, 20);
            default:
                throw new RuntimeException("Unknown dimension type: "+type);
        }
        // TODO(qui): deal with VARCHAR somewhere
    }

    private int compareKey(Object value, int dimension, TYPE type, MDIndexKey key) {
        switch (type) {
            case INT:
                return ((Integer) value).compareTo(key.getIntAttribute(dimension));
            case LONG:
                return ((Long) value).compareTo(key.getLongAttribute(dimension));
            case FLOAT:
                return ((Float) value).compareTo(key.getFloatAttribute(dimension));
            case DATE:
                return ((SimpleDate) value).compareTo(key.getDateAttribute(dimension));
            case STRING:
                return ((String) value).compareTo(key.getStringAttribute(dimension, 20));
            default:
                throw new RuntimeException("Unknown dimension type: "+type);
        }
    }

    public int getBucketId(MDIndexKey key) {
        if (compareKey(value, dimension, type, key) > 0) {
            if (leftChild == null) {
                return bucket.bucketId;
            }
            return leftChild.getBucketId(key);
        }
        else {
            if (rightChild == null) {
                return bucket.bucketId;
            }
            return rightChild.getBucketId(key);
        }
    }

    public List<MDIndex.Bucket> rangeSearch(MDIndexKey low, MDIndexKey high) {
        return null;
    }
}
