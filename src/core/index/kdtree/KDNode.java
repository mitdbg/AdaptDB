package core.index.kdtree;

import core.index.MDIndex;
import core.index.key.MDIndexKey;
import core.utils.SchemaUtils.TYPE;

import java.util.List;

/**
 * Created by qui on 1/31/15.
 */
public class KDNode {

    private int dimension;
    private Comparable value;
    private int numBuckets;

    private KDNode parent;
    private KDNode leftChild;
    private KDNode rightChild;

    public KDNode() {
        numBuckets = 1;
    }

    public int getDimension() {
        return dimension;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public void setValues(int dimension, TYPE type, MDIndexKey key) {
        this.dimension = dimension;
        switch (type) {
            case INT:
                this.value = key.getIntAttribute(dimension);
                break;
            case LONG:
                this.value = key.getLongAttribute(dimension);
                break;
            case FLOAT:
                this.value = key.getFloatAttribute(dimension);
                break;
            case DATE:
                this.value = (Comparable) key.getDateAttribute(dimension);
                break;
            case STRING:
                this.value = key.getStringAttribute(dimension, 20);
                break;
            default:
                throw new RuntimeException("Unknown dimension type: "+type);
        }
        // TODO(qui): deal with VARCHAR somewhere
    }

    public KDNode insert(MDIndexKey key) {
        if (value == null) {
            return this;
        } else if (value.compareTo(key.getIntAttribute(dimension)) >= 0) {
            if (leftChild == null) {
                leftChild = new KDNode();
                leftChild.parent = this;
                if (rightChild != null) {
                    numBuckets = rightChild.getNumBuckets() + 1;
                }
                return leftChild;
            } else {
                return leftChild.insert(key);
            }
        } else {
            if (rightChild == null) {
                rightChild = new KDNode();
                rightChild.parent = this;
                if (leftChild != null) {
                    numBuckets = leftChild.getNumBuckets() + 1;
                }
                return rightChild;
            } else {
                return rightChild.insert(key);
            }
        }
    }

    public int getBucketId(MDIndexKey key, int start) {
        if (numBuckets == 1) {
            return start;
        }
        int searchVal = key.getIntAttribute(dimension);
        if (value.compareTo(searchVal) >= 0) {
            return leftChild.getBucketId(key, start*2);
        }
        else {
            return rightChild.getBucketId(key, start*2+1);
        }
    }

    public List<MDIndex.Bucket> rangeSearch(MDIndexKey low, MDIndexKey high) {
        return null;
    }
}
