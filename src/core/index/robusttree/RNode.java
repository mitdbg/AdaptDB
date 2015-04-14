package core.index.robusttree;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import core.adapt.Predicate;
import core.index.MDIndex.Bucket;
import core.index.key.MDIndexKey;
import core.utils.RangeUtils.Range;
import core.utils.RangeUtils.SimpleDateRange.SimpleDate;
import core.utils.SchemaUtils.TYPE;

/**
 * Internal node in robust tree datastructure
 * @author anil
 */
public class RNode {

    public int attribute;
    public TYPE type;
    public Object value;
    public float quantile;

    public RNode parent;
    public RNode leftChild;
    public RNode rightChild;

    public Bucket bucket;

    public RNode() {

    }

    @Override
	public RNode clone() {
    	RNode r = new RNode();
    	r.attribute = this.attribute;
    	r.type = this.type;
    	r.value = this.value;
    	r.quantile = this.quantile;
    	r.parent = this.parent;
    	r.leftChild = this.leftChild;
    	r.rightChild = this.rightChild;
    	r.bucket = this.bucket;
    	return r;
    }

    public void setValues(int dimension, TYPE type, MDIndexKey key) {
        this.attribute = dimension;
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
        if (compareKey(value, attribute, type, key) > 0) {
            if (leftChild == null) {
                return bucket.getBucketId();
            }
            return leftChild.getBucketId(key);
        }
        else {
            if (rightChild == null) {
                return bucket.getBucketId();
            }
            return rightChild.getBucketId(key);
        }
    }

    public List<RNode> search(Predicate[] ps) {
    	if (bucket == null) {
        	boolean goLeft = true;
        	boolean goRight = true;
        	for (int i = 0; i < ps.length; i++) {
        		Predicate p = ps[i];
        		if (p.getAttribute() == attribute) {
        			Range r = p.getRange();
        			Object high = r.getHigh();
        			Object low  = r.getLow();

        			// TODO: One of these should be >=/<=; Check which
        			switch (type) {
	                    case INT:
	                    	if ((Integer)value > (Integer)high){
	                    		goRight = false;
	                    	} else if ((Integer) value < (Integer)low){
	                    		goLeft = false;
	                    	}

	                    case LONG:
	                    	if ((Long)value > (Long)high){
	                    		goRight = false;
	                    	} else if ((Long) value < (Long)low){
	                    		goLeft = false;
	                    	}

	                    case FLOAT:
	                    	if ((Float)value > (Float)high){
	                    		goRight = false;
	                    	} else if ((Float) value < (Float)low){
	                    		goLeft = false;
	                    	}

	                    case DATE:
	                    	if (((Date)value).compareTo((Date)high) > 0) {
	                    		goRight = false;
	                    	} else if (((Date)value).compareTo((Date)low) < 0) {
	                    		goLeft = false;
	                    	}

	                    case STRING:
	                    	if (value.hashCode() > high.hashCode()) {
	                    		goRight = false;
	                    	} else if (value.hashCode() < low.hashCode()){
	                    		goLeft = false;
	                    	}

	                    default:
	                        throw new RuntimeException("Unknown dimension type: "+type);
        			}
        		}
        	}

        	List<RNode> ret = null;
        	if (goLeft) {
        		ret = leftChild.search(ps);
        	}

        	if (goRight) {
        		if (ret == null) {
        			ret = rightChild.search(ps);
        		} else {
        			ret.addAll(rightChild.search(ps));
        		}
        	}

        	return ret;
    	} else {
    		List<RNode> ret = new LinkedList<RNode>();
    		ret.add(this);
    		return ret;
    	}
    }
}
