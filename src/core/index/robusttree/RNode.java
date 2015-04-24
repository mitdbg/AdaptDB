package core.index.robusttree;

import java.util.LinkedList;
import java.util.List;

import core.access.Predicate;
import core.index.MDIndex.Bucket;
import core.index.key.MDIndexKey;
import core.utils.RangeUtils.SimpleDateRange.SimpleDate;
import core.utils.SchemaUtils.TYPE;
import core.utils.TypeUtils;

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
        		if (p.attribute == attribute) {
        			switch (p.predtype) {
	                	case GEQ:
	                		if (TypeUtils.compareTo(p.value, value, type) > 0) goLeft = false;
	                		break;
	                	case LEQ:
	                		if (TypeUtils.compareTo(p.value, value, type) <= 0) goRight = false;
	                		break;
	                	case GT:
	                		if (TypeUtils.compareTo(p.value, value, type) >= 0) goLeft = false;
	                		break;
	                	case LT:
	                		if (TypeUtils.compareTo(p.value, value, type) < 0) goRight = false;
	                		break;
	                	case EQ:
	                		if (TypeUtils.compareTo(p.value, value, type) <= 0) goRight = false;
	                		else goLeft = false;
	                		break;
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

    public int numTuplesInSubtree() {
    	LinkedList<RNode> stack = new LinkedList<RNode>();
    	stack.add(this);
    	int total = 0;
    	while (stack.size() > 0) {
    		RNode t = stack.removeLast();
    		if (t.bucket != null) {
    			total += t.bucket.getNumTuples();
    		} else {
    			stack.add(t.rightChild);
    			stack.add(t.leftChild);
    		}
    	}

    	return total;
    }

    public int getAll() {
    	LinkedList<RNode> stack = new LinkedList<RNode>();
    	stack.add(this);
    	int total = 0;
    	while (stack.size() > 0) {
    		RNode t = stack.removeLast();
    		if (t.bucket != null) {
    			total += t.bucket.getNumTuples();
    		} else {
    			stack.add(t.rightChild);
    			stack.add(t.leftChild);
    		}
    	}

    	return total;
    }
}
