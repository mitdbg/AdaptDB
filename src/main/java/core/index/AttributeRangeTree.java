package core.index;

import com.google.common.base.Joiner;

import core.index.key.MDIndexKey;
import core.utils.TypeUtils;
import core.utils.TypeUtils.TYPE;

/**
 * Created by qui on 7/14/15.
 */
public class AttributeRangeTree implements MDIndex {

    Object[] bucketBoundaries;
    int attribute;
    TYPE type;

    public AttributeRangeTree(int attribute, TYPE type) {
        this.attribute = attribute;
        this.type = type;
    }

    @Override
    public MDIndex clone() throws CloneNotSupportedException {
        return new AttributeRangeTree(attribute, type);
    }

    @Override
    public void initBuild(int numBuckets) {
    }

    @Override
    public void insert(MDIndexKey key) {
    }

    @Override
    public void bulkLoad(MDIndexKey[] keys) {
    }

    @Override
    public void initProbe() {
    }

    public void setBoundaries(Object[] boundaries) {
        this.bucketBoundaries = boundaries;
    }

    @Override
    public Object getBucketId(MDIndexKey key) {
        Object val;
        switch (type) {
            case INT: val = key.getIntAttribute(attribute); break;
            case LONG: val = key.getLongAttribute(attribute); break;
            case FLOAT: val = key.getFloatAttribute(attribute); break;
            case DATE: val = key.getDateAttribute(attribute); break;
            default:
                throw new RuntimeException("unsupported type");
        }
        for (int i = 0; i < bucketBoundaries.length; i++) {
            if (TypeUtils.compareTo(val, bucketBoundaries[i], type) < 1) {
                return i-1;
            }
        }
        return bucketBoundaries.length;
    }

    @Override
    public byte[] marshall() {
        return (type.toString() + ";" + String.valueOf(attribute) + ";" + Joiner.on(",").join(bucketBoundaries)).getBytes();
    }

    @Override
    public void unmarshall(byte[] bytes) {
        String[] tokens = (new String(bytes)).split(";");
        type = TYPE.valueOf(tokens[0]);
        attribute = Integer.parseInt(tokens[1]);
        String[] boundaries = tokens[2].split(",");
        bucketBoundaries = new Object[boundaries.length];
        for (int i = 0; i < boundaries.length; i++) {
            bucketBoundaries[i] = TypeUtils.deserializeValue(type, boundaries[i]);
        }
    }
}
