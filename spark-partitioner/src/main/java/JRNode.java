import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * Internal node in JoinRobustTree data structure
 *
 * @author yilu
 */

public class JRNode {

    public int attribute;
    public TypeUtils.TYPE type;
    public Object value;
    int bucketId;

    public JRNode parent;
    public JRNode leftChild;
    public JRNode rightChild;



    public JRNode() {
        attribute = -1;
        type = null;
        value = null;
        bucketId = -1;

        parent = null;
        leftChild = null;
        rightChild = null;
    }


    public void setValues(int dimension, TypeUtils.TYPE type, String key) {
        this.attribute = dimension;
        this.type = type;
        this.value = getValue(dimension, type, key);
    }

    private Object getValue(int dimension, TypeUtils.TYPE type, String key) {
        String[] attr = key.split(Global.SPLIT_DELIMITER);
        switch (type) {
            case INT:
                return TypeUtils.getIntAttribute(attr[dimension]);
            case LONG:
                return TypeUtils.getLongAttribute(attr[dimension]);
            case DOUBLE:
                return TypeUtils.getDoubleAttribute(attr[dimension]);
            case DATE:
                return TypeUtils.getDateAttribute(attr[dimension]);
            case STRING:
                return TypeUtils.getStringAttribute(attr[dimension]);
            default:
                throw new RuntimeException("Unknown dimension type: " + type);
        }
    }

    private int compareKey(Object value, int dimension, TypeUtils.TYPE type, String key) {
        String[] attr = key.split(Global.SPLIT_DELIMITER);
        switch (type) {
            case INT:
                return ((Integer) value).compareTo(TypeUtils.getIntAttribute(attr[dimension]));
            case LONG:
                return ((Long) value).compareTo(TypeUtils.getLongAttribute(attr[dimension]));
            case DOUBLE:
                return ((Double) value).compareTo(TypeUtils.getDoubleAttribute(attr[dimension]));
            case DATE:
                return ((TypeUtils.SimpleDate) value).compareTo(TypeUtils
                        .getDateAttribute(attr[dimension]));
            case STRING:
                return ((String) value).compareTo(TypeUtils.getStringAttribute(attr[dimension]));
            default:
                throw new RuntimeException("Unknown dimension type: " + type);
        }
    }

    public int getBucketId(String key) {
        if (this.bucketId != -1) {
            return bucketId;
        } else {
            if (compareKey(value, attribute, type, key) >= 0) {
                return leftChild.getBucketId(key);
            } else {
                return rightChild.getBucketId(key);
            }
        }
    }

    public String marshall() {
        String ret = "";
        LinkedList<JRNode> stack = new LinkedList<JRNode>();
        stack.add(this);
        while (stack.size() != 0) {
            JRNode n = stack.removeLast();
            String nStr;
            if (n.bucketId != -1) {
                nStr = String.format("b %d\n", n.bucketId);
            } else {
                nStr = String.format("n %d %s %s\n", n.attribute,
                        n.type.toString(),
                        TypeUtils.serializeValue(n.value, n.type));

                stack.add(n.rightChild);
                stack.add(n.leftChild);
            }
            ret += nStr;
        }
        return ret;
    }

    public void unmarshall(byte[] bytes) {
        String tree = new String(bytes);
        Scanner sc = new Scanner(tree);

        this.parseNode(sc);
    }

    public JRNode parseNode(Scanner sc) {
        String type = sc.next();
        if (type.equals("n")) {
            this.attribute = sc.nextInt();
            this.type = TypeUtils.TYPE.valueOf(sc.next());
            // For string tokens; we may have to read more than one token, so
            // read till end of line
            this.value = TypeUtils.deserializeValue(this.type, sc.nextLine()
                    .trim());

            this.leftChild = new JRNode();
            this.leftChild.parent = this;

            this.rightChild = new JRNode();
            this.rightChild.parent = this;

            this.leftChild.parseNode(sc);
            this.rightChild.parseNode(sc);

        } else if (type.equals("b")) {
            this.bucketId = sc.nextInt();
        } else {
            System.out.println("Bad things have happened in unmarshall");
            System.out.println(type);
        }

        return this;
    }
}
