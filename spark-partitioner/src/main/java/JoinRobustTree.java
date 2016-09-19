import java.util.*;


/**
 * Created by ylu on 1/21/16.
 */


public class JoinRobustTree {
    public int maxBuckets;
    public int numBuckets;
    public int numAttributes;
    public int joinAttributeDepth;

    public TypeUtils.TYPE[] dimensionTypes;
    JRNode root;

    public static Random randGenerator = new Random();

    public JoinRobustTree(byte[] indexBytes) {
        this.root = new JRNode();
        this.root.unmarshall(indexBytes);
        this.numBuckets = getNumBuckets(this.root);
    }

    public void setMaxBuckets(int maxBuckets) {
        this.maxBuckets = maxBuckets;
    }

    public int getMaxBuckets() {
        return maxBuckets;
    }

    public JRNode getRoot() {
        return root;
    }

    // Only used for testing
    public void setRoot(JRNode root) {
        this.root = root;
    }


    public int getBucketId(String key) {
        return root.getBucketId(key);
    }

    private int getNumBuckets(JRNode root){
        if(root.leftChild == null && root.rightChild == null){
            return 1;
        }
        return getNumBuckets(root.leftChild) + getNumBuckets(root.rightChild);
    }

    /**
     * Serializes the index to string Very brittle - Consider rewriting
     */

    public byte[] marshall() {
        // JVM optimizes shit so no need to use string builder / buffer
        // Format:
        // maxBuckets, numAttributes
        // types
        // nodes in pre-order

        String robustTree = "";
        robustTree += String.format("%d %d %d\n", this.maxBuckets,
                this.numAttributes, this.joinAttributeDepth);

        String types = "";
        for (int i = 0; i < this.numAttributes; i++) {
            types += this.dimensionTypes[i].toString() + " ";
        }
        types += "\n";
        robustTree += types;

        robustTree += this.root.marshall();

        return robustTree.getBytes();
    }

    public void unmarshall(byte[] bytes) {
        String tree = new String(bytes);
        Scanner sc = new Scanner(tree);
        this.maxBuckets = sc.nextInt();
        this.numAttributes = sc.nextInt();
        this.joinAttributeDepth = sc.nextInt();

        this.dimensionTypes = new TypeUtils.TYPE[this.numAttributes];
        for (int i = 0; i < this.numAttributes; i++) {
            this.dimensionTypes[i] = TypeUtils.TYPE.valueOf(sc.next());
        }

        this.root = new JRNode();
        this.root.parseNode(sc);
    }

}
