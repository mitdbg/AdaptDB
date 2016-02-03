package perf.benchmark;

import core.adapt.Predicate;
import core.adapt.Query;
import core.adapt.spark.RangePartitioner;
import core.adapt.spark.join.SparkJoinQuery;
import core.common.globals.Schema;
import core.common.globals.TableInfo;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.TypeUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;


public class CMTJoinWorkload {

    private ConfUtils cfg;

    private Schema schemaMH, schemaMHL, schemaSF;
    private String stringMH, stringMHL, stringSF;

    private String MH = "mh", MHL = "mhl", SF = "sf";
    private Predicate[] EmptyPredicates = {};


    private int method;

    private int memoryBudget;

    private int numQueries;

    private Random rand;

    public void setUp() {
        cfg = new ConfUtils(BenchmarkSettings.conf);
        rand = new Random();

        // Making things more deterministic.
        rand.setSeed(0);
    }


    public void loadSettings(String[] args) {
        int counter = 0;
        while (counter < args.length) {
            switch (args[counter]) {
                case "--schemaMH":
                    stringMH = args[counter + 1];
                    schemaMH = Schema.createSchema(stringMH);
                    counter += 2;
                    break;
                case "--schemaMHL":
                    stringMHL = args[counter + 1];
                    schemaMHL = Schema.createSchema(stringMHL);
                    counter += 2;
                    break;
                case "--schemaSF":
                    stringSF = args[counter + 1];
                    schemaSF = Schema.createSchema(stringSF);
                    counter += 2;
                    break;
                case "--method":
                    method = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                case "--numQueries":
                    numQueries = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                case "--budget":
                    memoryBudget = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                default:
                    // Something we don't use
                    counter += 2;
                    break;
            }
        }
    }

    public void cleanup(String path){
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        try {
            fs.delete(new Path(path), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void postProcessing(String path, String tableName, Schema schema) {

        /* rename part-0000i to i and create an info file*/

        try {
            FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
            String dest = path + "/data";

            // delete _SUCCESS

            fs.delete(new Path(dest + "/_SUCCESS"), false);
            FileStatus[] fileStatus = fs.listStatus(new Path(dest));

            for (int i = 0; i < fileStatus.length; i++) {
                String oldPath = fileStatus[i].getPath().toString();
                String baseName = FilenameUtils.getBaseName(oldPath);
                String dir = oldPath.substring(0, oldPath.length() - baseName.length());
                String newPath = dir + Integer.parseInt(baseName.substring(baseName.indexOf('-') + 1));

                fs.rename(new Path(oldPath), new Path(newPath));
            }


            /*  write out a fake (TOTAL_NUM_TUPLES is 0, delimiter is set to '|') info to make HDFSPartition Happy*/

            TableInfo tableInfo = new TableInfo(tableName, 0, ';', schema);
            tableInfo.save(cfg.getHDFS_WORKING_DIR(), cfg.getHDFS_REPLICATION_FACTOR(), fs);


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public Predicate getPredicate(Schema schema, String pred) {
        String[] parts = pred.split(" ");
        int attrId = schema.getAttributeId(parts[0].trim());

        if (attrId == -1) {
            throw new RuntimeException("Unknown attr: " + parts[0].trim());
        }

        TypeUtils.TYPE attrType = schema.getType(attrId);
        Object value = TypeUtils.deserializeValue(attrType, parts[2].trim().replaceAll("'", ""));
        String predTypeStr = parts[1].trim();
        Predicate.PREDTYPE predType;
        switch (predTypeStr) {
            case ">":
                predType = Predicate.PREDTYPE.GT;
                break;
            case ">=":
                predType = Predicate.PREDTYPE.GEQ;
                break;
            case "<":
                predType = Predicate.PREDTYPE.LT;
                break;
            case "<=":
                predType = Predicate.PREDTYPE.LEQ;
                break;
            case "=":
                predType = Predicate.PREDTYPE.EQ;
                break;
            default:
                throw new RuntimeException("Unknown predType " + predTypeStr);
        }

        Predicate p = new Predicate(attrId, attrType, value, predType);
        return p;
    }



    public ArrayList<ArrayList<Query>> generateWorkload() {
        byte[] stringBytes = HDFSUtils.readFile(
                HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()),
                "/user/mdindex/cmt_queries.log");
        String queriesString = new String(stringBytes);
        String[] queries = queriesString.split("\n");
        ArrayList<ArrayList<Query>> ret = new ArrayList<ArrayList<Query>>();
        for (int i=0; i<queries.length; i++) {
            String query = queries[i];
            String[] predicates = query.split(";");
            ArrayList<Predicate> mhPreds = new ArrayList<Predicate>();
            ArrayList<Predicate> sfPreds = new ArrayList<Predicate>();

            ArrayList<Query> q = new ArrayList<Query>();

            for (int j=0; j<predicates.length; j++) {
                if(predicates[j].startsWith(MH)){
                    Predicate p = getPredicate(schemaMH, predicates[j]);
                    mhPreds.add(p);
                } else {
                    Predicate p = getPredicate(schemaSF, predicates[j]);
                    sfPreds.add(p);
                }
            }

            Predicate[] mhArray = mhPreds.toArray(new Predicate[mhPreds.size()]);
            Predicate[] sfArray = sfPreds.toArray(new Predicate[sfPreds.size()]);

            Query q_mh = new Query(MH, mhArray);
            Query q_sf = new Query(SF, sfArray);

            q.add(q_mh);
            q.add(q_sf);

            ret.add(q);
        }

        return ret;
    }


    // sf ⋈ (mhl ⋈ mh)
    public void runWorkload(){

        ArrayList<ArrayList<Query>> queries = generateWorkload();

        SparkJoinQuery sq = new SparkJoinQuery(cfg);
        sq.setDelimiter(";");

        for (ArrayList<Query> q: queries) {

            Query q_mh = q.get(0);
            Query q_sf = q.get(1);

            System.out.println("INFO: Query_MH:" + q_mh.toString());
            System.out.println("INFO: Query_sf:" + q_sf.toString());


            long start = System.currentTimeMillis();

            String mhl_join_mh = "mhl_join_mh";

            String stringMHL_join_MH = stringMHL + ", " + stringMH;
            Schema schemaMHL_join_MH = Schema.createSchema(stringMHL_join_MH);

            JavaPairRDD<LongWritable, Text> rdd = sq.createJoinScanRDD(MHL, stringMHL, new Query(MHL, EmptyPredicates), schemaMHL.getAttributeId("mhl_mapmatch_history_id"), "NULL",
                    MH, stringMH, q_mh, schemaMH.getAttributeId("mh_id"), "NULL",schemaMHL_join_MH.getAttributeId("mhl_dataset_id"),  memoryBudget);

            String cutPoints = sq.getCutPoints(SF, 0); // long[] = {1, 2, 3};

            Partitioner partitioner = new RangePartitioner(cutPoints);

            JavaRDD<Text> rdd_mhl_join_ml = rdd.partitionBy(partitioner).values();

            String dest = cfg.getHDFS_WORKING_DIR() + "/" + mhl_join_mh;

            rdd_mhl_join_ml.saveAsTextFile(dest + "/data");

            long result = rdd_mhl_join_ml.count();

            //System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

            postProcessing(dest, mhl_join_mh, schemaMHL_join_MH);

            rdd = sq.createJoinScanRDD(SF, stringSF, q_sf, schemaSF.getAttributeId("sf_id"), "NULL",
                    mhl_join_mh, stringMHL_join_MH, new Query(mhl_join_mh, EmptyPredicates), schemaMHL_join_MH.getAttributeId("mhl_dataset_id"), cutPoints,0, memoryBudget);

            result = rdd.count();

            System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

            cleanup(dest);

        }
    }

    public static void main(String[] args) {

        BenchmarkSettings.loadSettings(args);
        BenchmarkSettings.printSettings();

        CMTJoinWorkload t = new CMTJoinWorkload();
        t.loadSettings(args);
        t.setUp();

        switch (t.method) {
            case 1:
                t.runWorkload();
                break;
            default:
                break;
        }
    }
}
