package perf.benchmark;

/**
 * Created by ylu on 10/5/16.
 */


import core.adapt.JoinQuery;
import core.adapt.Predicate;
import core.common.globals.Globals;
import core.common.globals.Schema;
import core.common.globals.TableInfo;
import core.common.index.JRNode;
import core.common.index.JoinRobustTree;
import core.common.index.MDIndex;
import core.common.key.ParsedTupleList;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.TypeUtils;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;


public class CMT {


    public static String mh = "mh";
    public static String mhl = "mhl";
    public static String sf = "sf";

    public static String stringMH = "mh_id LONG,mh_dataset_id STRING,mh_uploadtime DATE,mh_runtime STRING,mh_trip_start DATE,mh_data_count_minutes STRING,mh_data_count_accel_samples STRING,mh_data_count_netloc_samples STRING,mh_data_count_gps_samples STRING,mh_observed_sample_rate STRING,mh_distance_mapmatched_km STRING,mh_distance_gps_km STRING,mh_ground_truth_present STRING,mh_timing_mapmatch DOUBLE,mh_distance_pct_path_error STRING,mh_build_version STRING,mh_timing_queue_wait DOUBLE,mh_data_trip_length STRING,mh_battery_maximum_level STRING,mh_battery_minimum_level STRING,mh_battery_drain_rate_per_hour STRING,mh_battery_plugged_duration_hours STRING,mh_battery_delay_from_drive_end_seconds STRING,mh_startlat STRING,mh_startlon STRING,mh_endlat STRING,mh_endlon STRING,mh_data_count_output_gps_speeding_points STRING,mh_speeding_slow_gps_points STRING,mh_speeding_10kmh_gps_points STRING,mh_speeding_20kmh_gps_points STRING,mh_speeding_40kmh_gps_points STRING,mh_speeding_80kmh_gps_points STRING,mh_output_accel_valid_minutes STRING,mh_output_gps_moving_minutes STRING,mh_output_gps_moving_and_accel_valid_minutes STRING,mh_data_time_till_first_gps_minutes STRING,mh_score_di_accel STRING,mh_score_di_brake STRING,mh_score_di_turn STRING,mh_score_di_car_motion STRING,mh_score_di_phone_motion STRING,mh_score_di_speeding STRING,mh_score_di_night STRING,mh_star_rating STRING,mh_trip_end DATE,mh_score_di_car_motion_with_accel STRING,mh_score_di_car_motion_with_speeding STRING,mh_score_di_distance_km_with_accel STRING,mh_score_di_distance_km_with_speeding STRING,mh_score_accel_per_sec_ntile STRING,mh_score_brake_per_sec_ntile STRING,mh_score_turn_per_sec_ntile STRING,mh_score_speeding_per_sec_ntile STRING,mh_score_phone_motion_per_sec_ntile STRING,mh_score_accel_per_km_ntile STRING,mh_score_brake_per_km_ntile STRING,mh_score_turn_per_km_ntile STRING,mh_score_speeding_per_km_ntile STRING,mh_score_phone_motion_per_km_ntile STRING,mh_score STRING,mh_distance_prepended_km STRING,mh_recording_start DATE,mh_score_di_distance_km STRING,mh_recording_end DATE,mh_recording_startlat STRING,mh_recording_startlon STRING,mh_display_distance_km STRING,mh_display_trip_start DATE,mh_display_startlat STRING,mh_display_startlon STRING,mh_data_count_gyro_samples STRING,mh_star_rating_accel STRING,mh_star_rating_brake STRING,mh_star_rating_turn STRING,mh_star_rating_speeding STRING,mh_star_rating_phone_motion STRING,mh_is_night STRING,mh_battery_total_drain STRING,mh_battery_total_drain_duration_hours STRING,mh_score_smoothness STRING,mh_score_awareness STRING,mh_star_rating_night STRING,mh_star_rating_smoothness STRING,mh_star_rating_awareness STRING,mh_hide STRING,mh_data_count_tag_accel_samples STRING,mh_quat_i STRING,mh_quat_j STRING,mh_quat_k STRING,mh_quat_r STRING,mh_passenger_star_rating STRING,mh_suspension_damping_ratio STRING,mh_suspension_natural_frequency STRING,mh_suspension_fit_error STRING,mh_driving STRING,mh_trip_mode STRING,mh_classification_confidence STRING,mh_gk_trip_mode STRING,mh_gk_confidence STRING,mh_offroad_trip_mode STRING,mh_offroad_confidence STRING,mh_driver_confidence STRING,mh_timing_processing_preprocessing DOUBLE,mh_timing_processing_gatekeeper DOUBLE,mh_timing_processing_accelpipeline DOUBLE,mh_timing_processing_offroad DOUBLE,mh_timing_processing_suspension DOUBLE,mh_timing_processing_scoring DOUBLE,mh_timing_processing_hitchhiker DOUBLE,mh_data_count_obd_samples STRING,mh_data_count_pressure_samples STRING,mh_raw_sampling_mode STRING,mh_data_count_magnetometer_samples STRING,mh_location_disabled_date DATE";
    public static String stringMHL = "mhl_dataset_id LONG, mhl_mapmatch_history_id LONG";
    public static String stringSF = "sf_id LONG,sf_uploadtime DATE,sf_deviceid STRING,sf_driveid STRING,sf_state INT,sf_dest_server STRING,sf_companyid INT,sf_hardware_manufacturer STRING,sf_hardware_model STRING,sf_hardware_bootloader STRING,sf_hardware_build STRING,sf_hardware_carrier STRING,sf_android_fw_version STRING,sf_android_api_version STRING,sf_android_codename STRING,sf_android_baseband STRING,sf_raw_hardware_string STRING,sf_raw_os_string STRING,sf_utc_offset_with_dst STRING,sf_app_version STRING,sf_file_format STRING,sf_start_reason STRING,sf_stop_reason STRING,sf_previous_driveid STRING,sf_userid STRING,sf_tag_mac_address STRING,sf_tag_trip_number STRING,sf_primary_driver_app_user_id STRING,sf_tag_last_connection_number STRING,sf_gps_points_lsh_key_1 STRING,sf_gps_points_lsh_key_2 STRING,sf_gps_points_lsh_key_3 STRING,sf_hidden_by_support STRING";

    public static int mhBuckets = 4096, mhlBuckets = 64, sfBuckets = 2048;

    public static Map<Integer, Predicate> predicatesMH, predicatesSF;

    public static ParsedTupleList mhSample, mhlSample, sfSample;


    public static ParsedTupleList loadSample(String tableName) {

        ConfUtils cfg = new ConfUtils("/home/mdindex/yilu/mdindex/conf/cmt.properties");
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        // Load table info.
        Globals.loadTableInfo(tableName, cfg.getHDFS_WORKING_DIR(), fs);
        TableInfo tableInfo = Globals.getTableInfo(tableName);

        String pathToSample = cfg.getHDFS_WORKING_DIR() + "/" + tableInfo.tableName + "/sample";

        byte[] sampleBytes = HDFSUtils.readFile(fs, pathToSample);

        // read all the sample files and put them into the sample key set
        ParsedTupleList sample = new ParsedTupleList(tableInfo.getTypeArray());
        sample.unmarshall(sampleBytes, tableInfo.delimiter);

        return sample;
    }

    public static void init() {
        mhSample = loadSample(mh);
        mhlSample = loadSample(mhl);
        sfSample = loadSample(sf);
    }

    public static JoinRobustTree getAdaptDbIndexWithQ(String tableName, ParsedTupleList sample, JoinQuery q, int numBuckets, int depth) {

        MDIndex.Bucket.maxBucketId = 0;

        TableInfo tableInfo = Globals.getTableInfo(tableName);

        JoinRobustTree rt = new JoinRobustTree(tableInfo);
        rt.joinAttributeDepth = depth;
        rt.setMaxBuckets(numBuckets);
        rt.loadSample(sample);
        rt.initProbe(q);

        return rt;
    }



    public static Predicate getPredicate(Schema schema, String pred) {
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



    public static void initPredicates() {

        Schema schemaMH = Schema.createSchema(stringMH);
        Schema schemaSF = Schema.createSchema(stringSF);

        predicatesMH = new HashMap();
        predicatesSF = new HashMap();

        byte[] stringBytes = HDFSUtils.readFile(HDFSUtils.getFSByHadoopHome("/home/mdindex/hadoop-2.6.0"), "/user/yilu/cmt_queries.log");

        String queriesString = new String(stringBytes);
        String[] queries = queriesString.split("\n");


        for (int i=0; i<queries.length; i++) {
            String query = queries[i];
            String[] predicates = query.split(";");

            ArrayList<JoinQuery> q = new ArrayList<JoinQuery>();

            for (int j=0; j<predicates.length; j++) {
                if(predicates[j].startsWith(mh)){
                    Predicate p = getPredicate(schemaMH, predicates[j]);
                    if(!predicatesMH.containsKey(p.attribute)){
                        predicatesMH.put(p.attribute, p);
                    }
                } else {
                    Predicate p = getPredicate(schemaSF, predicates[j]);
                    if(!predicatesSF.containsKey(p.attribute)){
                        predicatesSF.put(p.attribute, p);
                    }
                }
            }
        }
    }



    public static JoinRobustTree getMH() {
        Schema schemaMH = Schema.createSchema(stringMH);
        JoinQuery q_mh = new JoinQuery(mh, schemaMH.getAttributeId("mh_id") , predicatesMH.values().toArray(new Predicate[predicatesMH.size()]));
        return getAdaptDbIndexWithQ(mh, mhSample, q_mh, mhBuckets, 6);
    }

    public static JoinRobustTree getMHL() {
        Schema schemaMHL = Schema.createSchema(stringMHL);
        JoinQuery q_mhl = new JoinQuery(mhl, schemaMHL.getAttributeId("mhl_mapmatch_history_id") ,new Predicate[]{});
        return getAdaptDbIndexWithQ(mhl, mhlSample, q_mhl, mhlBuckets, 6);
    }

    public static JoinRobustTree getSF() {
        Schema schemaMH = Schema.createSchema(stringSF);
        JoinQuery q_sf = new JoinQuery(sf, schemaMH.getAttributeId("sf_id") , predicatesSF.values().toArray(new Predicate[predicatesSF.size()]));
        return getAdaptDbIndexWithQ(sf, sfSample, q_sf, sfBuckets, 6);
    }

    public static void main(String[] args) {

        init();

        String tableName = args[0];
        String outpath = args[1];

        JoinRobustTree tree = null;

        switch (tableName) {
            case "mh":
                tree = getMH();
                break;
            case "mhl":
                tree = getMHL();
                break;
            case "sf":
                tree = getSF();
                break;
        }

        // save index to hdfs


        Configuration conf = new Configuration();
        String coreSitePath = "/home/mdindex/hadoop-2.6.0/etc/hadoop/core-site.xml";
        conf.addResource(new Path(coreSitePath));
        Path path = new Path(outpath);
        try {
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(path)) {
                fs.delete(path, false);
            }
            System.out.println("creating file " + outpath);
            HDFSUtils.safeCreateFile(fs, outpath, (short) 1);

            byte[] indexBytes = tree.marshall();
            System.out.println("writing file " + outpath);
            HDFSUtils.writeFile(fs, outpath, (short) 1, indexBytes, 0, indexBytes.length, false);

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Done!");
    }
}
