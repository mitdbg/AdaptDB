package core.adapt.spark;

import core.adapt.JoinQuery;
import core.adapt.Predicate;
import core.adapt.spark.join.JoinPlanner;
import core.common.globals.Schema;
import core.utils.TypeUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;

/**
 * Created by ylu on 5/6/16.
 */
public class JoinPlannerTest {
    private static Random rand = new Random();

    private static String lineitem = "lineitem", orders = "orders", customer = "customer", supplier = "supplier", part = "part";

    private static String MH = "mh", MHL = "mhl", SF = "sf";

    private static String[] mktSegmentVals = new
            String[]{"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"};
    private static String[] regionNameVals = new
            String[]{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};
    private static String[] partTypeVals = new
            String[]{"BRASS", "COPPER", "NICKEL", "STEEL", "TIN"};
    private static String[] shipModeVals = new
            String[]{"AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"};

    private static Schema schemaCustomer, schemaLineitem, schemaOrders, schemaPart, schemaSupplier;
    private static String stringCustomer, stringLineitem, stringOrders, stringPart, stringSupplier;

    private static Schema schemaMH, schemaMHL, schemaSF;
    private static String stringMH, stringMHL, stringSF;

    private static void setup() {
        stringLineitem = "l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string";
        schemaLineitem = Schema.createSchema(stringLineitem);
        stringOrders = "o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate date, o_orderpriority string, o_clerk string, o_shippriority int";
        schemaOrders = Schema.createSchema(stringOrders);
        stringCustomer = "c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string";
        schemaCustomer = Schema.createSchema(stringCustomer);
        stringSupplier = "s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string";
        schemaSupplier = Schema.createSchema(stringSupplier);
        stringPart = "p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double";
        schemaPart = Schema.createSchema(stringPart);
        stringMH = "mh_id LONG,mh_dataset_id STRING,mh_uploadtime DATE,mh_runtime STRING,mh_trip_start DATE,mh_data_count_minutes STRING,mh_data_count_accel_samples STRING,mh_data_count_netloc_samples STRING,mh_data_count_gps_samples STRING,mh_observed_sample_rate STRING,mh_distance_mapmatched_km STRING,mh_distance_gps_km STRING,mh_ground_truth_present STRING,mh_timing_mapmatch DOUBLE,mh_distance_pct_path_error STRING,mh_build_version STRING,mh_timing_queue_wait DOUBLE,mh_data_trip_length STRING,mh_battery_maximum_level STRING,mh_battery_minimum_level STRING,mh_battery_drain_rate_per_hour STRING,mh_battery_plugged_duration_hours STRING,mh_battery_delay_from_drive_end_seconds STRING,mh_startlat STRING,mh_startlon STRING,mh_endlat STRING,mh_endlon STRING,mh_data_count_output_gps_speeding_points STRING,mh_speeding_slow_gps_points STRING,mh_speeding_10kmh_gps_points STRING,mh_speeding_20kmh_gps_points STRING,mh_speeding_40kmh_gps_points STRING,mh_speeding_80kmh_gps_points STRING,mh_output_accel_valid_minutes STRING,mh_output_gps_moving_minutes STRING,mh_output_gps_moving_and_accel_valid_minutes STRING,mh_data_time_till_first_gps_minutes STRING,mh_score_di_accel STRING,mh_score_di_brake STRING,mh_score_di_turn STRING,mh_score_di_car_motion STRING,mh_score_di_phone_motion STRING,mh_score_di_speeding STRING,mh_score_di_night STRING,mh_star_rating STRING,mh_trip_end DATE,mh_score_di_car_motion_with_accel STRING,mh_score_di_car_motion_with_speeding STRING,mh_score_di_distance_km_with_accel STRING,mh_score_di_distance_km_with_speeding STRING,mh_score_accel_per_sec_ntile STRING,mh_score_brake_per_sec_ntile STRING,mh_score_turn_per_sec_ntile STRING,mh_score_speeding_per_sec_ntile STRING,mh_score_phone_motion_per_sec_ntile STRING,mh_score_accel_per_km_ntile STRING,mh_score_brake_per_km_ntile STRING,mh_score_turn_per_km_ntile STRING,mh_score_speeding_per_km_ntile STRING,mh_score_phone_motion_per_km_ntile STRING,mh_score STRING,mh_distance_prepended_km STRING,mh_recording_start DATE,mh_score_di_distance_km STRING,mh_recording_end DATE,mh_recording_startlat STRING,mh_recording_startlon STRING,mh_display_distance_km STRING,mh_display_trip_start DATE,mh_display_startlat STRING,mh_display_startlon STRING,mh_data_count_gyro_samples STRING,mh_star_rating_accel STRING,mh_star_rating_brake STRING,mh_star_rating_turn STRING,mh_star_rating_speeding STRING,mh_star_rating_phone_motion STRING,mh_is_night STRING,mh_battery_total_drain STRING,mh_battery_total_drain_duration_hours STRING,mh_score_smoothness STRING,mh_score_awareness STRING,mh_star_rating_night STRING,mh_star_rating_smoothness STRING,mh_star_rating_awareness STRING,mh_hide STRING,mh_data_count_tag_accel_samples STRING,mh_quat_i STRING,mh_quat_j STRING,mh_quat_k STRING,mh_quat_r STRING,mh_passenger_star_rating STRING,mh_suspension_damping_ratio STRING,mh_suspension_natural_frequency STRING,mh_suspension_fit_error STRING,mh_driving STRING,mh_trip_mode STRING,mh_classification_confidence STRING,mh_gk_trip_mode STRING,mh_gk_confidence STRING,mh_offroad_trip_mode STRING,mh_offroad_confidence STRING,mh_driver_confidence STRING,mh_timing_processing_preprocessing DOUBLE,mh_timing_processing_gatekeeper DOUBLE,mh_timing_processing_accelpipeline DOUBLE,mh_timing_processing_offroad DOUBLE,mh_timing_processing_suspension DOUBLE,mh_timing_processing_scoring DOUBLE,mh_timing_processing_hitchhiker DOUBLE,mh_data_count_obd_samples STRING,mh_data_count_pressure_samples STRING,mh_raw_sampling_mode STRING,mh_data_count_magnetometer_samples STRING,mh_location_disabled_date DATE";
        schemaMH = Schema.createSchema(stringMH);
        stringMHL = "mhl_dataset_id LONG, mhl_mapmatch_history_id LONG";
        schemaMHL = Schema.createSchema(stringMHL);
        stringSF = "sf_id LONG,sf_uploadtime DATE,sf_deviceid STRING,sf_driveid STRING,sf_state INT,sf_dest_server STRING,sf_companyid INT,sf_hardware_manufacturer STRING,sf_hardware_model STRING,sf_hardware_bootloader STRING,sf_hardware_build STRING,sf_hardware_carrier STRING,sf_android_fw_version STRING,sf_android_api_version STRING,sf_android_codename STRING,sf_android_baseband STRING,sf_raw_hardware_string STRING,sf_raw_os_string STRING,sf_utc_offset_with_dst STRING,sf_app_version STRING,sf_file_format STRING,sf_start_reason STRING,sf_stop_reason STRING,sf_previous_driveid STRING,sf_userid STRING,sf_tag_mac_address STRING,sf_tag_trip_number STRING,sf_primary_driver_app_user_id STRING,sf_tag_last_connection_number STRING,sf_gps_points_lsh_key_1 STRING,sf_gps_points_lsh_key_2 STRING,sf_gps_points_lsh_key_3 STRING,sf_hidden_by_support STRING";
        schemaSF = Schema.createSchema(stringSF);


        rand.setSeed(0);
    }

    public static void main(String[] args){

        setup();

        int rand_3 = rand.nextInt(mktSegmentVals.length);
        String c_mktsegment = mktSegmentVals[rand_3];
        Calendar c = new GregorianCalendar();
        int dateOffset = (int) (rand.nextFloat() * (31 + 28 + 31));
        c.set(1995, Calendar.MARCH, 01);
        c.add(Calendar.DAY_OF_MONTH, dateOffset);
        TypeUtils.SimpleDate d3 = new TypeUtils.SimpleDate(c.get(Calendar.YEAR),
                c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH));


        Predicate p1_3 = new Predicate(schemaCustomer.getAttributeId("c_mktsegment"), TypeUtils.TYPE.STRING, c_mktsegment, Predicate.PREDTYPE.LEQ);
        Predicate p2_3 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TypeUtils.TYPE.DATE, d3, Predicate.PREDTYPE.LT);
        Predicate p3_3 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TypeUtils.TYPE.DATE, d3, Predicate.PREDTYPE.GT);

        JoinQuery q_c = null;
        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_orderkey"), new Predicate[]{p2_3});
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p3_3});



        Configuration conf = new Configuration();


        conf.set("WORKING_DIR", "/user/yilu");
        conf.set("HADOOP_HOME", "/Users/ylu/Documents/workspace/hadoop-2.6.0");
        conf.set("ZOOKEEPER_HOSTS","localhost");
        conf.setInt("HDFS_REPLICATION_FACTOR", 1);


        conf.set("DATASET1", lineitem);
        conf.set("DATASET2", orders);


        conf.set("DATASET1_QUERY", q_l.toString());
        conf.set("DATASET2_QUERY", q_o.toString());


        JoinPlanner planner = new JoinPlanner(conf);

    }
}
