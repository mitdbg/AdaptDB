#fab setup:localjoin_lineitem create_table_info
#fab setup:localjoin_lineitem bulk_sample_gen
#fab setup:localjoin_lineitem create_robust_tree
#fab setup:localjoin_lineitem write_partitions


fab setup:join_orders create_table_info
fab setup:join_orders bulk_sample_gen
fab setup:join_orders create_robust_tree
fab setup:join_orders write_partitions


#fab setup:localjoin_part create_table_info
#fab setup:localjoin_part bulk_sample_gen
#fab setup:localjoin_part create_robust_tree
#fab setup:localjoin_part write_partitions


#fab setup:localjoin_supplier create_table_info
#fab setup:localjoin_supplier bulk_sample_gen
#fab setup:localjoin_supplier create_robust_tree
#fab setup:localjoin_supplier write_partitions


#fab setup:localjoin_customer create_table_info
#fab setup:localjoin_customer bulk_sample_gen
#fab setup:localjoin_customer create_robust_tree
#fab setup:localjoin_customer write_partitions


#fab setup:localjoin_mh create_table_info
#fab setup:localjoin_mh bulk_sample_gen
#fab setup:localjoin_mh create_robust_tree
#fab setup:localjoin_mh write_partitions

#fab setup:localjoin_mhl create_table_info
#fab setup:localjoin_mhl bulk_sample_gen
#fab setup:localjoin_mhl create_robust_tree
#fab setup:localjoin_mhl write_partitions

#fab setup:localjoin_sf create_table_info
#fab setup:localjoin_sf bulk_sample_gen
#fab setup:localjoin_sf create_robust_tree
#fab setup:localjoin_sf write_partitions

