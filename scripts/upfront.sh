fab setup:lineitem100 create_table_info
fab setup:lineitem100 bulk_sample_gen
fab setup:lineitem100 create_join_robust_tree
fab setup:lineitem100 write_join_partitions


fab setup:orders100 create_table_info
fab setup:orders100 bulk_sample_gen
fab setup:orders100 create_join_robust_tree
fab setup:orders100 write_join_partitions


fab setup:part100 create_table_info
fab setup:part100 bulk_sample_gen
fab setup:part100 create_join_robust_tree
fab setup:part100 write_join_partitions


fab setup:supplier100 create_table_info
fab setup:supplier100 bulk_sample_gen
fab setup:supplier100 create_join_robust_tree
fab setup:supplier100 write_join_partitions


fab setup:customer100 create_table_info
fab setup:customer100 bulk_sample_gen
fab setup:customer100 create_join_robust_tree
fab setup:customer100 write_join_partitions


#fab setup:join_mh create_table_info
#fab setup:join_mh bulk_sample_gen
#fab setup:join_mh create_join_robust_tree
#fab setup:join_mh write_join_partitions

#fab setup:join_mhl create_table_info
#fab setup:join_mhl bulk_sample_gen
#fab setup:join_mhl create_join_robust_tree
#fab setup:join_mhl write_join_partitions

#fab setup:join_sf create_table_info
#fab setup:join_sf bulk_sample_gen
#fab setup:join_sf create_join_robust_tree
#fab setup:join_sf write_join_partitions

