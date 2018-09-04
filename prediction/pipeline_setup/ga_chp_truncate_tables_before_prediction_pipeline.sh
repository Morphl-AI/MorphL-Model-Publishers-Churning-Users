cqlsh ${MORPHL_SERVER_IP_ADDRESS} -u morphl -p ${MORPHL_CASSANDRA_PASSWORD} \
  -f /opt/ga_chp/prediction/pipeline_setup/ga_chp_truncate_tables_before_prediction_pipeline.cql

