GCP_PROJECT_ID=$(jq -r '.project_id' ${KEY_FILE_LOCATION})
HDFS_PORT=9000
FQ_BQ_AVRO_HDFS_DIR=hdfs://${MORPHL_SERVER_IP_ADDRESS}:${HDFS_PORT}/${BQ_AVRO_HDFS_DIR}
DATA_ID=ga_sessions_$(echo ${DAY_OF_DATA_CAPTURE} | sed 's/-//g')
gcloud config set project ${GCP_PROJECT_ID}
gcloud auth activate-service-account --key-file=${KEY_FILE_LOCATION}
bq ls &>/dev/null
bq extract --destination_format=AVRO ${SRC_BQ_DATASET}.${DATA_ID} gs://${DEST_GCS_BUCKET}/${DATA_ID}.avro
gsutil cp gs://${DEST_GCS_BUCKET}/${DATA_ID}.avro /opt/landing/
hdfs dfs -mkdir -p ${FQ_BQ_AVRO_HDFS_DIR}
hdfs dfs -copyFromLocal -f /opt/landing/${DATA_ID}.avro ${FQ_BQ_AVRO_HDFS_DIR}/${DATA_ID}.avro
# rm /opt/landing/${DATA_ID}.avro
