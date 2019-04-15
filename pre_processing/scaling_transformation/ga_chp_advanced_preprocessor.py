from os import getenv
from distributed import Client
from pyspark.sql import SparkSession
from scaler_transformer import ScalerTransformer

MODEL_DAY_AS_STR = getenv('MODEL_DAY_AS_STR')
UNIQUE_HASH = getenv('UNIQUE_HASH')

MASTER_URL = 'local[*]'
APPLICATION_NAME = 'preprocessor'

TRAINING_OR_PREDICTION = getenv('TRAINING_OR_PREDICTION')

MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')

HDFS_PORT = 9000
HDFS_DIR_INPUT_TRAINING = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{MODEL_DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_preproc_training'
HDFS_DIR_OUTPUT_TRAINING = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{MODEL_DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_scaled_features_training'
HDFS_DIR_INPUT_PREDICTION = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{MODEL_DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_preproc_prediction'
HDFS_DIR_OUTPUT_PREDICTION = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{MODEL_DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_scaled_features_prediction'


def process_dataframe(client, spark_session, hdfs_dir_input, hdfs_dir_output):
    data_df = spark_session.read.parquet(hdfs_dir_input)
    st = ScalerTransformer(data_df)
    scaled_features = st.get_transformed_data()
    scaled_features.repartition(npartitions=32).to_parquet(hdfs_dir_output)


def main():
    spark_session = (
        SparkSession.builder
        .appName(APPLICATION_NAME)
        .master(MASTER_URL)
        .config('spark.sql.shuffle.partitions', 16)
        .config('parquet.enable.summary-metadata', 'true')
        .getOrCreate())

    log4j = spark_session.sparkContext._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    client = Client()
    if TRAINING_OR_PREDICTION == 'training':
        process_dataframe(client, spark_session, HDFS_DIR_INPUT_TRAINING,
                          HDFS_DIR_OUTPUT_TRAINING)
    else:
        process_dataframe(client, spark_session, HDFS_DIR_INPUT_PREDICTION,
                          HDFS_DIR_OUTPUT_PREDICTION)


if __name__ == '__main__':
    main()
