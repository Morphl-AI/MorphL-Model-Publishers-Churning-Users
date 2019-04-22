from os import getenv

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from keras.models import load_model

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

import numpy as np

MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')

DAY_AS_STR = getenv('DAY_AS_STRING')
MODEL_DAY_AS_STR = getenv('MODEL_DAY_AS_STR')
UNIQUE_HASH = getenv('UNIQUE_HASH')

MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
MASTER_URL = 'local[*]'
APPLICATION_NAME = 'batch-inference'

HDFS_PORT = 9000
HDFS_DIR_INPUT = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{MODEL_DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_scaled_features_prediction'

CASS_REQ_TIMEOUT = 3600.0


def update_predictions_statistics(predictions_df):

    auth_provider = PlainTextAuthProvider(
        username=MORPHL_CASSANDRA_USERNAME,
        password=MORPHL_CASSANDRA_PASSWORD
    )

    cluster = Cluster(
        [MORPHL_SERVER_IP_ADDRESS], auth_provider=auth_provider)

    spark_session_cass = cluster.connect(MORPHL_CASSANDRA_KEYSPACE)

    prep_stmt_predictions_statistics = spark_session_cass.prepare(
        'UPDATE ga_chp_predictions_statistics SET loyal=loyal+?, neutral=neutral+?, churning=churning+?, lost=lost+? WHERE prediction_date=?')

    loyal = predictions_df.filter('prediction < 0.4').count()
    neutral = predictions_df.filter(
        '(prediction > 0.4) and (prediction <= 0.6)').count()
    churning = predictions_df.filter(
        '(prediction > 0.6) and (prediction <= 0.9)').count()
    lost = predictions_df.filter(
        '(prediction > 0.9) and (prediction <= 1.0)').count()

    bind_list = [loyal, neutral, churning, lost, DAY_AS_STR]

    spark_session_cass.execute(
        prep_stmt_predictions_statistics, bind_list, timeout=CASS_REQ_TIMEOUT)


def batch_inference(partition):
    churn_model_file = f'/opt/models/{MODEL_DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_churn_model.h5'
    churn_model = load_model(churn_model_file)

    result = []

    for row in partition:
        array_to_predict = np.array([[row.pageviews,
                                      row.unique_pageviews,
                                      row.hits,
                                      row.u_sessions,
                                      row.entrances,
                                      row.bounces,
                                      row.exits,
                                      row.session_count,
                                      row.is_desktop,
                                      row.is_mobile,
                                      row.is_tablet,
                                      row.session_duration,
                                      row.time_on_page
                                      ]])

        result.append((row.client_id, ) +
                      tuple(churn_model.predict(array_to_predict).tolist()[0]))

    return iter(result)


def main():
    spark_session = (
        SparkSession.builder
        .appName(APPLICATION_NAME)
        .master(MASTER_URL)
        .config('spark.cassandra.connection.host', MORPHL_SERVER_IP_ADDRESS)
        .config('spark.cassandra.auth.username', MORPHL_CASSANDRA_USERNAME)
        .config('spark.cassandra.auth.password', MORPHL_CASSANDRA_PASSWORD)
        .config('spark.sql.shuffle.partitions', 16)
        .getOrCreate()
    )

    log4j = spark_session.sparkContext._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    df = (spark_session.read.parquet(HDFS_DIR_INPUT))

    predictions_df = (df
                      .rdd
                      .mapPartitions(batch_inference)
                      .repartition(32)
                      .toDF()
                      .withColumnRenamed('_1', 'client_id')
                      .withColumnRenamed('_2', 'prediction')
                      )

    save_options_ga_chp_predictions = {
        'keyspace': MORPHL_CASSANDRA_KEYSPACE,
        'table': 'ga_chp_predictions'
    }

    save_options_ga_chp_predictions_by_date = {
        'keyspace': MORPHL_CASSANDRA_KEYSPACE,
        'table': 'ga_chp_predictions_by_prediction_date'
    }

    (predictions_df
     .write
     .format('org.apache.spark.sql.cassandra')
     .mode('append')
     .options(**save_options_ga_chp_predictions)
     .save()
     )

    predictions_by_date_df = predictions_df.withColumn(
        'prediction_date', lit(DAY_AS_STR))

    (predictions_by_date_df
     .write
     .format('org.apache.spark.sql.cassandra')
     .mode('append')
     .options(**save_options_ga_chp_predictions_by_date)
     .save()
     )

    update_predictions_statistics(predictions_df)


if __name__ == '__main__':
    main()
