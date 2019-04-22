from os import getenv
import json

from pyspark.sql import SparkSession


from keras.models import Sequential
from keras.optimizers import RMSprop
from keras.layers import Dense

MODEL_DAY_AS_STR = getenv('MODEL_DAY_AS_STR')
UNIQUE_HASH = getenv('UNIQUE_HASH')
MODELS_DIR = getenv('MODELS_DIR')
MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')

HDFS_PORT = 9000
HDFS_DIR_INPUT = f'hdfs://{MORPHL_SERVER_IP_ADDRESS}:{HDFS_PORT}/{MODEL_DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_scaled_features_training'
JSON_DIR_SCORES = f'{MODELS_DIR}/{MODEL_DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_churn_scores.json'
MODEL_FILE_DIR = f'{MODELS_DIR}/{MODEL_DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_churn_model.h5'


def main():
    spark_session = (
        SparkSession.builder
        .appName('model_generator')
        .master('local[*]')
        .config('spark.sql.shuffle.partitions', 16)
        .config('parquet.enable.summary-metadata', 'true')
        .getOrCreate())

    df = spark_session.read.parquet(HDFS_DIR_INPUT)

    train_validation_set, test_set = df.randomSplit(
        [0.8, 0.2], 42)
    train_set, validation_set = train_validation_set.randomSplit(
        [0.8, 0.2], 42)

    sets = {}

    # All sets are computed so we can operate on them. The output label 'churned
    # is dropped and placed into a separate set.
    sets['train_X'] = train_set.drop(
        'churned').toPandas().drop('index', axis=1)
    sets['train_Y'] = train_set.select('churned').toPandas()

    sets['validation_X'] = validation_set.drop(
        'churned').toPandas().drop('index', axis=1)
    sets['validation_Y'] = validation_set.select('churned').toPandas()

    sets['test_X'] = test_set.drop('churned').toPandas().drop('index', axis=1)
    sets['test_Y'] = test_set.select('churned').toPandas()

    model = Sequential()

    # Determine the number of input variables.
    input_dim = len(sets['test_X'].columns)

    # Add a layer to the model with a sigmoid activation.
    model.add(Dense(1, input_dim=input_dim, activation='sigmoid'))

    # Initialize an RMSprop optimizer.
    rmsprop = RMSprop(
        lr=0.001, rho=0.9, epsilon=None, decay=0.0)

    # Configure the model for training, specifing the loss function as binary crossentropy
    # and the metric as accuracy.
    model.compile(optimizer=rmsprop, loss='binary_crossentropy',
                  metrics=['accuracy'])

    # Train the model using the training and validation sets.
    model.fit(sets['train_X'], sets['train_Y'], epochs=50, verbose=0,
              validation_data=(sets['validation_X'], sets['validation_Y']))

    # Evaluate the model using the test set.
    score = model.evaluate(sets['test_X'], sets['test_Y'], verbose=0)

    scores = {'loss': score[0], 'accuracy': score[1]}

    # Save the evaluation scores to a .json file who's name and path are made up of 'model_day_as_str', 'unique_hash' and 'model_dir' respectively.
    with open(JSON_DIR_SCORES, 'w') as writer:
        writer.write(json.dumps(scores))

    # Save the model in a similar way.
    model.save(MODEL_FILE_DIR)


if __name__ == '__main__':
    main()
