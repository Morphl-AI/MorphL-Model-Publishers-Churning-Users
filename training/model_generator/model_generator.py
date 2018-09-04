from os import getenv
from sklearn.model_selection import train_test_split
from keras.optimizers import RMSprop
from keras.models import Sequential
from keras.layers import Dense
import json


class ModelGenerator:
    def __init__(self, dask_df):
        self.day_as_str = getenv('DAY_AS_STR')
        self.unique_hash = getenv('UNIQUE_HASH')
        self.models_dir = getenv('MODELS_DIR')

        self.train_validation_set, self.test_set = dask_df.random_split(
            [0.8, 0.2], random_state=42)
        self.train_set, self.validation_set = self.train_validation_set.random_split(
            [0.8, 0.2], random_state=42)

    def get_XY_train_test_validation_sets(self):
        sets = {}

        sets['train_X'] = self.train_set.drop('churned', axis=1).compute()
        sets['train_Y'] = self.train_set['churned'].copy().compute()

        sets['validation_X'] = self.validation_set.drop(
            'churned', axis=1).compute()
        sets['validation_Y'] = self.validation_set['churned'].copy().compute()

        sets['test_X'] = self.test_set.drop('churned', axis=1).compute()
        sets['test_Y'] = self.test_set['churned'].copy().compute()

        return sets

    def generate_and_save_model(self):
        model = Sequential()
        sets = self.get_XY_train_test_validation_sets()
        input_dim = len(sets['test_X'].columns)

        model.add(Dense(1, input_dim=input_dim, activation='sigmoid'))

        rmsprop = RMSprop(
            lr=0.001, rho=0.9, epsilon=None, decay=0.0)

        model.compile(optimizer=rmsprop, loss='binary_crossentropy',
                      metrics=['accuracy'])

        model.fit(sets['train_X'], sets['train_Y'], epochs=50, verbose=0,
                  validation_data=(sets['validation_X'], sets['validation_Y']))

        score = model.evaluate(sets['test_X'], sets['test_Y'], verbose=0)

        scores = {'score': score[0], 'accuracy': score[1]}

        churn_scores_json_file = f'{self.models_dir}/{self.day_as_str}_{self.unique_hash}_ga_chp_churn_scores.json'
        with open(churn_scores_json_file, 'w') as writer:
            writer.write(json.dumps(scores))

        churn_model_file = f'{self.models_dir}/{self.day_as_str}_{self.unique_hash}_ga_chp_churn_model.h5'
        model.save(churn_model_file)

