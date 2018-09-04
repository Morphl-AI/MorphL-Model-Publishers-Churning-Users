import dask.dataframe as dd
import numpy as np
from os import getenv
from sklearn.externals import joblib
from sklearn.preprocessing.data import PowerTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, Normalizer
from sklearn.impute import SimpleImputer


class ScalerTransformer:
    def __init__(self, dask_df):

        self.num_labels = ['pageviews', 'unique_pageviews',
                           'u_sessions', 'entrances', 'bounces', 'exits', 'session_count']
        self.gauss_labels = ['session_duration', 'time_on_page']
        self.cat_labels = ['is_desktop', 'is_mobile', 'is_tablet']
        self.dask_df = dask_df
        self.day_as_str = getenv('DAY_AS_STR')
        self.unique_hash = getenv('UNIQUE_HASH')
        self.training_or_prediction = getenv('TRAINING_OR_PREDICTION')
        self.models_dir = getenv('MODELS_DIR')

    def get_transformed_numeric_data(self):
        updated_data_bc = {}

        for column in self.num_labels:
            # Add 1 to shift data to right and avoid zeros.
            data_in_column = self.dask_df[column]
            data = data_in_column.compute().values.reshape(-1, 1) + 1

            pkl_file = f'{self.models_dir}/{self.day_as_str}_{self.unique_hash}_ga_chp_box_cox_{column}.pkl'
            if self.training_or_prediction == 'prediction':
                box_cox = joblib.load(pkl_file)
                data_bc = box_cox.transform(data)
            else:
                box_cox = PowerTransformer(method='box-cox')
                box_cox.fit(data)
                joblib.dump(box_cox, pkl_file)
                data_bc = box_cox.transform(data)

            updated_data_bc[column] = data_bc.T.tolist()[0]

        bc_list = []

        for column in self.num_labels:
            bc_list.append(updated_data_bc[column])

        bc_array = np.array(bc_list).transpose()

        transformed_bc_data = dd.from_array(bc_array, columns=self.num_labels)

        pkl_file = f'{self.models_dir}/{self.day_as_str}_{self.unique_hash}_ga_chp_pipeline.pkl'
        if self.training_or_prediction == 'prediction':
            pipeline = joblib.load(pkl_file)
            transformed_numeric = pipeline.transform(transformed_bc_data)
        else:
            pipeline = Pipeline([
                # Replace zeros with mean value.
                ('imputer', SimpleImputer(strategy="mean", missing_values=0)),
                # Scale in interval (0, 1)
                ('normalizer', Normalizer()),
                # Substract mean and divide by variance.
                ('scaler', StandardScaler()),
            ])

            pipeline.fit(transformed_bc_data)
            joblib.dump(pipeline, pkl_file)
            transformed_numeric = pipeline.transform(transformed_bc_data)

        return dd.from_array(transformed_numeric, columns=self.num_labels)

    def get_transformed_gauss_data(self):
        logged_data = self.dask_df[self.gauss_labels]

        # Apply log
        for column in self.gauss_labels:
            logged_data[column] = np.log1p(self.dask_df[column])

        logged_data_array = np.array(logged_data)
        return dd.from_array(logged_data_array, columns=self.gauss_labels)

    def get_churned_data(self):
        churned_data_array = np.array(self.dask_df['churned'])
        return dd.from_array(churned_data_array, columns=['churned'])

    def get_cat_data(self):
        cat_data_array = np.array(self.dask_df[self.cat_labels])
        return dd.from_array(cat_data_array, columns=self.cat_labels)

    def get_client_id_data(self):
        client_id_data_array = np.array(self.dask_df['client_id'])
        return dd.from_array(client_id_data_array, columns=['client_id'])

    def get_transformed_data(self):

        concat_list = []

        if self.training_or_prediction == 'prediction':
            concat_list.append(self.get_client_id_data())

        concat_list.append(self.get_transformed_numeric_data())
        concat_list.append(self.get_transformed_gauss_data())
        concat_list.append(self.get_cat_data())

        if self.training_or_prediction == 'training':
            concat_list.append(self.get_churned_data())

        return dd.concat(concat_list, axis=1)

