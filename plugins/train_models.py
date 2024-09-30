import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump


def compute_model_score(model, X, y):
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()

    return model_score


def train_and_save_model(model, X, y, path_to_model='./app/model.pckl'):
    # training the model
    model.fit(X, y)
    # saving model
    print(str(model), 'saved at ', path_to_model)
    dump(model, path_to_model)


def prepare_data(path_to_data='/app/clean_data/fulldata.csv'):
    # reading data
    df = pd.read_csv(path_to_data)
    # ordering data according to city and date
    df = df.sort_values(['city', 'date'], ascending=True)

    dfs = []

    for c in df['city'].unique():
        df_temp = df[df['city'] == c].copy()  # Create a copy to avoid SettingWithCopyWarning

        # creating target
        df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)

        # creating features
        for i in range(1, 10):
            df_temp.loc[:, f'temp_m-{i}'] = df_temp['temperature'].shift(-i)

        # deleting null values
        df_temp = df_temp.dropna()

        dfs.append(df_temp)

    # concatenating datasets
    df_final = pd.concat(dfs, axis=0, ignore_index=False)

    # deleting date variable
    df_final = df_final.drop(['date'], axis=1)

    # creating dummies for city variable
    df_final = pd.get_dummies(df_final)

    features = df_final.drop(['target'], axis=1)
    target = df_final['target']

    return features, target


# if __name__ == '__main__':

#     # this is Task 4 within the DAG
#     X, y = prepare_data('./clean_data/fulldata.csv')
#     # X, y = prepare_data('/home/ubuntu/exam_airflow_AFTRETH/clean_data/fulldata.csv')

#     score_lr = compute_model_score(LinearRegression(), X, y)
#     score_dt = compute_model_score(DecisionTreeRegressor(), X, y)
#     score_rf = compute_model_score(RandomForestRegressor(), X, y)

#     best_score = max(score_lr, score_dt, score_rf)

#     if best_score == score_lr:
#         model = LinearRegression()
#     elif best_score == score_dt:
#         model = DecisionTreeRegressor()
#     else:
#         model = RandomForestRegressor()

#     train_and_save_model(
#         model,
#         X,
#         y,
#         '/app/clean_data/best_model.pickle'
#         # '/home/ubuntu/exam_airflow_AFTRETH/clean_data/best_model.pickle'
#         )