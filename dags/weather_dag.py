from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
# Import the functions from plugins scripts
from pull_data import fetch_weather_data
from transform_data import transform_data_into_csv
# Import the functions from the train_models module
from train_models import prepare_data, train_and_save_model, compute_model_score
from train_models import LinearRegression, DecisionTreeRegressor, RandomForestRegressor

# Define the functions used in the DAG
def train_best_model(score_lr, score_dt, score_rf, **context):
    X, y = prepare_data('/app/clean_data/fulldata.csv')

    best_score = max(score_lr, score_dt, score_rf)

    if best_score == score_lr:
        model = LinearRegression()
    elif best_score == score_dt:
        model = DecisionTreeRegressor()
    else:
        model = RandomForestRegressor()

    train_and_save_model(
        model,
        X,
        y,
        '/app/clean_data/best_model.pickle'
    )

def train_and_evaluate_model(model_type):
    X, y = prepare_data('/app/clean_data/fulldata.csv')
    model = model_type()
    score = compute_model_score(model, X, y)
    return score

# Define the DAG
with DAG('weather_data_pipeline',
         schedule_interval='*/1 * * * *',
         tags=['exam', 'datascientest'],
         default_args={
             'owner': 'airflow',
             'start_date': days_ago(0, minute=1),
         },
         catchup=False) as dag:

    ## Task 1: Fetch weather data from API
    fetch_weather_data = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
    )

    ## Task 2: Transform data into CSV (last 20 files)
    transform_data_csv_20 = PythonOperator(
        task_id='transform_data_csv_20',
        python_callable=transform_data_into_csv,
        op_kwargs={'n_files': 20, 'filename': 'data.csv'},
    )

    ## Task 3: Transform data into CSV (all files)
    transform_data_csv_all = PythonOperator(
        task_id='transform_data_csv_all',
        python_callable=transform_data_into_csv,
        op_kwargs={'n_files': None, 'filename': 'fulldata.csv'},
    )

    ## Task 4: Train and evaluate models
    with TaskGroup('train_and_evaluate_models') as train_and_evaluate_models_group:
        train_linear_regression = PythonOperator(
            task_id='train_linear_regression',
            python_callable=train_and_evaluate_model,
            op_kwargs={'model_type': LinearRegression},
            do_xcom_push=True,
        )

        train_decision_tree = PythonOperator(
            task_id='train_decision_tree',
            python_callable=train_and_evaluate_model,
            op_kwargs={'model_type': DecisionTreeRegressor},
            do_xcom_push=True,
        )

        train_random_forest = PythonOperator(
            task_id='train_random_forest',
            python_callable=train_and_evaluate_model,
            op_kwargs={'model_type': RandomForestRegressor},
            do_xcom_push=True,
        )

    ## Task 5: Train and save the best model
    train_best_model = PythonOperator(
        task_id='train_best_model',
        python_callable=train_best_model,
        op_kwargs={'score_lr': "{{ ti.xcom_pull(task_ids='train_and_evaluate_models.train_linear_regression', key='return_value') }}",
                   'score_dt': "{{ ti.xcom_pull(task_ids='train_and_evaluate_models.train_decision_tree', key='return_value') }}",
                   'score_rf': "{{ ti.xcom_pull(task_ids='train_and_evaluate_models.train_random_forest', key='return_value') }}"},
    )

    # Set task dependencies
    fetch_weather_data >> [transform_data_csv_20, transform_data_csv_all]
    transform_data_csv_all >> train_and_evaluate_models_group >> train_best_model