import pandas as pd
import os
import json

def transform_data_into_csv(n_files=None, filename='data.csv'):
    parent_folder = '/app/raw_files'
    files = sorted(os.listdir(parent_folder), reverse=True)
    if n_files:
        files = files[:n_files]

    dfs = []

    for f in files:
        with open(os.path.join(parent_folder, f), 'r') as file:
            data = json.load(file)  # Load the JSON data correctly
            if isinstance(data, list):  # Check if the data is a list
                for item in data:  # Iterate over the list
                    dfs.append(
                        {
                            'temperature': item['main']['temp'],
                            'city': item['name'],
                            'pressure': item['main']['pressure'],
                            'date': f.split('.')[0]
                        }
                    )
            else:  # If the data is not a list, assume it's a dictionary
                dfs.append(
                    {
                        'temperature': data['main']['temp'],
                        'city': data['name'],
                        'pressure': data['main']['pressure'],
                        'date': f.split('.')[0]
                    }
                )

    df = pd.DataFrame(dfs)

    print('\n', df.head(10))

    clean_data_path = '/app/clean_data'

    df.to_csv(os.path.join(clean_data_path, filename), index=False)


# # this is Task 2 within the DAG
# transform_data_into_csv(n_files=20, filename='data.csv')
# # this is Task 3 within the DAG
# transform_data_into_csv(n_files=None, filename='fulldata.csv')