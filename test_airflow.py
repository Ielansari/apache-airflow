from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

@dag(schedule=timedelta(hours=1),catchup=False,start_date=datetime(2024, 4, 19))

def test_airflow():
    
    atlas_server = "host.docker.internal"
    hive_data_path = "/home/volume/data/data"
    embedding_script_path = "/home/volume/code/atlas_scripts/tabbie_embeddings.py"
    embedding_env_path = 'python'
    ml_model_path = "/home/volume/code/atlas_scripts"
    embeddings_path = "/home/volume/data/embeddings"


    @task
    def check_unclassified_result():
        import requests

        url = f"http://{atlas_server}:21000/api/atlas/v2/search/basic"

        auth = ("admin", "admin")

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # payload1 = {
        #     "query": "hive_db",
        #     "typeName": "hive_db"
        # }
        # payload2 = {
        #     "query": "hive_table",
        #     "typeName": "hive_table"
        # }
        payload3 = {
            "query": "hive_column",
            "typeName": "hive_column",
	 "limit":10000,
	 "offset":0,
	 "excludeDeletedEntities": True
        }
        # response1 = requests.get(url, headers=headers, params=payload1, auth=auth)
        # response2 = requests.get(url, headers=headers, params=payload2, auth=auth)
        response3 = requests.get(url, headers=headers, params=payload3, auth=auth)
        #response_list = [response1, response2, response3]

        columns = []
        
        if response3.status_code == 200:
            entities = response3.json()
            for entity in entities['entities']:
                if len(entity['classificationNames']) >0:
                    columns.append((entity['attributes']['qualifiedName'],entity['guid']))
            return columns
        else:
            print("Erreur lors de la récupération des entités :", response3.status_code)
            return 0

    @task
    def retreive_data_hive(columns):
        from pyhive import hive
        import pandas as pd

        # Connection parameters
        hive_host = 'host.docker.internal'
        hive_port = 10000  # Default Hive port
        hive_username = 'admin'
        hive_database = 'default'

        # Establish connection (NOSASL - No Authentication)
        conn = hive.Connection(
            host=hive_host,
            port=hive_port,
            username=hive_username,
            database=hive_database,
        )

        # Create cursor
        cursor = conn.cursor()
        cursor_column = conn.cursor()

        # Example query
        #query = "SELECT * FROM hv_csv_table LIMIT 3"
        
        # some retreived files
        
        for column in columns:
            table = str(column[0].split(".")[1])
            query = f"SELECT * FROM {table} LIMIT 50"
            query_column = f"DESCRIBE {table}"
            
            cursor.execute(query)
            cursor_column.execute(query_column)

            columns_head = [column_rec[0] for column_rec in list(cursor_column.fetchall())]

            pd.DataFrame(list(cursor.fetchall()), columns=columns_head).to_csv(f"{hive_data_path}/{table}.csv")

        return 1

    @task
    def generate_embeddings(check_columns):
        import subprocess
        import os
        import time
        
        #result =subprocess.run([embedding_env_path, embedding_script_path])        
        while len(os.listdir(embeddings_path)) == 0:
            time.sleep(5)
        return 1
    
    @task
    def classification(result):
        from sklearn.svm import SVC
        import pickle
        import pandas as pd
        
        # deserialize the classification model
        with open(f"{ml_model_path}/model.pkl", "br") as f:
            model = pickle.load(f)

        #load the generated embeddings
        if result:
            table = pd.read_csv(f"{embeddings_path}/000tabert_embeddings.csv")
            table.index = pd.MultiIndex.from_frame(table.loc[:, ["Unnamed: 0", "Unnamed: 1"]])
            table.drop(["Unnamed: 0", "Unnamed: 1"], axis=1, inplace=True)
            # make prediction
            return (model.predict(table).tolist(), table.index.tolist())
    

    @task
    def update_atlas(results,classification_results):
        import requests

        url_guid = f"http://{atlas_server}:21000/api/atlas/v2/entity/guid"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        auth = ("admin", "admin")
        def delete_classification_from_entity(entity_guid, classification_name):
            classification_url = f"{url_guid}/{entity_guid}/classification/{classification_name}"
            response = requests.delete(classification_url, headers=headers, auth=auth)
            if response.status_code == 204:
                print(f"Classification '{classification_name}' deleted successfully from the entity with GUID {entity_guid}.")
            else:
                print(f"Error deleting '{classification_name}' tags from the entity with GUID {entity_guid}: {response.text}")

        classification_names = ["Public", "Restreint", "Confidentiel", "Secret", "Tres Secret"]

        for result in results:
            for classification_name in classification_names:
                delete_classification_from_entity(result[1], classification_name) 

        map_prediction = {0: "Public", 1: "Restreint", 2: "Confidentiel", 3: "Secret", 4: "Tres Secret"}
        for prediction, column, guid in zip(classification_results[0], classification_results[1], results):
            classification_url = f"{url_guid}/{guid[1]}/classifications"
            classification_data = {
                "typeName": map_prediction[prediction]
            }

            response = requests.post(classification_url, headers=headers, json=[classification_data], auth=auth)

            if response.status_code == 204:
                print(f"Classification '{map_prediction[prediction]}' added successfully to the entity '{column}' with GUID {guid[1]}.")
            else:
                print(f"Error adding classification '{map_prediction[prediction]}' to the entity '{column}' with GUID {guid}: {response.text}")


    check_columns = check_unclassified_result()
        
    if check_columns:
        relult = retreive_data_hive(check_columns)
        result1 = generate_embeddings(relult)
        classification_result = classification(result1)
        update_atlas(check_columns, classification_result)
    


dag = test_airflow()
    