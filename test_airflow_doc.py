from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

@dag(schedule=timedelta(hours=1),catchup=False,start_date=datetime(2024, 4, 19))

def test_airflow_doc():

    atlas_server = "host.docker.internal"
    #hive_data_path = "/home/ibra/PFE_project/test_airflow/data"
    #embedding_script_path = "/home/ibra/PFE_project/test_airflow/tabert_embeddings.py"
    #embedding_env_path = '/home/ibra/.conda/envs/tabert/bin/python'
    ml_model_path = "/home/volume/code/atlas_scripts"
    #embeddings_path = "/home/ibra/PFE_project/test_airflow/embeddings"
    directory = "/data/news_documents_data"

    @task
    def check_new_document():
        from hdfs import InsecureClient

        max_text_length = 100	
        last_time = 1714638048494
        client = InsecureClient('http://host.docker.internal:50070', user='host.docker.internal')
        file_paths = []
        def check_directory(directory):
            file_list = client.list(directory)
            for file in file_list:
                if directory=="/":
                    directory = ""
                if client.status(f"{directory}/{file}")["modificationTime"] > last_time :
                    if  client.status(f"{directory}/{file}")["type"] != 'DIRECTORY':
                        file_paths.append(f"{directory}/{file}")
                    else:
                        check_directory(f"{directory}/{file}")
        
        check_directory(directory)
        print(file_paths)

        return file_paths

    @task
    def retreive_doc_hdfs(document_paths):
        from hdfs import InsecureClient
        client = InsecureClient('http://host.docker.internal:50070', user='host.docker.internal')
        docs = []
        for doc_path in document_paths:
            try:
                with client.read(doc_path) as reader:
                    # Read data in chunks if necessary
                    chunk_size = 1024
                    doc_data = b''
                    for chunk in reader:
                        doc_data += chunk
                    docs.append(doc_data.decode("utf-8"))
            except Exception as e:
                # Handle exceptions gracefully
                print(f"Error reading document {doc_path}: {e}")
        return docs

#    @task
#    def classification_docs(documents):
#        predictions = []
#        doc_paths = []
#        for doc in documents:
#            predictions.append(1)
#        return predictions

    # @task
    # def generate_embeddings(check_columns):
    #     import subprocess
    #     import os
    #     import time

    #     result = subprocess.run([embedding_env_path, embedding_script_path])
    #     while len(os.listdir(embeddings_path)) == 0:
    #         time.sleep(5)
    #     return 1

    @task
    def classification_docs(documents):
        import pickle
        from keras.models import load_model
        from keras.preprocessing.text import Tokenizer
        from keras.preprocessing.sequence import pad_sequences
        import numpy as np

        # Load the saved model for classification
        model = load_model('/home/volume/code/atlas_scripts/cnn_model.h5') 
        
        max_text_length = 100
        max_features = 4530  # This should match your embedding input_dim
        tokenizer = Tokenizer(num_words=max_features)
        #tokenizer = Tokenizer()
        tokenizer.fit_on_texts(documents)
        text_sequence = tokenizer.texts_to_sequences(documents)
        padded_sequence = pad_sequences(text_sequence, maxlen=max_text_length)
        predictions = model.predict(padded_sequence)
        predicted_labels = np.argmax(predictions, axis=1)
        return predicted_labels

    @task
    def update_atlas_doc(file_paths, predictions):
        import os
        import requests

        for file_path, prediction in zip(file_paths,predictions):
            file_name = os.path.basename(file_path)
            payload = {
                "entity": {
                    "typeName": "hdfs_path",
                    "attributes": {
                        "qualifiedName": "hdfs:/{}".format(file_path),
                        "name": file_name,
                        "path": file_path,
                        # Add more attributes as needed
                    }
                }
            }

            atlas_api_url = f"http://host.docker.internal:21000/api/atlas/v2/entity"
            username = "admin"
            password = "admin"

            # Make a POST request to create the entity
            response = requests.post(
                atlas_api_url,
                headers={"Content-Type": "application/json","Accept": "application/json"},
                auth=(username, password),
                json=payload
            )

            if response.status_code == 200:
                print("Entity created successfully for file:", file_path)
                
                guid = response.json()["mutatedEntities"]["CREATE"][0]["guid"]
                url_guid = f"http://{atlas_server}:21000/api/atlas/v2/entity/guid"
                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }
                auth = ("admin", "admin")

                map_prediction = {0: "Public", 1: "Restreint", 2: "Confidentiel", 3: "Secret", 4: "Tres Secret"}
                
                classification_url = f"{url_guid}/{guid}/classifications"
                classification_data = {
                    "typeName": map_prediction[prediction]
                }

                response = requests.post(classification_url, headers=headers, json=[classification_data], auth=auth)

                if response.status_code == 204:
                    print(f"Classification '{map_prediction[prediction]}' added successfully to the entity '{file_path}' with GUID {guid[1]}.")
                else:
                    print(f"Error adding classification '{map_prediction[prediction]}' to the entity '{file_path}' with GUID {guid}: {response.text}")
            else:   
                print("Failed to create entity for file:", file_path)
                print("Response:", response.text)


    # @task
    # def update_atlas_doc(results,classification_results):
    #     import requests

    #     url_guid = f"http://{atlas_server}:21000/api/atlas/v2/entity/guid"
    #     headers = {
    #         "Content-Type": "application/json",
    #         "Accept": "application/json",
    #     }
    #     auth = ("admin", "admin")

    #     map_prediction = {0: "Public", 1: "Restreint", 2: "Confidentiel", 3: "Secret", 4: "Tres Secret"}
    #     for prediction, column, guid in zip(classification_results[0], classification_results[1], results):
    #         classification_url = f"{url_guid}/{guid[1]}/classifications"
    #         classification_data = {
    #             "typeName": map_prediction[prediction]
    #         }

    #         response = requests.post(classification_url, headers=headers, json=[classification_data], auth=auth)

    #         if response.status_code == 204:
    #             print(f"Classification '{map_prediction[prediction]}' added successfully to the entity '{column}' with GUID {guid[1]}.")
    #         else:
    #             print(f"Error adding classification '{map_prediction[prediction]}' to the entity '{column}' with GUID {guid}: {response.text}")


    check_columns = check_new_document()

    if check_columns:
        relult = retreive_doc_hdfs(check_columns)
        classification_result = classification_docs(relult)
        update_atlas_doc(check_columns, classification_result)

dag = test_airflow_doc()