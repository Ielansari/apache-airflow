import numpy as np
import pandas as pd
from transformers import BertTokenizer, TFBertModel
import os
import tensorflow as tf

#gpus = tf.config.experimental.list_physical_devices('GPU')
#if gpus:
    #try:
        #for gpu in gpus:
            #tf.config.experimental.set_memory_growth(gpu, True)
    #except RuntimeError as e:
        #print(e)

def compute_average_embeddings(data, row_embeddings, column_embeddings):
    average_embeddings = []

    for i in range(len(data)):
        for j in range(len(data.columns)):
            if i < len(row_embeddings) and j < len(column_embeddings):
                cell_embedding = (row_embeddings[i] + column_embeddings[j]) / 2
                average_embeddings.append(cell_embedding)

    return np.array(average_embeddings)

def get_bert_embeddings(column):
    tokenizer = BertTokenizer.from_pretrained('bert-base-cased')
    bert_model = TFBertModel.from_pretrained("bert-base-cased")

    input_ids = tokenizer.encode(column, add_special_tokens=True, max_length=512, truncation=True, padding='max_length', return_tensors='tf')
    outputs = bert_model(input_ids)

    embeddings = outputs[0].numpy()  # BERT embeddings for the entire sequence
    return np.mean(embeddings, axis=1)

def generate_embeddings(tables):
    data_embed = []
    columns = []
    for tab_name in tables:
        data = pd.read_csv(tab_name)  # Load the dataframe from CSV file
        row_embeddings = []
        for _, row in data.head(10).iterrows():  # Iterate over rows of the dataframe
            row_text = ' '.join(row.astype(str))
            row_embedding = get_bert_embeddings(row_text).squeeze()
            row_embeddings.append(row_embedding)

        column_embeddings = []
        for column in data.columns:
            column_text = ' '.join(data[column].head(10).astype(str))
            column_embedding = get_bert_embeddings(column_text).squeeze()
            column_embeddings.append(column_embedding)

        average_embeddings = compute_average_embeddings(data, row_embeddings, column_embeddings).reshape(10, data.shape[1], -1).mean(axis=0)

        for emb, column in zip(average_embeddings, data.columns):
            columns.append((os.path.splitext(os.path.basename(tab_name))[0], column))
            data_embed.append(list(emb))
        print(tab_name)
    index = pd.MultiIndex.from_tuples(columns)
    data_tabbie = pd.DataFrame(data_embed, index = index)
    data_tabbie.to_csv("/home/volume/data/embeddings/tabbie_embeddings.csv")

# Example usage:
generate_embeddings([f"/home/volume/data/data/{table}" for table in os.listdir("/home/volume/data/data")])  # Pass a list of table filenames as input
#generate_embeddings(["/home/volume/data/data/account.csv"])