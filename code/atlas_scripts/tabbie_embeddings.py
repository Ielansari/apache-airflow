import numpy as np
import pandas as pd
import torch
import os
from transformers import BertTokenizer, BertModel

def compute_average_embeddings(data, row_embeddings, column_embeddings):
    average_embeddings = []

    for i in range(len(data)):
        for j in range(len(data.columns)):
            if i < len(row_embeddings) and j < len(column_embeddings):
                cell_embedding = (row_embeddings[i] + column_embeddings[j]) / 2
                average_embeddings.append(cell_embedding)

    return np.array(average_embeddings)

def get_bert_embeddings(text):
    # Tokenize text and convert to input_ids
    inputs = tokenizer(text, padding='max_length', truncation=True, max_length=512, return_tensors='pt')
    # Get BERT embeddings
    with torch.no_grad():
        outputs = bert_model(**inputs)
    # Average pooling over tokens
    embeddings = torch.mean(outputs.last_hidden_state, dim=1).numpy()
    return embeddings.squeeze()

if __name__ == "__main__":
    # Charger le tokenizer et le modèle BERT
    tokenizer = BertTokenizer.from_pretrained('bert-base-cased')
    bert_model = BertModel.from_pretrained("bert-base-cased")

    tables = []

    for i in os.listdir("/home/volume/data/data/"):
        if os.path.splitext(i)[1] == ".csv":
            tables.append(i)

    data_embed = []
    columns = []

    for tab_name in tables:
        data = pd.read_csv(f"/home/volume/data/data/{tab_name}")
        row_embeddings = []
        for _, row in data.head(10).iterrows():
            row_text = ' '.join(row.astype(str))
            row_embedding = get_bert_embeddings(row_text).squeeze()
            row_embeddings.append(row_embedding,)

        column_embeddings = []
        for column in data:
            column_text = ' '.join(data[column].head(10).astype(str))
            column_embedding = get_bert_embeddings(column_text).squeeze()
            column_embeddings.append(column_embedding)

        average_embeddings = compute_average_embeddings(data, row_embeddings, column_embeddings).reshape(10,data.shape[1],-1).mean(axis=0)

        for emb, column in zip(average_embeddings, data):
            columns.append((os.path.splitext(tab_name)[0], column))
            data_embed.append(list(emb))
        print(tab_name)

    index = pd.MultiIndex.from_tuples(columns)
    data_tabbie = pd.DataFrame(data_embed, index = index)

    # Enregistrer les embeddings dans un fichier CSV
    data_tabbie.to_csv("/home/volume/data/embeddings/tabbie_embeddings.csv")