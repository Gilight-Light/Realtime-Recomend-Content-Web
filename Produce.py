import pandas as pd
import scipy.sparse as sparse
import numpy as np
import random
import implicit
from sklearn.preprocessing import MinMaxScaler
from sklearn import metrics
from kafka import KafkaProducer
from datetime import datetime
import time



articles_df = pd.read_csv('Data/shared_articles.csv')
interactions_df = pd.read_csv('Data/users_interactions.csv')
articles_df.drop(['authorUserAgent', 'authorRegion', 'authorCountry'], axis=1, inplace=True)
interactions_df.drop(['userAgent', 'userRegion', 'userCountry'], axis=1, inplace=True)

articles_df = articles_df[articles_df['eventType'] == 'CONTENT SHARED']
articles_df.drop('eventType', axis=1, inplace=True)

df = pd.merge(interactions_df[['contentId','personId', 'eventType']], \
              articles_df[['contentId', 'title']], \
              how = 'inner', on = 'contentId')

event_type_strength = {
   'VIEW': 1.0,
   'LIKE': 2.0, 
   'BOOKMARK': 3.0, 
   'FOLLOW': 4.0,
   'COMMENT CREATED': 5.0,  
}

df['eventStrength'] = df['eventType'].apply(lambda x: event_type_strength[x])

df = df.drop_duplicates() # loai bo trung lap
grouped_df = df.groupby(['personId', 'contentId', 'title']).sum().reset_index()

grouped_df['title'] = grouped_df['title'].astype("category")
grouped_df['personId'] = grouped_df['personId'].astype("category")
grouped_df['contentId'] = grouped_df['contentId'].astype("category")
grouped_df['person_id'] = grouped_df['personId'].cat.codes
grouped_df['content_id'] = grouped_df['contentId'].cat.codes

sparse_content_person = sparse.csr_matrix((grouped_df['eventStrength'].astype(float), (grouped_df['content_id'], grouped_df['person_id'])))
sparse_person_content = sparse.csr_matrix((grouped_df['eventStrength'].astype(float), (grouped_df['person_id'], grouped_df['content_id'])))

model = implicit.als.AlternatingLeastSquares(factors=20, regularization=0.1, iterations=50)

alpha = 15
data = (sparse_content_person * alpha).astype('double')

# Fit the model
model.fit(data)


def recomend_system(content_id):
    n_similar = 10

    person_vecs = model.user_factors
    content_vecs = model.item_factors

    content_norms = np.sqrt((content_vecs * content_vecs).sum(axis=1))

    scores = content_vecs.dot(content_vecs[content_id]) / content_norms
    top_idx = np.argpartition(scores, -n_similar)[-n_similar:]
    similar = sorted(zip(top_idx, scores[top_idx] / content_norms[content_id]), key=lambda x: -x[1])
    for content in similar:
        idx, score = content
        print(grouped_df.title.loc[grouped_df.content_id == idx].iloc[0])
        
id_test = 450
result_df = grouped_df[grouped_df['person_id'] == id_test]
result_df = result_df.sort_values(by='eventStrength', ascending=False)
show_df = result_df['title']
max_event_id = result_df['eventStrength'].idxmax()
max_content_id = result_df.loc[max_event_id, 'content_id']
recomend_system(max_content_id)

KAFKA_TOPIC_NAME_CONS = "songTopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
