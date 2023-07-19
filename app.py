import os
import pandas as pd
from sqlalchemy import create_engine
from catboost import CatBoostClassifier
from schema import PostGet, Response

from datetime import datetime
from typing import List
from fastapi import FastAPI
from loguru import logger
import hashlib

app = FastAPI()

def batch_load_sql(query: str) -> pd.DataFrame:
    CHUNKSIZE = 200000
    engine = create_engine("postgresql://username:password@"
                           "postgres.lab.karpov.courses:6432/startml")
    conn = engine.connect().execution_options(stream_results=True)
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=CHUNKSIZE):
        chunks.append(chunk_dataframe)
        logger.info(f'Got chunk: {len(chunk_dataframe)}')
    conn.close()
    return pd.concat(chunks, ignore_index=True)


def get_model_path_t(path: str) -> str:
    if os.environ.get("IS_LMS") == "1":
        MODEL_PATH = '/workdir/user_input/model_test'
    else:
        MODEL_PATH = path
    return MODEL_PATH

def get_model_path_c(path: str) -> str:
    if os.environ.get("IS_LMS") == "1":
        MODEL_PATH = '/workdir/user_input/model_control'
    else:
        MODEL_PATH = path
    return MODEL_PATH


def load_features() -> pd.DataFrame:
    logger.info('loading liked posts')
    liked_posts = batch_load_sql("""SELECT DISTINCT post_id, user_id FROM public.feed_data WHERE action = 'like'""")
    logger.info('loading post features mod')
    post_features_mod = pd.read_sql('SELECT * FROM "i-djatlov_features_lesson_22"',
                                con="postgresql://username:password@postgres.lab.karpov.courses:6432/startml")
    logger.info('loading post features')
    post_features = pd.read_sql("""SELECT * FROM public.post_text_df""",
                                con="postgresql://username:password@postgres.lab.karpov.courses:6432/startml")
    logger.info('loading user features')
    user_features = pd.read_sql("""SELECT * FROM public.user_data""",
                                con="postgresql://username:password@postgres.lab.karpov.courses:6432/startml")
    return [liked_posts, post_features, post_features_mod, user_features]


def load_control_model():
    model_path = get_model_path_c(<path>)
    model_c = CatBoostClassifier()
    model_c.load_model(model_path)
    return model_c

def load_test_model():
    model_path = get_model_path_t(<path>)
    model_t = CatBoostClassifier()
    model_t.load_model(model_path)
    return model_t

salt = 'my_random_salt'
exp_gr_num = 2
def get_exp_group(user_id: int) -> str:
    group = int(hashlib.md5((str(user_id) + salt).encode()).hexdigest(), 16) % exp_gr_num
    if group == 0:
        group = 'control'
    elif group == 1:
        group = 'test'
    return group


logger.info('loading models')
model_control = load_control_model()
model_test = load_test_model()
logger.info('loading features')
features = load_features()
logger.info('service is up and running')

def get_recommended_feed_c(id: int, time: datetime, limit: int = 5):
    logger.info(f'user_id: {id}')
    logger.info('reading features')
    user_features = features[3].loc[features[3].user_id == id]
    user_features = user_features.drop('user_id', axis=1)

    logger.info('dropping columns')
    post_features = features[1].drop('text', axis=1)

    content = features[1][['post_id', 'text', 'topic']]

    logger.info('zipping everything')
    add_user_features = dict(zip(user_features.columns, user_features.values[0]))
    logger.info('assigning everything')
    user_posts_features = post_features.assign(**add_user_features)
    user_posts_features = user_posts_features.set_index('post_id')

    logger.info('add time info')
    user_posts_features['hour'] = time.hour
    user_posts_features['month'] = time.month

    logger.info('predicting')
    predicts = model_control.predict_proba(user_posts_features)[:, 1]
    user_posts_features['predicts'] = predicts

    logger.info('deleting like posts')
    liked_posts = features[0]
    liked_posts = liked_posts[liked_posts.user_id == id].post_id.values
    filtered_ = user_posts_features[~user_posts_features.index.isin(liked_posts)]

    recommended_posts = filtered_.sort_values('predicts')[-limit:].index

    return [
        PostGet(**{
            "id": i,
            "text": content[content.post_id == i].text.values[0],
            "topic": content[content.post_id == i].topic.values[0]
        }) for i in recommended_posts
    ]

def get_recommended_feed_t(id: int, time: datetime, limit: int = 5):
    logger.info(f'user_id: {id}')
    logger.info('reading features')
    user_features = features[3].loc[features[3].user_id == id]
    user_features = user_features.drop('user_id', axis=1)

    logger.info('dropping columns')
    post_features = features[2].drop('text', axis=1)

    content = features[2][['post_id', 'text', 'topic']]

    logger.info('zipping everything')
    add_user_features = dict(zip(user_features.columns, user_features.values[0]))
    logger.info('assigning everything')
    user_posts_features = post_features.assign(**add_user_features)
    user_posts_features = user_posts_features.set_index('post_id')

    logger.info('add time info')
    user_posts_features['hour'] = time.hour
    user_posts_features['month'] = time.month

    logger.info('predicting')
    predicts = model_test.predict_proba(user_posts_features)[:, 1]
    user_posts_features['predicts'] = predicts

    logger.info('deleting like posts')
    liked_posts = features[0]
    liked_posts = liked_posts[liked_posts.user_id == id].post_id.values
    filtered_ = user_posts_features[~user_posts_features.index.isin(liked_posts)]

    recommended_posts = filtered_.sort_values('predicts')[-limit:].index

    return [
        PostGet(**{
            "id": i,
            "text": content[content.post_id == i].text.values[0],
            "topic": content[content.post_id == i].topic.values[0]
        }) for i in recommended_posts
    ]



@app.get("/post/recommendations/", response_model=Response)
def recommended_posts(id: int, time: datetime, limit: int = 5) -> Response:
    if get_exp_group(id) == 'test':
        logger.info(f'user: {id}, exp_group: {get_exp_group(id)}')
        return Response(exp_group=get_exp_group(id), recommendations=get_recommended_feed_t(id, time, limit))
    elif get_exp_group(id) == 'control':
        logger.info(f'user: {id}, exp_group: {get_exp_group(id)}')
        return Response(exp_group=get_exp_group(id), recommendations=get_recommended_feed_c(id, time, limit))