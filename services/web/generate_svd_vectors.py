import sqlalchemy
from sqlalchemy.sql import text
import numpy as np
import os
import json
import logging
import chajda.embeddings

def generate_svd(*, method='db', fraction:float=0.01, limit:int=None, lang='en', max_d:int=None, max_n:int=None, output_folder='./vectors'):
    if method=='hardcoded':
        output_path = os.path.join(output_folder, f'svd_vh_{method}')
        words = [
            'anger', 'angry', 'agitated', 'calm', 'serene',
            'gender', 'female', 'mother', 'queen', 'she', 'male', 'father', 'king', 'he',
            'happiness', 'sad', 'misery', 'dissatisfaction', 'bore', 'happy', 'joy', 'pleasure', 'delight',
            'intelligence', 'intelligent', 'stupid',
            'freedom', 'oppression', 'freedom',
            'scary', 'relaxing', 'unafraid', 'comfort','scary', 'afraid', 'fear' ,
            'sentiment', 'positive', 'good', 'better', 'best', 'negative', 'bad', 'worse', 'worst' ,
            'success', 'successful', 'failed',
            'threat', 'safety', 'peace', 'friend', 'ally', 'danger', 'war', 'menace', 'enemy',
            'truth', 'truth', 'lie',
            'war', 'peace', 'war',
            'wealth', 'wealth', 'affluence', 'rich', 'asset', 'debt', 'poverty', 'poor', 'liability',
            ]
        embedding = chajda.embeddings.get_embedding(lang='en', max_n=max_n, max_d=max_d, storage_dir='./embeddings')
        points = [ embedding.kv[word] for word in words ]
        mat = np.array(points)

    elif method=='db':

        output_path = os.path.join(output_folder, f'svd_vh_{method}_fraction={fraction}_limit={limit}.npy')

        # postgres connection
        logging.info('connecting to database')
        DB_USER = os.environ.get('DB_USER')
        DB_PASSWORD = os.environ.get('DB_PASSWORD')
        DB_NAME = os.environ.get('DB_NAME')
        DB_URI = f'postgresql://{DB_USER}:{DB_PASSWORD}@db/{DB_NAME}'
        engine = sqlalchemy.create_engine(DB_URI, connect_args={
            'connect_timeout': 10,
            'application_name': 'svd',
            })


        # query the database
        logging.info('querying database')
        limit_str = f'limit {limit}' if limit else ''
        fraction_str = f'random()<{fraction}' if fraction else 'true'
        sql = text(f'SELECT context FROM contextvector where {fraction_str} {limit_str};')
        connection = engine.connect()
        res = connection.execute(sql)
        res = [ json.loads(row[0]) for row in res ]
        mat = np.array(res)

    logging.info('performing svd')
    logging.info(f"mat.shape={mat.shape}")
    u, s, vh = np.linalg.svd(mat)
    logging.info(f"s[:100]={s[:100]}")

    logging.info(f'saving vh to {output_path}')
    np.save(output_path,vh)

    
################################################################################
# standalone executable code
################################################################################

if __name__ == '__main__':

    # setup logging
    logging.basicConfig(
        format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
        level=logging.INFO,
        force=True,
        )

    # run the downloader
    from clize import run
    run(generate_svd)
