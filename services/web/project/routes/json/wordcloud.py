from project import app
from project.utils import do_query, render_template, debug_timer
import chajda
import chajda.embeddings
import chajda.tsvector
from flask import request
import itertools
import logging
import datetime
import numpy as np


@app.route('/json/wordcloud')
def json_wordcloud():
    pos_words = request.args.get('pos_words','')
    pos_words = pos_words.split()

    neg_words = request.args.get('neg_words','')
    neg_words = neg_words.split()

    cloud_words = request.args.get('cloud_words','')
    cloud_words = cloud_words.split()
    logging.info('cloud_words='+str(cloud_words))

    cloud_only = request.args.get('cloud_only', False)
    if cloud_only != False:
        cloud_only = True
    logging.info(f'cloud_only={cloud_only}')

    try:
        n = int(request.args.get('n'))
    except TypeError:
        n = 1000

    return make_wordcloud(pos_words, neg_words, cloud_words, cloud_only, n=n)


def cosine_distance(a,b):
    return 1-np.dot(a,b)/np.linalg.norm(a)/np.linalg.norm(b)


def mean(xs):
    return sum(xs)/(len(xs)+1e-20)


def make_wordcloud(pos_words, neg_words, cloud_words, cloud_only, lang='en', n=100):
    '''
    >>> make_wordcloud(['cat'], ['dog'], [])
    '''
    embedding = chajda.embeddings.get_embedding(lang='en', max_n=50000, max_d=None, storage_dir='./embeddings')
    projectionvector, unknown_words = embedding.make_projectionvector(pos_words, neg_words)

    def words_to_points(words):
        points = []
        for word in words:
            try:
                points.append(embedding.kv[word])
            except KeyError:
                pass
        return points

    # targets are the points in the space that represent our concepts
    target_words = set(pos_words + neg_words)
    target_points = words_to_points(target_words)
    target_mean = mean(target_points);
    distance_modifier = mean([cosine_distance(target_mean, point) for point in target_points])
    if len(target_points) > 1:
        target_points += target_mean;

    # compute the words to plot
    search_words = cloud_words if cloud_only else target_words | set(cloud_words)
    neighbors = []
    for word in search_words:
        try:
            for neighbor, rank in embedding.most_similar(word, topn=n//len(search_words)+1):  #, restrict_vocab=50000):
                neighbors.append(neighbor)
        except KeyError:
            pass
    plot_words = set(chajda.tsvector.lemmatize(lang,' '.join(neighbors),add_positions=False).split())

    # compute the plotting coordinates
    results = []
    for word in plot_words:
        try:
            # compute distance
            distance = mean([cosine_distance(embedding.kv[word], point) for point in target_points]) - distance_modifier

            # add the result
            # NOTE:
            # np.float32 is not serializable to json, so convert to python float
            results.append({
                'text': word,
                'projection': float(np.dot(projectionvector, embedding.kv[word])),
                #'projection': float(np.dot(projectionvector, embedding.kv[word]/np.linalg.norm(embedding.kv[word]))),
                'frequency': embedding.word_frequency(word),
                'distance': float(distance)
                })
        except KeyError:
            pass

    return {
        'projection' : [c['projection'] for c in results],
        'frequency' : [c['frequency'] for c in results],
        'text' : [c['text'] for c in results],
        'distance': [c['distance'] for c in results],
        }
