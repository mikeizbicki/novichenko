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
import math


@app.route('/json/wordcloud')
def json_wordcloud():
    pos_words = request.args.get('pos_words','')
    pos_words = pos_words.split()

    neg_words = request.args.get('neg_words','')
    neg_words = neg_words.split()

    seed_words = request.args.get('seed_words','')
    seed_words = seed_words.split()
    logging.info('seed_words='+str(seed_words))

    lang_in = request.args.get('lang_in','en')
    lang_out = request.args.get('lang_out','en')

    try:
        dim = int(request.args.get('dim'))
    except (TypeError,ValueError):
        dim = 25
    logging.info(f'dim={dim}')

    try:
        n = int(request.args.get('n'))
    except (TypeError,ValueError):
        n = 200

    try:
        n_top = int(request.args.get('n_top'))
    except (TypeError,ValueError):
        n_top = 0

    return make_wordcloud(pos_words, neg_words, seed_words, lang_in, lang_out, dim, n, n_top)


def cosine_distance(a,b):
    return 1-np.dot(a,b)/np.linalg.norm(a)/np.linalg.norm(b)


def mean(xs):
    return sum(xs)/(len(xs)+1e-20)


def make_wordcloud(pos_words, neg_words, seed_words, lang_in='en', lang_out='en', dim=25, n=100, n_top=0):
    '''
    >>> make_wordcloud(['cat'], ['dog'], [])
    '''
    logging.info(f'lang_in={lang_in}; lang_out={lang_out}')
    embedding_out = chajda.embeddings.get_embedding(lang=lang_out, max_n=100000, max_d=dim, storage_dir='./embeddings')
    embedding_in = chajda.embeddings.get_embedding(lang=lang_in, max_n=100000, max_d=dim, storage_dir='./embeddings')
    #projectionvector, unknown_words = embedding_in.make_projectionvector(pos_words, neg_words)
    projector, unknown_words = embedding_in.make_projector(pos_words, neg_words)

    def words_to_points(words):
        points = []
        for word in words:
            try:
                points.append(embedding_in.kv[word])
            except KeyError:
                pass
        return points

    # compute the angle between the positive and negative words
    if neg_words:
        pos_point = mean(words_to_points(set(pos_words)))
        neg_point = mean(words_to_points(set(neg_words)))
        angle = float(np.arccos(np.dot(pos_point,neg_point)/np.linalg.norm(pos_point)/np.linalg.norm(neg_point)))/math.pi*180
    else:
        angle = 0.0

    # targets are the points in the space that represent our concepts
    target_words = set(pos_words + neg_words)
    target_points = words_to_points(target_words)
    target_mean = mean(target_points)
    if len(target_points) > 1:
        target_points += target_mean

    # compute the words to plot
    search_words = seed_words if seed_words and len(seed_words)>0 else target_words | set(seed_words)
    search_points = words_to_points(search_words)
    neighbors = []
    for point in search_points:
        try:
            for neighbor, rank in embedding_out.most_similar(point, topn=n//len(search_points)+1):  #, restrict_vocab=50000):
                neighbors.append(neighbor)
        except KeyError:
            pass
    neighbors += embedding_out.kv.index_to_key[:n_top]
    plot_words = set(chajda.tsvector.lemmatize(lang_out,' '.join(neighbors), add_positions=False).split())

    # compute the plotting coordinates
    results = []
    for word in plot_words:
        try:
            # compute distance
            distance = mean([cosine_distance(embedding_out.kv[word], point) for point in target_points])

            # add the result
            # NOTE:
            # np.float32 is not serializable to json, so convert to python float
            results.append({
                'text': word,
                'projection': projector(word),
                #'projection': float(np.dot(projectionvector, embedding_out.kv[word])),
                #'projection': float(np.dot(projectionvector, embedding.kv[word]/np.linalg.norm(embedding.kv[word]))),
                'frequency': embedding_out.word_frequency(word),
                'distance': float(distance)
                })
        except KeyError:
            pass

    return {
        'projection' : [c['projection'] for c in results],
        'frequency' : [c['frequency'] for c in results],
        'text' : [c['text'] for c in results],
        'distance': [c['distance'] for c in results],
        'angle': angle,
        }
