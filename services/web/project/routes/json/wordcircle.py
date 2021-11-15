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


#logging.basicConfig()
#logging.getLogger().setLevel(logging.DEBUG)

@app.route('/json/wordcircle')
def json_wordcircle():
    pos_words = request.args.get('pos_words','')
    pos_words = pos_words.split()

    neg_words = request.args.get('neg_words','')
    neg_words = neg_words.split()

    subspace_wordss = [ pos_words, neg_words ] if len(neg_words) > 0 else [pos_words]

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
        n_top = 200

    try:
        top_word_minlen = int(request.args.get('top_word_minlen'))
    except (TypeError,ValueError):
        top_word_minlen = 4

    try:
        n_top_take_every = int(request.args.get('n_top_take_every'))
    except (TypeError,ValueError):
        n_top_take_every = 10

    center = request.args.get('center', 'true')
    if 'f' in center.lower():
        center = False
    else:
        center = True

    return make_wordcircle(subspace_wordss, seed_words, lang_in, lang_out, dim, n, n_top, top_word_minlen, n_top_take_every, center)


def make_wordcircle(subspace_wordss, seed_words=[], lang_in='en', lang_out='en', dim=25, n=100, n_top=0, top_word_minlen=4, n_top_take_every=10, center=True):
    '''
    >>> make_wordcloud([['cat'], ['dog']])
    '''
    logging.info(f'lang_in={lang_in}; lang_out={lang_out}')
    #max_n = None
    #max_d = None
    max_n = 100000
    max_d = dim
    embedding_out = chajda.embeddings.get_embedding(lang=lang_out, max_n=max_n, max_d=max_d, storage_dir='./embeddings')
    embedding_in = chajda.embeddings.get_embedding(lang=lang_in, max_n=max_n, max_d=max_d, storage_dir='./embeddings')
    #embedding_out = chajda.embeddings.get_embedding(name='wiki.en.align.vec', max_n=max_n, max_d=max_d, storage_dir='./embeddings')
    #embedding_in = chajda.embeddings.get_embedding(name='wiki.en.align.vec', max_n=max_n, max_d=max_d, storage_dir='./embeddings')
    logging.info('make_wordcircle')

    # (experimental) mean center
    def center_vocabulary(this, center=True):
        if not hasattr(this, 'centered'):
            this.centered = False
            this.mu = np.mean(this.kv.vectors, axis=0)
            this.mu_norms = np.linalg.norm(this.kv.vectors - this.mu, axis=1)

        if this.centered and center:
            return
        elif not this.centered and not center:
            return
        elif this.centered and not center:
            this.kv.vectors *= np.expand_dims(this.mu_norms, axis=1)
            this.kv.vectors += this.mu
            this.centered = False
        elif not this.centered and center:
            this.centered = True
            this.kv.vectors -= this.mu
            this.kv.vectors /= np.expand_dims(this.mu_norms, axis=1)

    center_vocabulary(embedding_out, center)
    center_vocabulary(embedding_in, center)
    
    # helper functions

    def words_to_points(words):
        return [ embedding_in.kv[word] for word in words if word in embedding_in.kv ]

    def mean(xs):
        if len(xs) == 0:
            return 0
        else:
            return sum(xs)/len(xs)

    def normalize(v):
        return v/np.linalg.norm(v)

    # compute output words

    top_words = []
    candidate_words = 0
    for word in embedding_out.kv.index_to_key:
        if len(top_words) >= n_top:
            break
        if len(word) >= top_word_minlen:
            candidate_words += 1
            if candidate_words % n_top_take_every == 0:
                top_words.append(word)

    subspace_words = [ word for words in subspace_wordss for word in words ]
    search_words = subspace_words + seed_words
    search_points = words_to_points(search_words)
    neighbor_words = []
    for point in search_points:
        for neighbor, rank in embedding_out.most_similar(point, topn=n//len(search_points)+1):
            neighbor_words.append(neighbor)

    words = list(set(top_words + neighbor_words))
    words_points = np.array(words_to_points(words))
    logging.debug(f"words_points.shape={words_points.shape}")
    frequencies = [ embedding_out.word_frequency(word) for word in words ]
    

    # compute projection values

    subspace_pointss = [ words_to_points(words) for words in subspace_wordss ]
    mus = [ mean(subspace_points) for subspace_points in subspace_pointss ]
    mus = np.array([ mu/np.linalg.norm(mu) for mu in mus ])

    '''
    vs = mus
    V = np.array(vs)
    logging.info(f"V.shape={V.shape}")

    P_V = V.T @ np.linalg.inv(V @ V.T) @ V
    logging.info(f"P_V.shape={P_V.shape}")

    mus_V = mus @ P_V
    ys_mus = np.expand_dims(mus_V[0,:], axis=0)
    xs_mus = np.expand_dims(mus_V[1,:], axis=0)

    logging.info(f"ys_mus.shape={ys_mus.shape}")
    logging.info(f"xs_mus.shape={xs_mus.shape}")

    angles = [ float(np.arccos(np.dot(mus[0],mu)/np.linalg.norm(mus[0])/np.linalg.norm(mu)))/math.pi*180 for mu in mus[1:] ]

    words_points_V = words_points @ P_V
    ys = np.expand_dims(words_points_V[0,:], axis=0)
    xs = np.expand_dims(words_points_V[1,:], axis=0)
    logging.info(f"ys.shape={ys.shape}")
    logging.info(f"xs.shape={xs.shape}")

    pre_distances = (mus[0] - words_points) @ (np.eye(dim) - P_V)
    logging.info(f"pre_distances.shape={pre_distances.shape}")
    distances = np.linalg.norm(pre_distances, axis=1)
    logging.info(f"distances.shape={distances.shape}")

    return {
        'words': {
            'words' : words,
            'ys' : ys.T.tolist(),
            'xs' : xs.T.tolist(),
            'distances' : distances.tolist(),
            'frequencies': frequencies,
        },
        'mus': {
            'ys': ys_mus.T.tolist(),
            'xs': xs_mus.T.tolist(),
            'angles': angles,
            }
        }
        '''

    '''
    vs = np.reshape(np.array([ mu-mus[0] for mu in mus[1:]]), [len(mus)-1, dim])
    PP_V.shape_vs = vs.T @ np.linalg.inv(vs @ vs.T) @ vs
    P_vsinv = np.eye(dim) - P_vs
    logging.info(f"P_vs.shape={P_vs.shape}")
    logging.info(f"P_vsinv.shape={P_vsinv.shape}")

    ys_mus = np.linalg.norm(mus @ P_vs, axis=1)
    xs_mus = np.linalg.norm(mus @ P_vsinv, axis=1)
    logging.info(f"ys_mus.shape={ys_mus.shape}")
    logging.info(f"xs_mus.shape={xs_mus.shape}")

    angles = [ float(np.arccos(np.dot(mus[0],mu)/np.linalg.norm(mus[0])/np.linalg.norm(mu)))/math.pi*180 for mu in mus[1:] ]

    ys = np.linalg.norm(words_points @ P_vs, axis=1)
    xs = np.linalg.norm(words_points @ P_vsinv, axis=1)
    logging.info(f"ys.shape={ys.shape}")
    logging.info(f"xs.shape={xs.shape}")

    pre_distances = (mus[0] - words_points) @ (np.eye(dim) - P_vs)
    logging.info(f"pre_distances.shape={pre_distances.shape}")
    distances = np.linalg.norm(pre_distances, axis=1)
    logging.info(f"distances.shape={distances.shape}")

    return {
        'words': {
            'words' : words,
            'ys' : ys.T.tolist(),
            'xs' : xs.T.tolist(),
            'distances' : distances.tolist(),
            'frequencies': frequencies,
        },
        'mus': {
            'ys': ys_mus.T.tolist(),
            'xs': xs_mus.T.tolist(),
            'angles': angles,
            }
        }
    '''

    #ys_projector = np.reshape(np.array([  mu - mus[0]    for mu in mus[1:] ]), [len(mus)-1, dim])
    #xs_projector = np.reshape(np.array([ (mu + mus[0])/2 for mu in mus[1:] ]), [len(mus)-1, dim])
    ys_projector = np.reshape(np.array([ normalize(mu - mus[0]) for mu in mus[1:] ]), [len(mus)-1, dim])
    xs_projector = np.reshape(np.array([ normalize(mu + mus[0]) for mu in mus[1:] ]), [len(mus)-1, dim])
    logging.info(f"ys_projector.shape={ys_projector.shape}")
    logging.info(f"xs_projector.shape={xs_projector.shape}")

    if len(subspace_wordss) > 1:
        P_S = ys_projector.T @ np.linalg.inv(ys_projector @ ys_projector.T) @ ys_projector
        logging.info(f"P_S.shape={P_S.shape}")
    else:
        P_S = 0.0

    ys_mus = mus @ ys_projector.T
    xs_mus = mus @ xs_projector.T

    angles = [ float(np.arccos(np.dot(mus[0],mu)/np.linalg.norm(mus[0])/np.linalg.norm(mu)))/math.pi*180 for mu in mus[1:] ]

    ys = words_points @ ys_projector.T 
    xs = words_points @ xs_projector.T
    logging.info(f"ys.shape={ys.shape}")
    logging.info(f"xs.shape={xs.shape}")

    pre_distances = (mus[0] - words_points) @ (np.eye(dim) - P_S)
    logging.info(f"pre_distances.shape={pre_distances.shape}")
    distances = np.linalg.norm(pre_distances, axis=1)
    logging.info(f"distances.shape={distances.shape}")

    return {
        'words': {
            'words' : words,
            'ys' : ys.T.tolist(),
            'xs' : xs.T.tolist(),
            'distances' : distances.tolist(),
            'frequencies': frequencies,
        },
        'mus': {
            'ys': ys_mus.T.tolist(),
            'xs': xs_mus.T.tolist(),
            'angles': angles,
            }
        }
