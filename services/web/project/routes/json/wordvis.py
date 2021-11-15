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

@app.route('/json/wordvis')
def json_wordvis():
    wordss = request.args.get('words','')
    subspace_wordss = [ words.split() for words in wordss.split(',') ]


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
        logging.info(f"request.args.get('a')={request.args.get('a')}")
        a = float(request.args.get('a'))
    except (TypeError,ValueError):
        a = None
    logging.info(f"a={a}")

    try:
        n = int(request.args.get('n'))
    except (TypeError,ValueError):
        n = 200

    try:
        n_rand = int(request.args.get('n_rand'))
    except (TypeError,ValueError):
        n_rand = 0

    try:
        n_top = int(request.args.get('n_top'))
    except (TypeError,ValueError):
        n_top = 0

    try:
        top_word_minlen = int(request.args.get('top_word_minlen'))
    except (TypeError,ValueError):
        top_word_minlen = 4

    try:
        n_top_take_every = int(request.args.get('n_top_take_every'))
    except (TypeError,ValueError):
        n_top_take_every = 1

    center = request.args.get('center', 'true')
    if 'f' in center.lower():
        center = False
    else:
        center = True

    whiten = request.args.get('whiten', 'true')
    if 'f' in whiten.lower():
        whiten = False
    else:
        whiten = True

    try:
        pca_dims = int(request.args.get('pca_dims'), 0)
    except (TypeError,ValueError):
        pca_dims = 0

    return make_wordvis(subspace_wordss, seed_words, lang_in, lang_out, dim, a, n, n_rand, n_top, top_word_minlen, n_top_take_every, center, pca_dims, whiten)


rand_dim=30
P_rand = np.random.normal(size=[rand_dim,300])
P_rand[0] /= np.linalg.norm(P_rand[0])
P_rand[1] /= np.linalg.norm(P_rand[1])
P_rand[2] /= np.linalg.norm(P_rand[2])


def make_wordvis(subspace_wordss, seed_words=[], lang_in='en', lang_out='en', dim=25, a=None, n=100, n_rand=0, n_top=0, top_word_minlen=4, n_top_take_every=10, center=True, pca_dims=0, whiten=True):
    '''
    >>> make_wordcloud([['cat'], ['dog']])
    '''
    logging.info(f"whiten={whiten}")
    logging.info(f"pca_dims={pca_dims}")
    logging.info(f"subspace_wordss={subspace_wordss}")
    logging.info(f'lang_in={lang_in}; lang_out={lang_out}')
    logging.info(f"n_rand={n_rand}")
    logging.info(f"n_top={n_top}")
    #max_n = None
    #max_d = None
    max_n = 100000
    max_d = dim
    embedding_out = chajda.embeddings.get_embedding(lang=lang_out, max_n=max_n, max_d=max_d, storage_dir='./embeddings')
    embedding_in = chajda.embeddings.get_embedding(lang=lang_in, max_n=max_n, max_d=max_d, storage_dir='./embeddings')
    #embedding_out = chajda.embeddings.get_embedding(name='wiki.en.align.vec', max_n=max_n, max_d=max_d, storage_dir='./embeddings')
    #embedding_in = chajda.embeddings.get_embedding(name='wiki.en.align.vec', max_n=max_n, max_d=max_d, storage_dir='./embeddings')
    logging.info('make_wordvis')

    # (experimental) mean center
    def center_vocabulary(this, center=True, pca_dims=0, whiten=True):
        if not hasattr(this, 'centered'):
            this.centered = False
            this.mu = np.mean(this.kv.vectors, axis=0)
            this.mu_norms = np.linalg.norm(this.kv.vectors - this.mu, axis=1)



        if this.centered == center:
            pass
        elif this.centered and not center:
            this.kv.vectors *= np.expand_dims(this.mu_norms, axis=1)
            this.kv.vectors += this.mu
            this.centered = False
        elif not this.centered and center:
            this.centered = True
            this.kv.vectors -= this.mu
            this.kv.vectors /= np.expand_dims(this.mu_norms, axis=1)


        if not hasattr(this, 'centered_pca'):
            from sklearn.decomposition import PCA
            this.centered_pca = PCA()
            this.centered_pca.fit(this.kv.vectors)
            logging.info(f"this.centered_pca.explained_variance_ratio_={this.centered_pca.explained_variance_ratio_}")
            this.centered_pca_dims = 0

        logging.info(f"this.centered_pca_dims={this.centered_pca_dims}")
        if this.centered_pca_dims == pca_dims:
            pass
        else:
            logging.info(f"this.centered_pca.components_[0,:].shape={this.centered_pca.components_[0,:].shape}")
            logging.info(f"this.kv.vectors.shape={this.kv.vectors.shape}")
            U = this.centered_pca.components_
            logging.info(f"U[0].shape={U[0].shape}")
            this.kv.vectors @ U[0:1].T
            logging.info(f"(this.kv.vectors @ U[0]).shape={(this.kv.vectors @ U[0]).shape}")
            logging.info(f"np.einsum('j,ij,j->ij', U[0], this.kv.vectors, U[0]).shape={np.einsum('j,ij,j->ij', U[0], this.kv.vectors, U[0]).shape}")
            np.expand_dims(this.kv.vectors @ U[0], axis=1) @ np.expand_dims(U[0], axis=0)



            #logging.info(f"np.expand_dims(this.kv.vectors @ U[0], axis=1) @ np.expand_dims(U[0], axis=0).shape={np.expand_dims(this.kv.vectors @ U[0], axis=1) @ np.expand_dims(U[0], axis=0).shape}")
            this.kv.vectors += sum([ (this.kv.vectors @ U[i:i+1].T) @ U[i:i+1] for i in range(this.centered_pca_dims)])
            this.kv.vectors -= sum([ (this.kv.vectors @ U[i:i+1].T) @ U[i:i+1] for i in range(pca_dims)])
            this.centered_pca_dims == pca_dims


        if not hasattr(this, 'whitened'):
            this.whitened = False

        U = this.centered_pca.components_
        logging.info(f"U={U}")
        logging.info(f"np.linalg.norm(U)={np.linalg.norm(U)}")
        logging.info(f"(this.whitened, whiten)={(this.whitened, whiten)}")
        if this.whitened == whiten:
            pass
        elif whiten:
            logging.info(f"np.linalg.norm(this.kv.vectors[1000:1010], axis=1)={np.linalg.norm(this.kv.vectors[1000:1010], axis=1)}")
            this.kv.vectors = this.kv.vectors @ (U * this.centered_pca.singular_values_**(-1/2))
            this.kv.vectors /= np.expand_dims(np.linalg.norm(this.kv.vectors, axis=1), axis=1)

            this.whitened = True
            logging.info(f"np.linalg.norm(this.kv.vectors[1000:1010], axis=1)={np.linalg.norm(this.kv.vectors[1000:1010], axis=1)}")
        else:
            this.kv.vectors = this.kv.vectors @ np.linalg.inv(U)
            this.whitened = False
        logging.info(f"(this.whitened, whiten)={(this.whitened, whiten)}")


    center_vocabulary(embedding_out, center, pca_dims, whiten)
    center_vocabulary(embedding_in, center, pca_dims, whiten)
    
    # helper functions

    def words_to_points(e, words):
        return [ e.kv[word] for word in words if word in e.kv ]

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

    rand_indexes = [ round(math.exp(i/n_rand*math.log(len(embedding_out.kv.index_to_key)))) for i in range(n_rand) ]
    rand_words = [ embedding_out.kv.index_to_key[i] for i in rand_indexes ]
    #if n_rand>0:
    #    for i,word in enumerate(embedding_out.kv.index_to_key[10:-len(embedding_out.kv.index_to_key)//2]):
    #        if i % (len(embedding_out.kv.index_to_key)/n_rand) == 0:
    #            logging.info(f"candidate_words={candidate_words}")
    #            rand_words.append(word)

    subspace_words = [ word for words in subspace_wordss for word in words ]
    search_words = subspace_words + seed_words
    logging.info(f"search_words={search_words}")
    search_points = words_to_points(embedding_in, search_words)
    neighbor_words = []
    for point in search_points:
        for neighbor, rank in embedding_out.most_similar(point, topn=max(1,n//len(search_points))):
            neighbor_words.append(neighbor)

    words = list(set(rand_words + top_words + neighbor_words))
    logging.info(f"len(words)={len(words)}")
    words_points = np.array(words_to_points(embedding_out, words))
    logging.info(f"words_points.shape={words_points.shape}")
    frequencies = np.array([ embedding_out.word_frequency(word) for word in words ])
    
    if a:
        words_points = words_points * (a / (a + np.expand_dims(frequencies, axis=1)))
        logging.info(f"a={a}")

    # compute projection values

    subspace_pointss = [ words_to_points(embedding_in, words) for words in subspace_wordss ]

    V = np.array([ normalize(mean(subspace_points)) for subspace_points in subspace_pointss ])
    logging.info(f"V.shape={V.shape}")

    p0 = normalize(np.sum(V, axis=0))
    logging.info(f"p0.shape={p0.shape}")
    if len(search_words) > 0:
        P = np.array([p0] + [normalize(vj - V[0]) for vj in V[1:]])
        logging.info(f"P.shape={P.shape}")
        Pmod = P_rand[:rand_dim-P.shape[0], :P.shape[1]]
        logging.info(f"Pmod.shape={Pmod.shape}")
        P = np.concatenate([P, Pmod], axis=0)
        U = V @ P.T
        logging.info(f"U.shape={U.shape}")

    else:
        P = P_rand[:,:dim]
        logging.info(f"P.shape={P.shape}")
        #U = np.expand_dims(P_rand[0,:],axis=0)
        U = np.zeros((0,dims))
        logging.info(f"U.shape={U.shape}")
    logging.info(f"P.shape={P.shape}")

    angles = [ float(np.arccos(np.dot(V[0],vj)/np.linalg.norm(V[0])/np.linalg.norm(vj)))/math.pi*180 for vj in V[1:] ]

    W = words_points @ P.T
    logging.info(f"W.shape={W.shape}")

    distances = abs(W[:,0] - U[0,0])
    logging.info(f"distances.shape={distances.shape}")

    return {
        'words': {
            'words' : words,
            'Wtrans': W.T.tolist(),
            'distances' : distances.tolist(),
            'frequencies': frequencies.tolist(),
        },
        'mus': {
            'Utrans': U.T.tolist(),
            'angles': angles,
            }
        }

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

