import chajda.embeddings

langs = ['en','es','ko','el','th']

for lang in langs:
    chajda.embeddings.get_embedding(lang=lang, max_n=400000, max_d=None, storage_dir='./embeddings')
