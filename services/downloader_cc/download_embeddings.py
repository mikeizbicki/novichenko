import chajda.embeddings

langs = ['en','fr','es','ko','el','th']

for lang in langs:
    chajda.embeddings.get_embedding(lang=lang, max_n=100000, max_d=25, storage_dir='./embeddings')
