import chajda.embeddings

langs = ['en','fr','es','ko','el','th','zh','sv','de','ru','ar','pt']

for lang in langs:
    chajda.embeddings.get_embedding(lang=lang, max_n=100000, max_d=50, storage_dir='./embeddings')
