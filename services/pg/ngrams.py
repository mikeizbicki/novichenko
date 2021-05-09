def tsvector_to_ngrams(tsv, n, uniq=True):
    '''
    tsvector generated from the code to_tsvector('fancy apple pie crust is the most delicious fancy pie that I have ever eaten; I love pie.')

    >>> tsvector_to_ngrams("'appl':2 'crust':4 'delici':8 'eaten':15 'ever':14 'fanci':1,9 'love':17 'pie':3,10,18", 1, False)
    ['fanci', 'appl', 'pie', 'crust', 'delici', 'fanci', 'pie', 'ever', 'eaten', 'love', 'pie']
    >>> tsvector_to_ngrams("'appl':2 'crust':4 'delici':8 'eaten':25 'ever':14 'fanci':1,9 'love':17 'pie':3,10,18", 2, False)
    ['fanci', 'appl', 'fanci appl', 'pie', 'appl pie', 'crust', 'pie crust', 'delici', 'fanci', 'delici fanci', 'pie', 'fanci pie', 'ever', 'love', 'pie', 'love pie', 'eaten']
    >>> tsvector_to_ngrams("'appl':2 'crust':4 'delici':8 'eaten':25 'ever':14 'fanci':1,9 'love':17 'pie':3,10,18", 3, False)
    ['fanci', 'appl', 'fanci appl', 'pie', 'appl pie', 'fanci appl pie', 'crust', 'pie crust', 'appl pie crust', 'delici', 'fanci', 'delici fanci', 'pie', 'fanci pie', 'delici fanci pie', 'ever', 'love', 'pie', 'love pie', 'eaten']
    '''
    positioned_lexemes = []
    for item in tsv.split():
        lexeme, positions = item.split(':')
        for position in positions.split(','):
            try:
                position = int(position)
                positioned_lexemes.append((position,lexeme.strip("'")))
            except ValueError:
                pass


    positioned_lexemes.sort()
    ngrams = []
    for i,(pos,lexeme) in enumerate(positioned_lexemes):
        ngrams.append(lexeme)
        ngram = lexeme
        for j in range(1, min(n,i+1)):
            prev_pos,prev_lexeme = positioned_lexemes[i-j]
            if prev_pos == pos - j:
                ngram = prev_lexeme + ' ' + ngram
                ngrams.append(ngram)
            else:
                break
    if uniq:
        ngrams = set(ngrams)
    return ngrams

