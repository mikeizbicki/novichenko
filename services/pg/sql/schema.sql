-- tablespaces cannot be created within a transaction
CREATE TABLESPACE fastdata LOCATION '/fastdata';

-- if there is an error in the file, then we should abort;
-- the entire file is contained within a transaction, so either everything will be defined or nothing
\set ON_ERROR_STOP on

BEGIN;

-- this db doesn't directly use python,
-- but the pspacy and pgrollup extensions do
CREATE LANGUAGE plpython3u;

-- extensions for improved indexing
CREATE EXTENSION rum;
CREATE EXTENSION pspacy;

-- extensions used by pgrollup
CREATE EXTENSION hll;
CREATE EXTENSION tdigest;
CREATE EXTENSION datasketches;
CREATE EXTENSION topn;
CREATE EXTENSION pg_cron;

-- configure pgrollup for minimal overhead rollup tables
CREATE EXTENSION pgrollup;
UPDATE pgrollup_settings SET value='cron' WHERE name='default_mode';

-- extensions for improved debugging
CREATE EXTENSION pg_stat_statements;

/*******************************************************************************
 * generic helper functions
 */

/*
 * reverse an array, see: https://wiki.postgresql.org/wiki/Array_reverse
 */
CREATE OR REPLACE FUNCTION array_reverse(anyarray) RETURNS anyarray AS $$
SELECT ARRAY(
    SELECT $1[i]
    FROM generate_subscripts($1,1) AS s(i)
    ORDER BY i DESC
);
$$ LANGUAGE 'sql' STRICT IMMUTABLE PARALLEL SAFE;

/*
 * the btree index cannot support text column sizes that are large;
 * this function truncates the input to an acceptable size
 */
CREATE OR REPLACE FUNCTION btree_sanitize(t TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    RETURN SUBSTRING(t FOR 2048);
END
$$;

/*
 * extract all of the ngrams out of a tsvector
 *
 * FIXME:
 * This implementation is not particularly efficient:
 * It performs multiple passes over the data and has unneeded sort operations.
 * This appears to be necessary for a SQL implementation of this function.
 * A native-C implementation is likely able to avoid these issues.
 * In theory, this should be computable with only a single pass over the tsvector.
 */
CREATE OR REPLACE FUNCTION tsvector_to_ngrams(tsv tsvector)
RETURNS TABLE(lexeme TEXT) LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $$
WITH cte AS (
SELECT
    lexeme,
    unnest(positions) AS positions
FROM unnest(tsv)
ORDER BY positions
)
SELECT DISTINCT lexeme
FROM (
    SELECT lexeme
    FROM cte
    UNION
    SELECT 
        CASE WHEN lag(positions,1) OVER (ORDER BY positions) = positions-1
             THEN lag(lexeme,1) OVER (ORDER BY positions) || ' ' || lexeme 
             ELSE NULL
             END
             AS "2gram"
    FROM cte
    UNION
    SELECT
        CASE WHEN lag(positions,1) OVER (ORDER BY positions) = positions-1 AND lag(positions,2) OVER (ORDER BY positions) = positions-2
             THEN lag(lexeme,2) OVER (ORDER BY positions) || ' ' || lag(lexeme,1) OVER (ORDER BY positions) || ' ' || lexeme 
             WHEN lag(positions,1) OVER (ORDER BY positions) = positions-2
             THEN lag(lexeme,1) OVER (ORDER BY positions) || ' ' || '_' || ' ' || lexeme 
             ELSE NULL
             END
        AS "3gram"
    FROM cte
)t
WHERE lexeme IS NOT NULL;
$$;

/*
WITH cte AS (
SELECT
    lexeme,
    unnest(positions) AS positions
--FROM unnest(tsv)
FROM unnest(to_tsvector('fancy apple pie crust is the most delicious food item that I have ever eaten.'))
)
SELECT
    positions,
    lexeme AS "1gram",
    CASE WHEN lag(positions,1) OVER (ORDER BY positions) = positions-1
         THEN lag(lexeme,1) OVER (ORDER BY positions) || ' ' || lexeme 
         ELSE NULL
         END
         AS "2gram",
    CASE WHEN lag(positions,1) OVER (ORDER BY positions) = positions-1 AND lag(positions,2) OVER (ORDER BY positions) = positions-2
         THEN lag(lexeme,2) OVER (ORDER BY positions) || ' ' || lag(lexeme,1) OVER (ORDER BY positions) || ' ' || lexeme 
         WHEN lag(positions,1) OVER (ORDER BY positions) = positions-2
         THEN lag(lexeme,1) OVER (ORDER BY positions) || ' ' || '_' || ' ' || lexeme 
         ELSE NULL
         END
         AS "3gram"
FROM cte;
*/

/*******************************************************************************
 * functions for extracting the components of a url stored as text
 * NOTE:
 * the extension pguri (https://github.com/petere/pguri) is specifically designed for storing url data;
 * but it requires that all input urls be properly formatted;
 * that will not be the case for our urls,
 * and so that's why we must manually implement these functions
 */

/*
 * remove the scheme from an input url
 *
 * FIXME: what to do for mailto:blah@gmail.com ?
 */
CREATE OR REPLACE FUNCTION url_remove_scheme(url TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    RETURN COALESCE(SUBSTRING(url, '[^:/]*//(.*)'),url);
END 
$$;

do $$
BEGIN
    assert( url_remove_scheme('https://cnn.com') = 'cnn.com');
    assert( url_remove_scheme('https://cnn.com/') = 'cnn.com/');
    assert( url_remove_scheme('https://www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html') = 'www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html');
    assert( url_remove_scheme('http://cnn.com') = 'cnn.com');
    assert( url_remove_scheme('http://cnn.com/') = 'cnn.com/');
    assert( url_remove_scheme('http://www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html') = 'www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html');
    assert( url_remove_scheme('cnn.com') = 'cnn.com');
    assert( url_remove_scheme('cnn.com/') = 'cnn.com/');
    assert( url_remove_scheme('www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html') = 'www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html');
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION url_host(url TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    url_without_scheme TEXT = url_remove_scheme(url);
BEGIN
    RETURN SUBSTRING(url_without_scheme, '([^/?:]*):?[^/?]*[/?]?');
END 
$$;

do $$
BEGIN
    assert( url_host('https://cnn.com') = 'cnn.com');
    assert( url_host('https://cnn.com/') = 'cnn.com');
    assert( url_host('https://www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html') = 'www.cnn.com');
    assert( url_host('http://cnn.com') = 'cnn.com');
    assert( url_host('http://cnn.com/') = 'cnn.com');
    assert( url_host('http://www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html') = 'www.cnn.com');
    assert( url_host('cnn.com') = 'cnn.com');
    assert( url_host('cnn.com/') = 'cnn.com');
    assert( url_host('www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html') = 'www.cnn.com');
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION url_path(url TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    url_without_scheme TEXT = url_remove_scheme(url);
BEGIN
    RETURN COALESCE(SUBSTRING(url_without_scheme, '[^/?]+([/][^;#?]*)'),'/');
END 
$$;

do $$
BEGIN
    assert( url_path('https://cnn.com') = '/');
    assert( url_path('https://cnn.com/') = '/');
    assert( url_path('https://www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html') = '/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html');
    assert( url_path('http://cnn.com') = '/');
    assert( url_path('http://cnn.com/') = '/');
    assert( url_path('http://www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html') = '/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html');
    assert( url_path('cnn.com') = '/');
    assert( url_path('cnn.com/') = '/');
    assert( url_path('www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html') = '/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html');

    assert( url_path('https://example.com/path/to/index.html?a=b&c=d') = '/path/to/index.html');
    assert( url_path('https://example.com/index.html?a=b&c=d') = '/index.html');
    assert( url_path('https://example.com/?a=b&c=d') = '/');

    assert( url_path('https://example.com/path/to/index.html;test?a=b&c=d') = '/path/to/index.html');
    assert( url_path('https://example.com/index.html;test?a=b&c=d') = '/index.html');
    assert( url_path('https://example.com/;test?a=b&c=d') = '/');

    assert( url_path('https://example.com/path/to/index.html#test') = '/path/to/index.html');
    assert( url_path('https://example.com/index.html#test') = '/index.html');
    assert( url_path('https://example.com/#test') = '/');
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION url_query(url TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    RETURN COALESCE(SUBSTRING(url, '\?([^?#]*)'),'');
END 
$$;

do $$
BEGIN
    assert( url_query('https://example.com/path/to/index.html?a=b&c=d') = 'a=b&c=d');
    assert( url_query('https://example.com/index.html?a=b&c=d') = 'a=b&c=d');
    assert( url_query('https://example.com/?a=b&c=d') = 'a=b&c=d');

    assert( url_query('https://example.com/path/to/index.html?a=b&c=d#test') = 'a=b&c=d');
    assert( url_query('https://example.com/index.html?a=b&c=d#test') = 'a=b&c=d');
    assert( url_query('https://example.com/?a=b&c=d#test') = 'a=b&c=d');

    assert( url_query('https://example.com/path/to/index.html') = '');
    assert( url_query('https://example.com/index.html') = '');
    assert( url_query('https://example.com/') = '');

    assert( url_query('/path/to/index.html?a=b&c=d#test') = 'a=b&c=d');
    assert( url_query('/index.html?a=b&c=d#test') = 'a=b&c=d');
    assert( url_query('/?a=b&c=d#test') = 'a=b&c=d');
END;
$$ LANGUAGE plpgsql;


----------------------------------------
-- simplification functions

/*
 * remove extraneous leading subdomains from a host
 */
CREATE OR REPLACE FUNCTION host_simplify(host TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    RETURN COALESCE(
        SUBSTRING(host, '^www\d*\.(.*)'),
        SUBSTRING(host, '^m\.(.*)'),
        host
    );
END 
$$;

do $$
BEGIN
    assert( host_simplify('cnn.com') = 'cnn.com');
    assert( host_simplify('www.cnn.com') = 'cnn.com');
    assert( host_simplify('www2.cnn.com') = 'cnn.com');
    assert( host_simplify('www5.cnn.com') = 'cnn.com');
    assert( host_simplify('www577.cnn.com') = 'cnn.com');
    assert( host_simplify('bbc.co.uk') = 'bbc.co.uk');
    assert( host_simplify('www.bbc.co.uk') = 'bbc.co.uk');
    assert( host_simplify('en.wikipedia.org') = 'en.wikipedia.org');
    assert( host_simplify('m.wikipedia.org') = 'wikipedia.org');
    assert( host_simplify('naenara.com.kp') = 'naenara.com.kp');
END;
$$ LANGUAGE plpgsql;

/*
 * converts a host into the key syntax used by the common crawl
 * the main feature is that subdomains are in reverse order,
 * so string matches starting from the left hand side become increasingly specific
 */
CREATE OR REPLACE FUNCTION host_key(host TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    RETURN array_to_string(array_reverse(string_to_array(host,'.')),',')||')';
END 
$$;

do $$
BEGIN
    assert( host_key('cnn.com') = 'com,cnn)');
    assert( host_key('www.cnn.com') = 'com,cnn,www)');
    assert( host_key('www.bbc.co.uk') = 'uk,co,bbc,www)');
END;
$$ LANGUAGE plpgsql;

/*
 * converts from the host_key syntax into the standard host syntax;
 */
CREATE OR REPLACE FUNCTION host_unkey(host TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    RETURN array_to_string(array_reverse(string_to_array(substring(host from 0 for char_length(host)),',')),'.');
END 
$$;

do $$
BEGIN
    assert( host_unkey(host_key('cnn.com')) = 'cnn.com');
    assert( host_unkey(host_key('www.cnn.com')) = 'www.cnn.com');
    assert( host_unkey(host_key('www.bbc.co.uk')) = 'www.bbc.co.uk');
END;
$$ LANGUAGE plpgsql;

/*
 * removes default webpages like index.html from the end of the path,
 * and removes trailing slashes from the end of the path;
 * technically, these changes can modify the path to point to a new location,
 * but this is extremely rare in practice
 */
CREATE OR REPLACE FUNCTION path_simplify(path TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    path_without_index TEXT = COALESCE(
        SUBSTRING(path, '(.*/)index.\w{3,4}$'),
        path
    );
BEGIN
    RETURN COALESCE(
        SUBSTRING(path_without_index, '(.*)/$'),
        path_without_index
    );
END 
$$;

do $$
BEGIN
    assert( path_simplify('/path/to/index.html/more/paths') = '/path/to/index.html/more/paths');
    assert( path_simplify('/path/to/index.html') = '/path/to');
    assert( path_simplify('/path/to/index.htm') = '/path/to');
    assert( path_simplify('/path/to/index.asp') = '/path/to');
    assert( path_simplify('/path/to/') = '/path/to');
    assert( path_simplify('/index.html') = '');
    assert( path_simplify('/index.htm') = '');
    assert( path_simplify('/') = '');
    assert( path_simplify('') = '');
END;
$$ LANGUAGE plpgsql;


/*
 * sorts query terms and removes query terms used only for tracking
 * see: https://en.wikipedia.org/wiki/UTM_parameters
 * see: https://github.com/mpchadwick/tracking-query-params-registry/blob/master/data.csv
 * for the sorting step, see: https://stackoverflow.com/questions/2913368/sorting-array-elements
 */
CREATE OR REPLACE FUNCTION query_simplify(query TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    RETURN array_to_string(array(
        SELECT * FROM unnest(string_to_array(query,'&')) AS unnest
        WHERE unnest.unnest NOT LIKE 'utm_%'
        ORDER BY unnest.unnest ASC
    ),'&');
END 
$$;

do $$
BEGIN
    assert( query_simplify('a=1&b=2&utm_source=google.com') = 'a=1&b=2');
    assert( query_simplify('a=1&utm_source=google.com&b=2') = 'a=1&b=2');
    assert( query_simplify('utm_source=google.com&a=1&b=2') = 'a=1&b=2');
    assert( query_simplify('a=1&b=2') = 'a=1&b=2');
    assert( query_simplify('b=1&a=2') = 'a=2&b=1');
    assert( query_simplify('a=1') = 'a=1');
    assert( query_simplify('') = '');
END;
$$ LANGUAGE plpgsql;

----------------------------------------
-- functions for indexing

CREATE OR REPLACE FUNCTION url_host_key(url TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    url_lower TEXT = lower(url);
BEGIN
    RETURN btree_sanitize(host_key(host_simplify(url_host(url_lower))));
END 
$$;

do $$
BEGIN
    assert( url_host_key('https://example.com') = 'com,example)');
    assert( url_host_key('https://example.com/') = 'com,example)');
    assert( url_host_key('https://example.com/#test') = 'com,example)');
    assert( url_host_key('https://example.com/?param=12') = 'com,example)');
    assert( url_host_key('https://example.com/path/to') = 'com,example)');
    assert( url_host_key('https://example.com/path/to/') = 'com,example)');
    assert( url_host_key('https://example.com/path/to/#test') = 'com,example)');
    assert( url_host_key('https://example.com/path/to/?param=12') = 'com,example)');
    assert( url_host_key('https://Example.com/Path/To/?Param=12') = 'com,example)');
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION url_hostpath_key(url TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    url_lower TEXT = lower(url);
BEGIN
    RETURN btree_sanitize(host_key(host_simplify(url_host(url_lower))) || path_simplify(url_path(url_lower)));
END 
$$;

do $$
BEGIN
    assert( url_hostpath_key('https://example.com') = 'com,example)');
    assert( url_hostpath_key('https://example.com/') = 'com,example)');
    assert( url_hostpath_key('https://example.com/#test') = 'com,example)');
    assert( url_hostpath_key('https://example.com/?param=12') = 'com,example)');
    assert( url_hostpath_key('https://example.com/path/to') = 'com,example)/path/to');
    assert( url_hostpath_key('https://example.com/path/to/') = 'com,example)/path/to');
    assert( url_hostpath_key('https://example.com/path/to/#test') = 'com,example)/path/to');
    assert( url_hostpath_key('https://example.com/path/to/?param=12') = 'com,example)/path/to');
    assert( url_hostpath_key('https://Example.com/Path/To/?Param=12') = 'com,example)/path/to');
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION url_hostpathquery_key(url TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    url_lower TEXT = lower(url);
    query TEXT = query_simplify(url_query(url_lower));
BEGIN
    RETURN btree_sanitize(
        host_key(host_simplify(url_host(url_lower))) || 
        path_simplify(url_path(url_lower)) || 
        CASE WHEN length(query)>0
            THEN '?' || query
            ELSE ''
        END
    );
END 
$$;

do $$
BEGIN
    assert( url_hostpathquery_key('https://example.com') = 'com,example)');
    assert( url_hostpathquery_key('https://example.com/') = 'com,example)');
    assert( url_hostpathquery_key('https://example.com/#test') = 'com,example)');
    assert( url_hostpathquery_key('https://example.com/?param=12') = 'com,example)?param=12');
    assert( url_hostpathquery_key('https://example.com/path/to') = 'com,example)/path/to');
    assert( url_hostpathquery_key('https://example.com/path/to/') = 'com,example)/path/to');
    assert( url_hostpathquery_key('https://example.com/path/to/#test') = 'com,example)/path/to');
    assert( url_hostpathquery_key('https://example.com/path/to/?param=12') = 'com,example)/path/to?param=12');
    assert( url_hostpathquery_key('https://Example.com/Path/To/?Param=12') = 'com,example)/path/to?param=12');
END;
$$ LANGUAGE plpgsql;


/*******************************************************************************
 * preloaded data
 ******************************************************************************/

/*
 * stores information about iso639-3 language codes
 */
CREATE TABLE iso639 (
    part3       CHAR(3) UNIQUE NOT NULL,
    part2b      CHAR(3) UNIQUE,
    part2t      CHAR(3) UNIQUE,
    part1       CHAR(2) UNIQUE,
    scope       CHAR(1) NOT NULL, -- I(ndividual), M(acrolanguage), S(pecial)
    type        CHAR(1) NOT NULL, -- A(ncient), C(onstructed), E(xtinct), H(istorical), L(iving), S(pecial)
    ref_name    TEXT UNIQUE NOT NULL,
    comment     TEXT
);
COPY iso639 FROM '/tmp/data/iso639/iso-639-3.tab' DELIMITER E'\t' CSV HEADER;

CREATE OR REPLACE FUNCTION language_iso639(language TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    language_simplified TEXT = LOWER(TRIM( E'"'' \t:-_,\n' FROM language));
    ret TEXT;
BEGIN
    /*
    IF LENGTH(language_simplified) = 2 OR SUBSTRING(language_simplified, 3, 1) IN (':','_','-',' ') THEN
        SELECT part3 INTO ret FROM iso639 WHERE part1 = SUBSTRING(language_simplified, 1, 2);
        RETURN COALESCE(ret,'invalid');
    ELSIF LENGTH(language_simplified) = 3 OR SUBSTRING(language_simplified, 4, 1) IN (':','_','-',' ') THEN
        RETURN SUBSTRING(language_simplified, 1, 3);
    ELSE 
        SELECT part3 INTO ret FROM iso639 WHERE lower(ref_name) = language_simplified;
        RETURN COALESCE(ret,'invalid');
    END IF;
    */
    RETURN CASE
        WHEN LENGTH(language_simplified) <= 3 THEN language_simplified
        WHEN SUBSTRING(language_simplified, 3, 1) IN (':','_','-',' ') THEN SUBSTRING(language_simplified, 1, 2)
        WHEN SUBSTRING(language_simplified, 4, 1) IN (':','_','-',' ') THEN SUBSTRING(language_simplified, 1, 3)
        ELSE 'invalid'
        END;
END 
$$;
do $$
BEGIN
    assert( language_iso639('aat') = 'aat');
    assert( language_iso639('EN   ') = 'en');
    assert( language_iso639('en:   ') = 'en');
    assert( language_iso639('"en"') = 'en');
    assert( language_iso639('en:us') = 'en');
    assert( language_iso639('EN-us') = 'en');
    assert( language_iso639('en_us') = 'en');
    assert( language_iso639('eng:us') = 'eng');
    assert( language_iso639('eNG-us') = 'eng');
    assert( language_iso639('eng_us') = 'eng');
    assert( language_iso639('english') = 'invalid');
END;
$$ LANGUAGE plpgsql;

/*
 * stores the information about metahtml's test cases
 */
CREATE TABLE metahtml_test (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    jsonb JSONB NOT NULL
);
COPY metahtml_test(jsonb) FROM '/tmp/metahtml/golden.jsonl';

CREATE MATERIALIZED VIEW metahtml_test_summary AS (
    SELECT
        hll_count(jsonb->>'url') AS url,
        hll_count(url_hostpathquery_key(jsonb->>'url')) AS hostpathquery,
        hll_count(url_hostpath_key(jsonb->>'url')) AS hostpath,
        hll_count(url_host_key(jsonb->>'url')) AS host
    FROM metahtml_test
);

CREATE MATERIALIZED VIEW metahtml_test_summary_host AS (
    SELECT
        url_host_key(jsonb->>'url') AS host,
        hll_count(jsonb->>'url') AS url,
        hll_count(url_hostpathquery_key(jsonb->>'url')) AS hostpathquery,
        hll_count(url_hostpath_key(jsonb->>'url')) AS hostpath
    FROM metahtml_test
    GROUP BY host
);

CREATE MATERIALIZED VIEW metahtml_test_summary_language AS (
    SELECT
        language_iso639(jsonb->>'language'),
        hll_count(jsonb->>'url') AS url,
        hll_count(url_hostpathquery_key(jsonb->>'url')) AS hostpathquery,
        hll_count(url_hostpath_key(jsonb->>'url')) AS hostpath,
        hll_count(url_host_key(jsonb->>'url')) AS host
    FROM metahtml_test
    GROUP BY language_iso639
);

/*
 * stores website rank information
 */
CREATE TABLE top_1m_alexa (
    rank INTEGER,
    host TEXT
);
COPY top_1m_alexa FROM '/tmp/data/top-1m-alexa/top-1m.csv' DELIMITER ',' CSV HEADER;
CREATE INDEX top_1m_alexa_idx ON top_1m_alexa(url_host_key(host),rank);

CREATE TABLE top_1m_opendns (
    rank INTEGER,
    host TEXT
);
COPY top_1m_opendns FROM '/tmp/data/top-1m-opendns/top-1m.csv' DELIMITER ',' CSV HEADER;
CREATE INDEX top_1m_opendns_idx ON top_1m_opendns(url_host_key(host),rank);

/*
 * stores manually annotated information about hostnames
 */
CREATE TABLE hostnames (
    id_hostnames INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hostname VARCHAR(253) NOT NULL CHECK (hostname = lower(hostname)),
    priority TEXT,
    name_native TEXT,
    name_latin TEXT,
    language TEXT,
    country TEXT,
    type TEXT
);
COPY hostnames(hostname,priority,name_native,name_latin,country,language,type) FROM '/tmp/data/hostnames.csv' DELIMITER ',' CSV HEADER;

CREATE VIEW hostnames_untested AS (
    SELECT url_host_key(hostname)
    FROM hostnames
    WHERE
        COALESCE(priority,'') != 'ban' AND
        url_host_key(hostnames.hostname) NOT IN (
            SELECT DISTINCT url_host_key(jsonb->>'url')
            FROM metahtml_test
        )
    ORDER BY country,hostname
    );

/*
 * uses data scraped from allsides.com
 */
CREATE TABLE allsides (
    id_allsides INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url TEXT,
    type TEXT,
    name TEXT,
    bias TEXT
);
COPY allsides(url,type,name,bias) FROM '/tmp/data/allsides/allsides.csv' CSV HEADER;

CREATE VIEW allsides_untested AS (
    SELECT DISTINCT
        url_host_key(url) AS host_key
    FROM allsides
    WHERE
        url_host_key(url) NOT IN (
            SELECT DISTINCT url_host_key(jsonb->>'url')
            FROM metahtml_test
        )
    ORDER BY host_key
    );

CREATE VIEW allsides_summary AS (
    SELECT
        type,
        bias,
        count(*)
    FROM allsides
    GROUP BY type,bias
);

/*
 * uses data scraped from mediabiasfactcheck.com
 */
CREATE TABLE mediabiasfactcheck (
    id_allsides INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url TEXT,
    name TEXT,
    image_pseudoscience TEXT,
    image_factual TEXT,
    image_conspiracy TEXT,
    image_bias TEXT,
    freedom_rank TEXT,
    country TEXT
);
COPY mediabiasfactcheck(url,name,image_pseudoscience,image_factual,image_conspiracy,image_bias,freedom_rank,country) FROM '/tmp/data/mediabiasfactcheck/mediabiasfactcheck.csv' CSV HEADER;

CREATE VIEW mediabiasfactcheck_untested AS (
    SELECT DISTINCT
        url_host_key(url) AS host_key
    FROM mediabiasfactcheck
    WHERE
        url_host_key(url) NOT IN (
            SELECT DISTINCT url_host_key(jsonb->>'url')
            FROM metahtml_test
        )
    ORDER BY host_key
    );

/*
SELECT create_rollup(
    'mediabiasfactcheck',
    'mediabiasfactcheck_rollup_image_bias',
    wheres => $$
        image_bias
    $$
);

SELECT create_rollup(
    'mediabiasfactcheck',
    'mediabiasfactcheck_rollup_image_conspiracy',
    wheres => $$
        image_conspiracy
    $$
);

SELECT create_rollup(
    'mediabiasfactcheck',
    'mediabiasfactcheck_rollup_image_pseudoscience',
    wheres => $$
        image_pseudoscience
    $$
);

SELECT create_rollup(
    'mediabiasfactcheck',
    'mediabiasfactcheck_rollup_image_factual',
    wheres => $$
        image_factual
    $$
);

SELECT create_rollup(
    'mediabiasfactcheck',
    'mediabiasfactcheck_rollup_country',
    wheres => $$
        country
    $$
);
*/

/*
 * This dataset annotates the bias of specific urls
 * See: https://deepblue.lib.umich.edu/data/concern/data_sets/8w32r569d?locale=en
 */
CREATE TABLE quantifyingnewsmediabias (
    id_quantifyingnewsmediabias INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url TEXT,
    q3 TEXT,
    perceived SMALLINT,
    primary_topic TEXT,
    secondary_topic TEXT,
    democrat_vote TEXT,
    republican_vote TEXT
);
COPY quantifyingnewsmediabias(url,q3,perceived,primary_topic,secondary_topic,democrat_vote,republican_vote) FROM '/tmp/data/QuantifyingNewsMediaBias/newsArticlesWithLabels.tsv' DELIMITER E'\t' CSV HEADER;

CREATE VIEW quantifyingnewsmediabias_untested AS (
    SELECT DISTINCT
        url_host_key(url) AS host_key
    FROM quantifyingnewsmediabias
    WHERE
        url_host_key(url) NOT IN (
            SELECT DISTINCT url_host_key(jsonb->>'url')
            FROM metahtml_test
        )
    ORDER BY host_key
    );

CREATE VIEW qualitifyingnewsmediabias_summary AS (
    SELECT
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath,
        hll_count(url_host_key(url)) AS host
    FROM quantifyingnewsmediabias
);

/*******************************************************************************
 * main tables
 ******************************************************************************/

/*
 * stores information about the source of the data
 */
CREATE TABLE source (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    name TEXT UNIQUE NOT NULL
);
INSERT INTO source (id,name) VALUES (-1,'metahtml');

/*
 * The primary table for storing extracted content
 */

CREATE TABLE metahtml (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_source INTEGER NOT NULL REFERENCES source(id),
    accessed_at TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    url TEXT NOT NULL,
    jsonb JSONB NOT NULL,
    title tsvector,
    content tsvector
);


--------------------------------------------------------------------------------
-- rollups for tracking debug info

CREATE MATERIALIZED VIEW metahtml_versions AS (
    SELECT 
        jsonb->>'version' AS version,
        hll_count(url) as url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath,
        hll_count(url_host_key(url)) AS host
    FROM metahtml
    GROUP BY version
);

-- FIXME:
-- the "type" column is not detailed enough, but str(e) is too detailed.
CREATE MATERIALIZED VIEW metahtml_exceptions_host AS (
    SELECT
        url_host(url) AS host,
        jsonb->'exception'->>'type' AS type,
        jsonb->'exception'->>'location' AS location,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY host,type,location
);

CREATE MATERIALIZED VIEW metahtml_insert AS (
    SELECT
        date_trunc('hour', inserted_at) AS insert_hour,
        hll_count(id_source),
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath,
        hll_count(url_host_key(url)) AS host
    FROM metahtml
    GROUP BY insert_hour
);

--------------------------------------------------------------------------------
-- rollups for pagerank

-- FIXME: we should hll_count the dest urls, but it doesn't work with set functions
CREATE MATERIALIZED VIEW metahtml_linksall_host AS (
    SELECT
        url_host_key(url) AS src,
        url_host_key(jsonb_array_elements(jsonb->'links.all'->'best'->'value')->>'href') AS dest,
        hll_count(url) AS src_url,
        hll_count(url_hostpathquery_key(url)) AS src_hostpathquery,
        hll_count(url_hostpath_key(url)) AS src_hostpath
    FROM metahtml
    GROUP BY src,dest
);

/* FIXME: materialize this
SELECT
    dest,
    sum(src_url             /COALESCE(rank,1000000)) as src_url,
    sum(src_hostpathquery   /COALESCE(rank,1000000)) as src_hostpathquery,
    sum(src_hostpath        /COALESCE(rank,1000000)) as src_hostpath,
    sum(1                   /COALESCE(rank,1000000)) as src_host
FROM metahtml_linksall_host
LEFT JOIN top_1m_alexa ON (url_host_key(metahtml_linksall_host.src) = url_host_key(top_1m_alexa.host))
GROUP BY dest
ORDER BY src_host DESC;
*/

--------------------------------------------------------------------------------
-- rollups for basic stats

CREATE MATERIALIZED VIEW metahtml_hoststats AS (
    SELECT
        url_host_key(url) AS host,
        jsonb->'language'->'best'->>'value' AS language,
        hll_count(id_source) AS id_source,
        hll_count(jsonb->'timestamp.published') AS timestamp_published,
        hll_count(jsonb->'timestamp.modified') AS timestamp_modified,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY host,language
);

CREATE MATERIALIZED VIEW metahtml_hoststats_filtered AS (
    SELECT
        url_host_key(url) AS host,
        jsonb->'language'->'best'->>'value' AS language,
        hll_count(id_source) AS id_source,
        hll_count(jsonb->'timestamp.published') AS timestamp_published,
        hll_count(jsonb->'timestamp.modified') AS timestamp_modified,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    WHERE url ILIKE '%news%'
       OR url ILIKE '%blog%'
       OR url ILIKE '%article%'
       OR url ILIKE '%archive%'
    GROUP BY host,language
);

CREATE VIEW metahtml_langstats AS (
    SELECT
        language_iso639(language) AS language_iso639,
        sum(timestamp_published) AS timestamp_published,
        sum(timestamp_modified) AS timestamp_modified,
        sum(url) AS url,
        sum(hostpathquery) AS hostpathquery,
        sum(hostpath) AS hostpath,
        count(host) AS host
    FROM metahtml_hoststats
    GROUP BY language_iso639
);

CREATE VIEW hostnames_to_check AS (
    SELECT 
        host_unkey(host) as host,
        language_iso639(language),
        hostpath^1.5/(1+timestamp_published) as score,
        timestamp_published,
        hostpath
    FROM metahtml_hoststats_filtered
    WHERE host_unkey(host) NOT IN (SELECT host FROM metahtml_test_summary_host)
    ORDER BY score DESC,host
);

CREATE VIEW hostnames_to_check_untested AS (
    SELECT
        host_unkey(untested.url_host_key) as host
    FROM top_1m_alexa
    RIGHT JOIN
        (
        SELECT * FROM hostnames_untested
        UNION
        SELECT * FROM allsides_untested
        UNION
        SELECT * FROM mediabiasfactcheck_untested
        ) untested ON url_host_key(top_1m_alexa.host) = untested.url_host_key
    ORDER BY rank ASC
);

--------------------------------------------------------------------------------
-- rollups for text

CREATE MATERIALIZED VIEW metahtml_rollup_content_textlangmonth TABLESPACE fastdata AS (
    SELECT
        tsvector_to_ngrams(content) AS alltext,
        jsonb->'language'->'best'->>'value' AS language, 
        date_trunc('month',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY alltext,language,timestamp_published
);

CREATE MATERIALIZED VIEW metahtml_rollup_content_langmonth AS (
    SELECT
        jsonb->'language'->'best'->>'value' AS language, 
        date_trunc('month',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    WHERE length(content)>0
    GROUP BY language,timestamp_published
);

CREATE MATERIALIZED VIEW metahtml_rollup_title_textlangmonth TABLESPACE fastdata AS (
    SELECT
        tsvector_to_ngrams(title) AS alltext,
        jsonb->'language'->'best'->>'value' AS language, 
        date_trunc('month',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY alltext,language,timestamp_published
);

CREATE MATERIALIZED VIEW metahtml_rollup_title_langmonth AS (
    SELECT
        jsonb->'language'->'best'->>'value' AS language, 
        date_trunc('month',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    WHERE length(title)>0
    GROUP BY language,timestamp_published
);

CREATE MATERIALIZED VIEW metahtml_rollup_textlangmonth TABLESPACE fastdata AS (
    SELECT
        tsvector_to_ngrams(COALESCE(title || content, content, title)) AS alltext,
        jsonb->'language'->'best'->>'value' AS language, 
        date_trunc('month',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY alltext,language,timestamp_published
);

CREATE MATERIALIZED VIEW metahtml_rollup_langmonth AS (
    SELECT
        jsonb->'language'->'best'->>'value' AS language, 
        date_trunc('month',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY language,timestamp_published
);

/* indexes for text search of the form

SELECT
    jsonb->'title'->'best'->>'value'
FROM metahtml
WHERE
    spacy_tsvector(
        jsonb->'language'->'best'->>'value',
        jsonb->'title'->'best'->>'value'
    ) @@ 
    spacy_tsquery('en', 'covid');
*/

CREATE INDEX ON metahtml USING rum (title) TABLESPACE fastdata;
CREATE INDEX ON metahtml USING rum (content) TABLESPACE fastdata;

--CREATE INDEX ON metahtml USING rum (title, url_host_key(url)) TABLESPACE fastdata;
/*
CREATE INDEX ON metahtml USING rum(title RUM_TSVECTOR_ADDON_OPS, (jsonb->'timestamp.published'->'best'->'value'->>'lo'))
  WITH (ATTACH='(jsonb->''timestamp.published''->''best''->''value''->>''lo'')', TO='title');
*/
CREATE INDEX ON metahtml USING rum(content RUM_TSVECTOR_ADDON_OPS, accessed_at)
  WITH (ATTACH='accessed_at', TO='content');

COMMIT;


