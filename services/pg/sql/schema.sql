-- if there is an error in the file, then we should abort;
-- the entire file is contained within a transaction, so either everything will be defined or nothing
\set ON_ERROR_STOP on

BEGIN;

--------------------------------------------------------------------------------
-- general database health views
--------------------------------------------------------------------------------

-- this view computes the bloat of all tables
-- see: https://www.citusdata.com/blog/2017/10/20/monitoring-your-bloat-in-postgres/
CREATE VIEW health_bloat AS (
WITH constants AS (
    -- define some constants for sizes of things
    -- for reference down the query and easy maintenance
    SELECT current_setting('block_size')::numeric AS bs, 23 AS hdr, 8 AS ma
),
no_stats AS (
    -- screen out table who have attributes
    -- which dont have stats, such as JSON
    SELECT table_schema, table_name, 
        n_live_tup::numeric as est_rows,
        pg_table_size(relid)::numeric as table_size
    FROM information_schema.columns
        JOIN pg_stat_user_tables as psut
           ON table_schema = psut.schemaname
           AND table_name = psut.relname
        LEFT OUTER JOIN pg_stats
        ON table_schema = pg_stats.schemaname
            AND table_name = pg_stats.tablename
            AND column_name = attname 
    WHERE attname IS NULL
        AND table_schema NOT IN ('pg_catalog', 'information_schema')
    GROUP BY table_schema, table_name, relid, n_live_tup
),
null_headers AS (
    -- calculate null header sizes
    -- omitting tables which dont have complete stats
    -- and attributes which aren't visible
    SELECT
        hdr+1+(sum(case when null_frac <> 0 THEN 1 else 0 END)/8) as nullhdr,
        SUM((1-null_frac)*avg_width) as datawidth,
        MAX(null_frac) as maxfracsum,
        schemaname,
        tablename,
        hdr, ma, bs
    FROM pg_stats CROSS JOIN constants
        LEFT OUTER JOIN no_stats
            ON schemaname = no_stats.table_schema
            AND tablename = no_stats.table_name
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        AND no_stats.table_name IS NULL
        AND EXISTS ( SELECT 1
            FROM information_schema.columns
                WHERE schemaname = columns.table_schema
                    AND tablename = columns.table_name )
    GROUP BY schemaname, tablename, hdr, ma, bs
),
data_headers AS (
    -- estimate header and row size
    SELECT
        ma, bs, hdr, schemaname, tablename,
        (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
        (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
    FROM null_headers
),
table_estimates AS (
    -- make estimates of how large the table should be
    -- based on row and page size
    SELECT schemaname, tablename, bs,
        reltuples::numeric as est_rows, relpages * bs as table_bytes,
    CEIL((reltuples*
            (datahdr + nullhdr2 + 4 + ma -
                (CASE WHEN datahdr%ma=0
                    THEN ma ELSE datahdr%ma END)
                )/(bs-20))) * bs AS expected_bytes,
        reltoastrelid
    FROM data_headers
        JOIN pg_class ON tablename = relname
        JOIN pg_namespace ON relnamespace = pg_namespace.oid
            AND schemaname = nspname
    WHERE pg_class.relkind = 'r'
),
estimates_with_toast AS (
    -- add in estimated TOAST table sizes
    -- estimate based on 4 toast tuples per page because we dont have 
    -- anything better.  also append the no_data tables
    SELECT schemaname, tablename, 
        TRUE as can_estimate,
        est_rows,
        table_bytes + ( coalesce(toast.relpages, 0) * bs ) as table_bytes,
        expected_bytes + ( ceil( coalesce(toast.reltuples, 0) / 4 ) * bs ) as expected_bytes
    FROM table_estimates LEFT OUTER JOIN pg_class as toast
        ON table_estimates.reltoastrelid = toast.oid
            AND toast.relkind = 't'
),
table_estimates_plus AS (
-- add some extra metadata to the table data
-- and calculations to be reused
-- including whether we cant estimate it
-- or whether we think it might be compressed
    SELECT current_database() as databasename,
            schemaname, tablename, can_estimate, 
            est_rows,
            CASE WHEN table_bytes > 0
                THEN table_bytes::NUMERIC
                ELSE NULL::NUMERIC END
                AS table_bytes,
            CASE WHEN expected_bytes > 0 
                THEN expected_bytes::NUMERIC
                ELSE NULL::NUMERIC END
                    AS expected_bytes,
            CASE WHEN expected_bytes > 0 AND table_bytes > 0
                AND expected_bytes <= table_bytes
                THEN (table_bytes - expected_bytes)::NUMERIC
                ELSE 0::NUMERIC END AS bloat_bytes
    FROM estimates_with_toast
    UNION ALL
    SELECT current_database() as databasename, 
        table_schema, table_name, FALSE, 
        est_rows, table_size,
        NULL::NUMERIC, NULL::NUMERIC
    FROM no_stats
),
bloat_data AS (
    -- do final math calculations and formatting
    select current_database() as databasename,
        schemaname, tablename, can_estimate, 
        table_bytes, round(table_bytes/(1024^2)::NUMERIC,3) as table_mb,
        expected_bytes, round(expected_bytes/(1024^2)::NUMERIC,3) as expected_mb,
        round(bloat_bytes*100/table_bytes) as pct_bloat,
        round(bloat_bytes/(1024::NUMERIC^2),2) as mb_bloat,
        table_bytes, expected_bytes, est_rows
    FROM table_estimates_plus
)
-- filter output for bloated tables
SELECT databasename, schemaname, tablename,
    can_estimate,
    est_rows,
    pct_bloat, mb_bloat,
    table_mb
FROM bloat_data
-- this where clause defines which tables actually appear
-- in the bloat chart
-- example below filters for tables which are either 50%
-- bloated and more than 20mb in size, or more than 25%
-- bloated and more than 1GB in size
WHERE ( pct_bloat >= 50 AND mb_bloat >= 20 )
    OR ( pct_bloat >= 25 AND mb_bloat >= 1000 )
    OR can_estimate -- I added this line to show all tables
ORDER BY pct_bloat DESC
);


-- this view lists all currently running vacuum processes,
-- whether autovacuum or manual
CREATE VIEW health_vacuum AS (
SELECT   pid, 
         Age(query_start, Clock_timestamp()), 
         usename, 
         query 
FROM     pg_stat_activity 
WHERE    query != '<IDLE>' 
AND      query ilike '%vacuum%' 
ORDER BY query_start ASC
);

--------------------------------------------------------------------------------
-- create and configure citus
--------------------------------------------------------------------------------
CREATE EXTENSION citus;

CREATE VIEW citus_diskusage_node AS (
    SELECT
        'coordinator' AS node,
        pg_size_pretty(sum(pg_catalog.pg_database_size(datname))) AS size
    FROM pg_catalog.pg_database
    UNION ALL
    (
    SELECT
        pg_dist_node.nodename,
        pg_size_pretty(sum(shard_size))
    FROM citus_shards
    JOIN pg_dist_placement USING (shardid)
    JOIN pg_dist_node USING (groupid)
    GROUP BY pg_dist_node.nodename
    ORDER BY pg_dist_node.nodename
    )
);

CREATE VIEW citus_diskusage_relation AS (
    SELECT
        logicalrelid AS name,
        pg_size_pretty(citus_relation_size(logicalrelid)) AS relation_size,
        pg_size_pretty(citus_table_size(logicalrelid)) AS table_size,
        pg_size_pretty(citus_total_relation_size(logicalrelid) - citus_table_size(logicalrelid)) AS indexes_size,
        pg_size_pretty(citus_total_relation_size(logicalrelid)) AS total_size
    FROM pg_dist_partition
    ORDER BY name
);


--------------------------------------------------------------------------------
-- configure other extensions
--------------------------------------------------------------------------------

-- this db doesn't directly use python,
-- but the chajda and pgrollup extensions do
CREATE LANGUAGE plpython3u;

-- extensions for improved indexing
CREATE EXTENSION rum;
CREATE EXTENSION chajda;
CREATE EXTENSION vector;

CREATE SCHEMA partman;
CREATE EXTENSION pg_partman WITH SCHEMA partman;

-- extensions used by pgrollup
CREATE EXTENSION hll;
CREATE EXTENSION tdigest;
CREATE EXTENSION datasketches;
CREATE EXTENSION topn;

-- configure pg_cron
CREATE EXTENSION pg_cron;
CREATE VIEW cron_recent_status AS (
    with alljobs as (
        select command,start_time,end_time,status,split_part(return_message, E'\n', 1) as return_message from cron.job_run_details
    )
    select alljobs.* from (select command,max(start_time) last_success from alljobs where status='succeeded' group by command) t
    right outer join alljobs using (command)
    where last_success < start_time order by command,start_time
);
CREATE VIEW cron_running AS (
    select command,start_time,now()-start_time as duration from cron.job_run_details where status='running' order by command
);

-- configure pgrollup for minimal overhead rollup tables
CREATE EXTENSION pgrollup;
UPDATE pgrollup_settings SET value='cron' WHERE name='default_mode';
UPDATE pgrollup_settings SET value='10000' WHERE name='cron_block_size';
CREATE EVENT TRIGGER pgrollup_from_matview_trigger ON ddl_command_end WHEN TAG IN ('CREATE MATERIALIZED VIEW') EXECUTE PROCEDURE pgrollup_from_matview_event();

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
DECLARE
    host_no_dot TEXT = COALESCE(
        SUBSTRING(host, '^(.*)\.$'),
        host
    );
BEGIN
    RETURN COALESCE(
        SUBSTRING(host_no_dot, '^www\d*\.(.*)'),
        SUBSTRING(host_no_dot, '^m\.(.*)'),
        host_no_dot
    );
END 
$$;

do $$
BEGIN
    assert( host_simplify('cnn.com') = 'cnn.com');
    assert( host_simplify('cnn.com.') = 'cnn.com');
    assert( host_simplify('www.cnn.com') = 'cnn.com');
    assert( host_simplify('www2.cnn.com') = 'cnn.com');
    assert( host_simplify('www5.cnn.com') = 'cnn.com');
    assert( host_simplify('www577.cnn.com') = 'cnn.com');
    assert( host_simplify('www577.cnn.com.') = 'cnn.com');
    assert( host_simplify('bbc.co.uk') = 'bbc.co.uk');
    assert( host_simplify('bbc.co.uk.') = 'bbc.co.uk');
    assert( host_simplify('www.bbc.co.uk') = 'bbc.co.uk');
    assert( host_simplify('www.bbc.co.uk.') = 'bbc.co.uk');
    assert( host_simplify('en.wikipedia.org') = 'en.wikipedia.org');
    assert( host_simplify('m.wikipedia.org') = 'wikipedia.org');
    assert( host_simplify('m.wikipedia.org.') = 'wikipedia.org');
    assert( host_simplify('naenara.com.kp') = 'naenara.com.kp');
    assert( host_simplify('naenara.com.kp.') = 'naenara.com.kp');
END;
$$ LANGUAGE plpgsql;

/*
 * converts a host into the SURT syntax used by the common crawl
 * the main feature is that subdomains are in reverse order,
 * so string matches starting from the left hand side become increasingly specific
 */
CREATE OR REPLACE FUNCTION host_surt(host TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    RETURN array_to_string(array_reverse(string_to_array(host,'.')),',')||')';
END 
$$;

do $$
BEGIN
    assert( host_surt('cnn.com') = 'com,cnn)');
    assert( host_surt('www.cnn.com') = 'com,cnn,www)');
    assert( host_surt('www.bbc.co.uk') = 'uk,co,bbc,www)');
END;
$$ LANGUAGE plpgsql;

/*
 * converts from the host_surt syntax into the standard host syntax;
 */
CREATE OR REPLACE FUNCTION host_unsurt(host TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    RETURN array_to_string(array_reverse(string_to_array(substring(host from 0 for char_length(host)),',')),'.');
END 
$$;

do $$
BEGIN
    assert( host_unsurt(host_surt('cnn.com')) = 'cnn.com');
    assert( host_unsurt(host_surt('www.cnn.com')) = 'www.cnn.com');
    assert( host_unsurt(host_surt('www.bbc.co.uk')) = 'www.bbc.co.uk');
END;
$$ LANGUAGE plpgsql;

/*
 * converts a surt url (whether host/hostpath/hostpathquery) into a standard url without schema
 */
CREATE OR REPLACE FUNCTION unsurt(surt TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    host_surt TEXT = split_part(surt,')',1) || ')';
    pathquery TEXT = split_part(surt,')',2);
BEGIN
    RETURN host_unsurt(host_surt) || pathquery;
END 
$$;

do $$
BEGIN
    assert( unsurt(host_surt('cnn.com')) = 'cnn.com');
    assert( unsurt(host_surt('www.cnn.com')) = 'www.cnn.com');
    assert( unsurt(host_surt('www.bbc.co.uk')) = 'www.bbc.co.uk');
    assert( unsurt('com,example,subdomain)/a/b/c/d?test=1&example=2') = 'subdomain.example.com/a/b/c/d?test=1&example=2' );
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

CREATE OR REPLACE FUNCTION url_host_surt(url TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    url_lower TEXT = lower(url);
BEGIN
    RETURN btree_sanitize(host_surt(host_simplify(url_host(url_lower))));
END 
$$;

do $$
BEGIN
    assert( url_host_surt('https://example.com') = 'com,example)');
    assert( url_host_surt('https://example.com/') = 'com,example)');
    assert( url_host_surt('https://example.com/#test') = 'com,example)');
    assert( url_host_surt('https://example.com/?param=12') = 'com,example)');
    assert( url_host_surt('https://example.com/path/to') = 'com,example)');
    assert( url_host_surt('https://example.com/path/to/') = 'com,example)');
    assert( url_host_surt('https://example.com/path/to/#test') = 'com,example)');
    assert( url_host_surt('https://example.com/path/to/?param=12') = 'com,example)');
    assert( url_host_surt('https://Example.com/Path/To/?Param=12') = 'com,example)');
    assert( url_host_surt('https://Example.com./Path/To/?Param=12') = 'com,example)');
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION url_hostpath_surt(url TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    url_lower TEXT = lower(url);
BEGIN
    RETURN btree_sanitize(host_surt(host_simplify(url_host(url_lower))) || path_simplify(url_path(url_lower)));
END 
$$;

do $$
BEGIN
    assert( url_hostpath_surt('https://example.com') = 'com,example)');
    assert( url_hostpath_surt('https://example.com/') = 'com,example)');
    assert( url_hostpath_surt('https://example.com/#test') = 'com,example)');
    assert( url_hostpath_surt('https://example.com/?param=12') = 'com,example)');
    assert( url_hostpath_surt('https://example.com/path/to') = 'com,example)/path/to');
    assert( url_hostpath_surt('https://example.com/path/to/') = 'com,example)/path/to');
    assert( url_hostpath_surt('https://example.com/path/to/#test') = 'com,example)/path/to');
    assert( url_hostpath_surt('https://example.com/path/to/?param=12') = 'com,example)/path/to');
    assert( url_hostpath_surt('https://Example.com/Path/To/?Param=12') = 'com,example)/path/to');
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION url_hostpathquery_surt(url TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    url_lower TEXT = lower(url);
    query TEXT = query_simplify(url_query(url_lower));
BEGIN
    RETURN btree_sanitize(
        host_surt(host_simplify(url_host(url_lower))) || 
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
    assert( url_hostpathquery_surt('https://example.com') = 'com,example)');
    assert( url_hostpathquery_surt('https://example.com/') = 'com,example)');
    assert( url_hostpathquery_surt('https://example.com/#test') = 'com,example)');
    assert( url_hostpathquery_surt('https://example.com/?param=12') = 'com,example)?param=12');
    assert( url_hostpathquery_surt('https://example.com/path/to') = 'com,example)/path/to');
    assert( url_hostpathquery_surt('https://example.com/path/to/') = 'com,example)/path/to');
    assert( url_hostpathquery_surt('https://example.com/path/to/#test') = 'com,example)/path/to');
    assert( url_hostpathquery_surt('https://example.com/path/to/?param=12') = 'com,example)/path/to?param=12');
    assert( url_hostpathquery_surt('https://Example.com/Path/To/?Param=12') = 'com,example)/path/to?param=12');
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
        hll_count(url_hostpathquery_surt(jsonb->>'url')) AS hostpathquery,
        hll_count(url_hostpath_surt(jsonb->>'url')) AS hostpath,
        hll_count(url_host_surt(jsonb->>'url')) AS host
    FROM metahtml_test
);

CREATE MATERIALIZED VIEW metahtml_test_summary_host AS (
    SELECT
        url_host_surt(jsonb->>'url') AS host,
        hll_count(jsonb->>'url') AS url,
        hll_count(url_hostpathquery_surt(jsonb->>'url')) AS hostpathquery,
        hll_count(url_hostpath_surt(jsonb->>'url')) AS hostpath
    FROM metahtml_test
    GROUP BY host
);

CREATE MATERIALIZED VIEW metahtml_test_summary_language AS (
    SELECT
        language_iso639(jsonb->>'language'),
        hll_count(jsonb->>'url') AS url,
        hll_count(url_hostpathquery_surt(jsonb->>'url')) AS hostpathquery,
        hll_count(url_hostpath_surt(jsonb->>'url')) AS hostpath,
        hll_count(url_host_surt(jsonb->>'url')) AS host
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
CREATE INDEX top_1m_alexa_idx ON top_1m_alexa(url_host_surt(host),rank);

CREATE TABLE top_1m_opendns (
    rank INTEGER,
    host TEXT
);
COPY top_1m_opendns FROM '/tmp/data/top-1m-opendns/top-1m.csv' DELIMITER ',' CSV HEADER;
CREATE INDEX top_1m_opendns_idx ON top_1m_opendns(url_host_surt(host),rank);

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
    SELECT url_host_surt(hostname)
    FROM hostnames
    WHERE
        COALESCE(priority,'') != 'ban' AND
        url_host_surt(hostnames.hostname) NOT IN (
            SELECT DISTINCT url_host_surt(jsonb->>'url')
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
        url_host_surt(url) AS host_surt
    FROM allsides
    WHERE
        url_host_surt(url) NOT IN (
            SELECT DISTINCT url_host_surt(jsonb->>'url')
            FROM metahtml_test
        )
    ORDER BY host_surt
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
        url_host_surt(url) AS host_surt
    FROM mediabiasfactcheck
    WHERE
        url_host_surt(url) NOT IN (
            SELECT DISTINCT url_host_surt(jsonb->>'url')
            FROM metahtml_test
        )
    ORDER BY host_surt
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
        url_host_surt(url) AS host_surt
    FROM quantifyingnewsmediabias
    WHERE
        url_host_surt(url) NOT IN (
            SELECT DISTINCT url_host_surt(jsonb->>'url')
            FROM metahtml_test
        )
    ORDER BY host_surt
    );

CREATE VIEW qualitifyingnewsmediabias_summary AS (
    SELECT
        hll_count(url) AS url,
        hll_count(url_hostpathquery_surt(url)) AS hostpathquery,
        hll_count(url_hostpath_surt(url)) AS hostpath,
        hll_count(url_host_surt(url)) AS host
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
    urls_inserted INTEGER NOT NULL DEFAULT 0,
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
    jsonb JSONB NOT NULL
);
CREATE INDEX ON metahtml (url_hostpath_surt(url), accessed_at);

CREATE TABLE metahtml_view (
    id BIGSERIAL NOT NULL,
    timestamp_published TIMESTAMPTZ NOT NULL,
    host_surt TEXT NOT NULL CHECK (length(host_surt) < 2000), -- FIXME: this check is needed in order to create a btree index on this column
    hostpath_surt TEXT NOT NULL CHECK (length(hostpath_surt) < 2000), -- FIXME: CHECK (hostpath_surt = url_hostpath_surt(hostpath_surt)),
    language TEXT NOT NULL CHECK (language = language_iso639(language)), --FIXME: we need to standardize language names and change function
    title TEXT,
    description TEXT,
    content TEXT,
    tsv_title tsvector NOT NULL,
    tsv_content tsvector NOT NULL,
    links TEXT[],
    PRIMARY KEY (host_surt, id)
);
CREATE UNIQUE INDEX ON metahtml_view (host_surt, hostpath_surt);
--FIXME: add? CREATE UNIQUE INDEX ON metahtml_view (host_surt, date(timestamp_published), title);
CREATE INDEX ON metahtml_view USING rum(tsv_content);
CREATE INDEX ON metahtml_view USING rum(tsv_content RUM_TSVECTOR_ADDON_OPS, timestamp_published)
  WITH (ATTACH='timestamp_published', TO='tsv_content');

-- FIXME: this view should be materialized, but pgrollup needs to support outer joins
CREATE VIEW metahtml_view_conversion AS (
    SELECT *, round(coalesce(count_view,0)/count_metahtml::numeric,4) as conversion_rate 
    FROM (
        SELECT
            url_host_surt(url) as host_surt,
            count(*) as count_metahtml
        FROM metahtml
        GROUP BY host_surt
    )t1 
    FULL OUTER JOIN (
        SELECT
            host_surt,
            count(*) as count_view
        FROM metahtml_view
        GROUP BY host_surt
    )t2
    USING (host_surt)
    ORDER BY host_surt
);

SELECT * FROM pgrollup_rollups;

-- FIXME: this is just for testing
CREATE MATERIALIZED VIEW metahtml_view_count AS (
    SELECT
        count(1) AS count
    FROM metahtml_view
);
CREATE MATERIALIZED VIEW metahtml_count AS (
    SELECT
        count(1) AS count
    FROM metahtml
);

--------------------------------------------------------------------------------
-- rollups for tracking debug info

CREATE MATERIALIZED VIEW metahtml_versions AS (
    SELECT 
        jsonb->>'version' AS version,
        hll_count(url) as url,
        hll_count(url_hostpathquery_surt(url)) AS hostpathquery,
        hll_count(url_hostpath_surt(url)) AS hostpath,
        hll_count(url_host_surt(url)) AS host
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
        hll_count(url_hostpathquery_surt(url)) AS hostpathquery,
        hll_count(url_hostpath_surt(url)) AS hostpath
    FROM metahtml
    GROUP BY host,type,location
);

CREATE MATERIALIZED VIEW metahtml_insert AS (
    SELECT
        date_trunc('hour', inserted_at) AS insert_hour,
        hll_count(id_source),
        hll_count(url) AS url,
        hll_count(url_hostpathquery_surt(url)) AS hostpathquery,
        hll_count(url_hostpath_surt(url)) AS hostpath,
        hll_count(url_host_surt(url)) AS host
    FROM metahtml
    GROUP BY insert_hour
);

--------------------------------------------------------------------------------
-- rollups for pagerank

-- FIXME: we should hll_count the dest urls, but it doesn't work with set functions
CREATE MATERIALIZED VIEW metahtml_linksall_host AS (
    SELECT
        url_host_surt(url) AS src,
        url_host_surt(jsonb_array_elements(jsonb->'links.all'->'best'->'value')->>'href') AS dest,
        hll_count(url) AS src_url,
        hll_count(url_hostpathquery_surt(url)) AS src_hostpathquery,
        hll_count(url_hostpath_surt(url)) AS src_hostpath
    FROM metahtml
    GROUP BY src,dest
);

CREATE VIEW metahtml_linksall_summary AS (
    WITH results AS (
        SELECT
            src,
            dest,
            src_hostpath
        FROM metahtml_linksall_host
        WHERE dest IN (SELECT DISTINCT src FROM metahtml_linksall_host)
    )
    SELECT *,round((src_hostpath/src_total)::numeric,4) as fraction
    FROM results
    JOIN (
        SELECT src,src_hostpath as src_total
        FROM results
        WHERE src=dest
    )t USING (src)
    ORDER BY src,dest
);

SELECT dest,count(*) as src_host,sum(src_hostpath) as src_hostpath from metahtml_linksall_host group by dest order by src_host desc;

-- FIXME:
-- we should index this by words as well,
-- but we need the unnest to act as a cross product instead of in parallel
CREATE MATERIALIZED VIEW linksall_host AS (
    SELECT
        host_surt as src,
        url_host_surt(unnest(links)) AS dest,
        hll_count(hostpath_surt) AS src_hostpath
    FROM metahtml_view
    GROUP BY src,dest
);

CREATE VIEW linksall_summary AS (
    WITH results AS (
        SELECT
            src,
            dest,
            src_hostpath
        FROM linksall_host
        WHERE dest IN (SELECT DISTINCT src FROM linksall_host)
    )
    SELECT *,round((src_hostpath/src_total)::numeric,4) as fraction
    FROM results
    JOIN (
        SELECT src,src_hostpath as src_total
        FROM results
        WHERE src=dest
    )t USING (src)
    ORDER BY src,dest
);

/* FIXME: materialize this
SELECT
    dest,
    sum(src_url             /COALESCE(rank,1000000)) as src_url,
    sum(src_hostpathquery   /COALESCE(rank,1000000)) as src_hostpathquery,
    sum(src_hostpath        /COALESCE(rank,1000000)) as src_hostpath,
    sum(1                   /COALESCE(rank,1000000)) as src_host
FROM metahtml_linksall_host
LEFT JOIN top_1m_alexa ON (url_host_surt(metahtml_linksall_host.src) = url_host_surt(top_1m_alexa.host))
GROUP BY dest
ORDER BY src_host DESC;
*/

--------------------------------------------------------------------------------
-- rollups for basic stats

CREATE MATERIALIZED VIEW metahtml_hoststats AS (
    SELECT
        url_host_surt(url) AS host,
        hll_count(jsonb->'language'->'best'->>'value') AS language,
        hll_count(id_source) AS id_source,
        hll_count(jsonb->'timestamp.published') AS timestamp_published,
        hll_count(jsonb->'timestamp.modified') AS timestamp_modified,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_surt(url)) AS hostpathquery,
        hll_count(url_hostpath_surt(url)) AS hostpath
    FROM metahtml
    GROUP BY host
);

CREATE MATERIALIZED VIEW metahtml_hoststats_lang AS (
    SELECT
        url_host_surt(url) AS host,
        (jsonb->'language'->'best'->>'value') AS language,
        hll_count(id_source) AS id_source,
        hll_count(jsonb->'timestamp.published') AS timestamp_published,
        hll_count(jsonb->'timestamp.modified') AS timestamp_modified,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_surt(url)) AS hostpathquery,
        hll_count(url_hostpath_surt(url)) AS hostpath
    FROM metahtml
    GROUP BY host,language
);

CREATE MATERIALIZED VIEW metahtml_hoststats_lang_filtered AS (
    SELECT
        url_host_surt(url) AS host,
        (jsonb->'language'->'best'->>'value') AS language,
        hll_count(id_source) AS id_source,
        hll_count(jsonb->'timestamp.published') AS timestamp_published,
        hll_count(jsonb->'timestamp.modified') AS timestamp_modified,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_surt(url)) AS hostpathquery,
        hll_count(url_hostpath_surt(url)) AS hostpath
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
    FROM metahtml_hoststats_lang
    GROUP BY language_iso639
);

CREATE VIEW hostnames_to_check AS (
    SELECT 
        host_unsurt(host) as host,
        language_iso639(language),
        hostpath^1.5/(1+timestamp_published) as score,
        timestamp_published,
        hostpath
    FROM metahtml_hoststats_lang_filtered
    WHERE host_unsurt(host) NOT IN (SELECT host FROM metahtml_test_summary_host)
    ORDER BY score DESC,host
);

CREATE VIEW hostnames_to_check_untested AS (
    SELECT
        rank,
        host_unsurt(untested.url_host_surt) as host
    FROM top_1m_alexa
    RIGHT JOIN
        (
        SELECT * FROM hostnames_untested
        UNION
        SELECT * FROM allsides_untested
        /*
        UNION
        SELECT * FROM mediabiasfactcheck_untested
        */
        ) untested ON url_host_surt(top_1m_alexa.host) = url_host_surt
    ORDER BY rank ASC
);

--------------------------------------------------------------------------------
-- wordcount
--------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW metahtml_rollup_textlang AS (
    SELECT
        unnest(tsvector_to_ngrams(tsv_title || tsv_content)) AS alltext,
        language, 
        count(1) AS "count(1)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        theta_sketch_distinct(host_surt) AS host_surt
    FROM metahtml_view
    GROUP BY alltext,language
);

CREATE MATERIALIZED VIEW metahtml_rollup_textlang_host AS (
    SELECT
        unnest(tsvector_to_ngrams(tsv_title || tsv_content)) AS alltext,
        language, 
        count(1) AS "count(1)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        host_surt
    FROM metahtml_view
    GROUP BY alltext,language,host_surt
);

--------------------

CREATE MATERIALIZED VIEW metahtml_rollup_year_theta AS (
    SELECT
        count(1) AS "count(1)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        theta_sketch_distinct(host_surt) AS host_surt,
        date_trunc('year',timestamp_published) AS timestamp_published_year
    FROM metahtml_view
    GROUP BY timestamp_published_year
);

CREATE MATERIALIZED VIEW metahtml_rollup_year_host_theta AS (
    SELECT
        count(1) AS "count(1)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        host_surt,
        date_trunc('year',timestamp_published) AS timestamp_published_year
    FROM metahtml_view
    GROUP BY timestamp_published_year,host_surt
);

CREATE MATERIALIZED VIEW metahtml_rollup_langyear_theta AS (
    SELECT
        language, 
        count(1) AS "count(1)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        theta_sketch_distinct(host_surt) AS host_surt,
        date_trunc('year',timestamp_published) AS timestamp_published_year
    FROM metahtml_view
    GROUP BY language,timestamp_published_year
);

CREATE MATERIALIZED VIEW metahtml_rollup_langyear_host_theta AS (
    SELECT
        language, 
        count(1) AS "count(1)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        host_surt,
        date_trunc('year',timestamp_published) AS timestamp_published_year
    FROM metahtml_view
    GROUP BY language,timestamp_published_year,host_surt
);

CREATE MATERIALIZED VIEW metahtml_rollup_textlangyear_theta AS (
    SELECT
        unnest(tsvector_to_ngrams(tsv_title || tsv_content)) AS alltext,
        language, 
        count(1) AS "count(1)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        theta_sketch_distinct(host_surt) AS host_surt,
        date_trunc('year',timestamp_published) AS timestamp_published_year
    FROM metahtml_view
    GROUP BY alltext,language,timestamp_published_year
);

CREATE MATERIALIZED VIEW metahtml_rollup_textlangyear_host_theta AS (
    SELECT
        unnest(tsvector_to_ngrams(tsv_title || tsv_content)) AS alltext,
        language, 
        count(1) AS "count(1)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        host_surt,
        date_trunc('year',timestamp_published) AS timestamp_published_year
    FROM metahtml_view
    GROUP BY alltext,language,timestamp_published_year,host_surt
);

--------------------

CREATE MATERIALIZED VIEW metahtml_rollup_month_theta AS (
    SELECT
        count(1) AS "count(1)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        theta_sketch_distinct(host_surt) AS host_surt,
        date_trunc('month',timestamp_published) AS timestamp_published_month
    FROM metahtml_view
    GROUP BY timestamp_published_month
);

CREATE MATERIALIZED VIEW metahtml_rollup_month_host_theta AS (
    SELECT
        count(1) AS "count(1)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        host_surt,
        date_trunc('month',timestamp_published) AS timestamp_published_month
    FROM metahtml_view
    GROUP BY timestamp_published_month,host_surt
);

CREATE MATERIALIZED VIEW metahtml_rollup_langmonth_theta AS (
    SELECT
        language, 
        count(1) AS "count(1)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        theta_sketch_distinct(host_surt) AS host_surt,
        date_trunc('month',timestamp_published) AS timestamp_published_month
    FROM metahtml_view
    GROUP BY language,timestamp_published_month
);

CREATE MATERIALIZED VIEW metahtml_rollup_langmonth_host_theta AS (
    SELECT
        language, 
        count(1) AS "count(1)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        host_surt,
        date_trunc('month',timestamp_published) AS timestamp_published_month
    FROM metahtml_view
    GROUP BY language,timestamp_published_month,host_surt
);

CREATE MATERIALIZED VIEW metahtml_rollup_textlangmonth_theta AS (
    SELECT
        unnest(tsvector_to_ngrams(tsv_title || tsv_content)) AS alltext,
        language, 
        count(1) AS "count(1)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        theta_sketch_distinct(host_surt) AS host_surt,
        date_trunc('month',timestamp_published) AS timestamp_published_month
    FROM metahtml_view
    GROUP BY alltext,language,timestamp_published_month
);

CREATE MATERIALIZED VIEW metahtml_rollup_textlangmonth_host_theta AS (
    SELECT
        unnest(tsvector_to_ngrams(tsv_title || tsv_content)) AS alltext,
        language, 
        count(1) AS "count(1)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        host_surt,
        date_trunc('month',timestamp_published) AS timestamp_published_month
    FROM metahtml_view
    GROUP BY alltext,language,timestamp_published_month,host_surt
);

----------

CREATE MATERIALIZED VIEW metahtml_rollup_day_theta AS (
    SELECT
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        theta_sketch_distinct(host_surt) AS host_surt,
        date_trunc('day',timestamp_published) AS timestamp_published_day
    FROM metahtml_view
    GROUP BY timestamp_published_day
);

CREATE MATERIALIZED VIEW metahtml_rollup_day_host_theta AS (
    SELECT
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        host_surt,
        date_trunc('day',timestamp_published) AS timestamp_published_day
    FROM metahtml_view
    GROUP BY timestamp_published_day,host_surt
);

CREATE MATERIALIZED VIEW metahtml_rollup_langday_theta AS (
    SELECT
        language, 
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        theta_sketch_distinct(host_surt) AS host_surt,
        date_trunc('day',timestamp_published) AS timestamp_published_day
    FROM metahtml_view
    GROUP BY language,timestamp_published_day
);

CREATE MATERIALIZED VIEW metahtml_rollup_langday_host_theta AS (
    SELECT
        language, 
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        host_surt,
        date_trunc('day',timestamp_published) AS timestamp_published_day
    FROM metahtml_view
    GROUP BY language,timestamp_published_day,host_surt
);

CREATE MATERIALIZED VIEW metahtml_rollup_textlangday_theta AS (
    SELECT
        unnest(tsvector_to_ngrams(tsv_title || tsv_content)) AS alltext,
        language, 
        count(1) AS "count(1)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        theta_sketch_distinct(host_surt) AS host_surt,
        date_trunc('day',timestamp_published) AS timestamp_published_day
    FROM metahtml_view
    GROUP BY alltext,language,timestamp_published_day
);

CREATE MATERIALIZED VIEW metahtml_rollup_textlangday_host_theta AS (
    SELECT
        unnest(tsvector_to_ngrams(tsv_title || tsv_content)) AS alltext,
        language, 
        count(1) AS "count(1)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        host_surt,
        date_trunc('day',timestamp_published) AS timestamp_published_day
    FROM metahtml_view
    GROUP BY alltext,language,timestamp_published_day,host_surt
);

--------------------------------------------------------------------------------
-- contextwords
--------------------------------------------------------------------------------

/*
CREATE TABLE contextwords (
    id BIGSERIAL NOT NULL,
    timestamp_published TIMESTAMPTZ NOT NULL,
    count bigint,  -- FIXME: we could use smallint here and save some space, we just need to cast the rollups to int/bigint to avoid overflow
    words TEXT[],
    host_surt TEXT,
    hostpath_surt TEXT,
    focus TEXT,
	language TEXT,
    PRIMARY KEY (host_surt, id, hostpath_surt, focus)
);
*/

--------------------------------------------------------------------------------
-- wordcontext
--------------------------------------------------------------------------------

CREATE TABLE wordcontext (
    id BIGSERIAL NOT NULL,
    timestamp_published TIMESTAMPTZ NOT NULL,
    host_surt TEXT,
    hostpath_surt TEXT,
    focus TEXT,
    context TEXT[],
	language TEXT,
    PRIMARY KEY (host_surt, id, hostpath_surt, focus)
);

/*
CREATE MATERIALIZED VIEW wordcontext_focusmonthlang AS (
    SELECT 
        unnest(context) AS context,
        count(1) AS "count(1)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        theta_sketch_distinct(host_surt) AS host_surt,
        date_trunc('month',timestamp_published) AS timestamp_published_month,
        focus,
        language
    FROM wordcontext
    GROUP BY timestamp_published_month,focus,context,language
);

CREATE MATERIALIZED VIEW wordcontext_focusmonthlang_host AS (
    SELECT 
        unnest(context) AS context,
        count(1) AS "count(1)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        host_surt,
        date_trunc('month',timestamp_published) AS timestamp_published_month,
        focus,
        language
    FROM wordcontext
    GROUP BY timestamp_published_month,host_surt,focus,context,language
);
*/

/*
CREATE MATERIALIZED VIEW wordcontext AS (
    SELECT
        (tsvector_to_wordcontext(tsv_title || tsv_content)).focus as focus,
        (tsvector_to_wordcontext(tsv_title || tsv_content)).context as context,
        --sum((tsvector_to_wordcontext(tsv_title || tsv_content)).count) as count,
        hostpath_surt,
        host_surt,
        timestamp_published,
        language
        --count(1) as count
    FROM (select * from metahtml_view limit 1)t
    --GROUP BY 1,2,3,4,5,6
);
*/

--------------------------------------------------------------------------------
-- contextvector
--------------------------------------------------------------------------------

UPDATE pgrollup_settings SET value='1000000' WHERE name='cron_block_size';

CREATE TABLE contextvector (
    id BIGSERIAL NOT NULL,
    context vector(25),
    timestamp_published TIMESTAMPTZ NOT NULL,
    count bigint,  -- FIXME: we could use smallint here and save some space, we just need to cast the rollups to int/bigint to avoid overflow
    host_surt TEXT,
    hostpath_surt TEXT,
    focus TEXT,
	language TEXT,
    PRIMARY KEY (host_surt, id, hostpath_surt, focus)
);
CREATE INDEX ON contextvector USING ivfflat (context vector_ip_ops) WITH (lists=100);

CREATE MATERIALIZED VIEW contextvector_focuslang_host AS (
    SELECT 
        vector_sum(context) as "sum(context)",
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        focus,
        language,
        host_surt
    FROM contextvector
    GROUP BY host_surt,focus,language
);

-----

CREATE MATERIALIZED VIEW contextvector_yearlang AS (
    SELECT 
        vector_sum(context) as "sum(context)",
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        language,
        date_trunc('year',timestamp_published) AS timestamp_published_year
    FROM contextvector
    GROUP BY timestamp_published_year,language
);

CREATE MATERIALIZED VIEW contextvector_year AS (
    SELECT 
        vector_sum(context) as "sum(context)",
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        date_trunc('year',timestamp_published) AS timestamp_published_year
    FROM contextvector
    GROUP BY timestamp_published_year
);

CREATE MATERIALIZED VIEW contextvector_yearlang_host AS (
    SELECT 
        vector_sum(context) as "sum(context)", -- FIXME: storing lots of redundant information here
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        language,
        host_surt,
        date_trunc('year',timestamp_published) AS timestamp_published_year
    FROM contextvector
    GROUP BY timestamp_published_year,host_surt,language
);

CREATE MATERIALIZED VIEW contextvector_year_host AS (
    SELECT 
        vector_sum(context) as "sum(context)", -- FIXME: storing lots of redundant information here
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        host_surt,
        date_trunc('year',timestamp_published) AS timestamp_published_year
    FROM contextvector
    GROUP BY timestamp_published_year,host_surt
);

CREATE MATERIALIZED VIEW contextvector_focusyearlang AS (
    SELECT 
        vector_sum(context) as "sum(context)",
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        focus,
        language,
        date_trunc('year',timestamp_published) AS timestamp_published_year
    FROM contextvector
    GROUP BY timestamp_published_year,focus,language
);

CREATE MATERIALIZED VIEW contextvector_focusyearlang_host AS (
    SELECT 
        vector_sum(context) as "sum(context)",
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        focus,
        language,
        host_surt,
        date_trunc('year',timestamp_published) AS timestamp_published_year
    FROM contextvector
    GROUP BY timestamp_published_year,host_surt,focus,language
);

-----

CREATE MATERIALIZED VIEW contextvector_monthlang AS (
    SELECT 
        vector_sum(context) as "sum(context)",
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        language,
        date_trunc('month',timestamp_published) AS timestamp_published_month
    FROM contextvector
    GROUP BY timestamp_published_month,language
);

CREATE MATERIALIZED VIEW contextvector_month AS (
    SELECT 
        vector_sum(context) as "sum(context)",
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        date_trunc('month',timestamp_published) AS timestamp_published_month
    FROM contextvector
    GROUP BY timestamp_published_month
);

CREATE MATERIALIZED VIEW contextvector_monthlang_host AS (
    SELECT 
        vector_sum(context) as "sum(context)", -- FIXME: storing lots of redundant information here
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        language,
        host_surt,
        date_trunc('month',timestamp_published) AS timestamp_published_month
    FROM contextvector
    GROUP BY timestamp_published_month,host_surt,language
);

CREATE MATERIALIZED VIEW contextvector_month_host AS (
    SELECT 
        vector_sum(context) as "sum(context)", -- FIXME: storing lots of redundant information here
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        host_surt,
        date_trunc('month',timestamp_published) AS timestamp_published_month
    FROM contextvector
    GROUP BY timestamp_published_month,host_surt
);

CREATE MATERIALIZED VIEW contextvector_focusmonthlang AS (
    SELECT 
        vector_sum(context) as "sum(context)",
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        focus,
        language,
        date_trunc('month',timestamp_published) AS timestamp_published_month
    FROM contextvector
    GROUP BY timestamp_published_month,focus,language
);

CREATE MATERIALIZED VIEW contextvector_focusmonthlang_host AS (
    SELECT 
        vector_sum(context) as "sum(context)",
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        focus,
        language,
        host_surt,
        date_trunc('month',timestamp_published) AS timestamp_published_month
    FROM contextvector
    GROUP BY timestamp_published_month,host_surt,focus,language
);

-----

DROP EVENT TRIGGER pgrollup_from_matview_trigger;
CREATE MATERIALIZED VIEW contextvector_daylang AS (
    SELECT 
        vector_sum(context) as "sum(context)",
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        language,
        date_trunc('day',timestamp_published) AS timestamp_published_day
    FROM contextvector
    GROUP BY timestamp_published_day,language
);
SELECT pgrollup_from_matview('contextvector_daylang', partition_method=>'range', partition_keys=>ARRAY['timestamp_published_day']);
SELECT partman.create_parent('public.contextvector_daylang_raw', 'timestamp_published_day', 'native', 'daily');

CREATE MATERIALIZED VIEW contextvector_day AS (
    SELECT 
        vector_sum(context) as "sum(context)",
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        date_trunc('day',timestamp_published) AS timestamp_published_day
    FROM contextvector
    GROUP BY timestamp_published_day
);
SELECT pgrollup_from_matview('contextvector_day', partition_method=>'range', partition_keys=>ARRAY['timestamp_published_day']);
SELECT partman.create_parent('public.contextvector_day_raw', 'timestamp_published_day', 'native', 'daily');

CREATE MATERIALIZED VIEW contextvector_daylang_host AS (
    SELECT 
        vector_sum(context) as "sum(context)",
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        language,
        host_surt,
        date_trunc('day',timestamp_published) AS timestamp_published_day
    FROM contextvector
    GROUP BY timestamp_published_day,host_surt,language
);
SELECT pgrollup_from_matview('contextvector_daylang_host', partition_method=>'range', partition_keys=>ARRAY['timestamp_published_day']);
SELECT partman.create_parent('public.contextvector_daylang_host_raw', 'timestamp_published_day', 'native', 'daily');

CREATE MATERIALIZED VIEW contextvector_day_host AS (
    SELECT 
        vector_sum(context) as "sum(context)",
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        host_surt,
        date_trunc('day',timestamp_published) AS timestamp_published_day
    FROM contextvector
    GROUP BY timestamp_published_day,host_surt
);
SELECT pgrollup_from_matview('contextvector_day_host', partition_method=>'range', partition_keys=>ARRAY['timestamp_published_day']);
SELECT partman.create_parent('public.contextvector_day_host_raw', 'timestamp_published_day', 'native', 'daily');

CREATE MATERIALIZED VIEW contextvector_focusdaylang AS (
    SELECT 
        vector_sum(context) as "sum(context)",
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        focus,
        language,
        date_trunc('day',timestamp_published) AS timestamp_published_day
    FROM contextvector
    GROUP BY timestamp_published_day,focus,language
);
SELECT pgrollup_from_matview('contextvector_focusdaylang', partition_method=>'range', partition_keys=>ARRAY['timestamp_published_day']);
SELECT partman.create_parent('public.contextvector_focusdaylang_raw', 'timestamp_published_day', 'native', 'daily');

CREATE MATERIALIZED VIEW contextvector_focusdaylang_host AS (
    SELECT 
        vector_sum(context) as "sum(context)",
        sum(count) as "sum(count)",
        theta_sketch_distinct(hostpath_surt) AS hostpath_surt,
        vector_sum(context)/sum(count) as "avg(context)",
        focus,
        language,
        host_surt,
        date_trunc('day',timestamp_published) AS timestamp_published_day
    FROM contextvector
    GROUP BY timestamp_published_day,host_surt,focus,language
);
SELECT pgrollup_from_matview('contextvector_focusdaylang_host', partition_method=>'range', partition_keys=>ARRAY['timestamp_published_day']);
SELECT partman.create_parent('public.contextvector_focusdaylang_host_raw', 'timestamp_published_day', 'native', 'daily');

--------------------------------------------------------------------------------
-- update configuration options
--------------------------------------------------------------------------------
UPDATE partman.part_config SET infinite_time_partitions = true;
UPDATE pgrollup_settings SET value='10000' WHERE name='cron_block_size';
CREATE EVENT TRIGGER pgrollup_from_matview_trigger ON ddl_command_end WHEN TAG IN ('CREATE MATERIALIZED VIEW') EXECUTE PROCEDURE pgrollup_from_matview_event();

--------------------------------------------------------------------------------
-- distribute tables with citus
--------------------------------------------------------------------------------
SELECT create_distributed_table('metahtml_view', 'host_surt');
SELECT create_distributed_table('metahtml_rollup_year_host_theta_raw', 'metahtml_view.host_surt', colocate_with=>'metahtml_view');
SELECT create_distributed_table('metahtml_rollup_textlang_host_raw', 'metahtml_view.host_surt', colocate_with=>'metahtml_view');
SELECT create_distributed_table('metahtml_rollup_langyear_host_theta_raw', 'metahtml_view.host_surt', colocate_with=>'metahtml_view');
SELECT create_distributed_table('metahtml_rollup_textlangyear_host_theta_raw', 'metahtml_view.host_surt', colocate_with=>'metahtml_view');
SELECT create_distributed_table('metahtml_rollup_month_host_theta_raw', 'metahtml_view.host_surt', colocate_with=>'metahtml_view');
SELECT create_distributed_table('metahtml_rollup_langmonth_host_theta_raw', 'metahtml_view.host_surt', colocate_with=>'metahtml_view');
SELECT create_distributed_table('metahtml_rollup_textlangmonth_host_theta_raw', 'metahtml_view.host_surt', colocate_with=>'metahtml_view');
SELECT create_distributed_table('metahtml_rollup_day_host_theta_raw', 'metahtml_view.host_surt', colocate_with=>'metahtml_view');
SELECT create_distributed_table('metahtml_rollup_langday_host_theta_raw', 'metahtml_view.host_surt', colocate_with=>'metahtml_view');
SELECT create_distributed_table('metahtml_rollup_textlangday_host_theta_raw', 'metahtml_view.host_surt', colocate_with=>'metahtml_view');

SELECT create_distributed_table('contextvector', 'host_surt', colocate_with=>'metahtml_view');
SELECT create_distributed_table('contextvector_focuslang_host_raw', 'contextvector.host_surt', colocate_with=>'contextvector');
SELECT create_distributed_table('contextvector_focusdaylang_host_raw', 'contextvector.host_surt', colocate_with=>'contextvector');
SELECT create_distributed_table('contextvector_daylang_host_raw', 'contextvector.host_surt', colocate_with=>'contextvector');
SELECT create_distributed_table('contextvector_day_host_raw', 'contextvector.host_surt', colocate_with=>'contextvector');
SELECT create_distributed_table('contextvector_focusmonthlang_host_raw', 'contextvector.host_surt', colocate_with=>'contextvector');
SELECT create_distributed_table('contextvector_monthlang_host_raw', 'contextvector.host_surt', colocate_with=>'contextvector');
SELECT create_distributed_table('contextvector_month_host_raw', 'contextvector.host_surt', colocate_with=>'contextvector');
SELECT create_distributed_table('contextvector_focusyearlang_host_raw', 'contextvector.host_surt', colocate_with=>'contextvector');
SELECT create_distributed_table('contextvector_yearlang_host_raw', 'contextvector.host_surt', colocate_with=>'contextvector');
SELECT create_distributed_table('contextvector_year_host_raw', 'contextvector.host_surt', colocate_with=>'contextvector');
COMMIT;
