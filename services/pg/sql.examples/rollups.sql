-- rollup tables for measuring links/pagerank

-- FIXME:
-- add an index for finding backlinks
--
--FIXME: these should be in the distincts
--jsonb_array_elements(jsonb->'links.all'->'best'->'value')->>'href' AS dest_url,
--url_hostpathquery_key(jsonb_array_elements(jsonb->'links.all'->'best'->'value')->>'href') AS dest_hostpathquery,
--url_hostpath_key(jsonb_array_elements(jsonb->'links.all'->'best'->'value')->>'href') AS dest_hostpath

-- EXEC SQL IFDEF ALLROLLUPS;
CREATE MATERIALIZED VIEW metahtml_linkscontent_host AS (
    SELECT
        url_host(url) AS src,
        url_host(jsonb_array_elements(jsonb->'links.content'->'best'->'value')->>'href') AS dest,
        hll_count(url) AS src_url,
        hll_count(url_hostpathquery_key(url)) AS src_hostpathquery,
        hll_count(url_hostpath_key(url)) AS src_hostpath
    FROM metahtml
    GROUP BY src,dest
);

/*
-- FIXME:
-- we should add filtering onto this so that we only record exact pagerank details for a small subset of links
CREATE MATERIALIZED VIEW metahtml_linksall_hostpath AS (
    SELECT
        url_hostpath_key(url) AS src,
        url_hostpath_key(jsonb_array_elements(jsonb->'links.all'->'best'->'value')->>'href') AS dest,
        count(*)
    FROM metahtml
);

CREATE MATERIALIZED VIEW metahtml_linkscontent_hostpath AS (
    SELECT
        url_hostpath_key(url) AS src,
        url_hostpath_key(jsonb_array_elements(jsonb->'links.content'->'best'->'value')->>'href') AS dest,
        count(*)
    FROM metahtml
);
*/

-- EXEC SQL ENDIF;

-- rollups for text

-- EXEC SQL IFDEF ALLROLLUPS;

CREATE MATERIALIZED VIEW metahtml_rollup_texthostmonth TABLESPACE fastdata AS (
    SELECT
        unnest(tsvector_to_array(title || content)) AS alltext,
        url_host(url) AS host,
        date_trunc('month',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY alltext,host,timestamp_published
);

CREATE MATERIALIZED VIEW metahtml_rollup_textmonth TABLESPACE fastdata AS (
    SELECT
        unnest(tsvector_to_array(title || content)) AS alltext,
        date_trunc('month',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY alltext,timestamp_published
);

-- EXEC SQL ENDIF;

-- other rollups

-- EXEC SQL IFDEF ALLROLLUPS;

CREATE MATERIALIZED VIEW metahtml_rollup_langhost AS (
    SELECT
        url_host(url) AS host,
        jsonb->'language'->'best'->>'value' AS language,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY host,language
);

CREATE MATERIALIZED VIEW metahtml_rollup_lang AS (
    SELECT
        jsonb->'language'->'best'->>'value' AS language,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY language
);

CREATE MATERIALIZED VIEW metahtml_rollup_source AS (
    SELECT
        id_source,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath,
        hll_count(url_host_key(url)) AS host
    FROM metahtml
    GROUP BY id_source
);

CREATE MATERIALIZED VIEW metahtml_rollup_type AS (
    SELECT
        jsonb->'type'->'best'->>'value' AS type,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY type
);

CREATE MATERIALIZED VIEW metahtml_rollup_hosttype AS (
    SELECT
        url_host(url) AS host,
        jsonb->'type'->'best'->>'value' AS type,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY host,type
);

CREATE MATERIALIZED VIEW metahtml_rollup_host AS (
    SELECT
        url_host(url) AS host,
        hll_count(id_source) AS id_source,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY host
);

CREATE MATERIALIZED VIEW metahtml_rollup_hostaccess AS (
    SELECT
        url_host(url) AS host_key,
        date_trunc('day', accessed_at) AS access_day,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY host_key,access_day
);

-- EXEC SQL ENDIF;

-- EXEC SQL IFDEF ALLROLLUPS;

CREATE MATERIALIZED VIEW metahtml_rollup_hostinsert AS (
    SELECT
        url_host(url) AS host_key,
        date_trunc('hour', inserted_at) AS insert_hour,
        hll_count(id_source),
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY host_key,insert_hour
);

CREATE MATERIALIZED VIEW metahtml_rollup_access AS (
    SELECT
        date_trunc('day', accessed_at) AS access_day,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY access_day
);

CREATE MATERIALIZED VIEW metahtml_rollup_hostmonth AS (
    SELECT
        url_host(url) AS host,
        date_trunc('month',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY host,timestamp_published
);

CREATE MATERIALIZED VIEW metahtml_rollup_month TABLESPACE fastdata AS (
    SELECT
        date_trunc('month',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY timestamp_published
);

CREATE MATERIALIZED VIEW metahtml_rollup_pub AS (
    SELECT
        date_trunc('day',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY timestamp_published
);

CREATE MATERIALIZED VIEW metahtml_rollup_accesspub AS (
    SELECT
        date_trunc('day', accessed_at) AS access_day,
        date_trunc('day',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published,
        hll_count(url) AS url,
        hll_count(url_hostpathquery_key(url)) AS hostpathquery,
        hll_count(url_hostpath_key(url)) AS hostpath
    FROM metahtml
    GROUP BY access_day, timestamp_published
);

-- EXEC SQL ENDIF;

