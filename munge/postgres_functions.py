from sa_util import run_sql


sql = '''
BEGIN;
-- https://wiki.postgresql.org/wiki/Aggregate_Median
CREATE OR REPLACE FUNCTION _final_median(anyarray) RETURNS float8 AS $$
  WITH q AS
  (
     SELECT val
     FROM unnest($1) val
     WHERE VAL IS NOT NULL
     ORDER BY 1
  ),
  cnt AS
  (
    SELECT COUNT(*) AS c FROM q
  )
  SELECT AVG(val)::float8
  FROM
  (
    SELECT val FROM q
    LIMIT  2 - MOD((SELECT c FROM cnt), 2)
    OFFSET GREATEST(CEIL((SELECT c FROM cnt) / 2.0) - 1,0)
  ) q2;
$$ LANGUAGE SQL IMMUTABLE;

DROP AGGREGATE IF EXISTS usr_median(anyelement);

CREATE AGGREGATE usr_median(anyelement) (
  SFUNC=array_append,
  STYPE=anyarray,
  FINALFUNC=_final_median,
  INITCOND='{}'
);


-- https://wiki.postgresql.org/wiki/Aggregate_Mode

CREATE OR REPLACE FUNCTION _final_mode(anyarray)
  RETURNS anyelement AS
$BODY$
    SELECT a
    FROM unnest($1) a
    GROUP BY 1
    ORDER BY COUNT(1) DESC, 1
    LIMIT 1;
$BODY$
LANGUAGE SQL IMMUTABLE;

-- Tell Postgres how to use our aggregate
DROP AGGREGATE IF EXISTS usr_mode(anyelement);

CREATE AGGREGATE usr_mode(anyelement) (
  SFUNC=array_append, --Function to call for each row. Just builds the array
  STYPE=anyarray,
  FINALFUNC=_final_mode, --Function to call after everything has been added to array
  INITCOND='{}' --Initialize an empty array when starting
);


CREATE OR REPLACE FUNCTION _final_median2(anyarray) RETURNS float8 AS $$

  WITH y AS (
     SELECT val, row_number() OVER (ORDER BY total_area) AS rn
     FROM unnest($1) val
     WHERE VAL IS NOT NULL
   )
  , c AS (SELECT count(*) AS ct FROM y)

SELECT CASE WHEN c.ct%2 = 0 THEN
          round((SELECT avg(val) FROM y WHERE y.rn IN (c.ct/2, c.ct/2+1)), 3)
       ELSE
                (SELECT     val  FROM y WHERE y.rn = (c.ct+1)/2)
       END AS median
FROM   c;
$$ LANGUAGE SQL IMMUTABLE;

DROP AGGREGATE IF EXISTS usr_median2(anyelement);

CREATE AGGREGATE usr_median2(anyelement) (
  SFUNC=array_append,
  STYPE=anyarray,
  FINALFUNC=_final_median2,
  INITCOND='{}'
);


COMMIT;
'''

run_sql(sql)


sql = '''
CREATE OR REPLACE FUNCTION percent_diff(a float8, b float8) RETURNS float8 AS $BODY$
SELECT
    CASE
        WHEN $1 > 0
            THEN ($2 / $1) - 1.0
        ELSE NULL
    END;

$BODY$
LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION safe_divide(numeric, numeric)
  RETURNS numeric AS $$
   SELECT CASE
     WHEN $1 IS NULL OR $2 IS NULL OR $2 = 0 THEN NULL ELSE $1 / $2 END;
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION safe_divide(double precision, double precision)
  RETURNS numeric AS $$
   SELECT CASE
     WHEN $1 IS NULL OR $2 IS NULL OR $2 = 0 THEN NULL
     ELSE $1::numeric / $2::numeric END;
  $$ LANGUAGE SQL;

COMMIT;
'''

run_sql(sql)
