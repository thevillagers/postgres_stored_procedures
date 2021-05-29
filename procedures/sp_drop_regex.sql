/*

    Calls the vre_funcs.drop() procedure on all objects matching a regex

    USAGE:
        CALL vre_funcs.drop_regex(drop_regex:='^public\.test', relkind_regex:='.*');

*/

DROP   PROCEDURE IF EXISTS vre_funcs.drop_regex;
CREATE PROCEDURE           vre_funcs.drop_regex(
        drop_regex          TEXT,                   -- regular expression of objects to drop from the dp
        relkind_regex       TEXT DEFAULT '.*'       -- if you only want to drop certain relkinds (e.g. only tables) you can set that here
)
AS $$
DECLARE
    dyn_sql     varchar ;
BEGIN
    SELECT
          STRING_AGG(
            'CALL vre_funcs.drop(''' || QUOTE_IDENT(pgn.nspname)||'.'||QUOTE_IDENT(pgc.relname) || E''');'
          ,E'\n'
          )
      FROM
          pg_class      AS pgc
      INNER JOIN
          pg_namespace  AS pgn
      ON
          pgc.relnamespace  = pgn.oid
      WHERE
          pgn.nspname||'.'||pgc.relname ~ drop_regex
      AND pgc.relkind ~ relkind_regex
    INTO dyn_sql;

    IF dyn_sql IS NOT NULL THEN
        EXECUTE dyn_sql;
    END IF;
    
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Error when running query %', dyn_sql;
END ; $$
LANGUAGE 'plpgsql';
