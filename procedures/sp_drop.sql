/*

    Drops any object in the DB with just the name, casdace by default

    USAGE:
        CALL vre_funcs.drop(name:='schema.table', cascade:=TRUE);

*/

DROP   PROCEDURE IF EXISTS vre_funcs.drop;
CREATE PROCEDURE           vre_funcs.drop(
        name        TEXT,                   -- name of object to drop
        cascade     BOOLEAN DEFAULT TRUE    -- flag indicating whether or not to include "CASCADE" in the drop statement
)
AS $$
DECLARE
    drop_str    TEXT;
    schema_name TEXT    := SPLIT_PART(name, '.', 1);
    object_name TEXT    := SPLIT_PART(name, '.', 2);
    cascade_str TEXT    := CASE WHEN cascade THEN 'CASCADE' ELSE '' END;
BEGIN

    SELECT
            CASE
                WHEN pgc.relkind = 'r' THEN 'TABLE'
                WHEN pgc.relkind = 'f' THEN 'FOREIGN TABLE'
                WHEN pgc.relkind = 'i' THEN 'INDEX'
                WHEN pgc.relkind = 's' THEN 'SEQUENCE'
                WHEN pgc.relkind = 'v' THEN 'VIEW'
                WHEN pgc.relkind = 'm' THEN 'MATERIALIZED VIEW'
                WHEN pgc.relkind = 'c' THEN 'TYPE'
                ELSE NULL END
      FROM
            pg_class                AS pgc
      INNER JOIN
            pg_namespace            AS pgn
      ON
            pgc.relnamespace    = pgn.oid
      WHERE
            pgn.nspname         = schema_name
        AND pgc.relname         = object_name
    INTO drop_str;

    IF drop_str IS NOT NULL THEN
        EXECUTE FORMAT($sql$
        DROP %1$s IF EXISTS %2$s %3$s
        $sql$, drop_str, name, cascade_str);
    END IF;

    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error when trying to drop %', name;
END;
$$ LANGUAGE plpgsql;
