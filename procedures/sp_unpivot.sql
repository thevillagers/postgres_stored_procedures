/*

    Unpivots a table

    USAGE:
        CALL vre_funcs.unpivot(
            source_table           := sources_raw.zillow_zip_zhvi_allhomes,
            primary_key_regex      := 'regionname|regiontype|statename|city|countyname',
            unpivot_regex          := '\d{4}-\d{2}-\d{2}',
            output_table           := 'staging_zillow.zillow_zip_zhvi_allhomes_unpv',
            key_name               := 'date',
            value_name             := 'zhvi'
        );

*/
DROP   PROCEDURE IF EXISTS vre_funcs.unpivot;
CREATE PROCEDURE           vre_funcs.unpivot (
        source_table        VARCHAR                     -- table you want to unpivot
       ,primary_key_regex   VARCHAR                     -- misnomer, but regex for the columns you want to carry over in the unpivoted table
       ,unpivot_regex       VARCHAR                     -- regex for the columns you want to be unpivoted
       ,output_table        VARCHAR                     -- name for output table
       ,use_cstore          BOOLEAN DEFAULT TRUE        -- flag indicating whether output should be cstore
       ,key_name            VARCHAR DEFAULT 'key'       -- name of the "key" column after unpivoting
       ,value_name          VARCHAR DEFAULT 'value'     -- name of the "value" column after unpivoting
)
    AS $$
    DECLARE 
        schema_table_arr    VARCHAR[]   ;
        table_schema_str    VARCHAR := SPLIT_PART(source_table, '.', 1);
        table_name_str      VARCHAR := SPLIT_PART(source_table, '.', 2);
        temp_sql            VARCHAR     ;
        dyn_sql             VARCHAR     ;
        foreign_table_pt1   VARCHAR := '' ;
        foreign_table_pt2   VARCHAR := '' ;
        value_type          VARCHAR := 'TEXT' ;
        value_type_count    INTEGER     ;
        v_state   TEXT;
        v_msg     TEXT;
        v_detail  TEXT;
        v_hint    TEXT;
        v_context TEXT;
    BEGIN 
        IF use_cstore THEN
            foreign_table_pt1 := 'FOREIGN';
            foreign_table_pt2 := 'SERVER cstore_server OPTIONS (compression ''pglz'')';
        END IF;


        CREATE TEMP TABLE unpv_pk_cols_with_types ON COMMIT DROP AS 
        SELECT
                column_name,
                udt_name,
                ordinal_position
          FROM
                information_schema.columns
          WHERE
                table_schema = table_schema_str
            AND table_name   = table_name_str
            AND column_name !~ unpivot_regex
            AND column_name ~ primary_key_regex
        ;

        CREATE TEMP TABLE unpv_cols_with_types ON COMMIT DROP AS 
        SELECT
                column_name,
                udt_name
          FROM
                information_schema.columns
          WHERE
                table_schema = table_schema_str
            AND table_name   = table_name_str
            AND column_name  ~ unpivot_regex
        ;

        SELECT
                COUNT(DISTINCT udt_name)
          FROM
                unpv_cols_with_types
        INTO value_type_count;

        IF value_type_count = 1 THEN
            SELECT
                    MAX(udt_name)
              FROM
                    unpv_cols_with_types
            INTO value_type;
        END IF;

        SELECT
                STRING_AGG(
                    QUOTE_IDENT(column_name) || ' ' || udt_name,
                    E',\n' ORDER BY ordinal_position ASC
                )
          FROM
                unpv_pk_cols_with_types
        INTO temp_sql;


        CALL vre_funcs.drop(output_table);
        dyn_sql := FORMAT($sql$
        CREATE %1$s TABLE %2$s (
            %3$s,
            %4$s text,
            %5$s %6$s
        ) %7$s
        $sql$, foreign_table_pt1
         , output_table
         , temp_sql
         , key_name
         , value_name
         , value_type
         , foreign_table_pt2);
        EXECUTE dyn_sql;


        SELECT
                STRING_AGG(
                    QUOTE_IDENT(column_name), E',\n' ORDER BY ordinal_position ASC
                )
          FROM
                unpv_pk_cols_with_types
        INTO temp_sql;


        temp_sql := FORMAT($sql$
        SELECT
                STRING_AGG(
                    'INSERT INTO %1$s (
                        %2$s,
                        %3$s,
                        %4$s
                    )
                    SELECT
                        %2$s,
                        '''||column_name||''',
                        '||QUOTE_IDENT(column_name)||'
                    FROM
                        %5$s', E';\n'
                )
                    FROM 
                        unpv_cols_with_types
        $sql$, output_table, temp_sql, key_name, value_name, source_table);

        EXECUTE temp_sql INTO dyn_sql;

        EXECUTE dyn_sql;


        EXCEPTION WHEN OTHERS THEN 
            get stacked diagnostics
        v_state   = returned_sqlstate,
        v_msg     = message_text,
        v_detail  = pg_exception_detail,
        v_hint    = pg_exception_hint,
        v_context = pg_exception_context;

    raise notice E'Got exception:
        state  : %
        message: %
        detail : %
        hint   : %
        context: %', v_state, v_msg, v_detail, v_hint, v_context;
    END; $$
LANGUAGE 'plpgsql';


/*
EXAMPLE:

CALL vre_funcs.unpivot_table(
    source_table := 'sources_raw.state_zhvi_uc_sfrcondo_tier_0_33_0_67_sm_sa_mon',
    primary_key_regex := 'RegionID|SizeRank|RegionName|RegionType|StateName',
    unpivot_regex     := '\d{4}-\d{2}-\d{2}',
    output_table      := 'sources_inter.zip_zhvi_allhomes_unpv_fast',
    key_name          := 'date',
    value_name        := 'zhvi'
);
*/

SELECT * FROM sources_inter.zip_zhvi_allhomes_unpv_fast LIMIT 50;
