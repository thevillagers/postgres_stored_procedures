/*

    Pivots a table based on a pivot specification in the pivot_json field

    USAGE:
        CALL vre_funcs.pivot(
            source_table := 'sources_inter.zip_zhvi_allhomes_unpv_fast',
            primary_key_regex := '^(RegionName)$',
            pivot_json := '
            {
              "date": {"column": "zhvi::numeric", "function": "MAX([col])", "prefix": "date_"},
              "date": {"column": "zhvi", "function": "COUNT([col])", "prefix": "count_"}
              }',
            output_table := 'sources_inter.zip_zhvi_allhomes_unpv_fast_repiv',
            ignore_vals := $sql$zhvi IS NOT NULL$sql$
        );

    pivot_json formatting:
      I did this in kind of a shitty way but I don't want to change it now because I use it in quite a few places

      Basically, each pivot operation you want to perform is a key:value pair in the json field

      The key is the name of the column you want to use as the pivot key, and the value is a JSON object itself.
        The JSON object that is the value has two required keys: "column" and "function".
        "column" is the field that will be used for getting the value of your pivots, and "function" is the operation done in the aggregation.
          In the "function" value, you refer to the column being pivoted as [col] and this gets replaced by the necessary code
        Optional arguments are "prefix", which adds a prefix to the pivoted column names, and "suffix" that does similar
      
*/

DROP   PROCEDURE IF EXISTS vre_funcs.pivot;
CREATE PROCEDURE           vre_funcs.pivot (
    source_table        TEXT,                 -- table that you want to pivot
    primary_key_regex   TEXT,                 -- regular expression for columns that you want to be in the GROUP BY when pivoting
    pivot_json          TEXT,                 -- pivot_json, more details in comment above
    output_table        TEXT,                 -- output table name
    ignore_vals         TEXT DEFAULT NULL,    -- values to ignore when pivoting, example above
    use_cstore          BOOLEAN DEFAULT TRUE  -- doesn't actually do anything in this version, can ignore
)
AS $$
#variable_conflict use_variable
DECLARE 
    schema_table_arr    TEXT[];
    table_schema        TEXT := SPLIT_PART(source_table, '.', 1);
    table_name          TEXT := SPLIT_PART(source_table, '.', 2);
    temp_sql            TEXT;
    dyn_sql             TEXT;
    col_defs            TEXT;
    pk_cols             TEXT;
    v_state   TEXT;
    v_msg     TEXT;
    v_detail  TEXT;
    v_hint    TEXT;
    v_context TEXT;
BEGIN 

    DROP TABLE IF EXISTS pvt_param_list;
    CREATE TEMP TABLE pvt_param_list ON COMMIT DROP AS
    SELECT 'column'::TEXT      AS param, null::TEXT AS default UNION
    SELECT 'function'::TEXT    AS param, null::TEXT AS default UNION
    SELECT 'prefix'::TEXT      AS param, ''::TEXT AS default UNION 
    SELECT 'suffix'::TEXT      AS param, ''::TEXT AS default
    ;


    DROP TABLE IF EXISTS pvt_pivot_params;
    CREATE TEMP TABLE pvt_pivot_params ON COMMIT DROP AS
    SELECT 
            rown.pivot_grouping             AS pivot_grouping,
            rown.pivot_column               AS pivot_column,
            pmtr.key                        AS pivot_param_key,
            TRIM('"' FROM pmtr.value::TEXT) AS pivot_param_value
               ,null::TEXT               AS udt_name
              FROM 
                (
                    SELECT 
                        nrow.pivot_column   AS pivot_column
                       ,nrow.pivot_params   AS pivot_params
                       ,ROW_NUMBER() OVER() AS pivot_grouping
                      FROM 
                        (
                            SELECT
                                key         AS pivot_column 
                               ,value       AS pivot_params
                              FROM 
                                json_each(pivot_json::json)
                        )   AS nrow
                )   AS rown,
                LATERAL json_each(rown.pivot_params)    AS pmtr
    ;
    DROP TABLE IF EXISTS pvt_all_param_list;
    CREATE TEMP TABLE pvt_all_param_list ON COMMIT DROP AS
    SELECT DISTINCT
            usrp.pivot_grouping,
            usrp.pivot_column,
            dflt.param,
            dflt.default
      FROM
          (
            SELECT DISTINCT pivot_grouping, pivot_column FROM pvt_pivot_params
          )             AS usrp
      CROSS JOIN
          pvt_param_list  AS dflt
    ;


    DROP TABLE IF EXISTS pvt_all_params_joined;
    CREATE TEMP TABLE pvt_all_params_joined ON COMMIT DROP AS
    SELECT
            pful.pivot_grouping                             AS pivot_grouping,
            pful.pivot_column                               AS pivot_column,
            COALESCE(pusr.pivot_param_key, pful.param)      AS pivot_param_key,
            COALESCE(pusr.pivot_param_value, pful.default)  AS pivot_param_value
      FROM 
            pvt_pivot_params                                AS pusr
      FULL OUTER JOIN 
            pvt_all_param_list                              AS pful
      ON 
            pusr.pivot_grouping     = pful.pivot_grouping
        AND pusr.pivot_param_key    = pful.param
    ;

    DROP TABLE IF EXISTS pvt_pivot_distinct_pivot_cols;
    CREATE TEMP TABLE pvt_pivot_distinct_pivot_cols ON COMMIT DROP AS
    SELECT DISTINCT
            pivot_column
      FROM
            pvt_pivot_params
    ;

    DROP TABLE IF EXISTS pvt_pivot_col_vals_by_col;
    CREATE TEMP TABLE pvt_pivot_col_vals_by_col(
        column_name   TEXT,
        column_value  TEXT
    );


    temp_sql := FORMAT($sql$
        SELECT
            STRING_AGG(
                'INSERT INTO pvt_pivot_col_vals_by_col (
                    column_name
                   ,column_value
                )
                SELECT DISTINCT
                    '''||pivot_column||'''
                   ,'||pivot_column||'::TEXT
                  FROM 
                    %1$s'
            , E';\n\n'
            )
          FROM 
              pvt_pivot_distinct_pivot_cols
    $sql$, source_table);
    EXECUTE temp_sql INTO dyn_sql;
    EXECUTE dyn_sql;

    DROP TABLE IF EXISTS pvt_pivot_column_definitions;
    CREATE TEMP TABLE pvt_pivot_column_definitions ON COMMIT DROP AS
    SELECT
          REGEXP_REPLACE(pvt_fnc.pivot_param_value, '\[col\]', 'CASE WHEN '||pvt_col.pivot_column||' = '''||pvt_val.column_value||''' THEN '||pvt_col.pivot_param_value||' ELSE NULL END')||' AS '||QUOTE_IDENT(pvt_pre.pivot_param_value||pvt_val.column_value||pvt_suf.pivot_param_value) AS col_def
      FROM 
        pvt_all_params_joined        AS pvt_col
      INNER JOIN 
        pvt_all_params_joined        AS pvt_fnc
      ON 
            pvt_col.pivot_param_key = 'column'
        AND pvt_fnc.pivot_param_key = 'function'
        AND pvt_col.pivot_grouping  = pvt_fnc.pivot_grouping
      INNER JOIN 
        pvt_all_params_joined        AS pvt_pre
      ON 
            pvt_col.pivot_grouping  = pvt_pre.pivot_grouping
        AND pvt_pre.pivot_param_key = 'prefix'
      INNER JOIN 
        pvt_all_params_joined        AS pvt_suf
      ON
            pvt_col.pivot_grouping  = pvt_suf.pivot_grouping
        AND pvt_suf.pivot_param_key = 'suffix'
      INNER JOIN 
        pvt_pivot_col_vals_by_col    AS pvt_val
      ON 
        pvt_col.pivot_column        = pvt_val.column_name
    ;

    DROP TABLE IF EXISTS pvt_pivot_pk_cols;
    CREATE TEMP TABLE pvt_pivot_pk_cols ON COMMIT DROP AS 
    SELECT 
            info.column_name,
            info.udt_name,
            info.ordinal_position
      FROM 
            information_schema.columns  AS info
      WHERE 
            info.table_schema   = table_schema
        AND info.table_name     = table_name
        AND info.column_name    ~ primary_key_regex
    ;

    SELECT
            STRING_AGG(col_def, E',\n' ORDER BY col_def ASC)
      FROM 
            pvt_pivot_column_definitions
      INTO
            col_defs
    ;

    SELECT
            STRING_AGG(QUOTE_IDENT(column_name), E',\n' ORDER BY ordinal_position ASC)
      FROM 
            pvt_pivot_pk_cols
      INTO
            pk_cols
    ;

    IF ignore_vals IS NOT NULL THEN
        ignore_vals := E'WHERE\n'||ignore_vals;
    ELSE
        ignore_vals := '';
    END IF;

    CALL vre_funcs.drop(output_table);

    dyn_sql := FORMAT($sql$
    CREATE TABLE %1$s AS
    SELECT
        %2$s,
        %3$s
      FROM
        %4$s
      %5$s
      GROUP BY
        %2$s
    $sql$, output_table, pk_cols, col_defs, source_table, ignore_vals);
    EXECUTE dyn_sql;

    EXCEPTION WHEN OTHERS THEN 
        GET STACKED DIAGNOSTICS
            v_state   = returned_sqlstate,
            v_msg     = message_text,
            v_detail  = pg_exception_detail,
            v_hint    = pg_exception_hint,
            v_context = pg_exception_context
        ;

    RAISE NOTICE E'Got exception:
        state  : %
        message: %
        detail : %
        hint   : %
        context: %', v_state, v_msg, v_detail, v_hint, v_context;
END; $$
LANGUAGE 'plpgsql';




