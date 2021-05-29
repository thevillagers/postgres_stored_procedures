/*

    Inserts the contents of one table into another without having to worry about manually ordering columns or type casting

    USAGE:
        CALL vre_funcs.insert(
            insert_from := 'public.test1',
            insert_into := 'public.test2',
            add_cols    := TRUE,
            create_into_table := TRUE,
            use_cstore := TRUE
        );

*/


DROP PROCEDURE IF EXISTS vre_funcs.insert;
CREATE PROCEDURE         vre_funcs.insert(
    insert_from         TEXT,                   -- the table you want to pull the data to insert from
    insert_into         TEXT,                   -- the table you want to insert the data into
    add_cols            BOOLEAN DEFAULT TRUE,   -- flag indicating whether or not to add the columns that don't exist in insert_from to insert_into
    create_into_table   BOOLEAN DEFAULT FALSE,  -- flag indicating whether or not the iinsert_into table should be created fresh
    use_cstore          BOOLEAN DEFAULT TRUE,   -- flag indicating whether or not to make the table as cstore, if create_into_table is true
    select_col_regex    TEXT DEFAULT NULL,      -- regular expression of columns you want to pull from insert_from
    query_suffix        TEXT DEFAULT NULL       -- suffix for filtering which rows get pulled, if relevant. E.g. query_suffix:=$str$WHERE column='value'$str$
)
AS $$
#variable_conflict use_variable
DECLARE
    from_table_schema       TEXT := SPLIT_PART(insert_from, '.', 1);
    from_table_name         TEXT := SPLIT_PART(insert_from, '.', 2);
    into_table_schema       TEXT := SPLIT_PART(insert_into, '.', 1);
    into_table_name         TEXT := SPLIT_PART(insert_into, '.', 2);
    dyn_sql                 TEXT ;
    temp_sql                TEXT ;
    v_state                 TEXT ;
    v_msg                   TEXT ;
    v_detail                TEXT ;
    v_hint                  TEXT ;
    v_context               TEXT ;
    full_error_msg          TEXT ;
BEGIN


    IF create_into_table THEN
        CALL vre_funcs.create_empty_table(table_name:=insert_into, use_cstore:=use_cstore, drop_existing:=TRUE);
    END IF;

    DROP TABLE IF EXISTS from_table_cols;
    CREATE TEMP TABLE from_table_cols ON COMMIT DROP AS
    SELECT
            QUOTE_IDENT(info.column_name)   AS col_name,
            info.udt_name                   AS udt_name,
            info.ordinal_position           AS ordinal_position
      FROM
            information_schema.columns      AS info
      WHERE
            info.table_schema   = from_table_schema
        AND info.table_name     = from_table_name
        AND CASE WHEN select_col_regex IS NULL THEN TRUE ELSE info.column_name ~ select_col_regex END
    ;

    DROP TABLE IF EXISTS into_table_cols;
    CREATE TEMP TABLE into_table_cols ON COMMIT DROP AS 
    SELECT
            QUOTE_IDENT(info.column_name)   AS col_name,
            info.udt_name                   AS udt_name
      FROM
            information_schema.columns      AS info
      WHERE
            info.table_schema       = into_table_schema
        AND info.table_name         = into_table_name
    ;

    DROP TABLE IF EXISTS all_insert_cols;
    CREATE TEMP TABLE all_insert_cols ON COMMIT DROP AS 
    SELECT
            frmt.col_name           AS from_col_name,
            frmt.udt_name           AS from_udt_name,
            frmt.ordinal_position   AS from_ordinal_position,
            intt.col_name           AS into_col_name,
            intt.udt_name           AS into_udt_name
      FROM
            from_table_cols         AS frmt
      FULL OUTER JOIN
            into_table_cols         AS intt
      USING (col_name)
    ;

    IF add_cols THEN
        temp_sql := FORMAT($sql$
        SELECT
                STRING_AGG(
                    'ALTER TABLE %1$s ADD COLUMN ' || from_col_name || ' ' || from_udt_name, E';\n' ORDER BY from_ordinal_position ASC
                )
          FROM
                all_insert_cols
          WHERE
                into_col_name IS NULL AND from_col_name IS NOT NULL
        $sql$, insert_into);

        EXECUTE temp_sql INTO dyn_sql;
        IF dyn_sql IS NOT NULL THEN
            EXECUTE dyn_sql;
        END IF;
    END IF;

    DROP TABLE IF EXISTS query_strings;
    CREATE TEMP TABLE query_strings ON COMMIT DROP AS
    SELECT
            STRING_AGG(
                from_col_name || CASE WHEN into_udt_name IS NOT NULL AND from_udt_name != into_udt_name THEN  '::'||into_udt_name ELSE '' END || ' AS ' || from_col_name, E',\n'
            )                                       AS query_select_str,
            STRING_AGG(from_col_name, E',\n')       AS query_insert_str
      FROM
            all_insert_cols
      WHERE
            from_col_name IS NOT NULL
        AND CASE WHEN add_cols THEN TRUE ELSE into_col_name IS NOT NULL END
    ;

    SELECT
            'INSERT INTO ' || insert_into || '( ' ||
            query_insert_str || ') ' ||
            'SELECT ' || query_select_str ||
            ' FROM ' || insert_from || ' ' ||
            COALESCE(query_suffix, '')
      FROM
            query_strings
      INTO
            dyn_sql
    ;

    IF dyn_sql IS NOT NULL THEN
        EXECUTE dyn_sql;
    END IF;

    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            v_state     = RETURNED_SQLSTATE,
            v_msg       = MESSAGE_TEXT,
            v_detail    = PG_EXCEPTION_DETAIL,
            v_hint      = PG_EXCEPTION_HINT,
            v_context   = PG_EXCEPTION_CONTEXT
        ;

        full_error_msg := FORMAT($str$
            state: %1$s
            message: %2$s
            detail: %3$s
            hint: %4$s
            context: %5$s
        $str$, v_state, v_msg, v_detail, v_hint, v_context);

        RAISE EXCEPTION USING MESSAGE = full_error_msg;


END ; $$
LANGUAGE 'plpgsql';