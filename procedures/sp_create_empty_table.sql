/*

    Creates an empty table with the given name, by default drops the existing object with that name and makes the table as cstore

    USAGE:
        CALL vre_funcs.create_empty_table(table_name:='public.test_table', use_cstore:=FALSE, drop_existing:=TRUE);

*/

DROP PROCEDURE IF EXISTS vre_funcs.create_empty_table;
CREATE PROCEDURE         vre_funcs.create_empty_table(
    table_name      TEXT,                       -- name of the table to create
    use_cstore      BOOLEAN DEFAULT TRUE,       -- flag stating whether or not to make the table as cstore, default true
    drop_existing   BOOLEAN DEFAULT TRUE        -- flag stating whether or not any existing object with the name table_name should be dropped
)
AS $$
BEGIN
    IF drop_existing THEN
        CALL vre_funcs.drop(table_name);
    END IF;
    
    EXECUTE FORMAT($sql$
        CREATE %1$s TABLE %2$s () %3$s
    $sql$,
    CASE WHEN use_cstore THEN 'FOREIGN' ELSE '' END,
    table_name,
    CASE WHEN use_cstore THEN 'SERVER cstore_server OPTIONS (compression ''pglz'')' ELSE '' END);

END ; $$
LANGUAGE 'plpgsql';
