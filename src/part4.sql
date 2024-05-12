CREATE TABLE TableName1 (bibka int);
CREATE TABLE TableName2 (bibka int);

CREATE OR REPLACE PROCEDURE prc_destroy_tables_starting_with()
LANGUAGE plpgsql
AS
$procedure$
DECLARE
    table_name text;
BEGIN
    FOR table_name IN (SELECT tablename FROM pg_tables WHERE tablename LIKE 'tablename%')
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(table_name);
    END LOOP;
END;
$procedure$;

CALL prc_destroy_tables_starting_with();