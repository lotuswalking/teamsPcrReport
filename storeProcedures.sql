-- PROCEDURE: public.exportReport()
-- DROP PROCEDURE IF EXISTS public."exportReport"();
CREATE OR REPLACE PROCEDURE public."exportReport"() LANGUAGE 'plpgsql' AS $$
DECLARE tablename text := 'History_200302';
MonthName text := '202302';
BEGIN RAISE NOTICE 'Starting the procedure';
RAISE NOTICE 'tablename: %',
tablename;
RAISE NOTICE 'MonthName: %',
MonthName;
-- Your other code logic here
RAISE NOTICE 'Procedure completed';
END;
$$;
ALTER PROCEDURE public."exportReport"() OWNER TO uccadmin;