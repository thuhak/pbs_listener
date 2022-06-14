CREATE OR REPLACE FUNCTION pbs.job_finish()
RETURNS trigger
LANGUAGE plpgsql
AS $BODY$
DECLARE
    payload text;
BEGIN
    IF (NEW.ji_state = 9) THEN
        payload = jsonb_build_array(NEW.ji_jobid, hstore_to_jsonb(NEW.attributes - 'comment.'::text - 'exec_host2.'::text))::text;
        IF (length(payload) > 8000) THEN
            RAISE NOTICE 'payload too big';
        ELSE
            PERFORM pg_notify('job', payload);
        END IF;
    END IF;
RETURN null;
END
$BODY$;

CREATE TRIGGER after_job_finish AFTER UPDATE on pbs.job FOR EACH ROW EXECUTE PROCEDURE pbs.job_finish();