CREATE OR REPLACE FUNCTION pbs.job_finish()
RETURNS trigger
LANGUAGE plpgsql
AS $BODY$
BEGIN
IF (NEW.ji_state = 9) THEN
    PERFORM pg_notify('job', jsonb_build_array(NEW.ji_jobid, hstore_to_jsonb(NEW.attributes))::text
        ); END IF;
RETURN null;
END
$BODY$;

CREATE TRIGGER after_job_finish AFTER UPDATE on pbs.job FOR EACH ROW EXECUTE PROCEDURE pbs.job_finish();