
-- powershell line to help running sql file in container
-- docker exec -it data-ingest-db psql -U airflow -d data_ingest -c <metadata.sql



CREATE TABLE IF NOT EXISTS job_control_queue (
     job_id SERIAL PRIMARY KEY,
     dag_id_to_trigger VARCHAR(100) NOT NULL,
     is_ready BOOLEAN DEFAULT FALSE,
     job_params JSONB,
     last_triggered TIMESTAMP
);


INSERT INTO job_control_queue (dag_id, is_ready)
values('postgres_select', TRUE);

INSERT INTO job_control_queue (dag_id, is_ready)
values('select2', TRUE);


 