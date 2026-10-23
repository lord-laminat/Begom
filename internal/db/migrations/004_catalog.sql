ALTER TABLE files ADD COLUMN verified_at timestamptz;
ALTER TABLE publications ADD COLUMN reconciled_at timestamptz;
CREATE INDEX file_scrub_queue ON files(verified_at NULLS FIRST) WHERE ready;
