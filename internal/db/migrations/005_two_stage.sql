-- Version 2 uses a fresh deployment profile; do not reinterpret v1 jobs.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM jobs) THEN
    RAISE EXCEPTION 'Begom API v2 requires a fresh deployment profile; existing v1 jobs are not migrated';
  END IF;
END $$;
ALTER TABLE jobs ADD COLUMN kind text NOT NULL DEFAULT 'discovery' CHECK(kind IN ('discovery','download'));
ALTER TABLE items DROP CONSTRAINT items_status_check;
ALTER TABLE items ADD CONSTRAINT items_status_check CHECK(status IN ('pending','checking','not_found','found','available','download_queued','downloading','completed','skipped','forbidden','rate_limited','retry_wait','failed','cancelled'));
CREATE TABLE job_targets (
  job_id text NOT NULL REFERENCES jobs ON DELETE CASCADE,
  media_id text NOT NULL REFERENCES media_metadata,
  external_id numeric(20,0) NOT NULL CHECK(external_id>=0 AND external_id<=18446744073709551615),
  canonical_id text NOT NULL,
  PRIMARY KEY(job_id,media_id), UNIQUE(job_id,external_id)
);
ALTER TABLE media_metadata ADD COLUMN downloadable boolean NOT NULL DEFAULT false;
ALTER TABLE media_metadata ADD COLUMN checked_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX catalog_available ON media_metadata(id) WHERE downloadable;
CREATE INDEX discovery_media ON items(media_id,job_id) WHERE status='available';
CREATE INDEX ready_files_date ON files(created_at DESC,id) WHERE ready;
ALTER TABLE files ADD COLUMN metadata jsonb NOT NULL DEFAULT '{}';
CREATE TABLE file_previews (
  file_id text PRIMARY KEY REFERENCES files ON DELETE CASCADE,
  state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','processing','ready','failed')),
  relative_path text NOT NULL DEFAULT '',
  checksum text NOT NULL DEFAULT '',
  size_bytes bigint NOT NULL DEFAULT 0,
  duration_ms bigint,
  attempts integer NOT NULL DEFAULT 0,
  claimed_at timestamptz,
  error_code text NOT NULL DEFAULT '',
  updated_at timestamptz NOT NULL DEFAULT now()
);
