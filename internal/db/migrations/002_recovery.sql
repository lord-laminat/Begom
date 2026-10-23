CREATE TABLE system_profile(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),source_id text NOT NULL,mock boolean NOT NULL);
CREATE TABLE donor_quota_state(source_id text PRIMARY KEY,factor double precision NOT NULL DEFAULT 1 CHECK(factor>=0.1 AND factor<=1),cooldown timestamptz NOT NULL DEFAULT '-infinity',last_increase timestamptz NOT NULL DEFAULT now(),error_count integer NOT NULL DEFAULT 0,horizon_ms bigint NOT NULL DEFAULT 1700 CHECK(horizon_ms>=1700));
ALTER TABLE items ADD COLUMN sequence numeric(20,0) NOT NULL DEFAULT 0;
CREATE INDEX publications_recovery ON publications(created_at) WHERE state='verifying';
