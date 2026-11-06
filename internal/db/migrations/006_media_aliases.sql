-- Numeric pages can later redirect to another canonical material. Preserve the
-- old material/file while recording the current interpretation of each alias.
ALTER TABLE media_metadata DROP CONSTRAINT media_metadata_source_id_external_id_key;
CREATE INDEX media_external_id ON media_metadata(source_id,external_id);
CREATE TABLE media_aliases (
  source_id text NOT NULL,
  external_id numeric(20,0) NOT NULL,
  media_id text NOT NULL REFERENCES media_metadata,
  checked_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(source_id,external_id)
);
CREATE INDEX aliases_media ON media_aliases(media_id,external_id);
INSERT INTO media_aliases(source_id,external_id,media_id)
SELECT DISTINCT ON(m.source_id,i.external_id) m.source_id,i.external_id,m.id
FROM items i JOIN media_metadata m ON m.id=i.media_id
ORDER BY m.source_id,i.external_id,i.updated_at DESC;
