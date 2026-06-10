DROP FUNCTION IF EXISTS purge_published_outbox(TIMESTAMPTZ);
DROP FUNCTION IF EXISTS archive_and_drop_partition(DATE);
DROP FUNCTION IF EXISTS create_delivery_partition(DATE);
