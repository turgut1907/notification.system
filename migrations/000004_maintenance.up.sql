-- Partition maintenance helpers. These are invoked by the scheduler's Go
-- maintenance job (always available) and can equally be scheduled via pg_cron in
-- production. Keeping the DDL logic in SQL means the storage tuning lives in one place.

-- Create a daily partition for the given day if it does not already exist. Each
-- partition reserves page space for HOT updates and vacuums eagerly under churn.
CREATE OR REPLACE FUNCTION create_delivery_partition(p_day DATE)
RETURNS void AS $$
DECLARE
    part_name TEXT := 'notification_deliveries_p' || to_char(p_day, 'YYYYMMDD');
    start_ts  TIMESTAMPTZ := p_day::timestamptz;
    end_ts    TIMESTAMPTZ := (p_day + 1)::timestamptz;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = part_name) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF notification_deliveries ' ||
            'FOR VALUES FROM (%L) TO (%L) ' ||
            'WITH (fillfactor=80, autovacuum_vacuum_scale_factor=0.02, autovacuum_vacuum_cost_limit=2000)',
            part_name, start_ts, end_ts
        );
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Archive terminal rows from a day's partition into the cold tier, then drop the
-- partition (O(1) reclaim, no mass DELETE/vacuum churn). Rows older than the
-- retention window are guaranteed terminal in practice (max retry horizon << window).
CREATE OR REPLACE FUNCTION archive_and_drop_partition(p_day DATE)
RETURNS void AS $$
DECLARE
    part_name TEXT := 'notification_deliveries_p' || to_char(p_day, 'YYYYMMDD');
BEGIN
    IF EXISTS (SELECT 1 FROM pg_class WHERE relname = part_name) THEN
        EXECUTE format(
            'INSERT INTO notification_deliveries_archive ' ||
            '(id,request_id,channel,priority,status,attempt_count,provider_message_id,' ||
            'last_error,next_retry_at,locked_until,send_at,created_at,updated_at) ' ||
            'SELECT id,request_id,channel,priority,status,attempt_count,provider_message_id,' ||
            'last_error,next_retry_at,locked_until,send_at,created_at,updated_at ' ||
            'FROM %I WHERE status IN (''SENT'',''FAILED'',''CANCELLED'')',
            part_name
        );
        EXECUTE format('DROP TABLE %I', part_name);
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Purge published outbox rows older than the given cutoff.
CREATE OR REPLACE FUNCTION purge_published_outbox(p_before TIMESTAMPTZ)
RETURNS bigint AS $$
DECLARE
    deleted bigint;
BEGIN
    DELETE FROM outbox WHERE published_at IS NOT NULL AND published_at < p_before;
    GET DIAGNOSTICS deleted = ROW_COUNT;
    RETURN deleted;
END;
$$ LANGUAGE plpgsql;
