-- The production role secrets select these two existing application users.
-- A local single-user database may omit them; production readiness probes
-- exercise the actual grants before history or exporter can be enabled.
DO $grants$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'collector_query') THEN
        GRANT USAGE ON SCHEMA public TO collector_query;
        GRANT SELECT ON archive_sessions, archive_event_ids, archive_chat_hot,
            archive_unassigned_chat, archive_segments, archive_quality_gaps
            TO collector_query;
    END IF;

    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'collector_worker') THEN
        GRANT USAGE ON SCHEMA public TO collector_worker;
        GRANT SELECT, INSERT, UPDATE ON archive_sessions TO collector_worker;
        GRANT SELECT, INSERT ON archive_event_ids TO collector_worker;
        GRANT SELECT, INSERT, DELETE ON archive_chat_hot TO collector_worker;
        GRANT INSERT ON archive_unassigned_chat, archive_quality_gaps TO collector_worker;
        GRANT SELECT, INSERT ON archive_segments TO collector_worker;
        GRANT USAGE ON SEQUENCE archive_event_ids_position_seq,
            archive_unassigned_chat_id_seq, archive_segments_segment_id_seq,
            archive_quality_gaps_gap_id_seq TO collector_worker;
    END IF;
END
$grants$;
