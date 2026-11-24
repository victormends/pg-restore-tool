-- Table and TOAST storage growth audit (NOT a table bloat measurement).
-- Read-only catalog query using pg_class and pg_namespace; safe to run in
-- production without acquiring table locks or scanning heap/TOAST pages.
--
-- Note: This reports allocated relation sizes (heap, TOAST, indexes, total)
-- to identify tables growing due to large/serialized columns or retained data,
-- not dead-tuple bloat (which requires pgstattuple or visibility-map checks).

SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    pg_relation_size(c.oid) AS table_heap_bytes,
    pg_size_pretty(pg_relation_size(c.oid)) AS table_heap_size,
    COALESCE(pg_total_relation_size(c.reltoastrelid), 0) AS toast_bytes,
    pg_size_pretty(COALESCE(pg_total_relation_size(c.reltoastrelid), 0)) AS toast_size,
    COALESCE(pg_relation_size(c.reltoastrelid), 0) AS toast_heap_bytes,
    pg_indexes_size(c.oid) AS index_bytes,
    pg_size_pretty(pg_indexes_size(c.oid)) AS index_size,
    pg_total_relation_size(c.oid) AS total_bytes,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n
    ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  AND n.nspname NOT LIKE 'pg_temp_%'
  AND n.nspname NOT LIKE 'pg_toast_temp_%'
ORDER BY pg_total_relation_size(c.oid) DESC;
