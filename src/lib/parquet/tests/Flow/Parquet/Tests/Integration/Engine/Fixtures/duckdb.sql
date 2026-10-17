-- Fixtures written by DuckDB v1.5.2 (the nix shell's duckdb). Regenerate from this directory: duckdb < duckdb.sql

CREATE TABLE t AS SELECT * FROM (VALUES (0::UBIGINT), (9223372036854775808::UBIGINT), (18446744073709551615::UBIGINT));
COPY t TO 'uint64_duckdb.parquet' (FORMAT parquet);

-- StatisticsParityTest: the statistics both engines write (statistics_parity.php), as DuckDB reads them
SELECT path_in_schema, stats_min, stats_max, stats_min_value, stats_max_value, stats_null_count, min_is_exact, max_is_exact
FROM parquet_metadata('statistics_parity_php.parquet') ORDER BY column_id;
SELECT path_in_schema, stats_min, stats_max, stats_min_value, stats_max_value, stats_null_count, min_is_exact, max_is_exact
FROM parquet_metadata('statistics_parity_arrow.parquet') ORDER BY column_id;

-- FilterParityTest: FilterRows::rows(2048) (Context/FilterRows.php), four row groups of 2048 rows: DuckDB writes no
-- smaller group
SET TimeZone = 'UTC';
SET threads = 1;
COPY (
    SELECT
        i::BIGINT AS id,
        CASE i // 2048 WHEN 0 THEN '9' WHEN 1 THEN '10' WHEN 2 THEN '100' ELSE ['9', '10', '100'][i % 3 + 1] END AS label,
        CASE WHEN i = 2048 THEN '-0.0'::DOUBLE WHEN i = 2049 OR i // 2048 = 2 THEN 'NaN'::DOUBLE ELSE i::DOUBLE END AS score,
        CASE WHEN i // 2048 = 0 OR (i // 2048 = 1 AND i % 2 = 1) THEN NULL ELSE i::BIGINT END AS maybe,
        DATE '2024-01-01' + i::INTEGER AS day,
        TIMESTAMPTZ '2024-01-01 00:00:00.123456+00' + i * INTERVAL 1 HOUR AS ts,
        (i + 0.25)::DECIMAL(10, 2) AS amount,
        CASE WHEN i // 2048 = 3 AND i % 5 = 0 THEN NULL ELSE {'city': 'c' || (i // 2048)} END AS address,
        CASE WHEN i // 2048 >= 2 THEN 18446744073709551615::UBIGINT - (8191 - i)::UBIGINT ELSE i::UBIGINT END AS big,
        format('{:08x}-0000-4000-8000-{:012x}', i // 2048, i)::UUID AS uid,
        CASE i // 2048 WHEN 0 THEN false WHEN 1 THEN true ELSE i % 2 = 0 END AS flag,
        (i + 0.5)::FLOAT AS ratio,
        CASE i // 2048 WHEN 0 THEN 'ąbc' WHEN 1 THEN 'źdźbło' WHEN 2 THEN 'zebra' ELSE ['ąbc', 'źdźbło', 'zebra'][i % 3 + 1] END AS word
    FROM range(8192) t(i)
    ORDER BY i
) TO 'Filters/row_groups_duckdb.parquet' (FORMAT parquet, ROW_GROUP_SIZE 2048);
