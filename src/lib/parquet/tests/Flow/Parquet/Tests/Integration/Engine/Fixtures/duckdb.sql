-- Fixtures written by DuckDB v1.5.2 (the nix shell's duckdb). Regenerate from this directory: duckdb < duckdb.sql

CREATE TABLE t AS SELECT * FROM (VALUES (0::UBIGINT), (9223372036854775808::UBIGINT), (18446744073709551615::UBIGINT));
COPY t TO 'uint64_duckdb.parquet' (FORMAT parquet);

-- StatisticsParityTest: the statistics both engines write (statistics_parity.php), as DuckDB reads them
SELECT path_in_schema, stats_min, stats_max, stats_min_value, stats_max_value, stats_null_count, min_is_exact, max_is_exact
FROM parquet_metadata('statistics_parity_php.parquet') ORDER BY column_id;
SELECT path_in_schema, stats_min, stats_max, stats_min_value, stats_max_value, stats_null_count, min_is_exact, max_is_exact
FROM parquet_metadata('statistics_parity_arrow.parquet') ORDER BY column_id;
