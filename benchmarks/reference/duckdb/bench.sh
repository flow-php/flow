#!/usr/bin/env bash
# usage: bench.sh <scan|to_parquet|group_by|sort> <input.csv|input.parquet> <output-dir> <threads>
# scan: count the rows that pass a predicate touching every column, so every column is decoded (a bare count(*) on
# Parquet is answered from the footer).
set -euo pipefail

shape="$1"; input="$2"; output="$3"; threads="$4"

case "$input" in
    *.parquet) source="read_parquet('$input')" ;;
    *)         source="read_csv('$input', header = true)" ;;
esac

scan="id % 2 >= 0 AND quantity % 2 >= 0 AND amount >= 0 AND email <> '' AND city <> '' AND country <> '' AND first_name <> '' AND last_name <> '' AND order_uuid IS NOT NULL AND created_at IS NOT NULL AND active IS NOT NULL"

case "$shape" in
    scan)       query="SELECT count(*) FROM $source WHERE $scan;" ;;
    to_parquet) query="COPY (SELECT * FROM $source) TO '$output/to_parquet.parquet' (FORMAT parquet);" ;;
    group_by)   query="COPY (SELECT country, sum(amount) AS amount_sum, count(id) AS id_count, avg(amount) AS amount_avg FROM $source GROUP BY country) TO '$output/group_by.parquet' (FORMAT parquet);" ;;
    sort)       query="COPY (SELECT * FROM $source ORDER BY amount DESC) TO '$output/sort.parquet' (FORMAT parquet);" ;;
    *) echo "unknown shape: $shape" >&2; exit 2 ;;
esac

mkdir -p "$output"
duckdb -c "SET threads TO $threads; $query"
