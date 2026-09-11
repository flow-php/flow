<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Datasets;

/**
 * Named FixtureFormat rather than Format so the short name does not collide with Flow\Floe\Format.
 */
enum FixtureFormat: string
{
    /**
     * Every derived fixture is from_parquet(root) -> to_X(), and the root itself is
     * FakeRandomOrdersExtractor -> to_parquet, so these six trees are in the byte-producing
     * chain of every fixture regardless of its format. Floe lives inside src/core/etl/src.
     *
     * benchmarks/src/Datasets is in the chain because TextDataset and SellersDataset build their
     * rows in PHP rather than delegating to an adapter, so their bytes change with this tree.
     */
    private const CHAIN = [
        'src/core/etl/src',
        'src/core/etl/tests/Flow/ETL/Tests/Double/FakeRandomOrdersExtractor.php',
        'src/lib/types/src',
        'src/lib/filesystem/src',
        'src/adapter/etl-adapter-parquet/src',
        'src/lib/parquet/src',
        'benchmarks/src/Datasets',
    ];

    case csv = 'csv';
    case excel = 'excel';
    case floe = 'floe';
    case json = 'json';
    case json_lines = 'json_lines';
    case parquet = 'parquet';
    case text = 'text';
    case xml = 'xml';

    public function extension(): string
    {
        return match ($this) {
            self::csv => 'csv',
            self::excel => 'xlsx',
            self::floe => 'floe',
            self::json => 'json',
            self::json_lines => 'jsonl',
            self::parquet => 'parquet',
            self::text => 'txt',
            self::xml => 'xml',
        };
    }

    /**
     * Repo-root-relative trees whose change can change the bytes of a fixture in this format.
     *
     * @return list<string>
     */
    public function trees(): array
    {
        return [
            ...self::CHAIN,
            ...match ($this) {
                self::parquet, self::floe => [],
                self::csv => ['src/adapter/etl-adapter-csv/src'],
                self::json, self::json_lines => ['src/adapter/etl-adapter-json/src'],
                self::excel => ['src/adapter/etl-adapter-excel/src'],
                self::text => ['src/adapter/etl-adapter-text/src'],
                self::xml => ['src/adapter/etl-adapter-xml/src'],
            },
        ];
    }
}
